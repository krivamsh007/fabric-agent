#!/usr/bin/env python
"""
FabricOps one-click Delta maintenance runner.

What this script does:
1) Auto-discovers workspaces (or uses an explicit scope)
2) Runs MaintenanceGuard across the scope
3) Uses capacity-aware concurrency (Trial -> sequential)
4) Writes a run report to data/maintenance_runs/

Examples:
  # Safe preview (default dry-run) for all visible workspaces
  python scripts/run_fabricops_delta_maintenance.py --all-workspaces

  # Live run for one workspace
  python scripts/run_fabricops_delta_maintenance.py --workspace-ids <ws-guid> --live

  # Live run for all ENT_* workspaces
  python scripts/run_fabricops_delta_maintenance.py --all-workspaces --workspace-name-prefix ENT_ --live
"""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

from fabric_agent.api.fabric_client import FabricApiClient
from fabric_agent.core.config import FabricAuthConfig
from fabric_agent.guards.maintenance_guard import MaintenanceGuard


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "data" / "maintenance_runs"


def _items(data: Any) -> List[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    value = data.get("value")
    if isinstance(value, list):
        return value
    payload = data.get("data")
    if isinstance(payload, list):
        return payload
    return []


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _now_utc().isoformat()


def _parse_csv(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def _parse_iso_utc(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _load_last_success_index(lookback_runs: int) -> Dict[Tuple[str, str, str], datetime]:
    """
    Build table-level last-success index from recent maintenance reports.

    Key: (workspace_id, lakehouse_id, table_name_lower)
    Value: latest succeeded completion timestamp
    """
    index: Dict[Tuple[str, str, str], datetime] = {}
    if lookback_runs <= 0:
        return index

    reports = sorted(
        OUTPUT_DIR.glob("delta_maintenance_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )[:lookback_runs]

    for report_file in reports:
        try:
            payload = json.loads(report_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        records = payload.get("job_records", [])
        if not isinstance(records, list):
            continue
        for rec in records:
            if not isinstance(rec, dict):
                continue
            status = str(rec.get("status", "")).strip().lower()
            if status != "succeeded":
                continue
            ws_id = str(rec.get("workspace_id", "")).strip()
            lh_id = str(rec.get("lakehouse_id", "")).strip()
            table = str(rec.get("table_name", "")).strip().lower()
            if not ws_id or not lh_id or not table:
                continue
            ts = _parse_iso_utc(rec.get("completed_at") or rec.get("submitted_at"))
            if ts is None:
                continue
            key = (ws_id, lh_id, table)
            prev = index.get(key)
            if prev is None or ts > prev:
                index[key] = ts

    return index


async def _discover_workspaces(
    client: FabricApiClient,
    include_ids: List[str],
    include_names: List[str],
    exclude_names: List[str],
    workspace_name_prefix: str,
    max_workspaces: Optional[int],
) -> List[Tuple[str, str]]:
    resp = await client.get("/workspaces")
    candidates = _items(resp)

    include_ids_l = {x.lower() for x in include_ids}
    include_names_l = {x.lower() for x in include_names}
    exclude_names_l = {x.lower() for x in exclude_names}
    prefix_l = workspace_name_prefix.lower().strip()

    selected: List[Tuple[str, str]] = []
    for ws in candidates:
        ws_id = str(ws.get("id", "")).strip()
        ws_name = str(ws.get("displayName", ws_id)).strip()
        if not ws_id:
            continue

        ws_id_l = ws_id.lower()
        ws_name_l = ws_name.lower()

        if include_ids_l and ws_id_l not in include_ids_l:
            continue
        if include_names_l and ws_name_l not in include_names_l:
            continue
        if prefix_l and not ws_name_l.startswith(prefix_l):
            continue
        if exclude_names_l and ws_name_l in exclude_names_l:
            continue

        selected.append((ws_id, ws_name))

    if max_workspaces is not None and max_workspaces > 0:
        selected = selected[:max_workspaces]
    return selected


async def _discover_table_inventory(
    client: FabricApiClient,
    workspace_ids: List[str],
) -> List[Dict[str, str]]:
    """
    Discover tables across all lakehouses for selected workspaces.

    Schema-enabled lakehouses that reject /tables are skipped automatically.
    """
    inventory: List[Dict[str, str]] = []

    for ws_id in workspace_ids:
        lh_resp = await client.get(
            f"/workspaces/{ws_id}/items",
            params={"type": "Lakehouse"},
        )
        lakehouses = _items(lh_resp)
        for lh in lakehouses:
            lh_id = str(lh.get("id", "")).strip()
            lh_name = str(lh.get("displayName", lh_id)).strip()
            if not lh_id:
                continue
            try:
                tbl_resp = await client.get_raw(f"/workspaces/{ws_id}/lakehouses/{lh_id}/tables")
            except Exception:
                # Typical for schema-enabled lakehouses: UnsupportedOperationForSchemasEnabledLakehouse
                continue
            if tbl_resp.status_code != 200:
                continue
            data = tbl_resp.json() if tbl_resp.content else {}
            tables = data.get("data", data.get("value", []))
            if not isinstance(tables, list):
                continue
            for t in tables:
                if not isinstance(t, dict):
                    continue
                table_name = str(t.get("name", "")).strip()
                if not table_name:
                    continue
                inventory.append(
                    {
                        "workspace_id": ws_id,
                        "lakehouse_id": lh_id,
                        "lakehouse_name": lh_name,
                        "table_name": table_name,
                    }
                )
    return inventory


def _build_due_scope(
    inventory: List[Dict[str, str]],
    last_success_index: Dict[Tuple[str, str, str], datetime],
    min_days_between_maintenance: int,
    include_never_maintained: bool,
) -> Tuple[Dict[str, List[str]], Dict[str, Any]]:
    """
    Select only tables that are due for maintenance.
    """
    now = _now_utc()
    cutoff = now - timedelta(days=max(0, min_days_between_maintenance))

    by_lakehouse: Dict[str, List[str]] = {}
    stats = {
        "selection_mode": "due-only",
        "inventory_tables": len(inventory),
        "due_tables": 0,
        "skipped_recently_maintained": 0,
        "skipped_never_maintained": 0,
        "cutoff_utc": cutoff.isoformat(),
        "min_days_between_maintenance": min_days_between_maintenance,
        "include_never_maintained": include_never_maintained,
    }

    for row in inventory:
        ws_id = row["workspace_id"]
        lh_id = row["lakehouse_id"]
        table_name = row["table_name"]
        key = (ws_id, lh_id, table_name.lower())
        last_success = last_success_index.get(key)

        include = False
        if last_success is None:
            include = include_never_maintained
            if not include:
                stats["skipped_never_maintained"] += 1
        else:
            include = last_success <= cutoff
            if not include:
                stats["skipped_recently_maintained"] += 1

        if include:
            by_lakehouse.setdefault(lh_id, []).append(table_name)
            stats["due_tables"] += 1

    return by_lakehouse, stats


async def _main_async(args: argparse.Namespace) -> int:
    load_dotenv(ROOT / ".env", override=True)
    auth = FabricAuthConfig.from_env()

    include_ids = _parse_csv(args.workspace_ids)
    include_names = _parse_csv(args.workspace_names)
    exclude_names = _parse_csv(args.exclude_workspace_names)
    table_filter = _parse_csv(args.table_filter) or None

    dry_run = not args.live

    async with FabricApiClient(auth) as client:
        if args.all_workspaces or include_ids or include_names or args.workspace_name_prefix:
            scope = await _discover_workspaces(
                client=client,
                include_ids=include_ids,
                include_names=include_names,
                exclude_names=exclude_names,
                workspace_name_prefix=args.workspace_name_prefix,
                max_workspaces=args.max_workspaces,
            )
        else:
            raise RuntimeError(
                "No scope selected. Use --all-workspaces, --workspace-ids, "
                "--workspace-names, or --workspace-name-prefix."
            )

        if not scope:
            raise RuntimeError("Workspace discovery returned 0 targets.")

        workspace_ids = [ws_id for ws_id, _ in scope]
        workspace_names = {ws_id: ws_name for ws_id, ws_name in scope}

        due_scope_by_lakehouse: Optional[Dict[str, List[str]]] = None
        selection_summary: Dict[str, Any] = {
            "selection_mode": args.selection_mode,
            "inventory_tables": None,
            "due_tables": None,
        }

        if not table_filter and args.selection_mode == "due-only":
            inventory = await _discover_table_inventory(client, workspace_ids)
            last_success_index = _load_last_success_index(args.history_lookback_runs)
            due_scope_by_lakehouse, selection_summary = _build_due_scope(
                inventory=inventory,
                last_success_index=last_success_index,
                min_days_between_maintenance=args.min_days_between_maintenance,
                include_never_maintained=args.include_never_maintained,
            )
        elif table_filter:
            selection_summary = {
                "selection_mode": "explicit-table-filter",
                "table_filter_count": len(table_filter),
            }
        else:
            selection_summary = {
                "selection_mode": "all",
                "table_filter_count": 0,
            }

        guard = MaintenanceGuard(
            client=client,
            queue_pressure_threshold=args.queue_pressure_threshold,
            job_poll_interval_secs=args.job_poll_interval_secs,
            job_timeout_secs=args.job_timeout_secs,
            dry_run=dry_run,
            max_concurrency=args.max_concurrency,
            auto_concurrency_by_capacity=(not args.disable_auto_capacity),
        )

        result = await guard.run_maintenance(
            workspace_ids,
            table_filter=table_filter,
            table_filter_by_lakehouse=due_scope_by_lakehouse,
        )

        output_payload = {
            "ts_utc": _iso_now(),
            "mode": "DRY_RUN" if dry_run else "LIVE",
            "scope": {
                "workspace_count": len(scope),
                "workspaces": [
                    {"workspace_id": ws_id, "workspace_name": workspace_names.get(ws_id, ws_id)}
                    for ws_id in workspace_ids
                ],
                "table_filter": table_filter,
                "lakehouse_scoped_filter_count": (
                    sum(len(v) for v in due_scope_by_lakehouse.values())
                    if due_scope_by_lakehouse is not None
                    else None
                ),
            },
            "selection": selection_summary,
            "capacity_profile": {
                "auto_concurrency_by_capacity": (not args.disable_auto_capacity),
                "max_concurrency_override": args.max_concurrency,
                "resolved_workspace_concurrency": dict(guard._workspace_concurrency_cache),
                "resolved_workspace_capacity_hints": dict(guard._workspace_capacity_hint_cache),
            },
            "summary": {
                "run_id": result.run_id,
                "workspace_ids": result.workspace_ids,
                "dry_run": result.dry_run,
                "total_tables": result.total_tables,
                "validated": result.validated,
                "rejected": result.rejected,
                "submitted": result.submitted,
                "succeeded": result.succeeded,
                "failed": result.failed,
                "skipped_queue": result.skipped_queue,
                "errors": result.errors,
                "message": (
                    f"[{'DRY_RUN' if dry_run else 'LIVE'}] "
                    f"tables={result.total_tables}, validated={result.validated}, "
                    f"rejected={result.rejected}, submitted={result.submitted}, "
                    f"succeeded={result.succeeded}, failed={result.failed}, "
                    f"skipped_queue={result.skipped_queue}"
                ),
            },
            "job_records": [asdict(r) for r in result.job_records],
        }

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        out_file = OUTPUT_DIR / f"delta_maintenance_{_now_utc().strftime('%Y%m%dT%H%M%SZ')}.json"
        out_file.write_text(json.dumps(output_payload, indent=2, default=str), encoding="utf-8")

        print(output_payload["summary"]["message"])
        print(f"[report] {out_file}")

        if args.fail_on_job_failure and (result.failed > 0 or len(result.errors) > 0):
            return 1
        return 0


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="FabricOps one-click Delta maintenance runner.")

    scope = p.add_argument_group("Scope")
    scope.add_argument(
        "--all-workspaces",
        action="store_true",
        help="Auto-discover all accessible workspaces.",
    )
    scope.add_argument(
        "--workspace-ids",
        type=str,
        default="",
        help="Comma-separated workspace IDs.",
    )
    scope.add_argument(
        "--workspace-names",
        type=str,
        default="",
        help="Comma-separated workspace display names.",
    )
    scope.add_argument(
        "--workspace-name-prefix",
        type=str,
        default="",
        help="Optional workspace name prefix filter (e.g., ENT_).",
    )
    scope.add_argument(
        "--exclude-workspace-names",
        type=str,
        default="",
        help="Comma-separated workspace names to exclude.",
    )
    scope.add_argument(
        "--max-workspaces",
        type=int,
        default=None,
        help="Optional cap on discovered workspaces.",
    )

    run = p.add_argument_group("Run mode")
    run.add_argument(
        "--live",
        action="store_true",
        help="Submit real jobs. Default is dry-run.",
    )
    run.add_argument(
        "--table-filter",
        type=str,
        default="",
        help=(
            "Comma-separated table names to process. "
            "When set, this overrides auto-selection policy."
        ),
    )
    run.add_argument(
        "--selection-mode",
        choices=["all", "due-only"],
        default="due-only",
        help=(
            "Table selection policy when --table-filter is not provided. "
            "'due-only' selects only tables that are due by maintenance history."
        ),
    )
    run.add_argument(
        "--min-days-between-maintenance",
        type=int,
        default=7,
        help="For due-only mode: minimum days between successful maintenance runs per table.",
    )
    run.add_argument(
        "--include-never-maintained",
        action="store_true",
        default=True,
        help="For due-only mode: include tables with no prior successful maintenance record.",
    )
    run.add_argument(
        "--exclude-never-maintained",
        dest="include_never_maintained",
        action="store_false",
        help="For due-only mode: exclude tables with no prior successful maintenance record.",
    )
    run.add_argument(
        "--history-lookback-runs",
        type=int,
        default=120,
        help="How many previous maintenance reports to scan for last-success history.",
    )
    run.add_argument(
        "--queue-pressure-threshold",
        type=int,
        default=3,
        help="Skip submission when active jobs >= threshold.",
    )
    run.add_argument(
        "--job-poll-interval-secs",
        type=int,
        default=30,
        help="Polling interval for submitted jobs.",
    )
    run.add_argument(
        "--job-timeout-secs",
        type=int,
        default=900,
        help="Per-job timeout in seconds.",
    )
    run.add_argument(
        "--disable-auto-capacity",
        action="store_true",
        help="Disable capacity-based concurrency tuning (not recommended).",
    )
    run.add_argument(
        "--max-concurrency",
        type=int,
        default=None,
        help="Optional hard cap on concurrency after capacity profile is resolved.",
    )
    run.add_argument(
        "--fail-on-job-failure",
        action="store_true",
        default=True,
        help="Exit with non-zero code when live run has failed jobs/errors.",
    )
    run.add_argument(
        "--no-fail-on-job-failure",
        dest="fail_on_job_failure",
        action="store_false",
        help="Always exit zero, even when live run has failed jobs/errors.",
    )

    return p


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    rc = asyncio.run(_main_async(args))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
