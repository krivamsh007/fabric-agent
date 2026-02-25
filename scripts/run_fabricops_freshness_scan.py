#!/usr/bin/env python
"""
FabricOps one-click SQL Endpoint freshness scanner.

What this script does:
1) Auto-discovers workspaces (or uses an explicit scope)
2) Runs FreshnessGuard across the scope
3) Compares each table's lastSuccessfulSyncDateTime against SLA thresholds
4) Writes a run report to data/freshness_scans/

Examples:
  # Scan all visible workspaces
  python scripts/run_fabricops_freshness_scan.py --all-workspaces

  # Scan one workspace with custom SLA
  python scripts/run_fabricops_freshness_scan.py --workspace-ids <ws-guid> --sla-fact 0.5

  # Scan all ENT_* workspaces, fail CI if violations found
  python scripts/run_fabricops_freshness_scan.py --all-workspaces --workspace-name-prefix ENT_ --fail-on-violation
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

from fabric_agent.api.fabric_client import FabricApiClient
from fabric_agent.core.config import FabricAuthConfig
from fabric_agent.guards.freshness_guard import FreshnessGuard


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "data" / "freshness_scans"


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


async def _main_async(args: argparse.Namespace) -> int:
    load_dotenv(ROOT / ".env", override=True)
    auth = FabricAuthConfig.from_env()

    include_ids = _parse_csv(args.workspace_ids)
    include_names = _parse_csv(args.workspace_names)
    exclude_names = _parse_csv(args.exclude_workspace_names)

    sla_thresholds: Dict[str, float] = {
        "fact_*": args.sla_fact,
        "dim_*": args.sla_dim,
        "raw_*": args.sla_raw,
        "*": args.sla_default,
    }

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

        guard = FreshnessGuard(
            client=client,
            sla_thresholds=sla_thresholds,
            lro_timeout_secs=args.lro_timeout_secs,
        )

        result = await guard.scan(workspace_ids)

        # Build violation details for output
        violation_details = []
        for v in result.violations:
            ts = v.table_status
            detail: Dict[str, Any] = {
                "violation_id": v.violation_id,
                "workspace_id": v.workspace_id,
                "workspace_name": v.workspace_name,
                "sql_endpoint_id": v.sql_endpoint_id,
                "sql_endpoint_name": v.sql_endpoint_name,
                "detected_at": v.detected_at,
            }
            if ts:
                detail["table"] = ts.to_dict()
            violation_details.append(detail)

        output_payload = {
            "ts_utc": _iso_now(),
            "scope": {
                "workspace_count": len(scope),
                "workspaces": [
                    {"workspace_id": ws_id, "workspace_name": workspace_names.get(ws_id, ws_id)}
                    for ws_id in workspace_ids
                ],
            },
            "sla_thresholds": sla_thresholds,
            "summary": {
                "scan_id": result.scan_id,
                "workspace_ids": result.workspace_ids,
                "scanned_at": result.scanned_at,
                "total_tables": result.total_tables,
                "healthy_count": result.healthy_count,
                "violation_count": result.violation_count,
                "scan_duration_ms": result.scan_duration_ms,
                "errors": result.errors,
                "message": (
                    f"tables={result.total_tables}, healthy={result.healthy_count}, "
                    f"violations={result.violation_count}, "
                    f"duration={result.scan_duration_ms}ms"
                ),
            },
            "violations": violation_details,
        }

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        out_file = OUTPUT_DIR / f"freshness_scan_{_now_utc().strftime('%Y%m%dT%H%M%SZ')}.json"
        out_file.write_text(json.dumps(output_payload, indent=2, default=str), encoding="utf-8")

        print(output_payload["summary"]["message"])
        print(f"[report] {out_file}")

        if result.violation_count > 0:
            # Print a quick summary of violations to stdout
            for v in result.violations:
                ts = v.table_status
                if ts:
                    hours = f"{ts.hours_since_sync:.1f}h" if ts.hours_since_sync else "N/A"
                    print(
                        f"  VIOLATION: {v.workspace_name} / {ts.table_name} "
                        f"— {ts.freshness_status.value} ({hours} stale, SLA={ts.sla_threshold_hours}h)"
                    )

        if args.fail_on_violation and result.violation_count > 0:
            return 1
        return 0


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="FabricOps one-click SQL Endpoint freshness scanner.")

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

    sla = p.add_argument_group("SLA thresholds (hours)")
    sla.add_argument(
        "--sla-fact",
        type=float,
        default=1.0,
        help="SLA for fact_* tables (default: 1.0h).",
    )
    sla.add_argument(
        "--sla-dim",
        type=float,
        default=24.0,
        help="SLA for dim_* tables (default: 24.0h).",
    )
    sla.add_argument(
        "--sla-raw",
        type=float,
        default=6.0,
        help="SLA for raw_* tables (default: 6.0h).",
    )
    sla.add_argument(
        "--sla-default",
        type=float,
        default=24.0,
        help="SLA for unmatched tables (default: 24.0h).",
    )

    run = p.add_argument_group("Run options")
    run.add_argument(
        "--lro-timeout-secs",
        type=int,
        default=300,
        help="Max seconds to wait for refreshMetadata LRO per endpoint (default: 300).",
    )
    run.add_argument(
        "--fail-on-violation",
        action="store_true",
        default=False,
        help="Exit with non-zero code when violations are found (for CI gates).",
    )

    return p


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    rc = asyncio.run(_main_async(args))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
