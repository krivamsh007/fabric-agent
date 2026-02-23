#!/usr/bin/env python
"""
Trigger notebook-backed Fabric pipelines via REST API (no Fabric UI required).

Primary use:
- Activate day1/day2 schema-drift scenario CSVs from local repo to OneLake seed path
- Trigger DataPlatform + consumer pipelines
- Optionally wait/poll pipeline job instances to terminal state

Examples:
  python scripts/trigger_notebook_pipelines_api.py --scenario baseline
  python scripts/trigger_notebook_pipelines_api.py --scenario nonbreaking
  python scripts/trigger_notebook_pipelines_api.py --scenario breaking --no-wait
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from fabric_agent.api.fabric_client import FabricApiClient
from fabric_agent.core.config import FabricAuthConfig


ROOT = Path(__file__).resolve().parents[1]
SCENARIO_DIR = ROOT / "data" / "seed_data" / "scenarios" / "bronze"
OUTPUT_DIR = ROOT / "data" / "pipeline_runs"

SCENARIO_FILE_MAP = {
    "baseline": "raw_sales_transactions_day1_baseline.csv",
    "nonbreaking": "raw_sales_transactions_day2_nonbreaking_add_column.csv",
    "breaking": "raw_sales_transactions_day2_breaking_removed_column.csv",
}


@dataclass
class PipelineRunResult:
    workspace_name: str
    workspace_id: str
    pipeline_name: str
    pipeline_id: str
    triggered: bool
    instance_id: Optional[str]
    status: str
    failure_reason: Optional[str]
    started_utc: str
    ended_utc: Optional[str]
    duration_seconds: Optional[float]
    timed_out: bool = False
    error: Optional[str] = None


def _items(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    if isinstance(data.get("value"), list):
        return data["value"]
    if isinstance(data.get("data"), list):
        return data["data"]
    return []


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _now_utc().isoformat()


def _get_storage_credential():
    from azure.identity import ClientSecretCredential, DefaultAzureCredential

    use_interactive = os.getenv("USE_INTERACTIVE_AUTH", "false").strip().lower() in ("1", "true", "yes")
    if use_interactive:
        return DefaultAzureCredential(exclude_interactive_browser_credential=False)

    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    if not all([tenant_id, client_id, client_secret]):
        raise RuntimeError(
            "Missing AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET for OneLake upload."
        )
    return ClientSecretCredential(tenant_id, client_id, client_secret)


def _upload_seed_csv_to_onelake(
    workspace_id: str,
    lakehouse_id: str,
    local_csv: Path,
    target_rel_path: str = "Files/seed/bronze/raw_sales_transactions.csv",
) -> str:
    from azure.storage.filedatalake import DataLakeServiceClient

    if not local_csv.exists():
        raise FileNotFoundError(f"Scenario file not found: {local_csv}")

    cred = _get_storage_credential()
    svc = DataLakeServiceClient(
        account_url="https://onelake.dfs.fabric.microsoft.com",
        credential=cred,
    )

    fs = svc.get_file_system_client(file_system=workspace_id)
    full_path = f"{lakehouse_id}/{target_rel_path}"
    file_client = fs.get_file_client(full_path)

    with open(local_csv, "rb") as f:
        file_client.upload_data(f.read(), overwrite=True)

    return full_path


async def _resolve_workspace_id(client: FabricApiClient, workspace_name: str) -> str:
    resp = await client.get("/workspaces")
    for ws in _items(resp):
        if (ws.get("displayName") or "").lower() == workspace_name.lower():
            return ws.get("id")
    raise RuntimeError(f"Workspace not found: {workspace_name}")


async def _resolve_lakehouse_id(client: FabricApiClient, workspace_id: str, lakehouse_name: str) -> str:
    resp = await client.get(f"/workspaces/{workspace_id}/lakehouses")
    for lh in _items(resp):
        if (lh.get("displayName") or "").lower() == lakehouse_name.lower():
            return lh.get("id")
    raise RuntimeError(f"Lakehouse not found: {lakehouse_name} in workspace {workspace_id}")


async def _resolve_pipeline_id(client: FabricApiClient, workspace_id: str, pipeline_name: str) -> str:
    resp = await client.get(f"/workspaces/{workspace_id}/dataPipelines")
    for pl in _items(resp):
        if (pl.get("displayName") or "").lower() == pipeline_name.lower():
            return pl.get("id")
    raise RuntimeError(f"Pipeline not found: {pipeline_name} in workspace {workspace_id}")


async def _trigger_pipeline_instance(
    client: FabricApiClient,
    workspace_id: str,
    pipeline_id: str,
    pipeline_name: str,
) -> str:
    raw = await client.post_raw(
        f"/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances",
        params={"jobType": "Pipeline"},
        json_data={"executionData": {"pipelineName": pipeline_name}},
    )
    if raw.status_code not in (200, 202):
        raise RuntimeError(
            f"Pipeline trigger failed ({raw.status_code}): {raw.text[:300]}"
        )

    loc = raw.headers.get("location") or raw.headers.get("Location")
    if loc:
        return loc.rstrip("/").split("/")[-1]

    if raw.content:
        try:
            body = raw.json()
            if isinstance(body, dict) and body.get("id"):
                return str(body["id"])
        except Exception:
            pass

    # Fallback: query latest instance
    latest = await client.get(
        f"/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances",
        params={"jobType": "Pipeline", "$top": 1},
    )
    runs = _items(latest)
    if not runs:
        raise RuntimeError("Triggered pipeline but could not resolve instance id.")
    return str(runs[0].get("id"))


async def _get_pipeline_instance(
    client: FabricApiClient,
    workspace_id: str,
    pipeline_id: str,
    instance_id: str,
) -> Dict[str, Any]:
    return await client.get(
        f"/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{instance_id}",
        params={"jobType": "Pipeline"},
    )


async def _run_pipeline(
    client: FabricApiClient,
    workspace_name: str,
    workspace_id: str,
    pipeline_name: str,
    wait: bool,
    timeout_minutes: int,
    poll_seconds: int,
) -> PipelineRunResult:
    start_ts = _now_utc()
    try:
        pipeline_id = await _resolve_pipeline_id(client, workspace_id, pipeline_name)
        instance_id = await _trigger_pipeline_instance(client, workspace_id, pipeline_id, pipeline_name)

        if not wait:
            return PipelineRunResult(
                workspace_name=workspace_name,
                workspace_id=workspace_id,
                pipeline_name=pipeline_name,
                pipeline_id=pipeline_id,
                triggered=True,
                instance_id=instance_id,
                status="triggered",
                failure_reason=None,
                started_utc=start_ts.isoformat(),
                ended_utc=None,
                duration_seconds=None,
            )

        deadline = _now_utc().timestamp() + (timeout_minutes * 60)
        last_status = "unknown"
        last_failure = None

        while _now_utc().timestamp() < deadline:
            run = await _get_pipeline_instance(client, workspace_id, pipeline_id, instance_id)
            last_status = str(run.get("status", "unknown"))
            last_failure = run.get("failureReason")
            normalized = last_status.lower()

            if normalized in ("completed", "succeeded", "success"):
                end_ts = _now_utc()
                return PipelineRunResult(
                    workspace_name=workspace_name,
                    workspace_id=workspace_id,
                    pipeline_name=pipeline_name,
                    pipeline_id=pipeline_id,
                    triggered=True,
                    instance_id=instance_id,
                    status=last_status,
                    failure_reason=None,
                    started_utc=start_ts.isoformat(),
                    ended_utc=end_ts.isoformat(),
                    duration_seconds=(end_ts - start_ts).total_seconds(),
                )

            if normalized in ("failed", "cancelled", "canceled", "error"):
                end_ts = _now_utc()
                return PipelineRunResult(
                    workspace_name=workspace_name,
                    workspace_id=workspace_id,
                    pipeline_name=pipeline_name,
                    pipeline_id=pipeline_id,
                    triggered=True,
                    instance_id=instance_id,
                    status=last_status,
                    failure_reason=str(last_failure) if last_failure is not None else None,
                    started_utc=start_ts.isoformat(),
                    ended_utc=end_ts.isoformat(),
                    duration_seconds=(end_ts - start_ts).total_seconds(),
                    error="Pipeline run ended in failure state.",
                )

            await asyncio.sleep(poll_seconds)

        end_ts = _now_utc()
        return PipelineRunResult(
            workspace_name=workspace_name,
            workspace_id=workspace_id,
            pipeline_name=pipeline_name,
            pipeline_id=pipeline_id,
            triggered=True,
            instance_id=instance_id,
            status=last_status,
            failure_reason=str(last_failure) if last_failure is not None else None,
            started_utc=start_ts.isoformat(),
            ended_utc=end_ts.isoformat(),
            duration_seconds=(end_ts - start_ts).total_seconds(),
            timed_out=True,
            error=f"Timeout waiting for pipeline run after {timeout_minutes} minute(s).",
        )

    except Exception as exc:
        message = str(exc)
        end_ts = _now_utc()
        return PipelineRunResult(
            workspace_name=workspace_name,
            workspace_id=workspace_id,
            pipeline_name=pipeline_name,
            pipeline_id="",
            triggered=False,
            instance_id=None,
            status="error",
            failure_reason=None,
            started_utc=start_ts.isoformat(),
            ended_utc=end_ts.isoformat(),
            duration_seconds=(end_ts - start_ts).total_seconds(),
            error=message,
        )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Trigger notebook pipelines via Fabric API.")
    p.add_argument(
        "--scenario",
        choices=["none", "baseline", "nonbreaking", "breaking"],
        default="none",
        help="Optional schema-drift scenario to activate before triggering pipelines.",
    )
    p.add_argument(
        "--workspace-data-platform",
        default="ENT_DataPlatform_DEV",
        help="Workspace containing Bronze/Silver/Gold pipelines/notebooks.",
    )
    p.add_argument(
        "--workspace-sales",
        default="ENT_SalesAnalytics_DEV",
        help="Sales consumer workspace name.",
    )
    p.add_argument(
        "--workspace-finance",
        default="ENT_Finance_DEV",
        help="Finance consumer workspace name.",
    )
    p.add_argument("--pipeline-data-platform", default="PL_Daily_ETL")
    p.add_argument("--pipeline-sales", default="PL_SalesAnalytics_Consumer_Refresh")
    p.add_argument("--pipeline-finance", default="PL_Finance_Consumer_Refresh")
    p.add_argument(
        "--skip-consumers",
        action="store_true",
        help="Run only DataPlatform pipeline (not recommended for full readiness validation).",
    )
    p.add_argument(
        "--allow-skip-critical",
        action="store_true",
        help="Allow skipping critical consumer pipeline checks.",
    )
    p.add_argument(
        "--no-wait",
        action="store_true",
        help="Trigger runs and exit without waiting for completion.",
    )
    p.add_argument("--timeout-minutes", type=int, default=45)
    p.add_argument("--poll-seconds", type=int, default=20)
    return p.parse_args()


async def _main_async(args: argparse.Namespace) -> int:
    load_dotenv(ROOT / ".env", override=True)

    config = FabricAuthConfig.from_env()
    run_results: List[PipelineRunResult] = []
    scenario_upload: Dict[str, Any] = {}

    async with FabricApiClient(config) as client:
        ws_dp_id = await _resolve_workspace_id(client, args.workspace_data_platform)

        if args.skip_consumers and not args.allow_skip_critical:
            raise RuntimeError(
                "Critical consumer pipeline checks cannot be skipped for full validation. "
                "Remove --skip-consumers, or pass --allow-skip-critical for non-critical runs."
            )

        # Optional scenario activation (replaces bronze seed file used by 01_Ingest_Bronze).
        if args.scenario != "none":
            scenario_file = SCENARIO_DIR / SCENARIO_FILE_MAP[args.scenario]
            bronze_lh_id = await _resolve_lakehouse_id(client, ws_dp_id, "Bronze_Landing")
            onelake_path = _upload_seed_csv_to_onelake(
                workspace_id=ws_dp_id,
                lakehouse_id=bronze_lh_id,
                local_csv=scenario_file,
                target_rel_path="Files/seed/bronze/raw_sales_transactions.csv",
            )
            scenario_upload = {
                "scenario": args.scenario,
                "local_file": str(scenario_file),
                "onelake_target": onelake_path,
                "activated_utc": _iso_now(),
            }
            print(f"[scenario] Activated '{args.scenario}' -> {onelake_path}")

        # Primary pipeline (runs notebook chain for DataPlatform).
        print(f"[run] Triggering {args.workspace_data_platform}/{args.pipeline_data_platform}")
        run_results.append(
            await _run_pipeline(
                client=client,
                workspace_name=args.workspace_data_platform,
                workspace_id=ws_dp_id,
                pipeline_name=args.pipeline_data_platform,
                wait=not args.no_wait,
                timeout_minutes=args.timeout_minutes,
                poll_seconds=args.poll_seconds,
            )
        )

        # Optional consumer pipelines.
        if not args.skip_consumers:
            ws_sales_id = await _resolve_workspace_id(client, args.workspace_sales)
            ws_fin_id = await _resolve_workspace_id(client, args.workspace_finance)

            print(f"[run] Triggering {args.workspace_sales}/{args.pipeline_sales}")
            run_results.append(
                await _run_pipeline(
                    client=client,
                    workspace_name=args.workspace_sales,
                    workspace_id=ws_sales_id,
                    pipeline_name=args.pipeline_sales,
                    wait=not args.no_wait,
                    timeout_minutes=args.timeout_minutes,
                    poll_seconds=args.poll_seconds,
                )
            )

            print(f"[run] Triggering {args.workspace_finance}/{args.pipeline_finance}")
            run_results.append(
                await _run_pipeline(
                    client=client,
                    workspace_name=args.workspace_finance,
                    workspace_id=ws_fin_id,
                    pipeline_name=args.pipeline_finance,
                    wait=not args.no_wait,
                    timeout_minutes=args.timeout_minutes,
                    poll_seconds=args.poll_seconds,
                )
            )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        "ts_utc": _iso_now(),
        "scenario_upload": scenario_upload or None,
        "wait_mode": not args.no_wait,
        "results": [asdict(r) for r in run_results],
    }
    out_file = OUTPUT_DIR / f"pipeline_run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    out_file.write_text(json.dumps(out, indent=2), encoding="utf-8")

    ok = all(r.error is None for r in run_results)
    for r in run_results:
        flag = "PASS" if r.error is None else "FAIL"
        print(
            f"[{flag}] {r.workspace_name}/{r.pipeline_name} "
            f"instance={r.instance_id} status={r.status} "
            + (f"error={r.error}" if r.error else "")
        )
    print(f"[report] {out_file}")

    return 0 if ok else 1


def main() -> None:
    args = _parse_args()
    rc = asyncio.run(_main_async(args))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
