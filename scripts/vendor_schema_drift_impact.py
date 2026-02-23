#!/usr/bin/env python3
"""
Vendor file schema drift impact workflow.

Detects day1/day2 schema drift and combines it with downstream impact analysis.
Designed for existing workspace usage (default: ENT_DataPlatform_DEV).

Examples:
  python scripts/vendor_schema_drift_impact.py
  python scripts/vendor_schema_drift_impact.py --workspace ENT_DataPlatform_DEV --lakehouse Silver_Curated --table fact_sales
  python scripts/vendor_schema_drift_impact.py --email-to "dataops@company.com,bi@company.com"
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fabric_agent.api.fabric_client import FabricApiClient
from fabric_agent.core.config import FabricAuthConfig
from fabric_agent.integrations.email import EmailNotifier
from fabric_agent.schema.contracts import ColumnContract, DataContract
from fabric_agent.schema.drift import detect_drift
from fabric_agent.tools.workspace_graph import (
    GraphImpactAnalyzer,
    MultiWorkspaceGraphBuilder,
    WorkspaceGraphBuilder,
)
from scripts.analyze_data_impact import analyze as analyze_cross_workspace


def _load_schema(path: str) -> List[Dict[str, Any]]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"Schema file must be a JSON list: {path}")
    rows: List[Dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict) or "name" not in item:
            raise ValueError(f"Each schema row must be an object with 'name': {path}")
        rows.append(item)
    return rows


def _build_contract_from_schema(
    table_name: str,
    owner: str,
    day1_schema: List[Dict[str, Any]],
) -> DataContract:
    cols = [
        ColumnContract(
            name=str(c["name"]),
            type=str(c.get("type", "string")),
            nullable=bool(c.get("nullable", True)),
        )
        for c in day1_schema
    ]
    return DataContract(name=table_name, version=1, owner=owner, columns=cols)


def _risk_from_drift_and_impact(*, breaking: bool, total_downstream_assets: int) -> str:
    if breaking:
        if total_downstream_assets >= 20:
            return "critical"
        if total_downstream_assets >= 8:
            return "high_risk"
        return "medium_risk"
    if total_downstream_assets >= 15:
        return "high_risk"
    if total_downstream_assets >= 6:
        return "medium_risk"
    if total_downstream_assets >= 1:
        return "low_risk"
    return "safe"


def _required_changes(finding: Dict[str, Any]) -> List[str]:
    changes: List[str] = []
    added = finding.get("added", []) or []
    removed = finding.get("removed", []) or []
    type_changed = finding.get("type_changed", []) or []
    nullability_changed = finding.get("nullability_changed", []) or []

    if added:
        added_cols = ", ".join(str(c.get("name")) for c in added)
        changes.append(
            "Update ingestion mapping to include new columns and add defaults/null-safe transforms: "
            f"{added_cols}."
        )
    if type_changed:
        type_lines = ", ".join(
            f"{c.get('name')} ({c.get('from')} -> {c.get('to')})"
            for c in type_changed
        )
        changes.append(
            "Add explicit CAST/TRY_CAST logic in pipeline and validate downstream model compatibility: "
            f"{type_lines}."
        )
    if removed:
        removed_cols = ", ".join(str(c.get("name")) for c in removed)
        changes.append(
            "Create compatibility projection (or versioned table) for removed columns before switching consumers: "
            f"{removed_cols}."
        )
    if nullability_changed:
        null_lines = ", ".join(
            f"{c.get('name')} ({c.get('from')} -> {c.get('to')})"
            for c in nullability_changed
        )
        changes.append(
            "Review null handling in pipeline transformations and semantic model measures: "
            f"{null_lines}."
        )
    if not changes:
        changes.append("No schema changes detected. Keep current pipeline as-is and continue monitoring.")
    return changes


async def _resolve_workspace_id(client: FabricApiClient, workspace_name: str) -> str:
    resp = await client.get("/workspaces")
    for ws in resp.get("value", []):
        if (ws.get("displayName") or "").lower() == workspace_name.lower():
            return str(ws.get("id"))
    raise ValueError(f"Workspace not found: {workspace_name}")


def _resolve_table_node(
    graph: Dict[str, Any],
    *,
    workspace_name: Optional[str],
    lakehouse_name: str,
    table_name: str,
) -> Optional[Dict[str, Any]]:
    nodes = graph.get("nodes", [])
    workspace_id = None
    if workspace_name:
        workspace = next(
            (
                n
                for n in nodes
                if n.get("type") == "workspace"
                and (n.get("name") or "").lower() == workspace_name.lower()
            ),
            None,
        )
        workspace_id = workspace.get("id") if workspace else None

    lakehouse = next(
        (
            n
            for n in nodes
            if n.get("type") == "lakehouse"
            and n.get("name") == lakehouse_name
            and (not workspace_id or n.get("parent_id") == workspace_id)
        ),
        None,
    )
    if lakehouse:
        match = next(
            (
                n
                for n in nodes
                if n.get("type") == "table"
                and n.get("name") == table_name
                and n.get("parent_id") == lakehouse.get("id")
            ),
            None,
        )
        if match:
            return match
    return next(
        (
            n
            for n in nodes
            if n.get("type") == "table"
            and n.get("name") == table_name
        ),
        None,
    )


def _to_markdown(payload: Dict[str, Any]) -> str:
    source = payload["source"]
    drift = payload["drift"]
    impact = payload["impact"]
    summary = impact["summary"]
    lines: List[str] = []

    lines.append("# Vendor Schema Drift Impact\n")
    lines.append(
        f"**Source**: `{source['workspace']}/{source['lakehouse']}.{source['table']}`\n\n"
    )
    lines.append("## Drift Summary\n")
    lines.append(f"- Breaking: **{drift['breaking']}**\n")
    lines.append(f"- Added columns: **{len(drift['added'])}**\n")
    lines.append(f"- Removed columns: **{len(drift['removed'])}**\n")
    lines.append(f"- Type changes: **{len(drift['type_changed'])}**\n")
    lines.append(f"- Nullability changes: **{len(drift['nullability_changed'])}**\n")

    lines.append("\n## Required Changes\n")
    for ch in payload["required_changes"]:
        lines.append(f"- {ch}\n")

    lines.append("\n## Downstream Impact\n")
    lines.append(f"- Risk level: **{summary['risk_level']}**\n")
    lines.append(f"- Total downstream assets: **{summary['total_downstream_assets']}**\n")
    lines.append(f"- Consumer workspaces impacted: **{summary['consumer_workspaces_impacted']}**\n")
    lines.append(f"- Shortcuts impacted: **{summary['shortcuts_impacted']}**\n")
    lines.append(f"- Source workspace notebooks impacted: **{summary['source_notebooks_impacted']}**\n")
    lines.append(f"- Source workspace pipelines impacted: **{summary['source_pipelines_impacted']}**\n")
    lines.append(f"- Cross-workspace notebooks impacted: **{summary['cross_notebooks_impacted']}**\n")
    lines.append(f"- Cross-workspace pipelines impacted: **{summary['cross_pipelines_impacted']}**\n")
    lines.append(f"- Cross-workspace semantic models impacted: **{summary['cross_models_impacted']}**\n")
    lines.append(f"- Cross-workspace reports potentially impacted: **{summary['cross_reports_impacted']}**\n")

    email_info = payload.get("email_notification")
    if email_info:
        lines.append("\n## Email Notification\n")
        lines.append(f"- Sent: **{email_info.get('sent')}**\n")
        lines.append(f"- Recipients: `{', '.join(email_info.get('recipients', []))}`\n")

    return "".join(lines)


async def run(args: argparse.Namespace) -> Dict[str, Any]:
    observed_day2 = _load_schema(args.day2_schema)

    if args.contract:
        contract = DataContract.load(args.contract)
    else:
        observed_day1 = _load_schema(args.day1_schema)
        contract = _build_contract_from_schema(args.table, args.owner, observed_day1)

    finding = detect_drift(contract, observed_day2)
    finding_dict = {
        "summary": finding.summary,
        "breaking": finding.breaking,
        "added": finding.added,
        "removed": finding.removed,
        "type_changed": finding.type_changed,
        "nullability_changed": finding.nullability_changed,
    }

    cfg = FabricAuthConfig.from_env()
    async with FabricApiClient(cfg) as client:
        workspace_id = await _resolve_workspace_id(client, args.workspace)
        # Build a multi-workspace graph so table impact can traverse
        # producer -> shortcut -> consumer workspace dependencies.
        prefixes = [args.prefix] if args.prefix else None
        multi_builder = MultiWorkspaceGraphBuilder(include_measure_graph=False)
        graph = await multi_builder.build(
            client,
            workspace_names=[args.workspace],
            workspace_prefixes=prefixes,
            workspace_ids=[workspace_id],
        )
        # Backward-compatible fallback if the merged build returns unexpectedly empty.
        if not graph.get("nodes"):
            builder = WorkspaceGraphBuilder(include_measure_graph=False)
            graph = await builder.build(client, workspace_id, args.workspace)
        analyzer = GraphImpactAnalyzer(graph)

        table_node = _resolve_table_node(
            graph,
            workspace_name=args.workspace,
            lakehouse_name=args.lakehouse,
            table_name=args.table,
        )
        if table_node:
            source_impact = analyzer.analyze_table_change_impact(table_node["id"])
        else:
            source_impact = {
                "table_id": None,
                "affected_shortcuts": [],
                "affected_models": [],
                "affected_notebooks": [],
                "affected_pipelines": [],
                "affected_reports": [],
                "total_impact": 0,
                "risk_level": "unknown",
                "warning": (
                    f"Table '{args.table}' not found in graph for lakehouse '{args.lakehouse}'. "
                    "Seed/sync table metadata, then rerun."
                ),
            }

        cross_impact = await analyze_cross_workspace(
            client,
            prefix=args.prefix,
            src_ws_name=args.workspace,
            src_lh_name=args.lakehouse,
            src_table=args.table,
        )

    downstream_scans = cross_impact.get("downstream_best_effort", []) or []
    cross_notebooks = sum(len(x.get("notebooks_hit", []) or []) for x in downstream_scans)
    cross_pipelines = sum(len(x.get("pipelines_hit", []) or []) for x in downstream_scans)
    cross_models = sum(len(x.get("models_hit", []) or []) for x in downstream_scans)
    cross_reports = sum(len(x.get("reports_potential", []) or []) for x in downstream_scans)

    cross_summary = cross_impact.get("summary", {})
    shortcuts_impacted = int(cross_summary.get("shortcuts_impacted", 0))
    consumer_workspaces_impacted = int(cross_summary.get("consumer_workspaces_impacted", 0))

    source_notebooks = len(source_impact.get("affected_notebooks", []) or [])
    source_pipelines = len(source_impact.get("affected_pipelines", []) or [])
    source_models = len(source_impact.get("affected_models", []) or [])
    source_reports = len(source_impact.get("affected_reports", []) or [])

    total_downstream_assets = (
        shortcuts_impacted
        + source_notebooks
        + source_pipelines
        + source_models
        + source_reports
        + cross_notebooks
        + cross_pipelines
        + cross_models
        + cross_reports
    )

    summary = {
        "risk_level": _risk_from_drift_and_impact(
            breaking=finding.breaking,
            total_downstream_assets=total_downstream_assets,
        ),
        "total_downstream_assets": total_downstream_assets,
        "consumer_workspaces_impacted": consumer_workspaces_impacted,
        "shortcuts_impacted": shortcuts_impacted,
        "source_notebooks_impacted": source_notebooks,
        "source_pipelines_impacted": source_pipelines,
        "source_models_impacted": source_models,
        "source_reports_impacted": source_reports,
        "cross_notebooks_impacted": cross_notebooks,
        "cross_pipelines_impacted": cross_pipelines,
        "cross_models_impacted": cross_models,
        "cross_reports_impacted": cross_reports,
    }

    payload: Dict[str, Any] = {
        "source": {
            "workspace": args.workspace,
            "lakehouse": args.lakehouse,
            "table": args.table,
        },
        "drift": finding_dict,
        "required_changes": _required_changes(finding_dict),
        "impact": {
            "summary": summary,
            "source_workspace": source_impact,
            "cross_workspace": cross_impact,
        },
    }

    if args.email_to:
        recipients = [x.strip() for x in args.email_to.split(",") if x.strip()]
        notifier = EmailNotifier(
            smtp_host=args.smtp_host,
            smtp_port=args.smtp_port,
            username=args.smtp_user,
            password=args.smtp_password,
            from_address=args.smtp_from,
            use_tls=(not args.no_tls),
        )
        sent = notifier.notify_schema_drift_impact(recipients, payload)
        payload["email_notification"] = {"sent": sent, "recipients": recipients}

    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Detect vendor schema drift and estimate downstream impact in Fabric."
    )
    parser.add_argument("--workspace", default="ENT_DataPlatform_DEV", help="Source workspace name")
    parser.add_argument("--lakehouse", default="Silver_Curated", help="Source lakehouse name")
    parser.add_argument("--table", default="fact_sales", help="Source table name")
    parser.add_argument("--prefix", default="ENT", help="Enterprise workspace prefix")

    parser.add_argument("--contract", help="Existing contract YAML path (optional)")
    parser.add_argument(
        "--day1-schema",
        default="examples/vendor_day1_fact_sales_schema.json",
        help="Day1 schema JSON (used when --contract is not provided)",
    )
    parser.add_argument(
        "--day2-schema",
        default="examples/vendor_day2_fact_sales_schema.json",
        help="Day2 schema JSON with vendor changes",
    )
    parser.add_argument(
        "--owner",
        default="data-platform@example.com",
        help="Owner used when constructing a Day1-based temporary contract",
    )

    parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    parser.add_argument("--output", help="Optional output file path")

    parser.add_argument("--email-to", help="Comma-separated recipients for alert email")
    parser.add_argument("--smtp-host", default="localhost", help="SMTP host")
    parser.add_argument("--smtp-port", type=int, default=587, help="SMTP port")
    parser.add_argument("--smtp-user", help="SMTP username")
    parser.add_argument("--smtp-password", help="SMTP password")
    parser.add_argument("--smtp-from", default="fabric-agent@company.com", help="From address")
    parser.add_argument("--no-tls", action="store_true", help="Disable SMTP STARTTLS")

    args = parser.parse_args()
    if not args.contract and not args.day1_schema:
        parser.error("Provide --contract or --day1-schema.")
    return args


async def main_async(args: argparse.Namespace) -> int:
    payload = await run(args)

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if args.format == "json":
        print(json.dumps(payload, indent=2))
    else:
        print(_to_markdown(payload))
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(main_async(parse_args())))


if __name__ == "__main__":
    main()
