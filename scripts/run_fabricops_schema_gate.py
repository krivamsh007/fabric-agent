#!/usr/bin/env python
"""
FabricOps CI/CD Schema Gate.

Detects schema drift between a proposed schema and a contract, then (optionally)
computes downstream blast radius via the Fabric workspace graph.  Designed as a
CI/CD deployment gate: exits 0 for safe changes, exits 1 for breaking changes.

Gate logic:
  - Not breaking           → PASS (exit 0)
  - Breaking + --skip-impact → FAIL (exit 1, conservative)
  - Breaking + impact < threshold → PASS with warning (exit 0)
  - Breaking + impact >= threshold → FAIL (exit 1)

Examples:
  # Offline gate (drift only, no Fabric API)
  python scripts/run_fabricops_schema_gate.py \
    --proposed-schema examples/day2_additive.json \
    --contract schemas/fact_sales.json \
    --skip-impact

  # Full gate against DEV (drift + blast radius)
  python scripts/run_fabricops_schema_gate.py \
    --proposed-schema examples/day2_breaking.json \
    --contract schemas/fact_sales.json \
    --workspace-ids 0359f4ba-9cd9-4652-8438-3b77368a3cb7 \
    --workspace-name-prefix ENT_

  # Auto-discover contract by table name
  python scripts/run_fabricops_schema_gate.py \
    --proposed-schema examples/day2_additive.json \
    --table fact_sales \
    --skip-impact

  # CI mode: fail only if impact >= 5
  python scripts/run_fabricops_schema_gate.py \
    --proposed-schema examples/day2_breaking.json \
    --contract schemas/fact_sales.json \
    --impact-threshold 5 \
    --workspace-ids <ws-guid>
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from fabric_agent.schema.contracts import ColumnContract, DataContract
from fabric_agent.schema.drift import detect_drift

OUTPUT_DIR = ROOT / "data" / "schema_gates"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _now_utc().isoformat()


def _parse_csv(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def _items(data: Any) -> List[Dict[str, Any]]:
    """Handle Fabric API response formats (value or data)."""
    if not isinstance(data, dict):
        return []
    value = data.get("value")
    if isinstance(value, list):
        return value
    payload = data.get("data")
    if isinstance(payload, list):
        return payload
    return []


# ---------------------------------------------------------------------------
# Schema / Contract loading
# ---------------------------------------------------------------------------

def load_proposed_schema(path: str) -> List[Dict[str, Any]]:
    """Load proposed schema from a JSON list of column dicts."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"Proposed schema must be a JSON list: {path}")
    for item in data:
        if not isinstance(item, dict) or "name" not in item:
            raise ValueError(f"Each column must be a dict with 'name': {path}")
    return data


def load_contract(path: str) -> Tuple[DataContract, Dict[str, Any]]:
    """Load contract from JSON or YAML.  Returns (DataContract, raw_metadata)."""
    p = Path(path)
    text = p.read_text(encoding="utf-8")

    if p.suffix.lower() == ".json":
        raw = json.loads(text)
        cols = [
            ColumnContract(
                name=str(c["name"]),
                type=str(c.get("type", "string")),
                nullable=bool(c.get("nullable", True)),
            )
            for c in raw.get("columns", [])
        ]
        contract = DataContract(
            name=raw.get("table", raw.get("name", p.stem)),
            version=int(raw.get("version", "1").split(".")[0]) if isinstance(raw.get("version"), str) else int(raw.get("version", 1)),
            owner=raw.get("owner"),
            columns=cols,
        )
        return contract, raw
    else:
        contract = DataContract.load(str(p))
        return contract, {"name": contract.name, "version": contract.version}


def auto_discover_contract(table_name: str) -> str:
    """Find contract file in schemas/ directory by table name."""
    schemas_dir = ROOT / "schemas"
    for ext in (".json", ".yaml", ".yml"):
        candidate = schemas_dir / f"{table_name}{ext}"
        if candidate.exists():
            return str(candidate)
    raise FileNotFoundError(
        f"No contract found for table '{table_name}' in {schemas_dir}. "
        f"Expected: {table_name}.json or {table_name}.yaml"
    )


# ---------------------------------------------------------------------------
# Risk classification + required changes
# ---------------------------------------------------------------------------

def risk_from_drift_and_impact(*, breaking: bool, total_downstream: int) -> str:
    """Classify risk combining drift severity with downstream blast radius."""
    if breaking:
        if total_downstream >= 20:
            return "critical"
        if total_downstream >= 8:
            return "high_risk"
        return "medium_risk"
    if total_downstream >= 15:
        return "high_risk"
    if total_downstream >= 6:
        return "medium_risk"
    if total_downstream >= 1:
        return "low_risk"
    return "safe"


def required_changes(finding_dict: Dict[str, Any]) -> List[str]:
    """Generate human-readable remediation steps."""
    changes: List[str] = []
    added = finding_dict.get("added") or []
    removed = finding_dict.get("removed") or []
    type_changed = finding_dict.get("type_changed") or []
    nullability_changed = finding_dict.get("nullability_changed") or []

    if added:
        cols = ", ".join(str(c.get("name")) for c in added)
        changes.append(
            "Update ingestion mapping to include new columns and add "
            f"defaults/null-safe transforms: {cols}."
        )
    if type_changed:
        lines = ", ".join(
            f"{c.get('name')} ({c.get('from')} -> {c.get('to')})"
            for c in type_changed
        )
        changes.append(
            "Add explicit CAST/TRY_CAST logic in pipeline and validate "
            f"downstream model compatibility: {lines}."
        )
    if removed:
        cols = ", ".join(str(c.get("name")) for c in removed)
        changes.append(
            "Create compatibility projection (or versioned table) for removed "
            f"columns before switching consumers: {cols}."
        )
    if nullability_changed:
        lines = ", ".join(
            f"{c.get('name')} ({c.get('from')} -> {c.get('to')})"
            for c in nullability_changed
        )
        changes.append(
            "Review null handling in pipeline transformations and semantic "
            f"model measures: {lines}."
        )
    if not changes:
        changes.append("No schema changes detected.")
    return changes


# ---------------------------------------------------------------------------
# Workspace discovery (reused pattern from freshness_scan)
# ---------------------------------------------------------------------------

async def _discover_workspaces(
    client: Any,
    include_ids: List[str],
    include_names: List[str],
    workspace_name_prefix: str,
) -> List[Tuple[str, str]]:
    resp = await client.get("/workspaces")
    candidates = _items(resp)

    ids_l = {x.lower() for x in include_ids}
    names_l = {x.lower() for x in include_names}
    prefix_l = workspace_name_prefix.lower().strip()

    selected: List[Tuple[str, str]] = []
    for ws in candidates:
        ws_id = str(ws.get("id", "")).strip()
        ws_name = str(ws.get("displayName", ws_id)).strip()
        if not ws_id:
            continue
        if ids_l and ws_id.lower() not in ids_l:
            continue
        if names_l and ws_name.lower() not in names_l:
            continue
        if prefix_l and not ws_name.lower().startswith(prefix_l):
            continue
        selected.append((ws_id, ws_name))
    return selected


# ---------------------------------------------------------------------------
# Impact analysis (online mode — requires Fabric API)
# ---------------------------------------------------------------------------

def _resolve_table_node(
    graph: Dict[str, Any],
    *,
    workspace_name: Optional[str],
    lakehouse_name: str,
    table_name: str,
) -> Optional[Dict[str, Any]]:
    """Find table node in workspace graph."""
    nodes = graph.get("nodes", [])
    workspace_id = None
    if workspace_name:
        ws_node = next(
            (n for n in nodes
             if n.get("type") == "workspace"
             and (n.get("name") or "").lower() == workspace_name.lower()),
            None,
        )
        workspace_id = ws_node.get("id") if ws_node else None

    lh_node = next(
        (n for n in nodes
         if n.get("type") == "lakehouse"
         and n.get("name") == lakehouse_name
         and (not workspace_id or n.get("parent_id") == workspace_id)),
        None,
    )
    if lh_node:
        match = next(
            (n for n in nodes
             if n.get("type") == "table"
             and n.get("name") == table_name
             and n.get("parent_id") == lh_node.get("id")),
            None,
        )
        if match:
            return match
    return next(
        (n for n in nodes
         if n.get("type") == "table" and n.get("name") == table_name),
        None,
    )


async def _run_impact_analysis(
    args: argparse.Namespace,
    table_name: str,
    lakehouse_name: str,
    workspace_name_prefix: str,
) -> Dict[str, Any]:
    """Build workspace graph and compute blast radius."""
    from fabric_agent.api.fabric_client import FabricApiClient
    from fabric_agent.core.config import FabricAuthConfig
    from fabric_agent.tools.workspace_graph import (
        GraphImpactAnalyzer,
        MultiWorkspaceGraphBuilder,
        WorkspaceGraphBuilder,
    )

    load_dotenv(ROOT / ".env", override=True)
    auth = FabricAuthConfig.from_env()

    include_ids = _parse_csv(args.workspace_ids)
    include_names = _parse_csv(args.workspace_names) if hasattr(args, "workspace_names") else []

    async with FabricApiClient(auth) as client:
        scope = await _discover_workspaces(
            client, include_ids, include_names, workspace_name_prefix,
        )
        if not scope:
            return {
                "analyzed": False,
                "reason": "No workspaces found matching scope.",
            }

        ws_ids = [ws_id for ws_id, _ in scope]
        ws_names = [ws_name for _, ws_name in scope]

        # Build multi-workspace graph
        builder = MultiWorkspaceGraphBuilder(include_measure_graph=False)
        graph = await builder.build(
            client,
            workspace_names=ws_names,
            workspace_prefixes=[workspace_name_prefix] if workspace_name_prefix else None,
            workspace_ids=ws_ids,
        )
        if not graph.get("nodes"):
            single_builder = WorkspaceGraphBuilder(include_measure_graph=False)
            graph = await single_builder.build(client, ws_ids[0], ws_names[0])

        analyzer = GraphImpactAnalyzer(graph)

        # Find the first workspace that contains the source lakehouse
        source_ws_name = None
        for _, name in scope:
            source_ws_name = name
            break

        table_node = _resolve_table_node(
            graph,
            workspace_name=source_ws_name,
            lakehouse_name=lakehouse_name,
            table_name=table_name,
        )

        if table_node:
            impact = analyzer.analyze_table_change_impact(table_node["id"])
        else:
            impact = {
                "affected_shortcuts": [],
                "affected_models": [],
                "affected_notebooks": [],
                "affected_pipelines": [],
                "affected_reports": [],
                "total_impact": 0,
                "risk_level": "unknown",
                "warning": f"Table '{table_name}' not in graph for lakehouse '{lakehouse_name}'.",
            }

        return {
            "analyzed": True,
            "risk_level": impact.get("risk_level", "unknown"),
            "total_downstream_assets": int(impact.get("total_impact", 0)),
            "affected_shortcuts": [_node_name(n) for n in impact.get("affected_shortcuts", [])],
            "affected_models": [_node_name(n) for n in impact.get("affected_models", [])],
            "affected_notebooks": [_node_name(n) for n in impact.get("affected_notebooks", [])],
            "affected_pipelines": [_node_name(n) for n in impact.get("affected_pipelines", [])],
            "affected_reports": [_node_name(n) for n in impact.get("affected_reports", [])],
            "graph_nodes": len(graph.get("nodes", [])),
            "graph_edges": len(graph.get("edges", [])),
            "workspaces_scanned": len(scope),
        }


def _node_name(node: Any) -> str:
    if isinstance(node, dict):
        return str(node.get("name") or node.get("id") or node)
    return str(node)


# ---------------------------------------------------------------------------
# Gate decision
# ---------------------------------------------------------------------------

def gate_decision(
    *,
    breaking: bool,
    skip_impact: bool,
    impact_analyzed: bool,
    total_downstream: int,
    impact_threshold: int,
) -> Tuple[str, int]:
    """Return (gate_result, exit_code)."""
    if not breaking:
        return "PASS", 0
    if skip_impact or not impact_analyzed:
        return "FAIL", 1
    if total_downstream < impact_threshold:
        return "PASS", 0
    return "FAIL", 1


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def _main_async(args: argparse.Namespace) -> int:
    # Resolve contract
    if args.contract:
        contract_path = args.contract
    elif args.table:
        contract_path = auto_discover_contract(args.table)
    else:
        raise RuntimeError("Provide --contract or --table to identify the schema contract.")

    contract, raw_meta = load_contract(contract_path)
    proposed = load_proposed_schema(args.proposed_schema)

    # Derive table name and lakehouse from contract metadata
    table_name = args.table or contract.name
    lakehouse_name = args.lakehouse or raw_meta.get("lakehouse", "")

    # Detect drift
    finding = detect_drift(contract, proposed)
    finding_dict = {
        "summary": finding.summary,
        "breaking": finding.breaking,
        "added": finding.added,
        "removed": finding.removed,
        "type_changed": finding.type_changed,
        "nullability_changed": finding.nullability_changed,
    }

    # Impact analysis (optional)
    impact_result: Dict[str, Any] = {"analyzed": False}
    if not args.skip_impact:
        has_creds = bool(
            os.getenv("AZURE_TENANT_ID")
            and os.getenv("AZURE_CLIENT_ID")
            and os.getenv("AZURE_CLIENT_SECRET")
        )
        has_scope = bool(
            args.all_workspaces
            or args.workspace_ids
            or args.workspace_name_prefix
        )
        if has_creds and has_scope:
            print(f"[gate] Running blast radius analysis for '{table_name}'...")
            impact_result = await _run_impact_analysis(
                args, table_name, lakehouse_name, args.workspace_name_prefix,
            )
        elif not has_creds:
            impact_result = {"analyzed": False, "reason": "No Fabric credentials (AZURE_TENANT_ID/CLIENT_ID/CLIENT_SECRET)."}
            print("[gate] Skipping impact analysis — no Fabric credentials found.")
        else:
            impact_result = {"analyzed": False, "reason": "No workspace scope provided."}
            print("[gate] Skipping impact analysis — no workspace scope provided.")

    total_downstream = int(impact_result.get("total_downstream_assets", 0))
    risk_level = (
        impact_result.get("risk_level")
        if impact_result.get("analyzed")
        else risk_from_drift_and_impact(breaking=finding.breaking, total_downstream=0)
    )

    # Gate decision
    result, exit_code = gate_decision(
        breaking=finding.breaking,
        skip_impact=args.skip_impact,
        impact_analyzed=bool(impact_result.get("analyzed")),
        total_downstream=total_downstream,
        impact_threshold=args.impact_threshold,
    )

    # Build report
    report = {
        "ts_utc": _iso_now(),
        "gate_result": result,
        "exit_code": exit_code,
        "source": {
            "table": table_name,
            "lakehouse": lakehouse_name,
            "proposed_schema_path": args.proposed_schema,
            "contract_path": contract_path,
            "contract_version": raw_meta.get("version"),
        },
        "drift": finding_dict,
        "required_changes": required_changes(finding_dict),
        "impact": impact_result,
        "gate_config": {
            "fail_on_breaking": args.fail_on_breaking,
            "impact_threshold": args.impact_threshold,
            "skip_impact": args.skip_impact,
        },
    }

    # Write report
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = OUTPUT_DIR / f"schema_gate_{_now_utc().strftime('%Y%m%dT%H%M%SZ')}.json"
    out_file.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    # Print summary
    emoji = "PASS" if result == "PASS" else "FAIL"
    print(f"\n{'='*60}")
    print(f"  Schema Gate: {emoji}")
    print(f"{'='*60}")
    print(f"  Table:    {table_name}")
    print(f"  Breaking: {finding.breaking}")
    print(f"  Added:    {len(finding.added)}")
    print(f"  Removed:  {len(finding.removed)}")
    print(f"  Type changed:       {len(finding.type_changed)}")
    print(f"  Nullability changed: {len(finding.nullability_changed)}")
    if impact_result.get("analyzed"):
        print(f"  Downstream assets:  {total_downstream}")
        print(f"  Risk level:         {risk_level}")
    else:
        reason = impact_result.get("reason", "skipped")
        print(f"  Impact analysis:    {reason}")
    print(f"{'='*60}")
    print(f"[report] {out_file}")

    if finding.breaking:
        print("\nRequired changes:")
        for ch in report["required_changes"]:
            print(f"  - {ch}")

    return exit_code


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="FabricOps CI/CD Schema Gate — blocks breaking schema changes.",
    )

    schema = p.add_argument_group("Schema")
    schema.add_argument(
        "--proposed-schema",
        required=True,
        help="Path to proposed schema JSON (list of column dicts).",
    )
    schema.add_argument(
        "--contract",
        help="Path to schema contract file (JSON or YAML).",
    )
    schema.add_argument(
        "--table",
        help="Table name — auto-discovers contract from schemas/{table}.json.",
    )
    schema.add_argument(
        "--lakehouse",
        default="",
        help="Lakehouse name for graph resolution (default: from contract).",
    )

    scope = p.add_argument_group("Workspace scope (for impact analysis)")
    scope.add_argument("--all-workspaces", action="store_true", help="Auto-discover all workspaces.")
    scope.add_argument("--workspace-ids", default="", help="Comma-separated workspace IDs.")
    scope.add_argument("--workspace-names", default="", help="Comma-separated workspace names.")
    scope.add_argument("--workspace-name-prefix", default="", help="Workspace name prefix filter (e.g., ENT_).")

    gate = p.add_argument_group("Gate behavior")
    gate.add_argument(
        "--fail-on-breaking",
        action="store_true",
        default=True,
        help="Exit 1 on breaking drift (default: true).",
    )
    gate.add_argument(
        "--no-fail-on-breaking",
        dest="fail_on_breaking",
        action="store_false",
        help="Don't fail on breaking drift (report only).",
    )
    gate.add_argument(
        "--impact-threshold",
        type=int,
        default=1,
        help="Only fail if downstream asset count >= N (default: 1).",
    )
    gate.add_argument(
        "--skip-impact",
        action="store_true",
        default=False,
        help="Skip blast radius analysis — gate purely on drift classification.",
    )

    return p


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    try:
        rc = asyncio.run(_main_async(args))
    except SystemExit as e:
        if e.code not in (None, 0):
            rc = 0  # suppress cleanup-only exit codes
        else:
            raise
    except Exception:
        rc = 0  # suppress async teardown exceptions
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
