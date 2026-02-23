#!/usr/bin/env python3
"""
Analyze enterprise impact of a Lakehouse Table change.

What it answers:
- How many consumer lakehouses are impacted (via shortcuts)?
- Which shortcuts (name + destination workspace/lakehouse) point to the source table?
- Which downstream assets in those consumer workspaces *likely* reference the table:
  - Notebooks (code search)
  - Data Pipelines (definition search)
  - Semantic Models (TMSL search)
  - Reports (listed; optionally attempts basic mapping)

Usage examples:
  python scripts/analyze_data_impact.py --source "ENT_DataPlatform_DEV/Gold_Published.agg_daily_sales" --format markdown
  python scripts/analyze_data_impact.py --source "ENT_DataPlatform_DEV/Silver_Curated.fact_sales" --format json
  python scripts/analyze_data_impact.py --source "ENT_DataPlatform_DEV/Gold_Published" --format markdown   # lakehouse-level

Notes:
- Shortcuts are the key: they connect producer tables -> consumer lakehouses.
- Fabric APIs return different shapes for tables/shortcuts (value vs data). This script handles both.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from loguru import logger

from fabric_agent.core.config import FabricAuthConfig
from fabric_agent.api.fabric_client import FabricApiClient

# Reuse your enterprise config + ShortcutConfig
from scripts.bootstrap_enterprise_domains import build_enterprise_config, ShortcutConfig


# -----------------------------
# Helpers
# -----------------------------

def _items(resp: Any) -> List[Dict[str, Any]]:
    """Fabric responses sometimes return {value:[...]}, sometimes {data:[...]}."""
    if not isinstance(resp, dict):
        return []
    for k in ("value", "data", "items"):
        v = resp.get(k)
        if isinstance(v, list):
            return v
    return []

def _lower(s: Optional[str]) -> str:
    return (s or "").lower()

def parse_source(source: str) -> Tuple[str, str, Optional[str]]:
    """
    Parse:
      WS/LH.table
      WS/LH:table
      WS/LH        (lakehouse-level)
    """
    s = (source or "").strip()
    if "/" not in s:
        raise ValueError("source must look like 'Workspace/Lakehouse.table' or 'Workspace/Lakehouse'")
    ws, rest = s.split("/", 1)
    if ":" in rest:
        lh, tbl = rest.split(":", 1)
        return ws.strip(), lh.strip(), tbl.strip()
    if "." in rest:
        lh, tbl = rest.split(".", 1)
        return ws.strip(), lh.strip(), tbl.strip()
    return ws.strip(), rest.strip(), None

async def resolve_workspace_id(client: FabricApiClient, ws_name: str) -> str:
    wss = _items(await client.get("/workspaces"))
    for w in wss:
        if w.get("displayName") == ws_name:
            return w.get("id")
    raise RuntimeError(f"Workspace not found: {ws_name}")

async def resolve_lakehouse_id(client: FabricApiClient, ws_id: str, lh_name: str) -> str:
    lhs = _items(await client.get(f"/workspaces/{ws_id}/lakehouses"))
    for lh in lhs:
        if lh.get("displayName") == lh_name:
            return lh.get("id")
    raise RuntimeError(f"Lakehouse not found: {lh_name} (workspaceId={ws_id})")

async def list_lakehouses(client: FabricApiClient, ws_id: str) -> List[Dict[str, Any]]:
    return _items(await client.get(f"/workspaces/{ws_id}/lakehouses"))

async def list_tables(client: FabricApiClient, ws_id: str, lh_id: str) -> List[Dict[str, Any]]:
    # tables endpoint commonly returns {"data":[...]}
    resp = await client.get(f"/workspaces/{ws_id}/lakehouses/{lh_id}/tables", params={"maxResults": 100})
    return _items(resp)

async def list_shortcuts(client: FabricApiClient, ws_id: str, lh_id: str) -> List[Dict[str, Any]]:
    resp = await client.get(f"/workspaces/{ws_id}/items/{lh_id}/shortcuts")
    return _items(resp)

def shortcut_points_to(sc: Dict[str, Any], src_ws_id: str, src_lh_id: str, src_table: Optional[str]) -> bool:
    """
    Preferred: target.oneLake.workspaceId/itemId/path
    Fallback: string scan of JSON.
    """
    try:
        tgt = (sc.get("target") or {}).get("oneLake") or {}
        w = tgt.get("workspaceId")
        i = tgt.get("itemId")
        p = tgt.get("path") or ""
        if w == src_ws_id and i == src_lh_id:
            if not src_table:
                return True
            return _lower(p).endswith(_lower(f"Tables/{src_table}"))
    except Exception:
        pass

    # fallback
    blob = _lower(json.dumps(sc, default=str))
    if src_ws_id.lower() not in blob:
        return False
    if src_lh_id.lower() not in blob:
        return False
    if not src_table:
        return True
    return _lower(src_table) in blob

def _extract_inlinebase64_text(defn: Dict[str, Any]) -> str:
    """
    Decode any parts.payload where payloadType=InlineBase64 and return concatenated text.
    Works for notebooks/pipelines/models/reports definitions.
    """
    d = defn.get("definition", defn)
    parts = d.get("parts", []) if isinstance(d, dict) else []
    out: List[str] = []

    for p in parts:
        if _lower(p.get("payloadType")) != "inlinebase64":
            continue
        payload = p.get("payload")
        if not payload:
            continue
        try:
            decoded = base64.b64decode(payload).decode("utf-8", errors="replace")
            out.append(decoded)
        except Exception:
            continue

    return "\n".join(out)

async def scan_notebooks_for_table(
    client: FabricApiClient,
    ws_id: str,
    table_name: str,
) -> List[Dict[str, Any]]:
    nbs = _items(await client.get(f"/workspaces/{ws_id}/notebooks"))
    hits: List[Dict[str, Any]] = []
    needle = _lower(table_name)

    for nb in nbs:
        nb_id = nb.get("id")
        nb_name = nb.get("displayName")
        if not nb_id:
            continue
        try:
            defn = await client.post_with_lro(f"/workspaces/{ws_id}/notebooks/{nb_id}/getDefinition")
            text = _lower(_extract_inlinebase64_text(defn))
            if needle in text:
                hits.append({"id": nb_id, "name": nb_name})
        except Exception as e:
            logger.warning(f"Notebook definition scan failed for '{nb_name}': {e}")
    return hits

async def scan_pipelines_for_table(
    client: FabricApiClient,
    ws_id: str,
    table_name: str,
) -> List[Dict[str, Any]]:
    pipes = _items(await client.get(f"/workspaces/{ws_id}/dataPipelines"))
    hits: List[Dict[str, Any]] = []
    needle = _lower(table_name)

    for p in pipes:
        pid = p.get("id")
        pname = p.get("displayName")
        if not pid:
            continue
        try:
            defn = await client.post_with_lro(f"/workspaces/{ws_id}/dataPipelines/{pid}/getDefinition")
            text = _lower(_extract_inlinebase64_text(defn))
            if needle in text:
                hits.append({"id": pid, "name": pname})
        except Exception as e:
            logger.warning(f"Pipeline definition scan failed for '{pname}': {e}")
    return hits

async def scan_models_for_table(
    client: FabricApiClient,
    ws_id: str,
    table_name: str,
) -> List[Dict[str, Any]]:
    models = _items(await client.get(f"/workspaces/{ws_id}/semanticModels"))
    hits: List[Dict[str, Any]] = []
    needle = _lower(table_name)

    for m in models:
        mid = m.get("id")
        mname = m.get("displayName")
        if not mid:
            continue
        try:
            defn = await client.post_with_lro(
                f"/workspaces/{ws_id}/semanticModels/{mid}/getDefinition",
                params={"format": "TMSL"},
            )
            text = _lower(_extract_inlinebase64_text(defn))
            if needle in text:
                hits.append({"id": mid, "name": mname})
        except Exception as e:
            logger.warning(f"Semantic model definition scan failed for '{mname}': {e}")
    return hits

async def list_reports(client: FabricApiClient, ws_id: str) -> List[Dict[str, Any]]:
    return _items(await client.get(f"/workspaces/{ws_id}/reports"))


# -----------------------------
# Impact analysis
# -----------------------------

def consumers_from_config(domains: List[Any], src_ws: str, src_lh: str, src_tbl: Optional[str]) -> List[Tuple[str, str, ShortcutConfig]]:
    """
    Return list of (dest_ws_name, dest_lh_name, shortcut_config) that reference the source.
    """
    consumers: List[Tuple[str, str, ShortcutConfig]] = []
    for d in domains:
        for ws in d.workspaces:
            if not ws.shortcuts:
                continue
            # convention in your config: consumer workspace has 1 lakehouse (dest)
            dest_lh = ws.lakehouses[0].name if ws.lakehouses else None
            for sc in ws.shortcuts:
                if sc.source_workspace != src_ws:
                    continue
                if sc.source_lakehouse != src_lh:
                    continue
                if src_tbl and sc.source_table != src_tbl:
                    continue
                if not src_tbl:
                    # lakehouse-level: everything from that lakehouse counts
                    pass
                if dest_lh:
                    consumers.append((ws.name, dest_lh, sc))
    return consumers

async def analyze(
    client: FabricApiClient,
    *,
    prefix: str,
    src_ws_name: str,
    src_lh_name: str,
    src_table: Optional[str],
) -> Dict[str, Any]:
    domains = build_enterprise_config(prefix=prefix)

    # Resolve source IDs
    src_ws_id = await resolve_workspace_id(client, src_ws_name)
    src_lh_id = await resolve_lakehouse_id(client, src_ws_id, src_lh_name)

    # Config-based consumers (expected)
    expected = consumers_from_config(domains, src_ws_name, src_lh_name, src_table)

    # Live shortcut matches (actual)
    live_matches: List[Dict[str, Any]] = []
    affected_consumer_lakehouses: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

    # We only scan workspaces that have shortcuts configured (fast + aligned to your architecture)
    shortcut_workspaces = []
    for d in domains:
        for ws in d.workspaces:
            if ws.shortcuts and ws.lakehouses:
                shortcut_workspaces.append((ws.name, ws.lakehouses[0].name))

    for (dest_ws_name, dest_lh_name) in shortcut_workspaces:
        try:
            dest_ws_id = await resolve_workspace_id(client, dest_ws_name)
            dest_lh_id = await resolve_lakehouse_id(client, dest_ws_id, dest_lh_name)
            shortcuts = await list_shortcuts(client, dest_ws_id, dest_lh_id)
            for sc in shortcuts:
                if shortcut_points_to(sc, src_ws_id, src_lh_id, src_table):
                    live_matches.append({
                        "dest_workspace": dest_ws_name,
                        "dest_lakehouse": dest_lh_name,
                        "shortcut_name": sc.get("name") or sc.get("displayName"),
                        "shortcut_id": sc.get("id"),
                    })
                    affected_consumer_lakehouses.setdefault((dest_ws_name, dest_lh_name), []).append(sc)
        except Exception as e:
            logger.warning(f"Shortcut scan failed for {dest_ws_name}/{dest_lh_name}: {e}")

    # Downstream scans in consumer workspaces (best-effort)
    downstream: List[Dict[str, Any]] = []
    if src_table:
        for (dest_ws_name, dest_lh_name), _scs in affected_consumer_lakehouses.items():
            dest_ws_id = await resolve_workspace_id(client, dest_ws_name)

            notebooks = await scan_notebooks_for_table(client, dest_ws_id, src_table)
            pipelines = await scan_pipelines_for_table(client, dest_ws_id, src_table)
            models = await scan_models_for_table(client, dest_ws_id, src_table)
            reports = await list_reports(client, dest_ws_id)

            downstream.append({
                "workspace": dest_ws_name,
                "lakehouse": dest_lh_name,
                "notebooks_hit": notebooks,
                "pipelines_hit": pipelines,
                "models_hit": models,
                # reports mapping is hard without report def parsing; list as “potentially impacted” if model(s) hit
                "reports_potential": reports if models else [],
            })

    return {
        "source": {
            "workspace": src_ws_name,
            "lakehouse": src_lh_name,
            "table": src_table,
            "workspaceId": src_ws_id,
            "lakehouseId": src_lh_id,
        },
        "expected_consumers_from_config": [
            {"dest_workspace": w, "dest_lakehouse": lh, "shortcut": sc.name, "source_table": sc.source_table}
            for (w, lh, sc) in expected
        ],
        "live_shortcut_matches": live_matches,
        "summary": {
            "consumer_lakehouses_impacted": len(affected_consumer_lakehouses),
            "shortcuts_impacted": len(live_matches),
            "consumer_workspaces_impacted": len({w for (w, _lh) in affected_consumer_lakehouses.keys()}),
        },
        "downstream_best_effort": downstream,
    }

def to_markdown(result: Dict[str, Any]) -> str:
    src = result["source"]
    summ = result["summary"]
    lines: List[str] = []

    lines.append(f"# Data Impact Analysis\n")
    lines.append(f"**Source**: `{src['workspace']}/{src['lakehouse']}`")
    if src.get("table"):
        lines.append(f".`{src['table']}`\n")
    else:
        lines.append("\n")

    lines.append("## Summary\n")
    lines.append(f"- Consumer workspaces impacted: **{summ['consumer_workspaces_impacted']}**\n")
    lines.append(f"- Consumer lakehouses impacted: **{summ['consumer_lakehouses_impacted']}**\n")
    lines.append(f"- Shortcuts impacted (live): **{summ['shortcuts_impacted']}**\n")

    lines.append("## Live shortcut matches\n")
    if not result["live_shortcut_matches"]:
        lines.append("- (none found)\n")
    else:
        for m in result["live_shortcut_matches"]:
            lines.append(f"- `{m['dest_workspace']}/{m['dest_lakehouse']}` ← shortcut **{m['shortcut_name']}**\n")

    lines.append("\n## Expected shortcut plan (from config)\n")
    if not result["expected_consumers_from_config"]:
        lines.append("- (none in config)\n")
    else:
        for e in result["expected_consumers_from_config"]:
            lines.append(f"- `{e['dest_workspace']}/{e['dest_lakehouse']}` ← **{e['shortcut']}** (source_table={e['source_table']})\n")

    if src.get("table"):
        lines.append("\n## Downstream assets (best-effort scans)\n")
        for d in result["downstream_best_effort"]:
            lines.append(f"\n### {d['workspace']} / {d['lakehouse']}\n")

            lines.append("**Notebooks referencing table**\n")
            if not d["notebooks_hit"]:
                lines.append("- none\n")
            else:
                for nb in d["notebooks_hit"]:
                    lines.append(f"- {nb['name']}\n")

            lines.append("\n**Pipelines referencing table**\n")
            if not d["pipelines_hit"]:
                lines.append("- none\n")
            else:
                for p in d["pipelines_hit"]:
                    lines.append(f"- {p['name']}\n")

            lines.append("\n**Semantic models referencing table**\n")
            if not d["models_hit"]:
                lines.append("- none\n")
            else:
                for m in d["models_hit"]:
                    lines.append(f"- {m['name']}\n")

            if d["reports_potential"]:
                lines.append("\n**Reports potentially impacted (because model hit)**\n")
                for r in d["reports_potential"]:
                    lines.append(f"- {r.get('displayName')}\n")

    return "".join(lines)


# -----------------------------
# CLI
# -----------------------------

async def main_async(args: argparse.Namespace) -> int:
    load_dotenv()
    auth = FabricAuthConfig.from_env()

    src_ws, src_lh, src_tbl = parse_source(args.source)

    async with FabricApiClient(auth) as client:
        result = await analyze(
            client,
            prefix=args.prefix,
            src_ws_name=src_ws,
            src_lh_name=src_lh,
            src_table=src_tbl,
        )

    if args.format == "json":
        print(json.dumps(result, indent=2))
    else:
        print(to_markdown(result))

    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze enterprise impact of a lakehouse table via shortcuts + downstream assets")
    p.add_argument("--prefix", default="ENT", help="Workspace prefix used by build_enterprise_config() (default: ENT)")
    p.add_argument("--source", required=True, help="Source like 'WS/LH.table' or 'WS/LH' (lakehouse-level)")
    p.add_argument("--format", choices=["markdown", "json"], default="markdown")
    return p.parse_args()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async(parse_args())))
