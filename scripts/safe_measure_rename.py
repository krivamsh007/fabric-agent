#!/usr/bin/env python3
"""
Safe Measure Rename (end-to-end)
================================
Plan -> Apply -> Validate -> Rollback for renaming a measure safely across:
- Semantic model definition (TMDL)
- Dependent measures (expressions)
- Report definitions that reference the old measure

Usage:
  # 1) Dry run / impact only
  python scripts/safe_measure_rename.py --workspace "fabric-refactor-demo" --model "ENT_SemanticModel" --old "ENT_RenameMe_ProfitIndex" --new "ENT_ProfitIndex" --dry-run

  # 2) Apply
  python scripts/safe_measure_rename.py --workspace "fabric-refactor-demo" --model "ENT_SemanticModel" --old "ENT_RenameMe_ProfitIndex" --new "ENT_ProfitIndex" --apply

  # 3) Validate
  python scripts/safe_measure_rename.py --workspace "fabric-refactor-demo" --model "ENT_SemanticModel" --old "ENT_RenameMe_ProfitIndex" --validate

  # 4) Rollback
  python scripts/safe_measure_rename.py --rollback --operation-id <OP_ID> --workspace "fabric-refactor-demo"
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger

from fabric_agent.core.config import FabricAuthConfig
from fabric_agent.api.fabric_client import FabricApiClient


LOG_PATH = Path("memory/safe_rename_log.json")


# ----------------------------
# Helpers: workspace & items
# ----------------------------

async def resolve_workspace_id(client: FabricApiClient, workspace_name: str) -> str:
    resp = await client.get("/workspaces")
    for w in resp.get("value", []):
        name = (w.get("displayName") or w.get("name") or "").lower()
        if name == workspace_name.lower():
            return str(w.get("id"))
    raise ValueError(f"Workspace not found: {workspace_name}")


async def list_items(client: FabricApiClient, workspace_id: str) -> List[Dict[str, Any]]:
    resp = await client.get(f"/workspaces/{workspace_id}/items")
    return resp.get("value", [])


def pick_item(items: List[Dict[str, Any]], type_name: str, name_or_id: str) -> Dict[str, Any]:
    name_or_id_l = name_or_id.lower()
    for it in items:
        if (it.get("type") or "").lower() == type_name.lower():
            if str(it.get("id", "")).lower() == name_or_id_l:
                return it
            if (it.get("displayName") or it.get("name") or "").lower() == name_or_id_l:
                return it
    raise ValueError(f"{type_name} not found: {name_or_id}")


# ----------------------------
# Helpers: definition I/O
# ----------------------------

def _b64_decode(payload_b64: str) -> str:
    return base64.b64decode(payload_b64).decode("utf-8", errors="replace")


def _b64_encode(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("utf-8")


async def get_definition(client: FabricApiClient, workspace_id: str, item_type: str, item_id: str) -> Dict[str, Any]:
    """
    Calls Fabric getDefinition and returns the parsed JSON.
    Uses post_with_lro for safety in case Fabric returns 202.
    """
    resp = await client.post_with_lro(
        f"/workspaces/{workspace_id}/{item_type}/{item_id}/getDefinition",
        json_data={}
    )
    # post_with_lro already returns dict (response.json())
    return resp


async def update_definition(
    client: FabricApiClient,
    workspace_id: str,
    item_type: str,
    item_id: str,
    parts: List[Dict[str, str]],
) -> Dict[str, Any]:
    """
    Calls Fabric updateDefinition and returns the operation result (dict).
    """
    body = {"definition": {"parts": parts}}
    resp = await client.post_with_lro(
        f"/workspaces/{workspace_id}/{item_type}/{item_id}/updateDefinition",
        json_data=body
    )
    return resp

def extract_parts(defn: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(defn, dict):
        raise TypeError(f"Definition response not a dict: {type(defn)}")
    return (defn.get("definition") or {}).get("parts", [])


def parts_to_inline_base64(parts_text_by_path: Dict[str, str]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for path, text in parts_text_by_path.items():
        out.append({"path": path, "payload": _b64_encode(text), "payloadType": "InlineBase64"})
    return out


# ----------------------------
# Helpers: TMDL measure graph
# ----------------------------

_MEASURE_DECL_RE = re.compile(r"(?m)^\s*measure\s+('([^']+)'|\"([^\"]+)\"|([A-Za-z0-9_][A-Za-z0-9_ ]*))\s*(=|\{)", re.IGNORECASE)

def list_measures_in_tmdl(tmdl: str) -> List[str]:
    measures: List[str] = []
    for m in _MEASURE_DECL_RE.finditer(tmdl):
        name = m.group(2) or m.group(3) or (m.group(4) or "").strip()
        if name:
            measures.append(name)
    return sorted(set(measures))


def build_measure_dep_graph(tmdl: str) -> Dict[str, List[str]]:
    """
    Return adjacency list: measure -> referenced measures (based on [MeasureName] tokens).
    Conservative: only counts bracket references that match declared measure names.
    """
    declared = set(list_measures_in_tmdl(tmdl))
    deps: Dict[str, List[str]] = {m: [] for m in declared}

    # crude per-measure expression extraction:
    # find "measure X" blocks and read until next "measure " at same/less indent or EOF.
    # Good enough for rename safety checks.
    starts = [(m.start(), (m.group(2) or m.group(3) or (m.group(4) or "").strip())) for m in _MEASURE_DECL_RE.finditer(tmdl)]
    starts.sort(key=lambda x: x[0])
    for i, (pos, name) in enumerate(starts):
        end = starts[i + 1][0] if i + 1 < len(starts) else len(tmdl)
        block = tmdl[pos:end]
        refs = re.findall(r"\[([^\]]+)\]", block)
        deps[name] = sorted({r for r in refs if r in declared and r != name})
    return deps


def dependent_measures(dep_graph: Dict[str, List[str]], target: str) -> List[str]:
    """All measures that (directly or indirectly) depend on target."""
    # reverse graph
    rev: Dict[str, List[str]] = {m: [] for m in dep_graph}
    for m, refs in dep_graph.items():
        for r in refs:
            rev.setdefault(r, []).append(m)

    out: List[str] = []
    stack = list(rev.get(target, []))
    seen = set(stack)
    while stack:
        cur = stack.pop()
        out.append(cur)
        for nxt in rev.get(cur, []):
            if nxt not in seen:
                seen.add(nxt)
                stack.append(nxt)
    return sorted(set(out))


# ----------------------------
# Helpers: safe string replace
# ----------------------------

def replace_token_safe(text: str, old: str, new: str) -> Tuple[str, int]:
    """
    Replace old->new only when old appears as a token (not inside longer identifiers).
    Works for:
      - [OldMeasure] patterns
      - JSON strings containing OldMeasure
    """
    pat = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(old)}(?![A-Za-z0-9_])")
    new_text, n = pat.subn(new, text)
    return new_text, n


def rename_measure_in_tmdl(tmdl: str, old: str, new: str) -> Tuple[str, Dict[str, int]]:
    """
    1) Rename measure declaration line
    2) Replace [old] references across the model
    """
    stats = {"decl_renames": 0, "ref_rewrites": 0}

    # 1) rename declaration lines
    # handle: measure Old..., measure 'Old'..., measure "Old"...
    decl_pat = re.compile(
        rf"(?m)^(\s*measure\s+)('|\")?{re.escape(old)}(\2)?(\s*(=|\{{))",
        re.IGNORECASE
    )
    def _decl_repl(m):
        stats["decl_renames"] += 1
        q = m.group(2) or ""
        return f"{m.group(1)}{q}{new}{q}{m.group(4)}"

    tmdl2 = decl_pat.sub(_decl_repl, tmdl)

    # 2) rewrite bracket references
    tmdl3, n = re.subn(rf"\[{re.escape(old)}\]", f"[{new}]", tmdl2)
    stats["ref_rewrites"] += n
    return tmdl3, stats


# ----------------------------
# Logging + rollback
# ----------------------------

def load_log() -> Dict[str, Any]:
    if LOG_PATH.exists():
        return json.loads(LOG_PATH.read_text(encoding="utf-8"))
    return {"operations": []}


def append_operation(op: Dict[str, Any]) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = load_log()
    data["operations"].append(op)
    LOG_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


def find_operation(operation_id: str) -> Dict[str, Any]:
    data = load_log()
    for op in data.get("operations", []):
        if op.get("operation_id") == operation_id:
            return op
    raise ValueError(f"Operation not found: {operation_id}")


# ----------------------------
# Core: Safe Rename
# ----------------------------

@dataclass
class ReportHit:
    report_id: str
    report_name: str
    parts_touched: Dict[str, Dict[str, Any]]  # path -> {old_text, new_text, replacements}


async def scan_reports_for_measure(
    client: FabricApiClient,
    workspace_id: str,
    items: List[Dict[str, Any]],
    old: str,
    new: str,
) -> List[ReportHit]:
    hits: List[ReportHit] = []
    reports = [it for it in items if (it.get("type") or "").lower() == "report"]

    for r in reports:
        rid = str(r.get("id"))
        rname = r.get("displayName") or r.get("name") or rid
        try:
            defn = await get_definition(client, workspace_id, "reports", rid)
            touched: Dict[str, Dict[str, Any]] = {}
            for p in extract_parts(defn):
                path = p.get("path")
                payload = p.get("payload")
                if not path or not payload:
                    continue
                text = _b64_decode(payload)
                if old not in text:
                    continue
                new_text, n = replace_token_safe(text, old, new)
                if n > 0:
                    touched[path] = {"old_text": text, "new_text": new_text, "replacements": n}
            if touched:
                hits.append(ReportHit(report_id=rid, report_name=rname, parts_touched=touched))
        except Exception as e:
            logger.warning(f"Report scan failed: {rname} ({rid}): {e}")

    return hits


async def safe_rename_measure(
    client: FabricApiClient,
    workspace_id: str,
    workspace_name: str,
    model_id: str,
    model_name: str,
    old: str,
    new: str,
    apply: bool,
) -> Dict[str, Any]:
    op_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S") + f"-{abs(hash((workspace_id, model_id, old, new)))%10**8:08d}"
    logger.info(f"Operation: {op_id}")

    items = await list_items(client, workspace_id)

    # --- 1) Load semantic model definition
    model_def = await get_definition(client, workspace_id, "semanticModels", model_id)
    model_parts = extract_parts(model_def)
    tmdl_part = next((p for p in model_parts if (p.get("path") or "").lower().endswith(".tmdl")), None)
    if not tmdl_part:
        raise RuntimeError("No .tmdl part found in semantic model definition.")

    tmdl_before = _b64_decode(tmdl_part["payload"])

    declared = set(list_measures_in_tmdl(tmdl_before))
    if old not in declared:
        raise ValueError(f"Measure not found in model: {old}")
    if new in declared:
        raise ValueError(f"Target measure already exists in model: {new}")

    dep_graph = build_measure_dep_graph(tmdl_before)
    dependents = dependent_measures(dep_graph, old)

    # --- 2) Scan reports (child-most impact)
    report_hits = await scan_reports_for_measure(client, workspace_id, items, old, new)

    # --- 3) Build plan
    tmdl_after, stats = rename_measure_in_tmdl(tmdl_before, old, new)

    plan = {
        "operation_id": op_id,
        "workspace": {"id": workspace_id, "name": workspace_name},
        "semantic_model": {"id": model_id, "name": model_name},
        "rename": {"old": old, "new": new},
        "impact_child_most": {
            "reports_impacted": [
                {"report_name": h.report_name, "report_id": h.report_id,
                 "parts": [{"path": path, "replacements": meta["replacements"]} for path, meta in h.parts_touched.items()]}
                for h in report_hits
            ]
        },
        "impact_model": {
            "dependent_measures": dependents,
            "tmdl_change_stats": stats,
        },
        "apply_steps": [
            "Update semantic model definition (.tmdl) with renamed measure + rewritten [Old] references",
            "Update each impacted report definition part(s) that reference the old measure token",
        ],
        "rollback": {
            "supported": True,
            "how": f'python scripts/safe_measure_rename.py --rollback --operation-id {op_id} --workspace "{workspace_name}"'
        },
    }

    if not apply:
        return {"mode": "dry_run", "plan": plan}

    # --- 4) Apply semantic model update
    # Only update the .tmdl part (keep the rest unchanged)
    parts_update = []
    for p in model_parts:
        path = p.get("path")
        payload = p.get("payload")
        if not path or not payload:
            continue
        if (path or "").lower().endswith(".tmdl"):
            parts_update.append({"path": path, "payload": _b64_encode(tmdl_after), "payloadType": "InlineBase64"})
        else:
            # pass through unchanged parts (important to keep full definition consistent)
            parts_update.append({"path": path, "payload": payload, "payloadType": p.get("payloadType", "InlineBase64")})

    await update_definition(client, workspace_id, "semanticModels", model_id, parts_update)

    # --- 5) Apply report updates
    report_actions = []
    for h in report_hits:
        parts_text_by_path = {path: meta["new_text"] for path, meta in h.parts_touched.items()}
        report_parts = parts_to_inline_base64(parts_text_by_path)
        await update_definition(client, workspace_id, "reports", h.report_id, report_parts)

        report_actions.append({
            "report_id": h.report_id,
            "report_name": h.report_name,
            "parts": [{"path": path, "replacements": meta["replacements"]} for path, meta in h.parts_touched.items()]
        })

    # --- 6) Persist rollback data (store the exact before/after payloads for touched parts)
    op_record = {
        "operation_id": op_id,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "workspace": {"id": workspace_id, "name": workspace_name},
        "semantic_model": {
            "id": model_id,
            "name": model_name,
            "parts_before": [{"path": p.get("path"), "payload": p.get("payload"), "payloadType": p.get("payloadType", "InlineBase64")} for p in model_parts],
        },
        "reports": [
            {
                "id": h.report_id,
                "name": h.report_name,
                "parts_before": [{"path": path, "payload": _b64_encode(meta["old_text"]), "payloadType": "InlineBase64"} for path, meta in h.parts_touched.items()],
            }
            for h in report_hits
        ],
        "rename": {"old": old, "new": new},
        "applied": {"semantic_model_updated": True, "reports_updated": report_actions},
        "plan": plan,
    }
    append_operation(op_record)

    return {"mode": "applied", "plan": plan, "applied": op_record["applied"]}


async def rollback_operation(client: FabricApiClient, workspace_id: str, operation_id: str) -> Dict[str, Any]:
    op = find_operation(operation_id)
    if op["workspace"]["id"] != workspace_id:
        logger.warning("Workspace mismatch: rollback uses the workspace you pass on CLI.")

    # restore reports first (child-most first restore)
    for r in op.get("reports", []):
        rid = r["id"]
        parts = r.get("parts_before", [])
        await update_definition(client, workspace_id, "reports", rid, parts)

    # restore semantic model
    model = op["semantic_model"]
    await update_definition(client, workspace_id, "semanticModels", model["id"], model["parts_before"])

    return {"rolled_back": True, "operation_id": operation_id}


async def validate_no_old_refs(client: FabricApiClient, workspace_id: str, model_id: str, old: str) -> Dict[str, Any]:
    items = await list_items(client, workspace_id)

    # semantic model scan
    model_def = await get_definition(client, workspace_id, "semanticModels", model_id)
    model_text_hits = 0
    for p in extract_parts(model_def):
        payload = p.get("payload")
        if not payload:
            continue
        if old in _b64_decode(payload):
            model_text_hits += 1

    # reports scan
    report_hits = 0
    reports = [it for it in items if (it.get("type") or "").lower() == "report"]
    for r in reports:
        rid = str(r.get("id"))
        defn = await get_definition(client, workspace_id, "reports", rid)
        for p in extract_parts(defn):
            payload = p.get("payload")
            if not payload:
                continue
            if old in _b64_decode(payload):
                report_hits += 1
                break

    return {"old": old, "semantic_model_parts_with_old": model_text_hits, "reports_with_old": report_hits}


# ----------------------------
# CLI
# ----------------------------

async def main_async(args):
    cfg = FabricAuthConfig.from_env()
    async with FabricApiClient(cfg) as client:
        workspace_id = args.workspace_id or await resolve_workspace_id(client, args.workspace)
        workspace_name = args.workspace or workspace_id

        if args.rollback:
            res = await rollback_operation(client, workspace_id, args.operation_id)
            print(json.dumps(res, indent=2))
            return

        items = await list_items(client, workspace_id)
        model_item = pick_item(items, "semanticModel", args.model)
        model_id = str(model_item["id"])
        model_name = model_item.get("displayName") or model_item.get("name") or model_id

        if args.validate:
            res = await validate_no_old_refs(client, workspace_id, model_id, args.old)
            print(json.dumps(res, indent=2))
            return

        res = await safe_rename_measure(
            client=client,
            workspace_id=workspace_id,
            workspace_name=workspace_name,
            model_id=model_id,
            model_name=model_name,
            old=args.old,
            new=args.new,
            apply=args.apply,
        )
        print(json.dumps(res, indent=2))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--workspace", help="Workspace name")
    p.add_argument("--workspace-id", help="Workspace id")
    p.add_argument("--model", help="Semantic model name OR id")
    p.add_argument("--old", help="Old measure name")
    p.add_argument("--new", help="New measure name")
    p.add_argument("--dry-run", action="store_true", help="Impact report only")
    p.add_argument("--apply", action="store_true", help="Apply changes")
    p.add_argument("--validate", action="store_true", help="Validate old measure references are gone")
    p.add_argument("--rollback", action="store_true", help="Rollback a prior operation")
    p.add_argument("--operation-id", help="Operation ID to rollback")

    args = p.parse_args()

    if not args.workspace and not args.workspace_id:
        p.error("Provide --workspace or --workspace-id")

    if args.rollback:
        if not args.operation_id:
            p.error("--rollback requires --operation-id")
        asyncio.run(main_async(args))
        return

    if args.validate:
        if not args.model or not args.old:
            p.error("--validate requires --model and --old")
        asyncio.run(main_async(args))
        return

    if not args.model or not args.old or not args.new:
        p.error("Provide --model, --old, --new")

    if args.apply and args.dry_run:
        p.error("Choose either --dry-run or --apply")

    # default to dry-run if neither specified
    if not args.apply:
        args.dry_run = True

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
