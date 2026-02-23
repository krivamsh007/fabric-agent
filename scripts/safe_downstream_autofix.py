#!/usr/bin/env python3
"""
Safe downstream reference auto-fix with transactional rollback.

WHAT:
- Scans notebooks, pipelines, semantic models, and reports across multiple
  workspaces for token references (for example old table/column name).
- Applies safe token replacements through updateDefinition.
- If any update fails, it rolls back all already-applied updates in reverse order.

WHY:
- Bridges the gap between impact analysis and actual automated repair.
- Provides deterministic rollback for multi-item definition updates.

USAGE:
  # Dry run (default)
  python scripts/safe_downstream_autofix.py \
    --workspace-prefix ENT \
    --include-workspace ENT_DataPlatform_DEV \
    --old-token fact_sales \
    --new-token fact_sales_v2

  # Apply changes
  python scripts/safe_downstream_autofix.py \
    --workspace-prefix ENT \
    --include-workspace ENT_DataPlatform_DEV \
    --old-token fact_sales \
    --new-token fact_sales_v2 \
    --apply

  # Rollback an applied operation
  python scripts/safe_downstream_autofix.py --rollback --operation-id <OP_ID>
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
from uuid import uuid4

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fabric_agent.api.fabric_client import FabricApiClient
from fabric_agent.core.config import FabricAuthConfig


LOG_PATH = Path("memory/safe_downstream_autofix_log.json")

SUPPORTED_ENDPOINTS = {
    "notebook": "notebooks",
    "pipeline": "dataPipelines",
    "datapipeline": "dataPipelines",
    "semanticmodel": "semanticModels",
    "semantic_model": "semanticModels",
    "report": "reports",
}


@dataclass
class PlannedPatch:
    workspace_id: str
    workspace_name: str
    item_id: str
    item_name: str
    item_type: str
    endpoint: str
    replacements: int
    parts_before: List[Dict[str, Any]]
    parts_after: List[Dict[str, Any]]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_item_type(item_type: str) -> str:
    return (item_type or "").lower().replace(" ", "")


def _replacement_pattern(old_token: str) -> re.Pattern[str]:
    return re.compile(rf"(?<![A-Za-z0-9_]){re.escape(old_token)}(?![A-Za-z0-9_])")


def _replace_token(text: str, old_token: str, new_token: str) -> Tuple[str, int]:
    pat = _replacement_pattern(old_token)
    return pat.subn(new_token, text)


def _decode_payload(payload: str) -> str:
    return base64.b64decode(payload).decode("utf-8", errors="replace")


def _encode_payload(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("utf-8")


def _extract_parts(definition: Dict[str, Any]) -> List[Dict[str, Any]]:
    return (definition.get("definition") or {}).get("parts", [])


def _serialize_operation(op: Dict[str, Any]) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    if LOG_PATH.exists():
        doc = json.loads(LOG_PATH.read_text(encoding="utf-8"))
    else:
        doc = {"operations": []}
    doc["operations"].append(op)
    LOG_PATH.write_text(json.dumps(doc, indent=2), encoding="utf-8")


def _load_operation(operation_id: str) -> Dict[str, Any]:
    if not LOG_PATH.exists():
        raise ValueError("No operation log found.")
    doc = json.loads(LOG_PATH.read_text(encoding="utf-8"))
    for op in doc.get("operations", []):
        if op.get("operation_id") == operation_id:
            return op
    raise ValueError(f"Operation not found: {operation_id}")


async def _list_workspaces(client: FabricApiClient) -> List[Dict[str, Any]]:
    resp = await client.get("/workspaces")
    return resp.get("value", []) if isinstance(resp, dict) else []


async def _list_items(client: FabricApiClient, workspace_id: str) -> List[Dict[str, Any]]:
    resp = await client.get(f"/workspaces/{workspace_id}/items")
    return resp.get("value", []) if isinstance(resp, dict) else []


async def _get_definition(
    client: FabricApiClient,
    workspace_id: str,
    endpoint: str,
    item_id: str,
) -> Dict[str, Any]:
    return await client.post_with_lro(
        f"/workspaces/{workspace_id}/{endpoint}/{item_id}/getDefinition",
        json_data={},
    )


async def _update_definition(
    client: FabricApiClient,
    workspace_id: str,
    endpoint: str,
    item_id: str,
    parts: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return await client.post_with_lro(
        f"/workspaces/{workspace_id}/{endpoint}/{item_id}/updateDefinition",
        json_data={"definition": {"parts": parts}},
    )


def _build_patch(
    *,
    workspace_id: str,
    workspace_name: str,
    item_id: str,
    item_name: str,
    item_type: str,
    endpoint: str,
    definition: Dict[str, Any],
    old_token: str,
    new_token: str,
) -> Optional[PlannedPatch]:
    parts = _extract_parts(definition)
    if not parts:
        return None

    replacements = 0
    parts_after: List[Dict[str, Any]] = []
    parts_before: List[Dict[str, Any]] = []

    for p in parts:
        path = p.get("path")
        payload = p.get("payload")
        payload_type = p.get("payloadType", "InlineBase64")
        if not path or payload is None:
            continue

        current = {"path": path, "payload": payload, "payloadType": payload_type}
        parts_before.append(current)

        if str(payload_type).lower() == "inlinebase64":
            text = _decode_payload(payload)
            new_text, count = _replace_token(text, old_token, new_token)
            replacements += count
            if count > 0:
                parts_after.append(
                    {
                        "path": path,
                        "payload": _encode_payload(new_text),
                        "payloadType": "InlineBase64",
                    }
                )
            else:
                parts_after.append(current)
        else:
            parts_after.append(current)

    if replacements == 0:
        return None

    return PlannedPatch(
        workspace_id=workspace_id,
        workspace_name=workspace_name,
        item_id=item_id,
        item_name=item_name,
        item_type=item_type,
        endpoint=endpoint,
        replacements=replacements,
        parts_before=parts_before,
        parts_after=parts_after,
    )


def _select_workspaces(
    workspaces: List[Dict[str, Any]],
    *,
    workspace_prefixes: List[str],
    include_workspaces: List[str],
) -> List[Dict[str, Any]]:
    prefixes_l = [p.lower() for p in workspace_prefixes if p]
    includes_l = {w.lower() for w in include_workspaces if w}

    selected: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for ws in workspaces:
        ws_id = str(ws.get("id") or "")
        ws_name = (ws.get("displayName") or ws.get("name") or "").strip()
        ws_name_l = ws_name.lower()
        if not ws_id or not ws_name:
            continue

        hit = False
        if ws_name_l in includes_l:
            hit = True
        if any(ws_name_l.startswith(p) for p in prefixes_l):
            hit = True
        if hit and ws_id not in seen:
            selected.append(ws)
            seen.add(ws_id)
    return selected


async def _plan_patches(
    client: FabricApiClient,
    *,
    workspace_prefixes: List[str],
    include_workspaces: List[str],
    old_token: str,
    new_token: str,
) -> List[PlannedPatch]:
    all_ws = await _list_workspaces(client)
    selected_ws = _select_workspaces(
        all_ws,
        workspace_prefixes=workspace_prefixes,
        include_workspaces=include_workspaces,
    )
    patches: List[PlannedPatch] = []

    for ws in selected_ws:
        ws_id = str(ws.get("id"))
        ws_name = ws.get("displayName") or ws.get("name") or ws_id
        items = await _list_items(client, ws_id)
        for item in items:
            item_type = _normalize_item_type(str(item.get("type") or ""))
            endpoint = SUPPORTED_ENDPOINTS.get(item_type)
            if not endpoint:
                continue
            item_id = str(item.get("id") or "")
            item_name = item.get("displayName") or item.get("name") or item_id
            if not item_id:
                continue
            try:
                definition = await _get_definition(client, ws_id, endpoint, item_id)
                patch = _build_patch(
                    workspace_id=ws_id,
                    workspace_name=str(ws_name),
                    item_id=item_id,
                    item_name=str(item_name),
                    item_type=item_type,
                    endpoint=endpoint,
                    definition=definition,
                    old_token=old_token,
                    new_token=new_token,
                )
                if patch:
                    patches.append(patch)
            except Exception:
                # Best effort: skip items whose definitions are inaccessible.
                continue
    return patches


async def _apply_with_rollback(
    client: FabricApiClient,
    patches: List[PlannedPatch],
) -> Dict[str, Any]:
    applied: List[PlannedPatch] = []
    for p in patches:
        try:
            await _update_definition(
                client,
                p.workspace_id,
                p.endpoint,
                p.item_id,
                p.parts_after,
            )
            applied.append(p)
        except Exception as exc:
            rollback_errors: List[str] = []
            for done in reversed(applied):
                try:
                    await _update_definition(
                        client,
                        done.workspace_id,
                        done.endpoint,
                        done.item_id,
                        done.parts_before,
                    )
                except Exception as rb_exc:
                    rollback_errors.append(
                        f"{done.workspace_name}/{done.item_name}: {rb_exc}"
                    )
            return {
                "success": False,
                "error": str(exc),
                "applied_before_failure": len(applied),
                "rolled_back": len(rollback_errors) == 0,
                "rollback_errors": rollback_errors,
            }
    return {
        "success": True,
        "applied_count": len(applied),
        "rolled_back": False,
        "rollback_errors": [],
    }


async def run_autofix(args: argparse.Namespace) -> Dict[str, Any]:
    cfg = FabricAuthConfig.from_env()
    operation_id = str(uuid4())

    async with FabricApiClient(cfg) as client:
        patches = await _plan_patches(
            client,
            workspace_prefixes=args.workspace_prefix,
            include_workspaces=args.include_workspace,
            old_token=args.old_token,
            new_token=args.new_token,
        )

        total_replacements = sum(p.replacements for p in patches)
        plan = {
            "operation_id": operation_id,
            "generated_at": _utc_now(),
            "old_token": args.old_token,
            "new_token": args.new_token,
            "target_items": len(patches),
            "total_replacements": total_replacements,
            "items": [
                {
                    "workspace": p.workspace_name,
                    "item_name": p.item_name,
                    "item_type": p.item_type,
                    "replacements": p.replacements,
                }
                for p in patches
            ],
        }

        if not args.apply:
            return {"mode": "dry_run", "plan": plan}

        apply_result = await _apply_with_rollback(client, patches)
        op_record = {
            "operation_id": operation_id,
            "timestamp_utc": _utc_now(),
            "old_token": args.old_token,
            "new_token": args.new_token,
            "scope": {
                "workspace_prefix": args.workspace_prefix,
                "include_workspace": args.include_workspace,
            },
            "apply_result": apply_result,
            "applied_items": [
                {
                    "workspace_id": p.workspace_id,
                    "workspace_name": p.workspace_name,
                    "item_id": p.item_id,
                    "item_name": p.item_name,
                    "item_type": p.item_type,
                    "endpoint": p.endpoint,
                    "replacements": p.replacements,
                    "parts_before": p.parts_before,
                }
                for p in patches
            ],
        }
        _serialize_operation(op_record)
        return {
            "mode": "apply",
            "plan": plan,
            "apply_result": apply_result,
            "operation_id": operation_id,
        }


async def run_rollback(args: argparse.Namespace) -> Dict[str, Any]:
    op = _load_operation(args.operation_id)
    cfg = FabricAuthConfig.from_env()
    restored = 0
    errors: List[str] = []

    async with FabricApiClient(cfg) as client:
        for item in reversed(op.get("applied_items", [])):
            try:
                await _update_definition(
                    client,
                    item["workspace_id"],
                    item["endpoint"],
                    item["item_id"],
                    item.get("parts_before", []),
                )
                restored += 1
            except Exception as exc:
                errors.append(
                    f"{item.get('workspace_name')}/{item.get('item_name')}: {exc}"
                )

    return {
        "operation_id": args.operation_id,
        "rolled_back_items": restored,
        "errors": errors,
        "success": len(errors) == 0,
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Transactional downstream reference auto-fix for Fabric definitions."
    )
    p.add_argument(
        "--workspace-prefix",
        action="append",
        default=["ENT"],
        help="Workspace prefix to include (repeatable). Default: ENT",
    )
    p.add_argument(
        "--include-workspace",
        action="append",
        default=[],
        help="Explicit workspace name to include (repeatable).",
    )
    p.add_argument("--old-token", help="Token to replace.")
    p.add_argument("--new-token", help="Replacement token.")
    p.add_argument("--apply", action="store_true", help="Apply changes.")
    p.add_argument("--rollback", action="store_true", help="Rollback an operation.")
    p.add_argument("--operation-id", help="Operation ID for rollback.")

    args = p.parse_args()
    if args.rollback:
        if not args.operation_id:
            p.error("--rollback requires --operation-id")
        return args

    if not args.old_token or not args.new_token:
        p.error("--old-token and --new-token are required unless --rollback is used")
    return args


async def main_async(args: argparse.Namespace) -> int:
    if args.rollback:
        res = await run_rollback(args)
    else:
        res = await run_autofix(args)
    print(json.dumps(res, indent=2))
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(main_async(parse_args())))


if __name__ == "__main__":
    main()

