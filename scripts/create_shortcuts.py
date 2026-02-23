#!/usr/bin/env python
"""
Create OneLake shortcuts for the Enterprise demo (after tables exist).

Usage:
  python scripts/create_shortcuts.py
  python scripts/create_shortcuts.py --prefix ENT --dry-run

Notes:
- Source table existence is validated via Lakehouse List Tables API (preview) which returns {data:[...]} and supports pagination. :contentReference[oaicite:2]{index=2}
- Shortcut creation uses shortcutConflictPolicy=CreateOrOverwrite to be idempotent. :contentReference[oaicite:3]{index=3}
"""

from __future__ import annotations

import argparse
import asyncio
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from loguru import logger

from fabric_agent.core.config import FabricAuthConfig
from fabric_agent.api.fabric_client import FabricApiClient, FabricApiError

# Reuse enterprise config (shortcuts declared there)
from scripts.bootstrap_enterprise_domains import build_enterprise_config, DomainConfig, WorkspaceConfig


# -----------------------------
# Generic helpers
# -----------------------------

async def list_workspaces(client: FabricApiClient) -> List[dict]:
    resp = await client.get("/workspaces")
    return resp.get("value", []) or []


def find_by_display_name(items: List[dict], name: str) -> Optional[dict]:
    for it in items:
        if it.get("displayName") == name:
            return it
    return None


async def get_workspace_id(client: FabricApiClient, ws_name: str) -> str:
    wss = await list_workspaces(client)
    ws = find_by_display_name(wss, ws_name)
    if not ws:
        raise RuntimeError(f"Workspace not found: {ws_name}")
    return ws["id"]


async def get_lakehouse_id(client: FabricApiClient, ws_id: str, lh_name: str) -> str:
    resp = await client.get(f"/workspaces/{ws_id}/lakehouses")
    lh = find_by_display_name(resp.get("value", []) or [], lh_name)
    if not lh:
        raise RuntimeError(f"Lakehouse not found: {lh_name} (workspace {ws_id})")
    return lh["id"]


# -----------------------------
# LAKEHOUSE TABLES (CRITICAL FIX)
# -----------------------------
async def list_lakehouse_tables(client: FabricApiClient, ws_id: str, lh_id: str, max_results: int = 100) -> List[str]:
    """
    List registered tables in a lakehouse.

    The List Tables API (preview) returns tables under `data` and supports pagination via continuationToken. :contentReference[oaicite:4]{index=4}
    """
    names: List[str] = []
    token: Optional[str] = None

    while True:
        qs = f"?maxResults={max_results}"
        if token:
            qs += f"&continuationToken={token}"

        raw = await client.get_raw(f"/workspaces/{ws_id}/lakehouses/{lh_id}/tables{qs}")
        raw.raise_for_status()
        payload = raw.json() if raw.content else {}

        page = payload.get("data") or payload.get("value") or []
        if isinstance(page, list):
            for t in page:
                n = t.get("name") or t.get("displayName")
                if n:
                    names.append(n)

        token = payload.get("continuationToken")
        if not token:
            break

    return names


# -----------------------------
# SHORTCUTS LIST/CREATE
# -----------------------------
async def list_shortcuts(client: FabricApiClient, ws_id: str, item_id: str) -> List[dict]:
    """
    List shortcuts for a lakehouse item.
    Endpoint supports continuationToken and parentPath, but we only need full list for small volumes. :contentReference[oaicite:5]{index=5}
    """
    raw = await client.get_raw(f"/workspaces/{ws_id}/items/{item_id}/shortcuts")
    raw.raise_for_status()
    payload = raw.json() if raw.content else {}
    # This endpoint returns `value` (per docs and typical Fabric list patterns)
    return payload.get("value", []) or []


async def create_or_update_shortcut(
    client: FabricApiClient,
    dest_ws_id: str,
    dest_lh_id: str,
    src_ws_id: str,
    src_lh_id: str,
    shortcut_name: str,
    src_table: str,
    *,
    dry_run: bool = False,
) -> Tuple[bool, str]:
    """
    Create or overwrite a shortcut pointing to OneLake Delta table under Tables/<src_table>.
    Uses shortcutConflictPolicy=CreateOrOverwrite for idempotency. :contentReference[oaicite:6]{index=6}
    """
    payload = {
        "path": "Tables",
        "name": shortcut_name,
        "target": {
            "oneLake": {
                "workspaceId": src_ws_id,
                "itemId": src_lh_id,
                "path": f"Tables/{src_table}",
            }
        },
    }

    if dry_run:
        return True, f"[DRY RUN] Would CreateOrOverwrite shortcut '{shortcut_name}' -> {src_ws_id}/{src_lh_id}/Tables/{src_table}"

    # Create Shortcut API: POST /workspaces/{workspaceId}/items/{itemId}/shortcuts?shortcutConflictPolicy=CreateOrOverwrite :contentReference[oaicite:7]{index=7}
    url = f"/workspaces/{dest_ws_id}/items/{dest_lh_id}/shortcuts?shortcutConflictPolicy=CreateOrOverwrite"
    resp = await client.post_raw(url, json=payload)

    # If Fabric returns LRO for any reason, wait for completion
    if resp.status_code == 202:
        await client.wait_for_lro(resp)

    # 200 updated, 201 created are expected; anything else should raise
    resp.raise_for_status()
    return True, f"Shortcut '{shortcut_name}' created/updated in lakehouse {dest_lh_id}"


def collect_shortcut_workspaces(domains: List[DomainConfig]) -> List[WorkspaceConfig]:
    out: List[WorkspaceConfig] = []
    for d in domains:
        for ws in d.workspaces:
            if ws.shortcuts:
                out.append(ws)
    return out


# -----------------------------
# Main
# -----------------------------
async def main_async(args: argparse.Namespace) -> int:
    load_dotenv()

    domains = build_enterprise_config(prefix=args.prefix)
    target_workspaces = collect_shortcut_workspaces(domains)

    if not target_workspaces:
        logger.warning("No shortcuts configured in enterprise config.")
        return 0

    auth = FabricAuthConfig.from_env()
    async with FabricApiClient(auth) as client:
        try:
            # Resolve workspace IDs
            ws_ids: Dict[str, str] = {}
            for ws in target_workspaces:
                ws_ids[ws.name] = await get_workspace_id(client, ws.name)

            # Resolve source workspaces referenced by shortcuts
            for ws in target_workspaces:
                for sc in ws.shortcuts:
                    if sc.source_workspace not in ws_ids:
                        ws_ids[sc.source_workspace] = await get_workspace_id(client, sc.source_workspace)

            # Resolve lakehouse IDs (workspaceName, lakehouseName) -> lakehouseId
            lh_ids: Dict[Tuple[str, str], str] = {}

            # Destination lakehouse (first lakehouse in config)
            for ws in target_workspaces:
                if not ws.lakehouses:
                    raise RuntimeError(f"Workspace '{ws.name}' has shortcuts but no lakehouses in config.")
                dest_ws_id = ws_ids[ws.name]
                dest_lh_name = ws.lakehouses[0].name
                lh_ids[(ws.name, dest_lh_name)] = await get_lakehouse_id(client, dest_ws_id, dest_lh_name)

            # Source lakehouses
            for ws in target_workspaces:
                for sc in ws.shortcuts:
                    key = (sc.source_workspace, sc.source_lakehouse)
                    if key not in lh_ids:
                        src_ws_id = ws_ids[sc.source_workspace]
                        lh_ids[key] = await get_lakehouse_id(client, src_ws_id, sc.source_lakehouse)

            # Cache table lists per source lakehouse (avoid re-calling for each shortcut)
            table_cache: Dict[Tuple[str, str], set] = {}

            created = 0
            skipped = 0

            for ws in target_workspaces:
                dest_ws_id = ws_ids[ws.name]
                dest_lh_name = ws.lakehouses[0].name
                dest_lh_id = lh_ids[(ws.name, dest_lh_name)]

                # Existing shortcuts (not required, but nice for logging)
                existing = await list_shortcuts(client, dest_ws_id, dest_lh_id)
                existing_names = {s.get("name") for s in existing if s.get("name")}
                logger.info(f"{ws.name}/{dest_lh_name}: existingShortcuts={len(existing_names)}")

                for sc in ws.shortcuts:
                    src_ws_id = ws_ids[sc.source_workspace]
                    src_lh_id = lh_ids[(sc.source_workspace, sc.source_lakehouse)]

                    cache_key = (sc.source_workspace, sc.source_lakehouse)
                    if cache_key not in table_cache:
                        names = await list_lakehouse_tables(client, src_ws_id, src_lh_id)
                        table_cache[cache_key] = set(names)
                        logger.info(
                            f"Source {sc.source_workspace}/{sc.source_lakehouse}: tables={len(names)}"
                        )

                    if sc.source_table not in table_cache[cache_key]:
                        skipped += 1
                        logger.warning(
                            f"Skipping shortcut '{sc.name}' in {ws.name}/{dest_lh_name}: "
                            f"source table missing: {sc.source_workspace}/{sc.source_lakehouse}.{sc.source_table}"
                        )
                        continue

                    ok, msg = await create_or_update_shortcut(
                        client,
                        dest_ws_id,
                        dest_lh_id,
                        src_ws_id,
                        src_lh_id,
                        sc.name,
                        sc.source_table,
                        dry_run=args.dry_run,
                    )
                    if ok:
                        created += 1
                        logger.info(msg)

            logger.success(f"✅ Shortcuts done. created/updated={created} skipped={skipped}")
            return 0

        except FabricApiError as e:
            logger.error(f"Fabric API error: {e} | body={getattr(e, 'response_body', None)}")
            return 2
        except Exception:
            logger.exception("Shortcut creation failed")
            return 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Create OneLake shortcuts for Enterprise demo")
    p.add_argument("--prefix", default="ENT", help="Prefix used in workspace names (default: ENT)")
    p.add_argument("--dry-run", action="store_true", help="Print what would be done without making changes")
    return p.parse_args()


def main() -> None:
    raise SystemExit(asyncio.run(main_async(parse_args())))


if __name__ == "__main__":
    main()
