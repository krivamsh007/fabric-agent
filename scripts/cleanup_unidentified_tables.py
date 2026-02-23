#!/usr/bin/env python
"""
Cleanup Lakehouse Tables/Unidentified folders in OneLake.

Use this if you ended up with Delta folders under:
  Tables/Unidentified/<name>

Those folders are Delta files that are NOT registered as Lakehouse tables.
They can confuse validation + shortcut creation.

After cleanup, re-run the seeding notebooks that use saveAsTable() so the tables
show up under Tables normally.

Usage:
  # List unidentified folders (default: ENT_DataPlatform_DEV / Bronze_Landing)
  python scripts/cleanup_unidentified_tables.py --list-only

  # Delete specific unidentified folders
  python scripts/cleanup_unidentified_tables.py --tables raw_customer_events raw_inventory_snapshots --yes

  # Delete ALL folders under Tables/Unidentified
  python scripts/cleanup_unidentified_tables.py --all --yes
"""

from __future__ import annotations

import argparse
import asyncio
import os
from typing import List, Optional

from dotenv import load_dotenv
from loguru import logger

from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from fabric_agent.core.config import FabricAuthConfig
from fabric_agent.api.fabric_client import FabricApiClient


def _get_storage_credential_from_env():
    use_interactive = os.getenv("USE_INTERACTIVE_AUTH", "false").strip().lower() in ("1","true","yes","y")
    if use_interactive:
        return DefaultAzureCredential(exclude_interactive_browser_credential=False)

    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    if not (tenant_id and client_id and client_secret):
        raise RuntimeError("Missing AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET in .env")
    return ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)


async def get_workspace_id(client: FabricApiClient, workspace_name: str) -> str:
    resp = await client.get("/workspaces")
    for ws in resp.get("value", []):
        if ws.get("displayName") == workspace_name:
            return ws.get("id")
    raise RuntimeError(f"Workspace not found: {workspace_name}")

async def get_lakehouse_id(client: FabricApiClient, workspace_id: str, lakehouse_name: str) -> str:
    resp = await client.get(f"/workspaces/{workspace_id}/lakehouses")
    for lh in resp.get("value", []):
        if lh.get("displayName") == lakehouse_name:
            return lh.get("id")
    raise RuntimeError(f"Lakehouse not found: {lakehouse_name}")


def list_unidentified_folders(workspace_id: str, lakehouse_id: str) -> List[str]:
    cred = _get_storage_credential_from_env()
    svc = DataLakeServiceClient(account_url="https://onelake.dfs.fabric.microsoft.com", credential=cred)
    fs = svc.get_file_system_client(file_system=workspace_id)

    prefix = f"{lakehouse_id}/Tables/Unidentified/"
    names = set()

    try:
        paths = fs.get_paths(path=prefix)
        for p in paths:
            # p.name like: <lakehouseId>/Tables/Unidentified/<table>/...
            rest = p.name[len(prefix):]
            if not rest:
                continue
            first = rest.split("/", 1)[0]
            if first:
                names.add(first)
    except Exception as e:
        logger.warning(f"Could not list {prefix}: {e}")
        return []

    return sorted(names)


def delete_unidentified_folder(workspace_id: str, lakehouse_id: str, table_name: str) -> None:
    cred = _get_storage_credential_from_env()
    svc = DataLakeServiceClient(account_url="https://onelake.dfs.fabric.microsoft.com", credential=cred)
    fs = svc.get_file_system_client(file_system=workspace_id)

    dir_path = f"{lakehouse_id}/Tables/Unidentified/{table_name}"
    dir_client = fs.get_directory_client(dir_path)

    # delete_directory supports recursive delete in newer SDK versions via parameter recursive=True
    try:
        dir_client.delete_directory()
    except TypeError:
        # older signature
        dir_client.delete_directory(recursive=True)
    except Exception as e:
        raise RuntimeError(f"Failed to delete {dir_path}: {e}") from e


async def main_async(args: argparse.Namespace) -> int:
    load_dotenv()

    auth = FabricAuthConfig.from_env()
    client = FabricApiClient(auth)
    await client.initialize()

    try:
        ws_id = await get_workspace_id(client, args.workspace)
        lh_id = await get_lakehouse_id(client, ws_id, args.lakehouse)

        found = list_unidentified_folders(ws_id, lh_id)
        if not found:
            logger.info("No folders found under Tables/Unidentified.")
            return 0

        logger.info(f"Found {len(found)} folder(s) under Tables/Unidentified: {found}")

        if args.list_only:
            return 0

        if not args.yes:
            logger.error("Refusing to delete without --yes")
            return 2

        to_delete: List[str]
        if args.all:
            to_delete = found
        else:
            to_delete = args.tables or []

        if not to_delete:
            logger.error("Nothing selected to delete. Use --all or --tables ...")
            return 2

        for t in to_delete:
            if t not in found:
                logger.warning(f"Skipping {t} (not found under Unidentified)")
                continue
            logger.warning(f"Deleting Tables/Unidentified/{t} ...")
            await asyncio.to_thread(delete_unidentified_folder, ws_id, lh_id, t)

        logger.success("✅ Cleanup complete.")
        logger.info("Next: re-run the seeding notebooks that use saveAsTable().")
        return 0

    finally:
        await client.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Delete Tables/Unidentified folders in a Lakehouse")
    p.add_argument("--workspace", default="ENT_DataPlatform_DEV", help="Workspace display name")
    p.add_argument("--lakehouse", default="Bronze_Landing", help="Lakehouse display name")
    p.add_argument("--list-only", action="store_true", help="Only list Unidentified folders")
    p.add_argument("--all", action="store_true", help="Delete ALL folders under Tables/Unidentified")
    p.add_argument("--tables", nargs="*", help="Specific folder names to delete")
    p.add_argument("--yes", action="store_true", help="Confirm deletion")
    return p.parse_args()


def main() -> None:
    raise SystemExit(asyncio.run(main_async(parse_args())))


if __name__ == "__main__":
    main()
