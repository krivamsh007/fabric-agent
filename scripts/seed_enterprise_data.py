#!/usr/bin/env python
"""
Seed the Enterprise demo with data (local CSV -> OneLake Files).

Why this script exists
----------------------
In Fabric, the safest pattern is:

1) Upload raw/seed files into the Lakehouse **Files/** area
2) Use a Notebook (Spark) to create/register Lakehouse **Tables/** via saveAsTable()

If you write Delta files directly under Tables/ (or use APIs that only write files but don't
register tables), the Lakehouse UI may show them under **Tables → Unidentified**.

This script focuses on step (1): uploading seed CSVs into each lakehouse:

  Files/seed/<layer>/*.csv

Then you run the notebooks that the bootstrap script created:
  - 01_Ingest_Bronze (Bronze_Landing)
  - 02_Transform_Silver / 03_Build_Dimensions / 04_Build_Facts (Silver_Curated)
  - 05_Aggregate_Gold (Gold_Published)
  - 06_Data_Quality_Checks

Optional:
- If you *really* want to use the Lakehouse "Load Table" API, pass --load-api.
  (Not recommended if you're seeing Tables→Unidentified.)

Prereqs
-------
- Workspaces + lakehouses must exist (run bootstrap first):
    python scripts/bootstrap_enterprise_domains.py --capacity-id "<CAPACITY_GUID>" --yes --skip-shortcuts
- Your identity must have Contributor+ to the target workspaces.
- If uploading via service principal, set AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET.

Usage
-----
  # Upload seed CSVs to Files/seed/... (recommended)
  python scripts/seed_enterprise_data.py

  # Skip upload (if you uploaded via UI already)
  python scripts/seed_enterprise_data.py --skip-upload

  # Upload + also call Load Table API (not recommended)
  python scripts/seed_enterprise_data.py --load-api
"""

import argparse
import asyncio
import os
from dataclasses import dataclass
from typing import Dict, List

from dotenv import load_dotenv
from loguru import logger

from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from fabric_agent.core.config import FabricAuthConfig
from fabric_agent.api.fabric_client import FabricApiClient, FabricApiError


# ----------------------------
# Config
# ----------------------------

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "seed_data")

@dataclass
class TableSpec:
    table_name: str
    csv_file: str  # relative to DATA_DIR/<layer>/
    layer: str     # bronze/silver/gold


TABLES_BY_LAKEHOUSE: Dict[str, List[TableSpec]] = {
    "Bronze_Landing": [
        TableSpec("raw_sales_transactions", "raw_sales_transactions.csv", "bronze"),
        TableSpec("raw_inventory_snapshots", "raw_inventory_snapshots.csv", "bronze"),
        TableSpec("raw_customer_events", "raw_customer_events.csv", "bronze"),
    ],
    "Silver_Curated": [
        TableSpec("dim_date", "dim_date.csv", "silver"),
        TableSpec("dim_customer", "dim_customer.csv", "silver"),
        TableSpec("dim_product", "dim_product.csv", "silver"),
        TableSpec("dim_store", "dim_store.csv", "silver"),
        TableSpec("fact_sales", "fact_sales.csv", "silver"),
    ],
    "Gold_Published": [
        TableSpec("agg_daily_sales", "agg_daily_sales.csv", "gold"),
        TableSpec("agg_customer_360", "agg_customer_360.csv", "gold"),
        TableSpec("agg_inventory_health", "agg_inventory_health.csv", "gold"),
    ],
}

TARGET_WORKSPACES = [
    "ENT_DataPlatform_DEV",
    "ENT_DataPlatform_PROD",
]


# ----------------------------
# OneLake upload (ADLS Gen2)
# ----------------------------

def _get_storage_credential_from_env():
    """Return an Azure credential usable for OneLake (ADLS Gen2).

    - If USE_INTERACTIVE_AUTH=true, uses DefaultAzureCredential (browser/device login).
    - Otherwise uses client secret credentials from AZURE_* env vars.
    """
    use_interactive = os.getenv("USE_INTERACTIVE_AUTH", "false").strip().lower() in ("1","true","yes","y")
    if use_interactive:
        return DefaultAzureCredential(exclude_interactive_browser_credential=False)

    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    if not (tenant_id and client_id and client_secret):
        raise RuntimeError("Missing AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET in .env")
    return ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

def upload_csv_to_onelake(workspace_id: str, lakehouse_id: str, layer: str, local_path: str) -> str:
    """
    Upload a CSV into OneLake under the lakehouse item:
      {workspaceId}/{lakehouseId}/Files/seed/<layer>/<filename>.csv

    Returns the lakehouse-relative path used by APIs/notebooks:
      Files/seed/<layer>/<filename>.csv
    """
    filename = os.path.basename(local_path)
    rel_path = f"Files/seed/{layer}/{filename}"

    cred = _get_storage_credential_from_env()
    svc = DataLakeServiceClient(account_url="https://onelake.dfs.fabric.microsoft.com", credential=cred)

    fs_client = svc.get_file_system_client(file_system=workspace_id)
    dir_client = fs_client.get_directory_client(f"{lakehouse_id}/Files/seed/{layer}")
    try:
        dir_client.create_directory()
    except Exception:
        pass

    file_client = dir_client.get_file_client(filename)
    with open(local_path, "rb") as f:
        file_client.upload_data(f.read(), overwrite=True)

    return rel_path


# ----------------------------
# Fabric helpers
# ----------------------------

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
    raise RuntimeError(f"Lakehouse not found: {lakehouse_name} (workspace {workspace_id})")

async def load_table_from_csv(
    client: FabricApiClient,
    workspace_id: str,
    lakehouse_id: str,
    table_name: str,
    rel_path: str,
    *,
    lro_poll_seconds: float = 3.0,
    max_polls: int = 400,
) -> None:
    """(Optional) Lakehouse Load Table API (CSV -> Delta)."""
    body = {
        "relativePath": rel_path,
        "pathType": "File",
        "mode": "Overwrite",
        "recursive": False,
        "formatOptions": {"format": "Csv", "header": True, "delimiter": ","},
    }
    logger.info(f"[Load API] Loading table {table_name} from {rel_path}")
    await client.post_with_lro(
        f"/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables/{table_name}/load",
        json_data=body,
        lro_poll_seconds=lro_poll_seconds,
        max_polls=max_polls,
    )


# ----------------------------
# Main
# ----------------------------

async def main_async(args: argparse.Namespace) -> int:
    load_dotenv()

    auth = FabricAuthConfig.from_env()
    client = FabricApiClient(auth)
    await client.initialize()

    try:
        if not os.path.isdir(DATA_DIR):
            raise RuntimeError(f"Seed folder not found: {DATA_DIR}")

        for ws_name in TARGET_WORKSPACES:
            logger.info(f"Seeding workspace: {ws_name}")
            ws_id = await get_workspace_id(client, ws_name)

            for lakehouse_name, table_specs in TABLES_BY_LAKEHOUSE.items():
                lh_id = await get_lakehouse_id(client, ws_id, lakehouse_name)

                for spec in table_specs:
                    local_csv = os.path.join(DATA_DIR, spec.layer, spec.csv_file)
                    if not os.path.isfile(local_csv):
                        raise RuntimeError(f"Missing seed file: {local_csv}")

                    if not args.skip_upload:
                        logger.info(f"Uploading {local_csv} -> OneLake {ws_id}/{lh_id}")
                        rel_path = await asyncio.to_thread(upload_csv_to_onelake, ws_id, lh_id, spec.layer, local_csv)
                        logger.info(f"Uploaded -> {rel_path}")
                    else:
                        rel_path = f"Files/seed/{spec.layer}/{spec.csv_file}"
                        logger.warning(f"Skipping upload. Expecting file in Lakehouse Files at: {rel_path}")

                    if args.load_api:
                        await load_table_from_csv(client, ws_id, lh_id, spec.table_name, rel_path)

        logger.success("✅ Seed upload complete.")

        if not args.load_api:
            logger.info(
                "\nNext step:\n"
                "1) In Fabric UI, open each DataPlatform workspace\n"
                "2) Run notebook pipeline (or run notebooks manually):\n"
                "   - 01_Ingest_Bronze (Bronze_Landing)\n"
                "   - 02_Transform_Silver / 03_Build_Dimensions / 04_Build_Facts (Silver_Curated)\n"
                "   - 05_Aggregate_Gold (Gold_Published)\n"
                "3) Then create shortcuts:\n"
                "   python scripts/create_shortcuts.py\n"
            )

        return 0

    except FabricApiError as e:
        logger.error(f"Fabric API error while seeding: {e} | body={getattr(e,'response_body',None)}")
        logger.error("Common causes: workspace not on Fabric capacity, identity not Contributor+, tenant blocks SP for Fabric APIs.")
        return 2
    except Exception as e:
        logger.exception(e)
        return 1
    finally:
        await client.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--skip-upload", action="store_true", help="Skip OneLake upload (assumes CSVs already exist under Files/seed/...) ")
    p.add_argument("--load-api", action="store_true", help="Also call Lakehouse Load Table API (not recommended if you see Tables→Unidentified). ")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    raise SystemExit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
