#!/usr/bin/env python3
"""
COMPLETE ENTERPRISE BOOTSTRAP
=============================

A single script that:
1. Creates Fabric workspaces/lakehouses/notebooks/pipelines/semantic models
2. Uploads seed data to OneLake
3. Creates Delta tables from CSVs
4. Validates the environment

This combines:
- bootstrap_enterprise_domains.py (asset creation)
- seed_enterprise_data.py (data loading)
- validate_enterprise_environment.py (validation)

Usage:
    # Dry run (preview what will be created)
    python scripts/bootstrap_complete.py --workspace "your-workspace" --dry-run

    # Full execution
    python scripts/bootstrap_complete.py --workspace "your-workspace" --capacity-id "xxx" --yes

    # Create separate workspaces per domain
    python scripts/bootstrap_complete.py --prefix ENT --capacity-id "xxx" --yes

Author: Fabric Agent Team
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger

# Project imports
from fabric_agent.core.config import FabricAuthConfig
from fabric_agent.api.fabric_client import FabricApiClient, FabricApiError

# Import from existing scripts
from scripts.bootstrap_enterprise_domains import (
    build_enterprise_config,
    generate_enterprise_measures,
    EnterpriseBootstrapper,
)


# =============================================================================
# Data Seeding Configuration
# =============================================================================

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "seed_data"

@dataclass
class TableSpec:
    table_name: str
    csv_file: str
    layer: str


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


# =============================================================================
# OneLake Upload Functions
# =============================================================================

def get_storage_credential():
    """Get Azure credential for OneLake access."""
    from azure.identity import ClientSecretCredential, DefaultAzureCredential
    
    use_interactive = os.getenv("USE_INTERACTIVE_AUTH", "false").strip().lower() in ("1", "true", "yes")
    if use_interactive:
        return DefaultAzureCredential(exclude_interactive_browser_credential=False)
    
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    
    if not all([tenant_id, client_id, client_secret]):
        raise RuntimeError("Missing Azure credentials in .env")
    
    return ClientSecretCredential(tenant_id, client_id, client_secret)


def upload_csv_to_onelake(workspace_id: str, lakehouse_id: str, layer: str, local_path: str) -> str:
    """Upload CSV to OneLake and return relative path."""
    from azure.storage.filedatalake import DataLakeServiceClient
    
    filename = os.path.basename(local_path)
    rel_path = f"Files/seed/{layer}/{filename}"
    
    cred = get_storage_credential()
    svc = DataLakeServiceClient(
        account_url="https://onelake.dfs.fabric.microsoft.com",
        credential=cred
    )
    
    fs_client = svc.get_file_system_client(file_system=workspace_id)
    dir_client = fs_client.get_directory_client(f"{lakehouse_id}/Files/seed/{layer}")
    
    try:
        dir_client.create_directory()
    except Exception:
        pass  # Already exists
    
    file_client = dir_client.get_file_client(filename)
    with open(local_path, "rb") as f:
        file_client.upload_data(f.read(), overwrite=True)
    
    logger.info(f"Uploaded {filename} to {rel_path}")
    return rel_path


async def load_table_from_csv(
    client: FabricApiClient,
    workspace_id: str,
    lakehouse_id: str,
    table_name: str,
    relative_path: str,
) -> bool:
    """Load CSV into Delta table using Fabric API."""
    payload = {
        "relativePath": relative_path,
        "pathType": "File",
        "mode": "Overwrite",
        "formatOptions": {
            "format": "Csv",
            "header": True,
            "delimiter": ","
        }
    }
    
    try:
        await client.post_with_lro(
            f"/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables/{table_name}/load",
            json_data=payload,
            lro_poll_seconds=5.0,
            max_polls=60,
        )
        logger.info(f"Created Delta table: {table_name}")
        return True
    except FabricApiError as e:
        logger.error(f"Failed to load table {table_name}: {e}")
        return False


# =============================================================================
# Validation Functions
# =============================================================================

async def validate_environment(client: FabricApiClient, workspace_ids: Dict[str, str]) -> Dict:
    """Validate the created environment."""
    results = {
        "workspaces": 0,
        "lakehouses": 0,
        "tables": 0,
        "notebooks": 0,
        "pipelines": 0,
        "semantic_models": 0,
        "issues": []
    }
    
    for ws_name, ws_id in workspace_ids.items():
        results["workspaces"] += 1
        
        # Check lakehouses
        try:
            lh_resp = await client.get(f"/workspaces/{ws_id}/lakehouses")
            lakehouses = lh_resp.get("value", [])
            results["lakehouses"] += len(lakehouses)
            
            # Check tables in each lakehouse
            for lh in lakehouses:
                lh_id = lh.get("id")
                try:
                    tables_resp = await client.get(f"/workspaces/{ws_id}/lakehouses/{lh_id}/tables")
                    results["tables"] += len(tables_resp.get("value", []))
                except Exception:
                    pass
        except Exception as e:
            results["issues"].append(f"Error checking lakehouses in {ws_name}: {e}")
        
        # Check notebooks
        try:
            nb_resp = await client.get(f"/workspaces/{ws_id}/notebooks")
            results["notebooks"] += len(nb_resp.get("value", []))
        except Exception:
            pass
        
        # Check pipelines
        try:
            pl_resp = await client.get(f"/workspaces/{ws_id}/dataPipelines")
            results["pipelines"] += len(pl_resp.get("value", []))
        except Exception:
            pass
        
        # Check semantic models
        try:
            sm_resp = await client.get(f"/workspaces/{ws_id}/semanticModels")
            results["semantic_models"] += len(sm_resp.get("value", []))
        except Exception:
            pass
    
    return results


# =============================================================================
# Main Execution
# =============================================================================

async def run_complete_bootstrap(args):
    """Execute complete bootstrap: assets + data + validation."""
    
    # Load environment
    from dotenv import load_dotenv
    load_dotenv()
    
    print("\n" + "=" * 70)
    print("  COMPLETE ENTERPRISE FABRIC BOOTSTRAP")
    print("=" * 70)
    
    # Phase 1: Build configuration
    print("\n📋 PHASE 1: Building configuration...")
    domains = build_enterprise_config(prefix=args.prefix)
    measures = generate_enterprise_measures()
    
    total_items = 0
    for domain in domains:
        for ws in domain.workspaces:
            items = len(ws.lakehouses) + len(ws.notebooks) + len(ws.pipelines) + len(ws.semantic_models)
            total_items += items
            print(f"   {ws.name}: {items} items")
    
    print(f"\n   Total: {len(domains)} domains, {total_items} items, {len(measures)} measures")
    
    # Check seed data exists
    print("\n📁 PHASE 2: Checking seed data...")
    missing_files = []
    for lakehouse, specs in TABLES_BY_LAKEHOUSE.items():
        for spec in specs:
            csv_path = DATA_DIR / spec.layer / spec.csv_file
            if not csv_path.exists():
                missing_files.append(str(csv_path))
            else:
                print(f"   ✓ {spec.csv_file}")
    
    if missing_files:
        print(f"\n❌ Missing seed data files:")
        for f in missing_files:
            print(f"   - {f}")
        print("\nPlease ensure data/seed_data/ contains all required CSVs.")
        return
    
    if args.dry_run:
        print("\n[DRY RUN] Would create all assets and load seed data.")
        print("Run without --dry-run to execute.")
        return
    
    # Confirm
    if not args.yes:
        confirm = input("\nProceed with complete bootstrap? (yes/no): ")
        if confirm.lower() != "yes":
            print("Aborted.")
            return
    
    # Phase 3: Create Fabric assets
    print("\n🏗️  PHASE 3: Creating Fabric assets...")
    config = FabricAuthConfig.from_env()
    
    async with FabricApiClient(config) as client:
        bootstrapper = EnterpriseBootstrapper(
            client,
            dry_run=False,
            fixed_workspace=getattr(args, "workspace", None),
            capacity_id=getattr(args, "capacity_id", None),
            enable_reports=getattr(args, "enable_reports", False),
        )
        
        if args.workspace:
            await bootstrapper.set_fixed_workspace_by_name(args.workspace)
        
        results = []
        for domain in domains:
            if args.domain and domain.name != args.domain:
                continue
            
            print(f"\n   Creating domain: {domain.name}")
            result = await bootstrapper.bootstrap_domain(domain)
            results.append(result)
        
        # Phase 4: Upload seed data (recommended: Files/seed) and optionally create tables
        print("\n📤 PHASE 4: Uploading seed CSVs to OneLake Files/seed/...")

        # Get workspace and lakehouse IDs from bootstrapper
        for ws_name, ws_data in bootstrapper.created_items.items():
            if "DataPlatform" not in ws_name:
                continue  # Only seed DataPlatform workspaces

            ws_id = ws_data["workspace_id"]
            lakehouse_ids = ws_data.get("lakehouses", {})

            print(f"\n   Workspace: {ws_name}")

            for lakehouse_name, specs in TABLES_BY_LAKEHOUSE.items():
                lh_id = lakehouse_ids.get(lakehouse_name)
                if not lh_id:
                    print(f"   ⚠️  Lakehouse not found: {lakehouse_name}")
                    continue

                print(f"   Lakehouse: {lakehouse_name}")

                for spec in specs:
                    csv_path = DATA_DIR / spec.layer / spec.csv_file

                    try:
                        # Upload CSV to Files/seed/<layer>/
                        rel_path = upload_csv_to_onelake(ws_id, lh_id, spec.layer, str(csv_path))
                        print(f"      ✓ uploaded: {spec.csv_file} -> {rel_path}")

                        # Optional: create tables via API (some tenants show these as Unidentified)
                        if args.load_api:
                            success = await load_table_from_csv(client, ws_id, lh_id, spec.table_name, rel_path)
                            if success:
                                print(f"        ✓ loaded table: {spec.table_name}")
                            else:
                                print(f"        ✗ table load failed: {spec.table_name}")

                    except Exception as e:
                        print(f"      ✗ {spec.table_name}: {e}")

        if not args.load_api:
            print("""
ℹ️  Tables are NOT created by this script (default).
   Next: run the notebooks created by bootstrap to register tables via saveAsTable():
   - ENT_DataPlatform_* / Bronze_Landing: 01_Ingest_Bronze
   - ENT_DataPlatform_* / Silver_Curated: 02_Transform_Silver, 03_Build_Dimensions, 04_Build_Facts
   - ENT_DataPlatform_* / Gold_Published: 05_Aggregate_Gold
   Then run: python scripts/create_shortcuts.py
""")

        # Phase 5: Validation
        print("\n✅ PHASE 5: Validating environment...")
        validation = await validate_environment(client, bootstrapper.workspace_ids)
        
        print(f"\n   Validation Results:")
        print(f"   - Workspaces: {validation['workspaces']}")
        print(f"   - Lakehouses: {validation['lakehouses']}")
        print(f"   - Tables: {validation['tables']}")
        print(f"   - Notebooks: {validation['notebooks']}")
        print(f"   - Pipelines: {validation['pipelines']}")
        print(f"   - Semantic Models: {validation['semantic_models']}")
        
        if validation['issues']:
            print(f"\n   Issues:")
            for issue in validation['issues']:
                print(f"   - {issue}")
        
        # Save results
        output_path = Path(args.output) if args.output else Path("bootstrap_complete_results.json")
        output_data = {
            "assets": results,
            "validation": validation,
        }
        output_path.write_text(json.dumps(output_data, indent=2))
        print(f"\n📄 Results saved to: {output_path}")
        
        print("\n" + "=" * 70)
        print("  BOOTSTRAP COMPLETE!")
        print("=" * 70)
        print("\nNext steps:")
        print("1. Open Fabric portal and verify assets")
        print("2. Run notebooks to register tables via saveAsTable() (recommended)")
        print("3. Run: python scripts/create_shortcuts.py  (after Gold tables exist)")
        print("4. Test semantic model in Power BI")
        print("5. Run: python scripts/test_all_components.py --online --workspace 'your-workspace'")


def main():
    parser = argparse.ArgumentParser(
        description="Complete enterprise Fabric bootstrap (assets + data + validation)"
    )
    parser.add_argument("--prefix", default="ENT", help="Prefix for workspace names")
    parser.add_argument("--domain", help="Bootstrap specific domain only")
    parser.add_argument(
        "--workspace",
        help="Use existing workspace instead of creating new ones"
    )
    parser.add_argument(
        "--capacity-id",
        help="Fabric capacity ID to assign workspaces to"
    )
    parser.add_argument(
        "--enable-reports",
        action="store_true",
        help="Create placeholder reports"
    )
    parser.add_argument("--load-api", action="store_true", help="Also create tables via Lakehouse Load Table API (not recommended if you see Tables→Unidentified)")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation")
    parser.add_argument("--output", help="Output file for results")
    
    args = parser.parse_args()
    
    asyncio.run(run_complete_bootstrap(args))


if __name__ == "__main__":
    main()
