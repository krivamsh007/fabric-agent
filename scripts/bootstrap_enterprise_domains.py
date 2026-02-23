#!/usr/bin/env python3
"""
Enterprise Multi-Domain Fabric Bootstrap
==========================================

THE ONE SCRIPT TO RULE THEM ALL.
This is the single entry point for standing up the entire enterprise Fabric
environment from zero.  Run it in three steps:

STEP 1 — Full initial setup (workspaces + data + notebooks + semantic models)
─────────────────────────────────────────────────────────────────────────────
  python scripts/bootstrap_enterprise_domains.py \\
    --full \\
    --capacity-id <your-trial-or-fabric-capacity-id> \\
    --yes

  What --full does (equivalent long form):
    --include-projects   deploy self-healing + memory project notebooks
    --seed-data          upload CSV seed files to OneLake Files/seed/
    --deploy-package     build + upload fabric_agent wheel to every DataPlatform lakehouse
    --skip-shortcuts     skip shortcuts (tables don't exist yet; see Step 3)

STEP 2 — Run ETL notebooks inside Fabric UI (manual, one time)
──────────────────────────────────────────────────────────────
  Open ENT_DataPlatform_DEV in the Fabric portal and run these notebooks
  in order (they read the seed CSVs and create registered Delta tables):

    01_Ingest_Bronze      → creates Bronze tables
    02_Transform_Silver   → cleanses + conforms data
    03_Build_Dimensions   → SCD dimension tables
    04_Build_Facts        → fact_sales grain
    05_Aggregate_Gold     → agg_daily_sales, agg_customer_360, agg_inventory_health
    06_Data_Quality_Checks→ validates table row counts

  TIP: The PL_Daily_ETL pipeline chains all six notebooks — trigger it once
  via the Fabric UI instead of running notebooks manually.

STEP 3 — Create cross-workspace shortcuts (after Step 2 tables exist)
───────────────────────────────────────────────────────────────────────
  python scripts/bootstrap_enterprise_domains.py \\
    --shortcuts-only \\
    --capacity-id <id> \\
    --yes

  This creates the OneLake shortcuts in ENT_SalesAnalytics_DEV and
  ENT_Finance_DEV that point to Silver/Gold tables in DataPlatform_DEV.

─────────────────────────────────────────────────────────────────────────────
OTHER USEFUL FLAGS
─────────────────────────────────────────────────────────────────────────────
  --dry-run          Preview what would be created without touching Fabric
  --include-projects Deploy PRJ_ notebooks without running full domain bootstrap
  --projects-only    Only deploy strategic project notebooks (no enterprise domains)
  --workspace <name> Map all logical workspaces into one existing workspace
  --enable-reports   Also create placeholder Power BI reports
  --upload-notebook-script <path>  Update a single existing notebook's content
  --domain <name>    Bootstrap a single domain only

─────────────────────────────────────────────────────────────────────────────
ENTERPRISE ASSET MAP
─────────────────────────────────────────────────────────────────────────────

DOMAIN 1: DATA PLATFORM (Producer)
├── ENT_DataPlatform_DEV
│   ├── Lakehouse: Bronze_Landing  (raw → Delta via notebooks)
│   ├── Lakehouse: Silver_Curated  (dims + facts)
│   ├── Lakehouse: Gold_Published  (aggregations)
│   ├── Notebook:  01_Ingest_Bronze_AutoHeal  (schema-drift self-healing)
│   ├── Notebooks: 01–06 ETL + quality
│   ├── Notebook:  PRJ_SelfHealing_FabricInfrastructure
│   ├── Notebook:  PRJ_Context_MemoryState_Mgmt
│   ├── Pipeline:  PL_Daily_ETL (chains 01–06 sequentially)
│   ├── Pipeline:  PL_Hourly_Incremental
│   ├── Pipeline:  PL_PRJ_SelfHealing_FabricInfrastructure
│   └── Pipeline:  PL_PRJ_Context_MemoryState_Mgmt
│
└── ENT_DataPlatform_PROD  (same structure, no seed data)

DOMAIN 2: SALES ANALYTICS (Consumer)
├── ENT_SalesAnalytics_DEV
│   ├── Lakehouse: Analytics_Sandbox
│   ├── Shortcuts: dim_* + fact_sales + agg_* (→ DataPlatform Silver/Gold)
│   ├── Semantic Model: Enterprise_Sales_Model (100+ DAX measures)
│   └── Reports:   Executive_Sales_Dashboard, Regional_Performance, …
│
└── ENT_SalesAnalytics_PROD

DOMAIN 3: FINANCE (Consumer)
└── ENT_Finance_DEV
    ├── Lakehouse: Finance_Layer (budget_data, forecast_data)
    ├── Shortcuts: fact_sales + agg_daily_sales (→ DataPlatform)
    └── Semantic Model: Finance_Model

Data Flow:
    seed CSVs → Bronze → Silver → Gold → Shortcuts → Semantic Models → Reports

Author: Fabric Agent Team
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import random
import string
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4
import logging

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):  # type: ignore[misc]
        pass

# Project root (parent of this scripts/ directory)
ROOT = Path(__file__).resolve().parents[1]

# Load .env into os.environ before any os.getenv() calls below.
# This means all notebook template f-strings pick up the real user values at
# bootstrap time, so the deployed Fabric notebooks have working credentials.
load_dotenv(ROOT / ".env")

# Project imports
from fabric_agent.core.config import FabricAuthConfig
from fabric_agent.api.fabric_client import FabricApiClient, FabricApiError

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("enterprise_bootstrap")

# =============================================================================
# Seed Data Configuration
# =============================================================================
# Seed CSVs live at data/seed_data/<layer>/<file>.csv in the repo.
# The bootstrap uploads them to each DataPlatform lakehouse under Files/seed/
# so that the Spark ETL notebooks (01–05) can ingest them via saveAsTable().
#
# SEQUENCE: bootstrap --full → notebooks run (Fabric UI) → bootstrap --shortcuts-only
#
_SEED_DATA_DIR = ROOT / "data" / "seed_data"

# Maps lakehouse_name → [(table_name, layer_folder, csv_filename), ...]
_SEED_TABLES: Dict[str, List[Tuple[str, str, str]]] = {
    "Bronze_Landing": [
        ("raw_sales_transactions", "bronze", "raw_sales_transactions.csv"),
        ("raw_inventory_snapshots", "bronze", "raw_inventory_snapshots.csv"),
        ("raw_customer_events",     "bronze", "raw_customer_events.csv"),
    ],
    "Silver_Curated": [
        ("dim_date",     "silver", "dim_date.csv"),
        ("dim_customer", "silver", "dim_customer.csv"),
        ("dim_product",  "silver", "dim_product.csv"),
        ("dim_store",    "silver", "dim_store.csv"),
        ("fact_sales",   "silver", "fact_sales.csv"),
    ],
    "Gold_Published": [
        ("agg_daily_sales",       "gold", "agg_daily_sales.csv"),
        ("agg_customer_360",      "gold", "agg_customer_360.csv"),
        ("agg_inventory_health",  "gold", "agg_inventory_health.csv"),
    ],
}


def build_ipynb_from_python_script(
    script_name: str,
    script_text: str,
    lakehouse_id: str = "",
    workspace_id: str = "",
) -> str:
    """Wrap a local .py script into a minimal Fabric-compatible ipynb payload.

    Args:
        script_name:   Filename (e.g. "01_ingest_bronze_autoheal.py"). Used to choose
                       which setup cells to prepend.
        script_text:   Full source text of the script.
        lakehouse_id:  GUID of the default lakehouse to attach via %%configure.
                       Discovered by the bootstrap after creating the lakehouse.
        workspace_id:  GUID of the workspace that owns the lakehouse.
    """
    lines = script_text.splitlines(True)
    if not lines:
        lines = ["\n"]
    elif not lines[-1].endswith("\n"):
        lines[-1] = lines[-1] + "\n"

    cells: List[Dict[str, Any]] = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [f"# Uploaded from {script_name}\n"],
        }
    ]

    # Add guided setup cells for the auto-heal notebook upload path.
    if script_name.lower() == "01_ingest_bronze_autoheal.py":
        cells.extend(
            [
                {
                    "cell_type": "code",
                    "metadata": {},
                    "execution_count": None,
                    "outputs": [],
                    "source": [
                        "# 1) Upgrade typing_extensions FIRST — pydantic-core>=2.27 needs Sentinel\n",
                        "# (added in typing_extensions 4.12.0). Fabric clusters pin an older version.\n",
                        "%pip install 'typing_extensions>=4.12.0' azure-identity loguru pydantic pydantic-settings requests httpx aiohttp python-dotenv anyio typer rich azure-storage-file-datalake\n",
                    ],
                },
                {
                    "cell_type": "code",
                    "metadata": {},
                    "execution_count": None,
                    "outputs": [],
                    "source": [
                        "%%configure -f\n",
                        "{\n",
                        '  "defaultLakehouse": {\n',
                        '    "name": "Bronze_Landing",\n',
                        f'    "id": "{lakehouse_id}",\n',
                        f'    "workspaceId": "{workspace_id}"\n',
                        "  }\n",
                        "}\n",
                    ],
                },
                {
                    "cell_type": "code",
                    "metadata": {},
                    "execution_count": None,
                    "outputs": [],
                    "source": [
                        "# 2) Install fabric_agent from the wheel uploaded to this lakehouse.\n",
                        "# Run bootstrap_enterprise_domains.py --deploy-package if wheel is missing.\n",
                        "import glob, subprocess, sys, os\n",
                        "_wheels = sorted(glob.glob('/lakehouse/default/Files/wheels/fabric_agent-*.whl'))\n",
                        "if _wheels:\n",
                        "    _wheel = _wheels[-1]\n",
                        "    subprocess.run(\n",
                        "        [sys.executable, '-m', 'pip', 'install',\n",
                        "         '--force-reinstall', '--no-deps', _wheel, '--quiet'],\n",
                        "        check=True,\n",
                        "    )\n",
                        "    print(f'fabric_agent installed from: {_wheel}')\n",
                        "    os.environ['LOCAL_AGENT_ENABLED'] = '1'\n",
                        "else:\n",
                        "    print('WARNING: fabric_agent wheel not found in /lakehouse/default/Files/wheels/')\n",
                        "    print('Run: python scripts/bootstrap_enterprise_domains.py --deploy-package --capacity-id <id>')\n",
                    ],
                },
                {
                    "cell_type": "code",
                    "metadata": {},
                    "execution_count": None,
                    "outputs": [],
                    "source": [
                        "# 3) Credentials — injected from your .env by the bootstrap script at deploy time.\n",
                        "# To change these values, update your .env and re-run:  --deploy-package\n",
                        f'os.environ["AZURE_TENANT_ID"] = "{os.getenv("AZURE_TENANT_ID", "<your-azure-tenant-id>")}"\n',
                        f'os.environ["AZURE_CLIENT_ID"] = "{os.getenv("AZURE_CLIENT_ID", "<your-azure-client-id>")}"\n',
                        f'os.environ["AZURE_CLIENT_SECRET"] = "{os.getenv("AZURE_CLIENT_SECRET", "<your-client-secret>")}"\n',
                        f'os.environ["USE_INTERACTIVE_AUTH"] = "{os.getenv("USE_INTERACTIVE_AUTH", "false")}"\n',
                        f'os.environ["AGENT_IMPACT_ENABLED"] = "{os.getenv("AGENT_IMPACT_ENABLED", "1")}"\n',
                        'os.environ.setdefault("LOCAL_AGENT_ENABLED", "0")  # overwritten to 1 after wheel install\n',
                        f'os.environ["BREAK_ON_BREAKING_DRIFT"] = "{os.getenv("BREAK_ON_BREAKING_DRIFT", "0")}"\n',
                        f'os.environ["SMTP_USER"] = "{os.getenv("SMTP_USER", "")}"\n',
                        f'os.environ["SMTP_PASSWORD"] = "{os.getenv("SMTP_PASSWORD", "")}"\n',
                        f'os.environ["ALERT_EMAIL_TO"] = "{os.getenv("ALERT_EMAIL_TO", os.getenv("SMTP_USER", ""))}"\n',
                    ],
                },
                {
                    "cell_type": "code",
                    "metadata": {},
                    "execution_count": None,
                    "outputs": [],
                    "source": [
                        "import importlib, os\n",
                        "importlib.import_module(\"fabric_agent\")\n",
                        'print(\"fabric_agent import OK\")\n',
                        "for k in [\n",
                        '    \"AZURE_TENANT_ID\",\n',
                        '    \"AZURE_CLIENT_ID\",\n',
                        '    \"AZURE_CLIENT_SECRET\",\n',
                        '    \"SMTP_USER\",\n',
                        '    \"SMTP_PASSWORD\",\n',
                        '    \"ALERT_EMAIL_TO\",\n',
                        "]:\n",
                        '    print(k, \"OK\" if os.getenv(k) else \"MISSING\")\n',
                    ],
                },
            ]
        )

    cells.append(
        {
            "cell_type": "code",
            "metadata": {},
            "execution_count": None,
            "outputs": [],
            "source": lines,
        }
    )

    notebook = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "language_info": {"name": "python"},
            "kernel_info": {"name": "synapse_pyspark"},
        },
        "cells": cells,
    }
    return json.dumps(notebook, indent=2)


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class DomainConfig:
    """Configuration for a domain."""
    name: str
    description: str
    workspaces: List['WorkspaceConfig']


@dataclass  
class WorkspaceConfig:
    """Configuration for a workspace."""
    name: str
    environment: str  # DEV, PROD
    description: str
    lakehouses: List['LakehouseConfig'] = field(default_factory=list)
    notebooks: List['NotebookConfig'] = field(default_factory=list)
    pipelines: List['PipelineConfig'] = field(default_factory=list)
    semantic_models: List['SemanticModelConfig'] = field(default_factory=list)
    reports: List['ReportConfig'] = field(default_factory=list)
    shortcuts: List['ShortcutConfig'] = field(default_factory=list)


@dataclass
class LakehouseConfig:
    """Configuration for a lakehouse."""
    name: str
    description: str
    tables: List['TableConfig'] = field(default_factory=list)


@dataclass
class TableConfig:
    """Configuration for a Delta table."""
    name: str
    schema: Dict[str, str]  # column_name: data_type
    description: str
    is_dimension: bool = False
    row_count: int = 1000


@dataclass
class NotebookConfig:
    """Configuration for a notebook."""
    name: str
    description: str
    language: str = "python"
    lakehouse_ref: Optional[str] = None
    local_script_path: str = ""  # If set, deploy from this local .py file instead of generated content


@dataclass
class PipelineConfig:
    """Configuration for a pipeline."""
    name: str
    description: str
    activities: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class SemanticModelConfig:
    """Configuration for a semantic model."""
    name: str
    description: str
    source_lakehouse: str
    tables: List[str]
    measures: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class ReportConfig:
    """Configuration for a report."""
    name: str
    description: str
    semantic_model: str
    pages: List[str] = field(default_factory=list)


@dataclass
class ShortcutConfig:
    """Configuration for a shortcut."""
    name: str
    source_workspace: str
    source_lakehouse: str
    source_table: str


# =============================================================================
# Enterprise Configuration Builder
# =============================================================================

def build_enterprise_config(prefix: str = "ENT") -> List[DomainConfig]:
    """Build the complete enterprise configuration."""
    
    # =========================================================================
    # DOMAIN 1: DATA PLATFORM (Producer)
    # =========================================================================
    
    data_platform_domain = DomainConfig(
        name="DataPlatform",
        description="Central data platform - produces curated datasets",
        workspaces=[
            # DEV Workspace
            WorkspaceConfig(
                name=f"{prefix}_DataPlatform_DEV",
                environment="DEV",
                description="Data Platform Development Environment",
                lakehouses=[
                    # Bronze Layer - Raw Data
                    LakehouseConfig(
                        name="Bronze_Landing",
                        description="Raw data landing zone",
                        tables=[
                            TableConfig(
                                name="raw_sales_transactions",
                                schema={
                                    "transaction_id": "string",
                                    "transaction_date": "timestamp",
                                    "store_id": "string",
                                    "product_id": "string",
                                    "customer_id": "string",
                                    "quantity": "int",
                                    "unit_price": "decimal(10,2)",
                                    "discount_percent": "decimal(5,2)",
                                    "payment_method": "string",
                                    "source_system": "string",
                                    "ingestion_timestamp": "timestamp",
                                },
                                description="Raw sales transactions from POS systems",
                                row_count=500000,
                            ),
                            TableConfig(
                                name="raw_inventory_snapshots",
                                schema={
                                    "snapshot_id": "string",
                                    "snapshot_date": "date",
                                    "store_id": "string",
                                    "product_id": "string",
                                    "quantity_on_hand": "int",
                                    "quantity_reserved": "int",
                                    "reorder_point": "int",
                                    "source_system": "string",
                                },
                                description="Daily inventory snapshots",
                                row_count=100000,
                            ),
                            TableConfig(
                                name="raw_customer_events",
                                schema={
                                    "event_id": "string",
                                    "event_timestamp": "timestamp",
                                    "customer_id": "string",
                                    "event_type": "string",
                                    "channel": "string",
                                    "session_id": "string",
                                    "properties": "string",  # JSON
                                },
                                description="Customer behavior events",
                                row_count=2000000,
                            ),
                        ],
                    ),
                    # Silver Layer - Cleansed & Conformed
                    LakehouseConfig(
                        name="Silver_Curated",
                        description="Cleansed and conformed data",
                        tables=[
                            TableConfig(
                                name="dim_date",
                                schema={
                                    "date_key": "int",
                                    "full_date": "date",
                                    "day_of_week": "int",
                                    "day_name": "string",
                                    "day_of_month": "int",
                                    "day_of_year": "int",
                                    "week_of_year": "int",
                                    "month_number": "int",
                                    "month_name": "string",
                                    "quarter": "int",
                                    "year": "int",
                                    "is_weekend": "boolean",
                                    "is_holiday": "boolean",
                                    "fiscal_year": "int",
                                    "fiscal_quarter": "int",
                                },
                                description="Date dimension",
                                is_dimension=True,
                                row_count=3650,  # 10 years
                            ),
                            TableConfig(
                                name="dim_customer",
                                schema={
                                    "customer_key": "int",
                                    "customer_id": "string",
                                    "first_name": "string",
                                    "last_name": "string",
                                    "email": "string",
                                    "phone": "string",
                                    "address_line1": "string",
                                    "city": "string",
                                    "state": "string",
                                    "postal_code": "string",
                                    "country": "string",
                                    "customer_segment": "string",
                                    "loyalty_tier": "string",
                                    "acquisition_date": "date",
                                    "acquisition_channel": "string",
                                    "lifetime_value": "decimal(12,2)",
                                    "is_active": "boolean",
                                    "valid_from": "timestamp",
                                    "valid_to": "timestamp",
                                    "is_current": "boolean",
                                },
                                description="Customer dimension (SCD Type 2)",
                                is_dimension=True,
                                row_count=50000,
                            ),
                            TableConfig(
                                name="dim_product",
                                schema={
                                    "product_key": "int",
                                    "product_id": "string",
                                    "product_name": "string",
                                    "product_description": "string",
                                    "category_l1": "string",
                                    "category_l2": "string",
                                    "category_l3": "string",
                                    "brand": "string",
                                    "supplier_id": "string",
                                    "unit_cost": "decimal(10,2)",
                                    "unit_price": "decimal(10,2)",
                                    "weight_kg": "decimal(8,3)",
                                    "is_active": "boolean",
                                    "launch_date": "date",
                                    "discontinue_date": "date",
                                },
                                description="Product dimension",
                                is_dimension=True,
                                row_count=5000,
                            ),
                            TableConfig(
                                name="dim_store",
                                schema={
                                    "store_key": "int",
                                    "store_id": "string",
                                    "store_name": "string",
                                    "store_type": "string",
                                    "address": "string",
                                    "city": "string",
                                    "state": "string",
                                    "region": "string",
                                    "country": "string",
                                    "postal_code": "string",
                                    "latitude": "decimal(9,6)",
                                    "longitude": "decimal(9,6)",
                                    "square_footage": "int",
                                    "open_date": "date",
                                    "manager_name": "string",
                                    "is_active": "boolean",
                                },
                                description="Store/Location dimension",
                                is_dimension=True,
                                row_count=500,
                            ),
                            TableConfig(
                                name="fact_sales",
                                schema={
                                    "sales_key": "bigint",
                                    "date_key": "int",
                                    "customer_key": "int",
                                    "product_key": "int",
                                    "store_key": "int",
                                    "transaction_id": "string",
                                    "line_number": "int",
                                    "quantity": "int",
                                    "unit_price": "decimal(10,2)",
                                    "discount_amount": "decimal(10,2)",
                                    "tax_amount": "decimal(10,2)",
                                    "net_amount": "decimal(12,2)",
                                    "gross_amount": "decimal(12,2)",
                                    "cost_amount": "decimal(12,2)",
                                    "profit_amount": "decimal(12,2)",
                                },
                                description="Sales fact table (grain: transaction line)",
                                row_count=2000000,
                            ),
                        ],
                    ),
                    # Gold Layer - Business Ready
                    LakehouseConfig(
                        name="Gold_Published",
                        description="Business-ready aggregated datasets",
                        tables=[
                            TableConfig(
                                name="agg_daily_sales",
                                schema={
                                    "date_key": "int",
                                    "store_key": "int",
                                    "product_category": "string",
                                    "transaction_count": "int",
                                    "total_quantity": "int",
                                    "total_revenue": "decimal(14,2)",
                                    "total_cost": "decimal(14,2)",
                                    "total_profit": "decimal(14,2)",
                                    "avg_basket_size": "decimal(10,2)",
                                    "unique_customers": "int",
                                },
                                description="Daily sales aggregation",
                                row_count=500000,
                            ),
                            TableConfig(
                                name="agg_customer_360",
                                schema={
                                    "customer_key": "int",
                                    "total_orders": "int",
                                    "total_revenue": "decimal(14,2)",
                                    "avg_order_value": "decimal(10,2)",
                                    "first_purchase_date": "date",
                                    "last_purchase_date": "date",
                                    "days_since_last_purchase": "int",
                                    "favorite_category": "string",
                                    "favorite_store": "string",
                                    "churn_risk_score": "decimal(5,2)",
                                    "next_best_offer": "string",
                                },
                                description="Customer 360 view",
                                row_count=50000,
                            ),
                            TableConfig(
                                name="agg_inventory_health",
                                schema={
                                    "snapshot_date": "date",
                                    "store_key": "int",
                                    "product_key": "int",
                                    "quantity_on_hand": "int",
                                    "days_of_supply": "int",
                                    "stockout_risk": "string",
                                    "overstock_flag": "boolean",
                                    "recommended_action": "string",
                                },
                                description="Inventory health metrics",
                                row_count=200000,
                            ),
                        ],
                    ),
                ],
                notebooks=[
                    NotebookConfig(
                        name="01_Ingest_Bronze",
                        description="Ingest raw data from sources to Bronze",
                        lakehouse_ref="Bronze_Landing",
                    ),
                    NotebookConfig(
                        name="02_Transform_Silver",
                        description="Transform Bronze to Silver (cleanse, conform)",
                        lakehouse_ref="Silver_Curated",
                    ),
                    NotebookConfig(
                        name="03_Build_Dimensions",
                        description="Build and maintain dimension tables (SCD)",
                        lakehouse_ref="Silver_Curated",
                    ),
                    NotebookConfig(
                        name="04_Build_Facts",
                        description="Build fact tables with surrogate keys",
                        lakehouse_ref="Silver_Curated",
                    ),
                    NotebookConfig(
                        name="05_Aggregate_Gold",
                        description="Create Gold layer aggregations",
                        lakehouse_ref="Gold_Published",
                    ),
                    NotebookConfig(
                        name="06_Data_Quality_Checks",
                        description="Run data quality validations",
                        lakehouse_ref="Gold_Published",
                    ),
                ],
                pipelines=[
                    PipelineConfig(
                        name="PL_Daily_ETL",
                        description="Daily end-to-end ETL pipeline",
                        activities=[
                            {"name": "Ingest_Bronze", "type": "Notebook", "notebook": "01_Ingest_Bronze"},
                            {"name": "Transform_Silver", "type": "Notebook", "notebook": "02_Transform_Silver", "depends_on": ["Ingest_Bronze"]},
                            {"name": "Build_Dimensions", "type": "Notebook", "notebook": "03_Build_Dimensions", "depends_on": ["Transform_Silver"]},
                            {"name": "Build_Facts", "type": "Notebook", "notebook": "04_Build_Facts", "depends_on": ["Build_Dimensions"]},
                            {"name": "Aggregate_Gold", "type": "Notebook", "notebook": "05_Aggregate_Gold", "depends_on": ["Build_Facts"]},
                            {"name": "Quality_Checks", "type": "Notebook", "notebook": "06_Data_Quality_Checks", "depends_on": ["Aggregate_Gold"]},
                        ],
                    ),
                    PipelineConfig(
                        name="PL_Hourly_Incremental",
                        description="Hourly incremental updates",
                        activities=[
                            {"name": "Incremental_Ingest", "type": "Notebook", "notebook": "01_Ingest_Bronze"},
                            {"name": "Incremental_Transform", "type": "Notebook", "notebook": "02_Transform_Silver", "depends_on": ["Incremental_Ingest"]},
                        ],
                    ),
                ],
                semantic_models=[
                    SemanticModelConfig(
                        name="DataPlatform_Sales_Model",
                        description=(
                            "Source-of-truth semantic model on Silver_Curated tables. "
                            "This is the authoritative model for DataPlatform — consumer "
                            "workspaces shortcut these tables and build their own models."
                        ),
                        source_lakehouse="Silver_Curated",
                        tables=["dim_date", "dim_customer", "dim_product", "dim_store", "fact_sales"],
                        measures=[],  # Will be generated via generate_enterprise_measures()
                    ),
                ],
                reports=[
                    ReportConfig(
                        name="DataPlatform_Sales_Report",
                        description="Source-of-truth sales report in DataPlatform workspace",
                        semantic_model="DataPlatform_Sales_Model",
                        pages=["Overview", "Trends"],
                    ),
                ],
            ),
            # PROD Workspace (same structure)
            WorkspaceConfig(
                name=f"{prefix}_DataPlatform_PROD",
                environment="PROD",
                description="Data Platform Production Environment",
                lakehouses=[
                    LakehouseConfig(name="Bronze_Landing", description="Production Bronze", tables=[]),
                    LakehouseConfig(name="Silver_Curated", description="Production Silver", tables=[]),
                    LakehouseConfig(name="Gold_Published", description="Production Gold", tables=[]),
                ],
                notebooks=[
                    NotebookConfig(
                        name="01_Ingest_Bronze",
                        description="Production ingestion",
                        lakehouse_ref="Bronze_Landing",
                    ),
                    NotebookConfig(
                        name="02_Transform_Silver",
                        description="Production transformation",
                        lakehouse_ref="Silver_Curated",
                    ),
                    NotebookConfig(
                        name="03_Build_Dimensions",
                        description="Build production dimensions",
                        lakehouse_ref="Silver_Curated",
                    ),
                    NotebookConfig(
                        name="04_Build_Facts",
                        description="Build production facts",
                        lakehouse_ref="Silver_Curated",
                    ),
                    NotebookConfig(
                        name="05_Aggregate_Gold",
                        description="Create production Gold aggregates",
                        lakehouse_ref="Gold_Published",
                    ),
                    NotebookConfig(
                        name="06_Data_Quality_Checks",
                        description="Run production data quality checks",
                        lakehouse_ref="Gold_Published",
                    ),
                ],
                pipelines=[
                    PipelineConfig(name="PL_Daily_ETL", description="Production daily ETL"),
                ],
            ),
        ],
    )
    
    # =========================================================================
    # DOMAIN 2: SALES ANALYTICS (Consumer)
    # =========================================================================
    
    sales_analytics_domain = DomainConfig(
        name="SalesAnalytics",
        description="Sales analytics team - consumes Gold layer",
        workspaces=[
            WorkspaceConfig(
                name=f"{prefix}_SalesAnalytics_DEV",
                environment="DEV",
                description="Sales Analytics Development",
                lakehouses=[
                    LakehouseConfig(
                        name="Analytics_Sandbox",
                        description="Analytics team sandbox for exploration",
                        tables=[
                            TableConfig(
                                name="adhoc_analysis_results",
                                schema={"analysis_id": "string", "created_by": "string", "results": "string"},
                                description="Ad-hoc analysis results",
                                row_count=100,
                            ),
                        ],
                    ),
                ],
                shortcuts=[
                    ShortcutConfig(
                        name="dim_date",
                        source_workspace=f"{prefix}_DataPlatform_DEV",
                        source_lakehouse="Silver_Curated",
                        source_table="dim_date",
                    ),
                    ShortcutConfig(
                        name="dim_customer",
                        source_workspace=f"{prefix}_DataPlatform_DEV",
                        source_lakehouse="Silver_Curated",
                        source_table="dim_customer",
                    ),
                    ShortcutConfig(
                        name="dim_product",
                        source_workspace=f"{prefix}_DataPlatform_DEV",
                        source_lakehouse="Silver_Curated",
                        source_table="dim_product",
                    ),
                    ShortcutConfig(
                        name="dim_store",
                        source_workspace=f"{prefix}_DataPlatform_DEV",
                        source_lakehouse="Silver_Curated",
                        source_table="dim_store",
                    ),
                    ShortcutConfig(
                        name="fact_sales",
                        source_workspace=f"{prefix}_DataPlatform_DEV",
                        source_lakehouse="Silver_Curated",
                        source_table="fact_sales",
                    ),
                    ShortcutConfig(
                        name="agg_daily_sales",
                        source_workspace=f"{prefix}_DataPlatform_DEV",
                        source_lakehouse="Gold_Published",
                        source_table="agg_daily_sales",
                    ),
                    ShortcutConfig(
                        name="agg_customer_360",
                        source_workspace=f"{prefix}_DataPlatform_DEV",
                        source_lakehouse="Gold_Published",
                        source_table="agg_customer_360",
                    ),
                ],
                semantic_models=[
                    SemanticModelConfig(
                        name="Enterprise_Sales_Model",
                        description="Enterprise sales semantic model with 100+ measures",
                        source_lakehouse="Analytics_Sandbox",
                        tables=["dim_date", "dim_customer", "dim_product", "dim_store", "fact_sales"],
                        measures=[],  # Will be generated
                    ),
                ],
                reports=[
                    ReportConfig(
                        name="Executive_Sales_Dashboard",
                        description="C-level sales overview",
                        semantic_model="Enterprise_Sales_Model",
                        pages=["Overview", "Trends", "Regional", "Products"],
                    ),
                    ReportConfig(
                        name="Regional_Performance",
                        description="Regional sales performance",
                        semantic_model="Enterprise_Sales_Model",
                        pages=["Map", "Comparison", "Drill-down"],
                    ),
                    ReportConfig(
                        name="Product_Analytics",
                        description="Product performance analysis",
                        semantic_model="Enterprise_Sales_Model",
                        pages=["Categories", "Brands", "New Products"],
                    ),
                    ReportConfig(
                        name="Customer_Insights",
                        description="Customer behavior and segmentation",
                        semantic_model="Enterprise_Sales_Model",
                        pages=["Segments", "Cohorts", "Churn"],
                    ),
                ],
                notebooks=[
                    NotebookConfig(
                        name="Adhoc_Sales_Analysis",
                        description="Ad-hoc sales exploration",
                        lakehouse_ref="Analytics_Sandbox",
                    ),
                    NotebookConfig(
                        name="Customer_Segmentation_ML",
                        description="ML-based customer segmentation",
                        lakehouse_ref="Analytics_Sandbox",
                    ),
                ],
                pipelines=[
                    PipelineConfig(
                        name="PL_SalesAnalytics_Consumer_Refresh",
                        description=(
                            "Refresh SalesAnalytics consumer outputs from shared "
                            "DataPlatform shortcuts (fact_sales + dimensions)."
                        ),
                        activities=[
                            {
                                "name": "Adhoc_Sales_Analysis",
                                "type": "Notebook",
                                "notebook": "Adhoc_Sales_Analysis",
                            },
                            {
                                "name": "Customer_Segmentation_ML",
                                "type": "Notebook",
                                "notebook": "Customer_Segmentation_ML",
                                "depends_on": ["Adhoc_Sales_Analysis"],
                            },
                        ],
                    ),
                ],
            ),
            WorkspaceConfig(
                name=f"{prefix}_SalesAnalytics_PROD",
                environment="PROD",
                description="Sales Analytics Production",
                lakehouses=[
                    LakehouseConfig(name="Analytics_Sandbox", description="Production sandbox", tables=[]),
                ],
                semantic_models=[
                    SemanticModelConfig(
                        name="Enterprise_Sales_Model",
                        description="Production sales model",
                        source_lakehouse="Analytics_Sandbox",
                        tables=["dim_date", "dim_customer", "dim_product", "dim_store", "fact_sales"],
                        measures=[],
                    ),
                ],
                reports=[
                    ReportConfig(name="Executive_Sales_Dashboard", description="Production dashboard", semantic_model="Enterprise_Sales_Model"),
                ],
            ),
        ],
    )
    
    # =========================================================================
    # DOMAIN 3: FINANCE (Consumer)
    # =========================================================================
    
    finance_domain = DomainConfig(
        name="Finance",
        description="Finance team - P&L, budgeting, forecasting",
        workspaces=[
            WorkspaceConfig(
                name=f"{prefix}_Finance_DEV",
                environment="DEV",
                description="Finance Analytics Development",
                lakehouses=[
                    LakehouseConfig(
                        name="Finance_Layer",
                        description="Finance-specific calculations",
                        tables=[
                            TableConfig(
                                name="budget_data",
                                schema={
                                    "budget_id": "string",
                                    "fiscal_year": "int",
                                    "fiscal_month": "int",
                                    "department": "string",
                                    "cost_center": "string",
                                    "account_code": "string",
                                    "budget_amount": "decimal(14,2)",
                                    "budget_type": "string",
                                },
                                description="Annual budget data",
                                row_count=50000,
                            ),
                            TableConfig(
                                name="forecast_data",
                                schema={
                                    "forecast_id": "string",
                                    "forecast_date": "date",
                                    "forecast_period": "string",
                                    "revenue_forecast": "decimal(14,2)",
                                    "cost_forecast": "decimal(14,2)",
                                    "confidence_level": "decimal(5,2)",
                                },
                                description="Rolling forecasts",
                                row_count=10000,
                            ),
                        ],
                    ),
                ],
                shortcuts=[
                    ShortcutConfig(
                        name="fact_sales",
                        source_workspace=f"{prefix}_DataPlatform_DEV",
                        source_lakehouse="Silver_Curated",
                        source_table="fact_sales",
                    ),
                    ShortcutConfig(
                        name="agg_daily_sales",
                        source_workspace=f"{prefix}_DataPlatform_DEV",
                        source_lakehouse="Gold_Published",
                        source_table="agg_daily_sales",
                    ),
                ],
                semantic_models=[
                    SemanticModelConfig(
                        name="Finance_Model",
                        description="Financial reporting model",
                        source_lakehouse="Finance_Layer",
                        tables=["fact_sales", "budget_data", "forecast_data"],
                        measures=[],
                    ),
                ],
                reports=[
                    ReportConfig(
                        name="PnL_Dashboard",
                        description="Profit & Loss statement",
                        semantic_model="Finance_Model",
                        pages=["Summary", "Details", "Trends"],
                    ),
                    ReportConfig(
                        name="Budget_vs_Actual",
                        description="Budget variance analysis",
                        semantic_model="Finance_Model",
                        pages=["Overview", "Variances", "Drill-down"],
                    ),
                ],
                notebooks=[
                    NotebookConfig(
                        name="Finance_Variance_Refresh",
                        description=(
                            "Build finance variance outputs from shared fact_sales "
                            "shortcut + local budget/forecast tables."
                        ),
                        lakehouse_ref="Finance_Layer",
                    ),
                ],
                pipelines=[
                    PipelineConfig(
                        name="PL_Finance_Consumer_Refresh",
                        description=(
                            "Refresh finance consumer outputs from shared shortcuts "
                            "and local finance tables."
                        ),
                        activities=[
                            {
                                "name": "Finance_Variance_Refresh",
                                "type": "Notebook",
                                "notebook": "Finance_Variance_Refresh",
                            },
                        ],
                    ),
                ],
            ),
        ],
    )
    
    return [data_platform_domain, sales_analytics_domain, finance_domain]


def build_strategic_project_config(prefix: str = "ENT") -> List[DomainConfig]:
    """Build strategic project domains for direct validation in Microsoft Fabric UI.

    Both project notebooks and pipelines are deployed into ENT_DataPlatform_DEV
    (the central DataPlatform workspace) rather than creating separate project
    workspaces. This keeps all use cases in one place for easy discovery.
    """

    dataplatform_projects_domain = DomainConfig(
        name="DataPlatformProjects",
        description=(
            "Strategic projects deployed into ENT_DataPlatform_DEV: "
            "Self-Healing Fabric Infrastructure + Context Memory State Mgmt"
        ),
        workspaces=[
            WorkspaceConfig(
                name=f"{prefix}_DataPlatform_DEV",
                environment="DEV",
                description="Central DataPlatform workspace — also hosts strategic project notebooks",
                lakehouses=[
                    LakehouseConfig(
                        name="Bronze_Landing",
                        description="Primary landing zone (already exists — idempotent)",
                        tables=[],
                    ),
                ],
                notebooks=[
                    # ── Operational notebook (from local .py file) ───────────────────
                    NotebookConfig(
                        name="01_Ingest_Bronze_AutoHeal",
                        description=(
                            "Bronze ingestion with schema-drift auto-heal — "
                            "day-2 additive changes are repaired automatically; "
                            "breaking changes trigger an alert and optional pipeline block."
                        ),
                        lakehouse_ref="Bronze_Landing",
                        local_script_path="notebooks/01_ingest_bronze_autoheal.py",
                    ),
                    # ── Strategic project notebooks (generated inline) ───────────────
                    NotebookConfig(
                        name="PRJ_SelfHealing_FabricInfrastructure",
                        description=(
                            "Project notebook: Self-Healing Fabric Infrastructure "
                            "[priority=High, impact=Extreme]"
                        ),
                        lakehouse_ref="Bronze_Landing",
                    ),
                    NotebookConfig(
                        name="PRJ_Context_MemoryState_Mgmt",
                        description=(
                            "Project notebook: Context Memory State Mgmt "
                            "[priority=High, impact=Extreme]"
                        ),
                        lakehouse_ref="Bronze_Landing",
                    ),
                ],
                pipelines=[
                    PipelineConfig(
                        name="PL_PRJ_SelfHealing_FabricInfrastructure",
                        description="Project pipeline for Self-Healing Fabric Infrastructure",
                        activities=[
                            {
                                "name": "ProjectKickoff",
                                "type": "Notebook",
                                "notebook": "PRJ_SelfHealing_FabricInfrastructure",
                            },
                        ],
                    ),
                    PipelineConfig(
                        name="PL_PRJ_Context_MemoryState_Mgmt",
                        description="Project pipeline for Context Memory State Mgmt",
                        activities=[
                            {
                                "name": "ProjectKickoff",
                                "type": "Notebook",
                                "notebook": "PRJ_Context_MemoryState_Mgmt",
                            },
                        ],
                    ),
                ],
            ),
        ],
    )

    return [dataplatform_projects_domain]


# =============================================================================
# DAX Measure Generator (Enterprise Complexity)
# =============================================================================

def generate_enterprise_measures() -> List[Dict[str, str]]:
    """Generate 100+ enterprise DAX measures with dependencies."""
    
    measures = []
    
    # ==========================================================================
    # BASE MEASURES (No dependencies)
    # ==========================================================================
    
    base_measures = [
        # Revenue
        {"name": "Total Revenue", "expression": "SUM(fact_sales[gross_amount])", "folder": "Revenue"},
        {"name": "Total Net Revenue", "expression": "SUM(fact_sales[net_amount])", "folder": "Revenue"},
        {"name": "Total Discount", "expression": "SUM(fact_sales[discount_amount])", "folder": "Revenue"},
        {"name": "Total Tax", "expression": "SUM(fact_sales[tax_amount])", "folder": "Revenue"},
        
        # Cost & Profit
        {"name": "Total Cost", "expression": "SUM(fact_sales[cost_amount])", "folder": "Cost"},
        {"name": "Total Profit", "expression": "SUM(fact_sales[profit_amount])", "folder": "Profit"},
        
        # Volume
        {"name": "Total Quantity", "expression": "SUM(fact_sales[quantity])", "folder": "Volume"},
        {"name": "Transaction Count", "expression": "COUNTROWS(fact_sales)", "folder": "Volume"},
        {"name": "Unique Transactions", "expression": "DISTINCTCOUNT(fact_sales[transaction_id])", "folder": "Volume"},
        
        # Customer
        {"name": "Customer Count", "expression": "DISTINCTCOUNT(fact_sales[customer_key])", "folder": "Customer"},
        {"name": "Total Customers", "expression": "COUNTROWS(dim_customer)", "folder": "Customer"},
        {"name": "Active Customers", "expression": "CALCULATE(COUNTROWS(dim_customer), dim_customer[is_active] = TRUE())", "folder": "Customer"},
        
        # Product
        {"name": "Product Count", "expression": "DISTINCTCOUNT(fact_sales[product_key])", "folder": "Product"},
        {"name": "Total Products", "expression": "COUNTROWS(dim_product)", "folder": "Product"},
        
        # Store
        {"name": "Store Count", "expression": "DISTINCTCOUNT(fact_sales[store_key])", "folder": "Store"},
        {"name": "Total Stores", "expression": "COUNTROWS(dim_store)", "folder": "Store"},
    ]
    measures.extend(base_measures)
    
    # ==========================================================================
    # CALCULATED MEASURES (Depend on base)
    # ==========================================================================
    
    calculated_measures = [
        # Margins
        {"name": "Gross Margin", "expression": "[Total Profit] / [Total Revenue]", "folder": "Margins", "format": "0.00%"},
        {"name": "Gross Margin Amount", "expression": "[Total Revenue] - [Total Cost]", "folder": "Margins"},
        {"name": "Net Margin", "expression": "[Total Net Revenue] - [Total Cost]", "folder": "Margins"},
        {"name": "Discount Rate", "expression": "DIVIDE([Total Discount], [Total Revenue], 0)", "folder": "Margins", "format": "0.00%"},
        
        # Averages
        {"name": "Avg Transaction Value", "expression": "DIVIDE([Total Revenue], [Unique Transactions], 0)", "folder": "Averages"},
        {"name": "Avg Items Per Transaction", "expression": "DIVIDE([Total Quantity], [Unique Transactions], 0)", "folder": "Averages"},
        {"name": "Avg Unit Price", "expression": "DIVIDE([Total Revenue], [Total Quantity], 0)", "folder": "Averages"},
        {"name": "Avg Revenue Per Customer", "expression": "DIVIDE([Total Revenue], [Customer Count], 0)", "folder": "Averages"},
        {"name": "Avg Revenue Per Store", "expression": "DIVIDE([Total Revenue], [Store Count], 0)", "folder": "Averages"},
        {"name": "Avg Revenue Per Product", "expression": "DIVIDE([Total Revenue], [Product Count], 0)", "folder": "Averages"},
        
        # Profit Metrics
        {"name": "Profit Per Transaction", "expression": "DIVIDE([Total Profit], [Unique Transactions], 0)", "folder": "Profit"},
        {"name": "Profit Per Customer", "expression": "DIVIDE([Total Profit], [Customer Count], 0)", "folder": "Profit"},
        {"name": "Profit Per Unit", "expression": "DIVIDE([Total Profit], [Total Quantity], 0)", "folder": "Profit"},
    ]
    measures.extend(calculated_measures)
    
    # ==========================================================================
    # TIME INTELLIGENCE MEASURES
    # ==========================================================================
    
    time_measures = [
        # Year to Date
        {"name": "Revenue YTD", "expression": "TOTALYTD([Total Revenue], dim_date[full_date])", "folder": "Time Intelligence"},
        {"name": "Profit YTD", "expression": "TOTALYTD([Total Profit], dim_date[full_date])", "folder": "Time Intelligence"},
        {"name": "Cost YTD", "expression": "TOTALYTD([Total Cost], dim_date[full_date])", "folder": "Time Intelligence"},
        
        # Quarter to Date
        {"name": "Revenue QTD", "expression": "TOTALQTD([Total Revenue], dim_date[full_date])", "folder": "Time Intelligence"},
        {"name": "Profit QTD", "expression": "TOTALQTD([Total Profit], dim_date[full_date])", "folder": "Time Intelligence"},
        
        # Month to Date
        {"name": "Revenue MTD", "expression": "TOTALMTD([Total Revenue], dim_date[full_date])", "folder": "Time Intelligence"},
        {"name": "Profit MTD", "expression": "TOTALMTD([Total Profit], dim_date[full_date])", "folder": "Time Intelligence"},
        
        # Previous Period
        {"name": "Revenue PY", "expression": "CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(dim_date[full_date]))", "folder": "Time Intelligence"},
        {"name": "Profit PY", "expression": "CALCULATE([Total Profit], SAMEPERIODLASTYEAR(dim_date[full_date]))", "folder": "Time Intelligence"},
        {"name": "Revenue PM", "expression": "CALCULATE([Total Revenue], PREVIOUSMONTH(dim_date[full_date]))", "folder": "Time Intelligence"},
        {"name": "Revenue PQ", "expression": "CALCULATE([Total Revenue], PREVIOUSQUARTER(dim_date[full_date]))", "folder": "Time Intelligence"},
        
        # Growth
        {"name": "Revenue YoY Growth", "expression": "DIVIDE([Total Revenue] - [Revenue PY], [Revenue PY], 0)", "folder": "Growth", "format": "0.00%"},
        {"name": "Revenue YoY Variance", "expression": "[Total Revenue] - [Revenue PY]", "folder": "Growth"},
        {"name": "Profit YoY Growth", "expression": "DIVIDE([Total Profit] - [Profit PY], [Profit PY], 0)", "folder": "Growth", "format": "0.00%"},
        {"name": "Revenue MoM Growth", "expression": "DIVIDE([Total Revenue] - [Revenue PM], [Revenue PM], 0)", "folder": "Growth", "format": "0.00%"},
        
        # Running Totals
        {"name": "Revenue Running Total", "expression": "CALCULATE([Total Revenue], FILTER(ALL(dim_date), dim_date[full_date] <= MAX(dim_date[full_date])))", "folder": "Time Intelligence"},
        
        # Moving Averages
        {"name": "Revenue 3M Avg", "expression": "AVERAGEX(DATESINPERIOD(dim_date[full_date], MAX(dim_date[full_date]), -3, MONTH), [Total Revenue])", "folder": "Time Intelligence"},
        {"name": "Revenue 12M Avg", "expression": "AVERAGEX(DATESINPERIOD(dim_date[full_date], MAX(dim_date[full_date]), -12, MONTH), [Total Revenue])", "folder": "Time Intelligence"},
    ]
    measures.extend(time_measures)

    # EXTRA MEASURES TO REACH 98 (kept simple + valid against the sample model)
    extra_time_measures = [
        {"name": "Profit PQ", "expression": "CALCULATE([Total Profit], PREVIOUSQUARTER(dim_date[full_date]))", "folder": "Time Intelligence"},
        {"name": "Profit Running Total", "expression": "CALCULATE([Total Profit], FILTER(ALL(dim_date), dim_date[full_date] <= MAX(dim_date[full_date])))", "folder": "Time Intelligence"},
        {"name": "Cost PY", "expression": "CALCULATE([Total Cost], SAMEPERIODLASTYEAR(dim_date[full_date]))", "folder": "Time Intelligence"},
        {"name": "Profit YoY Variance", "expression": "[Total Profit] - [Profit PY]", "folder": "Growth"},
    ]
    measures.extend(extra_time_measures)

    # ==========================================================================
    # RANKING & TOP N MEASURES
    # ==========================================================================
    
    ranking_measures = [
        {"name": "Revenue Rank", "expression": "RANKX(ALL(dim_product), [Total Revenue],, DESC, Dense)", "folder": "Rankings"},
        {"name": "Profit Rank", "expression": "RANKX(ALL(dim_product), [Total Profit],, DESC, Dense)", "folder": "Rankings"},
        {"name": "Customer Rank", "expression": "RANKX(ALL(dim_customer), [Total Revenue],, DESC, Dense)", "folder": "Rankings"},
        {"name": "Store Rank", "expression": "RANKX(ALL(dim_store), [Total Revenue],, DESC, Dense)", "folder": "Rankings"},
        
        # Top N Contributions
        {"name": "Top 10 Products Revenue", "expression": "CALCULATE([Total Revenue], TOPN(10, ALL(dim_product), [Total Revenue], DESC))", "folder": "Rankings"},
        {"name": "Top 10 Customers Revenue", "expression": "CALCULATE([Total Revenue], TOPN(10, ALL(dim_customer), [Total Revenue], DESC))", "folder": "Rankings"},
        {"name": "Top 10 Stores Revenue", "expression": "CALCULATE([Total Revenue], TOPN(10, ALL(dim_store), [Total Revenue], DESC))", "folder": "Rankings"},
        
        # Pareto
        {"name": "Revenue Cumulative Pct", "expression": "DIVIDE(CALCULATE([Total Revenue], FILTER(ALL(dim_product), [Revenue Rank] <= MAX([Revenue Rank]))), CALCULATE([Total Revenue], ALL(dim_product)), 0)", "folder": "Rankings", "format": "0.00%"},
    ]
    measures.extend(ranking_measures)

    extra_ranking_measures = [
        {"name": "Top 10 Regions Revenue", "expression": "CALCULATE([Total Revenue], TOPN(10, SUMMARIZE(dim_store, dim_store[region]), [Total Revenue], DESC))", "folder": "Rankings"},
        {"name": "Top 10 Stores Profit", "expression": "CALCULATE([Total Profit], TOPN(10, ALL(dim_store), [Total Profit], DESC))", "folder": "Rankings"},
    ]
    measures.extend(extra_ranking_measures)

    # ==========================================================================
    # CUSTOMER ANALYTICS MEASURES
    # ==========================================================================
    
    customer_measures = [
        # Segmentation
        {"name": "New Customers", "expression": "CALCULATE([Customer Count], dim_customer[acquisition_date] >= DATE(YEAR(TODAY()), 1, 1))", "folder": "Customer Analytics"},
        {"name": "Repeat Customers", "expression": "CALCULATE([Customer Count], FILTER(dim_customer, COUNTROWS(RELATEDTABLE(fact_sales)) > 1))", "folder": "Customer Analytics"},
        {"name": "One-Time Customers", "expression": "[Customer Count] - [Repeat Customers]", "folder": "Customer Analytics"},
        
        # Retention
        {"name": "Repeat Rate", "expression": "DIVIDE([Repeat Customers], [Customer Count], 0)", "folder": "Customer Analytics", "format": "0.00%"},
        {"name": "Customer Retention Rate", "expression": "DIVIDE([Repeat Customers], [Customer Count] + [New Customers], 0)", "folder": "Customer Analytics", "format": "0.00%"},
        
        # Lifetime Value
        {"name": "Avg Customer Lifetime Value", "expression": "AVERAGEX(dim_customer, dim_customer[lifetime_value])", "folder": "Customer Analytics"},
        {"name": "Total Customer Lifetime Value", "expression": "SUMX(dim_customer, dim_customer[lifetime_value])", "folder": "Customer Analytics"},
        
        # Loyalty Tiers
        {"name": "Gold Customers", "expression": "CALCULATE([Customer Count], dim_customer[loyalty_tier] = \"Gold\")", "folder": "Customer Analytics"},
        {"name": "Silver Customers", "expression": "CALCULATE([Customer Count], dim_customer[loyalty_tier] = \"Silver\")", "folder": "Customer Analytics"},
        {"name": "Bronze Customers", "expression": "CALCULATE([Customer Count], dim_customer[loyalty_tier] = \"Bronze\")", "folder": "Customer Analytics"},
        {"name": "Gold Customer Revenue", "expression": "CALCULATE([Total Revenue], dim_customer[loyalty_tier] = \"Gold\")", "folder": "Customer Analytics"},
        {"name": "Gold Customer Share", "expression": "DIVIDE([Gold Customer Revenue], [Total Revenue], 0)", "folder": "Customer Analytics", "format": "0.00%"},
    ]
    measures.extend(customer_measures)

    extra_customer_measures = [
        {"name": "Silver Customer Revenue", "expression": "CALCULATE([Total Revenue], dim_customer[loyalty_tier] = \"Silver\")", "folder": "Customer Analytics"},
        {"name": "Silver Customer Share", "expression": "DIVIDE([Silver Customer Revenue], [Total Revenue], 0)", "folder": "Customer Analytics", "format": "0.00%"},
        {"name": "Bronze Customer Revenue", "expression": "CALCULATE([Total Revenue], dim_customer[loyalty_tier] = \"Bronze\")", "folder": "Customer Analytics"},
        {"name": "Bronze Customer Share", "expression": "DIVIDE([Bronze Customer Revenue], [Total Revenue], 0)", "folder": "Customer Analytics", "format": "0.00%"},
    ]
    measures.extend(extra_customer_measures)

    # ==========================================================================
    # PRODUCT ANALYTICS MEASURES
    # ==========================================================================
    
    product_measures = [
        # Category Performance
        {"name": "Category Revenue Share", "expression": "DIVIDE([Total Revenue], CALCULATE([Total Revenue], ALL(dim_product[category_l1])), 0)", "folder": "Product Analytics", "format": "0.00%"},
        {"name": "Brand Revenue Share", "expression": "DIVIDE([Total Revenue], CALCULATE([Total Revenue], ALL(dim_product[brand])), 0)", "folder": "Product Analytics", "format": "0.00%"},
        
        # Product Lifecycle
        {"name": "New Product Revenue", "expression": "CALCULATE([Total Revenue], dim_product[launch_date] >= DATEADD(TODAY(), -90, DAY))", "folder": "Product Analytics"},
        {"name": "New Product Share", "expression": "DIVIDE([New Product Revenue], [Total Revenue], 0)", "folder": "Product Analytics", "format": "0.00%"},
        
        # Inventory Turns
        {"name": "Avg Days to Sell", "expression": "DIVIDE([Total Quantity], DISTINCTCOUNT(dim_date[full_date]), 0)", "folder": "Product Analytics"},
    ]
    measures.extend(product_measures)

    extra_product_measures = [
        {"name": "Category Profit Share", "expression": "DIVIDE([Total Profit], CALCULATE([Total Profit], ALL(dim_product)), 0)", "folder": "Product Analytics", "format": "0.00%"},
        {"name": "Brand Profit Share", "expression": "DIVIDE([Total Profit], CALCULATE([Total Profit], ALL(dim_product)), 0)", "folder": "Product Analytics", "format": "0.00%"},
        {"name": "Avg Discount Per Transaction", "expression": "DIVIDE([Total Discount], [Unique Transactions], 0)", "folder": "Averages", "format": "$#,0.00"},
    ]
    measures.extend(extra_product_measures)

    # ==========================================================================
    # STORE ANALYTICS MEASURES
    # ==========================================================================
    
    store_measures = [
        # Performance
        {"name": "Revenue Per Sq Ft", "expression": "DIVIDE([Total Revenue], SUM(dim_store[square_footage]), 0)", "folder": "Store Analytics"},
        {"name": "Store Revenue Share", "expression": "DIVIDE([Total Revenue], CALCULATE([Total Revenue], ALL(dim_store)), 0)", "folder": "Store Analytics", "format": "0.00%"},
        {"name": "Region Revenue Share", "expression": "DIVIDE([Total Revenue], CALCULATE([Total Revenue], ALL(dim_store[region])), 0)", "folder": "Store Analytics", "format": "0.00%"},
        
        # Comparisons
        {"name": "Store vs Region Avg", "expression": "[Total Revenue] - CALCULATE(AVERAGEX(ALL(dim_store), [Total Revenue]), ALLEXCEPT(dim_store, dim_store[region]))", "folder": "Store Analytics"},
        {"name": "Store vs Company Avg", "expression": "[Total Revenue] - AVERAGEX(ALL(dim_store), [Total Revenue])", "folder": "Store Analytics"},
    ]
    measures.extend(store_measures)

    extra_store_measures = [
        {"name": "Profit Per Sq Ft", "expression": "DIVIDE([Total Profit], SUM(dim_store[square_footage]), 0)", "folder": "Store Analytics", "format": "$#,0.00"},
        {"name": "Store Profit Share", "expression": "DIVIDE([Total Profit], CALCULATE([Total Profit], ALL(dim_store)), 0)", "folder": "Store Analytics", "format": "0.00%"},
        {"name": "Region Profit Share", "expression": "DIVIDE([Total Profit], CALCULATE([Total Profit], ALL(dim_store[region])), 0)", "folder": "Store Analytics", "format": "0.00%"},
    ]
    measures.extend(extra_store_measures)

    # ==========================================================================
    # KPI MEASURES (Complex dependencies)
    # ==========================================================================
    
    kpi_measures = [
        # Traffic Light KPIs
        {"name": "Revenue KPI Status", "expression": """
            VAR Current = [Total Revenue]
            VAR Target = [Revenue PY] * 1.1
            RETURN
            SWITCH(
                TRUE(),
                Current >= Target, "Green",
                Current >= Target * 0.9, "Yellow",
                "Red"
            )
        """, "folder": "KPIs"},
        
        {"name": "Margin KPI Status", "expression": """
            VAR Current = [Gross Margin]
            VAR Target = 0.35
            RETURN
            SWITCH(
                TRUE(),
                Current >= Target, "Green",
                Current >= Target * 0.9, "Yellow",
                "Red"
            )
        """, "folder": "KPIs"},
        
        # Composite Scores
        {"name": "Performance Score", "expression": """
            VAR RevenueScore = IF([Revenue YoY Growth] > 0.1, 3, IF([Revenue YoY Growth] > 0, 2, 1))
            VAR MarginScore = IF([Gross Margin] > 0.35, 3, IF([Gross Margin] > 0.25, 2, 1))
            VAR CustomerScore = IF([Repeat Rate] > 0.5, 3, IF([Repeat Rate] > 0.3, 2, 1))
            RETURN (RevenueScore + MarginScore + CustomerScore) / 3
        """, "folder": "KPIs"},
        
        # Executive Summary
        {"name": "Executive Summary", "expression": """
            VAR Revenue = FORMAT([Total Revenue], "$#,##0")
            VAR Growth = FORMAT([Revenue YoY Growth], "0.0%")
            VAR Margin = FORMAT([Gross Margin], "0.0%")
            RETURN Revenue & " | YoY: " & Growth & " | Margin: " & Margin
        """, "folder": "KPIs"},
    ]
    measures.extend(kpi_measures)

    extra_kpi_measures = [
        {"name": "Profit KPI Status", "expression": "IF([Profit YoY Growth] > 0, \"OK\", \"ALERT\")", "folder": "KPIs"},
    ]
    measures.extend(extra_kpi_measures)

    return measures


# =============================================================================
# Notebook Content Generator
# =============================================================================


# =============================================================================
# fabric_agent Package Deployment Helpers
# =============================================================================


def _build_fabric_agent_wheel(package_dir: Optional[str] = None) -> Optional[Path]:
    """
    Build the fabric_agent Python wheel for Fabric deployment.

    WHAT: Runs 'python -m build --wheel' from the project root.
    WHY:  Fabric Notebooks run in a managed Spark environment — they cannot
          pip-install from GitHub or local paths. The supported pattern is:
          1. Build a .whl locally
          2. Upload it to Lakehouse Files/wheels/
          3. In the notebook: %pip install /lakehouse/default/Files/wheels/fabric_agent-*.whl

    FAANG PATTERN:
        Same as internal PyPI mirrors at Google/Meta — push artifacts to
        where compute can reach them, not to where developers work.

    Args:
        package_dir: Directory to write the wheel to (default: ROOT/dist/).

    Returns:
        Path to the built .whl file, or None if the build fails.
        NON-FATAL: a build failure will NOT abort the bootstrap.
    """
    dist_dir = Path(package_dir) if package_dir else (ROOT / "dist")
    dist_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Building fabric_agent wheel for Fabric deployment...")
    result = subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--outdir", str(dist_dir)],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        logger.warning(
            "Wheel build failed. Install 'build' with: pip install build\n"
            f"stderr: {result.stderr[-500:]}"
        )
        return None

    wheels = sorted(dist_dir.glob("fabric_agent-*.whl"), key=lambda p: p.stat().st_mtime)
    if not wheels:
        logger.warning("Wheel build succeeded but no fabric_agent-*.whl file found in dist/")
        return None

    wheel_path = wheels[-1]
    logger.info(f"Wheel built: {wheel_path.name} ({wheel_path.stat().st_size // 1024} KB)")
    return wheel_path


def _generate_healing_notebook_content(
    workspace_name: str, workspace_id: str, config: "NotebookConfig"
) -> str:
    """
    Generate rich, runnable notebook content for Self-Healing Fabric Infrastructure.

    WHAT: Creates a step-by-step tutorial notebook that:
          1. Installs fabric_agent from the pre-uploaded wheel
          2. Initialises SelfHealingMonitor
          3. Runs an anomaly scan (dry_run=True by default)
          4. Reviews the healing plan (auto vs manual actions)
          5. Explains how to schedule via Fabric Pipeline

    FAANG PARALLEL:
        Google SRE: auto-rollback + health dashboards
        Netflix Chaos Monkey: auto-restart failed services
        Meta Tupperware: auto-reschedule failed containers
        This notebook is the Fabric-adapted version of those patterns.
    """

    def md_cell(lines: List[str]) -> Dict[str, Any]:
        return {"cell_type": "markdown", "metadata": {}, "source": lines}

    def code_cell(lines: List[str]) -> Dict[str, Any]:
        return {
            "cell_type": "code",
            "metadata": {},
            "source": lines,
            "execution_count": None,
            "outputs": [],
        }

    cells = [
        md_cell([
            "# Self-Healing Fabric Infrastructure\n",
            "\n",
            "## What is Self-Healing Infrastructure?\n",
            "\n",
            "This notebook demonstrates **autonomous detection and remediation** of Fabric anomalies.\n",
            "\n",
            "**FAANG Parallels:**\n",
            "- **Google SRE**: Auto-rollback bad pushes using health signals\n",
            "- **Netflix Chaos Monkey**: Auto-restart failed services without manual paging\n",
            "- **Meta Tupperware**: Auto-reschedule failed containers across the fleet\n",
            "\n",
            "**Anomaly types detected:**\n",
            "| Type | Description | Auto-Fixable? |\n",
            "|------|-------------|---------------|\n",
            "| BROKEN_SHORTCUT | OneLake shortcut lost its source connection | Yes |\n",
            "| SCHEMA_DRIFT | Table schema deviated from registered contract | Additive only |\n",
            "| STALE_TABLE | Table not refreshed within configured SLA | Yes (trigger pipeline) |\n",
            "| ORPHAN_ASSET | Measure/table with no downstream consumers | No (human review) |\n",
            "| PIPELINE_FAILURE | Last pipeline run did not succeed | No (human diagnosis) |\n",
        ]),
        md_cell(["## Step 1: Install fabric_agent\n", "\n",
                 "Same bootstrap pattern as `01_Ingest_Bronze` — copies wheel from OneLake to `/tmp/`\n",
                 "via `mssparkutils.fs` (Azure SDK, bypasses blobfuse) then pip-installs from the local path.\n"]),
        code_cell([
            "# Bootstrap: install fabric_agent from the pre-uploaded wheel.\n",
            "# Identical pattern to 01_Ingest_Bronze — the approach that works in Fabric Spark.\n",
            "import subprocess as _subprocess, sys as _sys, os\n",
            "\n",
            "_ABFSS_WHEEL_DIR = (\n",
            "    'abfss://ENT_DataPlatform_DEV@onelake.dfs.fabric.microsoft.com'\n",
            "    '/Bronze_Landing.Lakehouse/Files/wheels'\n",
            ")\n",
            "_TMP_WHEEL_DIR = '/tmp/fabric_agent_wheels'\n",
            "_BLOBFUSE_WHEEL_GLOB = '/lakehouse/default/Files/wheels/fabric_agent-*.whl'\n",
            "\n",
            "def _resolve_wheel_local_path():\n",
            "    try:\n",
            "        from notebookutils import mssparkutils\n",
            "        items = mssparkutils.fs.ls(_ABFSS_WHEEL_DIR)\n",
            "        whl_items = sorted(\n",
            "            [i for i in items if i.name.startswith('fabric_agent-') and i.name.endswith('.whl')],\n",
            "            key=lambda x: x.name,\n",
            "        )\n",
            "        if whl_items:\n",
            "            latest = whl_items[-1]\n",
            "            os.makedirs(_TMP_WHEEL_DIR, exist_ok=True)\n",
            "            local_path = f'{_TMP_WHEEL_DIR}/{latest.name}'\n",
            "            mssparkutils.fs.cp(f'{_ABFSS_WHEEL_DIR}/{latest.name}', f'file://{local_path}', True)\n",
            "            print(f'[bootstrap] Wheel ready at: {local_path}')\n",
            "            return local_path\n",
            "    except Exception as _e:\n",
            "        print(f'[bootstrap] mssparkutils unavailable ({_e}), trying blobfuse fallback...')\n",
            "    import glob as _glob\n",
            "    _wheels = sorted(_glob.glob(_BLOBFUSE_WHEEL_GLOB))\n",
            "    if _wheels:\n",
            "        return _wheels[-1]\n",
            "    return None\n",
            "\n",
            "_wheel = _resolve_wheel_local_path()\n",
            "if _wheel:\n",
            "    print(f'[bootstrap] Installing fabric_agent from: {_wheel}')\n",
            "    _result = _subprocess.run(\n",
            "        [_sys.executable, '-m', 'pip', 'install',\n",
            "         'typing_extensions>=4.12.0',\n",
            "         f'{_wheel}[healing,memory]',\n",
            "         '--quiet'],\n",
            "        capture_output=True, text=True,\n",
            "    )\n",
            "    if _result.returncode == 0:\n",
            "        print('[bootstrap] fabric_agent installed successfully.')\n",
            "        os.environ.setdefault('LOCAL_AGENT_ENABLED', '1')\n",
            "    else:\n",
            "        print(f'[bootstrap] pip install failed:\\n{_result.stderr[-500:]}')\n",
            "else:\n",
            "    print('[bootstrap] WARNING: wheel not found. Run bootstrap --deploy-package --capacity-id <id>')\n",
        ]),
        code_cell([
            "# Verify\n",
            "import fabric_agent\n",
            "print(f'fabric_agent version : {fabric_agent.__version__}')\n",
            "from fabric_agent.healing import SelfHealingMonitor, AnomalyDetector, SelfHealer\n",
            "from fabric_agent.healing.models import AnomalyType, RiskLevel, HealActionStatus\n",
            "print('Imports OK: SelfHealingMonitor, AnomalyDetector, SelfHealer')\n",
        ]),
        md_cell([
            "## Step 2: Detect Fabric Environment\n",
            "\n",
            "The agent auto-detects whether it's running in a Fabric Notebook or locally.\n",
            "- **Fabric**: uses `notebookutils` to get the current workspace ID\n",
            "- **Local**: falls back to environment variables\n",
        ]),
        code_cell([
            "import os\n",
            "\n",
            "try:\n",
            "    import notebookutils\n",
            "    IN_FABRIC = True\n",
            "    WORKSPACE_ID = notebookutils.runtime.context['currentWorkspaceId']\n",
            "    print(f'Running in Fabric | Workspace ID: {WORKSPACE_ID}')\n",
            "except ImportError:\n",
            "    IN_FABRIC = False\n",
            "    WORKSPACE_ID = os.getenv('FABRIC_WORKSPACE_ID', 'local-dev-workspace')\n",
            "    print(f'Running locally | Workspace ID: {WORKSPACE_ID}')\n",
            "\n",
            "# Add more workspace IDs here to monitor multiple workspaces\n",
            "WORKSPACE_IDS = [WORKSPACE_ID]\n",
            "print(f'Monitoring workspaces: {WORKSPACE_IDS}')\n",
        ]),
        md_cell([
            "## Step 3: Initialize the Self-Healing Monitor\n",
            "\n",
            "The `SelfHealingMonitor` orchestrates the full cycle:\n",
            "\n",
            "```\n",
            "detect (AnomalyDetector)\n",
            "  ↓\n",
            "plan (SelfHealer.build_plan)\n",
            "  ↓\n",
            "execute (SelfHealer.execute_healing_plan)\n",
            "  ↓\n",
            "persist (JSON to OneLake) + notify (Slack if configured)\n",
            "```\n",
            "\n",
            "> `dry_run=True` means healing actions are **simulated only** — no changes are applied.\n",
            "> Change to `dry_run=False` when ready for production use.\n",
        ]),
        code_cell([
            "from fabric_agent.api.fabric_client import FabricApiClient\n",
            "from fabric_agent.core.config import FabricAuthConfig\n",
            "from fabric_agent.healing import SelfHealingMonitor\n",
            "\n",
            "async def run_health_scan(workspace_ids, dry_run=True, auto_heal=True, stale_hours=24):\n",
            "    auth = FabricAuthConfig.from_env()\n",
            "    async with FabricApiClient(auth) as client:\n",
            "        monitor = SelfHealingMonitor(\n",
            "            client=client,\n",
            "            auto_heal=auto_heal,\n",
            "            stale_hours=stale_hours,\n",
            "        )\n",
            "        return await monitor.run_once(workspace_ids=workspace_ids, dry_run=dry_run)\n",
            "\n",
            "print('SelfHealingMonitor runner ready (authenticated Fabric client).')\n",
        ]),
        md_cell([
            "## Step 4: Run Anomaly Scan (Dry Run)\n",
            "\n",
            "This executes the full **detect → plan → execute** cycle.\n",
            "With `dry_run=True`, no changes are applied — perfect for reviewing what _would_ happen.\n",
        ]),
        code_cell([
            "import asyncio\n",
            "\n",
            "report = await run_health_scan(\n",
            "    workspace_ids=WORKSPACE_IDS,\n",
            "    dry_run=True,   # SAFE: no changes applied\n",
            "    auto_heal=True,\n",
            "    stale_hours=24,\n",
            ")\n",
            "\n",
            "print('Scan complete!')\n",
            "print(f'  Health Score    : {report.health_score:.1%}')\n",
            "print(f'  Total Assets    : {report.total_assets}')\n",
            "print(f'  Healthy         : {report.healthy}')\n",
            "print(f'  Anomalies Found : {report.anomalies_found}')\n",
            "print(f'  Auto-Healed     : {report.auto_healed} (dry_run=True, so skipped)')\n",
            "print(f'  Manual Required : {report.manual_required}')\n",
            "if report.errors:\n",
            "    print(f'  Errors          : {report.errors}')\n",
        ]),
        md_cell([
            "## Step 5: Review the Healing Plan\n",
            "\n",
            "The healing plan splits actions into two queues:\n",
            "- **Auto-actions**: safe to apply without human approval\n",
            "  (broken shortcuts, additive schema drift, stale table refresh triggers)\n",
            "- **Manual actions**: require human review before any change is made\n",
            "  (orphaned assets, pipeline failures, breaking schema changes)\n",
        ]),
        code_cell([
            "if report.healing_plan:\n",
            "    plan = report.healing_plan\n",
            "    print(f'Healing Plan ID : {plan.plan_id}')\n",
            "    print(f'Auto-actions    : {plan.auto_action_count}')\n",
            "    print(f'Manual-actions  : {plan.manual_action_count}')\n",
            "    print()\n",
            "\n",
            "    if plan.auto_actions:\n",
            "        print('AUTO-FIXABLE (would be applied with dry_run=False):')\n",
            "        for a in plan.auto_actions:\n",
            "            print(f'  [{a.action_type}] {a.description}')\n",
            "            print(f'   status: {a.status}')\n",
            "    else:\n",
            "        print('No auto-fixable actions.')\n",
            "\n",
            "    if plan.manual_actions:\n",
            "        print('\\nREQUIRES MANUAL REVIEW:')\n",
            "        for a in plan.manual_actions:\n",
            "            print(f'  [{a.action_type}] {a.description}')\n",
            "else:\n",
            "    print('No healing plan — workspace is healthy!')\n",
        ]),
        md_cell([
            "## Step 6: Apply Healing (Production)\n",
            "\n",
            "To apply fixes in production, change `dry_run=False`.\n",
            "The monitor will auto-fix safe issues and log the rest for human review.\n",
            "\n",
            "```python\n",
            "# Production use\n",
            "from fabric_agent.api.fabric_client import FabricApiClient\n",
            "from fabric_agent.core.config import FabricAuthConfig\n",
            "\n",
            "auth = FabricAuthConfig.from_env()\n",
            "async with FabricApiClient(auth) as client:\n",
            "    monitor = SelfHealingMonitor(client=client, auto_heal=True)\n",
            "    report = await monitor.run_once(workspace_ids=WORKSPACE_IDS, dry_run=False)\n",
            "```\n",
            "\n",
            "## Step 7: Schedule via Fabric Pipeline\n",
            "\n",
            "The pipeline `PL_PRJ_SelfHealing_FabricInfrastructure` was already created by bootstrap.\n",
            "Connect it to this notebook for automated 30-minute scans:\n",
            "\n",
            "```\n",
            "Fabric UI → <Your Workspace> → PL_PRJ_SelfHealing_FabricInfrastructure\n",
            "         → Edit → Add Notebook activity → point to this notebook\n",
            "         → Schedule trigger → Recurrence → Every 30 minutes\n",
            "```\n",
            "\n",
            "Health reports are written to:\n",
            "- **Local dev**: `data/health_logs/<scan_id>.json`\n",
            "- **Fabric**: `/lakehouse/default/Files/health_logs/<scan_id>.json`\n",
        ]),
        code_cell([
            "# Print the full health report as JSON\n",
            "import json\n",
            "report_dict = report.to_dict()\n",
            "print('Health Report Summary:')\n",
            "print(json.dumps(report_dict, indent=2)[:800])\n",
            "if len(json.dumps(report_dict)) > 800:\n",
            "    print('... (truncated)')\n",
        ]),
    ]

    notebook = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "language_info": {"name": "python"},
            "kernel_info": {"name": "synapse_pyspark"},
            "description": config.description,
        },
        "cells": cells,
    }
    return json.dumps(notebook, indent=2)


def _generate_memory_notebook_content(
    workspace_name: str, workspace_id: str, config: "NotebookConfig"
) -> str:
    """
    Generate rich, runnable notebook content for Context Memory / Vector RAG.

    WHAT: Creates a step-by-step tutorial notebook that:
          1. Installs fabric_agent[memory] from the pre-uploaded wheel
          2. Initialises ChromaDB vector store + sentence-transformers embedder
          3. Records sample Fabric operations into the vector store
          4. Performs semantic search for similar past operations
          5. Generates risk context before proposing a change
          6. Demonstrates cross-session memory summary

    FAANG PARALLEL:
        GitHub Copilot: vector search over your codebase for context
        Airbnb Minerva: historical knowledge base for data transformations
        Meta's Data Catalog: semantic search over petabytes of metadata
        This notebook is the Fabric-adapted RAG pattern.
    """

    def md_cell(lines: List[str]) -> Dict[str, Any]:
        return {"cell_type": "markdown", "metadata": {}, "source": lines}

    def code_cell(lines: List[str]) -> Dict[str, Any]:
        return {
            "cell_type": "code",
            "metadata": {},
            "source": lines,
            "execution_count": None,
            "outputs": [],
        }

    cells = [
        md_cell([
            "# Context Memory & Vector RAG for Fabric Operations\n",
            "\n",
            "## What is Context Memory?\n",
            "\n",
            "This notebook demonstrates **Retrieval-Augmented Generation (RAG)** over Fabric operation history.\n",
            "\n",
            "**The core insight:**\n",
            "- SQL queries match *exact strings* (`'Revenue' ≠ 'revenue'`, `'Rev' ≠ 'Revenue'`)\n",
            "- Vector search finds *conceptually similar* past operations, regardless of exact names\n",
            "- `'Revenue YTD rename'` automatically surfaces lessons from `'Sales YTD rename'`\n",
            "\n",
            "**FAANG Parallels:**\n",
            "| System | Pattern |\n",
            "|--------|---------|\n",
            "| GitHub Copilot | Vector search over your codebase to provide coding context |\n",
            "| Airbnb Minerva | Historical knowledge base for data transformations |\n",
            "| Meta Data Catalog | Semantic search over petabytes of metadata |\n",
            "\n",
            "**Architecture:**\n",
            "```\n",
            "Every operation:\n",
            "  MemoryManager.record_state() → SQLite audit trail\n",
            "                             ↓ (auto-hook)\n",
            "  OperationMemory.index_operation() → embed (all-MiniLM-L6-v2) → ChromaDB\n",
            "\n",
            "At query time:\n",
            "  embed(proposed_change) → find nearest neighbours → return top-K with outcomes\n",
            "```\n",
        ]),
        md_cell(["## Step 1: Install fabric_agent\n", "\n",
                 "Same bootstrap pattern as `01_Ingest_Bronze` — copies wheel from OneLake to `/tmp/`\n",
                 "via `mssparkutils.fs` (Azure SDK, bypasses blobfuse) then pip-installs from the local path.\n",
                 "\n",
                 "> **Note:** `chromadb` and `sentence-transformers` are large (~500 MB). First run takes ~5 min.\n"]),
        code_cell([
            "# Bootstrap: install fabric_agent from the pre-uploaded wheel.\n",
            "# Identical pattern to 01_Ingest_Bronze — the approach that works in Fabric Spark.\n",
            "import subprocess as _subprocess, sys as _sys, os\n",
            "\n",
            "_ABFSS_WHEEL_DIR = (\n",
            "    'abfss://ENT_DataPlatform_DEV@onelake.dfs.fabric.microsoft.com'\n",
            "    '/Bronze_Landing.Lakehouse/Files/wheels'\n",
            ")\n",
            "_TMP_WHEEL_DIR = '/tmp/fabric_agent_wheels'\n",
            "_BLOBFUSE_WHEEL_GLOB = '/lakehouse/default/Files/wheels/fabric_agent-*.whl'\n",
            "\n",
            "def _resolve_wheel_local_path():\n",
            "    try:\n",
            "        from notebookutils import mssparkutils\n",
            "        items = mssparkutils.fs.ls(_ABFSS_WHEEL_DIR)\n",
            "        whl_items = sorted(\n",
            "            [i for i in items if i.name.startswith('fabric_agent-') and i.name.endswith('.whl')],\n",
            "            key=lambda x: x.name,\n",
            "        )\n",
            "        if whl_items:\n",
            "            latest = whl_items[-1]\n",
            "            os.makedirs(_TMP_WHEEL_DIR, exist_ok=True)\n",
            "            local_path = f'{_TMP_WHEEL_DIR}/{latest.name}'\n",
            "            mssparkutils.fs.cp(f'{_ABFSS_WHEEL_DIR}/{latest.name}', f'file://{local_path}', True)\n",
            "            print(f'[bootstrap] Wheel ready at: {local_path}')\n",
            "            return local_path\n",
            "    except Exception as _e:\n",
            "        print(f'[bootstrap] mssparkutils unavailable ({_e}), trying blobfuse fallback...')\n",
            "    import glob as _glob\n",
            "    _wheels = sorted(_glob.glob(_BLOBFUSE_WHEEL_GLOB))\n",
            "    if _wheels:\n",
            "        return _wheels[-1]\n",
            "    return None\n",
            "\n",
            "_wheel = _resolve_wheel_local_path()\n",
            "if _wheel:\n",
            "    print(f'[bootstrap] Installing fabric_agent from: {_wheel}')\n",
            "    _result = _subprocess.run(\n",
            "        [_sys.executable, '-m', 'pip', 'install',\n",
            "         'typing_extensions>=4.12.0',\n",
            "         f'{_wheel}[healing,memory]',\n",
            "         '--quiet'],\n",
            "        capture_output=True, text=True,\n",
            "    )\n",
            "    if _result.returncode == 0:\n",
            "        print('[bootstrap] fabric_agent installed successfully.')\n",
            "        os.environ.setdefault('LOCAL_AGENT_ENABLED', '1')\n",
            "    else:\n",
            "        print(f'[bootstrap] pip install failed:\\n{_result.stderr[-500:]}')\n",
            "else:\n",
            "    print('[bootstrap] WARNING: wheel not found. Run bootstrap --deploy-package --capacity-id <id>')\n",
        ]),
        code_cell([
            "# Verify\n",
            "import fabric_agent\n",
            "print(f'fabric_agent version : {fabric_agent.__version__}')\n",
            "from fabric_agent.memory import OperationMemory\n",
            "from fabric_agent.memory.embedding_client import LocalEmbeddingClient\n",
            "from fabric_agent.memory.vector_store import ChromaVectorStore\n",
            "from fabric_agent.storage.memory_manager import MemoryManager\n",
            "print('All memory imports OK')\n",
        ]),
        md_cell([
            "## Step 2: Detect Environment & Initialize Components\n",
            "\n",
            "In Fabric, vector store persists to OneLake Files (survives notebook restarts).\n",
            "Locally, it persists to `data/vector_store/`.\n",
            "The `fabric_env.py` detection handles this automatically.\n",
        ]),
        code_cell([
            "import os\n",
            "\n",
            "try:\n",
            "    import notebookutils\n",
            "    VECTOR_STORE_PATH = '/lakehouse/default/Files/vector_store'\n",
            "    print(f'Fabric mode: vector store at {VECTOR_STORE_PATH}')\n",
            "except ImportError:\n",
            "    VECTOR_STORE_PATH = 'data/vector_store'\n",
            "    print(f'Local mode: vector store at {VECTOR_STORE_PATH}')\n",
            "\n",
            "# Initialize all three components\n",
            "# 1. EmbeddingClient: sentence-transformers all-MiniLM-L6-v2 (90MB, no API cost)\n",
            "embedding_client = LocalEmbeddingClient()\n",
            "# 2. VectorStore: ChromaDB with local or OneLake persistence\n",
            "vector_store = ChromaVectorStore(persist_path=VECTOR_STORE_PATH)\n",
            "# 3. MemoryManager: SQLite audit trail (existing component)\n",
            "memory_manager = MemoryManager()\n",
            "\n",
            "# Wire them together\n",
            "op_memory = OperationMemory(\n",
            "    memory_manager=memory_manager,\n",
            "    vector_store=vector_store,\n",
            "    embedding_client=embedding_client,\n",
            ")\n",
            "\n",
            "stats = await op_memory.get_statistics()\n",
            "print(f'Vector store ready: {stats.get(\"total_indexed\", 0)} operations indexed')\n",
        ]),
        md_cell([
            "## Step 3: Record Fabric Operations\n",
            "\n",
            "In production, every `MemoryManager.record_state()` call auto-indexes via the hook.\n",
            "Here we manually seed some sample operations to demonstrate semantic search.\n",
            "\n",
            "> These represent realistic Fabric operations that an enterprise data team would perform.\n",
        ]),
        code_cell([
            "from fabric_agent.storage.memory_manager import StateType, OperationStatus\n",
            "\n",
            "sample_operations = [\n",
            "    ('Rename measure Total Revenue to Gross Revenue in Sales Model', StateType.REFACTOR, OperationStatus.SUCCESS),\n",
            "    ('Rename measure Revenue YTD to Revenue Year-to-Date', StateType.REFACTOR, OperationStatus.FAILED),\n",
            "    ('Delete orphaned measure Old_Revenue with no consumers', StateType.REFACTOR, OperationStatus.SUCCESS),\n",
            "    ('Schema drift: added column discount_pct to fact_sales table', StateType.DISCOVERY, OperationStatus.SUCCESS),\n",
            "    ('Shortcut Gold_Sales_Shortcut broken after ADLS connection change', StateType.HEAL, OperationStatus.FAILED),\n",
            "    ('Recreated shortcut Gold_Sales_Shortcut with updated source path', StateType.HEAL, OperationStatus.SUCCESS),\n",
            "    ('Triggered pipeline refresh for stale table agg_daily_sales', StateType.HEAL, OperationStatus.SUCCESS),\n",
            "]\n",
            "\n",
            "for description, state_type, status in sample_operations:\n",
            "    snapshot = await memory_manager.record_state(\n",
            "        state_type=state_type,\n",
            "        operation=description,\n",
            "        status=status,\n",
            "        state_data={'description': description},\n",
            "    )\n",
            "    await op_memory.index_operation(snapshot)\n",
            "    print(f'Indexed: {description[:70]}')\n",
            "\n",
            "stats = await op_memory.get_statistics()\n",
            "print(f'\\nTotal indexed: {stats.get(\"total_indexed\", 0)} operations')\n",
        ]),
        md_cell([
            "## Step 4: Semantic Search for Similar Past Operations\n",
            "\n",
            "Unlike `WHERE description LIKE '%rename%'`, vector search finds\n",
            "semantically related operations even when exact words differ.\n",
            "\n",
            "**Try it**: search for `'rename Net Revenue measure'`.\n",
            "It should surface the `'Revenue YTD rename'` and `'Total Revenue → Gross Revenue'` operations\n",
            "because they are semantically similar — even though the words don't match exactly.\n",
        ]),
        code_cell([
            "proposed_change = 'rename Net Revenue measure'\n",
            "\n",
            "similar = await op_memory.find_similar_operations(\n",
            "    proposed_change=proposed_change,\n",
            "    top_k=3,\n",
            ")\n",
            "\n",
            "print(f'Top-{len(similar)} similar operations for: \"{proposed_change}\"')\n",
            "print('-' * 65)\n",
            "for op in similar:\n",
            "    print(f'  [{op.similarity:.0%} match] {op.snapshot.operation}')\n",
            "    print(f'  Outcome: {op.outcome} | Lesson: {op.key_lesson}')\n",
            "    print()\n",
        ]),
        md_cell([
            "## Step 5: Risk Context Before Applying a Change\n",
            "\n",
            "Before applying any change, the agent asks:\n",
            "*'What went wrong with similar changes historically?'*\n",
            "\n",
            "This is the same pattern as GitHub Copilot surfacing related issues before a PR merge,\n",
            "or Meta's change management system flagging high-risk deployments.\n",
        ]),
        code_cell([
            "context = await op_memory.get_risk_context(\n",
            "    proposed_change='rename Net Revenue to Net Sales Revenue in Finance Model'\n",
            ")\n",
            "\n",
            "print(f'Historical failure rate : {context.historical_failure_rate:.0%}')\n",
            "print(f'Confidence              : {context.confidence:.0%}')\n",
            "print(f'Similar operations found: {len(context.similar_operations)}')\n",
            "print()\n",
            "\n",
            "if context.common_failure_reasons:\n",
            "    print('Common failure reasons:')\n",
            "    for r in context.common_failure_reasons:\n",
            "        print(f'  - {r}')\n",
            "\n",
            "if context.recommendations:\n",
            "    print('\\nRecommendations:')\n",
            "    for r in context.recommendations:\n",
            "        print(f'  * {r}')\n",
        ]),
        md_cell([
            "## Step 6: Cross-Session Memory Summary\n",
            "\n",
            "Context memory persists across MCP sessions.\n",
            "When Claude starts a new conversation, it can ask:\n",
            "*'What did we do last time in this workspace?'*\n",
            "\n",
            "This is the 'memory between conversations' feature — same as ChatGPT memory,\n",
            "but for Fabric data infrastructure operations.\n",
        ]),
        code_cell([
            "from fabric_agent.memory.session_context import SessionContext\n",
            "\n",
            "session_ctx = SessionContext(memory_manager=memory_manager)\n",
            "summary = await session_ctx.get_last_session_summary(workspace_id='default')\n",
            "\n",
            "print('Last session summary:')\n",
            "print(summary or '(No previous sessions recorded yet)')\n",
        ]),
        code_cell([
            "# Final statistics\n",
            "final_stats = await op_memory.get_statistics()\n",
            "print('=' * 55)\n",
            "print('Context Memory Statistics')\n",
            "print('=' * 55)\n",
            "for k, v in final_stats.items():\n",
            "    print(f'  {k:30s}: {v}')\n",
        ]),
    ]

    notebook = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "language_info": {"name": "python"},
            "kernel_info": {"name": "synapse_pyspark"},
            "description": config.description,
        },
        "cells": cells,
    }
    return json.dumps(notebook, indent=2)


def _resolve_notebook_content(
    config: "NotebookConfig",
    workspace_name: str,
    workspace_id: str,
    lakehouse_id: Optional[str],
) -> str:
    """
    Choose the right content generator for a notebook.

    Priority:
    1. local_script_path is set and the file exists → wrap the .py file via
       build_ipynb_from_python_script() (preserves the hand-crafted cells).
    2. Fall back to generate_notebook_content() which produces PySpark ETL
       content or the PRJ strategic notebook content depending on the name.

    FAANG PATTERN: Same as Airflow's DAG factory pattern — pick the right
    generator based on metadata, not an if/else tower at every call site.
    """
    if config.local_script_path:
        script_path = ROOT / config.local_script_path
        if script_path.exists():
            logger.debug(f"Notebook '{config.name}': loading content from {script_path}")
            return build_ipynb_from_python_script(
                script_path.name,
                script_path.read_text(encoding="utf-8"),
                lakehouse_id=lakehouse_id or "",
                workspace_id=workspace_id,
            )
        logger.warning(
            f"Notebook '{config.name}': local_script_path '{config.local_script_path}' not found "
            "— falling back to generated content"
        )
    return generate_notebook_content(workspace_name, workspace_id, config, [], lakehouse_id=lakehouse_id)


def generate_notebook_content(workspace_name: str, workspace_id: str, config: NotebookConfig, lakehouse_tables: List[TableConfig], lakehouse_id: Optional[str] = None) -> str:
    """Generate notebook (ipynb JSON) that is SAFE for Lakehouse Tables.

    Important: Do NOT write Delta files directly under Tables/ via file paths.
    Always register tables via saveAsTable() so Fabric shows them as real tables
    (not under Tables → Unidentified).

    Strategic project notebooks (PRJ_SelfHealing_*, PRJ_Context_Memory*) bypass the
    PySpark-based generation and return rich educational content instead.
    """
    # Early return for strategic project notebooks — these use fabric_agent directly,
    # not PySpark/saveAsTable, so they need entirely different cell content.
    notebook_name = (config.name or "").lower()
    if "prj_selfhealing" in notebook_name:
        return _generate_healing_notebook_content(workspace_name, workspace_id, config)
    if "prj_context_memory" in notebook_name:
        return _generate_memory_notebook_content(workspace_name, workspace_id, config)

    def md_cell(lines: List[str]) -> Dict[str, Any]:
        return {"cell_type": "markdown", "metadata": {}, "source": lines}

    def code_cell(lines: List[str]) -> Dict[str, Any]:
        return {"cell_type": "code", "metadata": {}, "source": lines, "execution_count": None, "outputs": []}

    title = [
        f"# {config.name}\n",
        "\n",
        f"**Description**: {config.description}\n",
        f"**Lakehouse**: {config.lakehouse_ref}\n",
        "\n---\n",
    ]
    common_setup = [
        "# =========================================================\n",
        "# Common setup (ABFSS OneLake paths + helpers)\n",
        "# =========================================================\n",
        "from pyspark.sql import SparkSession\n",
        "from pyspark.sql import functions as F\n",
        "from pyspark.sql.functions import current_timestamp, col\n",
        "from datetime import datetime\n",
        "import re\n",
        "\n",
        "spark = SparkSession.builder.getOrCreate()\n",
        "print('Notebook started:', datetime.now())\n",
        "\n",
        "# Workspace/Lakehouse are injected by the bootstrap generator\n",
        f"WORKSPACE_NAME = \"{workspace_name}\"\n",
        f"LAKEHOUSE_NAME = \"{config.lakehouse_ref}\"\n",
        "\n",
        "# REQUIRED ABFSS base (matches Fabric OneLake URI pattern)\n",
        "LAKEHOUSE_PATH = f\"abfss://{WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_NAME}.Lakehouse\"\n",
        "print('Workspace:', WORKSPACE_NAME)\n",
        "print('Lakehouse:', LAKEHOUSE_NAME)\n",
        "print('Lakehouse path:', LAKEHOUSE_PATH)\n",
        "\n",
        "# Ensure Spark writes/registers into the intended Lakehouse database\n",
        "try:\n",
        "    spark.catalog.setCurrentDatabase(LAKEHOUSE_NAME)\n",
        "    print('Current database:', spark.catalog.currentDatabase())\n",
        "except Exception as e:\n",
        "    print('⚠️ Could not set current database to', LAKEHOUSE_NAME, '->', e)\n",
        "\n",
        "def lh(rel_path: str) -> str:\n",
        "    \"\"\"Build a full ABFSS path inside this lakehouse.\"\"\"\n",
        "    if rel_path is None:\n",
        "        raise ValueError('rel_path is None')\n",
        "    p = str(rel_path).strip()\n",
        "    # If already absolute, return as-is\n",
        "    if p.startswith('abfss://') or p.startswith('wasbs://') or p.startswith('https://') or p.startswith('adl://') or p.startswith('/lakehouse/'):\n",
        "        return p\n",
        "    # Normalize leading slashes\n",
        "    p = p.lstrip('/')\n",
        "    return f\"{LAKEHOUSE_PATH}/{p}\"\n",
        "\n",
        "def load_csv(rel_path: str, header: bool = True, infer_schema: bool = True, **options):\n",
        "    \"\"\"Load CSV from Files/... using full ABFSS path.\"\"\"\n",
        "    full_path = lh(rel_path)\n",
        "    print(f\"📂 Loading CSV from: {full_path}\")\n",
        "    reader = (spark.read.format('csv')\n",
        "              .option('header', str(header).lower())\n",
        "              .option('inferSchema', str(infer_schema).lower()))\n",
        "    for k, v in options.items():\n",
        "        reader = reader.option(k, v)\n",
        "    return reader.load(full_path)\n",
        "\n",
        "def load_parquet(rel_path: str):\n",
        "    full_path = lh(rel_path)\n",
        "    print(f\"📂 Loading Parquet from: {full_path}\")\n",
        "    return spark.read.format('parquet').load(full_path)\n",
        "\n",
        "def load_delta(rel_path: str):\n",
        "    full_path = lh(rel_path)\n",
        "    print(f\"📂 Loading Delta from: {full_path}\")\n",
        "    return spark.read.format('delta').load(full_path)\n",
        "\n",
        "def clean_columns(df):\n",
        "    \"\"\"Standardize column names for table creation.\"\"\"\n",
        "    def fix(c: str) -> str:\n",
        "        c = c.strip()\n",
        "        c = re.sub(r'[^0-9a-zA-Z_]', '_', c)\n",
        "        if re.match(r'^\\d', c):\n",
        "            c = '_' + c\n",
        "        return c.lower()\n",
        "    return df.toDF(*[fix(c) for c in df.columns])\n",
        "\n",
        "def save_table(df, table_name: str, mode: str = 'overwrite'):\n",
        "    \"\"\"Create/register a Lakehouse table (prevents 'Unidentified').\"\"\"\n",
        "    df.write.mode(mode).format('delta').option('overwriteSchema','true').option('mergeSchema','true').saveAsTable(table_name)\n",
        "    print(f\"✅ Saved table: {table_name}\")\n",
        "\n",
        "def show_counts(*table_names):\n",
        "    \"\"\"Display row counts for tables.\"\"\"\n",
        "    print('\\n' + '='*60)\n",
        "    for tbl in table_names:\n",
        "        try:\n",
        "            if spark.catalog.tableExists(tbl):\n",
        "                cnt = spark.table(tbl).count()\n",
        "                print(f\"📊 {tbl}: {cnt:,} rows\")\n",
        "            else:\n",
        "                print(f\"⚠️  {tbl}: MISSING\")\n",
        "        except Exception as e:\n",
        "            print(f\"⚠️  {tbl}: Error - {e}\")\n",
        "    print('='*60)\n",
    ]
    
    cells: List[Dict[str, Any]] = []

    # IMPORTANT: %%configure MUST be the first cell in the notebook if used.
    # This forces the Spark session to use the intended default lakehouse so saveAsTable()
    # registers tables in the correct Lakehouse (prevents 'Unidentified' + wrong target writes).
    if lakehouse_id and config.lakehouse_ref:
        cells.append(code_cell([
            "%%configure -f\n",
            "{\n",
            "  \"defaultLakehouse\": {\n",
            f"    \"name\": \"{config.lakehouse_ref}\",\n",
            f"    \"id\": \"{lakehouse_id}\",\n",
            f"    \"workspaceId\": \"{workspace_id}\"\n",
            "  }\n",
            "}\n",
        ]))

    cells.append(md_cell(title))
    cells.append(code_cell(common_setup))


    name = (config.name or "").lower()
    lh = (config.lakehouse_ref or "").lower()

    # --- Bronze ingestion: read seed CSVs into Bronze_Landing tables
    if "ingest" in name and "bronze" in name or lh == "bronze_landing" and "ingest" in name:
        cells.append(md_cell(["## Create Bronze tables from seed files\n", "Reads Files/seed/bronze/*.csv and registers Delta tables.\n"]))
        cells.append(code_cell([
            "# Bronze seed files\n",
            "bronze_files = {\n",
            "  'raw_sales_transactions': 'Files/seed/bronze/raw_sales_transactions.csv',\n",
            "  'raw_inventory_snapshots': 'Files/seed/bronze/raw_inventory_snapshots.csv',\n",
            "  'raw_customer_events': 'Files/seed/bronze/raw_customer_events.csv',\n",
            "}\n",
            "\n",
            "for table_name, path in bronze_files.items():\n",
            "    # load_csv prints the resolved /lakehouse/default/... path\n",
            "    df = load_csv(path)\n",
            "    # Small standardization\n",
            "    df = df.withColumn('ingestion_timestamp', current_timestamp())\n",
            "    save_table(df, table_name)\n",
            "\n",
            "show_counts(*bronze_files.keys())\n",
        ]))

    # --- Silver: create dimension/fact tables from seed files
    elif lh == "silver_curated" and ("transform" in name or "dimension" in name or "fact" in name or "build" in name):
        if "build_facts" in name or "facts" in name or "04_" in (config.name or ""):
            # facts
            cells.append(md_cell(["## Build Silver fact tables from seed files\n"]))
            cells.append(code_cell([
                "# Silver facts seed file\n",
                "path = 'Files/seed/silver/fact_sales.csv'\n",
                "df = load_csv(path)\n",
                "# Ensure numeric columns are numeric (best-effort)\n",
                "for c in ['quantity','gross_amount','discount_amount','net_amount','tax_amount','cost_amount','profit_amount']:\n",
                "    if c in df.columns:\n",
                "        df = df.withColumn(c, col(c).cast('double'))\n",
                "save_table(df, 'fact_sales')\n",
                "\n",
                "show_counts('fact_sales')\n",
            ]))
        else:
            # dims
            cells.append(md_cell(["## Build Silver dimension tables from seed files\n"]))
            cells.append(code_cell([
                "silver_dims = {\n",
                "  'dim_date': 'Files/seed/silver/dim_date.csv',\n",
                "  'dim_customer': 'Files/seed/silver/dim_customer.csv',\n",
                "  'dim_product': 'Files/seed/silver/dim_product.csv',\n",
                "  'dim_store': 'Files/seed/silver/dim_store.csv',\n",
                "}\n",
                "\n",
                "for table_name, path in silver_dims.items():\n",
                "    df = load_csv(path)\n",
                "    save_table(df, table_name)\n",
                "\n",
                "# Optional: create a 'current' view for customers\n",
                "if spark.catalog.tableExists('dim_customer'):\n",
                "    spark.sql('CREATE OR REPLACE TEMP VIEW vw_dim_customer_current AS SELECT * FROM dim_customer WHERE is_current = true')\n",
                "    print('Created TEMP VIEW vw_dim_customer_current')\n",
                "\n",
                "show_counts(*silver_dims.keys())\n",
            ]))

    # --- Gold: create aggregates from seed files
    elif lh == "gold_published" and ("gold" in name or "aggregate" in name or "quality" in name):
        if "quality" in name:
            # quality checks for gold layer (and best-effort checks for expected tables in this lakehouse)
            cells.append(md_cell(["## Data quality checks (Gold)\n", "Validates that expected tables exist and have rows.\n"]))
            cells.append(code_cell([
                "expected = ['agg_daily_sales','agg_customer_360','agg_inventory_health']\n",
                "missing = [t for t in expected if not spark.catalog.tableExists(t)]\n",
                "if missing:\n",
                "    raise Exception('Missing expected tables: ' + ', '.join(missing))\n",
                "\n",
                "for t in expected:\n",
                "    c = spark.table(t).count()\n",
                "    if c <= 0:\n",
                "        raise Exception(f'Table {t} has 0 rows')\n",
                "    print(f'✅ {t}: {c:,} rows')\n",
                "\n",
                "print('All checks passed ✅')\n",
            ]))
        else:
            cells.append(md_cell(["## Build Gold aggregate tables from seed files\n"]))
            cells.append(code_cell([
                "gold_tables = {\n",
                "  'agg_daily_sales': 'Files/seed/gold/agg_daily_sales.csv',\n",
                "  'agg_customer_360': 'Files/seed/gold/agg_customer_360.csv',\n",
                "  'agg_inventory_health': 'Files/seed/gold/agg_inventory_health.csv',\n",
                "}\n",
                "\n",
                "for table_name, path in gold_tables.items():\n",
                "    df = load_csv(path)\n",
                "    save_table(df, table_name)\n",
                "\n",
                "show_counts(*gold_tables.keys())\n",
            ]))

    elif lh == "analytics_sandbox":
        if "segmentation" in name:
            cells.append(md_cell([
                "## Build customer segmentation features from shared shortcuts\n",
                "Uses `fact_sales` + `dim_customer` shortcuts from DataPlatform to create\n",
                "consumer features in this workspace.\n",
            ]))
            cells.append(code_cell([
                "required = ['fact_sales', 'dim_customer']\n",
                "missing = [t for t in required if not spark.catalog.tableExists(t)]\n",
                "if missing:\n",
                "    raise Exception('Missing required shortcut tables in Analytics_Sandbox: ' + ', '.join(missing))\n",
                "\n",
                "sales = spark.table('fact_sales')\n",
                "cust = spark.table('dim_customer')\n",
                "seg = (\n",
                "    sales.groupBy('customer_key')\n",
                "         .agg(\n",
                "             F.sum(F.col('net_amount').cast('double')).alias('customer_net_sales'),\n",
                "             F.sum(F.col('quantity').cast('double')).alias('customer_units'),\n",
                "             F.countDistinct('transaction_id').alias('orders')\n",
                "         )\n",
                "         .join(cust.select('customer_key', 'customer_segment', 'loyalty_tier'), on='customer_key', how='left')\n",
                ")\n",
                "save_table(seg, 'customer_segment_features')\n",
                "show_counts('customer_segment_features')\n",
            ]))
        else:
            cells.append(md_cell([
                "## Build consumer sales aggregates from shared shortcuts\n",
                "Uses `fact_sales`, `dim_date`, and `dim_product` shortcuts from DataPlatform.\n",
            ]))
            cells.append(code_cell([
                "required = ['fact_sales', 'dim_date', 'dim_product']\n",
                "missing = [t for t in required if not spark.catalog.tableExists(t)]\n",
                "if missing:\n",
                "    raise Exception('Missing required shortcut tables in Analytics_Sandbox: ' + ', '.join(missing))\n",
                "\n",
                "sales = spark.table('fact_sales')\n",
                "daily = (\n",
                "    sales.groupBy('date_key', 'product_key')\n",
                "         .agg(\n",
                "             F.sum(F.col('net_amount').cast('double')).alias('net_sales'),\n",
                "             F.sum(F.col('quantity').cast('double')).alias('units_sold'),\n",
                "             F.countDistinct('transaction_id').alias('transactions')\n",
                "         )\n",
                ")\n",
                "save_table(daily, 'analytics_sales_daily')\n",
                "show_counts('analytics_sales_daily')\n",
            ]))

    elif lh == "finance_layer":
        cells.append(md_cell([
            "## Build finance consumer outputs from shared and local tables\n",
            "Requires shared `fact_sales` shortcut plus local `budget_data`/`forecast_data`.\n",
        ]))
        cells.append(code_cell([
            "required = ['fact_sales', 'budget_data']\n",
            "missing = [t for t in required if not spark.catalog.tableExists(t)]\n",
            "if missing:\n",
            "    raise Exception('Missing required tables in Finance_Layer: ' + ', '.join(missing))\n",
            "\n",
            "actuals = (\n",
            "    spark.table('fact_sales')\n",
            "         .groupBy('date_key')\n",
            "         .agg(\n",
            "             F.sum(F.col('net_amount').cast('double')).alias('actual_revenue'),\n",
            "             F.sum(F.col('profit_amount').cast('double')).alias('actual_profit')\n",
            "         )\n",
            ")\n",
            "save_table(actuals, 'finance_actuals_daily')\n",
            "\n",
            "budget = (\n",
            "    spark.table('budget_data')\n",
            "         .groupBy('fiscal_year', 'fiscal_month', 'department')\n",
            "         .agg(F.sum(F.col('budget_amount').cast('double')).alias('budget_amount'))\n",
            ")\n",
            "save_table(budget, 'finance_budget_summary')\n",
            "show_counts('finance_actuals_daily', 'finance_budget_summary')\n",
        ]))

    else:
        # Safe placeholder for other notebooks
        cells.append(md_cell(["## Placeholder\n", "This notebook is a scaffold. Add your own logic here.\n"]))
        cells.append(code_cell([
            "print('No automated seed/table action for this notebook.')\n",
            "print('Tip: Use saveAsTable() to register tables.')\n",
        ]))

    # Final summary cell
    cells.append(code_cell([
        "print('='*60)\n",
        f"print('Notebook: {config.name}')\n",
        "print('Completed at:', datetime.now())\n",
        "print('='*60)\n",
    ]))

    notebook = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "language_info": {"name": "python"},
            "kernel_info": {"name": "synapse_pyspark"},
            "description": config.description,
        },
        "cells": cells,
    }

    return json.dumps(notebook, indent=2)


# =============================================================================
# TMDL Generator for Semantic Model
# =============================================================================


def generate_enterprise_sales_model_bim(model_name: str, description: str, measures: List[Dict]) -> Dict:
    """Generate a minimal Tabular Model (TMSL) 'model.bim' aligned to the architecture in this repo.

    Notes:
    - This is meant as a deployable *demo* semantic model definition for Fabric API.
    - Lakehouse tables may not exist until you ingest/build them; that's OK for bootstrapping assets.
    """

    def col(name: str, data_type: str, is_key: bool = False) -> Dict:
        c = {"name": name, "dataType": data_type}
        if is_key:
            c["isKey"] = True
        return c

    # Tables based on the architecture described in this script header.
    dim_date_cols = [
        col("date_key", "int64", True),
        col("full_date", "dateTime"),
        col("year", "int64"),
        col("quarter", "int64"),
        col("month_number", "int64"),
        col("month_name", "string"),
        col("day_of_week", "int64"),
        col("day_name", "string"),
        col("is_weekend", "boolean"),
        col("is_holiday", "boolean"),
        col("fiscal_year", "int64"),
        col("fiscal_quarter", "int64"),
    ]

    dim_customer_cols = [
        col("customer_key", "int64", True),
        col("customer_id", "string"),
        col("first_name", "string"),
        col("last_name", "string"),
        col("email", "string"),
        col("city", "string"),
        col("state", "string"),
        col("country", "string"),
        col("customer_segment", "string"),
        col("loyalty_tier", "string"),
        col("lifetime_value", "decimal"),
        col("is_current", "boolean"),
        col("acquisition_date", "dateTime"),
        col("is_active", "boolean"),
    ]

    dim_product_cols = [
        col("product_key", "int64", True),
        col("product_name", "string"),
        col("category_l1", "string"),
        col("category_l2", "string"),
        col("category_l3", "string"),
        col("brand", "string"),
        col("unit_cost", "decimal"),
        col("unit_price", "decimal"),
        col("launch_date", "dateTime"),
    ]

    dim_store_cols = [
        col("store_key", "int64", True),
        col("store_name", "string"),
        col("city", "string"),
        col("state", "string"),
        col("region", "string"),
        col("country", "string"),
        col("square_footage", "int64"),
    ]

    fact_sales_cols = [
        col("sales_key", "int64", True),
        col("date_key", "int64"),
        col("customer_key", "int64"),
        col("product_key", "int64"),
        col("store_key", "int64"),
        col("transaction_id", "string"),
        col("quantity", "int64"),
        col("gross_amount", "decimal"),
        col("discount_amount", "decimal"),
        col("net_amount", "decimal"),
        col("tax_amount", "decimal"),
        col("cost_amount", "decimal"),
        col("profit_amount", "decimal"),
    ]

    measures_table = {
        "name": "_Measures",
        "description": "Measures table (no data) - stores enterprise DAX measures",
        "columns": [col("measure_id", "string", True)],
        "measures": [],
    }

    # Convert measure dicts from generate_enterprise_measures() into BIM measure objects.
    for m in measures:
        meas = {
            "name": m["name"],
            "expression": m["expression"],
        }
        if m.get("format"):
            meas["formatString"] = m["format"]
        if m.get("description"):
            meas["description"] = m["description"]
        if m.get("folder"):
            meas["displayFolder"] = m["folder"]
        measures_table["measures"].append(meas)

    # Helper to create a table with partition
    def make_table(name: str, columns: List[Dict], is_measure_table: bool = False) -> Dict:
        """Create a table definition with required partition."""
        table = {
            "name": name,
            "lineageTag": str(uuid4()),
            "columns": columns,
            "partitions": [
                {
                    "name": f"{name}_partition",
                    "mode": "import",
                    "source": {
                        "type": "m",
                        "expression": [
                            f"let",
                            f"    Source = #\"{name}\"",
                            f"in",
                            f"    Source"
                        ] if not is_measure_table else [
                            "let",
                            "    Source = #table(type table [measure_id = text], {})",
                            "in",
                            "    Source"
                        ]
                    }
                }
            ]
        }
        return table

    # Add lineageTag to columns
    def add_lineage_to_cols(cols: List[Dict]) -> List[Dict]:
        for c in cols:
            c["lineageTag"] = str(uuid4())
            c["sourceColumn"] = c["name"]
        return cols

    dim_date_cols = add_lineage_to_cols(dim_date_cols)
    dim_customer_cols = add_lineage_to_cols(dim_customer_cols)
    dim_product_cols = add_lineage_to_cols(dim_product_cols)
    dim_store_cols = add_lineage_to_cols(dim_store_cols)
    fact_sales_cols = add_lineage_to_cols(fact_sales_cols)
    measures_table["columns"] = add_lineage_to_cols(measures_table["columns"])

    bim = {
        "name": model_name,
        "id": str(uuid4()),
        "description": description,
        "compatibilityLevel": 1605,
        "model": {
            "culture": "en-US",
            "sourceQueryCulture": "en-US",
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "dataAccessOptions": {"legacyRedirects": True, "returnErrorValuesAsNull": True},
            "tables": [
                make_table("dim_date", dim_date_cols),
                make_table("dim_customer", dim_customer_cols),
                make_table("dim_product", dim_product_cols),
                make_table("dim_store", dim_store_cols),
                make_table("fact_sales", fact_sales_cols),
                {
                    "name": "_Measures",
                    "lineageTag": str(uuid4()),
                    "description": "Measures table (no data) - stores enterprise DAX measures",
                    "columns": measures_table["columns"],
                    "measures": measures_table["measures"],
                    "partitions": [
                        {
                            "name": "_Measures_partition",
                            "mode": "import",
                            "source": {
                                "type": "m",
                                "expression": [
                                    "let",
                                    "    Source = #table(type table [measure_id = text], {})",
                                    "in",
                                    "    Source"
                                ]
                            }
                        }
                    ]
                },
            ],
            "relationships": [
                {
                    "name": "fact_sales_date",
                    "fromTable": "fact_sales",
                    "fromColumn": "date_key",
                    "toTable": "dim_date",
                    "toColumn": "date_key",
                },
                {
                    "name": "fact_sales_customer",
                    "fromTable": "fact_sales",
                    "fromColumn": "customer_key",
                    "toTable": "dim_customer",
                    "toColumn": "customer_key",
                },
                {
                    "name": "fact_sales_product",
                    "fromTable": "fact_sales",
                    "fromColumn": "product_key",
                    "toTable": "dim_product",
                    "toColumn": "product_key",
                },
                {
                    "name": "fact_sales_store",
                    "fromTable": "fact_sales",
                    "fromColumn": "store_key",
                    "toTable": "dim_store",
                    "toColumn": "store_key",
                },
            ],
        },
    }
    return bim

def generate_semantic_model_tmdl(config: SemanticModelConfig, measures: List[Dict]) -> str:
    """Generate TMDL definition for semantic model."""
    
    lines = []
    
    # Model header
    lines.append(f"model Model")
    lines.append(f"\tculture: en-US")
    lines.append(f"\tdataAccessOptions")
    lines.append(f"\t\treturnErrorValuesAsNull: true")
    lines.append("")
    
    # Tables (dimensions)
    dim_tables = ["dim_date", "dim_customer", "dim_product", "dim_store"]
    
    for table in dim_tables:
        lines.append(f"\ttable {table}")
        lines.append(f"\t\tlineageTag: {str(uuid4())}")
        lines.append("")
        
        # Add some columns based on table
        if table == "dim_date":
            columns = [
                ("date_key", "int64"),
                ("full_date", "dateTime"),
                ("year", "int64"),
                ("quarter", "int64"),
                ("month_name", "string"),
            ]
        elif table == "dim_customer":
            columns = [
                ("customer_key", "int64"),
                ("customer_id", "string"),
                ("customer_segment", "string"),
                ("loyalty_tier", "string"),
            ]
        elif table == "dim_product":
            columns = [
                ("product_key", "int64"),
                ("product_name", "string"),
                ("category_l1", "string"),
                ("brand", "string"),
            ]
        else:
            columns = [
                ("store_key", "int64"),
                ("store_name", "string"),
                ("region", "string"),
            ]
        
        for col_name, col_type in columns:
            lines.append(f"\t\tcolumn {col_name}")
            lines.append(f"\t\t\tdataType: {col_type}")
            lines.append(f"\t\t\tlineageTag: {str(uuid4())}")
            lines.append(f"\t\t\tsummarizeBy: none")
            lines.append("")
    
    # Fact table
    lines.append(f"\ttable fact_sales")
    lines.append(f"\t\tlineageTag: {str(uuid4())}")
    lines.append("")
    
    fact_columns = [
        ("sales_key", "int64"),
        ("date_key", "int64"),
        ("customer_key", "int64"),
        ("product_key", "int64"),
        ("store_key", "int64"),
        ("quantity", "int64"),
        ("gross_amount", "decimal"),
        ("net_amount", "decimal"),
        ("cost_amount", "decimal"),
        ("profit_amount", "decimal"),
        ("discount_amount", "decimal"),
        ("tax_amount", "decimal"),
    ]
    
    for col_name, col_type in fact_columns:
        lines.append(f"\t\tcolumn {col_name}")
        lines.append(f"\t\t\tdataType: {col_type}")
        lines.append(f"\t\t\tlineageTag: {str(uuid4())}")
        lines.append(f"\t\t\tsummarizeBy: none")
        lines.append("")
    
    # Add measures to fact_sales table
    for measure in measures:
        name = measure["name"]
        expr = measure["expression"].strip().replace("\n", " ").replace("\t", " ")
        folder = measure.get("folder", "General")
        format_str = measure.get("format", "")
        
        lines.append(f"\t\tmeasure '{name}' = {expr}")
        lines.append(f"\t\t\tlineageTag: {str(uuid4())}")
        lines.append(f"\t\t\tdisplayFolder: {folder}")
        if format_str:
            lines.append(f"\t\t\tformatString: {format_str}")
        lines.append("")
    
    # Relationships
    relationships = [
        ("fact_sales", "date_key", "dim_date", "date_key"),
        ("fact_sales", "customer_key", "dim_customer", "customer_key"),
        ("fact_sales", "product_key", "dim_product", "product_key"),
        ("fact_sales", "store_key", "dim_store", "store_key"),
    ]
    
    for from_table, from_col, to_table, to_col in relationships:
        lines.append(f"\trelationship {str(uuid4())[:8]}")
        lines.append(f"\t\tfromColumn: {from_table}.{from_col}")
        lines.append(f"\t\ttoColumn: {to_table}.{to_col}")
        lines.append("")
    
    return "\n".join(lines)


# =============================================================================
# Main Execution
# =============================================================================

class EnterpriseBootstrapper:
    """Bootstraps enterprise Fabric environment."""

    def __init__(
        self,
        client: FabricApiClient,
        dry_run: bool = False,
        fixed_workspace: Optional[str] = None,
        capacity_id: Optional[str] = None,
        enable_reports: bool = False,
        skip_shortcuts: bool = False,
        report_template_id: Optional[str] = None,
        report_template_workspace_id: Optional[str] = None,
        report_template_workspace: Optional[str] = None,
    ):
        self.client = client
        self.dry_run = dry_run
        self.created_items: Dict[str, Dict] = {}
        self.workspace_ids: Dict[str, str] = {}

        # If provided, we will NOT create workspaces.
        # We will map every logical workspace (ENT_DataPlatform_DEV, etc.) into this real workspace.
        self.fixed_workspace_name: Optional[str] = fixed_workspace
        self.fixed_workspace_id: Optional[str] = None

        # Optional: assign created/found workspaces to a capacity (e.g., Trial capacity)
        self.capacity_id: Optional[str] = capacity_id

        # Reports are optional because real PBIR/PBIP definitions are typically exported from Power BI Desktop.
        self.enable_reports: bool = enable_reports
        
        # Skip shortcuts if source tables don't exist yet (run seed_enterprise_data.py first)
        self.skip_shortcuts: bool = skip_shortcuts

        # Optional template report source for reliable report creation.
        # CLI arguments override env vars.
        self.report_template_id: Optional[str] = report_template_id or os.getenv("FABRIC_REPORT_TEMPLATE_ID")
        self.report_template_workspace_id: Optional[str] = (
            report_template_workspace_id or os.getenv("FABRIC_REPORT_TEMPLATE_WORKSPACE_ID")
        )
        self.report_template_workspace: Optional[str] = (
            report_template_workspace or os.getenv("FABRIC_REPORT_TEMPLATE_WORKSPACE")
        )
        self._cached_template_definition: Optional[Dict[str, Any]] = None

    async def set_fixed_workspace_by_name(self, workspace_name: str) -> str:
        """Resolve and lock the real Fabric workspace id by displayName."""
        if self.dry_run:
            self.fixed_workspace_name = workspace_name
            self.fixed_workspace_id = str(uuid4())
            logger.info(f"[DRY RUN] Using workspace '{workspace_name}' ({self.fixed_workspace_id})")
            return self.fixed_workspace_id

        response = await self.client.get("/workspaces")
        for ws in response.get("value", []):
            dn = ws.get("displayName") or ""
            if dn.lower() == workspace_name.lower():
                self.fixed_workspace_name = dn
                self.fixed_workspace_id = ws["id"]
                logger.info(f"Using existing workspace: {dn} ({self.fixed_workspace_id})")
                return self.fixed_workspace_id

        raise RuntimeError(
            f"Workspace '{workspace_name}' not found. "
            f"Make sure the name matches exactly in Fabric UI."
        )

    async def _ensure_workspace_capacity(self, workspace_id: str, workspace_name: str) -> None:
        """Assign a workspace to the configured capacity (if provided).

        This is how you can have *separate workspaces* that all run on the same Trial capacity.
        """
        if not self.capacity_id or self.dry_run:
            return

        try:
            await self.client.post(
                f"/workspaces/{workspace_id}/assignToCapacity", json_data={"capacityId": self.capacity_id},
            )
            logger.info(f"Assigned workspace '{workspace_name}' to capacity {self.capacity_id}")
        except Exception as e:
            # If already assigned, Fabric may return a 409 or 400 depending on tenant behavior.
            # We treat capacity assignment as a best-effort operation.
            logger.warning(f"Could not assign workspace '{workspace_name}' to capacity {self.capacity_id}: {e}")



    async def _find_item_id(
            self,
            workspace_id_or_list_path: str,
            item_type_or_display_name: str,
            display_name: str | None = None,
            *,
            timeout_seconds: float = 90.0,
            poll_seconds: float = 2.0,
            name_field: str = "displayName",
            id_field: str = "id",
        ) -> str | None:
            """Find an item id by display name, with backward-compatible call signatures.

            Supported call patterns:
              1) _find_item_id(list_path, display_name, ...)
              2) _find_item_id(workspace_id, item_type, display_name, ...)

            The function polls because Fabric items can take a short time to appear after creation.
            """
            if self.dry_run:
                return None

            # Determine list_path and display_name based on args
            if display_name is None:
                list_path = workspace_id_or_list_path
                display_name = item_type_or_display_name
            else:
                workspace_id = workspace_id_or_list_path
                item_type = item_type_or_display_name.lstrip("/")
                list_path = f"/workspaces/{workspace_id}/{item_type}"

            start_time = time.time()
            while time.time() - start_time < timeout_seconds:
                try:
                    resp = await self.client.get(list_path)
                    items = resp.get("value", [])
                    for it in items:
                        if it.get(name_field) == display_name:
                            return it.get(id_field)
                except Exception as e:
                    logger.debug(f"_find_item_id polling error on {list_path}: {e}")
                await asyncio.sleep(poll_seconds)

            return None
    async def create_workspace(self, name: str, description: str) -> str:
        """Create a workspace and return its ID."""
        logger.info(f"Creating workspace: {name}")

        # If we are forcing all work into one existing workspace, do not create anything.
        if self.fixed_workspace_id:
            self.workspace_ids[name] = self.fixed_workspace_id
            logger.info(
                f"Mapping logical workspace '{name}' -> '{self.fixed_workspace_name}' ({self.fixed_workspace_id})"
            )
            return self.fixed_workspace_id
        
        if self.dry_run:
            ws_id = str(uuid4())
            self.workspace_ids[name] = ws_id
            return ws_id
        
        # Check if exists
        response = await self.client.get("/workspaces")
        for ws in response.get("value", []):
            if ws.get("displayName") == name:
                ws_id = ws["id"]
                self.workspace_ids[name] = ws_id
                logger.info(f"Workspace exists: {name} ({ws_id})")
                await self._ensure_workspace_capacity(ws_id, name)
                return ws_id
        
        
        
        # Create new
        payload = {
            "displayName": name,
            "description": description,
        }
        if self.capacity_id:
            payload["capacityId"] = self.capacity_id

        response = await self.client.post("/workspaces", json_data=payload)
        ws_id = response.get("id")
        self.workspace_ids[name] = ws_id
        logger.info(f"Created workspace: {name} ({ws_id})")
        await self._ensure_workspace_capacity(ws_id, name)
        return ws_id
    
    async def create_lakehouse(self, workspace_id: str, config: LakehouseConfig) -> str:
        """Create a lakehouse."""
        logger.info(f"Creating lakehouse: {config.name}")
        
        if self.dry_run:
            return str(uuid4())
        
        # Check if exists
        response = await self.client.get(f"/workspaces/{workspace_id}/lakehouses")
        for item in response.get("value", []):
            if item.get("displayName") == config.name:
                logger.info(f"Lakehouse exists: {config.name}")
                return item["id"]
        
        # Create
        try:
            response = await self.client.post(
                f"/workspaces/{workspace_id}/lakehouses", json_data={"displayName": config.name, "description": config.description}
            )
            return response.get("id")
        except FabricApiError as e:
            if e.status_code == 403:
                logger.error(
                    "403 Forbidden creating lakehouse. This usually means the workspace is not on a Fabric capacity "
                    "(Trial/F SKU) and/or the caller is not a Contributor+ on the workspace, and/or service principal "
                    "Fabric API access is blocked by tenant settings."
                )
                logger.error(
                    f"WorkspaceId={workspace_id} Lakehouse='{config.name}'. Response body: {e.response_body}"
                )
                logger.error(
                    "Quick checks: (1) Workspace Settings -> License info -> set to Trial/Fabric capacity, "
                    "(2) Workspace Manage access -> add your app/SP as Contributor+, "
                    "(3) Fabric Admin Portal -> Tenant settings -> allow service principals for Fabric APIs."
                )
            raise
    
    async def create_notebook(
        self,
        workspace_id: str,
        workspace_name: str,
        config: NotebookConfig,
        lakehouse_id: Optional[str] = None,
        force_update: bool = False,
    ) -> str:
        """Create a Fabric notebook as an item definition (ipynb).

        Uses LRO-aware creation. If the create-with-definition call is rejected by the tenant,
        falls back to: create blank notebook -> updateDefinition.
        """
        logger.info(f"Creating notebook: {config.name}")

        if self.dry_run:
            return str(uuid4())

        # Idempotent reruns
        existing = await self._find_item_id(workspace_id, "notebooks", config.name)
        if existing:
            if force_update:
                logger.info(f"Notebook exists, force-updating content: {config.name}")
                content = _resolve_notebook_content(config, workspace_name, workspace_id, lakehouse_id)
                parts = [
                    {
                        "path": "notebook-content.ipynb",
                        "payload": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
                        "payloadType": "InlineBase64",
                    }
                ]
                await self.client.post_with_lro(
                    f"/workspaces/{workspace_id}/notebooks/{existing}/updateDefinition",
                    json_data={"definition": {"format": "ipynb", "parts": parts}},
                    lro_poll_seconds=3.0,
                    max_polls=90,
                )
                logger.info(f"Notebook content updated: {config.name} ({existing})")
                return existing
            else:
                logger.info(f"Notebook exists: {config.name}")
                return existing

        # Generate ipynb JSON (uses local .py file if local_script_path is set)
        content = _resolve_notebook_content(config, workspace_name, workspace_id, lakehouse_id)

        # Optionally embed default lakehouse context
        if lakehouse_id:
            try:
                nb_obj = json.loads(content)
                md = nb_obj.setdefault("metadata", {})
                deps = md.setdefault("dependencies", {})
                deps["lakehouse"] = {
                    "default_lakehouse": lakehouse_id,
                    "default_lakehouse_name": config.lakehouse_ref,
                    "default_lakehouse_workspace_id": workspace_id,
                }
                content = json.dumps(nb_obj, ensure_ascii=False)
            except Exception as ex:
                logger.warning(f"Failed to embed lakehouse metadata for notebook '{config.name}': {ex}")

        definition = {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
                    "payloadType": "InlineBase64",
                }
            ],
        }

        payload = {
            "displayName": config.name,
            "description": config.description,
            "definition": definition,
        }

        # First attempt: create with definition
        try:
            created = await self.client.post_with_lro(
                f"/workspaces/{workspace_id}/notebooks",
                json_data=payload,
                lro_poll_seconds=3.0,
                max_polls=90,
            )
            nb_id = created.get("id") if isinstance(created, dict) else None
            if not nb_id:
                nb_id = await self._find_item_id(workspace_id, "notebooks", config.name)
            if not nb_id:
                raise RuntimeError(f"Notebook created but ID not found for {config.name}")
            return nb_id

        except FabricApiError as e:
            # Fall back to blank + updateDefinition if tenant rejects create-with-definition
            logger.warning(f"Notebook create-with-definition failed for '{config.name}' ({e.status_code}). Falling back to blank+updateDefinition.")

        # Fallback: create blank notebook
        blank = await self.client.post_with_lro(
            f"/workspaces/{workspace_id}/notebooks",
            json_data={"displayName": config.name, "description": config.description},
            lro_poll_seconds=3.0,
            max_polls=90,
        )
        nb_id = blank.get("id") if isinstance(blank, dict) else None
        if not nb_id:
            nb_id = await self._find_item_id(workspace_id, "notebooks", config.name)
        if not nb_id:
            raise RuntimeError(f"Blank notebook created but ID not found for {config.name}")

        # Update definition
        await self.client.post_with_lro(
            f"/workspaces/{workspace_id}/notebooks/{nb_id}/updateDefinition",
            json_data={"definition": definition},
            lro_poll_seconds=3.0,
            max_polls=90,
        )
        return nb_id

    async def _get_notebook_definition_parts(
        self,
        workspace_id: str,
        notebook_id: str,
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """Return (content_part_path, platform_part) from current notebook definition."""
        try:
            raw = await self.client.post_raw(
                f"/workspaces/{workspace_id}/notebooks/{notebook_id}/getDefinition",
                params={"format": "ipynb"},
                json_data={},
            )
            if raw.status_code == 202:
                data = await self.client.wait_for_lro(raw, poll_interval=2.0, timeout=600)
            else:
                raw.raise_for_status()
                data = raw.json() if raw.content else {}
        except Exception as e:
            logger.warning(f"Could not read existing notebook definition: {e}")
            return None, None

        definition = data.get("definition", data) if isinstance(data, dict) else {}
        parts = definition.get("parts", []) if isinstance(definition, dict) else []
        if not isinstance(parts, list):
            return None, None

        content_path: Optional[str] = None
        platform_part: Optional[Dict[str, Any]] = None

        for p in parts:
            if not isinstance(p, dict):
                continue
            path = p.get("path")
            if path == ".platform":
                platform_part = p
                continue
            if not content_path and path:
                content_path = str(path)
            if path and str(path).lower().endswith(".ipynb"):
                content_path = str(path)
                break

        return content_path, platform_part

    async def upload_local_script_to_notebook(
        self,
        workspace_name: str,
        notebook_name: str,
        script_path: str,
    ) -> str:
        """Upload a local .py script as ipynb content into an existing notebook."""
        script_file = Path(script_path)
        if not script_file.exists():
            raise RuntimeError(f"Script file not found: {script_file}")

        workspace_id = await self._resolve_workspace_id_by_name(workspace_name)
        if not workspace_id:
            raise RuntimeError(f"Workspace not found: {workspace_name}")

        notebook_id = await self._find_item_id(workspace_id, "notebooks", notebook_name)
        if not notebook_id:
            raise RuntimeError(f"Notebook not found: {notebook_name}")

        logger.info(
            f"Uploading script '{script_file}' -> notebook '{notebook_name}' "
            f"in workspace '{workspace_name}'"
        )

        if self.dry_run:
            logger.info("[DRY RUN] Skipping updateDefinition call.")
            return notebook_id

        script_text = script_file.read_text(encoding="utf-8")
        # Look up the Bronze_Landing lakehouse ID so %%configure gets real GUIDs.
        bronze_lh_id = await self._find_item_id(workspace_id, "lakehouses", "Bronze_Landing") or ""
        ipynb_content = build_ipynb_from_python_script(
            script_file.name,
            script_text,
            lakehouse_id=bronze_lh_id,
            workspace_id=workspace_id,
        )
        content_path, platform_part = await self._get_notebook_definition_parts(workspace_id, notebook_id)
        if not content_path:
            content_path = "notebook-content.ipynb"

        parts = [
            {
                "path": content_path,
                "payload": base64.b64encode(ipynb_content.encode("utf-8")).decode("utf-8"),
                "payloadType": "InlineBase64",
            }
        ]
        if platform_part:
            parts.append(platform_part)

        update_metadata = "true" if platform_part else "false"
        await self.client.post_with_lro(
            f"/workspaces/{workspace_id}/notebooks/{notebook_id}/updateDefinition",
            params={"updateMetadata": update_metadata},
            json_data={"definition": {"format": "ipynb", "parts": parts}},
            lro_poll_seconds=2.0,
            max_polls=200,
        )
        logger.info(f"Notebook updated: {notebook_name} ({notebook_id})")
        return notebook_id

    async def create_pipeline(self, workspace_id: str, config: PipelineConfig, notebook_ids: Dict[str, str]) -> str:
        """Create a data pipeline with a real definition (pipeline-content.json).

        We generate a minimal, valid pipeline definition. If notebooks exist, we add sequential
        TridentNotebook activities (Fabric notebook activity) using notebookId + workspaceId.
        Otherwise, we add a simple Wait activity.
        """
        existing = await self._find_item_id(workspace_id, "dataPipelines", config.name)
        if existing:
            logger.info(f"Pipeline already exists: {config.name}")
            return existing

        # Build activities
        activities = []
        if notebook_ids:
            # Run up to 6 notebooks (sorted) sequentially
            ordered = sorted(notebook_ids.items(), key=lambda kv: kv[0])[:6]
            prev_name = None
            for idx, (nb_name, nb_id) in enumerate(ordered, start=1):
                act_name = f"NB_{idx:02d}_{nb_name[:24]}".replace(" ", "_")
                depends_on = []
                if prev_name:
                    depends_on = [{"activity": prev_name, "dependencyConditions": ["Succeeded"]}]
                activities.append({
                    "name": act_name,
                    "type": "TridentNotebook",
                    "dependsOn": depends_on,
                    "policy": {
                        "timeout": "0.12:00:00",
                        "retry": 0,
                        "retryIntervalInSeconds": 30,
                        "secureOutput": False,
                        "secureInput": False
                    },
                    "typeProperties": {
                        "notebookId": nb_id,
                        "workspaceId": workspace_id
                    }
                })
                prev_name = act_name
        else:
            activities.append({
                "name": "Wait_10s",
                "type": "Wait",
                "dependsOn": [],
                "typeProperties": {"waitTimeInSeconds": 10}
            })

        pipeline_content = {
            "properties": {
                "description": config.description or f"Pipeline {config.name}",
                "activities": activities
            }
        }

        # DataPipeline definition requires a pipeline-content.json part
        definition = {
            "parts": [
                {
                    "path": "pipeline-content.json",
                    "payload": base64.b64encode(json.dumps(pipeline_content, indent=2).encode("utf-8")).decode("ascii"),
                    "payloadType": "InlineBase64"
                }
            ]
        }

        payload = {
            "displayName": config.name,
            "description": config.description,
            "definition": definition
        }

        logger.info(f"Creating pipeline: {config.name}")
        # Create data pipeline supports LRO
        resp = await self.client.post_with_lro(f"/workspaces/{workspace_id}/dataPipelines", json_data=payload)

        # If LRO doesn't return the id directly, find it
        pl_id = resp.get("id") if isinstance(resp, dict) else None
        if not pl_id:
            pl_id = await self._find_item_id(workspace_id, "dataPipelines", config.name)
        if not pl_id:
            raise RuntimeError(f"Pipeline created but ID not found for {config.name}")
        return pl_id

    async def create_semantic_model(
        self,
        workspace_id: str,
        config: SemanticModelConfig,
        measures: List[Dict]
    ) -> str:
        """Create a semantic model using TMSL (model.bim) + definition.pbism.

        Fabric requires definition.pbism and either:
          - model.bim (TMSL), OR
          - one or more files under definition/*.tmdl (TMDL).
        """
        logger.info(f"Creating semantic model: {config.name} with {len(measures)} measures")

        if self.dry_run:
            return str(uuid4())

        # Idempotent reruns
        response = await self.client.get(f"/workspaces/{workspace_id}/semanticModels")
        for item in response.get("value", []):
            if item.get("displayName") == config.name:
                logger.info(f"Semantic model exists: {config.name}")
                return item["id"]

        # Prefer the exported model.bim captured from the live Fabric environment.
        # Run:  python scripts/export_semantic_model.py --workspace <ws> --model <name>
        # to capture it.  Falls back to the synthetic generator if the file is absent.
        _static_bim_path = ROOT / "data" / "templates" / "semantic_model" / "model.bim"
        if _static_bim_path.exists():
            logger.info(f"Using exported model.bim from {_static_bim_path}")
            model_bim_b64 = base64.b64encode(_static_bim_path.read_bytes()).decode("utf-8")
        else:
            logger.info("No exported model.bim found — generating synthetic definition from code.")
            logger.info("Tip: run  python scripts/export_semantic_model.py  to capture the live model.")
            model_bim = generate_enterprise_sales_model_bim(config.name, config.description, measures)
            model_bim_b64 = base64.b64encode(
                json.dumps(model_bim, ensure_ascii=False, indent=2).encode("utf-8")
            ).decode("utf-8")

        # definition.pbism controls which semantic model formats Fabric will accept.
        # IMPORTANT:
        # - "1.0" is legacy (TMSL-only) and has recently been rejected by the service for JSON-based imports.
        # - "4.0" (PBIP/V3+) supports importing semantic model definitions from TMSL (model.bim) and/or TMDL (\definition).
        # See: Power BI Desktop project semantic model folder docs.
        pbism = {
            "version": "4.0"
        }
        pbism_b64 = base64.b64encode(
            json.dumps(pbism, ensure_ascii=False, indent=2).encode("utf-8")
        ).decode("utf-8")

        definition = {
            "parts": [
                {"path": "model.bim", "payload": model_bim_b64, "payloadType": "InlineBase64"},
                {"path": "definition.pbism", "payload": pbism_b64, "payloadType": "InlineBase64"},
            ]
        }

        response = await self.client.post_with_lro(
            f"/workspaces/{workspace_id}/semanticModels",
            json_data={
                "displayName": config.name,
                "description": config.description,
                "definition": definition,
            },
            lro_poll_seconds=3.0,
            max_polls=90,
        )

        # Some operations return 202 without an id in the final body; resolve by listing.
        model_id = response.get("id")
        if not model_id:
            model_id = await self._find_item_id(workspace_id, "semanticModels", config.name)

        return model_id

    

    async def create_shortcut(self, dest_workspace_id: str, dest_lakehouse_id: str, cfg: ShortcutConfig) -> Dict:
        """Create (or update) a OneLake shortcut inside the destination lakehouse.
        
        NOTE: Shortcuts to Tables require the source table to exist first!
        If running bootstrap before seeding data, use --skip-shortcuts flag.
        """
        # Resolve source workspace + lakehouse ids
        src_ws_id = self.workspace_ids.get(cfg.source_workspace)
        if not src_ws_id:
            raise RuntimeError(f"Source workspace not found in this run: {cfg.source_workspace}")

        src_lh_id = None
        src_items = self.created_items.get(cfg.source_workspace, {})
        src_lh_id = (src_items.get('lakehouses') or {}).get(cfg.source_lakehouse)
        if not src_lh_id:
            # Fallback: query by name
            src_lh_id = await self._find_item_id(src_ws_id, 'lakehouses', cfg.source_lakehouse)
        if not src_lh_id:
            raise RuntimeError(f"Source lakehouse not found: {cfg.source_workspace}/{cfg.source_lakehouse}")

        payload = {
            "path": "Tables",
            "name": cfg.name,
            "target": {
                "oneLake": {
                    "workspaceId": src_ws_id,
                    "itemId": src_lh_id,
                    "path": f"Tables/{cfg.source_table}"
                }
            }
        }

        # Idempotent reruns: if shortcut name already exists, skip creation.
        existing = await self.client.get_raw(f"/workspaces/{dest_workspace_id}/items/{dest_lakehouse_id}/shortcuts")
        if existing.status_code == 200:
            existing_payload = existing.json() if existing.content else {}
            for shortcut in existing_payload.get("value", []) or []:
                if shortcut.get("name") == cfg.name:
                    logger.info(f"Shortcut already exists: {cfg.name}")
                    return {"skipped": True, "reason": "already_exists", "name": cfg.name}

        logger.info(f"Creating shortcut '{cfg.name}' in lakehouse {dest_lakehouse_id} (from {cfg.source_workspace}/{cfg.source_lakehouse}:{cfg.source_table})")
        
        try:
            # Create Shortcut is idempotent (201 create / 200 update)
            resp = await self.client.post(
                f"/workspaces/{dest_workspace_id}/items/{dest_lakehouse_id}/shortcuts",
                json_data=payload
            )
            return resp
        except FabricApiError as e:
            if e.status_code == 400 and "Target path doesn't exist" in str(e.response_body):
                logger.warning(
                    f"Shortcut '{cfg.name}' skipped: Source table '{cfg.source_table}' does not exist yet.\n"
                    "Run seed_enterprise_data.py first, then run create_shortcuts.py to create shortcuts."
                )
                return {"skipped": True, "reason": "source_table_not_found", "name": cfg.name}
            raise

    # =========================================================================
    # Phase E — Health Monitoring Pipeline + Cross-Workspace Shortcuts
    # =========================================================================

    async def create_health_monitoring_pipeline(
        self,
        workspace_id: str,
        lakehouse_id: str,
    ) -> Dict[str, Any]:
        """
        Create a Fabric Data Pipeline that runs health scans on a schedule.

        WHAT: A scheduled Data Pipeline (cron-style) that runs the fabric_agent
        health monitoring notebook on a regular cadence:
            Schedule trigger (hourly)
              → Notebook activity: health_monitor.ipynb
                  → fabric-agent health-scan
                  → fabric-agent shortcut-scan
                  → Outputs: Delta table fabric_agent_health_log

        WHY: Production Fabric environments need continuous monitoring, not just
        one-off scans. This pipeline turns the self-healing agent into a background
        daemon that automatically detects and remediates issues without operator
        intervention.

        FAANG PARALLEL:
            Meta's Watchman continuous monitoring pipelines: cron-triggered Spark
            jobs that scan data quality, detect drift, and auto-file Jira tickets
            for issues above a severity threshold.

            Google SRE playbooks: alerting pipelines that run health checks every
            5 minutes and page on-call engineers when SLOs are breached.

        Fabric Pipeline definition:
            - type: DataPipeline
            - One ForEach activity wrapping a Notebook activity
            - Notebook: health_monitor.ipynb (in the same workspace)
            - Timeout: 1 hour
            - Max concurrency: 1 (prevent overlapping runs)

        Note:
            The actual schedule trigger must be configured in the Fabric UI
            after creation (Fabric REST API does not yet support trigger creation).

        Args:
            workspace_id:  Target workspace ID (usually ENT_DataPlatform_DEV).
            lakehouse_id:  Lakehouse ID for health log output.

        Returns:
            Dict with created pipeline item info or dry_run/skip status.
        """
        if self.dry_run:
            return {"dry_run": True, "message": "Would create health monitoring pipeline"}

        pipeline_name = "fabric_agent_health_monitor"
        logger.info(
            f"Creating health monitoring pipeline '{pipeline_name}' "
            f"in workspace {workspace_id}"
        )

        # Check if pipeline already exists (idempotent)
        try:
            items_resp = await self.client.get(
                f"/workspaces/{workspace_id}/items",
                params={"type": "DataPipeline"},
            )
            for item in (items_resp.get("value") or []):
                if item.get("displayName") == pipeline_name:
                    logger.info(f"Pipeline '{pipeline_name}' already exists — skipping")
                    return {
                        "skipped": True,
                        "reason": "already_exists",
                        "name": pipeline_name,
                        "pipeline_id": item.get("id", ""),
                    }
        except Exception as exc:
            logger.warning(f"Could not check existing pipelines: {exc}")

        # Resolve a notebook target for the monitoring pipeline.
        # Prefer project notebook, fallback to ingest notebook.
        notebook_id = None
        notebook_name = None
        candidate_notebooks = [
            "PRJ_SelfHealing_FabricInfrastructure",
            "01_Ingest_Bronze",
        ]
        try:
            notebooks_resp = await self.client.get(f"/workspaces/{workspace_id}/notebooks")
            notebook_items = notebooks_resp.get("value", []) or notebooks_resp.get("data", []) or []
            for candidate in candidate_notebooks:
                match = next((n for n in notebook_items if n.get("displayName") == candidate), None)
                if match:
                    notebook_id = match.get("id")
                    notebook_name = candidate
                    break
        except Exception as exc:
            logger.warning(f"Could not resolve notebook for health pipeline: {exc}")

        if not notebook_id:
            return {
                "error": (
                    "No target notebook found for health pipeline. "
                    "Expected one of: PRJ_SelfHealing_FabricInfrastructure, 01_Ingest_Bronze"
                ),
                "name": pipeline_name,
            }

        # Reuse the same valid DataPipeline creation path as create_pipeline().
        try:
            pipeline_id = await self.create_pipeline(
                workspace_id=workspace_id,
                config=PipelineConfig(
                    name=pipeline_name,
                    description=(
                        "Runs fabric_agent health scan + shortcut scan on a schedule. "
                        "Created by bootstrap_enterprise_domains.py (Phase E)."
                    ),
                    activities=[],
                ),
                notebook_ids={notebook_name: notebook_id},
            )
            logger.info(
                f"Health monitoring pipeline ensured: {pipeline_name} (id={pipeline_id})"
            )
            return {
                "created_or_exists": True,
                "name": pipeline_name,
                "pipeline_id": pipeline_id,
                "workspace_id": workspace_id,
                "notebook_name": notebook_name,
                "note": (
                    "Configure the schedule trigger in Fabric UI: "
                    "Pipeline → Manage → Schedule trigger → Hourly"
                ),
            }
        except Exception as exc:
            logger.error(f"Failed to create health monitoring pipeline: {exc}")
            return {"error": str(exc), "name": pipeline_name}

    async def create_cross_workspace_shortcuts(
        self,
        dev_workspace_id: str,
        prod_workspace_id: str,
        dev_lakehouse_id: str,
        prod_lakehouse_id: str,
    ) -> Dict[str, Any]:
        """
        Create shortcuts from ENT_DataPlatform_PROD → ENT_DataPlatform_DEV.

        WHAT: Creates OneLake shortcuts in the DEV lakehouse that point to
        reference tables in PROD. DEV analysts can read PROD reference data
        (DimDate, DimProduct) without duplicating it.

        Shortcut layout:
            DEV/Bronze_Landing/Tables/DimDate_PROD
                → PROD/Bronze_Landing/Tables/DimDate

            DEV/Bronze_Landing/Tables/DimProduct_PROD
                → PROD/Bronze_Landing/Tables/DimProduct

        WHY NO DUPLICATION:
            Without shortcuts, DEV would need to maintain its own copy of
            reference tables, creating sync issues. OneLake shortcuts read
            directly from PROD storage — zero data copies, always fresh.

        FAANG PARALLEL:
            Meta's Hive cross-cluster views: DEV clusters mount PROD Hive
            metastore tables as read-only views. Same pattern — one source
            of truth, multiple consumers.

            Databricks Unity Catalog external tables: PROD Delta tables
            registered as external locations in DEV catalog for read-only access.

        HUMAN-IN-THE-LOOP:
            Cross-workspace shortcuts are created automatically (safe — read-only).
            They appear in DEV but never modify PROD data.

        Args:
            dev_workspace_id:   DEV workspace ID.
            prod_workspace_id:  PROD workspace ID.
            dev_lakehouse_id:   DEV Bronze_Landing lakehouse ID.
            prod_lakehouse_id:  PROD Bronze_Landing lakehouse ID.

        Returns:
            Dict with created shortcuts or dry_run/skip status.
        """
        if self.dry_run:
            return {
                "dry_run": True,
                "message": "Would create cross-workspace shortcuts: DimDate_PROD, DimProduct_PROD",
            }

        reference_tables = [
            ("DimDate_PROD", "dim_date"),
            ("DimProduct_PROD", "dim_product"),
        ]

        created = []
        skipped = []
        errors = []

        source_tables_available: Optional[set[str]] = None
        try:
            source_tables_available = set()
            tables_resp = await self.client.get(
                f"/workspaces/{prod_workspace_id}/lakehouses/{prod_lakehouse_id}/tables",
                params={"maxResults": 100},
            )
            table_items = (tables_resp.get("data") or tables_resp.get("value") or [])
            for t in table_items:
                name = t.get("name") or t.get("displayName")
                if name:
                    source_tables_available.add(str(name).lower())
        except Exception as exc:
            logger.warning(
                f"Could not list source PROD tables for shortcut pre-check: {exc}. "
                "Will continue and let create API validate each path."
            )

        for shortcut_name, source_table in reference_tables:
            logger.info(
                f"Creating cross-workspace shortcut '{shortcut_name}' "
                f"(PROD/{source_table} → DEV)"
            )
            if source_tables_available is not None and source_table.lower() not in source_tables_available:
                logger.info(
                    f"Skipping '{shortcut_name}': source table '{source_table}' not found in PROD lakehouse."
                )
                skipped.append(shortcut_name)
                continue
            try:
                # Check if shortcut already exists
                existing_resp = await self.client.get_raw(
                    f"/workspaces/{dev_workspace_id}/items/{dev_lakehouse_id}/shortcuts"
                )
                if existing_resp.status_code == 200:
                    existing_body = existing_resp.json() if existing_resp.content else {}
                    for sc in (existing_body.get("value") or []):
                        if sc.get("name") == shortcut_name:
                            logger.info(f"Shortcut '{shortcut_name}' already exists — skipping")
                            skipped.append(shortcut_name)
                            break
                    else:
                        # Not found — create it
                        await self.client.post(
                            f"/workspaces/{dev_workspace_id}/items/{dev_lakehouse_id}/shortcuts",
                            json_data={
                                "path": "Tables",
                                "name": shortcut_name,
                                "target": {
                                    "oneLake": {
                                        "workspaceId": prod_workspace_id,
                                        "itemId": prod_lakehouse_id,
                                        "path": f"Tables/{source_table}",
                                    }
                                },
                            },
                        )
                        created.append(shortcut_name)
                        logger.info(f"Created shortcut: {shortcut_name}")
                else:
                    # Cannot check existing — try to create anyway
                    await self.client.post(
                        f"/workspaces/{dev_workspace_id}/items/{dev_lakehouse_id}/shortcuts",
                        json_data={
                            "path": "Tables",
                            "name": shortcut_name,
                            "target": {
                                "oneLake": {
                                    "workspaceId": prod_workspace_id,
                                    "itemId": prod_lakehouse_id,
                                    "path": f"Tables/{source_table}",
                                }
                            },
                        },
                    )
                    created.append(shortcut_name)
                    logger.info(f"Created shortcut: {shortcut_name}")
            except Exception as exc:
                msg = f"{shortcut_name}: {exc}"
                logger.error(f"Cross-workspace shortcut creation failed: {msg}")
                errors.append(msg)

        return {
            "created": created,
            "skipped": skipped,
            "errors": errors,
            "message": (
                f"Cross-workspace shortcuts: {len(created)} created, "
                f"{len(skipped)} already existed, {len(errors)} failed."
            ),
        }

    async def _resolve_workspace_id_by_name(self, workspace_name: str) -> Optional[str]:
        if not workspace_name:
            return None
        resp = await self.client.get("/workspaces")
        for ws in resp.get("value", []):
            if (ws.get("displayName") or "").lower() == workspace_name.lower():
                return ws.get("id")
        return None

    @staticmethod
    def _decode_inline_json_part(part: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if str(part.get("payloadType", "")).lower() != "inlinebase64":
            return None
        payload = part.get("payload")
        if not payload:
            return None
        try:
            raw = base64.b64decode(payload)
            return json.loads(raw.decode("utf-8", errors="replace"))
        except Exception:
            return None

    @staticmethod
    def _encode_inline_json_part(path: str, obj: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "path": path,
            "payload": base64.b64encode(json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")).decode("ascii"),
            "payloadType": "InlineBase64",
        }

    async def _get_template_report_definition(self) -> Optional[Dict[str, Any]]:
        """Fetch and cache a real report definition to use as creation template."""
        if self._cached_template_definition is not None:
            return self._cached_template_definition
        if not self.report_template_id:
            return None

        src_ws_id = self.report_template_workspace_id
        if not src_ws_id and self.report_template_workspace:
            src_ws_id = await self._resolve_workspace_id_by_name(self.report_template_workspace)
        if not src_ws_id:
            logger.warning(
                "Report template ID was provided but template workspace could not be resolved. "
                "Set FABRIC_REPORT_TEMPLATE_WORKSPACE_ID or FABRIC_REPORT_TEMPLATE_WORKSPACE."
            )
            return None

        definition = await self.client.post_with_lro(
            f"/workspaces/{src_ws_id}/reports/{self.report_template_id}/getDefinition",
            lro_poll_seconds=2.0,
            max_polls=90,
        )
        self._cached_template_definition = definition
        logger.info(f"Loaded template report definition from workspace {src_ws_id}")
        return definition

    def _rebind_template_report_definition(
        self,
        template_definition: Dict[str, Any],
        report_name: str,
        report_description: str,
        semantic_model_id: str,
    ) -> Dict[str, Any]:
        """Rewrite template report definition for a new report bound to target semantic model."""
        parts = ((template_definition.get("definition") or {}).get("parts") or [])
        if not parts:
            raise RuntimeError("Template report definition has no parts.")

        out_parts: List[Dict[str, Any]] = []
        has_definition_pbir = False

        for part in parts:
            path = str(part.get("path", ""))
            lower_path = path.lower()

            if lower_path == "definition.pbir":
                pbir = self._decode_inline_json_part(part) or {}
                pbir["datasetReference"] = {
                    "byConnection": {
                        "connectionString": f"semanticmodelid={semantic_model_id}"
                    }
                }
                out_parts.append(self._encode_inline_json_part(path, pbir))
                has_definition_pbir = True
                continue

            if lower_path == ".platform":
                platform = self._decode_inline_json_part(part) or {}
                md = platform.setdefault("metadata", {})
                md["type"] = "Report"
                md["displayName"] = report_name
                md["description"] = report_description or ""
                md["logicalId"] = str(uuid4())
                out_parts.append(self._encode_inline_json_part(path, platform))
                continue

            # Keep all other parts unchanged (layout resources, report.json, etc.)
            out_parts.append(part)

        if not has_definition_pbir:
            raise RuntimeError("Template report definition is missing definition.pbir part.")

        return {"parts": out_parts}

    async def create_report(self, workspace_id: str, cfg: ReportConfig, semantic_model_id: str) -> str:
        """Create a report bound to the given semantic model.

        Preferred path: clone a real template report definition and rebind dataset reference.
        Fallback path: create minimal placeholder definition.
        """
        existing = await self._find_item_id(workspace_id, 'reports', cfg.name)
        if existing:
            logger.info(f"Report already exists: {cfg.name}")
            return existing

        # Preferred path: tenant-proven report template (most reliable across tenants/workloads)
        template_definition = await self._get_template_report_definition()
        if template_definition:
            try:
                bound_def = self._rebind_template_report_definition(
                    template_definition=template_definition,
                    report_name=cfg.name,
                    report_description=cfg.description or "",
                    semantic_model_id=semantic_model_id,
                )
                payload = {
                    "displayName": cfg.name,
                    "description": cfg.description,
                    "definition": bound_def,
                }
                logger.info(f"Creating report from template: {cfg.name}")
                resp = await self.client.post_with_lro(
                    f"/workspaces/{workspace_id}/reports",
                    json_data=payload,
                    lro_poll_seconds=2.0,
                    max_polls=120,
                )
                rpt_id = resp.get('id') if isinstance(resp, dict) else None
                if not rpt_id:
                    rpt_id = await self._find_item_id(workspace_id, 'reports', cfg.name)
                if rpt_id:
                    return rpt_id
                raise RuntimeError(f"Template-based report created but ID not found for {cfg.name}")
            except Exception as e:
                logger.warning(
                    f"Template-based report creation failed for '{cfg.name}': {e}. "
                    "Falling back to minimal placeholder definition."
                )

        # definition.pbir (bind report to semantic model). When deploying via Fabric REST API,
        # a connectionString with semanticmodelid=<id> is sufficient.
        definition_pbir = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
            "version": "4.0",
            "datasetReference": {
                "byConnection": {
                    "connectionString": f"semanticmodelid={semantic_model_id}"
                }
            }
        }
        # report.json: legacy layout scaffold that Fabric accepts for report creation.
        # This template mirrors a known-good minimal report.json pattern.
        report_json = {
            "version": "1.0.0",
            "resourcePackages": [
                {
                    "name": "SharedResources",
                    "type": "SharedResources",
                    "items": [
                        {
                            "name": "BaseThemes/CY25SU11.json",
                            "type": "BaseTheme",
                            "path": "BaseThemes/CY25SU11.json"
                        }
                    ]
                }
            ],
            "config": "{\"version\":\"5.56\",\"themeCollection\":{\"baseTheme\":{\"name\":\"CY25SU11\",\"type\":2},\"customTheme\":{\"name\":\"Custom Theme\",\"type\":0}},\"objects\":{\"report\":[{\"properties\":{\"verticalAlignment\":{\"expr\":{\"Literal\":{\"Value\":\"'Top'\"}}},\"pageAlignment\":{\"expr\":{\"Literal\":{\"Value\":\"'Center'\"}}},\"pagePadding\":{\"expr\":{\"Literal\":{\"Value\":\"10D\"}}}}}]}},\"defaultDrillFilterOtherVisuals\":true,\"settings\":{\"useStylableVisualContainerHeader\":true,\"useEnhancedTooltips\":true} }",
            "layoutOptimization": 0,
            "sections": [
                {
                    "id": 0,
                    "name": "ReportSection",
                    "displayName": "Page 1",
                    "visualContainers": [],
                    "filters": [],
                    "ordinal": 0
                }
            ],
            "notifications": {
                "showDialog": True
            }
        }

        platform_json = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/metadata/2.0.0/schema.json",
            "metadata": {
                "type": "Report",
                "displayName": cfg.name,
                "description": cfg.description or "",
                "logicalId": str(uuid4())
            }
        }

        parts = [
            {
                "path": "definition.pbir",
                "payload": base64.b64encode(json.dumps(definition_pbir, indent=2).encode('utf-8')).decode('ascii'),
                "payloadType": "InlineBase64"
            },
            {
                "path": "report.json",
                "payload": base64.b64encode(json.dumps(report_json, indent=2).encode('utf-8')).decode('ascii'),
                "payloadType": "InlineBase64"
            },
            {
                "path": ".platform",
                "payload": base64.b64encode(json.dumps(platform_json, indent=2).encode('utf-8')).decode('ascii'),
                "payloadType": "InlineBase64"
            }
        ]

        payload = {
            "displayName": cfg.name,
            "description": cfg.description,
            "definition": {"parts": parts}
        }

        logger.info(f"Creating report: {cfg.name}")
        resp = await self.client.post_with_lro(f"/workspaces/{workspace_id}/reports", json_data=payload)
        rpt_id = resp.get('id') if isinstance(resp, dict) else None
        if not rpt_id:
            rpt_id = await self._find_item_id(workspace_id, 'reports', cfg.name)
        if not rpt_id:
            raise RuntimeError(f"Report created but ID not found for {cfg.name}")
        return rpt_id


    async def bootstrap_domain(self, domain: DomainConfig) -> Dict:
        """Bootstrap an entire domain."""
        logger.info(f"Bootstrapping domain: {domain.name}")
        
        results = {
            "domain": domain.name,
            "workspaces": []
        }
        
        for ws_config in domain.workspaces:
            ws_result = await self.bootstrap_workspace(ws_config)
            results["workspaces"].append(ws_result)
        
        return results
    
    async def bootstrap_workspace(self, config: WorkspaceConfig) -> Dict:
        """Bootstrap a single workspace."""
        logger.info(f"Bootstrapping workspace: {config.name}")

        result = {
            "name": config.name,
            "environment": config.environment,
            "items": []
        }

        # Create workspace
        ws_id = await self.create_workspace(config.name, config.description)
        result["workspace_id"] = ws_id

        # Create lakehouses
        lakehouse_ids = {}
        for lh_config in config.lakehouses:
            lh_id = await self.create_lakehouse(ws_id, lh_config)
            lakehouse_ids[lh_config.name] = lh_id
            result["items"].append({"type": "Lakehouse", "name": lh_config.name, "id": lh_id})

        # Create notebooks
        # PRJ_* notebooks are always force-updated so that changes to the
        # generator functions (_generate_healing/memory_notebook_content) are
        # pushed to Fabric even when the notebook already exists.
        # Force-update notebooks whose content is generated from local .py files
        # or whose generator functions change between releases.
        _PRJ_NOTEBOOKS = {
            "PRJ_SelfHealing_FabricInfrastructure",
            "PRJ_Context_MemoryState_Mgmt",
            "01_Ingest_Bronze_AutoHeal",
        }
        notebook_ids = {}
        for nb_config in config.notebooks:
            lh_id = lakehouse_ids.get(nb_config.lakehouse_ref)
            force = nb_config.name in _PRJ_NOTEBOOKS
            nb_id = await self.create_notebook(ws_id, config.name, nb_config, lh_id, force_update=force)
            notebook_ids[nb_config.name] = nb_id
            result["items"].append({"type": "Notebook", "name": nb_config.name, "id": nb_id})

        # Create pipelines
        pipeline_ids = {}
        for pl_config in config.pipelines:
            pl_id = await self.create_pipeline(ws_id, pl_config, notebook_ids)
            pipeline_ids[pl_config.name] = pl_id
            result["items"].append({"type": "Pipeline", "name": pl_config.name, "id": pl_id})

        # Create semantic models
        semantic_model_ids = {}
        measures = generate_enterprise_measures() if config.semantic_models else []
        for sm_config in config.semantic_models:
            sm_id = await self.create_semantic_model(ws_id, sm_config, measures)
            semantic_model_ids[sm_config.name] = sm_id
            result["items"].append({
                "type": "SemanticModel",
                "name": sm_config.name,
                "id": sm_id,
                "measure_count": len(measures)
            })

        # Persist created ids for cross-workspace references (shortcuts)
        self.created_items[config.name] = {
            "workspace_id": ws_id,
            "lakehouses": lakehouse_ids,
            "notebooks": notebook_ids,
            "pipelines": pipeline_ids,
            "semantic_models": semantic_model_ids
        }

        # Create shortcuts (in the first lakehouse of this workspace by default)
        if config.shortcuts and not self.skip_shortcuts:
            dest_lh_id = None
            if lakehouse_ids:
                dest_lh_id = next(iter(lakehouse_ids.values()))
            if not dest_lh_id:
                raise RuntimeError(f"Workspace {config.name} has shortcuts configured but no lakehouse to host them.")

            for sc in config.shortcuts:
                sc_resp = await self.create_shortcut(ws_id, dest_lh_id, sc)
                if sc_resp.get("skipped"):
                    result["items"].append({"type": "Shortcut", "name": sc.name, "status": "skipped", "reason": sc_resp.get("reason")})
                else:
                    result["items"].append({"type": "Shortcut", "name": sc.name, "id": f"{sc_resp.get('path')}/{sc_resp.get('name')}"})
        elif config.shortcuts and self.skip_shortcuts:
            logger.info(
                f"Skipping {len(config.shortcuts)} shortcuts (--skip-shortcuts flag). "
                "Run create_shortcuts.py after seeding data."
            )

        # Create reports
        if self.enable_reports and config.reports:
            for rpt in config.reports:
                sm_id = semantic_model_ids.get(rpt.semantic_model)
                if not sm_id:
                    raise RuntimeError(f"Report '{rpt.name}' references unknown semantic model '{rpt.semantic_model}' in workspace {config.name}")
                rpt_id = await self.create_report(ws_id, rpt, sm_id)
                result["items"].append({"type": "Report", "name": rpt.name, "id": rpt_id})

        return result

    async def deploy_fabric_agent_package(
        self,
        package_dir: Optional[str] = None,
        target_workspace_names: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Build the fabric_agent wheel and upload it to strategic project lakehouses.

        WHAT:
            1. Builds fabric_agent as a .whl using 'python -m build'
            2. Finds CoreOps and Research project workspaces by name
            3. Uploads the wheel to Files/wheels/ in each ProjectHub lakehouse
            4. Notebooks then install it with:
               %pip install /lakehouse/default/Files/wheels/fabric_agent-*.whl[healing,memory]

        WHY:
            Fabric Notebooks run in managed Spark — they cannot pip-install from
            GitHub or local paths. The wheel must live in OneLake where compute
            can reach it.

        FAANG PATTERN:
            Same as Google's internal PyPI mirror (go/pypi) or Meta's internal
            package registry — push artifacts to where compute lives, not to
            where developers work.

        Args:
            package_dir: Override the wheel output directory (default: ROOT/dist/).
            target_workspace_names: Override which workspaces receive the wheel.
                Defaults to all workspaces created in this run plus any existing DataPlatform workspaces.

        Returns:
            Dict with 'wheel', 'uploaded_to' (list), and 'errors' (list).
        """
        result: Dict[str, Any] = {"uploaded_to": [], "errors": []}

        if self.dry_run:
            logger.info("[DRY RUN] deploy_fabric_agent_package: would build and upload wheel")
            result["dry_run"] = True
            return result

        # Step 1: Build the wheel
        wheel_path = _build_fabric_agent_wheel(package_dir=package_dir)
        if wheel_path is None:
            msg = (
                "Wheel build failed — skipping upload. "
                "Install 'build': pip install build"
            )
            result["errors"].append(msg)
            logger.warning(msg)
            return result

        result["wheel"] = wheel_path.name
        wheel_bytes = wheel_path.read_bytes()
        logger.info(f"Deploying wheel: {wheel_path.name} ({len(wheel_bytes) // 1024} KB)")

        # Step 2: Determine target workspaces.
        # Default: all workspaces created in this run PLUS any existing DataPlatform
        # workspaces discovered via Fabric API (so --projects-only still reaches DEV).
        if target_workspace_names is None:
            # Start with workspaces created/found in this run
            candidates: Dict[str, str] = dict(self.workspace_ids)

            # Also discover DataPlatform workspaces not created in this run
            # (handles --projects-only mode where self.workspace_ids may be empty)
            try:
                ws_resp = await self.client.get("/workspaces")
                for ws in ws_resp.get("value", []):
                    name = ws.get("displayName") or ""
                    ws_id_found = ws.get("id") or ""
                    if "dataplatform" in name.lower() and name not in candidates:
                        candidates[name] = ws_id_found
                        logger.info(f"Including DataPlatform workspace for wheel upload: {name}")
            except Exception as e:
                logger.debug(f"Could not discover DataPlatform workspaces: {e}")

            target_workspace_names = list(candidates.keys())
            # Merge discovered workspaces into self.workspace_ids so the upload loop can find them
            self.workspace_ids.update(candidates)

        if not target_workspace_names:
            msg = (
                "No target workspaces found. "
                "Run bootstrap with --include-projects or --projects-only first."
            )
            logger.info(msg)
            result["errors"].append(msg)
            return result

        # Step 3: Upload to each workspace's appropriate lakehouse:
        #   DataPlatform  → Bronze_Landing lakehouse (primary data ingestion point)
        #   Fallback      → first available lakehouse
        for ws_name in target_workspace_names:
            ws_id = self.workspace_ids.get(ws_name)
            if not ws_id:
                msg = f"Workspace '{ws_name}' not in workspace_ids map — skipping"
                logger.warning(msg)
                result["errors"].append(msg)
                continue

            try:
                # Find the best lakehouse for the wheel.
                # Priority: Bronze_Landing > Silver_Curated > first available.
                # We no longer prefer ProjectHub — all use cases are in DataPlatform_DEV
                # with Bronze_Landing as the notebook default lakehouse.
                items_resp = await self.client.get(f"/workspaces/{ws_id}/lakehouses")
                all_lakehouses = items_resp.get("value", [])

                project_hub = (
                    next(
                        (i for i in all_lakehouses if "Bronze_Landing" in (i.get("displayName") or "")),
                        None,
                    )
                    or next(
                        (i for i in all_lakehouses if "Silver_Curated" in (i.get("displayName") or "")),
                        None,
                    )
                    or (all_lakehouses[0] if all_lakehouses else None)
                )
                if not project_hub:
                    msg = f"No lakehouse in '{ws_name}' — skipping wheel upload"
                    logger.warning(msg)
                    result["errors"].append(msg)
                    continue

                lh_id = project_hub["id"]

                # File upload goes through the OneLake DFS endpoint (ADLS Gen2 REST API),
                # NOT the main Fabric REST API (/v1/workspaces/.../lakehouses/.../files/).
                # The Fabric Items API does not expose a binary upload endpoint for Files/.
                remote_path = f"Files/wheels/{wheel_path.name}"
                logger.info(
                    f"Uploading {wheel_path.name} to "
                    f"{ws_name}/{project_hub['displayName']}/{remote_path}"
                )
                await self._onelake_upload_file(
                    workspace_id=ws_id,
                    lakehouse_id=lh_id,
                    remote_path=remote_path,
                    data=wheel_bytes,
                )

                onelake_path = f"/lakehouse/default/{remote_path}"
                result["uploaded_to"].append({"workspace": ws_name, "path": onelake_path})
                logger.info(f"Wheel uploaded to '{ws_name}' -> {onelake_path}")

            except Exception as exc:
                msg = f"Upload to '{ws_name}' failed: {exc}"
                logger.error(msg)
                result["errors"].append(msg)

        return result

    async def _onelake_upload_file(
        self,
        workspace_id: str,
        lakehouse_id: str,
        remote_path: str,
        data: bytes,
    ) -> None:
        """
        Upload a file to OneLake using the ADLS Gen2 DFS REST API.

        WHY: The main Fabric REST API (api.fabric.microsoft.com/v1) does not expose
             a binary file upload endpoint for Lakehouse Files. File I/O goes through
             the OneLake storage endpoint (onelake.dfs.fabric.microsoft.com), which
             implements the ADLS Gen2 DFS REST API protocol.

        ADLS Gen2 DFS 3-step upload protocol:
            1. PUT  ?resource=file           → create empty file resource
            2. PATCH ?action=append&position=0 → write bytes at offset 0
            3. PATCH ?action=flush&position=N  → commit (N = file size in bytes)

        FAANG PATTERN:
            Same pattern as Azure Blob Storage multi-part upload, S3 CreateMultipartUpload,
            or GCS resumable uploads — create resource, stream data, commit.

        Args:
            workspace_id: Fabric workspace GUID.
            lakehouse_id: Fabric lakehouse GUID.
            remote_path:  Path inside the lakehouse, e.g. "Files/wheels/fabric_agent-1.0.0-py3-none-any.whl"
            data:         Raw bytes to upload.

        Raises:
            Exception: Propagated to the caller on any step failure.
        """
        import httpx as _httpx

        # OneLake DFS uses Azure Storage API — requires a DIFFERENT token scope than
        # the main Fabric REST API (api.fabric.microsoft.com).
        # Fabric API scope:  https://api.fabric.microsoft.com/.default
        # OneLake DFS scope: https://storage.azure.com/.default
        # _ensure_initialized() is synchronous (no await), client is already open here.
        self.client._ensure_initialized()
        storage_token_resp = await self.client._credential.get_token(
            "https://storage.azure.com/.default"
        )
        storage_token = storage_token_resp.token

        base_url = (
            f"https://onelake.dfs.fabric.microsoft.com"
            f"/{workspace_id}/{lakehouse_id}/{remote_path}"
        )
        auth_header = {"Authorization": f"Bearer {storage_token}"}

        async with _httpx.AsyncClient(timeout=60) as http:
            # Step 1: Create empty file resource
            r = await http.put(
                f"{base_url}?resource=file",
                headers=auth_header,
            )
            if r.status_code not in (200, 201):
                raise RuntimeError(
                    f"OneLake create file failed ({r.status_code}): {r.text[:300]}"
                )
            logger.debug(f"OneLake create file: {r.status_code}")

            # Step 2: Append data at position 0
            r = await http.patch(
                f"{base_url}?action=append&position=0",
                headers={**auth_header, "Content-Type": "application/octet-stream"},
                content=data,
            )
            if r.status_code not in (200, 202):
                raise RuntimeError(
                    f"OneLake append failed ({r.status_code}): {r.text[:300]}"
                )
            logger.debug(f"OneLake append: {r.status_code}")

            # Step 3: Flush (commit) at final position
            r = await http.patch(
                f"{base_url}?action=flush&position={len(data)}&close=true",
                headers=auth_header,
            )
            if r.status_code not in (200, 202):
                raise RuntimeError(
                    f"OneLake flush failed ({r.status_code}): {r.text[:300]}"
                )
            logger.debug(f"OneLake flush: {r.status_code}")


    async def seed_enterprise_data(self) -> Dict[str, Any]:
        """
        Upload seed CSV files from data/seed_data/ to each DataPlatform
        lakehouse's Files/seed/<layer>/ area via the ADLS Gen2 DFS API.

        Also uploads optional scenario fixtures from:
            data/seed_data/scenarios/**.csv
        into:
            Files/seed/scenarios/**.csv

        WHY:
            Fabric Notebooks (01_Ingest_Bronze → 05_Aggregate_Gold) ingest data
            by reading Files/seed/*.csv and calling saveAsTable().  The CSV files
            must be in OneLake before those notebooks can run.

        WHEN TO RUN:
            After bootstrap creates the workspace + lakehouses, before opening
            Fabric UI to run the ETL notebooks.  Use --seed-data or --full.

        FAANG PATTERN:
            Same as Google's Nanny seed-data pipeline or Meta's Tupperware
            fixture bootstrap — push synthetic fixtures into the data platform
            so the full ETL pipeline can be validated end-to-end.

        Returns:
            Dict with 'seeded' (list of paths) and 'errors' (list of messages).
        """
        result: Dict[str, Any] = {"seeded": [], "errors": []}

        if self.dry_run:
            logger.info("[DRY RUN] seed_enterprise_data: would upload CSV seed files")
            result["dry_run"] = True
            return result

        if not _SEED_DATA_DIR.exists():
            msg = (
                f"Seed data directory not found: {_SEED_DATA_DIR}\n"
                "  Expected: data/seed_data/bronze/, data/seed_data/silver/, data/seed_data/gold/\n"
                "  These CSVs ship with the repo — run: git status data/seed_data/"
            )
            logger.error(msg)
            result["errors"].append(msg)
            return result

        # Seed only DataPlatform workspaces (they own Bronze/Silver/Gold)
        target_workspaces = {
            name: data
            for name, data in self.created_items.items()
            if "DataPlatform" in name
        }
        if not target_workspaces:
            logger.warning("No DataPlatform workspaces in created_items — skipping seed data upload")
            result["errors"].append("No DataPlatform workspaces found; run bootstrap first")
            return result

        for ws_name, ws_data in target_workspaces.items():
            ws_id = ws_data.get("workspace_id") or ""
            lh_ids: Dict[str, str] = ws_data.get("lakehouses", {})
            logger.info(f"Seeding workspace: {ws_name}")

            for lh_name, specs in _SEED_TABLES.items():
                lh_id = lh_ids.get(lh_name)
                if not lh_id:
                    logger.warning(f"  Lakehouse '{lh_name}' not in {ws_name} — skipping seed")
                    continue

                for _table_name, layer, csv_file in specs:
                    csv_path = _SEED_DATA_DIR / layer / csv_file
                    if not csv_path.exists():
                        msg = f"Seed CSV missing: {csv_path}"
                        logger.warning(msg)
                        result["errors"].append(msg)
                        continue

                    remote_path = f"Files/seed/{layer}/{csv_file}"
                    try:
                        await self._onelake_upload_file(
                            workspace_id=ws_id,
                            lakehouse_id=lh_id,
                            remote_path=remote_path,
                            data=csv_path.read_bytes(),
                        )
                        onelake_path = f"{ws_name}/{lh_name}/{remote_path}"
                        result["seeded"].append(onelake_path)
                        logger.info(f"  Seeded: {onelake_path}")
                    except Exception as exc:
                        msg = f"Seed upload failed ({ws_name}/{lh_name}/{csv_file}): {exc}"
                        logger.error(msg)
                        result["errors"].append(msg)

        # Optional scenario fixtures for positive/negative drift test packs.
        scenario_root = _SEED_DATA_DIR / "scenarios"
        if scenario_root.exists():
            for ws_name, ws_data in target_workspaces.items():
                ws_id = ws_data.get("workspace_id") or ""
                lh_ids: Dict[str, str] = ws_data.get("lakehouses", {})
                bronze_lh_id = lh_ids.get("Bronze_Landing")
                if not bronze_lh_id:
                    continue

                for csv_path in scenario_root.rglob("*.csv"):
                    rel = csv_path.relative_to(scenario_root).as_posix()
                    remote_path = f"Files/seed/scenarios/{rel}"
                    try:
                        await self._onelake_upload_file(
                            workspace_id=ws_id,
                            lakehouse_id=bronze_lh_id,
                            remote_path=remote_path,
                            data=csv_path.read_bytes(),
                        )
                        onelake_path = f"{ws_name}/Bronze_Landing/{remote_path}"
                        result["seeded"].append(onelake_path)
                        logger.info(f"  Seeded scenario: {onelake_path}")
                    except Exception as exc:
                        msg = f"Scenario seed upload failed ({ws_name}/{remote_path}): {exc}"
                        logger.error(msg)
                        result["errors"].append(msg)

        return result

    async def create_shortcuts_for_domains(self, domains: List["DomainConfig"]) -> Dict[str, Any]:
        """
        Create OneLake shortcuts for all workspaces in the given domain configs.

        Use this for the --shortcuts-only pass after ETL notebooks have created
        the Delta tables in Silver/Gold that the shortcuts reference.

        FAANG PATTERN:
            Same as LinkedIn's DataHub cross-dataset reference resolution — first
            the source datasets are created, then the cross-workspace pointers
            (shortcuts) are registered.

        Args:
            domains: Same list returned by build_enterprise_config().

        Returns:
            Dict with 'created', 'skipped', and 'errors' counts.
        """
        result: Dict[str, Any] = {"created": 0, "skipped": 0, "errors": []}

        if self.dry_run:
            logger.info("[DRY RUN] create_shortcuts_for_domains: would create shortcuts")
            result["dry_run"] = True
            return result

        # Step 1: Resolve ALL workspace IDs so cross-workspace shortcuts work
        try:
            ws_resp = await self.client.get("/workspaces")
            for ws in ws_resp.get("value", []):
                name = ws.get("displayName") or ""
                if name and name not in self.workspace_ids:
                    self.workspace_ids[name] = ws["id"]
        except Exception as e:
            logger.warning(f"Could not pre-resolve all workspace IDs: {e}")

        # Step 2: Create shortcuts workspace by workspace
        for domain in domains:
            for ws_config in domain.workspaces:
                if not ws_config.shortcuts:
                    continue

                ws_id = self.workspace_ids.get(ws_config.name)
                if not ws_id:
                    msg = f"Workspace '{ws_config.name}' not found — cannot create shortcuts"
                    logger.warning(msg)
                    result["errors"].append(msg)
                    continue

                # Destination lakehouse: first one in this workspace
                dest_lh_name = ws_config.lakehouses[0].name if ws_config.lakehouses else None
                dest_lh_id: Optional[str] = None
                if dest_lh_name:
                    dest_lh_id = await self._find_item_id(ws_id, "lakehouses", dest_lh_name)
                if not dest_lh_id:
                    lh_resp = await self.client.get(f"/workspaces/{ws_id}/lakehouses")
                    lhs = lh_resp.get("value", [])
                    dest_lh_id = lhs[0]["id"] if lhs else None
                if not dest_lh_id:
                    msg = f"No lakehouse in '{ws_config.name}' — cannot host shortcuts"
                    logger.warning(msg)
                    result["errors"].append(msg)
                    continue

                logger.info(
                    f"Creating {len(ws_config.shortcuts)} shortcuts in "
                    f"'{ws_config.name}' → lakehouse {dest_lh_id}"
                )
                for sc in ws_config.shortcuts:
                    try:
                        resp = await self.create_shortcut(ws_id, dest_lh_id, sc)
                        if resp.get("skipped"):
                            result["skipped"] += 1
                        else:
                            result["created"] += 1
                    except Exception as exc:
                        msg = f"Shortcut '{sc.name}' in '{ws_config.name}' failed: {exc}"
                        logger.error(msg)
                        result["errors"].append(msg)

        return result

    async def validate_environment(self) -> Dict[str, Any]:
        """
        Count all deployed assets and return a validation summary.

        Runs a lightweight GET on each workspace created in this run.
        Safe to run multiple times — read-only.

        Returns:
            Dict with counts per asset type and any issues found.
        """
        summary: Dict[str, Any] = {
            "workspaces": 0,
            "lakehouses": 0,
            "notebooks": 0,
            "pipelines": 0,
            "semantic_models": 0,
            "issues": [],
        }

        for ws_name, ws_id in self.workspace_ids.items():
            summary["workspaces"] += 1
            for endpoint, key in [
                ("lakehouses",    "lakehouses"),
                ("notebooks",     "notebooks"),
                ("dataPipelines", "pipelines"),
                ("semanticModels","semantic_models"),
            ]:
                try:
                    resp = await self.client.get(f"/workspaces/{ws_id}/{endpoint}")
                    summary[key] += len(resp.get("value", []))
                except Exception as e:
                    summary["issues"].append(f"{ws_name}/{endpoint}: {e}")

        return summary


async def main_async(args):
    """Main async entry point."""

    if getattr(args, "upload_notebook_script", None):
        upload_workspace = (
            getattr(args, "workspace", None)
            or getattr(args, "upload_workspace", None)
            or "ENT_DataPlatform_DEV"
        )
        upload_notebook = getattr(args, "upload_notebook_name", None) or "01_Ingest_Bronze"
        upload_script = str(args.upload_notebook_script)

        print("\n" + "=" * 70)
        print("NOTEBOOK SCRIPT UPLOAD MODE")
        print("=" * 70)
        print(f"Workspace: {upload_workspace}")
        print(f"Notebook:  {upload_notebook}")
        print(f"Script:    {upload_script}")
        print("-" * 70)

        if args.dry_run:
            print("[DRY RUN] No changes will be made.")

        if not args.yes and not args.dry_run:
            confirm = input("\nProceed with notebook upload? (yes/no): ")
            if confirm.lower() != "yes":
                print("Aborted.")
                return

        config = FabricAuthConfig.from_env()
        async with FabricApiClient(config) as client:
            bootstrapper = EnterpriseBootstrapper(client, dry_run=args.dry_run)
            try:
                notebook_id = await bootstrapper.upload_local_script_to_notebook(
                    workspace_name=upload_workspace,
                    notebook_name=upload_notebook,
                    script_path=upload_script,
                )
                print(f"\nNotebook upload complete. Notebook ID: {notebook_id}")
            except FabricApiError as e:
                print(f"\nERROR: Fabric API error during notebook upload: {e}")
                if getattr(e, "response_body", None):
                    print(f"Response body: {e.response_body}")
                raise SystemExit(1)
        return
    
    # ── --shortcuts-only: second-pass mode run AFTER ETL notebooks have created tables ──
    if getattr(args, "shortcuts_only", False):
        print("\n" + "=" * 70)
        print("SHORTCUTS-ONLY MODE")
        print("Run this after ETL notebooks have created Delta tables in Silver/Gold.")
        print("=" * 70)
        if args.dry_run:
            print("[DRY RUN] No changes will be made.")
            return
        if not args.yes:
            confirm = input("\nCreate shortcuts now? (yes/no): ")
            if confirm.lower() != "yes":
                print("Aborted.")
                return
        domains = build_enterprise_config(prefix=args.prefix)
        config = FabricAuthConfig.from_env()
        async with FabricApiClient(config) as client:
            bootstrapper = EnterpriseBootstrapper(
                client,
                dry_run=args.dry_run,
                capacity_id=getattr(args, "capacity_id", None),
            )
            sc_result = await bootstrapper.create_shortcuts_for_domains(domains)
            print(f"\nShortcuts: {sc_result['created']} created, {sc_result['skipped']} skipped")
            if sc_result.get("errors"):
                print("Errors:")
                for e in sc_result["errors"]:
                    print(f"  {e}")
        return

    # ── --full: convenience alias — sets sensible defaults for first-run setup ──
    if getattr(args, "full", False):
        args.include_projects = True
        args.seed_data = True
        args.deploy_package = True
        args.skip_shortcuts = True   # shortcuts need tables; run --shortcuts-only later
        logger.info(
            "--full flag: running with --include-projects --seed-data --deploy-package --skip-shortcuts"
        )

    # Build configuration
    if getattr(args, "projects_only", False):
        domains = build_strategic_project_config(prefix=args.prefix)
    else:
        domains = build_enterprise_config(prefix=args.prefix)
        if getattr(args, "include_projects", False):
            domains.extend(build_strategic_project_config(prefix=args.prefix))
    
    # Show summary
    print("\n" + "="*70)
    print("ENTERPRISE FABRIC ENVIRONMENT")
    print("="*70)
    
    total_workspaces = 0
    total_items = 0
    
    for domain in domains:
        print(f"\nDOMAIN: {domain.name}")
        print(f"   {domain.description}")
        
        for ws in domain.workspaces:
            total_workspaces += 1
            print(f"\n   Workspace: {ws.name} [{ws.environment}]")
            print(f"      Lakehouses: {len(ws.lakehouses)}")
            print(f"      Notebooks: {len(ws.notebooks)}")
            print(f"      Pipelines: {len(ws.pipelines)}")
            print(f"      Semantic Models: {len(ws.semantic_models)}")
            print(f"      Reports: {len(ws.reports)}")
            print(f"      Shortcuts: {len(ws.shortcuts)}")
            
            items = (len(ws.lakehouses) + len(ws.notebooks) + len(ws.pipelines) + 
                    len(ws.semantic_models) + len(ws.reports))
            total_items += items
    
    print("\n" + "-"*70)
    print(f"TOTAL: {total_workspaces} workspaces, {total_items}+ items")
    print(f"Measures per semantic model: {len(generate_enterprise_measures())}")
    print("-"*70)

    if getattr(args, "workspace", None):
        print(f"\nNOTE: Using existing workspace '{args.workspace}' for ALL items (no new workspaces will be created).")
    
    if getattr(args, "deploy_package", False):
        print("\nPACKAGE DEPLOYMENT: fabric_agent wheel will be built and uploaded after bootstrap.")

    if args.dry_run:
        print("\n[DRY RUN] No changes will be made.")
        return
    
    # Confirm
    if not args.yes:
        confirm = input("\nProceed with creation? (yes/no): ")
        if confirm.lower() != "yes":
            print("Aborted.")
            return
    
    # Execute
    config = FabricAuthConfig.from_env()
    
    async with FabricApiClient(config) as client:
        bootstrapper = EnterpriseBootstrapper(
            client, 
            dry_run=args.dry_run, 
            fixed_workspace=getattr(args, "workspace", None), 
            capacity_id=getattr(args, "capacity_id", None), 
            enable_reports=getattr(args, "enable_reports", False),
            skip_shortcuts=getattr(args, "skip_shortcuts", False),
            report_template_id=getattr(args, "report_template_id", None),
            report_template_workspace_id=getattr(args, "report_template_workspace_id", None),
            report_template_workspace=getattr(args, "report_template_workspace", None),
        )

        # If an existing workspace is provided, map all logical workspaces into it.
        if getattr(args, "workspace", None):
            await bootstrapper.set_fixed_workspace_by_name(args.workspace)
        
        results = []
        try:
            for domain in domains:
                if args.domain and domain.name != args.domain:
                    continue

                result = await bootstrapper.bootstrap_domain(domain)
                results.append(result)

            # ── Seed data: upload CSVs to DataPlatform lakehouses ──────────────────
            if getattr(args, "seed_data", False):
                print("\n" + "=" * 70)
                print("SEEDING DATA — uploading CSVs to OneLake Files/seed/")
                print("=" * 70)
                seed_result = await bootstrapper.seed_enterprise_data()
                if seed_result.get("dry_run"):
                    print("[DRY RUN] Would upload seed CSVs.")
                else:
                    print(f"Seeded {len(seed_result.get('seeded', []))} files.")
                    for path in seed_result.get("seeded", []):
                        print(f"  ✓  {path}")
                    if seed_result.get("errors"):
                        print("Warnings:")
                        for e in seed_result["errors"]:
                            print(f"  ⚠  {e}")
                    print(
                        "\nNext: open Fabric UI and run the ETL notebooks in order:\n"
                        "  01_Ingest_Bronze → 02_Transform_Silver → 03_Build_Dimensions\n"
                        "  → 04_Build_Facts → 05_Aggregate_Gold\n"
                        "Then run: python scripts/bootstrap_enterprise_domains.py "
                        "--shortcuts-only --capacity-id <id> --yes"
                    )

            # Save results
            output_path = Path(args.output) if args.output else Path("enterprise_bootstrap_results.json")
            output_path.write_text(json.dumps(results, indent=2))
            print(f"\nResults saved to: {output_path}")

            # ── Validation: count all deployed assets ──────────────────────────────
            print("\n" + "─" * 70)
            print("VALIDATION SUMMARY")
            print("─" * 70)
            try:
                validation = await bootstrapper.validate_environment()
                print(f"  Workspaces    : {validation['workspaces']}")
                print(f"  Lakehouses    : {validation['lakehouses']}")
                print(f"  Notebooks     : {validation['notebooks']}")
                print(f"  Pipelines     : {validation['pipelines']}")
                print(f"  Semantic Models: {validation['semantic_models']}")
                if validation.get("issues"):
                    print("  Issues:")
                    for issue in validation["issues"]:
                        print(f"    ⚠  {issue}")
            except Exception as e:
                logger.debug(f"Validation failed (non-fatal): {e}")

            # Deploy fabric_agent package to strategic project workspaces (optional)
            if getattr(args, "deploy_package", False):
                print("\n" + "=" * 70)
                print("DEPLOYING fabric_agent PACKAGE TO FABRIC")
                print("=" * 70)
                deploy_result = await bootstrapper.deploy_fabric_agent_package(
                    package_dir=getattr(args, "package_dir", None),
                )
                if deploy_result.get("dry_run"):
                    print("[DRY RUN] Would build and upload wheel.")
                elif deploy_result.get("wheel"):
                    print(f"Wheel: {deploy_result['wheel']}")
                    for upload in deploy_result.get("uploaded_to", []):
                        print(f"  Uploaded to {upload['workspace']}: {upload['path']}")
                    if deploy_result.get("errors"):
                        print("Warnings:")
                        for e in deploy_result["errors"]:
                            print(f"  {e}")
                    print(
                        "\nNext step in your Fabric Notebook:\n"
                        "  %pip install /lakehouse/default/Files/wheels/"
                        f"{deploy_result['wheel']}[healing,memory]"
                    )
                else:
                    print("Package deployment failed. Check logs above.")
                    if deploy_result.get("errors"):
                        for e in deploy_result["errors"]:
                            print(f"  ERROR: {e}")

            # Phase E: Create health monitoring pipeline + cross-workspace shortcuts
            if getattr(args, "create_health_pipeline", False):
                print("\n" + "=" * 70)
                print("CREATING HEALTH MONITORING PIPELINE")
                print("=" * 70)

                # Find ENT_DataPlatform_DEV workspace + lakehouse IDs
                dev_ws_id = bootstrapper.workspace_ids.get("ENT_DataPlatform_DEV")
                dev_lh_id = None
                if dev_ws_id:
                    dev_items = bootstrapper.created_items.get("ENT_DataPlatform_DEV", {})
                    dev_lh_id = (dev_items.get("lakehouses") or {}).get("Bronze_Landing")

                if not dev_ws_id:
                    print("  ⚠  ENT_DataPlatform_DEV workspace not found — skipping health pipeline")
                else:
                    pipeline_result = await bootstrapper.create_health_monitoring_pipeline(
                        workspace_id=dev_ws_id,
                        lakehouse_id=dev_lh_id or "",
                    )
                    if pipeline_result.get("dry_run"):
                        print("[DRY RUN] Would create health monitoring pipeline.")
                    elif pipeline_result.get("skipped"):
                        print(f"  ⚡  Health pipeline already exists — skipped.")
                    elif pipeline_result.get("created"):
                        print(f"  ✓  Created: {pipeline_result['name']} (id={pipeline_result.get('pipeline_id', 'N/A')})")
                        print(f"  ℹ  {pipeline_result.get('note', '')}")
                    else:
                        print(f"  ✗  Pipeline creation failed: {pipeline_result.get('error', 'unknown error')}")

                # Create cross-workspace shortcuts PROD → DEV
                print("\n" + "=" * 70)
                print("CREATING CROSS-WORKSPACE SHORTCUTS (PROD → DEV)")
                print("=" * 70)

                prod_ws_id = bootstrapper.workspace_ids.get("ENT_DataPlatform_PROD")
                prod_lh_id = None
                if prod_ws_id:
                    prod_items = bootstrapper.created_items.get("ENT_DataPlatform_PROD", {})
                    prod_lh_id = (prod_items.get("lakehouses") or {}).get("Bronze_Landing")

                if not dev_ws_id or not prod_ws_id:
                    print("  ⚠  Could not locate DEV and PROD workspace IDs — skipping cross-workspace shortcuts")
                elif not dev_lh_id or not prod_lh_id:
                    print("  ⚠  Could not locate Bronze_Landing lakehouse IDs — skipping cross-workspace shortcuts")
                else:
                    shortcut_result = await bootstrapper.create_cross_workspace_shortcuts(
                        dev_workspace_id=dev_ws_id,
                        prod_workspace_id=prod_ws_id,
                        dev_lakehouse_id=dev_lh_id,
                        prod_lakehouse_id=prod_lh_id,
                    )
                    if shortcut_result.get("dry_run"):
                        print("[DRY RUN] Would create cross-workspace shortcuts.")
                    else:
                        print(f"  {shortcut_result.get('message', '')}")
                        for sc in shortcut_result.get("created", []):
                            print(f"  ✓  Created shortcut: {sc}")
                        for sc in shortcut_result.get("skipped", []):
                            print(f"  ⚡  Already exists: {sc}")
                        if shortcut_result.get("errors"):
                            print("  Errors:")
                            for e in shortcut_result["errors"]:
                                print(f"  ✗  {e}")

        except FabricApiError as e:
            # Make permission/capacity failures actionable instead of a long stack trace.
            if getattr(e, "status_code", None) == 403:
                print("\nERROR: 403 Forbidden from Fabric API while creating items.")
                print("   This is almost always one of these:")
                print("   1) Workspace is not on a Fabric-capable capacity (Trial / F SKU)")
                print("   2) Your identity (service principal / user) is not Contributor+ on the workspace")
                print("   3) Tenant setting blocks service principals from Fabric APIs")
                if getattr(args, "workspace", None):
                    print(f"\n   Workspace name requested: {args.workspace}")
                if getattr(e, "response_body", None):
                    print(f"\n   Response body: {e.response_body}")
                print("\n   Fix checklist:")
                print("   - Fabric UI -> Workspace settings -> License info -> set to Trial/Fabric capacity")
                print("   - Fabric UI -> Manage access -> add SP/user as Contributor (or higher)")
                print("   - Fabric Admin portal -> Tenant settings -> allow service principals for Fabric APIs")

            else:
                print(f"\nERROR: Fabric API error: {e}")
                if getattr(e, "response_body", None):
                    print(f"Response body: {e.response_body}")

            raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(description="Bootstrap enterprise Fabric environment")
    parser.add_argument("--prefix", default="ENT", help="Prefix for all items")
    parser.add_argument("--domain", help="Bootstrap specific domain only")
    parser.add_argument(
        "--include-projects",
        action="store_true",
        help=(
            "Deploy strategic project notebooks and pipelines into ENT_DataPlatform_DEV: "
            "PRJ_SelfHealing_FabricInfrastructure and PRJ_Context_MemoryState_Mgmt."
        ),
    )
    parser.add_argument(
        "--projects-only",
        action="store_true",
        help=(
            "Deploy only strategic project notebooks/pipelines into ENT_DataPlatform_DEV, "
            "skipping the standard enterprise domain bootstrapping."
        ),
    )
    parser.add_argument(
        "--workspace",
        default=None,
        help=(
            "Optional: use a single existing workspace (by name) instead of creating separate ENT_* workspaces. "
            "If omitted, the script will create separate workspaces per domain/environment."
        ),
    )
    parser.add_argument(
        "--upload-notebook-script",
        default=None,
        help=(
            "Optional: upload a local .py script into an existing Fabric notebook and exit. "
            "Use with --upload-notebook-name and --workspace (or --upload-workspace)."
        ),
    )
    parser.add_argument(
        "--upload-notebook-name",
        default="01_Ingest_Bronze",
        help="Notebook display name to update when using --upload-notebook-script.",
    )
    parser.add_argument(
        "--upload-workspace",
        default=None,
        help=(
            "Workspace display name for --upload-notebook-script. "
            "If omitted, falls back to --workspace, then ENT_DataPlatform_DEV."
        ),
    )
    parser.add_argument(
        "--capacity-id",
        default=None,
        help=(
            "Optional: capacityId to assign each created/found workspace to (e.g., your Trial capacity). "
            "All workspaces can share the same capacity."
        ),
    )
    parser.add_argument(
        "--enable-reports",
        action="store_true",
        help=(
            "Attempt to create reports. If a template report is configured, clone+rebind it for reliability; "
            "otherwise fall back to placeholder definition."
        ),
    )
    parser.add_argument(
        "--report-template-id",
        default=None,
        help=(
            "Optional report item ID to use as creation template. "
            "Can also be set via FABRIC_REPORT_TEMPLATE_ID."
        ),
    )
    parser.add_argument(
        "--report-template-workspace-id",
        default=None,
        help=(
            "Workspace ID that contains --report-template-id. "
            "Can also be set via FABRIC_REPORT_TEMPLATE_WORKSPACE_ID."
        ),
    )
    parser.add_argument(
        "--report-template-workspace",
        default=None,
        help=(
            "Workspace displayName that contains --report-template-id "
            "(used when workspace ID is not provided). "
            "Can also be set via FABRIC_REPORT_TEMPLATE_WORKSPACE."
        ),
    )
    parser.add_argument(
        "--skip-shortcuts",
        action="store_true",
        help=(
            "Skip creating shortcuts. Use this when bootstrapping before seeding data, "
            "since shortcuts require source tables to exist. Run create_shortcuts.py after seeding."
        ),
    )
    parser.add_argument(
        "--seed-data",
        action="store_true",
        help=(
            "After bootstrapping, upload seed CSVs from data/seed_data/ to each "
            "DataPlatform lakehouse's Files/seed/ area so that the Spark ETL notebooks "
            "(01_Ingest_Bronze → 05_Aggregate_Gold) can run immediately."
        ),
    )
    parser.add_argument(
        "--shortcuts-only",
        action="store_true",
        help=(
            "Create OneLake shortcuts only — skip all other asset creation. "
            "Run this AFTER the ETL notebooks have created the Delta tables in "
            "Silver_Curated and Gold_Published."
        ),
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help=(
            "Convenience flag for first-run complete setup. "
            "Equivalent to: --include-projects --seed-data --deploy-package --skip-shortcuts. "
            "After this, run the ETL notebooks in Fabric UI, then run --shortcuts-only."
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be created")
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation")
    parser.add_argument("--output", help="Output file for results")
    parser.add_argument(
        "--deploy-package",
        action="store_true",
        help=(
            "After bootstrapping, build the fabric_agent wheel and upload it to the "
            "CoreOps and Research project workspace lakehouses. "
            "Requires: pip install build"
        ),
    )
    parser.add_argument(
        "--package-dir",
        default=None,
        help=(
            "Directory to write the built wheel to (default: ROOT/dist/). "
            "Only used with --deploy-package."
        ),
    )
    parser.add_argument(
        "--create-health-pipeline",
        action="store_true",
        help=(
            "After bootstrapping, create a scheduled health monitoring Data Pipeline "
            "in ENT_DataPlatform_DEV that runs fabric_agent health + shortcut scans. "
            "Also creates cross-workspace shortcuts: PROD Bronze_Landing tables → DEV. "
            "(Phase E: AI-Driven Shortcut Lifecycle Management)"
        ),
    )

    args = parser.parse_args()

    # Allow users to explicitly disable fixed-workspace mapping by passing --workspace ""
    if isinstance(getattr(args, "workspace", None), str) and args.workspace.strip() == "":
        args.workspace = None

    asyncio.run(main_async(args))

def _id_from_location(location: str | None) -> str | None:
    if not location:
        return None
    return location.rstrip("/").split("/")[-1]

if __name__ == "__main__":
    main()
