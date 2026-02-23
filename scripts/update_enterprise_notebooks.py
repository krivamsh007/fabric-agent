#!/usr/bin/env python
import argparse
import asyncio
import base64
import json
from typing import Dict, Any, Optional, List, Tuple

from dotenv import load_dotenv
from loguru import logger

from fabric_agent.core.config import FabricAuthConfig
from fabric_agent.api.fabric_client import FabricApiClient, FabricApiError

load_dotenv()

DEFAULT_WORKSPACES = ["ENT_DataPlatform_DEV", "ENT_DataPlatform_PROD"]

# Notebook -> (target lakehouse, kind)
NOTEBOOK_PLAN: Dict[str, Tuple[str, str]] = {
    "01_Ingest_Bronze": ("Bronze_Landing", "bronze_seed"),
    "02_Transform_Silver": ("Silver_Curated", "silver_seed_all"),
    "03_Build_Dimensions": ("Silver_Curated", "silver_seed_dims"),
    "04_Build_Facts": ("Silver_Curated", "silver_seed_fact"),
    "05_Aggregate_Gold": ("Gold_Published", "gold_seed"),
    "06_Data_Quality_Checks": ("Gold_Published", "dq_checks"),
}

BRONZE_FILES = [
    ("raw_sales_transactions", "raw_sales_transactions.csv"),
    ("raw_inventory_snapshots", "raw_inventory_snapshots.csv"),
    ("raw_customer_events", "raw_customer_events.csv"),
]

SILVER_FILES_DIMS = [
    ("dim_date", "dim_date.csv"),
    ("dim_customer", "dim_customer.csv"),
    ("dim_product", "dim_product.csv"),
    ("dim_store", "dim_store.csv"),
]

SILVER_FILES_FACT = [
    ("fact_sales", "fact_sales.csv"),
]

GOLD_FILES = [
    ("agg_daily_sales", "agg_daily_sales.csv"),
    ("agg_customer_360", "agg_customer_360.csv"),
    ("agg_inventory_health", "agg_inventory_health.csv"),
]


def _cell_markdown(lines: List[str]) -> Dict[str, Any]:
    return {"cell_type": "markdown", "metadata": {}, "source": lines}


def _cell_code(lines: List[str]) -> Dict[str, Any]:
    # lines can be list[str] each ending with \n
    return {
        "cell_type": "code",
        "metadata": {},
        "source": lines,
        "execution_count": None,
        "outputs": [],
    }


def _configure_cell(workspace_id: str, lakehouse_id: str, lakehouse_name: str) -> Dict[str, Any]:
    """First cell to force Spark default lakehouse so saveAsTable registers into the intended Lakehouse."""
    return _cell_code([
        "%%configure -f\n",
        "{\n",
        '  "defaultLakehouse": {\n',
        f'    "name": "{lakehouse_name}",\n',
        f'    "id": "{lakehouse_id}",\n',
        f'    "workspaceId": "{workspace_id}"\n',
        "  }\n",
        "}\n",
    ])


def _common_setup_lines(workspace_name: str, lakehouse_name: str) -> List[str]:
    """
    This is the FIXED common_setup you requested:
      - imports current_timestamp/col + datetime
      - ABFSS base is EXACT: abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse
      - better logging
      - save_table (with overwriteSchema+mergeSchema)
      - show_counts helper
    """
    # IMPORTANT: We hardcode WORKSPACE_NAME + LAKEHOUSE_NAME here to ensure the ABFSS path
    # always matches your required format (no dependency on runtime context keys).
    return [
        "# =========================================================\n",
        "# Common setup (ABFSS OneLake paths + helpers)\n",
        "# =========================================================\n",
        "from pyspark.sql import functions as F\n",
        "from pyspark.sql.functions import current_timestamp, col\n",
        "from datetime import datetime\n",
        "import re\n",
        "\n",
        f"WORKSPACE_NAME = \"{workspace_name}\"\n",
        f"LAKEHOUSE_NAME = \"{lakehouse_name}\"\n",
        "\n",
        "# REQUIRED ABFSS base (matches Fabric OneLake URI pattern)\n",
        "LAKEHOUSE_PATH = f\"abfss://{WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_NAME}.Lakehouse\"\n",
        "\n",
        "print('Notebook started:', datetime.now())\n",
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
        "    if p.startswith('abfss://') or p.startswith('wasbs://') or p.startswith('https://') or p.startswith('adl://') or p.startswith('/lakehouse/'):\n",
        "        return p\n",
        "    p = p.lstrip('/')\n",
        "    return f\"{LAKEHOUSE_PATH}/{p}\"\n",
        "\n",
        "def load_csv(rel_path: str, header: bool = True, infer_schema: bool = True, **options):\n",
        "    \"\"\"Load CSV using full ABFSS path.\"\"\"\n",
        "    full_path = lh(rel_path)\n",
        "    print(f\"📂 Loading CSV from: {full_path}\")\n",
        "    reader = spark.read.format('csv').option('header', str(header).lower()).option('inferSchema', str(infer_schema).lower())\n",
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
        "\n",
    ]


def _seed_block(pairs: List[Tuple[str, str]], seed_folder: str) -> List[str]:
    lines: List[str] = []
    for table_name, file_name in pairs:
        lines += [
            f"# --- {table_name} ---\n",
            f"df = load_csv('Files/seed/{seed_folder}/{file_name}')\n",
            "df = clean_columns(df)\n",
            f"save_table(df, '{table_name}', mode='overwrite')\n",
            "\n",
        ]
    # show counts at end
    tbls = ", ".join([f"'{t}'" for t, _ in pairs])
    lines += [f"show_counts({tbls})\n"]
    return lines


def build_notebook_ipynb(
    notebook_name: str,
    workspace_id: str,
    workspace_name: str,
    lakehouse_id: str,
    lakehouse_name: str,
) -> str:
    cells: List[Dict[str, Any]] = []
    # MUST be first cell (forces default lakehouse for this notebook session)
    if lakehouse_id:
        cells.append(_configure_cell(workspace_id, lakehouse_id, lakehouse_name))

    cells.append(_cell_markdown([f"# {notebook_name}\n", "Enterprise demo notebook\n"]))
    cells.append(_cell_code(_common_setup_lines(workspace_name, lakehouse_name)))

    kind = NOTEBOOK_PLAN[notebook_name][1]

    if kind == "bronze_seed":
        cells.append(_cell_markdown(["## Seed Bronze tables\n"]))
        cells.append(_cell_code(_seed_block(BRONZE_FILES, "bronze")))

    elif kind == "silver_seed_all":
        cells.append(_cell_markdown(["## Seed Silver tables (dims + facts)\n"]))
        cells.append(_cell_code(_seed_block(SILVER_FILES_DIMS + SILVER_FILES_FACT, "silver")))

    elif kind == "silver_seed_dims":
        cells.append(_cell_markdown(["## Seed Silver dimensions\n"]))
        cells.append(_cell_code(_seed_block(SILVER_FILES_DIMS, "silver")))

    elif kind == "silver_seed_fact":
        cells.append(_cell_markdown(["## Seed Silver fact\n"]))
        cells.append(_cell_code(_seed_block(SILVER_FILES_FACT, "silver")))

    elif kind == "gold_seed":
        cells.append(_cell_markdown(["## Seed Gold tables\n"]))
        cells.append(_cell_code(_seed_block(GOLD_FILES, "gold")))

    elif kind == "dq_checks":
        cells.append(_cell_markdown(["## Data Quality Checks\n"]))
        cells.append(_cell_code([
            "tables = ['agg_daily_sales','agg_customer_360','agg_inventory_health']\n",
            "show_counts(*tables)\n",
        ]))

    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "language_info": {"name": "python"},
            "kernel_info": {"name": "synapse_pyspark"},
        },
        "cells": cells,
    }
    return json.dumps(nb, indent=2)


async def _get_ws_id(client: FabricApiClient, ws_name: str) -> Optional[str]:
    wss = (await client.get("/workspaces")).get("value", [])
    for w in wss:
        if w.get("displayName") == ws_name:
            return w.get("id")
    return None


async def _get_notebook_id(client: FabricApiClient, ws_id: str, nb_name: str) -> Optional[str]:
    nbs = (await client.get(f"/workspaces/{ws_id}/notebooks")).get("value", [])
    for nb in nbs:
        if nb.get("displayName") == nb_name:
            return nb.get("id")
    return None


async def _get_lakehouse_id(client: FabricApiClient, ws_id: str, lakehouse_name: str) -> Optional[str]:
    """Resolve a lakehouse ID by displayName."""
    lhs = (await client.get(f"/workspaces/{ws_id}/lakehouses")).get("value", [])
    for lh in lhs:
        if lh.get("displayName") == lakehouse_name:
            return lh.get("id")
    return None


async def _get_definition_with_lro(client: FabricApiClient, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    # NOTE: getDefinition is POST (not GET)
    raw = await client.post_raw(path, params=params, json_data={})
    if raw.status_code == 202:
        return await client.wait_for_lro(raw, poll_interval=2.0, timeout=600)
    raw.raise_for_status()
    return raw.json() if raw.content else {}


def _get_parts(defn_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    d = defn_payload.get("definition", defn_payload)
    parts = d.get("parts", []) if isinstance(d, dict) else []
    return parts if isinstance(parts, list) else []


def _find_content_part(parts: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Notebook definition usually has:
      - one content part (ipynb or .py/.sql etc)
      - optional .platform
    We consider "content" as any part that is NOT ".platform".
    Prefer ipynb-looking paths if multiple.
    """
    if not parts:
        return None

    candidates = [p for p in parts if isinstance(p, dict) and p.get("path") and p.get("path") != ".platform"]
    if not candidates:
        return None

    # Prefer ipynb
    for p in candidates:
        if str(p.get("path", "")).lower().endswith(".ipynb"):
            return p

    return candidates[0]


def _find_platform_part(parts: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for p in parts:
        if isinstance(p, dict) and p.get("path") == ".platform":
            return p
    return None


async def update_notebook_definition(client: FabricApiClient, ws_id: str, nb_id: str, ipynb_str: str) -> None:
    # Pull existing definition (to preserve existing content path + .platform)
    current = await _get_definition_with_lro(
        client,
        f"/workspaces/{ws_id}/notebooks/{nb_id}/getDefinition",
        params={"format": "ipynb"},
    )

    parts = _get_parts(current)
    content_part = _find_content_part(parts)
    platform_part = _find_platform_part(parts)

    # If we can't read current parts, still try update with a safe default path
    content_path = (content_part.get("path") if content_part else None) or "notebook-content.ipynb"

    new_content_part = {
        "path": content_path,  # IMPORTANT: reuse whatever Fabric expects for THIS notebook
        "payload": base64.b64encode(ipynb_str.encode("utf-8")).decode("utf-8"),
        "payloadType": "InlineBase64",
    }

    new_parts = [new_content_part]
    if platform_part:
        new_parts.append(platform_part)

    definition = {"format": "ipynb", "parts": new_parts}

    # updateMetadata should only be true if we are providing .platform
    update_metadata = "true" if platform_part else "false"

    try:
        await client.post_with_lro(
            f"/workspaces/{ws_id}/notebooks/{nb_id}/updateDefinition",
            params={"updateMetadata": update_metadata},
            json_data={"definition": definition},
            lro_poll_seconds=2.0,
            max_polls=200,
        )
    except FabricApiError as e:
        print("\n❌ updateDefinition failed")
        print("status_code:", getattr(e, "status_code", None))
        print("response_body:", getattr(e, "response_body", None))
        raise

async def main_async(args: argparse.Namespace) -> int:
    cfg = FabricAuthConfig.from_env()
    async with FabricApiClient(cfg) as client:
        for ws_name in args.workspaces:
            ws_id = await _get_ws_id(client, ws_name)
            if not ws_id:
                logger.warning(f"⚠️ workspace not found: {ws_name}")
                continue

            logger.info(f"== {ws_name} ({ws_id}) ==")

            # Cache lakehouse IDs by name (needed for %%configure)
            lakehouse_id_cache: Dict[str, str] = {}

            for nb_name, (lh_name, _) in NOTEBOOK_PLAN.items():
                nb_id = await _get_notebook_id(client, ws_id, nb_name)
                if not nb_id:
                    logger.warning(f"⚠️ notebook not found: {nb_name}")
                    continue

                lh_id = lakehouse_id_cache.get(lh_name)
                if not lh_id:
                    lh_id = await _get_lakehouse_id(client, ws_id, lh_name)
                    if lh_id:
                        lakehouse_id_cache[lh_name] = lh_id
                    else:
                        logger.warning(f"⚠️ lakehouse not found in {ws_name}: {lh_name} (notebook will not have %%configure)")

                ipynb = build_notebook_ipynb(nb_name, ws_id, ws_name, lh_id or "", lh_name)

                logger.info(f"Updating notebook: {nb_name} ({nb_id}) -> lakehouse={lh_name}")
                if not args.dry_run:
                    await update_notebook_definition(client, ws_id, nb_id, ipynb)
                    logger.success("✅ updated")
                else:
                    logger.info("DRY RUN: skipping updateDefinition call")

    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--workspaces", nargs="+", default=DEFAULT_WORKSPACES)
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async(parse_args())))
