#!/usr/bin/env python
"""
Validate that the Enterprise Fabric environment matches the target architecture.

Fixes:
- Fabric Lakehouse tables endpoint returns {"data":[...]} (not {"value":[...]}) in many tenants.
- Add maxResults=100
- Handle duplicate workspaces with same displayName (choose best match by expected lakehouse coverage).
"""

import argparse
import asyncio
import base64
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from loguru import logger

from fabric_agent.core.config import FabricAuthConfig
from fabric_agent.api.fabric_client import FabricApiClient, FabricApiError


# ----------------------------
# Expected architecture
# ----------------------------

@dataclass
class WorkspaceExpect:
    name: str
    lakehouses: int
    notebooks_min: int
    pipelines_min: int
    semantic_models_min: int
    reports_min: int
    shortcuts_min: int


EXPECTED_WORKSPACES: List[WorkspaceExpect] = [
    WorkspaceExpect("ENT_DataPlatform_DEV", 3, notebooks_min=6, pipelines_min=2, semantic_models_min=0, reports_min=0, shortcuts_min=0),
    WorkspaceExpect("ENT_DataPlatform_PROD", 3, notebooks_min=2, pipelines_min=1, semantic_models_min=0, reports_min=0, shortcuts_min=0),
    WorkspaceExpect("ENT_SalesAnalytics_DEV", 1, notebooks_min=2, pipelines_min=1, semantic_models_min=1, reports_min=1, shortcuts_min=1),
    WorkspaceExpect("ENT_SalesAnalytics_PROD", 1, notebooks_min=0, pipelines_min=0, semantic_models_min=1, reports_min=1, shortcuts_min=0),
    WorkspaceExpect("ENT_Finance_DEV", 1, notebooks_min=0, pipelines_min=1, semantic_models_min=1, reports_min=1, shortcuts_min=1),
]

EXPECTED_LAKEHOUSES_BY_WORKSPACE: Dict[str, List[str]] = {
    "ENT_DataPlatform_DEV": ["Bronze_Landing", "Silver_Curated", "Gold_Published"],
    "ENT_DataPlatform_PROD": ["Bronze_Landing", "Silver_Curated", "Gold_Published"],
    "ENT_SalesAnalytics_DEV": ["Analytics_Sandbox"],
    "ENT_SalesAnalytics_PROD": ["Analytics_Sandbox"],
    "ENT_Finance_DEV": ["Finance_Layer"],
}

EXPECTED_TABLES_BY_LAKEHOUSE: Dict[str, List[str]] = {
    "Bronze_Landing": ["raw_sales_transactions", "raw_inventory_snapshots", "raw_customer_events"],
    "Silver_Curated": ["dim_date", "dim_customer", "dim_product", "dim_store", "fact_sales"],
    "Gold_Published": ["agg_daily_sales", "agg_customer_360", "agg_inventory_health"],
}

SALES_MODEL_MEASURE_TARGET = 98


# ----------------------------
# Helpers for payload shapes
# ----------------------------

def _items_from_resp(resp: dict) -> List[dict]:
    """
    Fabric endpoints are inconsistent:
      - some return {"value":[...]}
      - some return {"data":[...], "continuationToken":..., "continuationUri":...}
    """
    if not isinstance(resp, dict):
        return []
    if isinstance(resp.get("data"), list):
        return resp["data"]
    if isinstance(resp.get("value"), list):
        return resp["value"]
    return []


# ----------------------------
# Fabric helper functions
# ----------------------------

async def get_all_workspaces(client: FabricApiClient) -> List[dict]:
    resp = await client.get("/workspaces")
    return _items_from_resp(resp)


async def list_items(client: FabricApiClient, workspace_id: str, item_type: str) -> List[dict]:
    resp = await client.get(f"/workspaces/{workspace_id}/{item_type}")
    return _items_from_resp(resp)


async def list_shortcuts(client: FabricApiClient, workspace_id: str, lakehouse_id: str) -> List[dict]:
    # Shortcuts live under Items
    resp = await client.get(f"/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts")
    return _items_from_resp(resp)


async def list_tables(client: FabricApiClient, workspace_id: str, lakehouse_id: str) -> List[dict]:
    # IMPORTANT: this endpoint returns {"data":[...]} in your tenant
    resp = await client.get(
        f"/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables",
        params={"maxResults": 100},
    )
    return _items_from_resp(resp)


async def choose_best_workspace_match(
    client: FabricApiClient,
    candidates: List[dict],
    expected_lakehouses: List[str],
) -> Optional[dict]:
    """
    If multiple workspaces share the same displayName (common in demos),
    pick the one that best matches expected lakehouse names.
    """
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    scored: List[Tuple[int, int, dict]] = []
    exp_set = set(expected_lakehouses)

    for ws in candidates:
        ws_id = ws.get("id")
        if not ws_id:
            continue
        lhs = await list_items(client, ws_id, "lakehouses")
        lh_names = {lh.get("displayName") for lh in lhs}
        match_count = len(exp_set.intersection(lh_names))
        total_lhs = len(lhs)
        scored.append((match_count, total_lhs, ws))

    if not scored:
        return candidates[0]

    scored.sort(reverse=True, key=lambda x: (x[0], x[1]))
    best = scored[0][2]

    logger.warning(
        f"Multiple workspaces named '{best.get('displayName')}'. "
        f"Picking workspaceId={best.get('id')} based on lakehouse coverage."
    )
    return best


async def get_semantic_model_measure_count(client: FabricApiClient, workspace_id: str, semantic_model_id: str) -> Optional[int]:
    try:
        resp = await client.post_with_lro(
            f"/workspaces/{workspace_id}/semanticModels/{semantic_model_id}/getDefinition",
            params={"format": "TMSL"},
            lro_poll_seconds=3.0,
            max_polls=200,
        )
        parts = (resp.get("definition", {}) or {}).get("parts", []) or []
        model_part = None
        for p in parts:
            path = (p.get("path") or "").lower()
            if path.endswith("model.bim") or path == "model.bim":
                model_part = p
                break
        if not model_part:
            return None
        payload_b64 = model_part.get("payload")
        if not payload_b64:
            return None
        raw = base64.b64decode(payload_b64)
        model = json.loads(raw.decode("utf-8", errors="replace"))
        measures = []
        for tbl in model.get("model", {}).get("tables", []):
            measures.extend(tbl.get("measures", []) or [])
        return len(measures)
    except Exception as e:
        logger.warning(f"Could not fetch measure count via getDefinition (permissions/tenant settings?): {e}")
        return None


# ----------------------------
# Validation
# ----------------------------

async def validate(client: FabricApiClient, require_reports: bool = True) -> Tuple[bool, List[str]]:
    failures: List[str] = []

    all_ws = await get_all_workspaces(client)

    for exp in EXPECTED_WORKSPACES:
        candidates = [w for w in all_ws if w.get("displayName") == exp.name]
        ws = await choose_best_workspace_match(client, candidates, EXPECTED_LAKEHOUSES_BY_WORKSPACE.get(exp.name, []))
        if not ws:
            failures.append(f"Missing workspace: {exp.name}")
            continue

        ws_id = ws.get("id")
        if not ws_id:
            failures.append(f"{exp.name}: workspace id missing")
            continue

        lakehouses = await list_items(client, ws_id, "lakehouses")
        notebooks = await list_items(client, ws_id, "notebooks")
        pipelines = await list_items(client, ws_id, "dataPipelines")
        semantic_models = await list_items(client, ws_id, "semanticModels")
        reports = await list_items(client, ws_id, "reports")

        # lakehouse count
        if len(lakehouses) < exp.lakehouses:
            failures.append(f"{exp.name}: lakehouses={len(lakehouses)} expected>={exp.lakehouses}")

        # minimums
        if len(notebooks) < exp.notebooks_min:
            failures.append(f"{exp.name}: notebooks={len(notebooks)} expected>={exp.notebooks_min}")
        if len(pipelines) < exp.pipelines_min:
            failures.append(f"{exp.name}: pipelines={len(pipelines)} expected>={exp.pipelines_min}")
        if len(semantic_models) < exp.semantic_models_min:
            failures.append(f"{exp.name}: semantic_models={len(semantic_models)} expected>={exp.semantic_models_min}")
        if require_reports and len(reports) < exp.reports_min:
            failures.append(f"{exp.name}: reports={len(reports)} expected>={exp.reports_min}")

        expected_lhs = EXPECTED_LAKEHOUSES_BY_WORKSPACE.get(exp.name, [])
        lh_by_name = {lh.get("displayName"): lh for lh in lakehouses}

        for lh_name in expected_lhs:
            lh = lh_by_name.get(lh_name)
            if not lh:
                failures.append(f"{exp.name}: missing lakehouse {lh_name}")
                continue

            lh_id = lh.get("id")
            if not lh_id:
                failures.append(f"{exp.name}/{lh_name}: lakehouse id missing")
                continue

            # Shortcuts check
            if exp.shortcuts_min > 0:
                try:
                    shortcuts = await list_shortcuts(client, ws_id, lh_id)
                    if len(shortcuts) < exp.shortcuts_min:
                        failures.append(f"{exp.name}/{lh_name}: shortcuts={len(shortcuts)} expected>={exp.shortcuts_min}")
                except Exception as e:
                    failures.append(f"{exp.name}/{lh_name}: could not list shortcuts ({e})")

            # Tables check for DataPlatform lakehouses only
            if lh_name in EXPECTED_TABLES_BY_LAKEHOUSE:
                tables = await list_tables(client, ws_id, lh_id)
                # Normalize names (case-insensitive)
                table_names = set()
                for t in tables:
                    n = t.get("name") or t.get("displayName")
                    if n:
                        table_names.add(str(n).lower())

                # Helpful debug if empty
                if not table_names:
                    logger.warning(
                        f"{exp.name}/{lh_name}: tables endpoint returned 0 names. "
                        f"(ws_id={ws_id}, lakehouse_id={lh_id})"
                    )

                for tn in EXPECTED_TABLES_BY_LAKEHOUSE[lh_name]:
                    if tn.lower() not in table_names:
                        failures.append(f"{exp.name}/{lh_name}: missing table {tn}")

        # Semantic model measures check (SalesAnalytics workspaces)
        if exp.name.startswith("ENT_SalesAnalytics"):
            sm = next((m for m in semantic_models if m.get("displayName") == "Enterprise_Sales_Model"), None)
            if sm:
                mc = await get_semantic_model_measure_count(client, ws_id, sm.get("id"))
                if mc is None:
                    logger.warning(f"{exp.name}: measure count check skipped (no definition access).")
                elif mc != SALES_MODEL_MEASURE_TARGET:
                    failures.append(f"{exp.name}: Enterprise_Sales_Model measures={mc} expected={SALES_MODEL_MEASURE_TARGET}")
            else:
                failures.append(f"{exp.name}: missing semantic model Enterprise_Sales_Model")

    return (len(failures) == 0), failures


async def main_async(args: argparse.Namespace) -> int:
    load_dotenv()
    auth = FabricAuthConfig.from_env()
    client = FabricApiClient(auth)
    await client.initialize()

    try:
        ok, failures = await validate(client, require_reports=not args.skip_reports)
        if ok:
            logger.success("VALIDATION PASSED ✅ Enterprise environment matches expected architecture.")
            return 0

        logger.error("VALIDATION FAILED ❌")
        for f in failures:
            logger.error(f" - {f}")

        if any("missing table" in f for f in failures):
            logger.info(
                "\nHint: If tables exist in UI but validation says missing, it’s usually the API payload shape.\n"
                "This script now reads tables from resp['data'] as well as resp['value'].\n"
            )
        return 1

    except FabricApiError as e:
        logger.error(f"Fabric API error: {e} | body={getattr(e,'response_body',None)}")
        return 2
    finally:
        await client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-reports",
        action="store_true",
        help="Skip report count validation (useful when report creation is restricted).",
    )
    return parser.parse_args()


def main() -> None:
    raise SystemExit(asyncio.run(main_async(parse_args())))


if __name__ == "__main__":
    main()
