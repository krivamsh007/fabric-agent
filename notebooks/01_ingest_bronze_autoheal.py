"""
Fabric Notebook Script: Bronze ingestion with schema drift auto-heal.

Target scenario:
- Day 2 file adds columns: auto-heal and continue.
- Day 2 file changes datatypes (or removes columns): write alert, send email
  (if configured), and optionally block the run based on BREAK_ON_BREAKING_DRIFT.

Upload this .py into Microsoft Fabric Notebook and run in workspace:
  ENT_DataPlatform_DEV / Bronze_Landing
"""

from __future__ import annotations

import asyncio
import json
import os
import smtplib
import threading
from datetime import datetime, timezone
from email.mime.text import MIMEText
from html import escape
from typing import Any, Dict, List, Optional


# ============================================================================
# Bootstrap: install fabric_agent wheel from Lakehouse (run once per session)
#
# WHY: Fabric Notebooks run in a managed Spark environment. The fabric_agent
# package is not pre-installed. We upload the wheel to Bronze_Landing via the
# bootstrap script and install it here at notebook startup.
#
# HOW (two-step, blobfuse-safe):
#   Step 1 — Locate the wheel via mssparkutils.fs.ls() using the abfss://
#             path. This uses the Azure Storage SDK directly and bypasses
#             blobfuse entirely, avoiding the "lstat in local failed errno=2"
#             cache-miss error that occurs when pip tries to read a .whl file
#             through the /lakehouse/default blobfuse mount.
#   Step 2 — Copy the wheel to /tmp/ with mssparkutils.fs.cp(), then pip-
#             install from the local path. pip never touches blobfuse.
#
# FALLBACK: if mssparkutils is unavailable (local dev), falls back to the
#           blobfuse glob path so local testing still works.
# ============================================================================
import subprocess as _subprocess
import sys as _sys

_ABFSS_WHEEL_DIR = (
    "abfss://ENT_DataPlatform_DEV@onelake.dfs.fabric.microsoft.com"
    "/Bronze_Landing.Lakehouse/Files/wheels"
)
_TMP_WHEEL_DIR = "/tmp/fabric_agent_wheels"
_BLOBFUSE_WHEEL_GLOB = "/lakehouse/default/Files/wheels/fabric_agent-*.whl"


def _resolve_wheel_local_path() -> "str | None":
    """
    Returns a local filesystem path to the latest fabric_agent wheel,
    copying it to /tmp/ if necessary to avoid blobfuse cache-miss errors.

    FAANG PARALLEL: At Meta/Google, build artifacts are stored in distributed
    object stores (GCS/S3) and pulled to local executor temp directories before
    use — never read directly through a FUSE mount — for exactly this reason.
    """
    # ── Strategy A: mssparkutils (Azure SDK path, bypasses blobfuse) ──────────
    try:
        from notebookutils import mssparkutils  # type: ignore

        items = mssparkutils.fs.ls(_ABFSS_WHEEL_DIR)
        whl_items = sorted(
            [i for i in items if i.name.startswith("fabric_agent-") and i.name.endswith(".whl")],
            key=lambda x: x.name,
        )
        if whl_items:
            latest = whl_items[-1]
            os.makedirs(_TMP_WHEEL_DIR, exist_ok=True)
            local_path = f"{_TMP_WHEEL_DIR}/{latest.name}"
            src_abfss = f"{_ABFSS_WHEEL_DIR}/{latest.name}"
            print(f"[bootstrap] Copying wheel to /tmp (bypasses blobfuse): {src_abfss}")
            mssparkutils.fs.cp(src_abfss, f"file://{local_path}", True)
            print(f"[bootstrap] Wheel ready at: {local_path}")
            return local_path
        print(f"[bootstrap] No fabric_agent-*.whl found in: {_ABFSS_WHEEL_DIR}")
        return None
    except Exception as _e:
        print(f"[bootstrap] mssparkutils path unavailable ({_e}), trying blobfuse fallback...")

    # ── Strategy B: blobfuse mount (local dev / fallback) ────────────────────
    import glob as _glob
    _wheels = sorted(_glob.glob(_BLOBFUSE_WHEEL_GLOB))
    if _wheels:
        print(f"[bootstrap] Found wheel via blobfuse mount: {_wheels[-1]}")
        return _wheels[-1]

    return None


_wheel = _resolve_wheel_local_path()
if _wheel:
    print(f"[bootstrap] Installing fabric_agent from: {_wheel}")
    # Keep installs deterministic in Fabric shared runtimes:
    # - no extras for this notebook (memory/healing extras are not required here)
    # - no dependency resolution to avoid unrelated runtime package conflicts
    _result = _subprocess.run(
        [
            _sys.executable,
            "-m",
            "pip",
            "install",
            "--force-reinstall",
            "--no-deps",
            _wheel,
            "--quiet",
        ],
        capture_output=True,
        text=True,
    )
    if _result.returncode == 0:
        print("[bootstrap] fabric_agent installed successfully.")
        os.environ.setdefault("LOCAL_AGENT_ENABLED", "1")
    else:
        print(f"[bootstrap] pip install failed:\n{_result.stderr[-500:]}")
else:
    print(
        "[bootstrap] WARNING: fabric_agent wheel not found.\n"
        "  Run the bootstrap script to upload it:\n"
        "    python scripts/bootstrap_enterprise_domains.py "
        "--deploy-package --capacity-id <your-capacity-id>"
    )


# ============================================================================
# Configuration
# ============================================================================
# ┌─────────────────────────────────────────────────────────────────────────┐
# │  CONFIGURE HERE — update these values for your environment              │
# │  The CLIENT_SECRET must be set as a Fabric notebook environment secret  │
# │  or passed via the pipeline parameters — never hardcode it here.        │
# └─────────────────────────────────────────────────────────────────────────┘

# ── Azure / Fabric identity ───────────────────────────────────────────────
AZURE_TENANT_ID  = "<your-azure-tenant-id>"    # your Azure Tenant ID (Portal → Azure AD → Overview)
AZURE_CLIENT_ID  = "<your-azure-client-id>"    # your Service Principal App ID (App Registrations)
# CLIENT_SECRET: set via Fabric secret scope or pipeline env — do NOT hardcode
# FABRIC_CAPACITY_ID = "<your-capacity-guid>"  # Run: python scripts/list_capacities.py

# ── Workspace / Lakehouse targets ─────────────────────────────────────────
WORKSPACE_NAME = os.getenv("WORKSPACE_NAME", "ENT_DataPlatform_DEV")
LAKEHOUSE_NAME = os.getenv("LAKEHOUSE_NAME", "Bronze_Landing")

# ── Source / target paths ─────────────────────────────────────────────────
SOURCE_REL_PATH = os.getenv(
    "SOURCE_REL_PATH",
    "Files/seed/bronze/raw_sales_transactions.csv",
)
TARGET_TABLE = os.getenv("TARGET_TABLE", "raw_sales_transactions")
CONTRACT_REL_PATH = os.getenv(
    "CONTRACT_REL_PATH",
    f"Files/contracts/{TARGET_TABLE}.schema.json",
)
ALERTS_DIR_REL_PATH = os.getenv("ALERTS_DIR_REL_PATH", "Files/alerts/schema_drift")

# ── Ingestion behaviour ───────────────────────────────────────────────────
WRITE_MODE = os.getenv("WRITE_MODE", "overwrite")  # "overwrite" or "append"
BREAK_ON_BREAKING_DRIFT = os.getenv("BREAK_ON_BREAKING_DRIFT", "0") in (
    "1",
    "true",
    "True",
    "yes",
    "on",
)

# Apply identity config to environment (env-var overrides take precedence).
os.environ.setdefault("AZURE_TENANT_ID", AZURE_TENANT_ID)
os.environ.setdefault("AZURE_CLIENT_ID", AZURE_CLIENT_ID)
os.environ.setdefault("USE_INTERACTIVE_AUTH", "false")
os.environ.setdefault("AGENT_IMPACT_ENABLED", "1")
os.environ.setdefault("LOCAL_AGENT_ENABLED", "0")  # overridden to "1" above if wheel installed
os.environ.setdefault("BREAK_ON_BREAKING_DRIFT", "0")
os.environ.setdefault("SMTP_HOST", "smtp.gmail.com")
os.environ.setdefault("SMTP_PORT", "587")
os.environ.setdefault("SMTP_USE_TLS", "1")

# Optional email configuration (Gmail-friendly defaults for testing)
# For Gmail, use an App Password (not your regular Gmail password).
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", SMTP_USER)  # comma-separated
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER or "fabric-agent@company.com")
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "1") not in ("0", "false", "False")

# Optional live impact analysis (fabric_agent graph analyzer)
AGENT_IMPACT_ENABLED = os.getenv("AGENT_IMPACT_ENABLED", "1") not in (
    "0",
    "false",
    "False",
)
LOCAL_AGENT_ENABLED = os.getenv("LOCAL_AGENT_ENABLED", "0") in (
    "1",
    "true",
    "True",
    "yes",
    "on",
)


# ============================================================================
# Helpers
# ============================================================================

ABFSS_BASE = (
    f"abfss://{WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/"
    f"{LAKEHOUSE_NAME}.Lakehouse"
)
SOURCE_PATH = f"{ABFSS_BASE}/{SOURCE_REL_PATH}"
CONTRACT_PATH = f"{ABFSS_BASE}/{CONTRACT_REL_PATH}"
ALERTS_DIR = f"{ABFSS_BASE}/{ALERTS_DIR_REL_PATH}"
TABLE_DELTA_PATH = f"{ABFSS_BASE}/Tables/{TARGET_TABLE}"

DOWNSTREAM_IMPACT_CATALOG = {
    "raw_sales_transactions": {
        "lakehouse_tables": [
            "ENT_DataPlatform_DEV/Silver_Curated.fact_sales",
            "ENT_DataPlatform_DEV/Gold_Published.agg_daily_sales",
        ],
        "shortcuts": [
            "ENT_SalesAnalytics_DEV/Analytics_Sandbox.fact_sales",
            "ENT_Finance_DEV/Finance_Layer.fact_sales",
        ],
        "semantic_models": [
            "ENT_SalesAnalytics_DEV/Enterprise_Sales_Model",
            "ENT_Finance_DEV/Finance_Model",
        ],
        "reports": [
            "ENT_SalesAnalytics_DEV/Executive_Sales_Dashboard",
            "ENT_SalesAnalytics_DEV/Regional_Performance",
            "ENT_SalesAnalytics_DEV/Product_Analytics",
            "ENT_SalesAnalytics_DEV/Customer_Insights",
            "ENT_Finance_DEV/PnL_Dashboard",
            "ENT_Finance_DEV/Budget_vs_Actual",
        ],
    }
}


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_fs():
    try:
        from notebookutils import mssparkutils  # type: ignore

        return mssparkutils.fs
    except Exception:
        return None


FS = _get_fs()


def fs_exists(path: str) -> bool:
    if FS:
        return bool(FS.exists(path))
    return False


def fs_head(path: str, max_bytes: int = 10_000_000) -> str:
    if FS:
        return str(FS.head(path, max_bytes))
    raise RuntimeError("Filesystem helper unavailable. Run this inside Fabric Notebook.")


def fs_put(path: str, content: str, overwrite: bool = True) -> None:
    if FS:
        FS.put(path, content, overwrite)
        return
    raise RuntimeError("Filesystem helper unavailable. Run this inside Fabric Notebook.")


def fs_mkdirs(path: str) -> None:
    if FS:
        FS.mkdirs(path)
        return
    raise RuntimeError("Filesystem helper unavailable. Run this inside Fabric Notebook.")


def schema_to_list(schema_obj) -> List[Dict[str, Any]]:
    return [
        {
            "name": str(f.name),
            "type": str(f.dataType).lower(),
            "nullable": bool(f.nullable),
        }
        for f in schema_obj.fields
    ]


def normalize_cols(cols: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {str(c["name"]).lower(): c for c in cols}


def detect_drift(contract_cols: List[Dict[str, Any]], observed_cols: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Prefer the shared library drift detector so policy stays consistent
    # across ingestion and self-healing monitor flows.
    try:
        from fabric_agent.schema.contracts import ColumnContract, DataContract
        from fabric_agent.schema.drift import detect_drift as shared_detect_drift

        contract = DataContract(
            name=TARGET_TABLE,
            version=1,
            owner="notebook-inline",
            columns=[
                ColumnContract(
                    name=str(c.get("name")),
                    type=str(c.get("type", "string")),
                    nullable=bool(c.get("nullable", True)),
                )
                for c in contract_cols
            ],
        )
        finding = shared_detect_drift(contract, observed_cols)
        return {
            "added": finding.added,
            "removed": finding.removed,
            "type_changed": finding.type_changed,
            "nullability_changed": finding.nullability_changed,
            "breaking": finding.breaking,
        }
    except Exception:
        # Fallback keeps notebook runnable even if shared package import fails.
        pass

    cidx = normalize_cols(contract_cols)
    oidx = normalize_cols(observed_cols)

    added = []
    removed = []
    type_changed = []
    nullability_changed = []

    for name, ocol in oidx.items():
        if name not in cidx:
            added.append(ocol)
            continue
        ccol = cidx[name]
        if str(ccol.get("type", "")).lower() != str(ocol.get("type", "")).lower():
            type_changed.append(
                {"name": ocol["name"], "from": ccol.get("type"), "to": ocol.get("type")}
            )
        if bool(ccol.get("nullable", True)) != bool(ocol.get("nullable", True)):
            nullability_changed.append(
                {
                    "name": ocol["name"],
                    "from": bool(ccol.get("nullable", True)),
                    "to": bool(ocol.get("nullable", True)),
                }
            )

    for name, ccol in cidx.items():
        if name not in oidx:
            removed.append(ccol)

    # Guardrail: only additive changes auto-heal.
    breaking = bool(removed or type_changed or nullability_changed)
    return {
        "added": added,
        "removed": removed,
        "type_changed": type_changed,
        "nullability_changed": nullability_changed,
        "breaking": breaking,
    }


def build_required_changes(drift: Dict[str, Any]) -> List[str]:
    changes: List[str] = []
    if drift["added"]:
        cols = ", ".join(c["name"] for c in drift["added"])
        changes.append(f"Update mapping and data contract for added columns: {cols}.")
    if drift["type_changed"]:
        cols = ", ".join(
            f"{c['name']} ({c['from']} -> {c['to']})" for c in drift["type_changed"]
        )
        changes.append(f"Add explicit cast logic or versioned schema for datatype changes: {cols}.")
    if drift["removed"]:
        cols = ", ".join(c["name"] for c in drift["removed"])
        changes.append(f"Create compatibility projection for removed columns: {cols}.")
    if drift["nullability_changed"]:
        cols = ", ".join(
            f"{c['name']} ({c['from']} -> {c['to']})" for c in drift["nullability_changed"]
        )
        changes.append(f"Review null handling for changed nullability: {cols}.")
    if not changes:
        changes.append("No schema changes detected.")
    return changes


def _resolve_table_node(
    graph: Dict[str, Any],
    *,
    workspace_name: Optional[str],
    lakehouse_name: str,
    table_name: str,
) -> Optional[Dict[str, Any]]:
    nodes = graph.get("nodes", [])
    workspace_id = None
    if workspace_name:
        workspace = next(
            (
                n
                for n in nodes
                if n.get("type") == "workspace"
                and (n.get("name") or "").lower() == workspace_name.lower()
            ),
            None,
        )
        workspace_id = workspace.get("id") if workspace else None

    lakehouse = next(
        (
            n
            for n in nodes
            if n.get("type") == "lakehouse"
            and n.get("name") == lakehouse_name
            and (not workspace_id or n.get("parent_id") == workspace_id)
        ),
        None,
    )
    if lakehouse:
        match = next(
            (
                n
                for n in nodes
                if n.get("type") == "table"
                and n.get("name") == table_name
                and n.get("parent_id") == lakehouse.get("id")
            ),
            None,
        )
        if match:
            return match
    return next(
        (n for n in nodes if n.get("type") == "table" and n.get("name") == table_name),
        None,
    )


def _stringify_nodes(items: List[Any]) -> List[str]:
    out: List[str] = []
    for item in items or []:
        if isinstance(item, dict):
            out.append(str(item.get("name") or item.get("id") or item))
        else:
            out.append(str(item))
    return [x for x in out if x and x != "None"]


async def _run_agent_impact_async() -> Dict[str, Any]:
    from fabric_agent.api.fabric_client import FabricApiClient
    from fabric_agent.core.config import FabricAuthConfig
    from fabric_agent.tools.workspace_graph import (
        GraphImpactAnalyzer,
        WorkspaceGraphBuilder,
    )

    cfg = FabricAuthConfig.from_env()
    async with FabricApiClient(cfg) as client:
        resp = await client.get("/workspaces")
        workspace_id = None
        for ws in resp.get("value", []):
            if (ws.get("displayName") or "").lower() == WORKSPACE_NAME.lower():
                workspace_id = ws.get("id")
                break
        if not workspace_id:
            raise RuntimeError(f"Workspace not found: {WORKSPACE_NAME}")

        # Build graph for this workspace only (not all ENT_* prefixed workspaces).
        # Scanning all workspaces is expensive and unnecessary here — we only need
        # the downstream impact of one table in one lakehouse.
        builder = WorkspaceGraphBuilder(include_measure_graph=False)
        graph = await builder.build(client, workspace_id, WORKSPACE_NAME)
        analyzer = GraphImpactAnalyzer(graph)

        table_node = _resolve_table_node(
            graph,
            workspace_name=WORKSPACE_NAME,
            lakehouse_name=LAKEHOUSE_NAME,
            table_name=TARGET_TABLE,
        )

        if table_node:
            # ── Precise: table-level impact analysis ──────────────────────────
            # The table was found in the Fabric catalog (registered after a
            # successful saveAsTable or via the lakehouse tables API).
            impact = analyzer.analyze_table_change_impact(table_node["id"])
            analysis_scope = "table"
        else:
            # ── Fallback: lakehouse-level impact analysis ──────────────────────
            # The Fabric Lakehouse Tables API only returns tables that have been
            # registered in the catalog.  On the FIRST run the Delta files are
            # written to storage but the catalog entry may not yet exist, so no
            # table node appears in the graph.  We fall back to scanning all
            # downstream consumers of the *lakehouse* itself, which is still
            # accurate because every shortcut / model that reads
            # raw_sales_transactions goes through Bronze_Landing.
            print(
                f"[agent] Table node '{LAKEHOUSE_NAME}.{TARGET_TABLE}' not yet in "
                "Fabric catalog — falling back to lakehouse-level impact analysis."
            )
            nodes = graph.get("nodes", [])
            lakehouse_node = next(
                (
                    n for n in nodes
                    if n.get("type") == "lakehouse"
                    and (n.get("name") or "").lower() == LAKEHOUSE_NAME.lower()
                ),
                None,
            )
            if not lakehouse_node:
                raise RuntimeError(
                    f"Neither table nor lakehouse node found in graph for "
                    f"'{LAKEHOUSE_NAME}'. Check that the lakehouse is accessible."
                )
            impact = analyzer.analyze_lakehouse_change_impact(lakehouse_node["id"])
            analysis_scope = "lakehouse"

        return {
            "enabled": True,
            "success": True,
            "analysis_scope": analysis_scope,
            "risk_level": impact.get("risk_level", "unknown"),
            "total_impact": int(impact.get("total_impact", 0) or 0),
            "affected_shortcuts": _stringify_nodes(impact.get("affected_shortcuts", [])),
            "affected_models": _stringify_nodes(impact.get("affected_models", [])),
            "affected_notebooks": _stringify_nodes(impact.get("affected_notebooks", [])),
            "affected_pipelines": _stringify_nodes(impact.get("affected_pipelines", [])),
            "affected_reports": _stringify_nodes(impact.get("affected_reports", [])),
        }


def run_agent_impact_analysis() -> Dict[str, Any]:
    if not AGENT_IMPACT_ENABLED:
        return {
            "enabled": False,
            "success": False,
            "reason": "AGENT_IMPACT_ENABLED is disabled.",
        }
    if not LOCAL_AGENT_ENABLED:
        return {
            "enabled": True,
            "success": False,
            "reason": (
                "Local agent disabled in notebook runtime "
                "(set LOCAL_AGENT_ENABLED=1 only if fabric_agent is installed)."
            ),
        }
    try:
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        if running_loop and running_loop.is_running():
            # Fabric notebooks already run an event loop; run async work in a thread.
            result_holder: Dict[str, Any] = {}
            error_holder: Dict[str, Exception] = {}

            def _worker() -> None:
                try:
                    result_holder["value"] = asyncio.run(_run_agent_impact_async())
                except Exception as ex:
                    error_holder["value"] = ex

            t = threading.Thread(target=_worker, daemon=True)
            t.start()
            t.join()

            if "value" in error_holder:
                raise error_holder["value"]
            return result_holder.get("value", {"enabled": True, "success": False})

        return asyncio.run(_run_agent_impact_async())
    except Exception as exc:
        return {
            "enabled": True,
            "success": False,
            "error": str(exc),
        }


def merge_downstream_impact(
    static_impact: Dict[str, Any],
    agent_impact: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    merged = dict(static_impact or {})
    if not agent_impact or not agent_impact.get("success"):
        return merged

    mapping = {
        "affected_shortcuts": "shortcuts",
        "affected_models": "semantic_models",
        "affected_notebooks": "notebooks",
        "affected_pipelines": "pipelines",
        "affected_reports": "reports",
    }
    for src_key, target_key in mapping.items():
        vals = _stringify_nodes(agent_impact.get(src_key, []))
        if vals:
            merged[target_key] = vals

    merged["risk_level"] = agent_impact.get("risk_level", merged.get("risk_level", "unknown"))
    merged["total_downstream_assets"] = agent_impact.get(
        "total_impact", merged.get("total_downstream_assets", 0)
    )
    return merged


def build_alert_payload(
    *,
    status: str,
    drift: Dict[str, Any],
    required_changes: List[str],
    agent_impact: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    static_impact = DOWNSTREAM_IMPACT_CATALOG.get(TARGET_TABLE, {})
    impact = merge_downstream_impact(static_impact, agent_impact)
    return {
        "ts_utc": _now_utc(),
        "status": status,
        "source": {
            "workspace": WORKSPACE_NAME,
            "lakehouse": LAKEHOUSE_NAME,
            "table": TARGET_TABLE,
            "path": SOURCE_PATH,
        },
        "drift": drift,
        "required_changes": required_changes,
        "downstream_impact": impact,
        "agent_impact_analysis": agent_impact
        or {"enabled": False, "success": False, "reason": "Not executed."},
    }


def send_email_alert(payload: Dict[str, Any]) -> bool:
    recipients = [x.strip() for x in ALERT_EMAIL_TO.split(",") if x.strip()]
    if not recipients or not SMTP_HOST:
        print("Email alert skipped (ALERT_EMAIL_TO or SMTP_HOST not configured).")
        return False

    subject = (
        f"[Fabric Drift] {payload['status'].upper()} "
        f"{WORKSPACE_NAME}/{LAKEHOUSE_NAME}.{TARGET_TABLE}"
    )
    impact = payload.get("downstream_impact", {})
    required = payload.get("required_changes", [])
    drift = payload.get("drift", {})

    req_html = "".join(f"<li>{escape(x)}</li>" for x in required) or "<li>none</li>"
    sem_models = "".join(
        f"<li>{escape(x)}</li>" for x in impact.get("semantic_models", [])
    ) or "<li>none</li>"
    reports = "".join(
        f"<li>{escape(x)}</li>" for x in impact.get("reports", [])
    ) or "<li>none</li>"
    shortcuts = "".join(
        f"<li>{escape(x)}</li>" for x in impact.get("shortcuts", [])
    ) or "<li>none</li>"
    notebooks = "".join(
        f"<li>{escape(x)}</li>" for x in impact.get("notebooks", [])
    ) or "<li>none</li>"
    pipelines = "".join(
        f"<li>{escape(x)}</li>" for x in impact.get("pipelines", [])
    ) or "<li>none</li>"
    risk_level = impact.get("risk_level", "unknown")
    total_assets = impact.get("total_downstream_assets", "unknown")
    agent_info = payload.get("agent_impact_analysis", {})
    agent_status = (
        "enabled/success"
        if agent_info.get("enabled") and agent_info.get("success")
        else "enabled/failed"
        if agent_info.get("enabled")
        else "disabled"
    )
    agent_error = agent_info.get("error", "")

    body = f"""
    <html><body style="font-family: Arial, sans-serif;">
      <h3>Fabric Schema Drift Alert</h3>
      <p><b>Status:</b> {escape(payload['status'])}</p>
      <p><b>Source:</b> {escape(WORKSPACE_NAME)}/{escape(LAKEHOUSE_NAME)}.{escape(TARGET_TABLE)}</p>
      <p><b>Breaking:</b> {escape(str(drift.get('breaking', False)))}</p>
      <p><b>Impact Risk Level:</b> {escape(str(risk_level))}</p>
      <p><b>Total Downstream Assets:</b> {escape(str(total_assets))}</p>
      <p><b>Agent Impact Mode:</b> {escape(agent_status)}</p>
      <p><b>Agent Error:</b> {escape(str(agent_error)) if agent_error else "none"}</p>
      <h4>Required Changes</h4>
      <ul>{req_html}</ul>
      <h4>Impacted Shortcuts</h4>
      <ul>{shortcuts}</ul>
      <h4>Impacted Notebooks</h4>
      <ul>{notebooks}</ul>
      <h4>Impacted Pipelines</h4>
      <ul>{pipelines}</ul>
      <h4>Impacted Semantic Models</h4>
      <ul>{sem_models}</ul>
      <h4>Impacted Reports</h4>
      <ul>{reports}</ul>
    </body></html>
    """

    try:
        msg = MIMEText(body, "html")
        msg["Subject"] = subject
        msg["From"] = SMTP_FROM
        msg["To"] = ", ".join(recipients)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            if SMTP_USE_TLS:
                server.starttls()
            if SMTP_USER and SMTP_PASSWORD:
                server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_FROM, recipients, msg.as_string())
        print(f"Email alert sent to {len(recipients)} recipients.")
        return True
    except Exception as exc:
        print(f"Email alert failed: {exc}")
        return False


def write_contract(contract_doc: Dict[str, Any]) -> None:
    fs_mkdirs(CONTRACT_PATH.rsplit("/", 1)[0])
    fs_put(CONTRACT_PATH, json.dumps(contract_doc, indent=2), True)


def write_alert(payload: Dict[str, Any]) -> str:
    fs_mkdirs(ALERTS_DIR)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = f"{ALERTS_DIR}/{TARGET_TABLE}_{stamp}.json"
    fs_put(out_path, json.dumps(payload, indent=2), True)
    return out_path


def write_target_table(df) -> str:
    """Write target table with default-context fallback for Fabric notebook sessions."""
    writer = (
        df.write.mode(WRITE_MODE)
        .format("delta")
        .option("overwriteSchema", "true")
        .option("mergeSchema", "true")
    )
    try:
        writer.saveAsTable(TARGET_TABLE)
        return "saveAsTable"
    except Exception as exc:
        msg = str(exc)
        # Fabric may not have a default Lakehouse context when notebook metadata is missing.
        if "No default context found" in msg or "partial namespaces" in msg:
            print(
                "No default lakehouse context found for saveAsTable; "
                "falling back to path-based Delta write."
            )
            writer.save(TABLE_DELTA_PATH)
            print(f"Path-based Delta write succeeded: {TABLE_DELTA_PATH}")
            return "path"
        raise


def get_existing_table_schema() -> Optional[List[Dict[str, Any]]]:
    """Return current target Delta schema when table already exists."""
    # Prefer path-based read: works even when default lakehouse context is missing.
    try:
        if fs_exists(TABLE_DELTA_PATH):
            current_df = spark.read.format("delta").load(TABLE_DELTA_PATH)
            return schema_to_list(current_df.schema)
    except Exception as exc:
        print(f"Could not read existing Delta schema from path: {exc}")

    # Fallback to table lookup if context supports it.
    try:
        current_df = spark.table(TARGET_TABLE)
        return schema_to_list(current_df.schema)
    except Exception:
        return None


def print_runtime_prereq_status() -> None:
    checks = {
        "AZURE_TENANT_ID": bool(os.getenv("AZURE_TENANT_ID")),
        "AZURE_CLIENT_ID": bool(os.getenv("AZURE_CLIENT_ID")),
        "AZURE_CLIENT_SECRET": bool(os.getenv("AZURE_CLIENT_SECRET")),
        "LOCAL_AGENT_ENABLED": bool(os.getenv("LOCAL_AGENT_ENABLED")),
        "SMTP_USER": bool(os.getenv("SMTP_USER")),
        "SMTP_PASSWORD": bool(os.getenv("SMTP_PASSWORD")),
        "ALERT_EMAIL_TO": bool(os.getenv("ALERT_EMAIL_TO")),
    }
    print("Runtime prereq status:")
    for key, ok in checks.items():
        print(f" - {key}: {'OK' if ok else 'MISSING'}")


# ============================================================================
# Main
# ============================================================================

print_runtime_prereq_status()
print(f"Reading source file: {SOURCE_PATH}")
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(SOURCE_PATH)
)
observed_cols = schema_to_list(df.schema)
existing_delta_cols = get_existing_table_schema()

if fs_exists(CONTRACT_PATH):
    contract_doc = json.loads(fs_head(CONTRACT_PATH))
    contract_cols = contract_doc.get("columns", [])
elif existing_delta_cols:
    contract_doc = {
        "name": TARGET_TABLE,
        "version": 1,
        "owner": "data-platform@example.com",
        "columns": existing_delta_cols,
    }
    write_contract(contract_doc)
    print(f"Contract bootstrap created from existing Delta table: {CONTRACT_PATH}")
    contract_cols = contract_doc["columns"]
else:
    contract_doc = {
        "name": TARGET_TABLE,
        "version": 1,
        "owner": "data-platform@example.com",
        "columns": observed_cols,
    }
    write_contract(contract_doc)
    print(f"Contract bootstrap created at: {CONTRACT_PATH}")
    contract_cols = contract_doc["columns"]

if existing_delta_cols:
    baseline_cols = existing_delta_cols
    baseline_source = "existing_delta_table"
else:
    baseline_cols = contract_cols
    baseline_source = "contract_file"

print(f"Schema baseline source: {baseline_source}")
drift = detect_drift(baseline_cols, observed_cols)
required_changes = build_required_changes(drift)
agent_impact: Optional[Dict[str, Any]] = None

print("Detected drift summary:")
print(
    json.dumps(
        {
            "added": len(drift["added"]),
            "removed": len(drift["removed"]),
            "type_changed": len(drift["type_changed"]),
            "nullability_changed": len(drift["nullability_changed"]),
            "breaking": drift["breaking"],
        },
        indent=2,
    )
)

if drift["breaking"]:
    print("Running agent impact analysis before blocking alert...")
    agent_impact = run_agent_impact_analysis()
    print(f"Agent impact analysis result: {json.dumps(agent_impact, indent=2)}")
    payload = build_alert_payload(
        status="blocked_breaking_drift",
        drift=drift,
        required_changes=required_changes,
        agent_impact=agent_impact,
    )
    alert_path = write_alert(payload)
    sent = send_email_alert(payload)
    print(f"Breaking drift detected. Alert written: {alert_path}. Email sent={sent}")
    if BREAK_ON_BREAKING_DRIFT:
        raise RuntimeError(
            "Blocking pipeline due to breaking schema drift (removed/type-changed columns). "
            "Review alert payload and required changes."
        )
    print(
        "Breaking drift detected, but BREAK_ON_BREAKING_DRIFT is disabled. "
        "Run marked alert-only; no table write performed."
    )
else:
    # Non-breaking path: auto-heal
    write_mode_used = write_target_table(df)
    print(
        f"Table write succeeded: {TARGET_TABLE} (mode={WRITE_MODE}, method={write_mode_used})"
    )

    if drift["added"] or drift["nullability_changed"]:
        print("Running agent impact analysis for non-breaking drift alert...")
        agent_impact = run_agent_impact_analysis()
        print(f"Agent impact analysis result: {json.dumps(agent_impact, indent=2)}")
        # Auto-update contract for additive/non-breaking drift.
        contract_doc["version"] = int(contract_doc.get("version", 1)) + 1
        contract_doc["columns"] = observed_cols
        write_contract(contract_doc)
        payload = build_alert_payload(
            status="auto_healed_non_breaking_drift",
            drift=drift,
            required_changes=required_changes,
            agent_impact=agent_impact,
        )
        alert_path = write_alert(payload)
        sent = send_email_alert(payload)
        print(f"Auto-heal completed. Alert written: {alert_path}. Email sent={sent}")
    else:
        print("No schema drift detected. Pipeline completed successfully.")
