"""
Fabric Agent MCP Server
=======================

Model Context Protocol server that exposes Fabric operations as tools
for AI agents (Claude, GPT, etc.)

This is the core of the AI Agent architecture - it allows any
MCP-compatible AI to interact with Microsoft Fabric.

Usage:
    python -m fabric_agent.mcp_server
    
Or via entry point:
    fabric-mcp
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger
from pydantic import ValidationError

# MCP SDK
try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp.types import Tool, TextContent
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False
    logger.warning("MCP SDK not installed. Install with: pip install mcp")

from dotenv import load_dotenv

from fabric_agent.core.agent import FabricAgent
from fabric_agent.core.config import AgentConfig, FabricAuthConfig
from fabric_agent.tools.models import (
    ListWorkspacesInput,
    SetWorkspaceInput,
    ListItemsInput,
    GetSemanticModelInput,
    GetMeasuresInput,
    GetReportDefinitionInput,
    AnalyzeImpactInput,
    RenameMeasureInput,
    SmartRenameMeasureInput,
    GetRefactorHistoryInput,
    RollbackInput,
    GetConnectionStatusInput,
    TargetType,
    # Healing
    ScanWorkspaceHealthInput,
    BuildHealingPlanInput,
    ExecuteHealingPlanInput,
    # Phase E — Shortcut Cascade
    ScanShortcutCascadeInput,
    BuildShortcutHealingPlanInput,
    ApproveShortcutHealingInput,
    ExecuteShortcutHealingInput,
    # Memory / RAG
    FindSimilarOperationsInput,
    GetRiskContextInput,
    MemoryStatsInput,
    ReindexMemoryInput,
    GetSessionSummaryInput,
)

# Load .env from repo root
_REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_REPO_ROOT / ".env", override=True)


def get_tool_definitions() -> List[Dict[str, Any]]:
    """Define all available MCP tools with their schemas."""
    return [
        {
            "name": "list_workspaces",
            "description": "List all accessible Microsoft Fabric workspaces.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
        {
            "name": "set_workspace",
            "description": "Set the active workspace for subsequent operations.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "workspace_name": {
                        "type": "string",
                        "description": "Name of the workspace to activate",
                    },
                },
                "required": ["workspace_name"],
            },
        },
        {
            "name": "list_items",
            "description": "List items in the current workspace (reports, models, etc.).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "item_type": {
                        "type": "string",
                        "description": "Filter: Report, SemanticModel, Lakehouse, etc.",
                    },
                },
                "required": [],
            },
        },
        {
            "name": "get_semantic_model",
            "description": "Get semantic model details including tables and measures.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "model_name": {
                        "type": "string",
                        "description": "Name of the semantic model",
                    },
                },
                "required": ["model_name"],
            },
        },
        {
            "name": "get_measures",
            "description": "Get all DAX measures from a semantic model.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "model_name": {
                        "type": "string",
                        "description": "Name of the semantic model",
                    },
                },
                "required": ["model_name"],
            },
        },
        {
            "name": "get_report_definition",
            "description": "Get report PBIR definition including pages and visuals.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "report_name": {
                        "type": "string",
                        "description": "Name of the report",
                    },
                },
                "required": ["report_name"],
            },
        },
        {
            "name": "analyze_impact",
            "description": "Analyze what breaks if you rename a measure/column. CALL THIS FIRST!",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target_type": {
                        "type": "string",
                        "enum": ["measure", "column"],
                        "description": "Type of object to analyze",
                    },
                    "target_name": {
                        "type": "string",
                        "description": "Name of the measure or column",
                    },
                    "model_name": {
                        "type": "string",
                        "description": "Semantic model name (for DAX analysis)",
                    },
                },
                "required": ["target_type", "target_name"],
            },
        },
        {
            "name": "rename_measure",
            "description": "Rename a measure across model and reports. Use dry_run=true first!",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "model_name": {
                        "type": "string",
                        "description": "Semantic model containing the measure",
                    },
                    "old_name": {
                        "type": "string",
                        "description": "Current measure name",
                    },
                    "new_name": {
                        "type": "string",
                        "description": "New measure name",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "default": True,
                        "description": "If true, only simulate changes",
                    },
                },
                "required": ["model_name", "old_name", "new_name"],
            },
        },
        {
            "name": "smart_rename_measure",
            "description": "Hero refactor: rename a DAX measure, update dependent measures, and log the full transaction for rollback.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "model_name": { "type": "string", "description": "Name of the semantic model" },
                    "old_name": { "type": "string", "description": "Existing measure name" },
                    "new_name": { "type": "string", "description": "New measure name" },
                    "dry_run": { "type": "boolean", "description": "Preview only (no changes applied)", "default": true },
                    "reasoning": { "type": "string", "description": "Reasoning for the change (stored in log)" }
                },
                "required": ["model_name", "old_name", "new_name"]
            }
        },

        {
            "name": "get_refactor_history",
            "description": "Get history of refactoring operations from audit trail.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "default": 10,
                        "description": "Maximum events to return",
                    },
                    "operation_type": {
                        "type": "string",
                        "description": "Filter by operation type",
                    },
                },
                "required": [],
            },
        },
        {
            "name": "rollback",
            "description": "Rollback to a previous state using audit trail.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "snapshot_id": {
                        "type": "string",
                        "description": "ID of the snapshot to rollback to",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "default": True,
                        "description": "If true, only preview rollback",
                    },
                },
                "required": ["snapshot_id"],
            },
        },
        {
            "name": "get_connection_status",
            "description": "Check current connection status to Fabric.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
        {
            "name": "setup_enterprise_demo",
            "description": """Create a complete enterprise Star Schema demo environment with:
1. Mock data generation (7 tables, 500K+ transactions)
2. Lakehouse creation
3. Data pipeline for CSV to Delta conversion  
4. Semantic Model with 80+ complex DAX measures including nested dependencies

Use dry_run=true first to preview what will be created!""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "demo_name": {
                        "type": "string",
                        "default": "Enterprise_Sales",
                        "description": "Name prefix for created items",
                    },
                    "num_transactions": {
                        "type": "integer",
                        "default": 500000,
                        "description": "Number of sales transactions (1000-5000000)",
                    },
                    "start_year": {
                        "type": "integer",
                        "default": 2021,
                        "description": "Start year for data (2010-2024)",
                    },
                    "end_year": {
                        "type": "integer",
                        "default": 2024,
                        "description": "End year for data (2015-2030)",
                    },
                    "create_lakehouse": {
                        "type": "boolean",
                        "default": True,
                        "description": "Create the lakehouse",
                    },
                    "create_pipeline": {
                        "type": "boolean",
                        "default": True,
                        "description": "Create the data pipeline",
                    },
                    "create_semantic_model": {
                        "type": "boolean",
                        "default": True,
                        "description": "Create semantic model with DAX measures",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "default": False,
                        "description": "If true, only generate data locally",
                    },
                },
                "required": [],
            },
        },
        {
            "name": "analyze_refactor_impact",
            "description": """⚠️ SAFETY-FIRST: Analyze the impact BEFORE refactoring!

Scans ALL reports and semantic models in the workspace to find dependencies.
Returns a Pre-Flight Report with:
- Risk Level (SAFE, LOW_RISK, MEDIUM_RISK, HIGH_RISK, CRITICAL)
- Affected reports and visuals
- DAX measure dependencies (direct and indirect)
- Warnings and recommendations
- Required confirmation for high-risk operations

ALWAYS call this before any rename or modification!""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target_type": {
                        "type": "string",
                        "enum": ["measure", "column", "table"],
                        "description": "Type of target to analyze",
                    },
                    "target_name": {
                        "type": "string",
                        "description": "Name of the measure, column, or table",
                    },
                    "model_name": {
                        "type": "string",
                        "description": "Semantic model containing the target",
                    },
                    "new_name": {
                        "type": "string",
                        "description": "Proposed new name (for rename operations)",
                    },
                    "include_indirect": {
                        "type": "boolean",
                        "default": True,
                        "description": "Include indirect dependencies",
                    },
                },
                "required": ["target_type", "target_name", "model_name"],
            },
        },
        {
            "name": "safe_refactor",
            "description": """🛡️ SAFE refactoring with full protection!

Implements safety-first approach:
1. Automatically runs impact analysis
2. Generates pre-flight report with risk assessment
3. Requires confirmation for HIGH_RISK/CRITICAL operations
4. Creates restore point before changes
5. Supports full rollback

Risk-based confirmation:
- SAFE/LOW_RISK: Proceeds automatically
- MEDIUM_RISK: Proceeds with warning
- HIGH_RISK: Requires confirmation="yes"
- CRITICAL: Requires typing exact phrase

ALWAYS use dry_run=true first!""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target_type": {
                        "type": "string",
                        "enum": ["measure", "column", "table"],
                        "description": "Type of target",
                    },
                    "target_name": {
                        "type": "string",
                        "description": "Current name",
                    },
                    "new_name": {
                        "type": "string",
                        "description": "New name",
                    },
                    "model_name": {
                        "type": "string",
                        "description": "Semantic model name",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "default": True,
                        "description": "If true, only simulate (RECOMMENDED FIRST)",
                    },
                    "confirmation": {
                        "type": "string",
                        "description": "Confirmation for high-risk ops ('yes' or exact phrase)",
                    },
                    "skip_impact_analysis": {
                        "type": "boolean",
                        "default": False,
                        "description": "Skip impact analysis (NOT RECOMMENDED)",
                    },
                    "force": {
                        "type": "boolean",
                        "default": False,
                        "description": "Force despite blocking issues (DANGEROUS)",
                    },
                    "create_restore_point": {
                        "type": "boolean",
                        "default": True,
                        "description": "Create restore point for rollback",
                    },
                },
                "required": ["target_type", "target_name", "new_name", "model_name"],
            },
        },
        {
            "name": "rollback_last_action",
            "description": """⏪ Rollback a refactoring operation!

Uses MemoryManager to find exact previous state and restore it.
Your safety net when something goes wrong!

Three ways to rollback:
1. rollback_last=true - Rolls back the most recent operation
2. operation_id="..." - Rolls back a specific operation
3. snapshot_id="..." - Rolls back to a specific snapshot

ALWAYS use dry_run=true first to preview!""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "rollback_last": {
                        "type": "boolean",
                        "default": False,
                        "description": "Rollback the last operation",
                    },
                    "operation_id": {
                        "type": "string",
                        "description": "Specific operation ID to rollback",
                    },
                    "snapshot_id": {
                        "type": "string",
                        "description": "Specific snapshot ID to rollback to",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "default": True,
                        "description": "If true, only preview (RECOMMENDED)",
                    },
                    "confirmation": {
                        "type": "string",
                        "description": "Confirmation ('yes' to proceed)",
                    },
                },
                "required": [],
            },
        },
        {
            "name": "build_workspace_graph",
            "description": """🔍 Build a comprehensive dependency graph for the workspace!

Creates a full dependency map including:
- All workspace items (Reports, Semantic Models, Notebooks, Pipelines, Lakehouses)
- Measure-level dependencies within Semantic Models
- Cross-item relationships (Report→Model, Pipeline→Notebook, Notebook→Lakehouse)

Use for:
- Impact analysis before making changes
- Understanding workspace structure
- Identifying critical dependencies

Returns JSON with nodes, edges, and statistics.""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "include_measures": {
                        "type": "boolean",
                        "default": True,
                        "description": "Include measure-level nodes and edges",
                    },
                    "max_measures_per_model": {
                        "type": "integer",
                        "default": 500,
                        "description": "Max measures to include per model (for performance)",
                    },
                    "save_to_file": {
                        "type": "string",
                        "description": "Optional: Save graph to JSON file",
                    },
                },
                "required": [],
            },
        },
        {
            "name": "analyze_change_impact",
            "description": """📊 Analyze impact of a proposed change using the dependency graph!

Supports multiple scenarios:
- measure_rename: Impact of renaming a DAX measure
- notebook_change: Impact of modifying a notebook
- lakehouse_change: Impact of changing a lakehouse schema
- table_schema_change: Impact of schema drift for a specific table

Returns affected items and risk level; lakehouse_change now includes
shortcuts, semantic models, reports, notebooks, and pipelines.""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "change_type": {
                        "type": "string",
                        "enum": ["measure_rename", "notebook_change", "lakehouse_change", "table_schema_change"],
                        "description": "Type of change to analyze",
                    },
                    "model_name": {
                        "type": "string",
                        "description": "Semantic model name (for measure_rename)",
                    },
                    "measure_name": {
                        "type": "string",
                        "description": "Measure name (for measure_rename)",
                    },
                    "notebook_name": {
                        "type": "string",
                        "description": "Notebook name (for notebook_change)",
                    },
                    "lakehouse_name": {
                        "type": "string",
                        "description": "Lakehouse name (for lakehouse_change)",
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Table name (for table_schema_change)",
                    },
                },
                "required": ["change_type"],
            },
        },
        # ------------------------------------------------------------------ #
        #  Self-Healing Infrastructure Tools                                   #
        # ------------------------------------------------------------------ #
        {
            "name": "scan_workspace_health",
            "description": """🏥 Scan workspace(s) for infrastructure anomalies.

Detects:
- BROKEN_SHORTCUT: shortcuts pointing to deleted/moved sources
- ORPHAN_ASSET: semantic models/tables with no downstream consumers
- SCHEMA_DRIFT: table schemas changed vs registered DataContract
- STALE_TABLE: tables not refreshed within SLA window
- PIPELINE_FAILURE: pipelines whose last run failed

FAANG PATTERN: Same as Google SRE Monarch + Meta ODS anomaly detection.
This is the "observe" phase of the self-healing observe→decide→act loop.

ALWAYS call this before execute_healing_plan to review what will be fixed!""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "workspace_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "One or more Fabric workspace IDs to scan",
                    },
                    "include_schema_drift": {
                        "type": "boolean",
                        "default": True,
                        "description": "Also check tables for schema drift",
                    },
                    "stale_hours": {
                        "type": "integer",
                        "default": 24,
                        "description": "Flag tables not refreshed within this many hours",
                    },
                },
                "required": ["workspace_ids"],
            },
        },
        {
            "name": "build_healing_plan",
            "description": """📋 Build a healing plan without applying changes.

Scans for anomalies then categorises remediation actions:
- auto_actions: safe to apply immediately (no approval needed)
- manual_actions: require human review/approval

Use this to preview what execute_healing_plan would do.""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "workspace_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Workspace IDs to scan and plan healing for",
                    },
                },
                "required": ["workspace_ids"],
            },
        },
        {
            "name": "execute_healing_plan",
            "description": """🔧 Scan, plan, and execute self-healing for workspace anomalies.

WORKFLOW:
1. Detect anomalies (scan_workspace_health)
2. Build remediation plan (build_healing_plan)
3. Execute safe auto-fixes

ALWAYS use dry_run=true first to preview changes!

Risk-gated execution:
- LOW anomalies: auto-apply (additive schema changes, stale table refresh)
- HIGH/CRITICAL: require manual approval, never auto-applied""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "workspace_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Workspace IDs to scan and heal",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "default": True,
                        "description": "If true, simulate all actions (RECOMMENDED FIRST)",
                    },
                },
                "required": ["workspace_ids"],
            },
        },
        # ------------------------------------------------------------------ #
        #  Phase E — Shortcut Cascade Tools                                    #
        # ------------------------------------------------------------------ #
        {
            "name": "scan_shortcut_cascade",
            "description": """🔗 Discover all broken OneLake shortcuts + their cascade of impacted assets.

Queries the Fabric REST API for actual shortcut definitions (source workspace,
lakehouse, path), verifies each source is accessible, then traverses the lineage
graph downstream to find every impacted table, semantic model, pipeline, and report.

WORKFLOW:
1. scan_shortcut_cascade → review blast radius
2. build_shortcut_healing_plan → review auto vs manual actions
3. approve_shortcut_healing → if manual actions exist (human gate)
4. execute_shortcut_healing → dry_run=true first, then dry_run=false

FAANG PARALLEL: Meta blast radius analysis — before any infra remediation,
automatically compute all downstream effects and show operators what's broken.""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "workspace_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of workspace IDs to scan for broken shortcuts",
                    },
                },
                "required": ["workspace_ids"],
            },
        },
        {
            "name": "build_shortcut_healing_plan",
            "description": """📋 Build a cascade healing plan with auto and manual action splits.

Creates a plan that splits fixes into:
- auto_actions: safe to apply immediately (shortcut recreation + model refresh)
- manual_actions: require human approval (pipeline config, cross-workspace changes)

Stores the plan in memory — use the returned plan_id for approve/execute calls.

If approval_required=True, call approve_shortcut_healing(plan_id) before executing.""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "workspace_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Workspace IDs to include in the healing plan",
                    },
                },
                "required": ["workspace_ids"],
            },
        },
        {
            "name": "approve_shortcut_healing",
            "description": """✅ Human-in-the-loop approval gate for shortcut healing plan.

Reviews and approves a pending shortcut healing plan. After calling this,
run execute_shortcut_healing(plan_id, dry_run=false) to apply changes.

This is the human-in-the-loop gate — review the manual_actions from
build_shortcut_healing_plan before approving.""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "plan_id": {
                        "type": "string",
                        "description": "Plan ID from build_shortcut_healing_plan",
                    },
                    "approved_by": {
                        "type": "string",
                        "default": "user",
                        "description": "Name or email of the approver (for audit trail)",
                    },
                },
                "required": ["plan_id"],
            },
        },
        {
            "name": "execute_shortcut_healing",
            "description": """🔧 Execute a shortcut cascade healing plan.

Recreates broken shortcuts via the correct Fabric API endpoint and triggers
Full refresh on all impacted semantic models.

ALWAYS use dry_run=true first to preview changes!

APPROVAL GATE: manual_actions (pipeline config, cross-workspace) only run
if approve_shortcut_healing() was called first. auto_actions always run.""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "plan_id": {
                        "type": "string",
                        "description": "Plan ID from build_shortcut_healing_plan",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "default": True,
                        "description": "True = simulation only. False = apply changes. ALWAYS use True first!",
                    },
                },
                "required": ["plan_id"],
            },
        },
        # ------------------------------------------------------------------ #
        #  Phase I — Enterprise Blast Radius                                   #
        # ------------------------------------------------------------------ #
        {
            "name": "get_enterprise_blast_radius",
            "description": """🌐 Compute cross-workspace cascade blast radius for a source asset change.

Builds a complete multi-workspace lineage graph and identifies ALL downstream
assets that will be impacted when a source table or asset changes — across
every workspace that shortcuts or consumes from it.

ENTERPRISE SCENARIO (3-workspace cascade):
  fact_sales schema change in DataPlatform_DEV impacts:
  ├── DataPlatform_DEV (source): Sales_Model → Sales_Report
  ├── SalesAnalytics_DEV (consumer): shortcut → pipeline → Enterprise_Model → 4 reports
  └── Finance_DEV (consumer): shortcut → pipeline → Finance_Model → PnL + Budget reports

RETURNS:
  - per_workspace: fine-grained per-asset breakdown per workspace
  - ordered_healing_steps: topologically sorted actions (execute in order)
  - risk_level: low | medium | high | critical

FAANG PARALLEL: LinkedIn DataHub impact analysis + Airbnb Minerva change propagation.""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "source_asset_name": {
                        "type": "string",
                        "description": "Name of the asset that changed (e.g. 'fact_sales')",
                    },
                    "workspace_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Workspace IDs to scan (source + all consumers)",
                    },
                    "change_description": {
                        "type": "string",
                        "default": "schema change",
                        "description": "Human-readable description of the change",
                    },
                },
                "required": ["source_asset_name", "workspace_ids"],
            },
        },
        # ------------------------------------------------------------------ #
        #  Phase K — Guards: Freshness + Maintenance                           #
        # ------------------------------------------------------------------ #
        {
            "name": "scan_freshness",
            "description": """Scan SQL Endpoint sync freshness across workspaces.

Triggers refreshMetadata on every SQL Endpoint and checks each table's
lastSuccessfulSyncDateTime against SLA thresholds (fnmatch patterns).

DETECTS: Tables silently days stale with no Fabric alert.
Live evidence: raw_customer_events was 6 days behind with zero notification.

RETURNS: Per-table freshness status + SLA violations.

FAANG PARALLEL: Google BigQuery slot replication lag monitoring in Monarch.""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "workspace_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Fabric workspace IDs to scan",
                    },
                    "sla_thresholds": {
                        "type": "object",
                        "additionalProperties": {"type": "number"},
                        "description": "Pattern-to-hours SLA map. Example: {'fact_*': 1.0, 'dim_*': 24.0}. Default: fact_*=1h, dim_*=24h, raw_*=6h, *=24h",
                    },
                    "lro_timeout_secs": {
                        "type": "integer",
                        "default": 300,
                        "description": "Max seconds to wait for refreshMetadata LRO per endpoint",
                    },
                },
                "required": ["workspace_ids"],
            },
        },
        {
            "name": "run_table_maintenance",
            "description": """Validate and optionally submit TableMaintenance jobs.

Pre-validates table names BEFORE submission to prevent silent Spark failures:
  - Control characters in name -> rejected
  - Schema-qualified (custom_schema.dim_date) -> rejected
  - Not in Delta registry -> rejected

Live evidence: invalid names accepted (202) but fail async, wasting 4-9 min
of Spark compute per bad submission on Trial capacity.

ALWAYS use dry_run=True first to preview what would be submitted.

FAANG PARALLEL: Stripe API gateway pre-submission validation.""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "workspace_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Fabric workspace IDs to process",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "default": True,
                        "description": "True = validate only, no submission. ALWAYS use True first!",
                    },
                    "table_filter": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional whitelist of table names. Omit = all tables.",
                    },
                    "queue_pressure_threshold": {
                        "type": "integer",
                        "default": 3,
                        "description": "Skip if active jobs >= this (Trial capacity protection)",
                    },
                },
                "required": ["workspace_ids"],
            },
        },
        # ------------------------------------------------------------------ #
        #  Memory / RAG Tools                                                  #
        # ------------------------------------------------------------------ #
        {
            "name": "find_similar_operations",
            "description": """🔍 Find past operations semantically similar to a proposed change.

Uses vector similarity (RAG) over operation history — not exact SQL matching.
"Revenue YTD rename" surfaces lessons from "Sales YTD rename" even with
different names.

FAANG PARALLEL: Same as GitHub Copilot context retrieval and Airbnb Minerva
historical refactor lookup.

Returns ranked list with similarity scores and key lessons from past outcomes.""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "proposed_change": {
                        "type": "string",
                        "description": "Description of the proposed change (natural language)",
                    },
                    "change_type": {
                        "type": "string",
                        "description": "Filter by change type (rename_measure, schema_drift, etc.)",
                    },
                    "top_k": {
                        "type": "integer",
                        "default": 5,
                        "description": "Number of similar operations to return",
                    },
                },
                "required": ["proposed_change"],
            },
        },
        {
            "name": "get_risk_context",
            "description": """⚠️ Get historical risk context for a proposed change.

Synthesises past similar operations into actionable risk signals:
- Historical failure rate for similar changes
- Common failure reasons / anti-patterns
- Concrete recommendations based on what worked (and what didn't)

Call this BEFORE any high-risk rename or schema change!""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "proposed_change": {
                        "type": "string",
                        "description": "Description of the proposed change (natural language)",
                    },
                    "top_k": {
                        "type": "integer",
                        "default": 10,
                        "description": "Number of historical operations to sample",
                    },
                },
                "required": ["proposed_change"],
            },
        },
        {
            "name": "memory_stats",
            "description": "Get statistics about the operation memory vector store (total indexed, collection size, last indexed).",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
        {
            "name": "reindex_operation_memory",
            "description": """🔄 Rebuild the vector index from the existing SQLite audit trail.

Use this after first-time setup, upgrading the embedding model,
or recovering from a corrupted vector store.

Reads all historical snapshots from SQLite and re-embeds them into ChromaDB.""",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
        {
            "name": "get_session_summary",
            "description": """📝 Get a summary of the last agent session for cross-session memory.

Retrieves persisted context so the agent knows what happened in prior
conversations — what was renamed, what rolled back, what's in-progress.

FAANG PARALLEL: Same pattern as ChatGPT memory, applied to data infrastructure ops.""",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "workspace_id": {
                        "type": "string",
                        "description": "Workspace ID to get session summary for (defaults to active workspace)",
                    },
                },
                "required": [],
            },
        },
    ]


class MCPServer:
    """
    MCP Server wrapper for FabricAgent.
    
    Handles tool routing and response formatting.
    """
    
    def __init__(self):
        """Initialize the MCP server."""
        self.agent: Optional[FabricAgent] = None
        self._initialized = False
    
    async def initialize(self) -> None:
        """Initialize the underlying agent."""
        if self._initialized:
            return
        
        logger.info("Initializing MCP server agent")
        
        try:
            self.agent = FabricAgent(
                log_level="DEBUG",
                storage_path=Path("./data/mcp_audit.db"),
            )
            await self.agent.initialize()
            self._initialized = True
            logger.info("MCP server agent initialized")
        except Exception as e:
            logger.error(f"Failed to initialize agent: {e}")
            self._initialized = False
    
    async def handle_tool_call(
        self,
        name: str,
        arguments: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Route tool calls to the appropriate handler.
        
        Args:
            name: Tool name.
            arguments: Tool arguments.
        
        Returns:
            Tool result as dictionary.
        """
        if not self._initialized:
            await self.initialize()
        
        if not self._initialized or not self.agent or not self.agent.tools:
            return {
                "error": "Agent not initialized",
                "details": "Failed to connect to Fabric API",
            }
        
        tools = self.agent.tools
        
        try:
            if name == "list_workspaces":
                result = await tools.list_workspaces(ListWorkspacesInput())
                return result.model_dump()
            
            elif name == "set_workspace":
                result = await tools.set_workspace(
                    SetWorkspaceInput(**arguments)
                )
                return result.model_dump()
            
            elif name == "list_items":
                result = await tools.list_items(ListItemsInput(**arguments))
                return result.model_dump()
            
            elif name == "get_semantic_model":
                result = await tools.get_semantic_model(
                    GetSemanticModelInput(**arguments)
                )
                return result.model_dump()
            
            elif name == "get_measures":
                result = await tools.get_measures(GetMeasuresInput(**arguments))
                return result.model_dump()
            
            elif name == "get_report_definition":
                result = await tools.get_report_definition(
                    GetReportDefinitionInput(**arguments)
                )
                return result.model_dump()
            
            elif name == "analyze_impact":
                # Convert string to enum
                if "target_type" in arguments:
                    arguments["target_type"] = TargetType(arguments["target_type"])
                result = await tools.analyze_impact(
                    AnalyzeImpactInput(**arguments)
                )
                return result.model_dump()
            
            elif name == "rename_measure":
                result = await tools.rename_measure(
                    RenameMeasureInput(**arguments)
                )
                return result.model_dump()
            elif name == "smart_rename_measure":
                result = await tools.smart_rename_measure(
                    SmartRenameMeasureInput(**arguments)
                )
                return result.model_dump()


            
            elif name == "get_refactor_history":
                result = await tools.get_refactor_history(
                    GetRefactorHistoryInput(**arguments)
                )
                return result.model_dump()
            
            elif name == "rollback":
                result = await tools.rollback(RollbackInput(**arguments))
                return result.model_dump()
            
            elif name == "get_connection_status":
                result = await tools.get_connection_status(
                    GetConnectionStatusInput()
                )
                return result.model_dump()
            
            elif name == "setup_enterprise_demo":
                from fabric_agent.tools.setup_enterprise_demo import SetupEnterpriseDemoInput
                result = await tools.setup_enterprise_demo(
                    SetupEnterpriseDemoInput(**arguments)
                )
                return result.model_dump()
            
            elif name == "analyze_refactor_impact":
                from fabric_agent.tools.safety_refactor import AnalyzeRefactorImpactInput
                result = await tools.analyze_refactor_impact(
                    AnalyzeRefactorImpactInput(**arguments)
                )
                return result.model_dump()
            
            elif name == "safe_refactor":
                from fabric_agent.tools.safety_refactor import SafeRefactorInput
                result = await tools.safe_refactor(
                    SafeRefactorInput(**arguments)
                )
                return result.model_dump()
            
            elif name == "rollback_last_action":
                from fabric_agent.tools.safety_refactor import RollbackInput
                result = await tools.rollback_last_action(
                    RollbackInput(**arguments)
                )
                return result.model_dump()
            
            elif name == "build_workspace_graph":
                from fabric_agent.tools.workspace_graph import WorkspaceGraphBuilder
                from pathlib import Path
                
                if not tools.workspace_id:
                    return {"error": "No workspace selected. Call set_workspace first."}
                
                builder = WorkspaceGraphBuilder(
                    include_measure_graph=arguments.get("include_measures", True),
                    max_measure_nodes_per_model=arguments.get("max_measures_per_model", 500),
                )
                
                graph = await builder.build(
                    client=tools.client,
                    workspace_id=tools.workspace_id,
                    workspace_name=tools.workspace_name,
                )
                
                # Optionally save to file
                save_path = arguments.get("save_to_file")
                if save_path:
                    Path(save_path).parent.mkdir(parents=True, exist_ok=True)
                    Path(save_path).write_text(
                        json.dumps(graph, indent=2), encoding="utf-8"
                    )
                    graph["saved_to"] = save_path
                
                return graph
            
            elif name == "analyze_change_impact":
                from fabric_agent.tools.workspace_graph import WorkspaceGraphBuilder, GraphImpactAnalyzer
                
                if not tools.workspace_id:
                    return {"error": "No workspace selected. Call set_workspace first."}
                
                change_type = arguments.get("change_type")
                
                # Build graph
                builder = WorkspaceGraphBuilder(include_measure_graph=True)
                graph = await builder.build(
                    client=tools.client,
                    workspace_id=tools.workspace_id,
                    workspace_name=tools.workspace_name,
                )
                
                analyzer = GraphImpactAnalyzer(graph)
                
                if change_type == "measure_rename":
                    model_name = arguments.get("model_name")
                    measure_name = arguments.get("measure_name")
                    
                    if not model_name or not measure_name:
                        return {"error": "model_name and measure_name required for measure_rename"}
                    
                    # Find model ID
                    model = next(
                        (n for n in graph["nodes"] 
                         if n["type"] == "semantic_model" and n["name"] == model_name),
                        None
                    )
                    if not model:
                        return {"error": f"Model not found: {model_name}"}
                    
                    return analyzer.analyze_measure_rename_impact(model["id"], measure_name)
                
                elif change_type == "notebook_change":
                    notebook_name = arguments.get("notebook_name")
                    
                    if not notebook_name:
                        return {"error": "notebook_name required for notebook_change"}
                    
                    notebook = next(
                        (n for n in graph["nodes"]
                         if n["type"] == "notebook" and n["name"] == notebook_name),
                        None
                    )
                    if not notebook:
                        return {"error": f"Notebook not found: {notebook_name}"}
                    
                    return analyzer.analyze_notebook_change_impact(notebook["id"])
                
                elif change_type == "lakehouse_change":
                    lakehouse_name = arguments.get("lakehouse_name")
                    
                    if not lakehouse_name:
                        return {"error": "lakehouse_name required for lakehouse_change"}
                    
                    lakehouse = next(
                        (n for n in graph["nodes"]
                         if n["type"] == "lakehouse" and n["name"] == lakehouse_name),
                        None
                    )
                    if not lakehouse:
                        return {"error": f"Lakehouse not found: {lakehouse_name}"}
                    
                    return analyzer.analyze_lakehouse_change_impact(lakehouse["id"])

                elif change_type == "table_schema_change":
                    table_name = arguments.get("table_name")
                    lakehouse_name = arguments.get("lakehouse_name")

                    if not table_name:
                        return {"error": "table_name required for table_schema_change"}

                    nodes = graph.get("nodes", [])
                    table_nodes = [
                        n for n in nodes
                        if n.get("type") == "table" and n.get("name") == table_name
                    ]

                    if lakehouse_name:
                        lakehouse = next(
                            (
                                n for n in nodes
                                if n.get("type") == "lakehouse" and n.get("name") == lakehouse_name
                            ),
                            None,
                        )
                        if not lakehouse:
                            return {"error": f"Lakehouse not found: {lakehouse_name}"}
                        table_nodes = [
                            n for n in table_nodes
                            if n.get("parent_id") == lakehouse.get("id")
                        ]

                    if not table_nodes:
                        if lakehouse_name:
                            return {"error": f"Table not found: {lakehouse_name}.{table_name}"}
                        return {"error": f"Table not found: {table_name}"}

                    impact = analyzer.analyze_table_change_impact(table_nodes[0]["id"])
                    impact["matched_tables"] = len(table_nodes)
                    return impact
                
                else:
                    return {"error": f"Unknown change_type: {change_type}"}

            elif name == "scan_workspace_health":
                result = await tools.scan_workspace_health(
                    ScanWorkspaceHealthInput(**arguments)
                )
                return result.model_dump()

            elif name == "build_healing_plan":
                result = await tools.build_healing_plan(
                    BuildHealingPlanInput(**arguments)
                )
                return result.model_dump()

            elif name == "execute_healing_plan":
                result = await tools.execute_healing_plan(
                    ExecuteHealingPlanInput(**arguments)
                )
                return result.model_dump()

            elif name == "find_similar_operations":
                result = await tools.find_similar_operations(
                    FindSimilarOperationsInput(**arguments)
                )
                return result.model_dump()

            elif name == "get_risk_context":
                result = await tools.get_risk_context(
                    GetRiskContextInput(**arguments)
                )
                return result.model_dump()

            elif name == "memory_stats":
                result = await tools.memory_stats(MemoryStatsInput(**arguments))
                return result.model_dump()

            elif name == "reindex_operation_memory":
                result = await tools.reindex_operation_memory(
                    ReindexMemoryInput(**arguments)
                )
                return result.model_dump()

            elif name == "get_session_summary":
                result = await tools.get_session_summary(
                    GetSessionSummaryInput(**arguments)
                )
                return result.model_dump()

            # Phase E — Shortcut Cascade Tools
            elif name == "scan_shortcut_cascade":
                result = await tools.scan_shortcut_cascade(
                    ScanShortcutCascadeInput(**arguments)
                )
                return result.model_dump()

            elif name == "build_shortcut_healing_plan":
                result = await tools.build_shortcut_healing_plan(
                    BuildShortcutHealingPlanInput(**arguments)
                )
                return result.model_dump()

            elif name == "approve_shortcut_healing":
                result = await tools.approve_shortcut_healing(
                    ApproveShortcutHealingInput(**arguments)
                )
                return result.model_dump()

            elif name == "execute_shortcut_healing":
                result = await tools.execute_shortcut_healing(
                    ExecuteShortcutHealingInput(**arguments)
                )
                return result.model_dump()

            # Phase I — Enterprise Blast Radius
            elif name == "get_enterprise_blast_radius":
                from fabric_agent.tools.models import EnterpriseBlastRadiusInput
                result = await tools.get_enterprise_blast_radius(
                    EnterpriseBlastRadiusInput(**arguments)
                )
                return result.model_dump()

            # Phase K — Guards: Freshness + Maintenance
            elif name == "scan_freshness":
                from fabric_agent.tools.models import ScanFreshnessInput
                result = await tools.scan_freshness(
                    ScanFreshnessInput(**arguments)
                )
                return result.model_dump()

            elif name == "run_table_maintenance":
                from fabric_agent.tools.models import RunTableMaintenanceInput
                result = await tools.run_table_maintenance(
                    RunTableMaintenanceInput(**arguments)
                )
                return result.model_dump()

            else:
                return {"error": f"Unknown tool: {name}"}

        except ValidationError as e:
            logger.error(f"Validation error in {name}: {e}")
            return {
                "error": "Validation error",
                "details": str(e),
            }
        
        except Exception as e:
            logger.error(f"Error in {name}: {e}")
            return {
                "error": str(e),
                "tool": name,
            }


# Global server instance
_mcp_server = MCPServer()


def create_mcp_server() -> Optional[Server]:
    """Create and configure the MCP server."""
    if not MCP_AVAILABLE:
        logger.error("MCP SDK not available")
        return None
    
    server = Server("fabric-agent")
    
    @server.list_tools()
    async def list_tools() -> List[Tool]:
        """List available tools."""
        return [Tool(**tool) for tool in get_tool_definitions()]
    
    @server.call_tool()
    async def call_tool(
        name: str,
        arguments: Dict[str, Any],
    ) -> List[TextContent]:
        """Handle tool call."""
        result = await _mcp_server.handle_tool_call(name, arguments)
        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
    
    return server


async def run_server() -> None:
    """Run the MCP server with stdio transport."""
    logger.info("Starting Fabric Agent MCP Server")
    
    server = create_mcp_server()
    if not server:
        logger.error("Failed to create MCP server")
        return
    
    # Initialize agent
    await _mcp_server.initialize()
    
    # Run with stdio transport
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


def main() -> None:
    """Entry point for the MCP server."""
    # Configure logging to stderr (stdout is for MCP JSON-RPC)
    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    )
    
    logger.info("🚀 Fabric Agent MCP Server starting...")
    asyncio.run(run_server())


if __name__ == "__main__":
    main()
