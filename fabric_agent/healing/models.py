"""
Self-Healing Infrastructure — Data Models
==========================================

Defines the core data structures for the healing system.

DATA FLOW:
    AnomalyDetector.scan()
        → List[Anomaly]                 ← each detected issue
    SelfHealer.build_plan()
        → HealingPlan                   ← what to fix and how
    SelfHealer.execute_healing_plan()
        → HealingResult                 ← what was done
    SelfHealingMonitor.run_once()
        → HealthReport                  ← full scan + heal summary

ANOMALY TYPES (from FAANG-grade infra playbooks):
    BROKEN_SHORTCUT     → shortcut points to deleted source
    SCHEMA_DRIFT        → table schema changed vs registered contract
    ORPHAN_ASSET        → measure/table with no downstream consumers
    STALE_TABLE         → table not refreshed within SLA window
    PIPELINE_FAILURE    → upstream pipeline last run failed

PHASE E — SHORTCUT CASCADE MODELS:
    ShortcutDefinition  → full shortcut spec from Fabric API (source + target)
    FixSuggestion       → one proposed fix for one impacted asset
    CascadeImpact       → all assets broken because of one broken shortcut
    ShortcutHealingPlan → the full cascade plan with auto vs manual splits
    ShortcutHealingResult → execution outcome for cascade plan
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


class AnomalyType(str, Enum):
    """
    Classification of detected infrastructure anomalies.

    Each type has a different auto-heal strategy — see healer.py.
    """
    BROKEN_SHORTCUT = "broken_shortcut"
    SCHEMA_DRIFT = "schema_drift"
    ORPHAN_ASSET = "orphan_asset"
    STALE_TABLE = "stale_table"
    PIPELINE_FAILURE = "pipeline_failure"


class RiskLevel(str, Enum):
    """Severity tier used to gate automated vs manual actions."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class HealActionStatus(str, Enum):
    """Outcome of a single heal action."""
    PENDING = "pending"
    APPLIED = "applied"
    SKIPPED = "skipped"    # dry_run or policy blocked
    FAILED = "failed"


class ActionType(str, Enum):
    """
    Machine-readable healing action types.

    Replaces bare string literals throughout the codebase — typos become
    AttributeErrors at import time rather than silent no-ops at runtime.

    FAANG PARALLEL:
        Google SRE runbooks define action categories as enum constants so
        auto-remediation pipelines can exhaustively match without string typos.
    """
    VERIFY_SHORTCUT = "verify_shortcut"
    RECREATE_SHORTCUT = "recreate_shortcut"
    TRIGGER_PIPELINE = "trigger_pipeline"
    REFRESH_MODEL = "refresh_model"
    NOTIFY_OWNER = "notify_owner"


class Urgency(str, Enum):
    """
    Time-sensitivity tier for a healing action, used to build the ordered
    healing plan: IMMEDIATE actions run first, ADVISORY last.

    IMMEDIATE         — must run before any dependent fix (e.g. verify shortcut
                        before refreshing the model that reads it)
    AFTER_SOURCE_FIXED — safe to run once the source asset is healthy
    ADVISORY          — informational; no automated action needed
    """
    IMMEDIATE = "immediate"
    AFTER_SOURCE_FIXED = "after_source_fixed"
    ADVISORY = "advisory"


@dataclass
class Anomaly:
    """
    A detected infrastructure problem in the Fabric workspace.

    DESIGN: Anomalies are immutable observations — they describe what
    was found, not what to do about it. Healing actions live in HealAction.

    Attributes:
        anomaly_id: Unique ID for this finding (UUID).
        anomaly_type: Classification (see AnomalyType).
        severity: Risk level driving auto-heal vs manual review routing.
        asset_id: Fabric item ID (workspace item, model, table, etc.).
        asset_name: Human-readable name for the affected asset.
        workspace_id: Workspace containing the anomaly.
        details: Free-text explanation of what was detected.
        can_auto_heal: True if policy allows automatic remediation.
        heal_action: Suggested action type string (e.g. "recreate_shortcut").
        detected_at: UTC timestamp of detection.
        metadata: Arbitrary extra context (contract path, last_refresh, etc.).
    """
    anomaly_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    anomaly_type: AnomalyType = AnomalyType.BROKEN_SHORTCUT
    severity: RiskLevel = RiskLevel.MEDIUM
    asset_id: str = ""
    asset_name: str = ""
    workspace_id: str = ""
    details: str = ""
    can_auto_heal: bool = False
    heal_action: Optional[str] = None
    detected_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "anomaly_id": self.anomaly_id,
            "anomaly_type": self.anomaly_type.value,
            "severity": self.severity.value,
            "asset_id": self.asset_id,
            "asset_name": self.asset_name,
            "workspace_id": self.workspace_id,
            "details": self.details,
            "can_auto_heal": self.can_auto_heal,
            "heal_action": self.heal_action,
            "detected_at": self.detected_at,
            "metadata": self.metadata,
        }


@dataclass
class HealAction:
    """
    A single remediation step within a HealingPlan.

    DESIGN: HealActions are the "what" and "result" of healing.
    The SelfHealer populates status + applied_at + error after execution.

    Attributes:
        action_id: Unique ID.
        anomaly_id: References the Anomaly this fixes.
        action_type: Machine-readable action name (e.g. "recreate_shortcut").
        description: Human-readable explanation for audit trail.
        requires_approval: True → queued for manual review, not auto-applied.
        dry_run_only: True → never auto-apply even if approved.
        status: Outcome (filled by SelfHealer).
        applied_at: UTC timestamp when the action was applied.
        error: Error message if status=FAILED.
    """
    action_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    anomaly_id: str = ""
    action_type: str = ""
    description: str = ""
    requires_approval: bool = False
    dry_run_only: bool = False
    status: HealActionStatus = HealActionStatus.PENDING
    applied_at: Optional[str] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def mark_applied(self) -> None:
        self.status = HealActionStatus.APPLIED
        self.applied_at = datetime.now(timezone.utc).isoformat()

    def mark_skipped(self, reason: str = "") -> None:
        self.status = HealActionStatus.SKIPPED
        self.error = reason

    def mark_failed(self, error: str) -> None:
        self.status = HealActionStatus.FAILED
        self.error = error

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action_id": self.action_id,
            "anomaly_id": self.anomaly_id,
            "action_type": self.action_type,
            "description": self.description,
            "requires_approval": self.requires_approval,
            "dry_run_only": self.dry_run_only,
            "status": self.status.value,
            "applied_at": self.applied_at,
            "error": self.error,
            "metadata": self.metadata,
        }


@dataclass
class HealingPlan:
    """
    Full remediation plan for a set of anomalies.

    Splits actions into:
    - auto_actions:   safe to apply immediately (additive schema changes, etc.)
    - manual_actions: require human approval (breaking changes, deletions)

    POLICY GATE: The SelfHealingMonitor checks can_auto_heal on each anomaly
    and the requires_approval flag on each action before executing.
    """
    plan_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    anomalies: List[Anomaly] = field(default_factory=list)
    auto_actions: List[HealAction] = field(default_factory=list)
    manual_actions: List[HealAction] = field(default_factory=list)
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def total_actions(self) -> int:
        return len(self.auto_actions) + len(self.manual_actions)

    @property
    def auto_action_count(self) -> int:
        return len(self.auto_actions)

    @property
    def manual_action_count(self) -> int:
        return len(self.manual_actions)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "anomalies": [a.to_dict() for a in self.anomalies],
            "auto_actions": [a.to_dict() for a in self.auto_actions],
            "manual_actions": [a.to_dict() for a in self.manual_actions],
            "total_actions": self.total_actions,
            "created_at": self.created_at,
        }


@dataclass
class HealingResult:
    """
    Outcome of executing a HealingPlan.

    Records which actions succeeded, failed, or were skipped (dry_run).
    This is persisted to OneLake as the audit trail for healing operations.
    """
    result_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    plan_id: str = ""
    applied: int = 0
    failed: int = 0
    skipped: int = 0
    dry_run: bool = True
    actions: List[HealAction] = field(default_factory=list)
    completed_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    errors: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return self.failed == 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "result_id": self.result_id,
            "plan_id": self.plan_id,
            "applied": self.applied,
            "failed": self.failed,
            "skipped": self.skipped,
            "dry_run": self.dry_run,
            "success": self.success,
            "completed_at": self.completed_at,
            "errors": self.errors,
        }


@dataclass
class HealthReport:
    """
    Full workspace health scan result.

    Generated by SelfHealingMonitor.run_once(). This is the top-level
    artifact — it summarises what was scanned, what was found, and what
    was (or will be) done about it.

    Written to:
    - OneLake Delta table: fabric_agent_health_log  (Fabric production)
    - data/health_logs/<scan_id>.json               (local development)

    SLACK TRIGGER: If manual_required > 0, SelfHealingMonitor sends a
    Slack/Teams alert so humans can review the manual_actions.
    """
    scan_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    workspace_ids: List[str] = field(default_factory=list)
    scanned_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    total_assets: int = 0
    healthy: int = 0
    anomalies_found: int = 0
    auto_healed: int = 0
    manual_required: int = 0
    healing_plan: Optional[HealingPlan] = None
    healing_result: Optional[HealingResult] = None
    scan_duration_ms: int = 0
    errors: List[str] = field(default_factory=list)

    @property
    def health_score(self) -> float:
        """0.0–1.0 health score. 1.0 = fully healthy."""
        if self.total_assets == 0:
            return 1.0
        return self.healthy / self.total_assets

    def to_dict(self) -> Dict[str, Any]:
        return {
            "scan_id": self.scan_id,
            "workspace_ids": self.workspace_ids,
            "scanned_at": self.scanned_at,
            "total_assets": self.total_assets,
            "healthy": self.healthy,
            "anomalies_found": self.anomalies_found,
            "auto_healed": self.auto_healed,
            "manual_required": self.manual_required,
            "health_score": self.health_score,
            "scan_duration_ms": self.scan_duration_ms,
            "healing_plan": self.healing_plan.to_dict() if self.healing_plan else None,
            "healing_result": self.healing_result.to_dict() if self.healing_result else None,
            "errors": self.errors,
        }


# ============================================================================
# Phase E — Shortcut Cascade Models
# ============================================================================


@dataclass
class ShortcutDefinition:
    """
    Full specification of a OneLake shortcut, sourced from the Fabric REST API.

    WHAT: A shortcut is a pointer in one lakehouse that references data stored
    in another lakehouse (possibly in a different workspace). When the source
    moves or is deleted, the shortcut becomes "broken" — and every table,
    semantic model, pipeline, and report built on top of it fails silently.

    WHY THIS MATTERS:
    Graph-based detection (the old approach) loses the source location once a
    shortcut breaks. By querying the Fabric API for shortcut definitions, we
    retain the source_workspace_id / source_lakehouse_id / source_path even
    after the source is gone — enabling intelligent recreation proposals.

    FAANG PARALLEL:
    Google Cloud Dataplex uses an identical pattern: store full lineage
    metadata (source + target) at discovery time so healing is possible even
    after source deletion.

    Attributes:
        shortcut_id:           Unique ID (synthetic — Fabric API returns name, not UUID).
        name:                  Shortcut name (e.g. "DimDate").
        workspace_id:          Workspace where this shortcut lives.
        lakehouse_id:          Lakehouse item ID containing the shortcut.
        source_workspace_id:   Source workspace (where the data originates).
        source_lakehouse_id:   Source lakehouse item ID.
        source_path:           Path within source lakehouse (e.g. "Tables/DimDate").
        is_healthy:            False when source is inaccessible (set by verify_shortcut_health).
    """
    shortcut_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    workspace_id: str = ""
    lakehouse_id: str = ""
    source_workspace_id: str = ""
    source_lakehouse_id: str = ""
    source_path: str = ""
    is_healthy: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "shortcut_id": self.shortcut_id,
            "name": self.name,
            "workspace_id": self.workspace_id,
            "lakehouse_id": self.lakehouse_id,
            "source_workspace_id": self.source_workspace_id,
            "source_lakehouse_id": self.source_lakehouse_id,
            "source_path": self.source_path,
            "is_healthy": self.is_healthy,
        }


@dataclass
class FixSuggestion:
    """
    One proposed remediation for one asset impacted by a broken shortcut.

    DESIGN: A broken shortcut cascades through the lineage graph — each layer
    needs a different fix. FixSuggestion represents a single proposed action
    for a single asset in that cascade.

    ACTION TYPES:
        recreate_shortcut  → safe to auto-apply when source path is known
        refresh_model      → safe to auto-apply after shortcut is recreated
        manual_review      → pipeline / cross-workspace repairs need human eyes

    The auto_applicable flag drives the auto_actions vs manual_actions split
    in ShortcutHealingPlan — identical to Netflix's automated remediation
    tiers (safe-to-automate vs escalate-to-on-call).

    FAANG PARALLEL:
    Meta's Deltastreamer failure recovery: each downstream pipeline gets its
    own suggestion (rewind checkpoint vs. full re-backfill vs. manual intervention).

    Attributes:
        asset_id:        Fabric item ID of the affected asset.
        asset_name:      Human-readable name (for display and audit trail).
        asset_type:      "shortcut" | "semantic_model" | "pipeline" | "report".
        suggestion:      Human-readable description of the proposed fix.
        action_type:     Machine-readable action key (drives executor dispatch).
        auto_applicable: True = safe to apply without additional approval.
        metadata:        Extra context (e.g. pipeline_id, model_refresh_type).
    """
    asset_id: str = ""
    asset_name: str = ""
    asset_type: str = ""
    suggestion: str = ""
    action_type: str = ""
    auto_applicable: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "asset_name": self.asset_name,
            "asset_type": self.asset_type,
            "suggestion": self.suggestion,
            "action_type": self.action_type,
            "auto_applicable": self.auto_applicable,
            "metadata": self.metadata,
        }


@dataclass
class CascadeImpact:
    """
    All assets broken because of a single broken shortcut.

    WHAT: When a shortcut breaks, the damage ripples through every layer
    of the lineage graph:
        Broken Shortcut
          → Tables that alias it (Delta Lake views over the shortcut)
          → Semantic Models built on those tables (Power BI datasets)
          → Pipelines that refresh those models (scheduled activities)
          → Reports consuming those models (dashboards for business users)

    CascadeImpact captures ALL of these downstream assets and generates
    per-asset FixSuggestions so the operator knows exactly what to fix and
    in what order.

    TRAVERSAL: Uses LineageGraph.get_all_downstream(shortcut_id) — the same
    graph engine used by the existing ImpactAgent.

    FAANG PARALLEL:
    Google Cloud Dataflow lineage impact analysis: when a source dataset
    changes, it computes the full graph of downstream jobs + dashboards
    and generates an ordered remediation plan.

    Attributes:
        shortcut:                 The broken shortcut that triggered this cascade.
        impacted_tables:          Table names resolved from the shortcut.
        impacted_semantic_models: Semantic model IDs built on those tables.
        impacted_pipelines:       Pipeline IDs that refresh the models.
        impacted_reports:         Report IDs consuming the models.
        fix_suggestions:          One FixSuggestion per impacted asset.
    """
    shortcut: Optional[ShortcutDefinition] = None
    impacted_tables: List[str] = field(default_factory=list)
    impacted_semantic_models: List[str] = field(default_factory=list)
    impacted_pipelines: List[str] = field(default_factory=list)
    impacted_reports: List[str] = field(default_factory=list)
    fix_suggestions: List[FixSuggestion] = field(default_factory=list)

    @property
    def total_impacted(self) -> int:
        return (
            len(self.impacted_tables)
            + len(self.impacted_semantic_models)
            + len(self.impacted_pipelines)
            + len(self.impacted_reports)
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "shortcut": self.shortcut.to_dict() if self.shortcut else None,
            "impacted_tables": self.impacted_tables,
            "impacted_semantic_models": self.impacted_semantic_models,
            "impacted_pipelines": self.impacted_pipelines,
            "impacted_reports": self.impacted_reports,
            "total_impacted": self.total_impacted,
            "fix_suggestions": [s.to_dict() for s in self.fix_suggestions],
        }


@dataclass
class ShortcutHealingPlan:
    """
    Full cross-workspace shortcut cascade healing plan with human-in-the-loop gate.

    WORKFLOW:
        1. ShortcutCascadeManager.build_cascade_plan(workspace_ids)
             → ShortcutHealingPlan (approval_required=True if manual_actions exist)
        2. Human reviews auto_actions + manual_actions
        3. Human calls approve_shortcut_healing(plan_id, approved_by="...")
             → sets approved_by + approved_at
        4. execute_shortcut_healing(plan_id, dry_run=False)
             → applies auto_actions + returns ShortcutHealingResult

    APPROVAL GATE: approval_required is set to True whenever any FixSuggestion
    has auto_applicable=False. This matches the existing AgentOrchestrator
    AWAITING_APPROVAL pattern — humans decide what to do with manual_actions.

    FAANG PARALLEL:
    Amazon's automated remediation system (used in Route 53 health checks):
    auto-remediates safe changes (DNS failover), escalates dangerous ones
    (cross-region traffic shifts) to the on-call engineer.

    Attributes:
        plan_id:          Unique plan identifier for tracking + resume.
        broken_shortcuts: All shortcuts found to be broken across all workspaces.
        cascade_impacts:  One CascadeImpact per broken shortcut.
        auto_actions:     Safe FixSuggestions — can apply without additional approval.
        manual_actions:   Risky FixSuggestions — require explicit human sign-off.
        approval_required: True when any manual_actions exist.
        approved_by:      Set by approve_shortcut_healing().
        approved_at:      UTC timestamp of approval.
        created_at:       UTC timestamp of plan creation.
    """
    plan_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    broken_shortcuts: List[ShortcutDefinition] = field(default_factory=list)
    cascade_impacts: List[CascadeImpact] = field(default_factory=list)
    auto_actions: List[FixSuggestion] = field(default_factory=list)
    manual_actions: List[FixSuggestion] = field(default_factory=list)
    approval_required: bool = False
    approved_by: Optional[str] = None
    approved_at: Optional[str] = None
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def is_approved(self) -> bool:
        return self.approved_by is not None

    @property
    def total_actions(self) -> int:
        return len(self.auto_actions) + len(self.manual_actions)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "broken_shortcuts": [s.to_dict() for s in self.broken_shortcuts],
            "cascade_impacts": [c.to_dict() for c in self.cascade_impacts],
            "auto_actions": [a.to_dict() for a in self.auto_actions],
            "manual_actions": [a.to_dict() for a in self.manual_actions],
            "auto_action_count": len(self.auto_actions),
            "manual_action_count": len(self.manual_actions),
            "approval_required": self.approval_required,
            "is_approved": self.is_approved,
            "approved_by": self.approved_by,
            "approved_at": self.approved_at,
            "created_at": self.created_at,
        }


@dataclass
class ShortcutHealingResult:
    """
    Execution outcome for a ShortcutHealingPlan.

    Records which actions were applied, which failed, and which were skipped
    (e.g. dry_run=True or approval not given). Persisted to the audit trail
    so the operator has a full record of what the agent did.

    Attributes:
        result_id:     Unique ID for this execution.
        plan_id:       References the ShortcutHealingPlan that was executed.
        applied:       Count of successfully applied actions.
        failed:        Count of actions that raised an error.
        skipped:       Count of actions skipped (dry_run or policy block).
        dry_run:       True = simulation only, nothing was actually changed.
        errors:        List of error messages from failed actions.
        completed_at:  UTC timestamp of execution completion.
    """
    result_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    plan_id: str = ""
    applied: int = 0
    failed: int = 0
    skipped: int = 0
    dry_run: bool = True
    errors: List[str] = field(default_factory=list)
    completed_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def success(self) -> bool:
        return self.failed == 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "result_id": self.result_id,
            "plan_id": self.plan_id,
            "applied": self.applied,
            "failed": self.failed,
            "skipped": self.skipped,
            "dry_run": self.dry_run,
            "success": self.success,
            "errors": self.errors,
            "completed_at": self.completed_at,
        }


# =============================================================================
# PHASE I — ENTERPRISE BLAST RADIUS MODELS
# =============================================================================
# These three dataclasses form the output of analyze_enterprise_blast_radius().
# They mirror what FAANG data platforms expose for change impact:
#   - LinkedIn DataHub: UpstreamLineage + DownstreamLineage with impactedDatasets
#   - Airbnb Minerva: ChangeImpact with ordered remediation steps
#   - Google Dataplex: DataScan impact report with per-asset risk classification
# =============================================================================


@dataclass
class ImpactedAssetDetail:
    """
    Fine-grained description of a single impacted asset in an enterprise blast radius.

    Attributes:
        asset_id:           Fabric item ID (or composite like "{lh_id}:table:name").
        asset_name:         Human-readable name.
        asset_type:         "table" | "shortcut" | "pipeline" | "semantic_model" | "report".
        workspace_id:       Workspace that owns this asset.
        workspace_name:     Display name of the owning workspace.
        impact_depth:       Hops from the source asset (1 = direct, 2 = one-hop transitive…).
        impact_path:        Ordered list of asset names from source to this asset.
        action_type:        What healing action is needed (ActionType enum).
        action_description: Free-text explanation for the operator.
        auto_applicable:    True if the healer can execute this without human approval.
        urgency:            Time-sensitivity tier (Urgency enum).
    """

    asset_id: str
    asset_name: str
    asset_type: str
    workspace_id: str
    workspace_name: str
    impact_depth: int
    impact_path: List[str]
    action_type: ActionType
    action_description: str
    auto_applicable: bool
    urgency: Urgency

    def to_dict(self) -> Dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "asset_name": self.asset_name,
            "asset_type": self.asset_type,
            "workspace_id": self.workspace_id,
            "workspace_name": self.workspace_name,
            "impact_depth": self.impact_depth,
            "impact_path": self.impact_path,
            "action_type": self.action_type.value,
            "action_description": self.action_description,
            "auto_applicable": self.auto_applicable,
            "urgency": self.urgency.value,
        }


@dataclass
class WorkspaceBlastRadius:
    """
    Aggregated blast radius for a single workspace impacted by a source change.

    The `role` field distinguishes the owning workspace (source) from downstream
    consumer workspaces.  Multiple consumers can be impacted when the same source
    table is shortcutted into many teams' workspaces — exactly how cross-team data
    dependencies work at FAANG scale.

    Attributes:
        workspace_id:      Fabric workspace ID.
        workspace_name:    Display name.
        role:              "source" | "consumer"
        impacted_assets:   Ordered list of ImpactedAssetDetail (BFS depth order).
    """

    workspace_id: str
    workspace_name: str
    role: str  # "source" | "consumer"
    impacted_assets: List[ImpactedAssetDetail] = field(default_factory=list)

    @property
    def auto_count(self) -> int:
        return sum(1 for a in self.impacted_assets if a.auto_applicable)

    @property
    def manual_count(self) -> int:
        return len(self.impacted_assets) - self.auto_count

    def to_dict(self) -> Dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "workspace_name": self.workspace_name,
            "role": self.role,
            "asset_count": len(self.impacted_assets),
            "auto_count": self.auto_count,
            "manual_count": self.manual_count,
            "impacted_assets": [a.to_dict() for a in self.impacted_assets],
        }


@dataclass
class EnterpriseBlastRadius:
    """
    Complete cross-workspace blast radius for a source asset change.

    This is the top-level result returned by analyze_enterprise_blast_radius() and
    the get_enterprise_blast_radius MCP tool.  It contains:
    - Per-workspace summaries (per_workspace)
    - A single topologically-ordered healing plan (ordered_healing_steps)
    - A risk classification to drive escalation policy

    The ordered_healing_steps list is sorted so that source-side actions come first,
    followed by cross-workspace shortcuts, then consumer pipelines, then semantic models,
    then reports (advisory).  This ordering mirrors how Airbnb Minerva schedules
    automated remediation after a metric source change.

    Attributes:
        source_asset_name:         Name of the table/column that changed.
        source_workspace_name:     Workspace that owns the source.
        change_description:        What changed (e.g. "schema change: column dropped").
        total_workspaces_impacted: Number of distinct workspaces affected.
        total_assets_impacted:     Total count of impacted assets across all workspaces.
        per_workspace:             Per-workspace breakdown.
        ordered_healing_steps:     Topologically sorted flat list of all healing actions.
        risk_level:                "low" | "medium" | "high" | "critical"
        generated_at:              UTC ISO timestamp.
    """

    source_asset_name: str
    source_workspace_name: str
    change_description: str
    total_workspaces_impacted: int
    total_assets_impacted: int
    per_workspace: List[WorkspaceBlastRadius] = field(default_factory=list)
    ordered_healing_steps: List[ImpactedAssetDetail] = field(default_factory=list)
    risk_level: str = "low"  # "low" | "medium" | "high" | "critical"
    generated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_asset_name": self.source_asset_name,
            "source_workspace_name": self.source_workspace_name,
            "change_description": self.change_description,
            "total_workspaces_impacted": self.total_workspaces_impacted,
            "total_assets_impacted": self.total_assets_impacted,
            "risk_level": self.risk_level,
            "generated_at": self.generated_at,
            "per_workspace": [w.to_dict() for w in self.per_workspace],
            "ordered_healing_steps": [s.to_dict() for s in self.ordered_healing_steps],
        }
