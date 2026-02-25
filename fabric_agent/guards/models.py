"""
Guards Domain Models
====================

Internal dataclasses for the Freshness Guard and Maintenance Guard modules.

Pattern: Same as ``healing/models.py`` — dataclasses with UUID4 IDs, UTC
timestamps, ``to_dict()``, and ``(str, Enum)`` for JSON-safe enums.

FAANG PARALLEL:
    Google SRE uses typed "Violation" and "Finding" structs in Monarch and
    Borgmon to represent SLA breaches uniformly across thousands of services.
    Each struct carries a UUID, timestamp, and severity so that the alerting
    pipeline can deduplicate, aggregate, and route without knowing the specific
    check that produced it. We replicate this pattern here.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    pass  # reserved for future FabricApiClient forward ref


# =============================================================================
# Enums
# =============================================================================


class FreshnessStatus(str, Enum):
    """Classification of a table's sync freshness relative to its SLA."""

    HEALTHY = "healthy"
    """Table's lastSuccessfulSyncDateTime is within the SLA threshold."""

    STALE = "stale"
    """Table exceeded its SLA threshold — data is older than allowed."""

    NEVER_SYNCED = "never_synced"
    """No lastSuccessfulSyncDateTime returned — table has never been synced."""

    SYNC_FAILED = "sync_failed"
    """Fabric reported sync status as Failure for this table."""

    UNKNOWN = "unknown"
    """Could not determine freshness (API error, LRO timeout, etc.)."""


class MaintenanceJobStatus(str, Enum):
    """Status of a TableMaintenance Spark job."""

    SUCCEEDED = "succeeded"
    RUNNING = "running"
    FAILED = "failed"
    CANCELLED = "cancelled"
    QUEUED = "queued"
    NOT_SUBMITTED = "not_submitted"
    SKIPPED_QUEUE = "skipped_queue"
    SKIPPED_DRY_RUN = "skipped_dry_run"


class GuardSeverity(str, Enum):
    """Severity level for guard violations — mirrors RiskLevel without cross-dep."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


# =============================================================================
# Freshness Guard Models
# =============================================================================


@dataclass
class TableSyncStatus:
    """
    Per-table sync status from the SQL Endpoint ``refreshMetadata`` LRO.

    FAANG PARALLEL:
        Google BigQuery slot replication lag is tracked per-table in Monarch
        with per-table P50/P99 SLA windows. This dataclass captures the
        same information: how stale is this specific table, and which SLA
        threshold was it measured against?
    """

    table_name: str = ""
    last_successful_sync_dt: Optional[str] = None
    sync_status: str = "Unknown"
    start_dt: Optional[str] = None
    end_dt: Optional[str] = None
    hours_since_sync: Optional[float] = None
    freshness_status: FreshnessStatus = FreshnessStatus.UNKNOWN
    sla_threshold_hours: float = 24.0
    sla_pattern: str = "*"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "last_successful_sync_dt": self.last_successful_sync_dt,
            "sync_status": self.sync_status,
            "start_dt": self.start_dt,
            "end_dt": self.end_dt,
            "hours_since_sync": self.hours_since_sync,
            "freshness_status": self.freshness_status.value,
            "sla_threshold_hours": self.sla_threshold_hours,
            "sla_pattern": self.sla_pattern,
        }


@dataclass
class FreshnessViolation:
    """One SLA violation — a table that exceeded its freshness threshold."""

    violation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    workspace_id: str = ""
    workspace_name: str = ""
    sql_endpoint_id: str = ""
    sql_endpoint_name: str = ""
    table_status: Optional[TableSyncStatus] = None
    detected_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "violation_id": self.violation_id,
            "workspace_id": self.workspace_id,
            "workspace_name": self.workspace_name,
            "sql_endpoint_id": self.sql_endpoint_id,
            "sql_endpoint_name": self.sql_endpoint_name,
            "table_status": self.table_status.to_dict() if self.table_status else None,
            "detected_at": self.detected_at,
        }


@dataclass
class FreshnessScanResult:
    """Complete result from ``FreshnessGuard.scan()``."""

    scan_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    workspace_ids: List[str] = field(default_factory=list)
    scanned_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    total_tables: int = 0
    healthy_count: int = 0
    violation_count: int = 0
    violations: List[FreshnessViolation] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    scan_duration_ms: int = 0

    @property
    def has_violations(self) -> bool:
        return self.violation_count > 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "scan_id": self.scan_id,
            "workspace_ids": self.workspace_ids,
            "scanned_at": self.scanned_at,
            "total_tables": self.total_tables,
            "healthy_count": self.healthy_count,
            "violation_count": self.violation_count,
            "violations": [v.to_dict() for v in self.violations],
            "errors": self.errors,
            "scan_duration_ms": self.scan_duration_ms,
        }


# =============================================================================
# Maintenance Guard Models
# =============================================================================


@dataclass
class TableValidationResult:
    """
    Result of validating a table name before submitting to TableMaintenance.

    FAANG PARALLEL:
        Stripe's API gateway validates every request against a JSON Schema
        contract at the edge — rejecting invalid requests at O(1) cost
        rather than burning backend compute on requests that will fail.
    """

    table_name: str = ""
    is_valid: bool = False
    rejection_reason: Optional[str] = None
    resolved_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "is_valid": self.is_valid,
            "rejection_reason": self.rejection_reason,
            "resolved_name": self.resolved_name,
        }


@dataclass
class MaintenanceJobRecord:
    """Record of a single TableMaintenance job submission and its outcome."""

    job_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    workspace_id: str = ""
    lakehouse_id: str = ""
    lakehouse_name: str = ""
    table_name: str = ""
    validation: Optional[TableValidationResult] = None
    submitted_at: Optional[str] = None
    fabric_job_id: Optional[str] = None
    status: MaintenanceJobStatus = MaintenanceJobStatus.NOT_SUBMITTED
    completed_at: Optional[str] = None
    error: Optional[str] = None
    dry_run: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "workspace_id": self.workspace_id,
            "lakehouse_id": self.lakehouse_id,
            "lakehouse_name": self.lakehouse_name,
            "table_name": self.table_name,
            "validation": self.validation.to_dict() if self.validation else None,
            "submitted_at": self.submitted_at,
            "fabric_job_id": self.fabric_job_id,
            "status": self.status.value,
            "completed_at": self.completed_at,
            "error": self.error,
            "dry_run": self.dry_run,
        }


@dataclass
class MaintenanceRunResult:
    """Outcome of ``MaintenanceGuard.run_maintenance()``."""

    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    workspace_ids: List[str] = field(default_factory=list)
    dry_run: bool = True
    total_tables: int = 0
    validated: int = 0
    rejected: int = 0
    submitted: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped_queue: int = 0
    job_records: List[MaintenanceJobRecord] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    started_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    completed_at: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.failed == 0 and len(self.errors) == 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "workspace_ids": self.workspace_ids,
            "dry_run": self.dry_run,
            "total_tables": self.total_tables,
            "validated": self.validated,
            "rejected": self.rejected,
            "submitted": self.submitted,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "skipped_queue": self.skipped_queue,
            "job_records": [r.to_dict() for r in self.job_records],
            "errors": self.errors,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }
