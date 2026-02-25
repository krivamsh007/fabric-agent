"""
Guard Module Unit Tests
========================

Tests for FreshnessGuard, MaintenanceGuard, GuardMonitor, and
the corresponding Pydantic models + MCP tool methods.

Uses AsyncMock for all API calls — no real Fabric API needed.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fabric_agent.guards.models import (
    FreshnessScanResult,
    FreshnessStatus,
    FreshnessViolation,
    GuardSeverity,
    MaintenanceJobRecord,
    MaintenanceJobStatus,
    MaintenanceRunResult,
    TableSyncStatus,
    TableValidationResult,
)
from fabric_agent.guards.freshness_guard import FreshnessGuard, DEFAULT_SLA_THRESHOLDS
from fabric_agent.guards.maintenance_guard import MaintenanceGuard
from fabric_agent.guards.monitor import GuardMonitor


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_mock_client() -> AsyncMock:
    """Create a mock FabricApiClient."""
    client = AsyncMock()
    client.get = AsyncMock(return_value={})
    client.get_raw = AsyncMock()
    client.post_raw = AsyncMock()
    client.post_with_lro = AsyncMock(return_value={})
    return client


def _utc_iso(hours_ago: float = 0) -> str:
    """Return an ISO UTC timestamp `hours_ago` hours in the past."""
    dt = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return dt.isoformat()


def _mock_response(status_code: int = 200, json_data: Any = None, headers: Optional[Dict] = None):
    """Create a mock httpx.Response-like object."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = True if json_data is not None else False
    resp.json = MagicMock(return_value=json_data or {})
    resp.headers = headers or {}
    return resp


# =============================================================================
# Freshness Guard Tests
# =============================================================================


class TestFreshnessGuard:
    """Tests for FreshnessGuard."""

    @pytest.mark.asyncio
    async def test_scan_no_client_returns_empty(self):
        """No client → empty scan result, no errors."""
        guard = FreshnessGuard(client=None)
        result = await guard.scan(["ws-1"])

        assert result.total_tables == 0
        assert result.violation_count == 0
        assert result.healthy_count == 0
        assert not result.has_violations

    @pytest.mark.asyncio
    async def test_scan_no_sql_endpoints(self):
        """Workspace with no SQL endpoints → 0 tables, 0 violations."""
        client = _make_mock_client()
        client.get_raw.return_value = _mock_response(200, {"value": []})

        guard = FreshnessGuard(client=client)
        result = await guard.scan(["ws-1"])

        assert result.total_tables == 0
        assert result.violation_count == 0

    @pytest.mark.asyncio
    async def test_scan_healthy_tables(self):
        """Tables synced within SLA → healthy, no violations."""
        client = _make_mock_client()

        # _list_sql_endpoints
        sql_endpoints_resp = _mock_response(200, {
            "value": [{"id": "ep-1", "displayName": "Bronze_Landing"}]
        })
        # _get_workspace_name
        ws_resp = _mock_response(200, {"displayName": "DEV"})

        client.get_raw.side_effect = [ws_resp, sql_endpoints_resp]

        # refreshMetadata LRO returns tables
        client.post_with_lro.return_value = {
            "tables": [
                {
                    "tableName": "dim_date",
                    "lastSuccessfulSyncDateTime": _utc_iso(hours_ago=2),
                    "status": "Success",
                },
                {
                    "tableName": "dim_customer",
                    "lastSuccessfulSyncDateTime": _utc_iso(hours_ago=1),
                    "status": "Success",
                },
            ]
        }

        guard = FreshnessGuard(client=client, sla_thresholds={"dim_*": 24.0, "*": 24.0})
        result = await guard.scan(["ws-1"])

        assert result.total_tables == 2
        assert result.healthy_count == 2
        assert result.violation_count == 0
        assert not result.has_violations

    @pytest.mark.asyncio
    async def test_scan_stale_table(self):
        """Table 48 hours old with 24h SLA → stale violation."""
        client = _make_mock_client()

        ws_resp = _mock_response(200, {"displayName": "DEV"})
        ep_resp = _mock_response(200, {
            "value": [{"id": "ep-1", "displayName": "Silver_Curated"}]
        })
        client.get_raw.side_effect = [ws_resp, ep_resp]

        client.post_with_lro.return_value = {
            "tables": [
                {
                    "tableName": "fact_sales",
                    "lastSuccessfulSyncDateTime": _utc_iso(hours_ago=48),
                    "status": "Success",
                },
            ]
        }

        guard = FreshnessGuard(client=client, sla_thresholds={"fact_*": 1.0, "*": 24.0})
        result = await guard.scan(["ws-1"])

        assert result.violation_count == 1
        assert result.has_violations
        v = result.violations[0]
        assert v.table_status.freshness_status == FreshnessStatus.STALE
        assert v.table_status.hours_since_sync > 47

    @pytest.mark.asyncio
    async def test_scan_never_synced(self):
        """Table with no lastSuccessfulSyncDateTime → never_synced."""
        client = _make_mock_client()

        ws_resp = _mock_response(200, {"displayName": "DEV"})
        ep_resp = _mock_response(200, {
            "value": [{"id": "ep-1", "displayName": "Bronze"}]
        })
        client.get_raw.side_effect = [ws_resp, ep_resp]

        client.post_with_lro.return_value = {
            "tables": [
                {"tableName": "raw_events", "status": "NotRun"},
            ]
        }

        guard = FreshnessGuard(client=client)
        result = await guard.scan(["ws-1"])

        assert result.violation_count == 1
        v = result.violations[0]
        assert v.table_status.freshness_status == FreshnessStatus.NEVER_SYNCED

    @pytest.mark.asyncio
    async def test_scan_sync_failed(self):
        """Table with Failure status → sync_failed violation."""
        client = _make_mock_client()

        ws_resp = _mock_response(200, {"displayName": "DEV"})
        ep_resp = _mock_response(200, {
            "value": [{"id": "ep-1", "displayName": "Bronze"}]
        })
        client.get_raw.side_effect = [ws_resp, ep_resp]

        client.post_with_lro.return_value = {
            "tables": [
                {
                    "tableName": "raw_sales",
                    "lastSuccessfulSyncDateTime": _utc_iso(hours_ago=1),
                    "status": "Failure",
                },
            ]
        }

        guard = FreshnessGuard(client=client)
        result = await guard.scan(["ws-1"])

        assert result.violation_count == 1
        assert result.violations[0].table_status.freshness_status == FreshnessStatus.SYNC_FAILED

    @pytest.mark.asyncio
    async def test_sla_pattern_matching(self):
        """First fnmatch pattern wins; fact_* before *."""
        guard = FreshnessGuard(client=None, sla_thresholds={
            "fact_*": 1.0,
            "dim_*": 24.0,
            "raw_*": 6.0,
            "*": 48.0,
        })

        hours, pattern = guard._resolve_sla("fact_sales")
        assert hours == 1.0
        assert pattern == "fact_*"

        hours, pattern = guard._resolve_sla("dim_date")
        assert hours == 24.0
        assert pattern == "dim_*"

        hours, pattern = guard._resolve_sla("other_table")
        assert hours == 48.0
        assert pattern == "*"

    @pytest.mark.asyncio
    async def test_scan_workspace_error_isolated(self):
        """Error in one workspace does not abort others."""
        client = _make_mock_client()

        # First workspace: raise exception
        # Second workspace: return healthy tables
        call_count = [0]
        original_get_raw = client.get_raw

        async def side_effect_get_raw(path, **kwargs):
            call_count[0] += 1
            if "ws-bad" in path:
                raise RuntimeError("API unreachable")
            return _mock_response(200, {"value": [], "displayName": "OK"})

        client.get_raw = AsyncMock(side_effect=side_effect_get_raw)

        guard = FreshnessGuard(client=client)
        result = await guard.scan(["ws-bad", "ws-good"])

        assert len(result.errors) == 1
        assert "ws-bad" in result.errors[0]

    def test_scan_result_to_dict(self):
        """FreshnessScanResult.to_dict() serializes correctly."""
        result = FreshnessScanResult(
            workspace_ids=["ws-1"],
            total_tables=5,
            healthy_count=4,
            violation_count=1,
        )
        d = result.to_dict()
        assert d["total_tables"] == 5
        assert d["healthy_count"] == 4
        assert d["violation_count"] == 1


# =============================================================================
# Maintenance Guard Tests
# =============================================================================


class TestMaintenanceGuard:
    """Tests for MaintenanceGuard validation and submission."""

    def test_validate_valid_table(self):
        """Valid table name in registry → passes."""
        guard = MaintenanceGuard(client=None)
        registered = [{"name": "fact_sales"}, {"name": "dim_date"}]
        result = guard._validate_table_name("fact_sales", registered)

        assert result.is_valid
        assert result.resolved_name == "fact_sales"

    def test_validate_control_chars(self):
        """Table name with control characters → rejected."""
        guard = MaintenanceGuard(client=None)
        result = guard._validate_table_name("dim_date\x01probe", [{"name": "dim_date"}])

        assert not result.is_valid
        assert "control character" in result.rejection_reason

    def test_validate_schema_qualified(self):
        """Schema-qualified name → rejected."""
        guard = MaintenanceGuard(client=None)
        result = guard._validate_table_name("custom_schema.dim_date", [{"name": "dim_date"}])

        assert not result.is_valid
        assert "schema-qualified" in result.rejection_reason

    def test_validate_not_in_registry(self):
        """Table not in registry → rejected."""
        guard = MaintenanceGuard(client=None)
        result = guard._validate_table_name("nonexistent", [{"name": "fact_sales"}])

        assert not result.is_valid
        assert "not found" in result.rejection_reason

    def test_validate_invalid_format(self):
        """Invalid table name format → rejected."""
        guard = MaintenanceGuard(client=None)
        result = guard._validate_table_name("123-bad-name", [{"name": "123-bad-name"}])

        assert not result.is_valid
        assert "invalid table name format" in result.rejection_reason

    @pytest.mark.asyncio
    async def test_dry_run_no_submission(self):
        """Dry run → validates but never submits."""
        client = _make_mock_client()

        # _list_lakehouses
        client.get.return_value = {
            "value": [{"id": "lh-1", "displayName": "Bronze"}]
        }
        # _list_delta_tables
        client.get_raw.side_effect = [
            # _resolve_workspace_concurrency → GET workspace
            _mock_response(200, {"displayName": "DEV"}),
            # _list_delta_tables
            _mock_response(200, {
                "data": [{"name": "fact_sales"}, {"name": "dim_date"}]
            }),
        ]

        guard = MaintenanceGuard(client=client, dry_run=True)
        result = await guard.run_maintenance(["ws-1"])

        assert result.dry_run
        assert result.total_tables == 2
        assert result.validated == 2
        assert result.rejected == 0
        assert result.submitted == 0
        # post_raw should never be called in dry run
        client.post_raw.assert_not_called()

    @pytest.mark.asyncio
    async def test_mixed_valid_invalid_tables(self):
        """Mix of valid and invalid tables → only valid ones are dry-run OK."""
        client = _make_mock_client()

        client.get.return_value = {
            "value": [{"id": "lh-1", "displayName": "Silver"}]
        }
        client.get_raw.side_effect = [
            _mock_response(200, {"displayName": "DEV"}),
            _mock_response(200, {
                "data": [{"name": "fact_sales"}, {"name": "dim_date"}]
            }),
        ]

        guard = MaintenanceGuard(client=client, dry_run=True)
        # Request tables including one not in registry
        result = await guard.run_maintenance(
            ["ws-1"], table_filter=["fact_sales", "nonexistent"]
        )

        # Only fact_sales is in target_tables (filter matched only 1 from registry)
        assert result.validated == 1
        assert result.total_tables == 1

    @pytest.mark.asyncio
    async def test_no_client_returns_empty(self):
        """No client → empty run result."""
        guard = MaintenanceGuard(client=None)
        result = await guard.run_maintenance(["ws-1"])

        assert result.total_tables == 0
        assert result.submitted == 0

    def test_maintenance_run_result_to_dict(self):
        """MaintenanceRunResult.to_dict() serializes correctly."""
        result = MaintenanceRunResult(
            workspace_ids=["ws-1"],
            total_tables=3,
            validated=2,
            rejected=1,
        )
        d = result.to_dict()
        assert d["total_tables"] == 3
        assert d["validated"] == 2
        assert d["rejected"] == 1

    def test_table_validation_result_to_dict(self):
        """TableValidationResult.to_dict() serializes correctly."""
        r = TableValidationResult(
            table_name="dim_date",
            is_valid=True,
            resolved_name="dim_date",
        )
        d = r.to_dict()
        assert d["table_name"] == "dim_date"
        assert d["is_valid"] is True


# =============================================================================
# Guard Monitor Tests
# =============================================================================


class TestGuardMonitor:
    """Tests for GuardMonitor orchestrator."""

    @pytest.mark.asyncio
    async def test_run_once_both_guards(self):
        """run_once executes both guards independently."""
        client = _make_mock_client()

        # Mock responses for freshness (ws name + no endpoints)
        client.get_raw.return_value = _mock_response(200, {
            "value": [], "displayName": "DEV"
        })
        # Mock responses for maintenance (no lakehouses)
        client.get.return_value = {"value": []}

        monitor = GuardMonitor(client=client)
        summary = await monitor.run_once(["ws-1"])

        assert summary["freshness"] is not None
        assert summary["maintenance"] is not None
        assert len(summary["errors"]) == 0

    @pytest.mark.asyncio
    async def test_run_once_freshness_only(self):
        """run_once with run_maintenance=False → maintenance is None."""
        client = _make_mock_client()
        client.get_raw.return_value = _mock_response(200, {
            "value": [], "displayName": "DEV"
        })

        monitor = GuardMonitor(client=client)
        summary = await monitor.run_once(
            ["ws-1"], run_freshness=True, run_maintenance=False
        )

        assert summary["freshness"] is not None
        assert summary["maintenance"] is None

    @pytest.mark.asyncio
    async def test_run_once_error_isolation(self):
        """One guard failing doesn't block the other."""
        client = _make_mock_client()

        # Freshness guard: API errors are caught per-workspace inside scan(),
        # so the guard itself returns a result with errors rather than raising.
        client.get_raw.side_effect = RuntimeError("API down")
        # Maintenance guard gets empty lakehouses
        client.get.return_value = {"value": []}

        monitor = GuardMonitor(client=client)
        summary = await monitor.run_once(["ws-1"])

        # Freshness completed (with per-workspace errors), maintenance succeeded
        assert summary["freshness"] is not None
        freshness = summary["freshness"]
        assert len(freshness["errors"]) >= 1
        assert summary["maintenance"] is not None

    @pytest.mark.asyncio
    async def test_run_loop_max_iterations(self):
        """run_loop respects max_iterations."""
        client = _make_mock_client()
        client.get_raw.return_value = _mock_response(200, {
            "value": [], "displayName": "DEV"
        })
        client.get.return_value = {"value": []}

        monitor = GuardMonitor(client=client)
        # Run 2 iterations with 0 interval
        await monitor.run_loop(
            ["ws-1"],
            interval_minutes=0,
            max_iterations=2,
        )
        # Should complete without hanging


# =============================================================================
# Pydantic Model Tests
# =============================================================================


class TestGuardPydanticModels:
    """Tests for Pydantic serialization models in tools/models.py."""

    def test_scan_freshness_input_defaults(self):
        from fabric_agent.tools.models import ScanFreshnessInput

        inp = ScanFreshnessInput(workspace_ids=["ws-1"])
        assert inp.sla_thresholds is None
        assert inp.lro_timeout_secs == 300

    def test_scan_freshness_output_roundtrip(self):
        from fabric_agent.tools.models import (
            ScanFreshnessOutput,
            FreshnessViolationInfo,
            TableSyncInfo,
        )

        output = ScanFreshnessOutput(
            scan_id="scan-123",
            workspace_ids=["ws-1"],
            scanned_at=_utc_iso(),
            total_tables=5,
            healthy_count=4,
            violation_count=1,
            violations=[
                FreshnessViolationInfo(
                    violation_id="v-1",
                    workspace_id="ws-1",
                    workspace_name="DEV",
                    sql_endpoint_id="ep-1",
                    sql_endpoint_name="Bronze",
                    table_status=TableSyncInfo(
                        table_name="fact_sales",
                        sync_status="Success",
                        hours_since_sync=48.0,
                        freshness_status="stale",
                        sla_threshold_hours=1.0,
                        sla_pattern="fact_*",
                    ),
                    detected_at=_utc_iso(),
                )
            ],
            scan_duration_ms=1234,
        )

        d = output.model_dump()
        assert d["violation_count"] == 1
        assert d["violations"][0]["table_status"]["table_name"] == "fact_sales"

    def test_run_table_maintenance_input_defaults(self):
        from fabric_agent.tools.models import RunTableMaintenanceInput

        inp = RunTableMaintenanceInput(workspace_ids=["ws-1"])
        assert inp.dry_run is True
        assert inp.table_filter is None
        assert inp.queue_pressure_threshold == 3

    def test_run_table_maintenance_output_roundtrip(self):
        from fabric_agent.tools.models import (
            RunTableMaintenanceOutput,
            MaintenanceJobInfo,
            TableValidationInfo,
        )

        output = RunTableMaintenanceOutput(
            run_id="run-123",
            workspace_ids=["ws-1"],
            dry_run=True,
            total_tables=3,
            validated=2,
            rejected=1,
            submitted=0,
            succeeded=0,
            failed=0,
            skipped_queue=0,
            job_records=[
                MaintenanceJobInfo(
                    job_id="j-1",
                    workspace_id="ws-1",
                    lakehouse_id="lh-1",
                    lakehouse_name="Bronze",
                    table_name="fact_sales",
                    validation=TableValidationInfo(
                        table_name="fact_sales",
                        is_valid=True,
                        resolved_name="fact_sales",
                    ),
                    status="skipped_dry_run",
                    dry_run=True,
                )
            ],
        )

        d = output.model_dump()
        assert d["total_tables"] == 3
        assert d["job_records"][0]["validation"]["is_valid"] is True


# =============================================================================
# Enum / Model Edge Cases
# =============================================================================


class TestGuardEnums:
    """Test enum values and dataclass edge cases."""

    def test_freshness_status_values(self):
        assert FreshnessStatus.HEALTHY.value == "healthy"
        assert FreshnessStatus.STALE.value == "stale"
        assert FreshnessStatus.NEVER_SYNCED.value == "never_synced"

    def test_maintenance_job_status_values(self):
        assert MaintenanceJobStatus.SUCCEEDED.value == "succeeded"
        assert MaintenanceJobStatus.SKIPPED_DRY_RUN.value == "skipped_dry_run"
        assert MaintenanceJobStatus.SKIPPED_QUEUE.value == "skipped_queue"

    def test_guard_severity_values(self):
        assert GuardSeverity.LOW.value == "low"
        assert GuardSeverity.CRITICAL.value == "critical"

    def test_table_sync_status_to_dict(self):
        ts = TableSyncStatus(
            table_name="dim_date",
            freshness_status=FreshnessStatus.HEALTHY,
            sla_threshold_hours=24.0,
        )
        d = ts.to_dict()
        assert d["freshness_status"] == "healthy"
        assert d["sla_threshold_hours"] == 24.0

    def test_maintenance_job_record_to_dict(self):
        record = MaintenanceJobRecord(
            table_name="fact_sales",
            status=MaintenanceJobStatus.SKIPPED_DRY_RUN,
            dry_run=True,
        )
        d = record.to_dict()
        assert d["status"] == "skipped_dry_run"
        assert d["dry_run"] is True
