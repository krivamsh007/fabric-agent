"""
Healer Error Path Tests
=======================

Verifies that the SelfHealer handles partial failures, circuit breaker trips,
missing metadata, and dry-run mode correctly during plan execution.

Key safety properties tested:
  - A 503 on one action does NOT block execution of remaining actions
  - dry_run=True produces zero API calls and all-SKIPPED result
  - Manual-approval actions are skipped without approval
  - Missing metadata (no pipeline_id, no source_path) returns False (not crash)
  - Audit trail is recorded after healing completes
  - Standalone refresh_semantic_model() now raises on error (Phase 3 fix)
  - Standalone trigger_pipeline() raises on error
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock

from conftest import make_fabric_error, make_anomaly

from fabric_agent.healing.models import (
    Anomaly,
    AnomalyType,
    HealAction,
    HealActionStatus,
    HealingPlan,
    RiskLevel,
)
from fabric_agent.healing.healer import SelfHealer


# =============================================================================
# Helpers
# =============================================================================

def _broken_shortcut_anomaly(
    name: str = "dim_date",
    workspace_id: str = "ws-001",
    *,
    source_path: str = "Tables/dim_date",
    lakehouse_id: str = "lh-001",
    source_workspace_id: str = "ws-src",
    source_lakehouse_id: str = "lh-src",
) -> Anomaly:
    """Create a BROKEN_SHORTCUT anomaly with full metadata for recreation."""
    return make_anomaly(
        anomaly_type=AnomalyType.BROKEN_SHORTCUT,
        asset_name=name,
        workspace_id=workspace_id,
        can_auto_heal=True,
        metadata={
            "source_path": source_path,
            "lakehouse_id": lakehouse_id,
            "source_workspace_id": source_workspace_id,
            "source_lakehouse_id": source_lakehouse_id,
        },
    )


def _stale_table_anomaly(
    name: str = "fact_sales",
    workspace_id: str = "ws-001",
    *,
    pipeline_id: str = "pl-001",
) -> Anomaly:
    """Create a STALE_TABLE anomaly with pipeline_id for triggering refresh."""
    return make_anomaly(
        anomaly_type=AnomalyType.STALE_TABLE,
        asset_name=name,
        workspace_id=workspace_id,
        can_auto_heal=True,
        metadata={"pipeline_id": pipeline_id},
    )


def _build_plan_with_anomalies(healer: SelfHealer, anomalies: list[Anomaly]) -> HealingPlan:
    """Build a plan from anomalies using the healer's build_plan method."""
    return healer.build_plan(anomalies)


# =============================================================================
# Tests — Action-level error handling
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.unit
async def test_heal_shortcut_503_marks_action_failed(mock_fabric_client):
    """503 on shortcut recreation → action.status == FAILED, error recorded."""
    mock_fabric_client.post.side_effect = make_fabric_error(503)

    healer = SelfHealer(client=mock_fabric_client)
    anomalies = [_broken_shortcut_anomaly("dim_date")]
    plan = healer.build_plan(anomalies)
    result = await healer.execute_healing_plan(plan, dry_run=False)

    assert result.failed == 1
    assert result.applied == 0
    assert not result.success
    failed_action = [a for a in result.actions if a.status == HealActionStatus.FAILED]
    assert len(failed_action) == 1
    assert "unavailable" in failed_action[0].error.lower()


@pytest.mark.asyncio
@pytest.mark.unit
async def test_heal_shortcut_success(mock_fabric_client):
    """Shortcut recreation succeeds → action.status == APPLIED."""
    mock_fabric_client.post.return_value = {"status": "created"}

    healer = SelfHealer(client=mock_fabric_client)
    anomalies = [_broken_shortcut_anomaly("dim_date")]
    plan = healer.build_plan(anomalies)
    result = await healer.execute_healing_plan(plan, dry_run=False)

    assert result.applied == 1
    assert result.failed == 0
    assert result.success
    mock_fabric_client.post.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.unit
async def test_heal_stale_table_503_marks_failed(mock_fabric_client):
    """503 on pipeline trigger → action.status == FAILED."""
    mock_fabric_client.post.side_effect = make_fabric_error(503)

    healer = SelfHealer(client=mock_fabric_client)
    anomalies = [_stale_table_anomaly("fact_sales")]
    plan = healer.build_plan(anomalies)
    result = await healer.execute_healing_plan(plan, dry_run=False)

    assert result.failed == 1
    assert not result.success


@pytest.mark.asyncio
@pytest.mark.unit
async def test_heal_stale_table_no_pipeline_id(mock_fabric_client):
    """Missing pipeline_id in metadata → returns False (no API call)."""
    healer = SelfHealer(client=mock_fabric_client)
    anomaly = make_anomaly(
        anomaly_type=AnomalyType.STALE_TABLE,
        asset_name="fact_sales",
        can_auto_heal=True,
        metadata={},  # No pipeline_id
    )
    plan = healer.build_plan([anomaly])
    result = await healer.execute_healing_plan(plan, dry_run=False)

    # Returns False → marked as FAILED with "returned False" reason
    assert result.failed == 1
    assert result.applied == 0
    mock_fabric_client.post.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.unit
async def test_heal_shortcut_no_source_path(mock_fabric_client):
    """Missing source_path → returns False (no API call)."""
    healer = SelfHealer(client=mock_fabric_client)
    anomaly = make_anomaly(
        anomaly_type=AnomalyType.BROKEN_SHORTCUT,
        asset_name="dim_date",
        can_auto_heal=True,
        metadata={"lakehouse_id": "lh-001"},  # No source_path
    )
    plan = healer.build_plan([anomaly])
    result = await healer.execute_healing_plan(plan, dry_run=False)

    assert result.failed == 1
    mock_fabric_client.post.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.unit
async def test_heal_shortcut_no_lakehouse_id(mock_fabric_client):
    """Missing lakehouse_id → returns False (no API call)."""
    healer = SelfHealer(client=mock_fabric_client)
    anomaly = make_anomaly(
        anomaly_type=AnomalyType.BROKEN_SHORTCUT,
        asset_name="dim_date",
        can_auto_heal=True,
        metadata={"source_path": "Tables/dim_date"},  # No lakehouse_id
    )
    plan = healer.build_plan([anomaly])
    result = await healer.execute_healing_plan(plan, dry_run=False)

    assert result.failed == 1
    mock_fabric_client.post.assert_not_called()


# =============================================================================
# Tests — Dry-run mode
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.unit
async def test_dry_run_skips_all_api_calls(mock_fabric_client):
    """dry_run=True → client.post never called."""
    healer = SelfHealer(client=mock_fabric_client)
    anomalies = [
        _broken_shortcut_anomaly("dim_date"),
        _stale_table_anomaly("fact_sales"),
    ]
    plan = healer.build_plan(anomalies)
    result = await healer.execute_healing_plan(plan, dry_run=True)

    mock_fabric_client.post.assert_not_called()
    mock_fabric_client.get.assert_not_called()
    assert result.dry_run is True


@pytest.mark.asyncio
@pytest.mark.unit
async def test_dry_run_counts_all_as_skipped(mock_fabric_client):
    """dry_run=True → result.skipped == total actions, applied == 0."""
    healer = SelfHealer(client=mock_fabric_client)
    anomalies = [
        _broken_shortcut_anomaly("dim_date"),
        _broken_shortcut_anomaly("dim_product"),
        _stale_table_anomaly("fact_sales"),
    ]
    plan = healer.build_plan(anomalies)
    result = await healer.execute_healing_plan(plan, dry_run=True)

    assert result.skipped == len(result.actions)
    assert result.applied == 0
    assert result.failed == 0
    # All actions should have reason "dry_run"
    for action in result.actions:
        assert action.status == HealActionStatus.SKIPPED
        assert action.error == "dry_run"


# =============================================================================
# Tests — Manual approval gate
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.unit
async def test_manual_actions_skipped_without_approval(mock_fabric_client):
    """requires_approval=True → SKIPPED with reason 'requires_approval'."""
    healer = SelfHealer(client=mock_fabric_client)

    # ORPHAN_ASSET always requires approval
    anomaly = make_anomaly(
        anomaly_type=AnomalyType.ORPHAN_ASSET,
        asset_name="unused_table",
        can_auto_heal=False,
    )
    plan = healer.build_plan([anomaly])
    result = await healer.execute_healing_plan(plan, dry_run=False)

    assert result.skipped == 1
    assert result.applied == 0
    skipped = [a for a in result.actions if a.status == HealActionStatus.SKIPPED]
    assert len(skipped) == 1
    assert skipped[0].error in ("requires_approval", "policy_blocked")


# =============================================================================
# Tests — Partial batch failure
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.unit
async def test_partial_batch_failure_continues(mock_fabric_client):
    """Action 1 succeeds, Action 2 fails, Action 3 succeeds → applied=2, failed=1."""
    call_count = {"n": 0}

    async def side_effect(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise make_fabric_error(503)
        return {}

    mock_fabric_client.post.side_effect = side_effect

    healer = SelfHealer(client=mock_fabric_client)
    anomalies = [
        _broken_shortcut_anomaly("s1"),
        _broken_shortcut_anomaly("s2"),
        _broken_shortcut_anomaly("s3"),
    ]
    plan = healer.build_plan(anomalies)
    result = await healer.execute_healing_plan(plan, dry_run=False)

    assert result.applied == 2
    assert result.failed == 1
    assert not result.success


# =============================================================================
# Tests — Audit trail recording
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.unit
async def test_healing_result_recorded_to_audit_trail(mock_fabric_client, mock_memory_manager):
    """memory.record_state called with correct plan_id after execution."""
    healer = SelfHealer(client=mock_fabric_client, memory=mock_memory_manager)
    anomalies = [_broken_shortcut_anomaly("dim_date")]
    plan = healer.build_plan(anomalies)
    result = await healer.execute_healing_plan(plan, dry_run=False)

    mock_memory_manager.record_state.assert_called_once()
    call_kwargs = mock_memory_manager.record_state.call_args
    state_data = call_kwargs.kwargs.get("state_data") or call_kwargs[1].get("state_data")
    assert state_data["plan_id"] == plan.plan_id


# =============================================================================
# Tests — Standalone methods (Phase I blast-radius executors)
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.unit
async def test_standalone_refresh_model_raises_on_503(mock_fabric_client):
    """After Phase 3 fix: refresh_semantic_model() raises, not returns False."""
    mock_fabric_client.post.side_effect = make_fabric_error(503)

    healer = SelfHealer(client=mock_fabric_client)
    with pytest.raises(Exception) as exc_info:
        await healer.refresh_semantic_model("model-001", "ws-001")

    assert "unavailable" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.unit
async def test_standalone_trigger_pipeline_raises_on_503(mock_fabric_client):
    """trigger_pipeline() raises on 503 — consistent with refresh_semantic_model."""
    mock_fabric_client.post.side_effect = make_fabric_error(503)

    healer = SelfHealer(client=mock_fabric_client)
    with pytest.raises(Exception) as exc_info:
        await healer.trigger_pipeline("pl-001", "ws-001")

    assert "unavailable" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.unit
async def test_standalone_refresh_model_success(mock_fabric_client):
    """refresh_semantic_model() succeeds → returns True."""
    mock_fabric_client.post.return_value = {}

    healer = SelfHealer(client=mock_fabric_client)
    result = await healer.refresh_semantic_model("model-001", "ws-001")

    assert result is True
    mock_fabric_client.post.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.unit
async def test_standalone_trigger_pipeline_no_client():
    """trigger_pipeline() with no client → returns False (no crash)."""
    healer = SelfHealer(client=None)
    result = await healer.trigger_pipeline("pl-001", "ws-001")
    assert result is False


@pytest.mark.asyncio
@pytest.mark.unit
async def test_standalone_refresh_model_no_client():
    """refresh_semantic_model() with no client → returns False."""
    healer = SelfHealer(client=None)
    result = await healer.refresh_semantic_model("model-001", "ws-001")
    assert result is False
