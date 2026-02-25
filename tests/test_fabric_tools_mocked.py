"""
Mocked FabricTools Integration Tests
=====================================

Tests the FabricTools MCP tool layer with mocked internal dependencies
(AnomalyDetector, SelfHealer, ShortcutCascadeManager, SelfHealingMonitor).

These tests verify the data transformation between internal models and
the Pydantic output models, error propagation, and edge cases like
empty workspaces and missing assets.

All dependencies are patched — no real Fabric API calls.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from conftest import make_anomaly, make_fabric_error

from fabric_agent.healing.models import (
    Anomaly,
    AnomalyType,
    HealAction,
    HealActionStatus,
    HealingPlan,
    HealingResult,
    HealthReport,
    RiskLevel,
    ShortcutDefinition,
    CascadeImpact,
    FixSuggestion,
    ShortcutHealingPlan,
    EnterpriseBlastRadius,
    WorkspaceBlastRadius,
    ImpactedAssetDetail,
    ActionType,
    Urgency,
)
from fabric_agent.tools.fabric_tools import FabricTools


# =============================================================================
# Helpers
# =============================================================================

def _make_tools(mock_client, mock_memory=None):
    """Create FabricTools with mocked client + memory."""
    mm = mock_memory or AsyncMock()
    mm.initialize = AsyncMock()
    mm.record_state = AsyncMock()
    return FabricTools(client=mock_client, memory_manager=mm)


# =============================================================================
# Tests — scan_workspace_health
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.unit
async def test_scan_workspace_health_returns_anomalies(mock_fabric_client):
    """Detector finds 2 anomalies → output.total_found == 2."""
    from fabric_agent.tools.models import ScanWorkspaceHealthInput

    anomalies = [
        make_anomaly(AnomalyType.STALE_TABLE, "fact_sales"),
        make_anomaly(AnomalyType.BROKEN_SHORTCUT, "dim_date"),
    ]

    tools = _make_tools(mock_fabric_client)

    with patch(
        "fabric_agent.healing.detector.AnomalyDetector", autospec=True
    ) as MockDet:
        instance = MockDet.return_value
        instance.scan = AsyncMock(return_value=anomalies)

        result = await tools.scan_workspace_health(
            ScanWorkspaceHealthInput(workspace_ids=["ws-001"])
        )

    assert result.total_found == 2
    assert len(result.anomalies) == 2


@pytest.mark.asyncio
@pytest.mark.unit
async def test_scan_workspace_health_empty_workspace(mock_fabric_client):
    """No items → 0 anomalies."""
    from fabric_agent.tools.models import ScanWorkspaceHealthInput

    tools = _make_tools(mock_fabric_client)

    with patch(
        "fabric_agent.healing.detector.AnomalyDetector", autospec=True
    ) as MockDet:
        instance = MockDet.return_value
        instance.scan = AsyncMock(return_value=[])

        result = await tools.scan_workspace_health(
            ScanWorkspaceHealthInput(workspace_ids=["ws-empty"])
        )

    assert result.total_found == 0
    assert result.anomalies == []
    assert result.critical_count == 0


# =============================================================================
# Tests — build_healing_plan
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.unit
async def test_build_healing_plan_categorizes_actions(mock_fabric_client):
    """Auto vs manual split matches anomaly.can_auto_heal."""
    from fabric_agent.tools.models import BuildHealingPlanInput

    auto_anomaly = make_anomaly(
        AnomalyType.BROKEN_SHORTCUT, "dim_date", can_auto_heal=True,
        metadata={"source_path": "Tables/dim_date", "lakehouse_id": "lh-001"},
    )
    manual_anomaly = make_anomaly(
        AnomalyType.ORPHAN_ASSET, "unused_table", can_auto_heal=False,
    )

    tools = _make_tools(mock_fabric_client)

    with patch(
        "fabric_agent.healing.detector.AnomalyDetector", autospec=True
    ) as MockDet:
        instance = MockDet.return_value
        instance.scan = AsyncMock(return_value=[auto_anomaly, manual_anomaly])

        result = await tools.build_healing_plan(
            BuildHealingPlanInput(workspace_ids=["ws-001"])
        )

    assert result.auto_action_count >= 1
    assert result.manual_action_count >= 1


# =============================================================================
# Tests — execute_healing_plan (dry_run)
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.unit
async def test_execute_healing_plan_dry_run(mock_fabric_client):
    """dry_run=True → all skipped, no client.post calls."""
    from fabric_agent.tools.models import ExecuteHealingPlanInput

    anomaly = make_anomaly(
        AnomalyType.BROKEN_SHORTCUT, "dim_date", can_auto_heal=True,
        metadata={"source_path": "Tables/dim_date", "lakehouse_id": "lh-001"},
    )
    plan = HealingPlan(anomalies=[anomaly])
    plan.auto_actions = [HealAction(anomaly_id=anomaly.anomaly_id, action_type="recreate_shortcut")]

    dry_result = HealingResult(plan_id=plan.plan_id, dry_run=True, skipped=1)
    report = HealthReport(
        total_assets=1,
        anomalies_found=1,
        healing_plan=plan,
        healing_result=dry_result,
    )

    tools = _make_tools(mock_fabric_client)

    with patch(
        "fabric_agent.healing.monitor.SelfHealingMonitor", autospec=True
    ) as MockMon:
        inst = MockMon.return_value
        inst.run_once = AsyncMock(return_value=report)

        result = await tools.execute_healing_plan(
            ExecuteHealingPlanInput(workspace_ids=["ws-001"], dry_run=True)
        )

    assert result.dry_run is True
    assert result.skipped == 1
    assert result.applied == 0
    mock_fabric_client.post.assert_not_called()


# =============================================================================
# Tests — scan_shortcut_cascade
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.unit
async def test_scan_shortcut_cascade_healthy_shortcuts(mock_fabric_client):
    """All shortcuts healthy → broken_shortcut_count == 0."""
    from fabric_agent.tools.models import ScanShortcutCascadeInput

    empty_plan = ShortcutHealingPlan(
        broken_shortcuts=[],
        cascade_impacts=[],
    )

    tools = _make_tools(mock_fabric_client)

    with patch(
        "fabric_agent.healing.shortcut_manager.ShortcutCascadeManager",
        autospec=True,
    ) as MockMgr:
        inst = MockMgr.return_value
        inst.build_cascade_plan = AsyncMock(return_value=empty_plan)

        result = await tools.scan_shortcut_cascade(
            ScanShortcutCascadeInput(workspace_ids=["ws-001"])
        )

    assert result.broken_shortcut_count == 0
    assert result.total_cascade_impact == 0
    assert "No broken shortcuts" in result.message


@pytest.mark.asyncio
@pytest.mark.unit
async def test_scan_shortcut_cascade_broken_shortcut(mock_fabric_client):
    """One broken shortcut → broken_shortcut_count == 1."""
    from fabric_agent.tools.models import ScanShortcutCascadeInput

    broken = ShortcutDefinition(
        name="dim_date",
        workspace_id="ws-001",
        lakehouse_id="lh-001",
        source_workspace_id="ws-src",
        source_lakehouse_id="lh-src",
        source_path="Tables/dim_date",
        is_healthy=False,
    )
    impact = CascadeImpact(
        shortcut=broken,
        impacted_tables=["dim_date"],
        impacted_semantic_models=["Sales_Model"],
    )
    plan = ShortcutHealingPlan(
        broken_shortcuts=[broken],
        cascade_impacts=[impact],
    )

    tools = _make_tools(mock_fabric_client)

    with patch(
        "fabric_agent.healing.shortcut_manager.ShortcutCascadeManager",
        autospec=True,
    ) as MockMgr:
        inst = MockMgr.return_value
        inst.build_cascade_plan = AsyncMock(return_value=plan)

        result = await tools.scan_shortcut_cascade(
            ScanShortcutCascadeInput(workspace_ids=["ws-001"])
        )

    assert result.broken_shortcut_count == 1
    assert result.total_cascade_impact == 2  # 1 table + 1 model


# =============================================================================
# Tests — get_enterprise_blast_radius
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.unit
async def test_get_enterprise_blast_radius_cross_workspace(mock_fabric_client):
    """Mock 2 workspaces → blast radius spans both."""
    from fabric_agent.tools.models import EnterpriseBlastRadiusInput

    asset1 = ImpactedAssetDetail(
        asset_id="t1", asset_name="fact_sales", asset_type="table",
        workspace_id="ws-001", workspace_name="DataPlatform",
        impact_depth=1, impact_path=["fact_sales"],
        action_type=ActionType.VERIFY_SHORTCUT,
        action_description="Verify shortcut",
        auto_applicable=True, urgency=Urgency.IMMEDIATE,
    )
    asset2 = ImpactedAssetDetail(
        asset_id="m1", asset_name="Sales_Model", asset_type="semantic_model",
        workspace_id="ws-002", workspace_name="Analytics",
        impact_depth=2, impact_path=["fact_sales", "Sales_Model"],
        action_type=ActionType.REFRESH_MODEL,
        action_description="Refresh model",
        auto_applicable=True, urgency=Urgency.AFTER_SOURCE_FIXED,
    )

    blast = EnterpriseBlastRadius(
        source_asset_name="fact_sales",
        source_workspace_name="DataPlatform",
        change_description="column dropped: customer_tier",
        total_workspaces_impacted=2,
        total_assets_impacted=2,
        per_workspace=[
            WorkspaceBlastRadius(
                workspace_id="ws-001", workspace_name="DataPlatform",
                role="source", impacted_assets=[asset1],
            ),
            WorkspaceBlastRadius(
                workspace_id="ws-002", workspace_name="Analytics",
                role="consumer", impacted_assets=[asset2],
            ),
        ],
        ordered_healing_steps=[asset1, asset2],
        risk_level="high",
    )

    tools = _make_tools(mock_fabric_client)

    with patch(
        "fabric_agent.healing.shortcut_manager.ShortcutCascadeManager",
        autospec=True,
    ) as MockMgr:
        inst = MockMgr.return_value
        inst.analyze_enterprise_blast_radius = AsyncMock(return_value=blast)

        result = await tools.get_enterprise_blast_radius(
            EnterpriseBlastRadiusInput(
                source_asset_name="fact_sales",
                workspace_ids=["ws-001", "ws-002"],
                change_description="column dropped: customer_tier",
            )
        )

    assert result.total_workspaces_impacted == 2
    assert result.total_assets_impacted == 2
    assert result.risk_level == "high"
    assert len(result.per_workspace) == 2
    assert len(result.ordered_healing_steps) == 2


@pytest.mark.asyncio
@pytest.mark.unit
async def test_get_enterprise_blast_radius_source_not_found(mock_fabric_client):
    """Source asset not in graph → risk=low, 0 assets."""
    from fabric_agent.tools.models import EnterpriseBlastRadiusInput

    blast = EnterpriseBlastRadius(
        source_asset_name="nonexistent_table",
        source_workspace_name="Unknown",
        change_description="N/A",
        total_workspaces_impacted=0,
        total_assets_impacted=0,
        per_workspace=[],
        ordered_healing_steps=[],
        risk_level="low",
    )

    tools = _make_tools(mock_fabric_client)

    with patch(
        "fabric_agent.healing.shortcut_manager.ShortcutCascadeManager",
        autospec=True,
    ) as MockMgr:
        inst = MockMgr.return_value
        inst.analyze_enterprise_blast_radius = AsyncMock(return_value=blast)

        result = await tools.get_enterprise_blast_radius(
            EnterpriseBlastRadiusInput(
                source_asset_name="nonexistent_table",
                workspace_ids=["ws-001"],
                change_description="N/A",
            )
        )

    assert result.total_assets_impacted == 0
    assert result.risk_level == "low"


# =============================================================================
# Tests — set_workspace edge case
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.unit
async def test_set_workspace_invalid_name(mock_fabric_client):
    """Workspace name not found → error in output."""
    from fabric_agent.tools.models import SetWorkspaceInput

    mock_fabric_client.get.return_value = {
        "value": [{"id": "ws-001", "displayName": "Sales"}]
    }

    tools = _make_tools(mock_fabric_client)

    with pytest.raises(ValueError, match="not found"):
        await tools.set_workspace(
            SetWorkspaceInput(workspace_name="NonexistentWorkspace")
        )


# =============================================================================
# Tests — list_workspaces error handling
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.unit
async def test_list_workspaces_503_returns_empty(mock_fabric_client):
    """503 on list_workspaces → returns empty list (graceful degradation)."""
    from fabric_agent.tools.models import ListWorkspacesInput

    mock_fabric_client.get.side_effect = make_fabric_error(503)

    tools = _make_tools(mock_fabric_client)
    result = await tools.list_workspaces(ListWorkspacesInput())

    assert result.count == 0
    assert result.workspaces == []
