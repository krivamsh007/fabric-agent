"""
Rollback Safety Tests
=====================

Verifies that SafeRenameEngine.rollback() survives partial API failures
and always attempts the semantic model rollback even when reports fail.

Key safety properties tested:
  - A 503 on one report does NOT block rollback of remaining reports or SM
  - The semantic model rollback runs in a finally-like block
  - dry_run=True previews without calling any API
  - Per-item success/failure is reported in RollbackResult
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from conftest import make_fabric_error


# =============================================================================
# Helpers
# =============================================================================

def _write_op_record(ops_dir: Path, op_id: str, *, report_ids: list[str] | None = None) -> Path:
    """Write a minimal operation record that rollback() can load."""
    report_ids = report_ids or []
    reports_before = {rid: {"format": "PBIR", "parts": []} for rid in report_ids}
    reports_after = {rid: {"format": "PBIR", "parts": []} for rid in report_ids}

    record = {
        "operation_id": op_id,
        "plan": {
            "workspace_id": "ws-001",
            "semantic_model_id": "sm-001",
        },
        "before": {
            "semantic_model": {"format": "TMDL", "parts": []},
            "reports": reports_before,
        },
        "after": {
            "semantic_model": {"format": "TMDL", "parts": []},
            "reports": reports_after,
        },
        "results": {},
    }
    op_path = ops_dir / f"{op_id}.json"
    op_path.write_text(json.dumps(record), encoding="utf-8")
    return op_path


# =============================================================================
# Tests
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.unit
async def test_rollback_happy_path(tmp_path, mock_fabric_client):
    """All reports + SM rolled back successfully → result.success=True."""
    from fabric_agent.refactor.safe_rename_engine import SafeRenameEngine, OPS_DIR

    ops_dir = tmp_path / "operations"
    ops_dir.mkdir()
    _write_op_record(ops_dir, "op_happy", report_ids=["r1", "r2"])

    import fabric_agent.refactor.safe_rename_engine as mod
    original_ops = mod.OPS_DIR
    mod.OPS_DIR = ops_dir
    try:
        engine = SafeRenameEngine(client=mock_fabric_client)
        result = await engine.rollback("op_happy")

        assert result.success is True
        assert set(result.reports_restored) == {"r1", "r2"}
        assert result.semantic_model_restored is True
        assert result.reports_failed == {}
        assert result.semantic_model_error is None
        assert mock_fabric_client.update_report_definition.call_count == 2
        assert mock_fabric_client.update_semantic_model_definition.call_count == 1
    finally:
        mod.OPS_DIR = original_ops


@pytest.mark.asyncio
@pytest.mark.unit
async def test_rollback_report_503_continues_to_next(tmp_path, mock_fabric_client):
    """Report r2 gets 503 → r1 and r3 are still restored, SM is still restored."""
    from fabric_agent.refactor.safe_rename_engine import SafeRenameEngine

    ops_dir = tmp_path / "operations"
    ops_dir.mkdir()
    _write_op_record(ops_dir, "op_503", report_ids=["r1", "r2", "r3"])

    err_503 = make_fabric_error(503)
    call_count = {"n": 0}

    async def side_effect_report(ws, rid, defn):
        call_count["n"] += 1
        if rid == "r2":
            raise err_503
        return {}

    mock_fabric_client.update_report_definition.side_effect = side_effect_report

    import fabric_agent.refactor.safe_rename_engine as mod
    original = mod.OPS_DIR
    mod.OPS_DIR = ops_dir
    try:
        engine = SafeRenameEngine(client=mock_fabric_client)
        result = await engine.rollback("op_503")

        assert result.success is False
        assert set(result.reports_restored) == {"r1", "r3"}
        assert "r2" in result.reports_failed
        assert result.semantic_model_restored is True  # SM still restored
    finally:
        mod.OPS_DIR = original


@pytest.mark.asyncio
@pytest.mark.unit
async def test_rollback_sm_attempted_even_if_all_reports_fail(tmp_path, mock_fabric_client):
    """All 3 reports fail → SM rollback is still attempted and succeeds."""
    from fabric_agent.refactor.safe_rename_engine import SafeRenameEngine

    ops_dir = tmp_path / "operations"
    ops_dir.mkdir()
    _write_op_record(ops_dir, "op_all_fail", report_ids=["r1", "r2", "r3"])

    mock_fabric_client.update_report_definition.side_effect = make_fabric_error(503)

    import fabric_agent.refactor.safe_rename_engine as mod
    original = mod.OPS_DIR
    mod.OPS_DIR = ops_dir
    try:
        engine = SafeRenameEngine(client=mock_fabric_client)
        result = await engine.rollback("op_all_fail")

        assert result.success is False
        assert len(result.reports_failed) == 3
        assert result.semantic_model_restored is True
        assert mock_fabric_client.update_semantic_model_definition.call_count == 1
    finally:
        mod.OPS_DIR = original


@pytest.mark.asyncio
@pytest.mark.unit
async def test_rollback_sm_failure_reported(tmp_path, mock_fabric_client):
    """SM rollback also fails → semantic_model_error is populated."""
    from fabric_agent.refactor.safe_rename_engine import SafeRenameEngine

    ops_dir = tmp_path / "operations"
    ops_dir.mkdir()
    _write_op_record(ops_dir, "op_sm_fail", report_ids=["r1"])

    mock_fabric_client.update_semantic_model_definition.side_effect = make_fabric_error(503)

    import fabric_agent.refactor.safe_rename_engine as mod
    original = mod.OPS_DIR
    mod.OPS_DIR = ops_dir
    try:
        engine = SafeRenameEngine(client=mock_fabric_client)
        result = await engine.rollback("op_sm_fail")

        assert result.success is False
        assert result.semantic_model_restored is False
        assert result.semantic_model_error is not None
        assert "r1" in result.reports_restored  # report was OK
    finally:
        mod.OPS_DIR = original


@pytest.mark.asyncio
@pytest.mark.unit
async def test_rollback_operation_not_found(tmp_path, mock_fabric_client):
    """Missing operation_id → FileNotFoundError."""
    from fabric_agent.refactor.safe_rename_engine import SafeRenameEngine

    import fabric_agent.refactor.safe_rename_engine as mod
    original = mod.OPS_DIR
    mod.OPS_DIR = tmp_path
    try:
        engine = SafeRenameEngine(client=mock_fabric_client)
        with pytest.raises(FileNotFoundError, match="Operation not found"):
            await engine.rollback("nonexistent_op")
    finally:
        mod.OPS_DIR = original


@pytest.mark.asyncio
@pytest.mark.unit
async def test_rollback_dry_run_no_api_calls(tmp_path, mock_fabric_client):
    """dry_run=True → no API calls made, returns preview."""
    from fabric_agent.refactor.safe_rename_engine import SafeRenameEngine

    ops_dir = tmp_path / "operations"
    ops_dir.mkdir()
    _write_op_record(ops_dir, "op_dry", report_ids=["r1", "r2"])

    import fabric_agent.refactor.safe_rename_engine as mod
    original = mod.OPS_DIR
    mod.OPS_DIR = ops_dir
    try:
        engine = SafeRenameEngine(client=mock_fabric_client)
        result = await engine.rollback("op_dry", dry_run=True)

        assert result.dry_run is True
        assert result.success is True
        assert set(result.reports_restored) == {"r1", "r2"}
        assert result.semantic_model_restored is True
        mock_fabric_client.update_report_definition.assert_not_called()
        mock_fabric_client.update_semantic_model_definition.assert_not_called()
    finally:
        mod.OPS_DIR = original


@pytest.mark.asyncio
@pytest.mark.unit
async def test_rollback_empty_reports(tmp_path, mock_fabric_client):
    """Only SM was changed (no reports) → rollback only touches SM."""
    from fabric_agent.refactor.safe_rename_engine import SafeRenameEngine

    ops_dir = tmp_path / "operations"
    ops_dir.mkdir()
    _write_op_record(ops_dir, "op_no_reports", report_ids=[])

    import fabric_agent.refactor.safe_rename_engine as mod
    original = mod.OPS_DIR
    mod.OPS_DIR = ops_dir
    try:
        engine = SafeRenameEngine(client=mock_fabric_client)
        result = await engine.rollback("op_no_reports")

        assert result.success is True
        assert result.reports_restored == []
        assert result.semantic_model_restored is True
        mock_fabric_client.update_report_definition.assert_not_called()
        mock_fabric_client.update_semantic_model_definition.assert_called_once()
    finally:
        mod.OPS_DIR = original


@pytest.mark.asyncio
@pytest.mark.unit
async def test_rollback_409_conflict_continues(tmp_path, mock_fabric_client):
    """409 on an already-rolled-back report → treated as failure but continues."""
    from fabric_agent.refactor.safe_rename_engine import SafeRenameEngine

    ops_dir = tmp_path / "operations"
    ops_dir.mkdir()
    _write_op_record(ops_dir, "op_409", report_ids=["r1", "r2"])

    async def side_effect(ws, rid, defn):
        if rid == "r1":
            raise make_fabric_error(409)
        return {}

    mock_fabric_client.update_report_definition.side_effect = side_effect

    import fabric_agent.refactor.safe_rename_engine as mod
    original = mod.OPS_DIR
    mod.OPS_DIR = ops_dir
    try:
        engine = SafeRenameEngine(client=mock_fabric_client)
        result = await engine.rollback("op_409")

        assert "r1" in result.reports_failed
        assert "r2" in result.reports_restored
        assert result.semantic_model_restored is True
    finally:
        mod.OPS_DIR = original
