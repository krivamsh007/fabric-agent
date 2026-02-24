import base64
from types import SimpleNamespace

import pytest

from fabric_agent.refactor import orchestrator as orch_mod


class _MemoryStub:
    def __init__(self) -> None:
        self.calls = []

    async def record_state(self, **kwargs):
        snap_id = f"snap-{len(self.calls) + 1}"
        self.calls.append((snap_id, kwargs))
        return SimpleNamespace(id=snap_id)


def _fallback_definition() -> dict:
    payload = base64.b64encode(b"measure 'Old Measure' = 1\n").decode("ascii")
    return {
        "definition": {
            "parts": [
                {
                    "path": "model.tmdl",
                    "payloadType": "InlineBase64",
                    "payload": payload,
                }
            ]
        }
    }


class _FakeMeasure:
    def __init__(self, name: str, expression: str = "") -> None:
        self.Name = name
        self.Expression = expression


class _FakeTable:
    def __init__(self, name: str, measures):
        self.Name = name
        self.Measures = measures


class _FakeTom:
    def __init__(self) -> None:
        self.model = SimpleNamespace(
            Tables=[
                _FakeTable(
                    "Facts",
                    [
                        _FakeMeasure("Old Measure", "1"),
                        _FakeMeasure("Child Measure", "[Old Measure] + 1"),
                    ],
                )
            ]
        )


class _FakeCtx:
    def __enter__(self):
        self._tom = _FakeTom()
        return self._tom

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class _FakeFabric:
    def connect_semantic_model(self, model_name, readonly, workspace):
        return _FakeCtx()


@pytest.mark.asyncio
async def test_apply_smart_rename_dry_run_uses_snapshot_id_chain(tmp_path):
    memory = _MemoryStub()
    orchestrator = orch_mod.RefactorOrchestrator(
        memory=memory,
        json_log=orch_mod.RefactorJsonLog(tmp_path / "refactor_log.json"),
    )

    _, snapshot_ids = await orchestrator.apply_smart_rename(
        workspace_id="ws-1",
        workspace_name="WS",
        model_name="Model",
        old_name="Old Measure",
        new_name="New Measure",
        reasoning="test",
        dry_run=True,
        definition_fallback=_fallback_definition(),
        user="tester",
    )

    assert snapshot_ids == ["snap-1"]
    assert memory.calls[0][1]["parent_id"] is None


@pytest.mark.asyncio
async def test_apply_smart_rename_live_path_uses_snapshot_id_chain(monkeypatch, tmp_path):
    memory = _MemoryStub()
    orchestrator = orch_mod.RefactorOrchestrator(
        memory=memory,
        json_log=orch_mod.RefactorJsonLog(tmp_path / "refactor_log.json"),
    )
    monkeypatch.setattr(orch_mod, "_try_import_sempy", lambda: _FakeFabric())

    _, snapshot_ids = await orchestrator.apply_smart_rename(
        workspace_id="ws-1",
        workspace_name="WS",
        model_name="Model",
        old_name="Old Measure",
        new_name="New Measure",
        reasoning="test",
        dry_run=False,
        user="tester",
    )

    assert snapshot_ids == ["snap-1", "snap-2", "snap-3"]
    assert [call[1]["parent_id"] for call in memory.calls] == [None, "snap-1", "snap-2"]
