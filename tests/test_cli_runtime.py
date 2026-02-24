import pytest
import typer

import fabric_agent.cli as cli


class _DummyAgent:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def test_run_async_propagates_runtime_errors():
    async def _boom():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        cli._run_async(_boom)


def test_run_async_maps_nonzero_system_exit_to_typer_exit():
    async def _exit():
        raise SystemExit(3)

    with pytest.raises(typer.Exit) as exc:
        cli._run_async(_exit)

    assert exc.value.exit_code == 3


def test_run_async_closes_global_agent(monkeypatch):
    dummy = _DummyAgent()
    monkeypatch.setattr(cli, "_agent", dummy)

    async def _ok():
        return 42

    assert cli._run_async(_ok) == 42
    assert dummy.closed is True
    assert cli._agent is None
