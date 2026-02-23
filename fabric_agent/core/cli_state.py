"""CLI state persistence.

The CLI is invoked as separate processes (e.g., `fabric-agent set-workspace ...`
followed by `fabric-agent list-items ...`). To provide a smooth user experience,
we persist the *active workspace* selection to disk.

This module stores **only non-sensitive metadata** (workspace id + workspace name)
under the user's home directory (or FABRIC_AGENT_STATE_DIR if set).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class CliState:
    workspace_id: Optional[str] = None
    workspace_name: Optional[str] = None


def _state_dir() -> Path:
    # Cross-platform, user-scoped directory.
    override = os.getenv("FABRIC_AGENT_STATE_DIR")
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / ".fabric_agent"


def state_path() -> Path:
    return _state_dir() / "state.json"


def load_state() -> CliState:
    p = state_path()
    if not p.exists():
        return CliState()
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return CliState(
            workspace_id=(data.get("workspace_id") or None),
            workspace_name=(data.get("workspace_name") or None),
        )
    except Exception:
        # If state is corrupted, fail open.
        return CliState()


def save_state(state: CliState) -> None:
    d = _state_dir()
    d.mkdir(parents=True, exist_ok=True)
    payload = {
        "workspace_id": state.workspace_id,
        "workspace_name": state.workspace_name,
    }
    state_path().write_text(json.dumps(payload, indent=2), encoding="utf-8")


def set_active_workspace(workspace_id: str, workspace_name: str) -> None:
    save_state(CliState(workspace_id=workspace_id, workspace_name=workspace_name))
