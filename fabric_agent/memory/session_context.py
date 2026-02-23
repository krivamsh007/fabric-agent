"""
Session Context — Cross-Session Agent Memory
=============================================

WHAT: Persists agent context across MCP sessions so the agent "remembers"
      what happened in previous conversations.

WHY: Each MCP session with Claude or GPT starts completely fresh. The agent
     has no memory of what it did in the last session — which means it asks
     the same questions, makes the same mistakes, and loses context.

     SessionContext solves this by:
     1. Saving a structured summary at the end of each session
     2. Loading it at the start of the next session
     3. Injecting it as context so the agent can say "Last session I renamed
        3 measures and one required rollback — let me check that model first"

FAANG PARALLEL:
     - ChatGPT memory feature: persists user preferences and past topics
     - GitHub Copilot Workspace: remembers what you were working on
     - Google Assistant: user history and preferences
     - Slack AI: "continuing from where we left off"

HOW IT WORKS:
     1. At session end: serialize the last N snapshots into a JSON summary
        file in data/sessions/{workspace_id}_{date}.json
     2. At session start: load the latest session file for the workspace
     3. Return a human-readable summary that is injected into the agent's
        system prompt as "Previous session context:"

USAGE:
     from fabric_agent.memory.session_context import SessionContext

     ctx = SessionContext(memory_manager)

     # At end of session — save what happened
     session_id = await ctx.save_session(workspace_id="ENT_SalesAnalytics_DEV")

     # At start of next session — load previous context
     summary = await ctx.get_last_session_summary("ENT_SalesAnalytics_DEV")
     print(summary)
     # "Last session (2025-01-15): Renamed 3 measures (2 succeeded, 1 rolled back).
     #  Watch out for 'Revenue YTD' — it has 12 downstream dependencies."
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from uuid import uuid4

from loguru import logger

from fabric_agent.core.fabric_env import get_env

if TYPE_CHECKING:
    from fabric_agent.storage.memory_manager import MemoryManager, StateSnapshot


# =============================================================================
# Data Models
# =============================================================================

@dataclass
class SessionSummary:
    """
    Structured summary of a completed agent session.

    Stored as JSON in data/sessions/ (local) or Lakehouse Files/sessions/ (Fabric).
    """
    session_id: str
    workspace_id: Optional[str]
    workspace_name: Optional[str]
    started_at: str
    ended_at: str
    total_operations: int
    successful_operations: int
    failed_operations: int
    rolled_back_operations: int
    operations_summary: List[Dict[str, Any]] = field(default_factory=list)
    key_findings: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    next_session_context: str = ""  # injected into next session's prompt

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "workspace_id": self.workspace_id,
            "workspace_name": self.workspace_name,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "total_operations": self.total_operations,
            "successful_operations": self.successful_operations,
            "failed_operations": self.failed_operations,
            "rolled_back_operations": self.rolled_back_operations,
            "operations_summary": self.operations_summary,
            "key_findings": self.key_findings,
            "warnings": self.warnings,
            "next_session_context": self.next_session_context,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SessionSummary":
        return cls(**data)

    def to_human_readable(self) -> str:
        """
        Generate a human-readable summary suitable for injection into agent context.

        This is what the next session's agent will read as "previous context."
        """
        lines = [
            f"=== Previous Session Summary ===",
            f"Session: {self.session_id}",
            f"Date: {self.ended_at[:10]}",
        ]
        if self.workspace_name:
            lines.append(f"Workspace: {self.workspace_name}")

        lines.append(
            f"Operations: {self.total_operations} total "
            f"({self.successful_operations} succeeded, "
            f"{self.failed_operations} failed, "
            f"{self.rolled_back_operations} rolled back)"
        )

        if self.operations_summary:
            lines.append("\nWhat happened:")
            for op in self.operations_summary[:5]:
                status_icon = "✓" if op.get("status") == "success" else "✗"
                lines.append(
                    f"  {status_icon} {op.get('operation', '?')} on "
                    f"'{op.get('target_name', '?')}'"
                )

        if self.key_findings:
            lines.append("\nKey findings:")
            for finding in self.key_findings[:3]:
                lines.append(f"  • {finding}")

        if self.warnings:
            lines.append("\nWarnings for next session:")
            for warning in self.warnings[:3]:
                lines.append(f"  ⚠ {warning}")

        if self.next_session_context:
            lines.append(f"\nContext: {self.next_session_context}")

        return "\n".join(lines)


# =============================================================================
# SessionContext
# =============================================================================

class SessionContext:
    """
    Cross-session state management for the Fabric Agent.

    Stores and retrieves session summaries so agents can pick up
    where they left off after a session ends.
    """

    def __init__(self, memory_manager: "MemoryManager"):
        self._manager = memory_manager
        self._sessions_dir = Path(get_env().session_store_path())

    # -------------------------------------------------------------------------
    # Save
    # -------------------------------------------------------------------------

    async def save_session(
        self,
        workspace_id: Optional[str] = None,
        workspace_name: Optional[str] = None,
        lookback_limit: int = 50,
    ) -> str:
        """
        Save the current session state to disk.

        Reads the last N operations from SQLite, synthesizes a summary,
        and writes it as JSON to the sessions directory.

        Args:
            workspace_id:   Filter to a specific workspace (optional).
            workspace_name: Human name for the workspace (for the summary).
            lookback_limit: How many recent operations to include.

        Returns:
            session_id: The ID of the saved session.
        """
        snapshots = await self._manager.get_history(
            workspace_id=workspace_id,
            limit=lookback_limit,
        )

        summary = self._build_summary(snapshots, workspace_id, workspace_name)
        session_file = self._session_file_path(summary.session_id)

        session_file.parent.mkdir(parents=True, exist_ok=True)
        with session_file.open("w", encoding="utf-8") as f:
            json.dump(summary.to_dict(), f, indent=2)

        logger.info(
            f"Session saved: {summary.session_id} "
            f"({summary.total_operations} operations, "
            f"file: {session_file.name})"
        )
        return summary.session_id

    # -------------------------------------------------------------------------
    # Load
    # -------------------------------------------------------------------------

    async def get_last_session_summary(
        self,
        workspace_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Get the human-readable summary of the last session.

        Returns the summary string suitable for injection into the next
        session's agent context as "Previous session context:".

        Args:
            workspace_id: Filter to a specific workspace (optional).

        Returns:
            Human-readable summary string, or None if no previous session found.
        """
        summary = await self._load_latest_session(workspace_id)
        if summary is None:
            return None
        return summary.to_human_readable()

    async def load_session(self, session_id: str) -> Optional[SessionSummary]:
        """
        Load a specific session by ID.

        Args:
            session_id: Session ID returned by save_session().

        Returns:
            SessionSummary or None if not found.
        """
        session_file = self._session_file_path(session_id)
        if not session_file.exists():
            return None
        with session_file.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return SessionSummary.from_dict(data)

    async def list_sessions(
        self,
        workspace_id: Optional[str] = None,
        limit: int = 10,
    ) -> List[SessionSummary]:
        """
        List recent sessions, newest first.

        Args:
            workspace_id: Filter to specific workspace.
            limit:        Maximum number to return.

        Returns:
            List of SessionSummary objects.
        """
        if not self._sessions_dir.exists():
            return []

        session_files = sorted(
            self._sessions_dir.glob("session_*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )

        results = []
        for path in session_files[:limit * 3]:  # load extra for filtering
            try:
                with path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                summary = SessionSummary.from_dict(data)
                if workspace_id is None or summary.workspace_id == workspace_id:
                    results.append(summary)
                if len(results) >= limit:
                    break
            except Exception as e:
                logger.warning(f"Failed to load session {path}: {e}")

        return results

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _build_summary(
        self,
        snapshots: List["StateSnapshot"],
        workspace_id: Optional[str],
        workspace_name: Optional[str],
    ) -> SessionSummary:
        """Build a SessionSummary from a list of snapshots."""
        session_id = f"session_{uuid4().hex[:12]}"
        now = datetime.now(timezone.utc).isoformat()

        if not snapshots:
            return SessionSummary(
                session_id=session_id,
                workspace_id=workspace_id,
                workspace_name=workspace_name,
                started_at=now,
                ended_at=now,
                total_operations=0,
                successful_operations=0,
                failed_operations=0,
                rolled_back_operations=0,
                next_session_context="No operations were performed in this session.",
            )

        # Count outcomes
        def get_status(s: "StateSnapshot") -> str:
            return s.status if isinstance(s.status, str) else s.status.value

        successful = sum(1 for s in snapshots if get_status(s).lower() == "success")
        failed = sum(1 for s in snapshots if get_status(s).lower() == "failed")
        rolled_back = sum(1 for s in snapshots if get_status(s).lower() == "rolled_back")

        # Build operations list
        ops_summary = [
            {
                "operation": s.operation,
                "target_name": s.target_name,
                "status": get_status(s),
                "timestamp": s.timestamp if isinstance(s.timestamp, str) else str(s.timestamp),
            }
            for s in snapshots[:20]
        ]

        # Key findings
        findings = []
        if rolled_back > 0:
            rb_names = [s.target_name for s in snapshots if get_status(s).lower() == "rolled_back"]
            findings.append(
                f"{rolled_back} rollback(s) required: {', '.join(filter(None, rb_names[:3]))}"
            )
        if failed > 0:
            findings.append(
                f"{failed} operation(s) failed — check dependency count before retrying"
            )
        if successful > 0:
            findings.append(f"{successful} operation(s) completed successfully")

        # Warnings for next session
        warnings = []
        if rolled_back > 0:
            warnings.append(
                "Some operations were rolled back — validate with dry_run before next attempt"
            )

        # Context string
        context = (
            f"Last session: {len(snapshots)} operations — "
            f"{successful} succeeded, {failed} failed, {rolled_back} rolled back. "
        )
        if snapshots:
            last_op = snapshots[0]
            context += f"Last action: {last_op.operation} on '{last_op.target_name}'."

        ws_name = workspace_name or (snapshots[0].workspace_name if snapshots else None)
        start_ts = snapshots[-1].timestamp if isinstance(snapshots[-1].timestamp, str) else str(snapshots[-1].timestamp)

        return SessionSummary(
            session_id=session_id,
            workspace_id=workspace_id,
            workspace_name=ws_name,
            started_at=start_ts,
            ended_at=now,
            total_operations=len(snapshots),
            successful_operations=successful,
            failed_operations=failed,
            rolled_back_operations=rolled_back,
            operations_summary=ops_summary,
            key_findings=findings,
            warnings=warnings,
            next_session_context=context,
        )

    async def _load_latest_session(
        self,
        workspace_id: Optional[str],
    ) -> Optional[SessionSummary]:
        """Load the most recent session for a workspace."""
        sessions = await self.list_sessions(workspace_id=workspace_id, limit=1)
        return sessions[0] if sessions else None

    def _session_file_path(self, session_id: str) -> Path:
        """Get the file path for a session file."""
        return self._sessions_dir / f"{session_id}.json"
