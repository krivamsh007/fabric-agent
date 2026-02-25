"""
Guard Monitor — Orchestrator for Freshness + Maintenance Guards
================================================================

Composes ``FreshnessGuard`` and ``MaintenanceGuard`` into a single
``run_once()`` / ``run_loop()`` entry point, with email alerting and
audit trail recording.

FAANG PARALLEL:
    Google SRE's "monitoring pipeline" pattern: a single supervisor process
    runs multiple independent health checks, aggregates their results, and
    dispatches alerts. Each check is fault-isolated — one failure never
    blocks or suppresses the others.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from html import escape
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from fabric_agent.guards.freshness_guard import FreshnessGuard
from fabric_agent.guards.maintenance_guard import MaintenanceGuard

if TYPE_CHECKING:
    from fabric_agent.api.fabric_client import FabricApiClient
    from fabric_agent.integrations.email import EmailNotifier
    from fabric_agent.storage.memory_manager import MemoryManager

logger = logging.getLogger(__name__)


class GuardMonitor:
    """
    Orchestrates freshness + maintenance guards.

    Args:
        client:                   FabricApiClient.
        memory:                   MemoryManager for audit trail.
        email_notifier:           Optional EmailNotifier for alerts.
        alert_recipients:         Email addresses.
        sla_thresholds:           Passed to FreshnessGuard.
        queue_pressure_threshold: Passed to MaintenanceGuard.
        maintenance_dry_run:      If True, MaintenanceGuard never submits jobs.
        lro_timeout_secs:         Passed to FreshnessGuard.
    """

    def __init__(
        self,
        client: Optional[FabricApiClient] = None,
        memory: Optional[MemoryManager] = None,
        email_notifier: Optional[EmailNotifier] = None,
        alert_recipients: Optional[List[str]] = None,
        sla_thresholds: Optional[Dict[str, float]] = None,
        queue_pressure_threshold: int = 3,
        maintenance_dry_run: bool = True,
        lro_timeout_secs: int = 300,
    ):
        self.client = client
        self.memory = memory
        self.email_notifier = email_notifier
        self.alert_recipients = alert_recipients or []

        self.freshness_guard = FreshnessGuard(
            client=client,
            sla_thresholds=sla_thresholds,
            email_notifier=email_notifier,
            alert_recipients=self.alert_recipients,
            memory=memory,
            lro_timeout_secs=lro_timeout_secs,
        )

        self.maintenance_guard = MaintenanceGuard(
            client=client,
            email_notifier=email_notifier,
            alert_recipients=self.alert_recipients,
            memory=memory,
            queue_pressure_threshold=queue_pressure_threshold,
            dry_run=maintenance_dry_run,
        )

    async def run_once(
        self,
        workspace_ids: List[str],
        run_freshness: bool = True,
        run_maintenance: bool = True,
        table_filter: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Run freshness + maintenance guards for the given workspaces.

        Each guard is independently try/excepted — one failure never blocks
        the other.

        Returns:
            Summary dict with freshness and maintenance results.
        """
        summary: Dict[str, Any] = {
            "freshness": None,
            "maintenance": None,
            "ran_at": datetime.now(timezone.utc).isoformat(),
            "errors": [],
        }

        # Freshness guard
        if run_freshness:
            try:
                freshness_result = await self.freshness_guard.scan(workspace_ids)
                summary["freshness"] = freshness_result.to_dict()
                await self._record_freshness_audit(freshness_result)
                if freshness_result.has_violations:
                    await self._send_freshness_alert(freshness_result)
            except Exception as exc:
                logger.error("Freshness guard failed: %s", exc)
                summary["errors"].append(f"freshness: {exc}")

        # Maintenance guard
        if run_maintenance:
            try:
                maintenance_result = await self.maintenance_guard.run_maintenance(
                    workspace_ids, table_filter=table_filter
                )
                summary["maintenance"] = maintenance_result.to_dict()
                await self._record_maintenance_audit(maintenance_result)
            except Exception as exc:
                logger.error("Maintenance guard failed: %s", exc)
                summary["errors"].append(f"maintenance: {exc}")

        return summary

    async def run_loop(
        self,
        workspace_ids: List[str],
        interval_minutes: int = 60,
        max_iterations: Optional[int] = None,
        run_freshness: bool = True,
        run_maintenance: bool = True,
    ) -> None:
        """Continuous polling loop."""
        iteration = 0
        while max_iterations is None or iteration < max_iterations:
            iteration += 1
            logger.info("Guard monitor iteration %d", iteration)
            try:
                await self.run_once(
                    workspace_ids,
                    run_freshness=run_freshness,
                    run_maintenance=run_maintenance,
                )
            except Exception as exc:
                logger.error("Guard monitor iteration %d failed: %s", iteration, exc)

            if max_iterations is not None and iteration >= max_iterations:
                break
            await asyncio.sleep(interval_minutes * 60)

    # ── Alerting ─────────────────────────────────────────────────────────

    async def _send_freshness_alert(self, result: Any) -> None:
        """Send email alert when freshness violations are detected."""
        if not self.email_notifier or not self.alert_recipients:
            return

        violations = result.violations
        worst = sorted(
            violations,
            key=lambda v: (v.table_status.hours_since_sync or 9999)
            if v.table_status
            else 9999,
            reverse=True,
        )

        rows = ""
        for v in worst[:20]:
            ts = v.table_status
            if not ts:
                continue
            hours = f"{ts.hours_since_sync:.1f}" if ts.hours_since_sync else "N/A"
            rows += (
                f"<tr><td>{escape(v.workspace_name)}</td>"
                f"<td>{escape(ts.table_name)}</td>"
                f"<td style='color:red'>{hours}</td>"
                f"<td>{ts.sla_threshold_hours}</td>"
                f"<td>{escape(ts.freshness_status.value)}</td></tr>"
            )

        html = (
            "<html><body style='font-family:Arial,sans-serif'>"
            f"<h3>Freshness Guard: {result.violation_count} SLA Violation(s)</h3>"
            f"<p>Scanned {result.total_tables} tables across "
            f"{len(result.workspace_ids)} workspace(s).</p>"
            "<table border='1' cellpadding='6' style='border-collapse:collapse'>"
            "<tr style='background:#f0f0f0'><th>Workspace</th><th>Table</th>"
            "<th>Hours Stale</th><th>SLA (hr)</th><th>Status</th></tr>"
            f"{rows}</table>"
            "</body></html>"
        )

        subject = (
            f"[Fabric Guard] {result.violation_count} freshness violation(s) detected"
        )
        self.email_notifier._send_email(self.alert_recipients, subject, html)

    # ── Audit trail ──────────────────────────────────────────────────────

    async def _record_freshness_audit(self, result: Any) -> None:
        """Record freshness scan result to MemoryManager."""
        if not self.memory:
            return
        try:
            from fabric_agent.storage.memory_manager import StateType

            await self.memory.record_state(
                state_type=StateType.CUSTOM,
                operation="freshness_scan",
                state_data=result.to_dict(),
            )
        except Exception as exc:
            logger.warning("Failed to record freshness audit: %s", exc)

    async def _record_maintenance_audit(self, result: Any) -> None:
        """Record maintenance run result to MemoryManager."""
        if not self.memory:
            return
        try:
            from fabric_agent.storage.memory_manager import StateType

            await self.memory.record_state(
                state_type=StateType.CUSTOM,
                operation="table_maintenance",
                state_data=result.to_dict(),
            )
        except Exception as exc:
            logger.warning("Failed to record maintenance audit: %s", exc)
