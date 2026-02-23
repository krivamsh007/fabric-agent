"""
Self-Healing Monitor
====================

Orchestration loop that connects detector → healer → persist → notify.

SCHEDULING OPTIONS:
    1. Fabric Pipeline (recommended for production):
       Schedule a Fabric Pipeline to call this every 30 minutes.
       The Notebook health_monitor.ipynb wraps this class.

    2. Local polling (development/testing):
       monitor.run_loop(workspace_ids=[...], interval_minutes=30)

    3. One-shot (CI / scripts):
       report = await monitor.run_once(workspace_ids=[...])

OUTPUTS:
    - HealthReport object (always returned)
    - JSON file in data/health_logs/ (local) or OneLake (Fabric)
    - Slack/Teams notification if manual_required > 0

FAANG PATTERN:
    Google Monarch: continuous health monitoring with alerting.
    Meta Scuba: always-on anomaly detection → auto-routing.
    Netflix Hystrix: circuit-breaker + auto-recovery.
    This is Fabric-adapted self-healing: detect → decide → act → notify.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, TYPE_CHECKING

from loguru import logger

from fabric_agent.healing.detector import AnomalyDetector
from fabric_agent.healing.healer import SelfHealer
from fabric_agent.healing.models import HealthReport, HealingPlan

if TYPE_CHECKING:
    from fabric_agent.api.fabric_client import FabricApiClient
    from fabric_agent.storage.memory_manager import MemoryManager
    from fabric_agent.integrations.slack import SlackNotifier


class SelfHealingMonitor:
    """
    Orchestrates the full detect → heal → persist → notify loop.

    USAGE (one-shot):
        monitor = SelfHealingMonitor(client=client, memory=memory_manager)
        report = await monitor.run_once(["ws-abc123"], dry_run=True)
        print(report.health_score)

    USAGE (polling loop — local dev):
        await monitor.run_loop(["ws-abc123"], interval_minutes=5)

    USAGE (Fabric Notebook — production):
        # Schedule this notebook via a Fabric Pipeline every 30 minutes
        from fabric_agent.healing import SelfHealingMonitor
        monitor = SelfHealingMonitor(client=client)
        report = await monitor.run_once(workspace_ids, dry_run=False)

    PARAMETERS:
        auto_heal: Set False to disable all auto-fixing (report-only mode).
        dry_run:   When run_once() dry_run=True, all healer actions are SKIPPED.
        slack_notifier: Optional SlackNotifier; sends alert if manual review needed.
    """

    def __init__(
        self,
        client: Optional["FabricApiClient"] = None,
        memory: Optional["MemoryManager"] = None,
        slack_notifier: Optional["SlackNotifier"] = None,
        contracts_dir: Optional[str] = None,
        stale_hours: int = 24,
        health_log_dir: Optional[str] = None,
        auto_heal: bool = True,
    ):
        self.client = client
        self.memory = memory
        self.slack_notifier = slack_notifier
        self.auto_heal = auto_heal

        self.detector = AnomalyDetector(
            client=client,
            contracts_dir=contracts_dir,
            stale_hours=stale_hours,
        )
        self.healer = SelfHealer(client=client, memory=memory)

        # Where to persist health reports locally
        if health_log_dir:
            self._log_dir = Path(health_log_dir)
        else:
            try:
                from fabric_agent.core.fabric_env import get_lakehouse_path
                self._log_dir = Path(get_lakehouse_path("health_logs"))
            except Exception:
                self._log_dir = Path("data/health_logs")

    # ------------------------------------------------------------------ #
    # Public Interface                                                      #
    # ------------------------------------------------------------------ #

    async def run_once(
        self,
        workspace_ids: List[str],
        dry_run: bool = True,
    ) -> HealthReport:
        """
        Run a single detect → heal → persist → notify cycle.

        Args:
            workspace_ids: List of Fabric workspace IDs to scan.
            dry_run: If True, healing actions are simulated only.

        Returns:
            HealthReport with full scan + heal summary.
        """
        start_ms = int(time.time() * 1000)
        report = HealthReport(workspace_ids=workspace_ids)

        logger.info(
            f"Self-healing scan starting | workspaces={workspace_ids} | dry_run={dry_run}"
        )

        try:
            # PHASE 1: DETECT
            anomalies = await self.detector.scan(workspace_ids)
            report.anomalies_found = len(anomalies)
            report.total_assets = await self._estimate_total_assets(workspace_ids)
            report.healthy = max(0, report.total_assets - len(anomalies))

            if not anomalies:
                logger.info("No anomalies detected — workspace is healthy")
                report.scan_duration_ms = int(time.time() * 1000) - start_ms
                await self._persist_report(report)
                return report

            # PHASE 2: BUILD PLAN
            plan = self.healer.build_plan(anomalies)
            report.healing_plan = plan
            report.manual_required = plan.manual_action_count

            # PHASE 3: HEAL (if enabled)
            if self.auto_heal:
                result = await self.healer.execute_healing_plan(plan, dry_run=dry_run)
                report.healing_result = result
                report.auto_healed = result.applied
                if result.errors:
                    report.errors.extend(result.errors)
            else:
                logger.info("Auto-healing disabled — report only mode")

            # PHASE 4: PERSIST
            report.scan_duration_ms = int(time.time() * 1000) - start_ms
            await self._persist_report(report)

            # PHASE 5: NOTIFY if manual actions needed
            if report.manual_required > 0:
                await self._notify_if_needed(report)

            logger.info(
                f"Scan complete | score={report.health_score:.2%} | "
                f"anomalies={report.anomalies_found} | "
                f"healed={report.auto_healed} | manual={report.manual_required}"
            )

        except Exception as e:
            logger.error(f"Self-healing scan failed: {e}")
            report.errors.append(str(e))
            report.scan_duration_ms = int(time.time() * 1000) - start_ms
            await self._persist_report(report)

        return report

    async def run_loop(
        self,
        workspace_ids: List[str],
        interval_minutes: int = 30,
        dry_run: bool = False,
        max_iterations: Optional[int] = None,
    ) -> None:
        """
        Continuous polling loop — runs run_once() every interval_minutes.

        For local development / testing. In production, use a Fabric Pipeline
        to schedule the Notebook instead (more reliable, better logging).

        Args:
            workspace_ids: Workspaces to monitor.
            interval_minutes: Time between scans.
            dry_run: If True, never apply changes (safe for testing).
            max_iterations: Stop after N iterations (None = run forever).
        """
        iteration = 0
        logger.info(
            f"Starting self-healing loop | interval={interval_minutes}m | "
            f"workspaces={workspace_ids} | dry_run={dry_run}"
        )

        while max_iterations is None or iteration < max_iterations:
            try:
                await self.run_once(workspace_ids, dry_run=dry_run)
            except Exception as e:
                logger.error(f"Loop iteration {iteration} failed: {e}")

            iteration += 1
            if max_iterations is None or iteration < max_iterations:
                logger.info(f"Next scan in {interval_minutes} minutes...")
                await asyncio.sleep(interval_minutes * 60)

    # ------------------------------------------------------------------ #
    # Internal Helpers                                                      #
    # ------------------------------------------------------------------ #

    async def _estimate_total_assets(self, workspace_ids: List[str]) -> int:
        """
        Rough count of total items across all scanned workspaces.
        Used for health_score calculation.
        """
        if not self.client:
            return 0
        total = 0
        for ws_id in workspace_ids:
            try:
                data = await self.client.get(f"/workspaces/{ws_id}/items")
                items = data.get("value", []) if isinstance(data, dict) else []
                total += len(items)
            except Exception:
                pass
        return total

    async def _persist_report(self, report: HealthReport) -> None:
        """
        Persist the HealthReport to local JSON and/or OneLake.

        LOCAL:   data/health_logs/<scan_id>.json
        FABRIC:  /lakehouse/default/Files/health_logs/<scan_id>.json
                 (Delta table write is future work — requires Delta writer)
        """
        try:
            self._log_dir.mkdir(parents=True, exist_ok=True)
            report_path = self._log_dir / f"{report.scan_id}.json"
            report_path.write_text(
                json.dumps(report.to_dict(), indent=2),
                encoding="utf-8",
            )
            logger.debug(f"Health report saved: {report_path}")
        except Exception as e:
            logger.warning(f"Could not persist health report: {e}")

    async def _notify_if_needed(self, report: HealthReport) -> None:
        """
        Send a Slack/Teams notification when manual actions are required.

        Only fires when report.manual_required > 0 — avoids notification
        spam for healthy workspaces.
        """
        if not self.slack_notifier:
            logger.info(
                f"Manual actions needed ({report.manual_required}) "
                "but no Slack notifier configured."
            )
            return

        try:
            # Build a minimal summary object compatible with SlackNotifier
            # (SlackNotifier uses duck typing via Any)
            summary = _ManualActionSummary(report)
            await self.slack_notifier.notify_impact(impact_report=summary)
            logger.info(f"Slack notification sent for {report.manual_required} manual actions")
        except Exception as e:
            logger.warning(f"Slack notification failed: {e}")


class _ManualActionSummary:
    """
    Duck-typed summary object for SlackNotifier.notify_impact().

    SlackNotifier accepts Any for impact_report and accesses fields
    by attribute. This adapts HealthReport to that interface.
    """

    def __init__(self, report: HealthReport):
        self._report = report

    @property
    def source_asset(self):
        class _Asset:
            name = f"{len(self._report.workspace_ids)} workspace(s)"
        return _Asset()

    @property
    def risk_level(self):
        class _Risk:
            value = "high" if self._report.manual_required > 3 else "medium"
        return _Risk()

    @property
    def risk_score(self) -> int:
        return min(100, self._report.manual_required * 10)

    @property
    def change_type(self) -> str:
        return "Self-Healing Scan"

    @property
    def total_affected(self) -> int:
        return self._report.anomalies_found

    @property
    def affected_workspaces(self) -> list:
        return self._report.workspace_ids

    @property
    def auto_fix_count(self) -> int:
        return self._report.auto_healed

    @property
    def manual_fix_count(self) -> int:
        return self._report.manual_required

    @property
    def warnings(self) -> list:
        lines = []
        if self._report.healing_plan:
            for action in self._report.healing_plan.manual_actions[:3]:
                lines.append(action.description)
        return lines
