"""
Freshness Guard — SQL Endpoint Sync Lag Detection
==================================================

Detects when SQL Endpoint sync silently falls behind Lakehouse Delta writes.

PROBLEM:
    Every Fabric Lakehouse has an auto-created SQL Analytics Endpoint. The sync
    between Lakehouse (Delta) and SQL Endpoint is asynchronous, opaque, and has
    no built-in alerting. Tables can be days stale with no notification.

    Live test evidence (2026-02-24): ``raw_customer_events`` was 6 days stale.
    No Fabric alert was generated. ``refreshMetadata`` API returned per-table
    ``lastSuccessfulSyncDateTime`` confirming the gap.

ALGORITHM:
    1. Discover SQL endpoints: ``GET /workspaces/{ws}/sqlEndpoints``
    2. Trigger refresh:       ``POST /workspaces/{ws}/sqlEndpoints/{id}/refreshMetadata``
    3. Poll LRO to completion
    4. Parse per-table ``lastSuccessfulSyncDateTime``
    5. Compare against SLA thresholds (pattern → hours, fnmatch)
    6. Return violations

FAANG PARALLEL:
    Google BigQuery slot replication lag — tracked per-table in Monarch with
    SLA windows. Netflix Keystone per-stream freshness detector — each stream
    has an independent SLA; one slow stream doesn't mask others.
"""
from __future__ import annotations

import fnmatch
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from fabric_agent.guards.models import (
    FreshnessScanResult,
    FreshnessStatus,
    FreshnessViolation,
    TableSyncStatus,
)

if TYPE_CHECKING:
    from fabric_agent.api.fabric_client import FabricApiClient
    from fabric_agent.integrations.email import EmailNotifier
    from fabric_agent.storage.memory_manager import MemoryManager

logger = logging.getLogger(__name__)

DEFAULT_SLA_THRESHOLDS: Dict[str, float] = {
    "fact_*": 1.0,
    "dim_*": 24.0,
    "raw_*": 6.0,
    "*": 24.0,
}


class FreshnessGuard:
    """
    Detects SQL Endpoint sync lag across Fabric workspaces.

    Args:
        client:           FabricApiClient for API calls (None = offline/test mode).
        sla_thresholds:   Pattern-to-hours SLA map. First fnmatch wins.
        email_notifier:   Optional EmailNotifier for alerting on violations.
        alert_recipients: Email addresses to notify on violations.
        memory:           MemoryManager for audit trail.
        lro_timeout_secs: Max seconds to wait for refreshMetadata LRO (default 300).
    """

    def __init__(
        self,
        client: Optional[FabricApiClient] = None,
        sla_thresholds: Optional[Dict[str, float]] = None,
        email_notifier: Optional[EmailNotifier] = None,
        alert_recipients: Optional[List[str]] = None,
        memory: Optional[MemoryManager] = None,
        lro_timeout_secs: int = 300,
    ):
        self.client = client
        self.sla_thresholds = sla_thresholds or dict(DEFAULT_SLA_THRESHOLDS)
        self.email_notifier = email_notifier
        self.alert_recipients = alert_recipients or []
        self.memory = memory
        self.lro_timeout_secs = lro_timeout_secs

    # ── Public API ───────────────────────────────────────────────────────

    async def scan(self, workspace_ids: List[str]) -> FreshnessScanResult:
        """
        Full freshness scan across all workspaces.

        Each workspace is independently try/excepted — one failed workspace
        does not abort others. Errors are recorded in ``result.errors``.

        Args:
            workspace_ids: List of Fabric workspace GUIDs.

        Returns:
            FreshnessScanResult with all violations and per-table status.
        """
        t0 = time.perf_counter()
        result = FreshnessScanResult(workspace_ids=list(workspace_ids))

        total_tables = 0
        for ws_id in workspace_ids:
            try:
                violations, table_count = await self._scan_workspace(ws_id)
                result.violations.extend(violations)
                total_tables += table_count
            except Exception as exc:
                logger.warning("Freshness scan failed for workspace %s: %s", ws_id, exc)
                result.errors.append(f"workspace {ws_id}: {exc}")

        result.total_tables = total_tables
        result.violation_count = len(result.violations)
        result.healthy_count = total_tables - result.violation_count
        result.scan_duration_ms = int((time.perf_counter() - t0) * 1000)
        return result

    # ── Private helpers ──────────────────────────────────────────────────

    async def _scan_workspace(
        self, workspace_id: str
    ) -> Tuple[List[FreshnessViolation], int]:
        """Scan one workspace. Returns (violations, table_count)."""
        ws_name = await self._get_workspace_name(workspace_id)
        endpoints = await self._list_sql_endpoints(workspace_id)
        if not endpoints:
            logger.info("No SQL endpoints in workspace %s", workspace_id)
            return [], 0

        violations: List[FreshnessViolation] = []
        table_count = 0

        for ep in endpoints:
            ep_id = ep.get("id", "")
            ep_name = ep.get("displayName", ep_id)
            try:
                table_statuses = await self._refresh_and_parse(workspace_id, ep_id)
                table_count += len(table_statuses)
                for ts in table_statuses:
                    if ts.freshness_status in (
                        FreshnessStatus.STALE,
                        FreshnessStatus.NEVER_SYNCED,
                        FreshnessStatus.SYNC_FAILED,
                    ):
                        violations.append(
                            FreshnessViolation(
                                workspace_id=workspace_id,
                                workspace_name=ws_name,
                                sql_endpoint_id=ep_id,
                                sql_endpoint_name=ep_name,
                                table_status=ts,
                            )
                        )
            except Exception as exc:
                logger.warning(
                    "SQL endpoint %s scan failed: %s", ep_id, exc
                )

        return violations, table_count

    async def _list_sql_endpoints(self, workspace_id: str) -> List[Dict[str, Any]]:
        """
        ``GET /workspaces/{ws}/sqlEndpoints``

        Uses ``get_raw`` (not ``get``) because 404 is expected on workspaces
        with no lakehouses.
        """
        if not self.client:
            return []
        resp = await self.client.get_raw(
            f"/workspaces/{workspace_id}/sqlEndpoints"
        )
        if resp.status_code == 200:
            data = resp.json() if resp.content else {}
            return data.get("value", [])
        return []

    async def _refresh_and_parse(
        self, workspace_id: str, endpoint_id: str
    ) -> List[TableSyncStatus]:
        """
        ``POST /workspaces/{ws}/sqlEndpoints/{id}/refreshMetadata`` → LRO → parse.

        Uses ``post_with_lro`` which handles the 202 → poll loop.
        On LRO timeout, returns empty list and logs a warning.
        """
        if not self.client:
            return []

        lro_result = await self.client.post_with_lro(
            f"/workspaces/{workspace_id}/sqlEndpoints/{endpoint_id}/refreshMetadata",
            json_data={},
            timeout=self.lro_timeout_secs,
        )

        # The LRO result may contain tables at different paths depending on
        # the Fabric API version. Try common locations.
        tables_raw: List[Dict[str, Any]] = []
        if isinstance(lro_result, dict):
            tables_raw = (
                lro_result.get("tables", [])
                or lro_result.get("value", [])
                or lro_result.get("syncStatuses", [])
            )
            # If the result itself is the operation wrapper, dig into properties
            props = lro_result.get("properties", {})
            if isinstance(props, dict) and not tables_raw:
                tables_raw = props.get("tables", [])

        return [self._parse_table_sync(t) for t in tables_raw if isinstance(t, dict)]

    def _parse_table_sync(self, raw: Dict[str, Any]) -> TableSyncStatus:
        """Parse one table entry from ``refreshMetadata`` response."""
        table_name = raw.get("tableName", raw.get("name", ""))
        last_sync = raw.get("lastSuccessfulSyncDateTime")
        sync_status = raw.get("status", "Unknown")
        start_dt = raw.get("startDateTime")
        end_dt = raw.get("endDateTime")

        sla_hours, sla_pattern = self._resolve_sla(table_name)

        # Classify freshness
        hours_since: Optional[float] = None
        freshness = FreshnessStatus.UNKNOWN

        if sync_status in ("Failure", "Error"):
            freshness = FreshnessStatus.SYNC_FAILED
        elif not last_sync:
            freshness = FreshnessStatus.NEVER_SYNCED
        else:
            try:
                sync_dt = datetime.fromisoformat(last_sync.replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                delta = now - sync_dt
                hours_since = delta.total_seconds() / 3600.0
                freshness = (
                    FreshnessStatus.STALE
                    if hours_since > sla_hours
                    else FreshnessStatus.HEALTHY
                )
            except (ValueError, TypeError):
                freshness = FreshnessStatus.UNKNOWN

        return TableSyncStatus(
            table_name=table_name,
            last_successful_sync_dt=last_sync,
            sync_status=sync_status,
            start_dt=start_dt,
            end_dt=end_dt,
            hours_since_sync=hours_since,
            freshness_status=freshness,
            sla_threshold_hours=sla_hours,
            sla_pattern=sla_pattern,
        )

    def _resolve_sla(self, table_name: str) -> Tuple[float, str]:
        """
        Resolve SLA threshold for a table name via fnmatch patterns.

        Iterates ``sla_thresholds`` in insertion order; first match wins.
        Returns ``(threshold_hours, matched_pattern)``.
        """
        lower = table_name.lower()
        for pattern, hours in self.sla_thresholds.items():
            if fnmatch.fnmatch(lower, pattern.lower()):
                return hours, pattern
        return 24.0, "*"

    async def _get_workspace_name(self, workspace_id: str) -> str:
        """Resolve workspace display name; falls back to ID."""
        if not self.client:
            return workspace_id
        try:
            resp = await self.client.get_raw(f"/workspaces/{workspace_id}")
            if resp.status_code == 200:
                data = resp.json() if resp.content else {}
                return data.get("displayName", workspace_id)
        except Exception:
            pass
        return workspace_id
