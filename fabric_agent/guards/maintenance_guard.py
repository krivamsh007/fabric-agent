"""
Maintenance Guard — Table Maintenance Pre-Submission Validation
================================================================

Validates table names before submitting to the Fabric TableMaintenance API,
then submits sequentially (Trial capacity safe), and polls for completion.

PROBLEM:
    The Fabric TableMaintenance API accepts ANY string as tableName with HTTP 202.
    When the Spark job actually starts, it cannot find the table and fails with a
    non-descriptive ``TableNotFound`` error. On Trial capacity each failed job
    wastes 4-9 minutes blocking the single Spark slot.

    Live test evidence (2026-02-24):
        - Control char in name (``dim_date\\x01probe``) → accepted (202), failed async
        - Schema-qualified (``custom_schema.dim_date``) → accepted (202), failed async
        - Valid name (``fact_sales``) → accepted (202), succeeded after 264 seconds

ALGORITHM:
    1. Discover lakehouses: ``GET /workspaces/{ws}/items?type=Lakehouse``
    2. List tables per LH:  ``GET /workspaces/{ws}/lakehouses/{lh}/tables``
    3. Validate each table name (control chars, schema-qualified, registry lookup)
    4. Check queue pressure: ``GET /workspaces/{ws}/items/{lh}/jobs/instances``
    5. Submit one at a time: ``POST .../jobs/instances?jobType=TableMaintenance``
    6. Poll to terminal:    ``GET .../jobs/instances/{id}``

FAANG PARALLEL:
    Stripe API gateway validation — reject invalid requests at O(1) cost at the
    edge before forwarding to backend services. Google Dataproc job validation —
    validates Spark parameters before submission to prevent "accepted but doomed" jobs.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple

from fabric_agent.guards.models import (
    MaintenanceJobRecord,
    MaintenanceJobStatus,
    MaintenanceRunResult,
    TableValidationResult,
)

if TYPE_CHECKING:
    from fabric_agent.api.fabric_client import FabricApiClient
    from fabric_agent.integrations.email import EmailNotifier
    from fabric_agent.storage.memory_manager import MemoryManager

logger = logging.getLogger(__name__)

_CONTROL_CHAR_RE = re.compile(r"[\x00-\x1f\x7f-\x9f]")
_VALID_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_F_SKU_RE = re.compile(r"\bF(\d+)\b", re.IGNORECASE)

# Fabric job statuses → our enum
_STATUS_MAP = {
    "notstarted": MaintenanceJobStatus.QUEUED,
    "inprogress": MaintenanceJobStatus.RUNNING,
    "completed": MaintenanceJobStatus.SUCCEEDED,
    "succeeded": MaintenanceJobStatus.SUCCEEDED,
    "success": MaintenanceJobStatus.SUCCEEDED,
    "failed": MaintenanceJobStatus.FAILED,
    "error": MaintenanceJobStatus.FAILED,
    "cancelled": MaintenanceJobStatus.CANCELLED,
    "canceled": MaintenanceJobStatus.CANCELLED,
}


class MaintenanceGuard:
    """
    Validates and submits Fabric TableMaintenance jobs with pre-submission guard.

    Args:
        client:                    FabricApiClient.
        email_notifier:            Optional EmailNotifier for failure alerts.
        alert_recipients:          Email addresses for failure notifications.
        memory:                    MemoryManager for audit trail.
        queue_pressure_threshold:  Max active jobs before skipping (default 3).
        job_poll_interval_secs:    Seconds between status polls (default 30).
        job_timeout_secs:          Max seconds to wait per job (default 900).
        dry_run:                   If True, validate but never submit.
    """

    def __init__(
        self,
        client: Optional[FabricApiClient] = None,
        email_notifier: Optional[EmailNotifier] = None,
        alert_recipients: Optional[List[str]] = None,
        memory: Optional[MemoryManager] = None,
        queue_pressure_threshold: int = 3,
        job_poll_interval_secs: int = 30,
        job_timeout_secs: int = 900,
        dry_run: bool = True,
        max_concurrency: Optional[int] = None,
        auto_concurrency_by_capacity: bool = True,
    ):
        self.client = client
        self.email_notifier = email_notifier
        self.alert_recipients = alert_recipients or []
        self.memory = memory
        self.queue_pressure_threshold = queue_pressure_threshold
        self.job_poll_interval_secs = job_poll_interval_secs
        self.job_timeout_secs = job_timeout_secs
        self.dry_run = dry_run
        self.max_concurrency = (
            max(1, int(max_concurrency)) if max_concurrency is not None else None
        )
        self.auto_concurrency_by_capacity = auto_concurrency_by_capacity
        self._workspace_concurrency_cache: Dict[str, int] = {}
        self._workspace_capacity_hint_cache: Dict[str, Dict[str, Any]] = {}

    # ── Public API ───────────────────────────────────────────────────────

    async def run_maintenance(
        self,
        workspace_ids: List[str],
        table_filter: Optional[List[str]] = None,
        table_filter_by_lakehouse: Optional[Dict[str, List[str]]] = None,
    ) -> MaintenanceRunResult:
        """
        Full maintenance run: discover → validate → queue-check → submit → poll.

        Args:
            workspace_ids: Fabric workspace GUIDs to process.
            table_filter:  Optional whitelist of table names. None = all tables.

        Returns:
            MaintenanceRunResult with full per-table audit trail.
        """
        result = MaintenanceRunResult(
            workspace_ids=list(workspace_ids), dry_run=self.dry_run
        )

        for ws_id in workspace_ids:
            try:
                records = await self._run_workspace_maintenance(
                    workspace_id=ws_id,
                    table_filter=table_filter,
                    table_filter_by_lakehouse=table_filter_by_lakehouse,
                )
                result.job_records.extend(records)
            except Exception as exc:
                logger.warning("Maintenance failed for workspace %s: %s", ws_id, exc)
                result.errors.append(f"workspace {ws_id}: {exc}")

        # Aggregate counts
        result.total_tables = len(result.job_records)
        result.validated = sum(
            1 for r in result.job_records if r.validation and r.validation.is_valid
        )
        result.rejected = sum(
            1 for r in result.job_records if r.validation and not r.validation.is_valid
        )
        result.submitted = sum(
            1
            for r in result.job_records
            if r.status
            not in (
                MaintenanceJobStatus.NOT_SUBMITTED,
                MaintenanceJobStatus.SKIPPED_QUEUE,
                MaintenanceJobStatus.SKIPPED_DRY_RUN,
            )
        )
        result.succeeded = sum(
            1 for r in result.job_records if r.status == MaintenanceJobStatus.SUCCEEDED
        )
        result.failed = sum(
            1
            for r in result.job_records
            if r.status in (MaintenanceJobStatus.FAILED, MaintenanceJobStatus.CANCELLED)
        )
        result.skipped_queue = sum(
            1 for r in result.job_records if r.status == MaintenanceJobStatus.SKIPPED_QUEUE
        )
        result.completed_at = datetime.now(timezone.utc).isoformat()
        return result

    # ── Workspace / Lakehouse iteration ──────────────────────────────────

    async def _run_workspace_maintenance(
        self,
        workspace_id: str,
        table_filter: Optional[List[str]],
        table_filter_by_lakehouse: Optional[Dict[str, List[str]]],
    ) -> List[MaintenanceJobRecord]:
        """Process all lakehouses in one workspace."""
        lakehouses = await self._list_lakehouses(workspace_id)
        records: List[MaintenanceJobRecord] = []
        workspace_concurrency = await self._resolve_workspace_concurrency(workspace_id)
        queue_threshold = max(
            1, min(self.queue_pressure_threshold, workspace_concurrency)
        )

        logger.info(
            "Maintenance workspace profile: workspace=%s max_concurrency=%s queue_threshold=%s",
            workspace_id,
            workspace_concurrency,
            queue_threshold,
        )

        if workspace_concurrency <= 1:
            for lh in lakehouses:
                lh_id = lh.get("id", "")
                lh_name = lh.get("displayName", lh_id)
                try:
                    lh_records = await self._run_lakehouse_maintenance(
                        workspace_id=workspace_id,
                        lakehouse_id=lh_id,
                        lakehouse_name=lh_name,
                        table_filter=table_filter,
                        table_filter_by_lakehouse=table_filter_by_lakehouse,
                        queue_threshold=queue_threshold,
                    )
                    records.extend(lh_records)
                except Exception as exc:
                    logger.warning("Lakehouse %s maintenance failed: %s", lh_id, exc)
            return records

        sem = asyncio.Semaphore(workspace_concurrency)

        async def _process_lakehouse(lh: Dict[str, Any]) -> List[MaintenanceJobRecord]:
            lh_id = lh.get("id", "")
            lh_name = lh.get("displayName", lh_id)
            async with sem:
                return await self._run_lakehouse_maintenance(
                    workspace_id=workspace_id,
                    lakehouse_id=lh_id,
                    lakehouse_name=lh_name,
                    table_filter=table_filter,
                    table_filter_by_lakehouse=table_filter_by_lakehouse,
                    queue_threshold=queue_threshold,
                )

        tasks = [asyncio.create_task(_process_lakehouse(lh)) for lh in lakehouses]
        for task in asyncio.as_completed(tasks):
            try:
                records.extend(await task)
            except Exception as exc:
                logger.warning("Lakehouse maintenance task failed: %s", exc)

        return records

    async def _run_lakehouse_maintenance(
        self,
        workspace_id: str,
        lakehouse_id: str,
        lakehouse_name: str,
        table_filter: Optional[List[str]],
        table_filter_by_lakehouse: Optional[Dict[str, List[str]]],
        queue_threshold: int,
    ) -> List[MaintenanceJobRecord]:
        """Validate + submit maintenance for all tables in one lakehouse."""
        registered_tables = await self._list_delta_tables(workspace_id, lakehouse_id)
        if not registered_tables:
            return []

        # Apply table_filter if provided
        target_tables: List[Dict[str, Any]]
        scoped_filter: Optional[List[str]] = None
        if table_filter_by_lakehouse is not None:
            # Explicit scoped mode: missing key means "run none" for this lakehouse.
            scoped_filter = table_filter_by_lakehouse.get(lakehouse_id, [])

        if scoped_filter is not None:
            scoped_set = {t.lower() for t in scoped_filter}
            target_tables = [
                t for t in registered_tables
                if t.get("name", "").lower() in scoped_set
            ]
        elif table_filter:
            filter_lower = {t.lower() for t in table_filter}
            target_tables = [
                t for t in registered_tables
                if t.get("name", "").lower() in filter_lower
            ]
        else:
            target_tables = registered_tables

        records: List[MaintenanceJobRecord] = []
        for table_dict in target_tables:
            table_name = table_dict.get("name", "")
            if not table_name:
                continue

            record = MaintenanceJobRecord(
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                lakehouse_name=lakehouse_name,
                table_name=table_name,
                dry_run=self.dry_run,
            )

            # Step 1: Validate
            validation = self._validate_table_name(table_name, registered_tables)
            record.validation = validation

            if not validation.is_valid:
                record.status = MaintenanceJobStatus.NOT_SUBMITTED
                record.error = validation.rejection_reason
                records.append(record)
                continue

            # Step 2: Dry-run check
            if self.dry_run:
                record.status = MaintenanceJobStatus.SKIPPED_DRY_RUN
                records.append(record)
                continue

            # Step 3: Queue pressure check
            active_jobs = await self._check_queue_pressure(workspace_id, lakehouse_id)
            if active_jobs >= queue_threshold:
                record.status = MaintenanceJobStatus.SKIPPED_QUEUE
                record.error = (
                    f"active_jobs={active_jobs} >= threshold={queue_threshold}"
                )
                records.append(record)
                continue

            # Step 4: Submit
            try:
                fabric_job_id = await self._submit_maintenance_job(
                    workspace_id, lakehouse_id, table_name
                )
                record.fabric_job_id = fabric_job_id
                record.submitted_at = datetime.now(timezone.utc).isoformat()

                # Step 5: Poll to terminal
                final_status = await self._poll_job_status(
                    workspace_id, lakehouse_id, fabric_job_id
                )
                record.status = final_status
                record.completed_at = datetime.now(timezone.utc).isoformat()
                if final_status == MaintenanceJobStatus.FAILED:
                    record.error = "Spark job failed (check Fabric job history for details)"
            except Exception as exc:
                record.status = MaintenanceJobStatus.FAILED
                record.error = str(exc)
                record.completed_at = datetime.now(timezone.utc).isoformat()

            records.append(record)

        return records

    # ── Validation ───────────────────────────────────────────────────────

    def _validate_table_name(
        self,
        table_name: str,
        registered_tables: List[Dict[str, Any]],
    ) -> TableValidationResult:
        """
        Pre-submission validation. Intentionally strict — a false positive
        (rejecting a valid table) wastes one cycle; a false negative (submitting
        an invalid name) wastes 4-9 minutes of Spark compute.

        Checks:
            1. Control characters → reject
            2. Schema-qualified (contains ``.``) → reject
            3. Table name format (``^[A-Za-z_][A-Za-z0-9_]*$``) → reject
            4. Registry lookup (case-insensitive) → reject if not found
        """
        # Check 1: control characters
        if _CONTROL_CHAR_RE.search(table_name):
            return TableValidationResult(
                table_name=table_name,
                is_valid=False,
                rejection_reason="control character in table name",
            )

        # Check 2: schema-qualified
        if "." in table_name:
            return TableValidationResult(
                table_name=table_name,
                is_valid=False,
                rejection_reason="schema-qualified name not supported (use bare table name)",
            )

        # Check 3: name format
        if not _VALID_TABLE_NAME_RE.match(table_name):
            return TableValidationResult(
                table_name=table_name,
                is_valid=False,
                rejection_reason=f"invalid table name format: '{table_name}'",
            )

        # Check 4: registry lookup
        registered_names = {
            t.get("name", "").strip().lower() for t in registered_tables
        }
        canonical = table_name.strip().lower()
        if canonical not in registered_names:
            return TableValidationResult(
                table_name=table_name,
                is_valid=False,
                rejection_reason=f"table '{table_name}' not found in lakehouse Delta registry",
            )

        return TableValidationResult(
            table_name=table_name,
            is_valid=True,
            resolved_name=canonical,
        )

    # ── API helpers ──────────────────────────────────────────────────────

    async def _list_lakehouses(self, workspace_id: str) -> List[Dict[str, Any]]:
        """``GET /workspaces/{ws}/items?type=Lakehouse``"""
        if not self.client:
            return []
        data = await self.client.get(
            f"/workspaces/{workspace_id}/items", params={"type": "Lakehouse"}
        )
        return data.get("value", []) if isinstance(data, dict) else []

    async def _list_delta_tables(
        self, workspace_id: str, lakehouse_id: str
    ) -> List[Dict[str, Any]]:
        """``GET /workspaces/{ws}/lakehouses/{lh}/tables``"""
        if not self.client:
            return []
        resp = await self.client.get_raw(
            f"/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables"
        )
        if resp.status_code == 200:
            data = resp.json() if resp.content else {}
            return data.get("data", data.get("value", []))
        return []

    async def _check_queue_pressure(
        self, workspace_id: str, lakehouse_id: str
    ) -> int:
        """Count active TableMaintenance jobs. Returns 0 on API error (optimistic)."""
        if not self.client:
            return 0
        try:
            resp = await self.client.get_raw(
                f"/workspaces/{workspace_id}/items/{lakehouse_id}/jobs/instances",
                params={"jobType": "TableMaintenance", "$top": 20},
            )
            if resp.status_code != 200:
                return 0
            data = resp.json() if resp.content else {}
            runs = data.get("value", data.get("data", []))
            active_states = {"NotStarted", "InProgress"}
            return sum(
                1
                for r in runs
                if str(r.get("status", "")).strip() in active_states
            )
        except Exception:
            return 0

    async def _submit_maintenance_job(
        self, workspace_id: str, lakehouse_id: str, table_name: str
    ) -> str:
        """
        ``POST /workspaces/{ws}/items/{lh}/jobs/instances?jobType=TableMaintenance``

        Returns the Fabric job instance ID from the Location header.
        Uses ``post_raw`` for manual polling control (not ``post_with_lro``).
        """
        if not self.client:
            raise RuntimeError("No client — cannot submit maintenance job")

        payload = {
            "executionData": {
                "tableName": table_name,
                "optimizeSettings": {"vOrder": True},
            }
        }

        resp = await self.client.post_raw(
            f"/workspaces/{workspace_id}/items/{lakehouse_id}/jobs/instances",
            params={"jobType": "TableMaintenance"},
            json_data=payload,
        )

        if resp.status_code not in (200, 202):
            body = resp.json() if resp.content else {}
            raise RuntimeError(
                f"TableMaintenance submit failed: HTTP {resp.status_code} — {body}"
            )

        # Extract job instance ID from Location header
        location = resp.headers.get("Location", "") or resp.headers.get("location", "")
        if location:
            # Location: .../jobs/instances/{job_id}
            parts = location.rstrip("/").split("/")
            return parts[-1] if parts else ""
        return ""

    async def _poll_job_status(
        self,
        workspace_id: str,
        lakehouse_id: str,
        job_id: str,
    ) -> MaintenanceJobStatus:
        """Poll job status until terminal or timeout."""
        if not job_id or not self.client:
            return MaintenanceJobStatus.FAILED

        deadline = time.perf_counter() + self.job_timeout_secs
        terminal = {"succeeded", "failed", "cancelled", "canceled", "completed", "error"}

        while time.perf_counter() < deadline:
            await asyncio.sleep(self.job_poll_interval_secs)
            try:
                resp = await self.client.get_raw(
                    f"/workspaces/{workspace_id}/items/{lakehouse_id}"
                    f"/jobs/instances/{job_id}"
                )
                if resp.status_code != 200:
                    continue
                data = resp.json() if resp.content else {}
                status = str(data.get("status", "")).strip().lower()
                if status in terminal:
                    return _STATUS_MAP.get(status, MaintenanceJobStatus.FAILED)
            except Exception as exc:
                logger.warning("Poll error for job %s: %s", job_id, exc)

        logger.warning("Job %s timed out after %ds", job_id, self.job_timeout_secs)
        return MaintenanceJobStatus.FAILED

    # -- Capacity-aware execution profile -------------------------------------

    async def _resolve_workspace_concurrency(self, workspace_id: str) -> int:
        """
        Resolve execution concurrency for a workspace.

        Rule #1 (always): Trial capacity runs sequentially (max_concurrency=1).
        Otherwise choose based on capacity hints and cap with explicit max_concurrency.
        """
        cached = self._workspace_concurrency_cache.get(workspace_id)
        if cached is not None:
            return cached

        # Safe default for unknown environments.
        chosen = 1
        hints: Dict[str, Any] = {}

        if self.auto_concurrency_by_capacity:
            hints = await self._get_workspace_capacity_hints(workspace_id)
            chosen = self._concurrency_from_capacity_hints(hints)

        # Hard override: trial must stay sequential.
        if self._looks_like_trial_capacity(hints):
            chosen = 1

        # Explicit constructor limit acts as a hard cap.
        if self.max_concurrency is not None:
            chosen = min(chosen, self.max_concurrency)

        chosen = max(1, int(chosen))
        self._workspace_concurrency_cache[workspace_id] = chosen
        return chosen

    async def _get_workspace_capacity_hints(self, workspace_id: str) -> Dict[str, Any]:
        cached = self._workspace_capacity_hint_cache.get(workspace_id)
        if cached is not None:
            return cached

        hints: Dict[str, Any] = {}
        if not self.client:
            return hints

        try:
            ws_resp = await self.client.get_raw(f"/workspaces/{workspace_id}")
            if ws_resp.status_code == 200:
                ws = ws_resp.json() if ws_resp.content else {}
                if isinstance(ws, dict):
                    hints.update(ws)
        except Exception:
            pass

        # Some tenants only expose capacityName via environment.
        env_cap_name = os.getenv("FABRIC_CAPACITY_NAME", "").strip()
        if env_cap_name:
            hints.setdefault("capacityName", env_cap_name)

        self._workspace_capacity_hint_cache[workspace_id] = hints
        return hints

    def _looks_like_trial_capacity(self, hints: Dict[str, Any]) -> bool:
        hint_texts = self._collect_text_values(hints)
        for text in hint_texts:
            if "trial" in text.lower():
                return True

        # If we already have concrete capacity hints and none indicates "trial",
        # trust those hints over environment fallback.
        if hint_texts:
            return False

        if str(os.getenv("FABRIC_TRIAL_CAPACITY", "")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }:
            return True
        return False

    def _concurrency_from_capacity_hints(self, hints: Dict[str, Any]) -> int:
        """
        Auto-tune concurrency by capacity/SKU hints.

        Conservative defaults:
          - unknown: 1
          - F2/F4/F8: 2
          - F16/F32: 4
          - F64+: 6
        """
        if self._looks_like_trial_capacity(hints):
            return 1

        for text in self._collect_text_values(hints):
            match = _F_SKU_RE.search(text)
            if not match:
                continue
            try:
                size = int(match.group(1))
            except ValueError:
                continue
            if size <= 8:
                return 2
            if size <= 32:
                return 4
            return 6

        return 1

    def _collect_text_values(self, payload: Any) -> List[str]:
        out: List[str] = []

        def _walk(node: Any) -> None:
            if isinstance(node, str):
                out.append(node)
                return
            if isinstance(node, dict):
                for value in node.values():
                    _walk(value)
                return
            if isinstance(node, list):
                for value in node:
                    _walk(value)

        _walk(payload)
        return out
