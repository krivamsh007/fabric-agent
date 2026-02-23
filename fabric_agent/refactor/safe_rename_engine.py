from __future__ import annotations

import json
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

from fabric_agent.api.fabric_client import FabricApiClient, FabricApiError
from .models import DefinitionPart, ItemDefinition

OPS_DIR = Path("memory/operations")
OPS_DIR.mkdir(parents=True, exist_ok=True)

PLANS_DIR = Path("memory/plans")
PLANS_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class RenameImpact:
    semantic_model_parts_changed: list[str]
    reports_changed: dict[str, list[str]]  # report_id -> part paths
    risk: str
    notes: list[str]


def _safe_bracket_replace(text: str, old_bracketed: str, new_bracketed: str) -> tuple[str, int]:
    """Replace exact bracketed token occurrences.

    This intentionally does *not* do fuzzy matches; it replaces the exact token
    (e.g., "[Sales]") anywhere it appears, including inside JSON strings.

    For production scenarios, you may want to extend this with grammar-aware
    parsing for specific formats. The goal here is a safe, deterministic baseline.
    """
    if not (old_bracketed.startswith("[") and old_bracketed.endswith("]")):
        raise ValueError("old must be bracketed like [Measure Name]")
    if not (new_bracketed.startswith("[") and new_bracketed.endswith("]")):
        raise ValueError("new must be bracketed like [Measure Name]")
    pattern = re.escape(old_bracketed)
    new_text, n = re.subn(pattern, new_bracketed, text)
    return new_text, n


def _rewrite_definition(defn: ItemDefinition, old_b: str, new_b: str) -> tuple[ItemDefinition, list[str], int]:
    changed_paths: list[str] = []
    new_parts: list[DefinitionPart] = []
    hits = 0

    for part in defn.parts:
        try:
            txt = part.decode_utf8()
        except Exception:
            # Binary / non-UTF parts are preserved as-is.
            new_parts.append(part)
            continue

        new_txt, n = _safe_bracket_replace(txt, old_b, new_b)
        if n > 0:
            hits += n
            changed_paths.append(part.path)
            new_parts.append(DefinitionPart.from_text(part.path, new_txt))
        else:
            new_parts.append(part)

    return ItemDefinition(format=defn.format, parts=new_parts), changed_paths, hits


def _risk_score(model_hits: int, report_hits: int) -> str:
    # Reports are weighted heavier because breaking visuals is the primary risk.
    score = model_hits + (2 * report_hits)
    if score >= 25:
        return "HIGH"
    if score >= 5:
        return "MEDIUM"
    return "LOW"


class SafeRenameEngine:
    """Report-safe measure rename with audit + rollback (REST-only)."""

    def __init__(self, client: FabricApiClient):
        self._c = client

    async def plan_artifacts(
        self,
        workspace_id: str,
        semantic_model_id: str,
        old_bracketed: str,
        new_bracketed: str,
        operation_id: str | None = None,
        *,
        report_ids: list[str] | None = None,
        report_format: str = "PBIR",
        semantic_model_format: str = "TMDL",
    ) -> tuple[str, RenameImpact, dict, dict, dict, dict[str, dict], dict[str, dict]]:
        """Plan a safe rename and return artifacts needed for HTML review packets.

        Returns:
          (operation_id, impact, plan_doc, sm_before, sm_after, reports_before, reports_after)
        """
        op_id = operation_id or f"op_{uuid.uuid4().hex[:12]}"
        notes: list[str] = []

        try:
            sm_before = await self._c.get_semantic_model_definition(
                workspace_id, semantic_model_id, format=semantic_model_format
            )
        except FabricApiError as e:
            notes.append(f"Failed to fetch semantic model definition: {e}")
            raise

        sm_def = ItemDefinition.from_api(sm_before)
        sm_after_def, sm_changed, sm_hits = _rewrite_definition(sm_def, old_bracketed, new_bracketed)
        sm_after = sm_after_def.to_api()

        reports = report_ids
        if reports is None:
            try:
                reports = [r.get("id") for r in await self._c.list_reports(workspace_id) if r.get("id")]
            except Exception as e:
                notes.append(f"Failed to list reports: {e}")
                reports = []

        reports_changed: dict[str, list[str]] = {}
        reports_before: dict[str, dict] = {}
        reports_after: dict[str, dict] = {}
        total_report_hits = 0

        for rid in reports:
            try:
                rep_before = await self._c.get_report_definition(workspace_id, rid, format=report_format)
            except Exception as e:
                notes.append(f"Report {rid}: getDefinition failed ({e}); skipping")
                continue

            rep_def = ItemDefinition.from_api(rep_before)
            rep_after_def, rep_changed, rep_hits = _rewrite_definition(rep_def, old_bracketed, new_bracketed)
            if rep_changed:
                reports_changed[rid] = rep_changed
                reports_before[rid] = rep_before
                reports_after[rid] = rep_after_def.to_api()
                total_report_hits += rep_hits

        risk = _risk_score(sm_hits, total_report_hits)
        impact = RenameImpact(
            semantic_model_parts_changed=sm_changed,
            reports_changed=reports_changed,
            risk=risk,
            notes=notes,
        )

        plan_doc = {
            "operation_id": op_id,
            "ts": int(time.time()),
            "workspace_id": workspace_id,
            "semantic_model_id": semantic_model_id,
            "old": old_bracketed,
            "new": new_bracketed,
            "report_ids_requested": report_ids,
            "report_format": report_format,
            "semantic_model_format": semantic_model_format,
            "impact": {
                "semantic_model_parts_changed": sm_changed,
                "reports_changed": reports_changed,
                "risk": risk,
                "notes": notes,
            },
            "approval": {
                "status": "PENDING",
                "approved_by": None,
                "comment": None,
                "approved_ts": None,
            },
        }

        return op_id, impact, plan_doc, sm_before, sm_after, reports_before, reports_after

    async def plan(
        self,
        workspace_id: str,
        semantic_model_id: str,
        old_bracketed: str,
        new_bracketed: str,
        operation_id: str | None = None,
        *,
        report_ids: list[str] | None = None,
        report_format: str = "PBIR",
        semantic_model_format: str = "TMDL",
    ) -> tuple[str, RenameImpact, dict]:
        op_id, impact, plan_doc, *_ = await self.plan_artifacts(
            workspace_id,
            semantic_model_id,
            old_bracketed,
            new_bracketed,
            report_ids=report_ids,
            report_format=report_format,
            semantic_model_format=semantic_model_format,
            operation_id=operation_id,
        )
        return op_id, impact, plan_doc

    async def apply(
        self,
        workspace_id: str,
        semantic_model_id: str,
        old_bracketed: str,
        new_bracketed: str,
        *,
        report_ids: list[str] | None = None,
        report_format: str = "PBIR",
        semantic_model_format: str = "TMDL",
        require_changes: bool = True,
        approval: dict | None = None,
        operation_id: str | None = None,
    ) -> str:
        op_id, impact, plan_doc, sm_before, sm_after, reports_before, reports_after = await self.plan_artifacts(
            workspace_id,
            semantic_model_id,
            old_bracketed,
            new_bracketed,
            report_ids=report_ids,
            report_format=report_format,
            semantic_model_format=semantic_model_format,
            operation_id=operation_id,
        )

        if approval is not None:
            plan_doc["approval"] = approval

        if require_changes and not impact.semantic_model_parts_changed and not impact.reports_changed:
            raise ValueError("No changes detected; aborting (set require_changes=False to force)")

        # Execute: reports first, then model
        results: dict = {"reports": {}, "semantic_model": None}
        for rid, after_api in reports_after.items():
            logger.info("Updating report definition: {}", rid)
            results["reports"][rid] = await self._c.update_report_definition(workspace_id, rid, after_api)

        logger.info("Updating semantic model definition: {}", semantic_model_id)
        results["semantic_model"] = await self._c.update_semantic_model_definition(
            workspace_id, semantic_model_id, sm_after
        )

        # Persist op record
        op_path = OPS_DIR / f"{op_id}.json"
        op_record = {
            "operation_id": op_id,
            "plan": plan_doc,
            "before": {"semantic_model": sm_before, "reports": reports_before},
            "after": {"semantic_model": sm_after, "reports": reports_after},
            "results": results,
        }
        op_path.write_text(json.dumps(op_record, indent=2), encoding="utf-8")
        logger.success("Safe rename applied. Operation log: {}", op_path.as_posix())
        return op_id

    async def rollback(self, operation_id: str) -> None:
        op_path = OPS_DIR / f"{operation_id}.json"
        if not op_path.exists():
            raise FileNotFoundError(f"Operation not found: {operation_id}")

        record = json.loads(op_path.read_text(encoding="utf-8"))
        plan = record["plan"]
        ws = plan["workspace_id"]
        sm_id = plan["semantic_model_id"]
        before = record["before"]
        report_before: dict[str, dict] = before.get("reports", {})

        # Roll back reports first
        for rid, defn in report_before.items():
            logger.info("Rolling back report: {}", rid)
            await self._c.update_report_definition(ws, rid, defn)

        logger.info("Rolling back semantic model: {}", sm_id)
        await self._c.update_semantic_model_definition(ws, sm_id, before["semantic_model"])
        logger.success("Rollback completed for {}", operation_id)
