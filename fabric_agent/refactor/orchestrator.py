"""
Refactor Orchestration Layer
============================

Implements:
- Persistent JSON audit log at ./memory/refactor_log.json (in addition to MemoryManager)
- DAX measure dependency mapping via SemPy (with TMDL fallback for dry-runs)
- Transactional smart_rename_measure (rename + update referencing measures)
- Rollback by operation_id using the persisted JSON log
"""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from uuid import uuid4

from loguru import logger

# Local audit trail
from fabric_agent.storage.memory_manager import MemoryManager, OperationStatus, StateType


# =============================================================================
# Persistent JSON log (memory/refactor_log.json)
# =============================================================================

class RefactorJsonLog:
    """
    Append-only JSON log stored at ./memory/refactor_log.json.

    Format:
      {
        "version": 1,
        "entries": [
          { ... },
          ...
        ]
      }
    """

    def __init__(self, path: Path | str = Path("./memory/refactor_log.json")) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.write_text(json.dumps({"version": 1, "entries": []}, indent=2), encoding="utf-8")

    def append(self, entry: Dict[str, Any]) -> None:
        """Append a single entry, preserving existing entries."""
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or "entries" not in payload:
                payload = {"version": 1, "entries": []}
        except Exception:
            payload = {"version": 1, "entries": []}

        payload.setdefault("version", 1)
        payload.setdefault("entries", [])
        payload["entries"].append(entry)

        # Write pretty-printed for repo friendliness
        self.path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    def list_by_operation(self, operation_id: str) -> List[Dict[str, Any]]:
        """Return entries for an operation_id in original order."""
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            entries = payload.get("entries", [])
            if not isinstance(entries, list):
                return []
            return [e for e in entries if str(e.get("operation_id")) == str(operation_id)]
        except Exception:
            return []


# =============================================================================
# Internal data models
# =============================================================================

@dataclass(frozen=True)
class MeasureDef:
    table: str
    name: str
    expression: str


@dataclass(frozen=True)
class PlannedChange:
    seq: int
    target: str            # measure name
    change_type: str       # rename_measure | update_expression
    old_value: str
    new_value: str
    reasoning: str


@dataclass
class RenamePlan:
    operation_id: str
    workspace_name: str
    model_name: str
    old_name: str
    new_name: str
    referenced_by: List[str]
    changes: List[PlannedChange]


# =============================================================================
# Dependency helpers (SemPy-first)
# =============================================================================

def _try_import_sempy():
    try:
        import sempy.fabric as fabric  # type: ignore
        return fabric
    except Exception:
        return None


def _find_measure_obj(tom_model: Any, measure_name: str) -> Any:
    """
    Find a TOM measure object by name by scanning all tables.
    Works with SemPy TOM wrapper objects.
    """
    for tbl in getattr(tom_model, "Tables", []):
        for m in getattr(tbl, "Measures", []):
            if getattr(m, "Name", None) == measure_name:
                return m
    raise KeyError(f"Measure not found: {measure_name}")


def _list_measures_from_tom(tom_model: Any) -> List[MeasureDef]:
    out: List[MeasureDef] = []
    for tbl in getattr(tom_model, "Tables", []):
        tbl_name = getattr(tbl, "Name", "Unknown")
        for m in getattr(tbl, "Measures", []):
            out.append(
                MeasureDef(
                    table=tbl_name,
                    name=str(getattr(m, "Name", "")),
                    expression=str(getattr(m, "Expression", "")) or "",
                )
            )
    return out


def _replace_measure_refs(expression: str, old_name: str, new_name: str) -> str:
    """
    Replace DAX measure references: [Old] -> [New].
    This avoids partial replacements by matching bracketed identifiers.
    """
    # Measure references are typically unqualified: [Measure Name]
    # Use negative lookbehind/ahead to enforce exact bracket match.
    pattern = r"\[" + re.escape(old_name) + r"\]"
    return re.sub(pattern, f"[{new_name}]", expression)


# =============================================================================
# TMDL fallback (dry-run only, no SemPy)
# =============================================================================

def extract_measures_from_tmdl(definition: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract measure_name -> expression from Fabric semantic model definition (TMDL parts).
    This mirrors the parser used in safety_refactor.py but is kept independent.
    """
    measures: Dict[str, str] = {}
    parts = (definition.get("definition") or {}).get("parts", [])
    for part in parts:
        path = str(part.get("path", ""))
        if not path.endswith(".tmdl"):
            continue
        if str(part.get("payloadType", "")).lower() != "inlinebase64":
            continue
        try:
            payload = part.get("payload", "")
            text = base64.b64decode(payload).decode("utf-8", errors="replace")
        except Exception:
            continue

        current_name: Optional[str] = None
        expr_lines: List[str] = []

        for line in text.splitlines():
            m = re.match(r"\s*measure\s+['\"]?(.+?)['\"]?\s*=", line, flags=re.IGNORECASE)
            if m:
                if current_name and expr_lines:
                    measures[current_name] = "\n".join(expr_lines).strip()
                current_name = m.group(1).strip()
                expr_lines = [line.split("=", 1)[-1].strip()]
                continue

            if current_name:
                # End of measure definition
                if line.strip().startswith(("formatString:", "displayFolder:", "description:")):
                    if expr_lines:
                        measures[current_name] = "\n".join(expr_lines).strip()
                    current_name = None
                    expr_lines = []
                elif line.strip() and not line.strip().startswith("//"):
                    expr_lines.append(line)

        if current_name and expr_lines:
            measures[current_name] = "\n".join(expr_lines).strip()

    return measures


def build_dependency_graph_regex(measures: Dict[str, str]) -> Dict[str, List[str]]:
    """
    Best-effort dependency graph without SemPy.
    For each measure A, find measures it references via bracket tokens [X].
    """
    names = sorted(measures.keys(), key=len, reverse=True)
    graph: Dict[str, List[str]] = {n: [] for n in measures.keys()}

    for m_name, expr in measures.items():
        deps: List[str] = []
        # Tokenize bracket references
        refs = set(re.findall(r"\[([^\]]+)\]", expr or ""))
        for ref in refs:
            if ref in measures and ref != m_name:
                deps.append(ref)
        graph[m_name] = sorted(deps)
    return graph


# =============================================================================
# Orchestrator
# =============================================================================

class RefactorOrchestrator:
    """
    High-level operations that coordinate SemPy, logging, and MemoryManager.

    Notes:
    - Applying changes requires SemPy with XMLA write enabled.
    - Dry-runs can fall back to TMDL parsing if SemPy is unavailable.
    """

    def __init__(
        self,
        memory: MemoryManager,
        json_log: Optional[RefactorJsonLog] = None,
    ) -> None:
        self.memory = memory
        self.json_log = json_log or RefactorJsonLog()

    def plan_smart_rename(
        self,
        *,
        workspace_name: str,
        model_name: str,
        old_name: str,
        new_name: str,
        include_indirect: bool = False,
        definition_fallback: Optional[Dict[str, Any]] = None,
    ) -> RenamePlan:
        """
        Build a rename plan (no changes applied).

        When SemPy is available, uses ModelCalcDependencies for accurate references.
        Otherwise (dry-run only) can use TMDL fallback via definition_fallback.
        """
        operation_id = str(uuid4())
        fabric = _try_import_sempy()

        referenced_by: List[str] = []
        planned: List[PlannedChange] = []

        if fabric:
            try:
                # Connect read-only for planning
                with fabric.connect_semantic_model(model_name, readonly=True, workspace=workspace_name) as tom:
                    tom_model = getattr(tom, "model", tom)  # some wrappers expose .model
                    # Ensure measure exists & new does not
                    _find_measure_obj(tom_model, old_name)
                    try:
                        _find_measure_obj(tom_model, new_name)
                        raise ValueError(f"Target name already exists: {new_name}")
                    except KeyError:
                        pass

                    # Dependencies
                    deps_obj = None
                    if hasattr(fabric, "get_model_calc_dependencies"):
                        deps_obj = fabric.get_model_calc_dependencies(model_name, workspace=workspace_name)
                    # If deps_obj has helper methods, use them
                    if deps_obj and hasattr(deps_obj, "all_measure_references"):
                        old_obj = _find_measure_obj(tom_model, old_name)
                        refs_iter = deps_obj.all_measure_references(old_obj)
                        referenced_by = sorted({getattr(m, "Name", str(m)) for m in refs_iter})
                    else:
                        # Fallback within SemPy: scan expressions
                        all_measures = _list_measures_from_tom(tom_model)
                        referenced_by = sorted(
                            m.name for m in all_measures
                            if m.name != old_name and f"[{old_name}]" in (m.expression or "")
                        )

                    seq = 1

                    # Child updates first (expressions)
                    for child in referenced_by:
                        child_obj = _find_measure_obj(tom_model, child)
                        old_expr = str(getattr(child_obj, "Expression", "") or "")
                        new_expr = _replace_measure_refs(old_expr, old_name, new_name)
                        if new_expr != old_expr:
                            planned.append(
                                PlannedChange(
                                    seq=seq,
                                    target=child,
                                    change_type="update_expression",
                                    old_value=old_expr,
                                    new_value=new_expr,
                                    reasoning=f"Update reference [{old_name}] -> [{new_name}] to keep measure valid after rename.",
                                )
                            )
                            seq += 1

                    # Finally the rename
                    planned.append(
                        PlannedChange(
                            seq=seq,
                            target=old_name,
                            change_type="rename_measure",
                            old_value=old_name,
                            new_value=new_name,
                            reasoning="Rename the measure identifier.",
                        )
                    )

            except Exception as e:
                # If SemPy path failed but fallback definition provided, continue below
                logger.debug(f"SemPy planning failed, attempting fallback: {e}")
                fabric = None

        if not fabric:
            if not definition_fallback:
                raise RuntimeError(
                    "SemPy is not available for dependency mapping. "
                    "Provide a semantic model definition for dry-run planning."
                )
            measures = extract_measures_from_tmdl(definition_fallback)
            if old_name not in measures:
                raise ValueError(f"Measure not found in TMDL: {old_name}")
            if new_name in measures:
                raise ValueError(f"Target name already exists: {new_name}")

            graph = build_dependency_graph_regex(measures)
            # referenced_by = measures that depend on old_name
            referenced_by = sorted([m for m, deps in graph.items() if old_name in deps])

            seq = 1
            for child in referenced_by:
                old_expr = measures.get(child, "")
                new_expr = _replace_measure_refs(old_expr, old_name, new_name)
                if new_expr != old_expr:
                    planned.append(
                        PlannedChange(
                            seq=seq,
                            target=child,
                            change_type="update_expression",
                            old_value=old_expr,
                            new_value=new_expr,
                            reasoning=f"Update reference [{old_name}] -> [{new_name}] to keep measure valid after rename.",
                        )
                    )
                    seq += 1

            planned.append(
                PlannedChange(
                    seq=seq,
                    target=old_name,
                    change_type="rename_measure",
                    old_value=old_name,
                    new_value=new_name,
                    reasoning="Rename the measure identifier.",
                )
            )

        return RenamePlan(
            operation_id=operation_id,
            workspace_name=workspace_name,
            model_name=model_name,
            old_name=old_name,
            new_name=new_name,
            referenced_by=referenced_by,
            changes=planned,
        )

    async def apply_smart_rename(
        self,
        *,
        workspace_id: str,
        workspace_name: str,
        model_name: str,
        old_name: str,
        new_name: str,
        reasoning: str,
        dry_run: bool,
        definition_fallback: Optional[Dict[str, Any]] = None,
        user: Optional[str] = None,
    ) -> Tuple[RenamePlan, List[str]]:
        """
        Plan and optionally apply the smart rename operation.

        Returns:
          (plan, snapshot_ids)

        snapshot_ids is a chain (parent_id links) so the first id is the "transaction root".
        """
        plan = self.plan_smart_rename(
            workspace_name=workspace_name,
            model_name=model_name,
            old_name=old_name,
            new_name=new_name,
            definition_fallback=definition_fallback,
        )

        ts = datetime.now(timezone.utc).isoformat()
        snapshot_ids: List[str] = []
        parent_id: Optional[str] = None

        # Log planning as the first entry (helps auditing even on dry-runs)
        self.json_log.append(
            {
                "timestamp": ts,
                "operation_id": plan.operation_id,
                "action": "plan",
                "target_type": "measure",
                "workspace_name": workspace_name,
                "model_name": model_name,
                "old_value": old_name,
                "new_value": new_name,
                "reasoning": reasoning,
                "details": {
                    "referenced_by": plan.referenced_by,
                    "planned_changes": len(plan.changes),
                    "dry_run": dry_run,
                },
            }
        )

        # Persist a root snapshot
        root = await self.memory.record_state(
            state_type=StateType.REFACTOR,
            operation="smart_rename_measure",
            status=OperationStatus.SUCCESS if dry_run else OperationStatus.PENDING,
            workspace_id=workspace_id,
            workspace_name=workspace_name,
            target_name=old_name,
            old_value=old_name,
            new_value=new_name,
            state_data={
                "operation_id": plan.operation_id,
                "model_name": model_name,
                "dry_run": dry_run,
                "referenced_by": plan.referenced_by,
                "planned_changes": [c.__dict__ for c in plan.changes],
            },
            metadata={"operation_id": plan.operation_id},
            parent_id=None,
            user=user,
        )
        snapshot_ids.append(root.id)
        parent_id = root.id

        if dry_run:
            return plan, snapshot_ids

        fabric = _try_import_sempy()
        if not fabric:
            raise RuntimeError(
                "SemPy is required to apply changes (install semantic-link-sempy "
                "and ensure XMLA write is enabled)."
            )

        # Apply changes with SemPy TOM
        try:
            with fabric.connect_semantic_model(model_name, readonly=False, workspace=workspace_name) as tom:
                tom_model = getattr(tom, "model", tom)

                # Apply expression updates first
                for change in plan.changes:
                    if change.change_type == "update_expression":
                        m_obj = _find_measure_obj(tom_model, change.target)
                        old_expr = str(getattr(m_obj, "Expression", "") or "")
                        setattr(m_obj, "Expression", change.new_value)

                        # Audit log entry
                        entry = {
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "operation_id": plan.operation_id,
                            "action": "update_expression",
                            "target_type": "measure",
                            "target_name": change.target,
                            "workspace_name": workspace_name,
                            "model_name": model_name,
                            "old_value": old_expr,
                            "new_value": change.new_value,
                            "reasoning": change.reasoning,
                        }
                        self.json_log.append(entry)

                        snap = await self.memory.record_state(
                            state_type=StateType.REFACTOR,
                            operation="update_measure_expression",
                            status=OperationStatus.SUCCESS,
                            workspace_id=workspace_id,
                            workspace_name=workspace_name,
                            target_name=change.target,
                            old_value=old_expr,
                            new_value=change.new_value,
                            state_data={"operation_id": plan.operation_id, "model_name": model_name},
                            metadata={"operation_id": plan.operation_id},
                            parent_id=parent_id,
                            user=user,
                        )
                        snapshot_ids.append(snap.id)
                        parent_id = snap.id

                # Now rename the measure
                old_obj = _find_measure_obj(tom_model, old_name)
                setattr(old_obj, "Name", new_name)

                entry = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "operation_id": plan.operation_id,
                    "action": "rename_measure",
                    "target_type": "measure",
                    "target_name": old_name,
                    "workspace_name": workspace_name,
                    "model_name": model_name,
                    "old_value": old_name,
                    "new_value": new_name,
                    "reasoning": "Rename the measure identifier.",
                }
                self.json_log.append(entry)

                snap = await self.memory.record_state(
                    state_type=StateType.REFACTOR,
                    operation="rename_measure",
                    status=OperationStatus.SUCCESS,
                    workspace_id=workspace_id,
                    workspace_name=workspace_name,
                    target_name=old_name,
                    old_value=old_name,
                    new_value=new_name,
                    state_data={"operation_id": plan.operation_id, "model_name": model_name},
                    metadata={"operation_id": plan.operation_id},
                    parent_id=parent_id,
                    user=user,
                )
                snapshot_ids.append(snap.id)

        except Exception as e:
            logger.exception("smart_rename_measure failed; attempting to mark audit trail error")
            # Mark failure in audit trail
            await self.memory.record_state(
                state_type=StateType.REFACTOR,
                operation="smart_rename_measure",
                status=OperationStatus.FAILED,
                workspace_id=workspace_id,
                workspace_name=workspace_name,
                target_name=old_name,
                old_value=old_name,
                new_value=new_name,
                error_message=str(e),
                state_data={"operation_id": plan.operation_id, "model_name": model_name},
                metadata={"operation_id": plan.operation_id},
                parent_id=parent_id,
                user=user,
            )
            raise

        # Final "commit" marker
        self.json_log.append(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "operation_id": plan.operation_id,
                "action": "commit",
                "target_type": "measure",
                "workspace_name": workspace_name,
                "model_name": model_name,
                "old_value": old_name,
                "new_value": new_name,
                "reasoning": reasoning,
                "details": {"snapshot_chain": snapshot_ids},
            }
        )

        return plan, snapshot_ids

    async def rollback_operation(
        self,
        *,
        operation_id: str,
        workspace_id: str,
        workspace_name: str,
        model_name: str,
        dry_run: bool = True,
        user: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Roll back a previously applied operation by operation_id using memory/refactor_log.json.

        Applies reverse changes in reverse order:
          - rename_measure: set Name back to old_value
          - update_expression: restore Expression to old_value
        """
        entries = self.json_log.list_by_operation(operation_id)
        # Keep only applied changes (exclude plan/commit)
        applied = [e for e in entries if e.get("action") in ("rename_measure", "update_expression")]
        if not applied:
            return {
                "success": False,
                "operation_id": operation_id,
                "dry_run": dry_run,
                "message": "No applied changes found for this operation_id.",
                "changes": [],
            }

        # Reverse order for rollback
        applied_rev = list(reversed(applied))
        changes_preview: List[Dict[str, Any]] = []
        for e in applied_rev:
            changes_preview.append(
                {
                    "action": "rollback_" + str(e.get("action")),
                    "target_name": e.get("target_name"),
                    "old_value": e.get("new_value"),
                    "new_value": e.get("old_value"),
                    "reasoning": "Rollback operation.",
                }
            )

        if dry_run:
            return {
                "success": True,
                "operation_id": operation_id,
                "dry_run": True,
                "message": "Rollback preview generated. Set dry_run=false to execute.",
                "changes": changes_preview,
            }

        fabric = _try_import_sempy()
        if not fabric:
            raise RuntimeError("SemPy is required to execute rollback.")

        with fabric.connect_semantic_model(model_name, readonly=False, workspace=workspace_name) as tom:
            tom_model = getattr(tom, "model", tom)

            for e in applied_rev:
                action = e.get("action")
                if action == "update_expression":
                    target = str(e.get("target_name"))
                    m_obj = _find_measure_obj(tom_model, target)
                    setattr(m_obj, "Expression", str(e.get("old_value") or ""))
                elif action == "rename_measure":
                    # After rename, the measure currently has new_value as name
                    current_name = str(e.get("new_value"))
                    original_name = str(e.get("old_value"))
                    m_obj = _find_measure_obj(tom_model, current_name)
                    setattr(m_obj, "Name", original_name)

        # Record rollback marker
        self.json_log.append(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "operation_id": operation_id,
                "action": "rollback_commit",
                "target_type": "measure",
                "workspace_name": workspace_name,
                "model_name": model_name,
                "reasoning": "Rollback committed.",
            }
        )

        await self.memory.record_state(
            state_type=StateType.REFACTOR,
            operation="rollback_operation",
            status=OperationStatus.SUCCESS,
            workspace_id=workspace_id,
            workspace_name=workspace_name,
            target_name=operation_id,
            state_data={"operation_id": operation_id, "model_name": model_name, "changes": changes_preview},
            metadata={"operation_id": operation_id},
            user=user,
        )

        return {
            "success": True,
            "operation_id": operation_id,
            "dry_run": False,
            "message": "Rollback executed successfully.",
            "changes": changes_preview,
        }


# =============================================================================
# Markdown formatting helpers
# =============================================================================

def format_markdown_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    """Create a GitHub-flavored Markdown table."""
    def esc(x: Any) -> str:
        s = "" if x is None else str(x)
        return s.replace("\n", "<br>").replace("|", "\\|")

    header_line = "| " + " | ".join(map(esc, headers)) + " |"
    sep_line = "| " + " | ".join(["---"] * len(headers)) + " |"
    row_lines = ["| " + " | ".join(esc(c) for c in r) + " |" for r in rows]
    return "\n".join([header_line, sep_line, *row_lines])
