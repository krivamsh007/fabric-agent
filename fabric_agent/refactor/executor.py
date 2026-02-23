"""
Refactor Executor - ACTUAL Implementation
==========================================

This module ACTUALLY executes refactoring operations in Microsoft Fabric
using SemPy's Tabular Object Model (TOM) connection.

This is the critical piece that was missing - real execution, not placeholders.

Requirements:
    - semantic-link-sempy >= 0.7.0
    - Running in Fabric notebook OR with proper authentication

Usage:
    executor = RefactorExecutor(workspace_name="my-workspace")
    
    # Rename a measure
    result = await executor.rename_measure(
        model_name="Sales_Model",
        old_name="Total Sales",
        new_name="Revenue",
        update_references=True,
        dry_run=False
    )
"""

from __future__ import annotations

import re
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from loguru import logger


# ============================================================================
# Data Classes
# ============================================================================

class ExecutionStatus(str, Enum):
    """Status of execution."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"
    DRY_RUN = "dry_run"


@dataclass
class MeasureState:
    """Captured state of a measure for rollback."""
    name: str
    table_name: str
    expression: str
    format_string: Optional[str] = None
    description: Optional[str] = None
    display_folder: Optional[str] = None
    is_hidden: bool = False


@dataclass
class ChangeRecord:
    """Record of a single change made."""
    change_id: str
    change_type: str  # rename_measure, update_expression
    target_name: str
    old_value: str
    new_value: str
    timestamp: str
    status: ExecutionStatus = ExecutionStatus.PENDING
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "change_id": self.change_id,
            "change_type": self.change_type,
            "target_name": self.target_name,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "timestamp": self.timestamp,
            "status": self.status.value,
            "error_message": self.error_message,
        }


@dataclass
class Checkpoint:
    """Checkpoint for rollback."""
    checkpoint_id: str
    created_at: str
    model_name: str
    workspace_name: str
    measure_states: Dict[str, MeasureState] = field(default_factory=dict)
    changes: List[ChangeRecord] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "checkpoint_id": self.checkpoint_id,
            "created_at": self.created_at,
            "model_name": self.model_name,
            "workspace_name": self.workspace_name,
            "measure_states": {k: vars(v) for k, v in self.measure_states.items()},
            "changes": [c.to_dict() for c in self.changes],
        }


@dataclass
class ExecutionResult:
    """Result of a refactor execution."""
    success: bool
    operation_id: str
    operation_type: str
    model_name: str
    workspace_name: str
    dry_run: bool
    checkpoint_id: Optional[str] = None
    changes: List[ChangeRecord] = field(default_factory=list)
    affected_measures: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    execution_time_ms: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "operation_id": self.operation_id,
            "operation_type": self.operation_type,
            "model_name": self.model_name,
            "workspace_name": self.workspace_name,
            "dry_run": self.dry_run,
            "checkpoint_id": self.checkpoint_id,
            "changes": [c.to_dict() for c in self.changes],
            "affected_measures": self.affected_measures,
            "error_message": self.error_message,
            "execution_time_ms": self.execution_time_ms,
        }


# ============================================================================
# SemPy Integration Helpers
# ============================================================================

def _try_import_sempy():
    """Try to import SemPy. Returns None if not available."""
    try:
        import sempy.fabric as fabric
        return fabric
    except ImportError:
        logger.warning("SemPy not installed. Install with: pip install semantic-link-sempy")
        return None


def _find_measure_in_model(tom_model: Any, measure_name: str) -> Tuple[Any, Any]:
    """
    Find a measure by name in the TOM model.
    
    Returns:
        Tuple of (table, measure) or raises KeyError if not found.
    """
    for table in getattr(tom_model, "Tables", []):
        for measure in getattr(table, "Measures", []):
            if getattr(measure, "Name", None) == measure_name:
                return table, measure
    raise KeyError(f"Measure not found: {measure_name}")


def _get_all_measures(tom_model: Any) -> List[Tuple[str, str, str]]:
    """
    Get all measures from the model.
    
    Returns:
        List of (table_name, measure_name, expression) tuples.
    """
    measures = []
    for table in getattr(tom_model, "Tables", []):
        table_name = getattr(table, "Name", "Unknown")
        for measure in getattr(table, "Measures", []):
            measures.append((
                table_name,
                getattr(measure, "Name", ""),
                getattr(measure, "Expression", "") or ""
            ))
    return measures


def _find_referencing_measures(
    tom_model: Any,
    target_name: str
) -> List[Tuple[str, str, str]]:
    """
    Find all measures that reference the target measure.
    
    Returns:
        List of (table_name, measure_name, expression) for measures that reference target.
    """
    referencing = []
    pattern = rf"\[{re.escape(target_name)}\]"
    
    for table_name, measure_name, expression in _get_all_measures(tom_model):
        if measure_name == target_name:
            continue  # Skip the target itself
        
        if re.search(pattern, expression, re.IGNORECASE):
            referencing.append((table_name, measure_name, expression))
    
    return referencing


def _update_dax_references(
    expression: str,
    old_name: str,
    new_name: str
) -> str:
    """
    Update DAX expression to replace measure references.
    
    Replaces [OldName] with [NewName] in DAX expressions.
    """
    pattern = rf"\[{re.escape(old_name)}\]"
    return re.sub(pattern, f"[{new_name}]", expression, flags=re.IGNORECASE)


# ============================================================================
# Main Executor Class
# ============================================================================

class RefactorExecutor:
    """
    Executes refactoring operations against Microsoft Fabric semantic models.
    
    This class provides ACTUAL execution capabilities using SemPy's TOM connection.
    It supports:
    - Measure renaming with automatic reference updates
    - DAX expression modifications
    - Checkpoint creation for rollback
    - Full rollback to previous state
    
    Example:
        >>> executor = RefactorExecutor(workspace_name="Sales_Workspace")
        >>> 
        >>> # Dry run first
        >>> result = await executor.rename_measure(
        ...     model_name="Sales_Model",
        ...     old_name="Total Sales",
        ...     new_name="Revenue",
        ...     dry_run=True
        ... )
        >>> print(f"Would affect {len(result.affected_measures)} measures")
        >>> 
        >>> # Execute for real
        >>> result = await executor.rename_measure(
        ...     model_name="Sales_Model",
        ...     old_name="Total Sales",
        ...     new_name="Revenue",
        ...     dry_run=False
        ... )
        >>> 
        >>> # Rollback if needed
        >>> await executor.rollback(result.checkpoint_id)
    """
    
    def __init__(
        self,
        workspace_name: str,
        memory_manager: Optional[Any] = None,
    ):
        """
        Initialize the executor.
        
        Args:
            workspace_name: Name of the Fabric workspace.
            memory_manager: Optional MemoryManager for audit trail.
        """
        self.workspace_name = workspace_name
        self.memory = memory_manager
        self._checkpoints: Dict[str, Checkpoint] = {}
        self._fabric = _try_import_sempy()
        
        if not self._fabric:
            logger.warning(
                "SemPy not available. Execution will fail. "
                "Install with: pip install semantic-link-sempy"
            )
    
    def _ensure_sempy(self) -> None:
        """Ensure SemPy is available."""
        if not self._fabric:
            self._fabric = _try_import_sempy()
            if not self._fabric:
                raise RuntimeError(
                    "SemPy is required for refactor execution. "
                    "Install with: pip install semantic-link-sempy"
                )
    
    async def rename_measure(
        self,
        model_name: str,
        old_name: str,
        new_name: str,
        update_references: bool = True,
        dry_run: bool = True,
        reasoning: Optional[str] = None,
    ) -> ExecutionResult:
        """
        Rename a measure and optionally update all references.
        
        This is the HERO function - it actually performs the rename!
        
        Args:
            model_name: Name of the semantic model.
            old_name: Current measure name.
            new_name: New measure name.
            update_references: If True, update all DAX expressions that reference this measure.
            dry_run: If True, only simulate (no actual changes).
            reasoning: Reason for the change (stored in audit log).
        
        Returns:
            ExecutionResult with details of the operation.
        """
        import time
        start_time = time.time()
        
        operation_id = str(uuid4())
        changes: List[ChangeRecord] = []
        affected_measures: List[str] = []
        checkpoint_id: Optional[str] = None
        
        logger.info(
            f"{'[DRY RUN] ' if dry_run else ''}Renaming measure: "
            f"{model_name}.[{old_name}] -> [{new_name}]"
        )
        
        try:
            self._ensure_sempy()
            
            # Connect to the semantic model
            with self._fabric.connect_semantic_model(
                dataset=model_name,
                readonly=dry_run,
                workspace=self.workspace_name
            ) as tom:
                tom_model = getattr(tom, "model", tom)
                
                # 1. Find the target measure
                try:
                    table, measure = _find_measure_in_model(tom_model, old_name)
                except KeyError:
                    return ExecutionResult(
                        success=False,
                        operation_id=operation_id,
                        operation_type="rename_measure",
                        model_name=model_name,
                        workspace_name=self.workspace_name,
                        dry_run=dry_run,
                        error_message=f"Measure not found: {old_name}",
                        execution_time_ms=int((time.time() - start_time) * 1000),
                    )
                
                # 2. Find all referencing measures
                referencing = _find_referencing_measures(tom_model, old_name)
                affected_measures = [m[1] for m in referencing]
                
                logger.info(f"Found {len(referencing)} measures that reference [{old_name}]")
                
                # 3. Create checkpoint (before making changes)
                if not dry_run:
                    checkpoint = await self._create_checkpoint(
                        tom_model, model_name, old_name, referencing
                    )
                    checkpoint_id = checkpoint.checkpoint_id
                
                # 4. Update referencing measures first (if requested)
                if update_references:
                    for ref_table, ref_measure_name, ref_expression in referencing:
                        new_expression = _update_dax_references(
                            ref_expression, old_name, new_name
                        )
                        
                        change = ChangeRecord(
                            change_id=str(uuid4()),
                            change_type="update_expression",
                            target_name=ref_measure_name,
                            old_value=ref_expression[:500],
                            new_value=new_expression[:500],
                            timestamp=datetime.now(timezone.utc).isoformat(),
                            status=ExecutionStatus.DRY_RUN if dry_run else ExecutionStatus.PENDING,
                        )
                        
                        if not dry_run:
                            _, ref_measure_obj = _find_measure_in_model(tom_model, ref_measure_name)
                            setattr(ref_measure_obj, "Expression", new_expression)
                            change.status = ExecutionStatus.SUCCESS
                            logger.info(f"Updated expression in [{ref_measure_name}]")
                        
                        changes.append(change)
                
                # 5. Rename the measure itself
                rename_change = ChangeRecord(
                    change_id=str(uuid4()),
                    change_type="rename_measure",
                    target_name=old_name,
                    old_value=old_name,
                    new_value=new_name,
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    status=ExecutionStatus.DRY_RUN if dry_run else ExecutionStatus.PENDING,
                )
                
                if not dry_run:
                    setattr(measure, "Name", new_name)
                    rename_change.status = ExecutionStatus.SUCCESS
                    logger.info(f"Renamed measure [{old_name}] -> [{new_name}]")
                
                changes.append(rename_change)
                
                # 6. Save checkpoint with changes
                if checkpoint_id and not dry_run:
                    self._checkpoints[checkpoint_id].changes = changes
            
            # 7. Record to audit trail
            if self.memory and not dry_run:
                await self._record_to_audit(
                    operation_id=operation_id,
                    operation_type="rename_measure",
                    model_name=model_name,
                    old_name=old_name,
                    new_name=new_name,
                    changes=changes,
                    checkpoint_id=checkpoint_id,
                    reasoning=reasoning,
                )
            
            execution_time = int((time.time() - start_time) * 1000)
            
            return ExecutionResult(
                success=True,
                operation_id=operation_id,
                operation_type="rename_measure",
                model_name=model_name,
                workspace_name=self.workspace_name,
                dry_run=dry_run,
                checkpoint_id=checkpoint_id,
                changes=changes,
                affected_measures=affected_measures,
                execution_time_ms=execution_time,
            )
            
        except Exception as e:
            logger.error(f"Rename failed: {e}")
            return ExecutionResult(
                success=False,
                operation_id=operation_id,
                operation_type="rename_measure",
                model_name=model_name,
                workspace_name=self.workspace_name,
                dry_run=dry_run,
                checkpoint_id=checkpoint_id,
                changes=changes,
                error_message=str(e),
                execution_time_ms=int((time.time() - start_time) * 1000),
            )
    
    async def rollback(
        self,
        checkpoint_id: str,
        dry_run: bool = True,
    ) -> ExecutionResult:
        """
        Rollback to a previous checkpoint.
        
        Args:
            checkpoint_id: ID of the checkpoint to rollback to.
            dry_run: If True, only simulate the rollback.
        
        Returns:
            ExecutionResult with rollback details.
        """
        import time
        start_time = time.time()
        
        operation_id = str(uuid4())
        
        checkpoint = self._checkpoints.get(checkpoint_id)
        if not checkpoint:
            return ExecutionResult(
                success=False,
                operation_id=operation_id,
                operation_type="rollback",
                model_name="",
                workspace_name=self.workspace_name,
                dry_run=dry_run,
                error_message=f"Checkpoint not found: {checkpoint_id}",
                execution_time_ms=int((time.time() - start_time) * 1000),
            )
        
        rollback_changes: List[ChangeRecord] = []
        
        try:
            self._ensure_sempy()
            
            with self._fabric.connect_semantic_model(
                dataset=checkpoint.model_name,
                readonly=dry_run,
                workspace=self.workspace_name
            ) as tom:
                tom_model = getattr(tom, "model", tom)
                
                for measure_name, state in checkpoint.measure_states.items():
                    try:
                        table, measure = _find_measure_in_model(tom_model, measure_name)
                        current_name = getattr(measure, "Name", "")
                    except KeyError:
                        # Try to find by new name
                        new_name = None
                        for change in checkpoint.changes:
                            if change.change_type == "rename_measure" and change.old_value == measure_name:
                                new_name = change.new_value
                                break
                        
                        if new_name:
                            try:
                                table, measure = _find_measure_in_model(tom_model, new_name)
                                current_name = new_name
                            except KeyError:
                                continue
                        else:
                            continue
                    
                    # Restore name
                    if current_name != state.name:
                        change = ChangeRecord(
                            change_id=str(uuid4()),
                            change_type="rollback_rename",
                            target_name=current_name,
                            old_value=current_name,
                            new_value=state.name,
                            timestamp=datetime.now(timezone.utc).isoformat(),
                            status=ExecutionStatus.DRY_RUN if dry_run else ExecutionStatus.SUCCESS,
                        )
                        
                        if not dry_run:
                            setattr(measure, "Name", state.name)
                        
                        rollback_changes.append(change)
                    
                    # Restore expression
                    current_expr = getattr(measure, "Expression", "") or ""
                    if current_expr != state.expression:
                        change = ChangeRecord(
                            change_id=str(uuid4()),
                            change_type="rollback_expression",
                            target_name=state.name,
                            old_value=current_expr[:500],
                            new_value=state.expression[:500],
                            timestamp=datetime.now(timezone.utc).isoformat(),
                            status=ExecutionStatus.DRY_RUN if dry_run else ExecutionStatus.SUCCESS,
                        )
                        
                        if not dry_run:
                            setattr(measure, "Expression", state.expression)
                        
                        rollback_changes.append(change)
            
            return ExecutionResult(
                success=True,
                operation_id=operation_id,
                operation_type="rollback",
                model_name=checkpoint.model_name,
                workspace_name=self.workspace_name,
                dry_run=dry_run,
                checkpoint_id=checkpoint_id,
                changes=rollback_changes,
                execution_time_ms=int((time.time() - start_time) * 1000),
            )
            
        except Exception as e:
            return ExecutionResult(
                success=False,
                operation_id=operation_id,
                operation_type="rollback",
                model_name=checkpoint.model_name,
                workspace_name=self.workspace_name,
                dry_run=dry_run,
                error_message=str(e),
                execution_time_ms=int((time.time() - start_time) * 1000),
            )
    
    async def _create_checkpoint(
        self,
        tom_model: Any,
        model_name: str,
        target_measure: str,
        referencing_measures: List[Tuple[str, str, str]],
    ) -> Checkpoint:
        """Create a checkpoint capturing current state."""
        checkpoint_id = str(uuid4())
        
        measure_states: Dict[str, MeasureState] = {}
        
        try:
            table, measure = _find_measure_in_model(tom_model, target_measure)
            measure_states[target_measure] = MeasureState(
                name=getattr(measure, "Name", ""),
                table_name=getattr(table, "Name", ""),
                expression=getattr(measure, "Expression", "") or "",
                format_string=getattr(measure, "FormatString", None),
                description=getattr(measure, "Description", None),
            )
        except KeyError:
            pass
        
        for table_name, measure_name, expression in referencing_measures:
            measure_states[measure_name] = MeasureState(
                name=measure_name,
                table_name=table_name,
                expression=expression,
            )
        
        checkpoint = Checkpoint(
            checkpoint_id=checkpoint_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            model_name=model_name,
            workspace_name=self.workspace_name,
            measure_states=measure_states,
        )
        
        self._checkpoints[checkpoint_id] = checkpoint
        logger.info(f"Created checkpoint {checkpoint_id}")
        
        return checkpoint
    
    async def _record_to_audit(
        self,
        operation_id: str,
        operation_type: str,
        model_name: str,
        old_name: str,
        new_name: str,
        changes: List[ChangeRecord],
        checkpoint_id: Optional[str],
        reasoning: Optional[str],
    ) -> None:
        """Record operation to audit trail."""
        if not self.memory:
            return
        
        from fabric_agent.storage.memory_manager import StateType, OperationStatus
        
        await self.memory.record_state(
            state_type=StateType.REFACTOR,
            operation=operation_type,
            status=OperationStatus.SUCCESS,
            workspace_name=self.workspace_name,
            target_name=old_name,
            old_value=old_name,
            new_value=new_name,
            state_data={
                "operation_id": operation_id,
                "model_name": model_name,
                "checkpoint_id": checkpoint_id,
                "changes": [c.to_dict() for c in changes],
                "reasoning": reasoning,
            },
        )
    
    def get_checkpoint(self, checkpoint_id: str) -> Optional[Checkpoint]:
        """Get a checkpoint by ID."""
        return self._checkpoints.get(checkpoint_id)
    
    def list_checkpoints(self) -> List[str]:
        """List all checkpoint IDs."""
        return list(self._checkpoints.keys())


# ============================================================================
# Convenience Functions
# ============================================================================

async def quick_rename(
    workspace_name: str,
    model_name: str,
    old_name: str,
    new_name: str,
    dry_run: bool = True,
) -> ExecutionResult:
    """
    Quick helper to rename a measure.
    
    Example:
        >>> result = await quick_rename(
        ...     workspace_name="Sales_Workspace",
        ...     model_name="Sales_Model",
        ...     old_name="Total Sales",
        ...     new_name="Revenue",
        ...     dry_run=False
        ... )
    """
    executor = RefactorExecutor(workspace_name=workspace_name)
    return await executor.rename_measure(
        model_name=model_name,
        old_name=old_name,
        new_name=new_name,
        update_references=True,
        dry_run=dry_run,
    )
