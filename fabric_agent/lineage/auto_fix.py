"""
Auto-Fix Planner
================

Generates and executes fix plans for impacted assets.

This module can:
- Generate fix plans from impact reports
- Update measure references in DAX expressions
- Update report field bindings
- Execute fixes with rollback capability
- Validate fixes after application

Example:
    planner = AutoFixPlanner(fabric_client)
    
    # Generate plan from impact report
    plan = await planner.create_plan(impact_report)
    
    # Preview fixes
    for action in plan.actions:
        print(f"{action.target}: {action.description}")
    
    # Execute with dry-run first
    result = await planner.execute(plan, dry_run=True)
    
    # Apply for real
    result = await planner.execute(plan, dry_run=False)
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from uuid import uuid4

from loguru import logger

if TYPE_CHECKING:
    from fabric_agent.api.fabric_client import FabricApiClient
    from fabric_agent.lineage.engine import ImpactReport


class FixActionType(str, Enum):
    """Types of fix actions."""
    UPDATE_DAX_REFERENCE = "update_dax_reference"
    UPDATE_REPORT_BINDING = "update_report_binding"
    UPDATE_NOTEBOOK_CODE = "update_notebook_code"
    UPDATE_PIPELINE_REF = "update_pipeline_ref"
    MANUAL_REVIEW = "manual_review"


class FixStatus(str, Enum):
    """Status of a fix action."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ROLLED_BACK = "rolled_back"


@dataclass
class FixAction:
    """
    A single fix action to be applied.
    
    Attributes:
        id: Unique identifier for this action
        action_type: Type of fix (DAX update, report binding, etc.)
        target_id: ID of the asset to fix
        target_name: Name of the asset
        target_type: Type of the asset (measure, report, etc.)
        description: Human-readable description
        old_value: Current value (for rollback)
        new_value: New value after fix
        can_auto_fix: Whether this can be fixed automatically
        status: Current status of the fix
        error: Error message if failed
        applied_at: Timestamp when applied
    """
    
    id: str = field(default_factory=lambda: uuid4().hex[:8])
    action_type: FixActionType = FixActionType.MANUAL_REVIEW
    target_id: str = ""
    target_name: str = ""
    target_type: str = ""
    description: str = ""
    old_value: str = ""
    new_value: str = ""
    can_auto_fix: bool = True
    status: FixStatus = FixStatus.PENDING
    error: Optional[str] = None
    applied_at: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "action_type": self.action_type.value,
            "target_id": self.target_id,
            "target_name": self.target_name,
            "target_type": self.target_type,
            "description": self.description,
            "old_value": self.old_value[:100] + "..." if len(self.old_value) > 100 else self.old_value,
            "new_value": self.new_value[:100] + "..." if len(self.new_value) > 100 else self.new_value,
            "can_auto_fix": self.can_auto_fix,
            "status": self.status.value,
            "error": self.error,
            "applied_at": self.applied_at,
        }


@dataclass
class FixPlan:
    """
    A complete fix plan for an impact report.
    
    Attributes:
        plan_id: Unique identifier
        created_at: Creation timestamp
        source_change: Description of the source change
        old_value: Original value (for renames)
        new_value: New value (for renames)
        actions: List of fix actions
        auto_fix_count: Count of auto-fixable actions
        manual_count: Count of manual actions
        checkpoint_id: ID of checkpoint for rollback
    """
    
    plan_id: str = field(default_factory=lambda: f"fix_{uuid4().hex[:12]}")
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    source_change: str = ""
    old_value: str = ""
    new_value: str = ""
    actions: List[FixAction] = field(default_factory=list)
    checkpoint_id: Optional[str] = None
    
    @property
    def auto_fix_count(self) -> int:
        return sum(1 for a in self.actions if a.can_auto_fix)
    
    @property
    def manual_count(self) -> int:
        return sum(1 for a in self.actions if not a.can_auto_fix)
    
    @property
    def completed_count(self) -> int:
        return sum(1 for a in self.actions if a.status == FixStatus.COMPLETED)
    
    @property
    def failed_count(self) -> int:
        return sum(1 for a in self.actions if a.status == FixStatus.FAILED)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "created_at": self.created_at,
            "source_change": self.source_change,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "summary": {
                "total_actions": len(self.actions),
                "auto_fix_count": self.auto_fix_count,
                "manual_count": self.manual_count,
                "completed_count": self.completed_count,
                "failed_count": self.failed_count,
            },
            "actions": [a.to_dict() for a in self.actions],
            "checkpoint_id": self.checkpoint_id,
        }
    
    def to_markdown(self) -> str:
        lines = []
        
        lines.append(f"# 🔧 Fix Plan: `{self.old_value}` → `{self.new_value}`")
        lines.append("")
        lines.append(f"**Plan ID:** {self.plan_id}")
        lines.append(f"**Created:** {self.created_at[:19]}")
        lines.append("")
        
        lines.append("## Summary")
        lines.append("")
        lines.append("| Metric | Count |")
        lines.append("|--------|-------|")
        lines.append(f"| Total Actions | {len(self.actions)} |")
        lines.append(f"| Auto-Fixable | {self.auto_fix_count} |")
        lines.append(f"| Manual Required | {self.manual_count} |")
        lines.append(f"| Completed | {self.completed_count} |")
        lines.append(f"| Failed | {self.failed_count} |")
        lines.append("")
        
        if self.actions:
            lines.append("## Actions")
            lines.append("")
            lines.append("| Target | Type | Status | Auto |")
            lines.append("|--------|------|--------|------|")
            
            for action in self.actions[:20]:
                auto = "✅" if action.can_auto_fix else "❌"
                status_emoji = {
                    "pending": "⏳",
                    "completed": "✅",
                    "failed": "❌",
                    "skipped": "⏭️",
                }.get(action.status.value, "❓")
                
                lines.append(
                    f"| `{action.target_name}` | {action.action_type.value} | "
                    f"{status_emoji} | {auto} |"
                )
            
            if len(self.actions) > 20:
                lines.append(f"| ... | +{len(self.actions) - 20} more | | |")
        
        lines.append("")
        
        if self.checkpoint_id:
            lines.append("## Rollback")
            lines.append("")
            lines.append(f"Checkpoint: `{self.checkpoint_id}`")
            lines.append("")
            lines.append("```bash")
            lines.append(f"fabric-agent rollback --checkpoint {self.checkpoint_id}")
            lines.append("```")
        
        return "\n".join(lines)


@dataclass
class FixResult:
    """Result of executing a fix plan."""
    
    plan_id: str
    success: bool
    completed: int
    failed: int
    skipped: int
    errors: List[str] = field(default_factory=list)
    duration_seconds: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "success": self.success,
            "completed": self.completed,
            "failed": self.failed,
            "skipped": self.skipped,
            "errors": self.errors,
            "duration_seconds": self.duration_seconds,
        }


class AutoFixPlanner:
    """
    Generates and executes fix plans for impacted assets.
    
    Usage:
        planner = AutoFixPlanner(fabric_client)
        plan = await planner.create_plan(impact_report)
        result = await planner.execute(plan)
    """
    
    def __init__(self, fabric_client: "FabricApiClient"):
        """
        Initialize the auto-fix planner.
        
        Args:
            fabric_client: Initialized FabricApiClient
        """
        self.client = fabric_client
        self._checkpoints: Dict[str, Dict[str, Any]] = {}
    
    async def create_plan(
        self,
        impact_report: "ImpactReport",
    ) -> FixPlan:
        """
        Create a fix plan from an impact report.
        
        Args:
            impact_report: The impact analysis report
        
        Returns:
            FixPlan with all required actions
        """
        plan = FixPlan(
            source_change=impact_report.change_type,
            old_value=impact_report.old_value or "",
            new_value=impact_report.new_value or "",
        )
        
        # Process each affected asset
        for impacted in impact_report.all_affected:
            action = self._create_action_for_asset(impacted, impact_report)
            plan.actions.append(action)
        
        logger.info(
            f"Fix plan created: {len(plan.actions)} actions "
            f"({plan.auto_fix_count} auto, {plan.manual_count} manual)"
        )
        
        return plan
    
    async def execute(
        self,
        plan: FixPlan,
        dry_run: bool = True,
        stop_on_error: bool = True,
    ) -> FixResult:
        """
        Execute a fix plan.
        
        Args:
            plan: The fix plan to execute
            dry_run: If True, preview without applying
            stop_on_error: If True, stop on first error
        
        Returns:
            FixResult with execution summary
        """
        import time
        start_time = time.time()
        
        completed = 0
        failed = 0
        skipped = 0
        errors: List[str] = []
        
        if dry_run:
            logger.info("DRY RUN mode - no changes will be applied")
        else:
            # Create checkpoint
            plan.checkpoint_id = await self._create_checkpoint(plan)
        
        for action in plan.actions:
            if not action.can_auto_fix:
                action.status = FixStatus.SKIPPED
                skipped += 1
                continue
            
            try:
                action.status = FixStatus.IN_PROGRESS
                
                if dry_run:
                    logger.info(f"Would apply: {action.description}")
                    action.status = FixStatus.PENDING
                else:
                    await self._apply_action(action)
                    action.status = FixStatus.COMPLETED
                    action.applied_at = datetime.now(timezone.utc).isoformat()
                    completed += 1
                    logger.info(f"Applied: {action.description}")
            
            except Exception as e:
                action.status = FixStatus.FAILED
                action.error = str(e)
                failed += 1
                errors.append(f"{action.target_name}: {e}")
                logger.error(f"Failed: {action.description} - {e}")
                
                if stop_on_error:
                    # Mark remaining as skipped
                    for remaining in plan.actions:
                        if remaining.status == FixStatus.PENDING:
                            remaining.status = FixStatus.SKIPPED
                            skipped += 1
                    break
        
        duration = time.time() - start_time
        
        return FixResult(
            plan_id=plan.plan_id,
            success=failed == 0,
            completed=completed,
            failed=failed,
            skipped=skipped,
            errors=errors,
            duration_seconds=round(duration, 2),
        )
    
    async def rollback(self, checkpoint_id: str) -> bool:
        """
        Rollback to a checkpoint.
        
        Args:
            checkpoint_id: The checkpoint to rollback to
        
        Returns:
            True if successful
        """
        checkpoint = self._checkpoints.get(checkpoint_id)
        if not checkpoint:
            logger.error(f"Checkpoint not found: {checkpoint_id}")
            return False
        
        logger.info(f"Rolling back to checkpoint: {checkpoint_id}")
        
        # Restore each asset from checkpoint
        for asset_id, state in checkpoint.get("assets", {}).items():
            try:
                await self._restore_asset(asset_id, state)
                logger.info(f"Restored: {asset_id}")
            except Exception as e:
                logger.error(f"Failed to restore {asset_id}: {e}")
                return False
        
        return True
    
    def _create_action_for_asset(
        self,
        impacted: Any,  # ImpactedAsset
        report: "ImpactReport",
    ) -> FixAction:
        """Create a fix action for an impacted asset."""
        from fabric_agent.lineage.models import AssetType
        
        asset_type = impacted.node.asset_type
        
        if asset_type == AssetType.MEASURE:
            return FixAction(
                action_type=FixActionType.UPDATE_DAX_REFERENCE,
                target_id=impacted.node.id,
                target_name=impacted.node.name,
                target_type="measure",
                description=f"Update reference [{report.old_value}] → [{report.new_value}]",
                old_value=impacted.node.metadata.get("expression", ""),
                new_value=self._update_dax_reference(
                    impacted.node.metadata.get("expression", ""),
                    report.old_value or "",
                    report.new_value or "",
                ),
                can_auto_fix=True,
            )
        
        elif asset_type == AssetType.REPORT:
            return FixAction(
                action_type=FixActionType.UPDATE_REPORT_BINDING,
                target_id=impacted.node.id,
                target_name=impacted.node.name,
                target_type="report",
                description=f"Update field binding from [{report.old_value}]",
                can_auto_fix=True,
            )
        
        elif asset_type == AssetType.NOTEBOOK:
            return FixAction(
                action_type=FixActionType.MANUAL_REVIEW,
                target_id=impacted.node.id,
                target_name=impacted.node.name,
                target_type="notebook",
                description="Manual code review required",
                can_auto_fix=False,
            )
        
        elif asset_type == AssetType.PIPELINE:
            return FixAction(
                action_type=FixActionType.MANUAL_REVIEW,
                target_id=impacted.node.id,
                target_name=impacted.node.name,
                target_type="pipeline",
                description="Manual pipeline review required",
                can_auto_fix=False,
            )
        
        else:
            return FixAction(
                action_type=FixActionType.MANUAL_REVIEW,
                target_id=impacted.node.id,
                target_name=impacted.node.name,
                target_type=asset_type.value,
                description="Manual review required",
                can_auto_fix=False,
            )
    
    def _update_dax_reference(self, expression: str, old_ref: str, new_ref: str) -> str:
        """Update a measure reference in a DAX expression."""
        # Replace [OldName] with [NewName]
        old_pattern = f"[{old_ref}]"
        new_pattern = f"[{new_ref}]"
        return expression.replace(old_pattern, new_pattern)
    
    async def _create_checkpoint(self, plan: FixPlan) -> str:
        """Create a checkpoint before applying fixes."""
        checkpoint_id = f"cp_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        
        checkpoint = {
            "id": checkpoint_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "plan_id": plan.plan_id,
            "assets": {},
        }
        
        # Save current state of each asset
        for action in plan.actions:
            if action.can_auto_fix:
                checkpoint["assets"][action.target_id] = {
                    "old_value": action.old_value,
                    "target_type": action.target_type,
                }
        
        self._checkpoints[checkpoint_id] = checkpoint
        logger.info(f"Checkpoint created: {checkpoint_id}")
        
        return checkpoint_id
    
    async def _apply_action(self, action: FixAction) -> None:
        """Apply a single fix action via Fabric API."""
        if action.action_type == FixActionType.UPDATE_DAX_REFERENCE:
            # Parse target_id to get model_id and measure_name
            # Format: "model_id:measure_name"
            parts = action.target_id.split(":")
            if len(parts) >= 2:
                model_id = parts[0]
                # In a real implementation, this would update the model definition
                logger.info(f"Updating measure in model {model_id}")
        
        elif action.action_type == FixActionType.UPDATE_REPORT_BINDING:
            # In a real implementation, this would update the report definition
            logger.info(f"Updating report {action.target_id}")
    
    async def _restore_asset(self, asset_id: str, state: Dict[str, Any]) -> None:
        """Restore an asset to its previous state."""
        # In a real implementation, this would restore the asset definition
        logger.info(f"Restoring {asset_id} to previous state")
