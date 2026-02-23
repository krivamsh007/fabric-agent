"""
Refactor Module
===============

Safe refactoring operations for Microsoft Fabric semantic models.

Components:
- RefactorExecutor: Executes actual changes via SemPy TOM
- RefactorOrchestrator: Coordinates multi-step refactoring
"""

from fabric_agent.refactor.executor import (
    RefactorExecutor,
    ExecutionResult,
    ExecutionStatus,
    ChangeRecord,
    Checkpoint,
    MeasureState,
    quick_rename,
)

__all__ = [
    "RefactorExecutor",
    "ExecutionResult",
    "ExecutionStatus",
    "ChangeRecord",
    "Checkpoint",
    "MeasureState",
    "quick_rename",
]
