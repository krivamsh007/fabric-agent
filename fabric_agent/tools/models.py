"""
Tool Input/Output Models
========================

Pydantic models for validating MCP tool inputs and outputs.
All models include detailed docstrings and field descriptions.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ============================================================================
# Common Models
# ============================================================================

class ToolResult(BaseModel):
    """
    Base result model for all tool operations.
    
    Attributes:
        success: Whether the operation succeeded.
        error: Error message if failed.
        data: Operation-specific result data.
    """
    
    success: bool = Field(
        default=True,
        description="Whether the operation completed successfully",
    )
    error: Optional[str] = Field(
        default=None,
        description="Error message if the operation failed",
    )
    data: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Operation-specific result data",
    )


class ImpactSeverity(str, Enum):
    """Severity levels for impact analysis."""
    
    NONE = "none"
    """No impact detected."""
    
    LOW = "low"
    """1-3 items affected."""
    
    MEDIUM = "medium"
    """4-10 items affected."""
    
    HIGH = "high"
    """11-25 items affected."""
    
    CRITICAL = "critical"
    """More than 25 items affected."""


class TargetType(str, Enum):
    """Types of refactoring targets."""
    
    MEASURE = "measure"
    """DAX measure in a semantic model."""
    
    COLUMN = "column"
    """Column in a table."""
    
    TABLE = "table"
    """Entire table."""


# ============================================================================
# Workspace Tool Models
# ============================================================================

class ListWorkspacesInput(BaseModel):
    """
    Input for the list_workspaces tool.
    
    This tool lists all accessible Fabric workspaces.
    No parameters required.
    """
    pass


class WorkspaceInfo(BaseModel):
    """Information about a Fabric workspace."""
    
    id: str = Field(..., description="Unique workspace ID (GUID)")
    name: str = Field(..., description="Display name of the workspace")
    type: str = Field(
        default="Workspace",
        description="Workspace type",
    )


class ListWorkspacesOutput(BaseModel):
    """
    Output from the list_workspaces tool.
    
    Attributes:
        workspaces: List of accessible workspaces.
        count: Total number of workspaces.
    """
    
    workspaces: List[WorkspaceInfo] = Field(
        default_factory=list,
        description="List of accessible workspaces",
    )
    count: int = Field(
        default=0,
        description="Total number of workspaces",
    )


class SetWorkspaceInput(BaseModel):
    """
    Input for the set_workspace tool.
    
    Sets the active workspace for subsequent operations.
    
    Attributes:
        workspace_name: Name of the workspace to activate.
    
    Example:
        >>> input = SetWorkspaceInput(workspace_name="Sales Analytics")
    """
    
    workspace_name: str = Field(
        ...,
        description="Name of the workspace to set as active",
        min_length=1,
        max_length=256,
    )
    
    @field_validator("workspace_name")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        """Strip leading/trailing whitespace from workspace name."""
        return v.strip()


class SetWorkspaceOutput(BaseModel):
    """
    Output from the set_workspace tool.
    
    Attributes:
        workspace_id: ID of the activated workspace.
        workspace_name: Name of the activated workspace.
    """
    
    workspace_id: str = Field(..., description="Activated workspace ID")
    workspace_name: str = Field(..., description="Activated workspace name")


class ListItemsInput(BaseModel):
    """
    Input for the list_items tool.
    
    Lists items in the current workspace with optional filtering.
    
    Attributes:
        item_type: Optional filter by item type.
    
    Example:
        >>> # List all items
        >>> input = ListItemsInput()
        >>> 
        >>> # List only reports
        >>> input = ListItemsInput(item_type="Report")
    """
    
    item_type: Optional[str] = Field(
        default=None,
        description="Filter by type: Report, SemanticModel, Lakehouse, Notebook, etc.",
    )


class ItemInfo(BaseModel):
    """Information about a Fabric item."""
    
    id: str = Field(..., description="Unique item ID")
    name: str = Field(..., description="Display name")
    type: str = Field(..., description="Item type (Report, SemanticModel, etc.)")


class ListItemsOutput(BaseModel):
    """
    Output from the list_items tool.
    
    Attributes:
        items: List of items in the workspace.
        count: Total number of items.
    """
    
    items: List[ItemInfo] = Field(
        default_factory=list,
        description="List of workspace items",
    )
    count: int = Field(default=0, description="Total item count")


# ============================================================================
# Semantic Model Tool Models
# ============================================================================

class GetSemanticModelInput(BaseModel):
    """
    Input for the get_semantic_model tool.
    
    Retrieves details of a semantic model including tables, columns, and measures.
    
    Attributes:
        model_name: Name of the semantic model.
    
    Example:
        >>> input = GetSemanticModelInput(model_name="Sales Analytics")
    """
    
    model_name: str = Field(
        ...,
        description="Name of the semantic model to retrieve",
        min_length=1,
        max_length=256,
    )


class MeasureInfo(BaseModel):
    """Information about a DAX measure."""
    
    name: str = Field(..., description="Measure name")
    table: Optional[str] = Field(None, description="Parent table name")
    expression: Optional[str] = Field(None, description="DAX expression")
    description: Optional[str] = Field(None, description="Measure description")


class GetSemanticModelOutput(BaseModel):
    """
    Output from the get_semantic_model tool.
    
    Attributes:
        model_id: Semantic model ID.
        model_name: Semantic model name.
        measures: List of measures in the model.
        measure_count: Number of measures.
        parts_count: Number of definition parts.
        definition: Raw model definition (TMDL).
    """
    
    model_id: str = Field(..., description="Model ID")
    model_name: str = Field(..., description="Model name")
    measures: List[MeasureInfo] = Field(
        default_factory=list,
        description="Measures in the model",
    )
    measure_count: int = Field(default=0, description="Number of measures")
    parts_count: int = Field(default=0, description="Number of definition parts")
    definition: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Full model definition",
    )


class GetMeasuresInput(BaseModel):
    """
    Input for the get_measures tool.
    
    Retrieves all DAX measures from a semantic model.
    
    Attributes:
        model_name: Name of the semantic model.
    """
    
    model_name: str = Field(
        ...,
        description="Name of the semantic model",
        min_length=1,
    )


class GetMeasuresOutput(BaseModel):
    """
    Output from the get_measures tool.
    
    Attributes:
        model_name: Name of the semantic model.
        measures: List of measures.
        count: Number of measures.
    """
    
    model_name: str = Field(..., description="Model name")
    measures: List[MeasureInfo] = Field(
        default_factory=list,
        description="List of measures",
    )
    count: int = Field(default=0, description="Number of measures")


# ============================================================================
# Report Tool Models
# ============================================================================

class GetReportDefinitionInput(BaseModel):
    """
    Input for the get_report_definition tool.
    
    Retrieves the PBIR definition of a report including pages and visuals.
    
    Attributes:
        report_name: Name of the report.
    """
    
    report_name: str = Field(
        ...,
        description="Name of the report",
        min_length=1,
    )


class VisualInfo(BaseModel):
    """Information about a report visual."""
    
    id: Optional[str] = Field(None, description="Visual ID")
    name: Optional[str] = Field(None, description="Visual name")
    type: Optional[str] = Field(None, description="Visual type (chart, table, etc.)")
    page: Optional[str] = Field(None, description="Parent page name")


class GetReportDefinitionOutput(BaseModel):
    """
    Output from the get_report_definition tool.
    
    Attributes:
        report_id: Report ID.
        report_name: Report name.
        pages: List of page names.
        visuals: List of visuals in the report.
        visual_count: Number of visuals.
        definition: Parsed report.json content.
        definition_parts: Raw definition parts.
        parts_count: Number of parts.
    """
    
    report_id: str = Field(..., description="Report ID")
    report_name: str = Field(..., description="Report name")
    pages: List[str] = Field(default_factory=list, description="Page names")
    visuals: List[VisualInfo] = Field(
        default_factory=list,
        description="Visuals in the report",
    )
    visual_count: int = Field(default=0, description="Number of visuals")
    definition: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Parsed report definition",
    )
    definition_parts: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Raw definition parts",
    )
    parts_count: int = Field(default=0, description="Number of parts")


# ============================================================================
# Impact Analysis Tool Models
# ============================================================================

class AnalyzeImpactInput(BaseModel):
    """
    Input for the analyze_impact tool.
    
    Analyzes what will break if you rename a measure or column.
    ALWAYS call this before any rename operation.
    
    Attributes:
        target_type: Type of object to analyze ('measure' or 'column').
        target_name: Name of the measure or column.
        model_name: Semantic model name (required for DAX dependency analysis).
    
    Example:
        >>> input = AnalyzeImpactInput(
        ...     target_type="measure",
        ...     target_name="Sales",
        ...     model_name="Sales Analytics"
        ... )
    """
    
    target_type: TargetType = Field(
        ...,
        description="Type of object to analyze: 'measure' or 'column'",
    )
    target_name: str = Field(
        ...,
        description="Name of the measure or column to analyze",
        min_length=1,
    )
    model_name: Optional[str] = Field(
        default=None,
        description="Semantic model name (required for DAX dependency analysis)",
    )


class AffectedVisual(BaseModel):
    """Information about an affected visual."""
    
    report: str = Field(..., description="Report name")
    page: str = Field(..., description="Page name")
    visual_id: Optional[str] = Field(None, description="Visual ID")
    visual_name: Optional[str] = Field(None, description="Visual name")
    visual_type: Optional[str] = Field(None, description="Visual type")


class AffectedMeasure(BaseModel):
    """Information about an affected measure."""
    
    measure_name: str = Field(..., description="Measure name")
    expression: Optional[str] = Field(None, description="DAX expression")
    dependency_type: str = Field(
        default="uses",
        description="How this measure depends on the target",
    )


class AnalyzeImpactOutput(BaseModel):
    """
    Output from the analyze_impact tool.
    
    Attributes:
        target_type: Type that was analyzed.
        target_name: Name that was analyzed.
        severity: Impact severity level.
        total_impact: Total number of affected items.
        affected_reports: List of affected report names.
        affected_visuals: Details of affected visuals.
        affected_measures: DAX measures that reference the target.
        safe_to_rename: True if no dependencies found.
    """
    
    target_type: str = Field(..., description="Analyzed target type")
    target_name: str = Field(..., description="Analyzed target name")
    severity: ImpactSeverity = Field(
        default=ImpactSeverity.NONE,
        description="Impact severity",
    )
    total_impact: int = Field(default=0, description="Total affected items")
    affected_reports: List[str] = Field(
        default_factory=list,
        description="Affected report names",
    )
    affected_visuals: List[AffectedVisual] = Field(
        default_factory=list,
        description="Affected visual details",
    )
    affected_measures: List[AffectedMeasure] = Field(
        default_factory=list,
        description="Affected DAX measures",
    )
    safe_to_rename: bool = Field(
        default=True,
        description="True if safe to rename (no dependencies)",
    )


# ============================================================================
# Refactoring Tool Models
# ============================================================================

class RenameMeasureInput(BaseModel):
    """
    Input for the rename_measure tool.
    
    Renames a measure across the semantic model and all reports.
    Use dry_run=true first to preview changes!
    
    Attributes:
        model_name: Semantic model containing the measure.
        old_name: Current measure name.
        new_name: New measure name.
        dry_run: If true, only simulate changes (recommended first step).
    
    Example:
        >>> # First, preview the changes
        >>> input = RenameMeasureInput(
        ...     model_name="Sales Analytics",
        ...     old_name="Sales",
        ...     new_name="Net_Revenue",
        ...     dry_run=True
        ... )
    """
    
    model_name: str = Field(
        ...,
        description="Semantic model containing the measure",
        min_length=1,
    )
    old_name: str = Field(
        ...,
        description="Current measure name",
        min_length=1,
    )
    new_name: str = Field(
        ...,
        description="New measure name",
        min_length=1,
    )
    dry_run: bool = Field(
        default=True,
        description="If true, only simulate changes (recommended)",
    )
    
    @field_validator("new_name")
    @classmethod
    def validate_new_name(cls, v: str, info) -> str:
        """Ensure new name is different from old name."""
        old_name = info.data.get("old_name", "")
        if v.strip().lower() == old_name.strip().lower():
            raise ValueError("new_name must be different from old_name")
        return v.strip()


class ChangeDetail(BaseModel):
    """Details of a single change in a refactoring operation."""
    
    file: str = Field(..., description="File that was modified")
    change_type: str = Field(..., description="Type of change")
    details: Optional[str] = Field(None, description="Additional details")




class SmartRenameMeasureInput(BaseModel):
    """
    Input for the smart_rename_measure tool.

    This is the "hero" refactor operation:
    - Renames a measure
    - Updates any other measures that reference it so they don't break
    - Persists an append-only log to memory/refactor_log.json
    - Records an audit chain to MemoryManager so the whole refactor can be rolled back as one transaction

    Attributes:
        model_name: Semantic model (dataset) name.
        old_name: Existing measure name.
        new_name: New measure name.
        dry_run: If true, only preview the plan (no changes applied).
        reasoning: Free-text reason for this change (stored in the log).
    """

    model_name: str = Field(..., description="Name of the semantic model", min_length=1)
    old_name: str = Field(..., description="Existing measure name", min_length=1)
    new_name: str = Field(..., description="New measure name", min_length=1)
    dry_run: bool = Field(default=True, description="If true, only preview changes")
    reasoning: Optional[str] = Field(default=None, description="Reasoning for this change (stored in audit log)")

    @field_validator("new_name")
    @classmethod
    def validate_new_name(cls, v: str, info: Any) -> str:
        v = v.strip()
        if not v:
            raise ValueError("new_name must not be empty")
        return v


class SmartRenameMeasureOutput(BaseModel):
    """
    Output from the smart_rename_measure tool.

    Attributes:
        operation: Operation name.
        operation_id: Transaction id (used for rollback).
        model_name: Semantic model name.
        old_name: Original measure name.
        new_name: New measure name.
        dry_run: Whether this was a simulation.
        referenced_by: Measures that reference the old measure (direct references).
        changes: Planned/applied changes.
        status: Operation status string.
        message: Human-readable markdown summary (includes tables).
        snapshot_ids: Audit chain snapshot ids (first is the transaction root).
    """

    operation: str = Field(default="smart_rename_measure", description="Operation name")
    operation_id: str = Field(..., description="Transaction id for rollback")
    model_name: str = Field(..., description="Semantic model name")
    old_name: str = Field(..., description="Original measure name")
    new_name: str = Field(..., description="New measure name")
    dry_run: bool = Field(..., description="Whether this was a dry run")
    referenced_by: List[str] = Field(default_factory=list, description="Measures that reference the old measure")
    changes: List[ChangeDetail] = Field(default_factory=list, description="Changes made or planned")
    status: str = Field(default="", description="Status")
    message: str = Field(default="", description="Markdown summary")
    snapshot_ids: List[str] = Field(default_factory=list, description="Audit trail snapshot chain")

class RenameMeasureOutput(BaseModel):
    """
    Output from the rename_measure tool.
    
    Attributes:
        operation: Name of the operation.
        old_name: Original measure name.
        new_name: New measure name.
        dry_run: Whether this was a simulation.
        impact: Impact analysis results.
        changes: List of changes made (or that would be made).
        status: Operation status.
        message: Human-readable status message.
        snapshot_id: ID of the audit trail snapshot (for rollback).
    """
    
    operation: str = Field(
        default="rename_measure",
        description="Operation name",
    )
    old_name: str = Field(..., description="Original measure name")
    new_name: str = Field(..., description="New measure name")
    dry_run: bool = Field(..., description="Whether this was a dry run")
    impact: Optional[AnalyzeImpactOutput] = Field(
        default=None,
        description="Impact analysis results",
    )
    changes: List[ChangeDetail] = Field(
        default_factory=list,
        description="Changes made or planned",
    )
    status: str = Field(default="success", description="Operation status")
    message: Optional[str] = Field(None, description="Status message")
    snapshot_id: Optional[str] = Field(
        None,
        description="Audit trail snapshot ID for rollback",
    )


# ============================================================================
# History Tool Models
# ============================================================================

class GetRefactorHistoryInput(BaseModel):
    """
    Input for the get_refactor_history tool.
    
    Retrieves history of refactoring operations from the audit trail.
    
    Attributes:
        limit: Maximum number of events to return.
        operation_type: Filter by operation type.
    
    Example:
        >>> # Get last 10 operations
        >>> input = GetRefactorHistoryInput(limit=10)
        >>> 
        >>> # Get only rename operations
        >>> input = GetRefactorHistoryInput(operation_type="rename_measure")
    """
    
    limit: int = Field(
        default=10,
        description="Maximum events to return",
        ge=1,
        le=100,
    )
    operation_type: Optional[str] = Field(
        default=None,
        description="Filter by operation type",
    )


class RefactorEvent(BaseModel):
    """A single refactoring event from history."""
    
    id: str = Field(..., description="Event ID")
    timestamp: str = Field(..., description="When the event occurred (ISO format)")
    operation: str = Field(..., description="Operation name")
    target: Optional[str] = Field(None, description="Target of the operation")
    old_value: Optional[str] = Field(None, description="Previous value")
    new_value: Optional[str] = Field(None, description="New value")
    affected_items: List[str] = Field(
        default_factory=list,
        description="Items affected by this operation",
    )
    status: str = Field(..., description="Operation status")
    user: Optional[str] = Field(None, description="User who performed the operation")


class GetRefactorHistoryOutput(BaseModel):
    """
    Output from the get_refactor_history tool.
    
    Attributes:
        events: List of refactoring events.
        count: Number of events returned.
    """
    
    events: List[RefactorEvent] = Field(
        default_factory=list,
        description="Refactoring events",
    )
    count: int = Field(default=0, description="Number of events")


# ============================================================================
# Rollback Tool Models
# ============================================================================

class RollbackInput(BaseModel):
    """
    Input for the rollback tool.

    Supports two rollback modes:
    1) operation_id: roll back a full refactor transaction (recommended)
    2) snapshot_id: legacy rollback by snapshot id (if implemented)

    At least one of operation_id or snapshot_id must be provided.

    Attributes:
        operation_id: Transaction id produced by smart_rename_measure.
        snapshot_id: Snapshot id from MemoryManager (legacy).
        model_name: Semantic model name (required when using operation_id).
        dry_run: If true, only preview.
    """

    operation_id: Optional[str] = Field(
        default=None,
        description="Transaction id to rollback (recommended)",
    )
    snapshot_id: Optional[str] = Field(
        default=None,
        description="Snapshot ID to rollback to (legacy)",
    )
    model_name: Optional[str] = Field(
        default=None,
        description="Semantic model name (required for operation_id rollback)",
    )
    dry_run: bool = Field(
        default=True,
        description="If true, only preview the rollback",
    )

    @model_validator(mode="after")
    def _validate_one_of(self) -> "RollbackInput":
        if not self.operation_id and not self.snapshot_id:
            raise ValueError("Provide either operation_id or snapshot_id")
        if self.operation_id and not self.model_name:
            raise ValueError("model_name is required when using operation_id")
        return self


class RollbackOutput(BaseModel):

    """
    Output from the rollback tool.
    
    Attributes:
        snapshot_id: ID of the snapshot rolled back to.
        dry_run: Whether this was a simulation.
        changes: Changes that were (or would be) reverted.
        status: Operation status.
        message: Human-readable message.
    """
    
    snapshot_id: str = Field(..., description="Target snapshot ID")
    dry_run: bool = Field(..., description="Whether this was a dry run")
    changes: List[ChangeDetail] = Field(
        default_factory=list,
        description="Changes reverted",
    )
    status: str = Field(default="success", description="Status")
    message: Optional[str] = Field(None, description="Status message")


# ============================================================================
# Connection Tool Models
# ============================================================================

class GetConnectionStatusInput(BaseModel):
    """
    Input for the get_connection_status tool.
    
    Checks the current connection status to Fabric.
    No parameters required.
    """
    pass


class GetConnectionStatusOutput(BaseModel):
    """
    Output from the get_connection_status tool.
    
    Attributes:
        connected: Whether connected to Fabric.
        workspace_id: Current workspace ID (if set).
        workspace_name: Current workspace name (if set).
        auth_mode: Authentication mode in use.
        error: Error message if not connected.
    """
    
    connected: bool = Field(..., description="Whether connected to Fabric")
    workspace_id: Optional[str] = Field(None, description="Current workspace ID")
    workspace_name: Optional[str] = Field(None, description="Current workspace name")
    auth_mode: Optional[str] = Field(None, description="Authentication mode")
    error: Optional[str] = Field(None, description="Error if not connected")


# ============================================================================
# Use Case 2: Context Memory / Vector RAG Tool Models
# ============================================================================

class FindSimilarOperationsInput(BaseModel):
    """
    Input for find_similar_operations MCP tool.

    Searches the vector store for past operations semantically similar
    to the proposed change description.

    Example prompt to Claude:
        "Before I rename Total Revenue, find similar past operations"
        → Claude calls find_similar_operations("rename Total Revenue to Gross Revenue")
        → Returns 5 past renames with their outcomes (success/failed/rolled_back)
    """
    proposed_change: str = Field(
        ...,
        description="Natural language description of the proposed operation. "
                    "E.g. 'rename Total Revenue to Gross Revenue in Sales Model'",
        min_length=5,
    )
    change_type: Optional[str] = Field(
        default=None,
        description="Optional filter: restrict to a specific operation type "
                    "(e.g. 'rename_measure', 'schema_drift', 'rollback')",
    )
    top_k: int = Field(
        default=5,
        description="Number of similar operations to return",
        ge=1,
        le=20,
    )


class SimilarOperationInfo(BaseModel):
    """A single similar past operation returned by find_similar_operations."""
    snapshot_id: str = Field(..., description="Audit trail snapshot ID")
    operation: str = Field(..., description="Operation type (e.g. rename_measure)")
    target_name: Optional[str] = Field(None, description="What was changed")
    old_value: Optional[str] = Field(None, description="Previous value")
    new_value: Optional[str] = Field(None, description="New value")
    status: str = Field(..., description="Outcome: success | failed | rolled_back")
    timestamp: str = Field(..., description="When this operation occurred (ISO format)")
    similarity: float = Field(..., description="Semantic similarity score (0.0 to 1.0)")
    key_lesson: str = Field(..., description="One-line lesson from this historical operation")


class FindSimilarOperationsOutput(BaseModel):
    """Output from find_similar_operations."""
    proposed_change: str = Field(..., description="The input change description")
    similar_operations: List[SimilarOperationInfo] = Field(
        default_factory=list,
        description="Similar past operations, sorted by similarity (highest first)",
    )
    count: int = Field(default=0, description="Number of results returned")
    vector_store_size: int = Field(
        default=0,
        description="Total operations indexed in vector store",
    )


class GetRiskContextInput(BaseModel):
    """
    Input for get_risk_context MCP tool.

    Returns synthesized risk intelligence based on similar past operations:
    - historical failure rate
    - common failure reasons
    - actionable recommendations

    This is the tool agents call before executing any significant operation.
    """
    proposed_change: str = Field(
        ...,
        description="Natural language description of the proposed operation",
        min_length=5,
    )
    top_k: int = Field(
        default=10,
        description="How many similar operations to use for context calculation",
        ge=3,
        le=30,
    )


class RiskContextOutput(BaseModel):
    """Output from get_risk_context — synthesized risk intelligence."""
    proposed_change: str = Field(..., description="The input change description")
    sample_size: int = Field(..., description="Number of similar operations found")
    historical_failure_rate: float = Field(
        ...,
        description="Fraction of similar operations that failed (0.0 to 1.0)",
    )
    historical_rollback_rate: float = Field(
        ...,
        description="Fraction of similar operations that required rollback (0.0 to 1.0)",
    )
    common_failure_reasons: List[str] = Field(
        default_factory=list,
        description="Most common reasons similar operations failed",
    )
    recommendations: List[str] = Field(
        default_factory=list,
        description="Actionable recommendations based on historical context",
    )
    confidence: float = Field(
        ...,
        description="Confidence in the context (0.0 = no data, 1.0 = high confidence)",
    )
    risk_summary: str = Field(..., description="One-line risk summary")
    is_high_risk: bool = Field(
        ...,
        description="True if history suggests >30% failure rate with sufficient data",
    )


class MemoryStatsInput(BaseModel):
    """Input for memory_stats MCP tool (no parameters needed)."""
    pass


class MemoryStatsOutput(BaseModel):
    """Output from memory_stats — vector store and audit trail statistics."""
    vector_store_backend: str = Field(..., description="ChromaDB, InMemory, etc.")
    vector_store_count: int = Field(..., description="Number of operations indexed")
    vector_store_path: str = Field(..., description="Where the vector store is persisted")
    audit_trail_count: int = Field(default=0, description="Total operations in SQLite audit trail")
    embedding_backend: str = Field(..., description="LocalEmbeddingClient, AzureOpenAI, etc.")
    embedding_dimension: int = Field(..., description="Vector dimension (384 for MiniLM)")


class ReindexMemoryInput(BaseModel):
    """Input for reindex_operation_memory MCP tool."""
    confirm: bool = Field(
        default=False,
        description="Set to true to confirm the reindex. "
                    "This re-embeds ALL audit trail history into the vector store.",
    )


class ReindexMemoryOutput(BaseModel):
    """Output from reindex_operation_memory."""
    operations_indexed: int = Field(..., description="Number of operations reindexed")
    success: bool = Field(..., description="Whether reindex completed successfully")
    message: str = Field(..., description="Status message")


class GetSessionSummaryInput(BaseModel):
    """Input for get_session_summary MCP tool."""
    workspace_id: Optional[str] = Field(
        default=None,
        description="Filter to a specific workspace ID (optional)",
    )


class GetSessionSummaryOutput(BaseModel):
    """Output from get_session_summary — previous session context."""
    has_previous_session: bool = Field(..., description="Whether a previous session exists")
    summary: Optional[str] = Field(
        None,
        description="Human-readable previous session summary, or None if no history",
    )
    session_id: Optional[str] = Field(None, description="Previous session ID")


# ============================================================================
# Healing / Health Tools
# ============================================================================


class AnomalyInfo(BaseModel):
    """Serialized Anomaly for MCP output."""
    anomaly_id: str = Field(..., description="Unique anomaly ID")
    anomaly_type: str = Field(..., description="AnomalyType value")
    severity: str = Field(..., description="RiskLevel: low/medium/high/critical")
    asset_id: str = Field(default="", description="Fabric item ID")
    asset_name: str = Field(..., description="Human-readable asset name")
    workspace_id: str = Field(default="", description="Workspace ID")
    details: str = Field(..., description="Explanation of the anomaly")
    can_auto_heal: bool = Field(..., description="Whether auto-remediation is possible")
    heal_action: Optional[str] = Field(None, description="Suggested heal action type")
    detected_at: str = Field(..., description="UTC ISO timestamp")


class ScanWorkspaceHealthInput(BaseModel):
    """Input for scan_workspace_health MCP tool."""
    workspace_ids: List[str] = Field(
        ...,
        description="One or more Fabric workspace IDs to scan",
    )
    include_schema_drift: bool = Field(
        default=True,
        description="Also check tables for schema drift against registered contracts",
    )
    stale_hours: int = Field(
        default=24,
        description="Flag tables not refreshed within this many hours as STALE",
    )


class ScanWorkspaceHealthOutput(BaseModel):
    """Output from scan_workspace_health — list of detected anomalies."""
    workspace_ids: List[str] = Field(..., description="Scanned workspace IDs")
    anomalies: List[AnomalyInfo] = Field(default_factory=list)
    total_found: int = Field(..., description="Total anomalies found")
    critical_count: int = Field(default=0, description="Critical severity count")
    high_count: int = Field(default=0, description="High severity count")
    medium_count: int = Field(default=0, description="Medium severity count")
    low_count: int = Field(default=0, description="Low severity count")
    auto_healable_count: int = Field(default=0, description="Anomalies that can be auto-healed")
    message: str = Field(default="", description="Summary message")


class BuildHealingPlanInput(BaseModel):
    """Input for build_healing_plan MCP tool."""
    workspace_ids: List[str] = Field(
        ...,
        description="Workspace IDs to scan and plan healing for",
    )


class HealActionInfo(BaseModel):
    """Serialized HealAction for MCP output."""
    action_id: str = Field(..., description="Unique action ID")
    anomaly_id: str = Field(..., description="Anomaly this action fixes")
    action_type: str = Field(..., description="Machine-readable action type")
    description: str = Field(..., description="Human-readable description")
    requires_approval: bool = Field(..., description="Whether human approval is needed")
    status: str = Field(..., description="HealActionStatus value")


class BuildHealingPlanOutput(BaseModel):
    """Output from build_healing_plan — categorised actions."""
    plan_id: str = Field(..., description="Unique plan ID")
    anomaly_count: int = Field(..., description="Total anomalies in plan")
    auto_action_count: int = Field(..., description="Actions safe to auto-apply")
    manual_action_count: int = Field(..., description="Actions requiring approval")
    auto_actions: List[HealActionInfo] = Field(default_factory=list)
    manual_actions: List[HealActionInfo] = Field(default_factory=list)
    message: str = Field(default="", description="Summary message")


class ExecuteHealingPlanInput(BaseModel):
    """Input for execute_healing_plan MCP tool."""
    workspace_ids: List[str] = Field(
        ...,
        description="Workspace IDs to scan, plan, and heal",
    )
    dry_run: bool = Field(
        default=True,
        description="If True, simulate all actions without applying changes. "
                    "ALWAYS use True first!",
    )


class ExecuteHealingPlanOutput(BaseModel):
    """Output from execute_healing_plan — heal result."""
    plan_id: str = Field(..., description="Healing plan ID")
    applied: int = Field(..., description="Actions successfully applied")
    failed: int = Field(..., description="Actions that failed")
    skipped: int = Field(..., description="Actions skipped (dry_run or requires_approval)")
    dry_run: bool = Field(..., description="Whether this was a dry run")
    success: bool = Field(..., description="True if no failures")
    errors: List[str] = Field(default_factory=list, description="Error messages")
    message: str = Field(default="", description="Summary message")


# ============================================================================
# Phase E — Shortcut Cascade Tools
# ============================================================================


class ShortcutDefinitionInfo(BaseModel):
    """
    Info about a single OneLake shortcut, sourced from Fabric REST API.

    Included in scan and plan outputs so the user can see exactly which
    shortcuts are broken and where their sources are supposed to be.
    """
    shortcut_id: str = Field(..., description="Synthetic shortcut identifier")
    name: str = Field(..., description="Shortcut name in the lakehouse")
    workspace_id: str = Field(..., description="Workspace containing the shortcut")
    lakehouse_id: str = Field(..., description="Lakehouse item ID containing the shortcut")
    source_workspace_id: str = Field(..., description="Source workspace ID (where data comes from)")
    source_lakehouse_id: str = Field(..., description="Source lakehouse item ID")
    source_path: str = Field(..., description="Path within source lakehouse (e.g. 'Tables/DimDate')")
    is_healthy: bool = Field(..., description="False if source is inaccessible")


class FixSuggestionInfo(BaseModel):
    """
    A single proposed fix for one asset impacted by a broken shortcut.

    auto_applicable=True → safe to apply immediately (shortcut recreation, model refresh).
    auto_applicable=False → needs human approval (pipeline config, cross-workspace moves).
    """
    asset_id: str = Field(..., description="Fabric item ID of the affected asset")
    asset_name: str = Field(..., description="Human-readable asset name")
    asset_type: str = Field(
        ...,
        description="Asset type: 'shortcut' | 'semantic_model' | 'pipeline' | 'report'",
    )
    suggestion: str = Field(..., description="Human-readable description of the proposed fix")
    action_type: str = Field(
        ...,
        description="Machine-readable action: 'recreate_shortcut' | 'refresh_model' | 'manual_review'",
    )
    auto_applicable: bool = Field(
        ...,
        description="True = safe to apply without additional approval",
    )


class CascadeImpactInfo(BaseModel):
    """
    All assets broken because of one broken shortcut, with per-asset fix suggestions.

    Captures the full blast radius: shortcut → tables → semantic models → pipelines → reports.
    """
    shortcut_name: str = Field(..., description="Name of the broken shortcut")
    impacted_count: int = Field(..., description="Total number of impacted downstream assets")
    impacted_tables: List[str] = Field(
        default_factory=list, description="Table names that alias the broken shortcut"
    )
    impacted_semantic_models: List[str] = Field(
        default_factory=list, description="Semantic model names built on impacted tables"
    )
    impacted_pipelines: List[str] = Field(
        default_factory=list, description="Pipeline names that reference impacted models"
    )
    impacted_reports: List[str] = Field(
        default_factory=list, description="Report names consuming impacted models"
    )
    fix_suggestions: List[FixSuggestionInfo] = Field(
        default_factory=list, description="One fix suggestion per impacted asset"
    )


class ScanShortcutCascadeInput(BaseModel):
    """Input for scan_shortcut_cascade — which workspaces to scan."""
    workspace_ids: List[str] = Field(
        ...,
        description="List of workspace IDs to scan for broken shortcuts",
    )


class ScanShortcutCascadeOutput(BaseModel):
    """
    Output from scan_shortcut_cascade — broken shortcuts + cascade analysis.

    Returns the full blast radius for each broken shortcut:
    which downstream assets are affected and what the suggested fixes are.
    """
    broken_shortcut_count: int = Field(..., description="Total broken shortcuts found")
    total_cascade_impact: int = Field(
        ..., description="Total downstream assets affected across all broken shortcuts"
    )
    broken_shortcuts: List[ShortcutDefinitionInfo] = Field(
        default_factory=list, description="Details of each broken shortcut"
    )
    cascade_impacts: List[CascadeImpactInfo] = Field(
        default_factory=list, description="Per-shortcut cascade analysis"
    )
    message: str = Field(default="", description="Summary message")


class BuildShortcutHealingPlanInput(BaseModel):
    """Input for build_shortcut_healing_plan."""
    workspace_ids: List[str] = Field(
        ...,
        description="Workspace IDs to include in the healing plan",
    )


class BuildShortcutHealingPlanOutput(BaseModel):
    """
    Output from build_shortcut_healing_plan — the full plan ready for review.

    Shows the auto_actions (safe to apply immediately) and manual_actions
    (require human sign-off). approval_required=True means you must call
    approve_shortcut_healing() before execute_shortcut_healing() will run
    the manual actions.
    """
    plan_id: str = Field(..., description="Plan ID — pass to approve/execute calls")
    approval_required: bool = Field(
        ...,
        description="True if any manual_actions exist (human review needed)",
    )
    auto_action_count: int = Field(..., description="Safe actions (will auto-apply)")
    manual_action_count: int = Field(..., description="Actions requiring human approval")
    auto_actions: List[FixSuggestionInfo] = Field(
        default_factory=list, description="Safe fix suggestions"
    )
    manual_actions: List[FixSuggestionInfo] = Field(
        default_factory=list, description="Fix suggestions requiring approval"
    )
    message: str = Field(default="", description="Summary message")


class ApproveShortcutHealingInput(BaseModel):
    """Input for approve_shortcut_healing — approve a pending plan."""
    plan_id: str = Field(..., description="Plan ID from build_shortcut_healing_plan")
    approved_by: str = Field(
        default="user",
        description="Name or email of the approver (for audit trail)",
    )


class ApproveShortcutHealingOutput(BaseModel):
    """Output from approve_shortcut_healing."""
    plan_id: str = Field(..., description="The approved plan ID")
    approved: bool = Field(..., description="True if approval succeeded")
    approved_by: str = Field(..., description="Who approved")
    message: str = Field(default="", description="Confirmation message")


class ExecuteShortcutHealingInput(BaseModel):
    """Input for execute_shortcut_healing — run a plan."""
    plan_id: str = Field(..., description="Plan ID from build_shortcut_healing_plan")
    dry_run: bool = Field(
        default=True,
        description=(
            "True = simulation only (no changes made). "
            "False = apply changes. ALWAYS use True first!"
        ),
    )


class ExecuteShortcutHealingOutput(BaseModel):
    """
    Output from execute_shortcut_healing — execution result.

    Reports how many actions were applied, failed, or skipped.
    Check errors[] for details on any failures.
    """
    plan_id: str = Field(..., description="Plan ID that was executed")
    applied: int = Field(..., description="Actions successfully applied")
    failed: int = Field(..., description="Actions that failed")
    skipped: int = Field(..., description="Actions skipped (dry_run or approval gate)")
    dry_run: bool = Field(..., description="Whether this was a dry run")
    success: bool = Field(..., description="True if no failures")
    errors: List[str] = Field(default_factory=list, description="Error messages from failed actions")
    message: str = Field(default="", description="Summary message")


# =============================================================================
# Phase I — Enterprise Blast Radius MCP Tool Models
# =============================================================================


class EnterpriseBlastRadiusInput(BaseModel):
    """
    Input for get_enterprise_blast_radius — cross-workspace cascade impact analysis.

    Provide the name of the source asset that changed (e.g. 'fact_sales') plus
    the workspace IDs to scan.  The tool builds the full lineage graph across all
    workspaces and returns a per-workspace breakdown with an ordered healing plan.
    """
    source_asset_name: str = Field(
        ...,
        description="Name of the table/asset that changed (e.g. 'fact_sales')",
    )
    workspace_ids: List[str] = Field(
        ...,
        description="List of workspace IDs to include in the blast radius scan",
    )
    change_description: str = Field(
        default="schema change",
        description="Human-readable description of the change (e.g. 'column dropped')",
    )


class ImpactedAssetInfo(BaseModel):
    """Single impacted asset detail within an enterprise blast radius."""
    asset_id: str = Field(..., description="Fabric item ID")
    asset_name: str = Field(..., description="Display name")
    asset_type: str = Field(..., description="Asset type: table | shortcut | pipeline | semantic_model | report")
    workspace_id: str = Field(..., description="Owning workspace ID")
    workspace_name: str = Field(..., description="Owning workspace name")
    impact_depth: int = Field(..., description="Hops from the source asset (1 = direct)")
    impact_path: List[str] = Field(default_factory=list, description="Asset name chain from source to this asset")
    action_type: str = Field(..., description="Healing action type")
    action_description: str = Field(..., description="Human-readable action description")
    auto_applicable: bool = Field(..., description="True if healer can run this without human approval")
    urgency: str = Field(..., description="immediate | after_source_fixed | advisory")


class WorkspaceImpactInfo(BaseModel):
    """Per-workspace summary within an enterprise blast radius."""
    workspace_id: str = Field(..., description="Workspace ID")
    workspace_name: str = Field(..., description="Workspace name")
    role: str = Field(..., description="source | consumer")
    asset_count: int = Field(..., description="Total impacted assets in this workspace")
    auto_count: int = Field(..., description="Auto-healable actions")
    manual_count: int = Field(..., description="Manual-review actions")
    impacted_assets: List[ImpactedAssetInfo] = Field(
        default_factory=list, description="Per-asset details"
    )


class EnterpriseBlastRadiusOutput(BaseModel):
    """
    Output from get_enterprise_blast_radius — complete cross-workspace impact analysis.

    Contains:
    - per_workspace: per-workspace breakdown of impacted assets
    - ordered_healing_steps: topologically sorted list of all actions to execute
    - risk_level: low | medium | high | critical
    """
    source_asset_name: str = Field(..., description="Source asset that changed")
    source_workspace_name: str = Field(..., description="Workspace owning the source")
    change_description: str = Field(..., description="What changed")
    total_workspaces_impacted: int = Field(..., description="Number of workspaces affected")
    total_assets_impacted: int = Field(..., description="Total assets affected across all workspaces")
    risk_level: str = Field(..., description="low | medium | high | critical")
    generated_at: str = Field(..., description="UTC ISO timestamp")
    per_workspace: List[WorkspaceImpactInfo] = Field(
        default_factory=list, description="Per-workspace breakdown"
    )
    ordered_healing_steps: List[ImpactedAssetInfo] = Field(
        default_factory=list,
        description="Topologically sorted healing actions (execute in order)",
    )
    message: str = Field(default="", description="Summary message")


# =============================================================================
# Guards — Freshness Guard + Maintenance Guard Tool Models
# =============================================================================


# ── Freshness Guard ──────────────────────────────────────────────────────────


class TableSyncInfo(BaseModel):
    """Serialized TableSyncStatus for MCP output."""

    table_name: str = Field(..., description="Delta table name")
    last_successful_sync_dt: Optional[str] = Field(
        None, description="UTC ISO last successful sync"
    )
    sync_status: str = Field(
        ..., description="Fabric sync status: Success/Failure/NotRun"
    )
    hours_since_sync: Optional[float] = Field(
        None, description="Hours since last successful sync"
    )
    freshness_status: str = Field(
        ..., description="healthy/stale/never_synced/sync_failed/unknown"
    )
    sla_threshold_hours: float = Field(
        ..., description="SLA threshold in hours for this table"
    )
    sla_pattern: str = Field(..., description="Pattern that matched (e.g. fact_*)")


class FreshnessViolationInfo(BaseModel):
    """Serialized FreshnessViolation for MCP output."""

    violation_id: str = Field(..., description="Unique violation ID")
    workspace_id: str = Field(
        ..., description="Workspace containing the SQL endpoint"
    )
    workspace_name: str = Field(..., description="Workspace display name")
    sql_endpoint_id: str = Field(..., description="SQL endpoint item ID")
    sql_endpoint_name: str = Field(..., description="SQL endpoint display name")
    table_status: TableSyncInfo = Field(..., description="Full table sync status")
    detected_at: str = Field(..., description="UTC ISO timestamp of detection")


class ScanFreshnessInput(BaseModel):
    """
    Input for ``scan_freshness`` MCP tool.

    Scans all SQL Endpoints in the given workspaces and checks each table's
    ``lastSuccessfulSyncDateTime`` against configurable SLA thresholds.
    """

    workspace_ids: List[str] = Field(
        ..., description="One or more Fabric workspace IDs to scan"
    )
    sla_thresholds: Optional[Dict[str, float]] = Field(
        default=None,
        description=(
            "Pattern-to-hours SLA map. Keys are fnmatch patterns, values are "
            "hour thresholds. First match wins. "
            "Example: {'fact_*': 1.0, 'dim_*': 24.0, '*': 24.0}. "
            "Defaults to {'fact_*': 1.0, 'dim_*': 24.0, '*': 24.0} if omitted."
        ),
    )
    lro_timeout_secs: int = Field(
        default=300,
        description="Max seconds to wait for refreshMetadata LRO per SQL endpoint",
    )


class ScanFreshnessOutput(BaseModel):
    """Output from ``scan_freshness`` — list of SLA violations by table."""

    scan_id: str = Field(..., description="Unique scan ID")
    workspace_ids: List[str] = Field(..., description="Scanned workspace IDs")
    scanned_at: str = Field(..., description="UTC ISO scan start time")
    total_tables: int = Field(..., description="Total tables evaluated")
    healthy_count: int = Field(..., description="Tables within SLA")
    violation_count: int = Field(..., description="Tables exceeding SLA")
    violations: List[FreshnessViolationInfo] = Field(
        default_factory=list,
        description="Full details for each SLA violation",
    )
    errors: List[str] = Field(
        default_factory=list,
        description="Per-endpoint errors that did not abort the scan",
    )
    scan_duration_ms: int = Field(..., description="Wall clock duration in ms")
    message: str = Field(default="", description="Summary message")


# ── Maintenance Guard ────────────────────────────────────────────────────────


class TableValidationInfo(BaseModel):
    """Serialized TableValidationResult for MCP output."""

    table_name: str = Field(..., description="Submitted table name")
    is_valid: bool = Field(
        ..., description="True if table passed all pre-submission checks"
    )
    rejection_reason: Optional[str] = Field(
        None,
        description="Why the table was rejected (control char / schema-qualified / not found)",
    )
    resolved_name: Optional[str] = Field(
        None, description="Canonical name if valid"
    )


class MaintenanceJobInfo(BaseModel):
    """Serialized MaintenanceJobRecord for MCP output."""

    job_id: str = Field(..., description="Unique job record ID")
    workspace_id: str = Field(..., description="Workspace owning the lakehouse")
    lakehouse_id: str = Field(..., description="Lakehouse item ID")
    lakehouse_name: str = Field(..., description="Lakehouse display name")
    table_name: str = Field(..., description="Table name submitted")
    validation: TableValidationInfo = Field(
        ..., description="Pre-submission validation result"
    )
    fabric_job_id: Optional[str] = Field(
        None, description="Fabric job instance ID"
    )
    status: str = Field(..., description="MaintenanceJobStatus value")
    submitted_at: Optional[str] = Field(
        None, description="UTC ISO submission time"
    )
    completed_at: Optional[str] = Field(
        None, description="UTC ISO completion time"
    )
    error: Optional[str] = Field(None, description="Error if status=failed")
    dry_run: bool = Field(..., description="True if simulated only")


class RunTableMaintenanceInput(BaseModel):
    """
    Input for ``run_table_maintenance`` MCP tool.

    Validates table names before submission (preventing silent Spark failures),
    checks queue pressure, and submits jobs sequentially for Trial capacity
    compatibility.
    """

    workspace_ids: List[str] = Field(
        ..., description="One or more Fabric workspace IDs to process"
    )
    dry_run: bool = Field(
        default=True,
        description=(
            "If True, validate table names and check queue pressure but do NOT "
            "submit jobs. ALWAYS use True first to preview what would be submitted."
        ),
    )
    table_filter: Optional[List[str]] = Field(
        default=None,
        description=(
            "Whitelist of specific table names to maintain. "
            "None = maintain all registered Delta tables."
        ),
    )
    queue_pressure_threshold: int = Field(
        default=3,
        description="Skip submission if active job count >= this (Trial capacity protection)",
    )


class RunTableMaintenanceOutput(BaseModel):
    """Output from ``run_table_maintenance`` — validation + job audit."""

    run_id: str = Field(..., description="Unique run ID")
    workspace_ids: List[str] = Field(..., description="Processed workspace IDs")
    dry_run: bool = Field(..., description="Whether this was a simulation")
    total_tables: int = Field(
        ..., description="Tables discovered across all lakehouses"
    )
    validated: int = Field(
        ..., description="Tables that passed pre-submission validation"
    )
    rejected: int = Field(
        ...,
        description="Tables rejected by the guard (would have failed Spark)",
    )
    submitted: int = Field(
        ..., description="Tables submitted to TableMaintenance API"
    )
    succeeded: int = Field(
        ..., description="Jobs that reached Succeeded status"
    )
    failed: int = Field(..., description="Jobs that failed or were cancelled")
    skipped_queue: int = Field(
        ..., description="Tables skipped due to queue pressure"
    )
    job_records: List[MaintenanceJobInfo] = Field(
        default_factory=list,
        description="Full audit record for each table processed",
    )
    errors: List[str] = Field(
        default_factory=list, description="Per-workspace/lakehouse errors"
    )
    message: str = Field(default="", description="Summary message")
