"""
Tools Module
============

MCP tools with Pydantic validation and comprehensive docstrings.
"""

from fabric_agent.tools.fabric_tools import FabricTools
from fabric_agent.tools.models import (
    # Common
    ToolResult,
    ImpactSeverity,
    TargetType,
    # Workspace
    ListWorkspacesInput,
    ListWorkspacesOutput,
    WorkspaceInfo,
    SetWorkspaceInput,
    SetWorkspaceOutput,
    ListItemsInput,
    ListItemsOutput,
    ItemInfo,
    # Semantic Model
    GetSemanticModelInput,
    GetSemanticModelOutput,
    MeasureInfo,
    GetMeasuresInput,
    GetMeasuresOutput,
    # Report
    GetReportDefinitionInput,
    GetReportDefinitionOutput,
    VisualInfo,
    # Impact
    AnalyzeImpactInput,
    AnalyzeImpactOutput,
    AffectedVisual,
    AffectedMeasure,
    # Refactor
    RenameMeasureInput,
    RenameMeasureOutput,
    ChangeDetail,
    # History
    GetRefactorHistoryInput,
    GetRefactorHistoryOutput,
    RefactorEvent,
    # Rollback
    RollbackInput,
    RollbackOutput,
    # Connection
    GetConnectionStatusInput,
    GetConnectionStatusOutput,
)

# Enterprise Demo
from fabric_agent.tools.setup_enterprise_demo import (
    SetupEnterpriseDemoInput,
    SetupEnterpriseDemoOutput,
    SetupEnterpriseDemoTool,
    CreatedItem,
    GeneratedData,
    GeneratedMeasure,
)

from fabric_agent.tools.enterprise_data_generator import (
    EnterpriseDataGenerator,
    GeneratorConfig,
)

from fabric_agent.tools.enterprise_dax_generator import (
    EnterpriseDAXGenerator,
    DAXMeasure,
    MeasureCategory,
)

# Safety-First Refactoring
from fabric_agent.tools.safety_refactor import (
    # Enums
    RiskLevel,
    RefactorType,
    DependencyType,
    # Models
    DependencyInfo,
    AffectedItem,
    PreFlightReport,
    # Input/Output
    AnalyzeRefactorImpactInput,
    AnalyzeRefactorImpactOutput,
    SafeRefactorInput,
    SafeRefactorOutput,
    RollbackInput as SafeRollbackInput,
    RollbackOutput as SafeRollbackOutput,
    # Engine
    ImpactAnalyzer,
    SafeRefactoringEngine,
)

# Workspace Graph & Impact Analysis
from fabric_agent.tools.workspace_graph import (
    NodeType,
    EdgeType,
    GraphNode,
    GraphEdge,
    WorkspaceGraphBuilder,
    GraphImpactAnalyzer,
)

__all__ = [
    # Main class
    "FabricTools",
    # Common
    "ToolResult",
    "ImpactSeverity",
    "TargetType",
    # Workspace
    "ListWorkspacesInput",
    "ListWorkspacesOutput",
    "WorkspaceInfo",
    "SetWorkspaceInput",
    "SetWorkspaceOutput",
    "ListItemsInput",
    "ListItemsOutput",
    "ItemInfo",
    # Semantic Model
    "GetSemanticModelInput",
    "GetSemanticModelOutput",
    "MeasureInfo",
    "GetMeasuresInput",
    "GetMeasuresOutput",
    # Report
    "GetReportDefinitionInput",
    "GetReportDefinitionOutput",
    "VisualInfo",
    # Impact
    "AnalyzeImpactInput",
    "AnalyzeImpactOutput",
    "AffectedVisual",
    "AffectedMeasure",
    # Refactor
    "RenameMeasureInput",
    "RenameMeasureOutput",
    "ChangeDetail",
    # History
    "GetRefactorHistoryInput",
    "GetRefactorHistoryOutput",
    "RefactorEvent",
    # Rollback
    "RollbackInput",
    "RollbackOutput",
    # Connection
    "GetConnectionStatusInput",
    "GetConnectionStatusOutput",
    # Enterprise Demo
    "SetupEnterpriseDemoInput",
    "SetupEnterpriseDemoOutput",
    "SetupEnterpriseDemoTool",
    "CreatedItem",
    "GeneratedData",
    "GeneratedMeasure",
    "EnterpriseDataGenerator",
    "GeneratorConfig",
    "EnterpriseDAXGenerator",
    "DAXMeasure",
    "MeasureCategory",
    # Safety-First Refactoring
    "RiskLevel",
    "RefactorType",
    "DependencyType",
    "DependencyInfo",
    "AffectedItem",
    "PreFlightReport",
    "AnalyzeRefactorImpactInput",
    "AnalyzeRefactorImpactOutput",
    "SafeRefactorInput",
    "SafeRefactorOutput",
    "SafeRollbackInput",
    "SafeRollbackOutput",
    "ImpactAnalyzer",
    "SafeRefactoringEngine",
    # Workspace Graph
    "NodeType",
    "EdgeType",
    "GraphNode",
    "GraphEdge",
    "WorkspaceGraphBuilder",
    "GraphImpactAnalyzer",
]
