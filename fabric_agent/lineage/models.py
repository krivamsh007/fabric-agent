"""
Lineage Data Models
===================

Core data structures for the lineage engine.

These models represent:
- Asset types (tables, measures, reports, etc.)
- Dependency types (reads_from, uses_model, etc.)
- Risk levels for impact assessment
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field


class AssetType(str, Enum):
    """
    All Microsoft Fabric asset types supported by the lineage engine.
    
    Categories:
    - Workspace level: The container for all assets
    - Data layer: Lakehouses, tables, shortcuts
    - Compute layer: Notebooks, pipelines, dataflows
    - BI layer: Semantic models, measures, relationships
    - Presentation layer: Reports, dashboards, visuals
    """
    
    # Workspace level
    WORKSPACE = "workspace"
    
    # Data layer
    LAKEHOUSE = "lakehouse"
    TABLE = "table"
    SHORTCUT = "shortcut"
    COLUMN = "column"
    WAREHOUSE = "warehouse"
    
    # Compute layer
    NOTEBOOK = "notebook"
    PIPELINE = "pipeline"
    DATAFLOW = "dataflow"
    SPARK_JOB = "spark_job"
    EVENTSTREAM = "eventstream"
    
    # BI layer
    SEMANTIC_MODEL = "semantic_model"
    MEASURE = "measure"
    CALCULATED_COLUMN = "calculated_column"
    CALCULATED_TABLE = "calculated_table"
    RELATIONSHIP = "relationship"
    HIERARCHY = "hierarchy"
    
    # Presentation layer
    REPORT = "report"
    REPORT_PAGE = "report_page"
    VISUAL = "visual"
    DASHBOARD = "dashboard"
    DASHBOARD_TILE = "dashboard_tile"
    
    # External
    EXTERNAL_SOURCE = "external_source"
    KQL_DATABASE = "kql_database"
    
    @classmethod
    def from_fabric_type(cls, fabric_type: str) -> "AssetType":
        """Convert Fabric API item type to AssetType."""
        mapping = {
            "semanticmodel": cls.SEMANTIC_MODEL,
            "dataset": cls.SEMANTIC_MODEL,
            "report": cls.REPORT,
            "notebook": cls.NOTEBOOK,
            "pipeline": cls.PIPELINE,
            "datapipeline": cls.PIPELINE,
            "lakehouse": cls.LAKEHOUSE,
            "warehouse": cls.WAREHOUSE,
            "dataflow": cls.DATAFLOW,
            "dashboard": cls.DASHBOARD,
            "eventstream": cls.EVENTSTREAM,
            "kqldatabase": cls.KQL_DATABASE,
        }
        normalized = fabric_type.lower().replace(" ", "").replace("gen2", "")
        return mapping.get(normalized, cls.EXTERNAL_SOURCE)
    
    @property
    def category(self) -> str:
        """Get the category for this asset type."""
        data_layer = {self.LAKEHOUSE, self.TABLE, self.SHORTCUT, self.COLUMN, self.WAREHOUSE}
        compute_layer = {self.NOTEBOOK, self.PIPELINE, self.DATAFLOW, self.SPARK_JOB, self.EVENTSTREAM}
        bi_layer = {self.SEMANTIC_MODEL, self.MEASURE, self.CALCULATED_COLUMN, self.RELATIONSHIP, self.HIERARCHY}
        presentation_layer = {self.REPORT, self.REPORT_PAGE, self.VISUAL, self.DASHBOARD, self.DASHBOARD_TILE}
        
        if self in data_layer:
            return "data"
        elif self in compute_layer:
            return "compute"
        elif self in bi_layer:
            return "bi"
        elif self in presentation_layer:
            return "presentation"
        else:
            return "other"


class DependencyType(str, Enum):
    """
    Types of dependencies between assets.
    
    Categories:
    - Data flow: How data moves between assets
    - Compute: How compute assets orchestrate each other
    - BI: How BI assets reference each other
    - Containment: Parent-child relationships
    """
    
    # Data flow dependencies
    READS_FROM = "reads_from"
    WRITES_TO = "writes_to"
    SHORTCUT_TO = "shortcut_to"
    COPIES_FROM = "copies_from"
    
    # Compute dependencies
    EXECUTES = "executes"
    TRIGGERS = "triggers"
    CALLS = "calls"
    SCHEDULES = "schedules"
    
    # BI dependencies
    USES_MODEL = "uses_model"
    REFERENCES_MEASURE = "references_measure"
    REFERENCES_TABLE = "references_table"
    REFERENCES_COLUMN = "references_column"
    USES_RELATIONSHIP = "uses_relationship"
    
    # Containment
    CONTAINS = "contains"
    PART_OF = "part_of"
    DEFINED_IN = "defined_in"
    
    @property
    def is_data_flow(self) -> bool:
        return self in {self.READS_FROM, self.WRITES_TO, self.SHORTCUT_TO, self.COPIES_FROM}
    
    @property
    def is_breaking(self) -> bool:
        """Is this dependency type typically breaking if the source changes?"""
        return self in {
            self.READS_FROM,
            self.SHORTCUT_TO,
            self.USES_MODEL,
            self.REFERENCES_MEASURE,
            self.REFERENCES_TABLE,
            self.REFERENCES_COLUMN,
        }


class RiskLevel(str, Enum):
    """
    Risk levels for impact assessment.
    
    Levels:
    - NONE: No impact
    - LOW: Minor impact, safe to proceed
    - MEDIUM: Some impact, review recommended
    - HIGH: Significant impact, approval required
    - CRITICAL: Severe impact, executive approval required
    """
    
    NONE = "none"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
    
    @classmethod
    def from_score(cls, score: int) -> "RiskLevel":
        """Convert risk score (0-100) to risk level."""
        if score >= 80:
            return cls.CRITICAL
        elif score >= 60:
            return cls.HIGH
        elif score >= 40:
            return cls.MEDIUM
        elif score >= 20:
            return cls.LOW
        else:
            return cls.NONE
    
    @property
    def requires_approval(self) -> bool:
        return self in {self.HIGH, self.CRITICAL}
    
    @property
    def color(self) -> str:
        """Get color for UI display."""
        colors = {
            self.NONE: "#95a5a6",
            self.LOW: "#2ecc71",
            self.MEDIUM: "#f39c12",
            self.HIGH: "#e74c3c",
            self.CRITICAL: "#8e44ad",
        }
        return colors.get(self, "#95a5a6")
    
    @property
    def emoji(self) -> str:
        """Get emoji for display."""
        emojis = {
            self.NONE: "⚪",
            self.LOW: "🟢",
            self.MEDIUM: "🟡",
            self.HIGH: "🔴",
            self.CRITICAL: "⛔",
        }
        return emojis.get(self, "❓")


@dataclass
class LineageNode:
    """
    A node in the lineage graph representing a Fabric asset.
    
    Attributes:
        id: Unique identifier (usually the Fabric item ID)
        name: Display name of the asset
        asset_type: Type of the asset (table, measure, report, etc.)
        workspace_id: ID of the containing workspace
        workspace_name: Name of the containing workspace
        parent_id: ID of the parent asset (e.g., model ID for a measure)
        metadata: Additional asset-specific data
    """
    
    id: str
    name: str
    asset_type: AssetType
    workspace_id: Optional[str] = None
    workspace_name: Optional[str] = None
    parent_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __hash__(self):
        return hash(self.id)
    
    def __eq__(self, other):
        if isinstance(other, LineageNode):
            return self.id == other.id
        return False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "asset_type": self.asset_type.value,
            "workspace_id": self.workspace_id,
            "workspace_name": self.workspace_name,
            "parent_id": self.parent_id,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LineageNode":
        return cls(
            id=data["id"],
            name=data["name"],
            asset_type=AssetType(data["asset_type"]),
            workspace_id=data.get("workspace_id"),
            workspace_name=data.get("workspace_name"),
            parent_id=data.get("parent_id"),
            metadata=data.get("metadata", {}),
        )
    
    @property
    def qualified_name(self) -> str:
        """Get fully qualified name including workspace."""
        if self.workspace_name:
            return f"{self.workspace_name}/{self.name}"
        return self.name


@dataclass
class LineageEdge:
    """
    An edge (dependency) in the lineage graph.
    
    Attributes:
        source_id: ID of the source node
        target_id: ID of the target node
        dependency_type: Type of the dependency
        metadata: Additional edge-specific data
    """
    
    source_id: str
    target_id: str
    dependency_type: DependencyType
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "dependency_type": self.dependency_type.value,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LineageEdge":
        return cls(
            source_id=data["source_id"],
            target_id=data["target_id"],
            dependency_type=DependencyType(data["dependency_type"]),
            metadata=data.get("metadata", {}),
        )


@dataclass
class ImpactedAsset:
    """
    An asset that is impacted by a proposed change.
    
    Attributes:
        node: The affected lineage node
        impact_type: "direct" or "transitive"
        depth: Distance from the source of the change
        risk_level: Assessed risk for this asset
        can_auto_fix: Whether the asset can be automatically fixed
        fix_description: Description of the required fix
        breaking: Whether this is a breaking change for this asset
    """
    
    node: LineageNode
    impact_type: str  # "direct" or "transitive"
    depth: int
    risk_level: RiskLevel
    can_auto_fix: bool = True
    fix_description: Optional[str] = None
    breaking: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "node": self.node.to_dict(),
            "impact_type": self.impact_type,
            "depth": self.depth,
            "risk_level": self.risk_level.value,
            "can_auto_fix": self.can_auto_fix,
            "fix_description": self.fix_description,
            "breaking": self.breaking,
        }
