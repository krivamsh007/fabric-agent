"""
Fabric Lineage Engine
======================

Enterprise-grade lineage tracking across ALL Microsoft Fabric asset types.

This module provides:
- Forward lineage: What does this asset affect?
- Backward lineage: What affects this asset?
- Cross-workspace lineage via OneLake shortcuts
- Pipeline execution dependencies
- Impact analysis with risk scoring
- Auto-fix plan generation

Architecture:
    LineageEngine
    ├── GraphBuilder (builds the dependency graph)
    ├── ImpactAnalyzer (calculates risk and affected assets)
    ├── PipelineParser (extracts pipeline dependencies)
    └── AutoFixPlanner (generates fix plans)

Example:
    from fabric_agent.lineage import LineageEngine, LineageNode, AssetType
    
    engine = LineageEngine(fabric_client)
    await engine.build_graph(workspace_ids=["ws-123", "ws-456"])
    
    # Analyze impact of renaming a table
    source = LineageNode(
        id="table-789",
        name="agg_daily_sales",
        asset_type=AssetType.TABLE,
    )
    report = await engine.analyze_impact(source, change_type="rename")
    
    print(f"Risk Score: {report.risk_score}/100")
    print(f"Affected Assets: {report.total_affected}")

Author: Fabric Agent Team
"""

from fabric_agent.lineage.engine import (
    LineageEngine,
    LineageGraph,
    LineageNode,
    LineageEdge,
    ImpactReport,
    ImpactedAsset,
)
from fabric_agent.lineage.models import (
    AssetType,
    DependencyType,
    RiskLevel,
)
from fabric_agent.lineage.pipeline_parser import PipelineParser
from fabric_agent.lineage.auto_fix import AutoFixPlanner, FixPlan, FixAction

__all__ = [
    # Core engine
    "LineageEngine",
    "LineageGraph",
    "LineageNode",
    "LineageEdge",
    # Impact analysis
    "ImpactReport",
    "ImpactedAsset",
    # Models
    "AssetType",
    "DependencyType",
    "RiskLevel",
    # Pipeline parsing
    "PipelineParser",
    # Auto-fix
    "AutoFixPlanner",
    "FixPlan",
    "FixAction",
]
