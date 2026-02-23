"""
Lineage Engine Core
====================

The main lineage engine that builds and queries dependency graphs.

Key capabilities:
1. Build lineage graphs from Fabric workspaces
2. Trace forward/backward dependencies
3. Analyze impact of proposed changes
4. Calculate risk scores
5. Generate recommendations
"""

from __future__ import annotations

import asyncio
import base64
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4

from loguru import logger

from fabric_agent.lineage.models import (
    AssetType,
    DependencyType,
    RiskLevel,
    LineageNode,
    LineageEdge,
    ImpactedAsset,
)

if TYPE_CHECKING:
    from fabric_agent.api.fabric_client import FabricApiClient


@dataclass
class LineageGraph:
    """Complete lineage graph for one or more workspaces."""
    
    nodes: Dict[str, LineageNode] = field(default_factory=dict)
    edges: List[LineageEdge] = field(default_factory=list)
    root_id: Optional[str] = None
    workspace_ids: List[str] = field(default_factory=list)
    built_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    def add_node(self, node: LineageNode) -> None:
        self.nodes[node.id] = node
    
    def add_edge(self, edge: LineageEdge) -> None:
        self.edges.append(edge)
    
    def get_node(self, node_id: str) -> Optional[LineageNode]:
        return self.nodes.get(node_id)
    
    def get_downstream(self, node_id: str) -> List[LineageNode]:
        downstream_ids = {e.target_id for e in self.edges if e.source_id == node_id}
        return [self.nodes[nid] for nid in downstream_ids if nid in self.nodes]
    
    def get_upstream(self, node_id: str) -> List[LineageNode]:
        upstream_ids = {e.source_id for e in self.edges if e.target_id == node_id}
        return [self.nodes[nid] for nid in upstream_ids if nid in self.nodes]
    
    def get_all_downstream(self, node_id: str, max_depth: int = 10) -> Dict[str, int]:
        result: Dict[str, int] = {}
        visited: Set[str] = {node_id}
        queue: List[Tuple[str, int]] = [(node_id, 0)]
        
        while queue:
            current_id, depth = queue.pop(0)
            if depth >= max_depth:
                continue
            for downstream in self.get_downstream(current_id):
                if downstream.id not in visited:
                    visited.add(downstream.id)
                    result[downstream.id] = depth + 1
                    queue.append((downstream.id, depth + 1))
        return result
    
    def get_all_upstream(self, node_id: str, max_depth: int = 10) -> Dict[str, int]:
        result: Dict[str, int] = {}
        visited: Set[str] = {node_id}
        queue: List[Tuple[str, int]] = [(node_id, 0)]
        
        while queue:
            current_id, depth = queue.pop(0)
            if depth >= max_depth:
                continue
            for upstream in self.get_upstream(current_id):
                if upstream.id not in visited:
                    visited.add(upstream.id)
                    result[upstream.id] = depth + 1
                    queue.append((upstream.id, depth + 1))
        return result
    
    def get_nodes_by_type(self, asset_type: AssetType) -> List[LineageNode]:
        return [n for n in self.nodes.values() if n.asset_type == asset_type]
    
    @property
    def stats(self) -> Dict[str, Any]:
        type_counts: Dict[str, int] = {}
        for node in self.nodes.values():
            type_counts[node.asset_type.value] = type_counts.get(node.asset_type.value, 0) + 1
        return {
            "node_count": len(self.nodes),
            "edge_count": len(self.edges),
            "workspace_count": len(self.workspace_ids),
            "nodes_by_type": type_counts,
        }
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "nodes": [n.to_dict() for n in self.nodes.values()],
            "edges": [e.to_dict() for e in self.edges],
            "root_id": self.root_id,
            "workspace_ids": self.workspace_ids,
            "built_at": self.built_at,
            "stats": self.stats,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LineageGraph":
        graph = cls(
            root_id=data.get("root_id"),
            workspace_ids=data.get("workspace_ids", []),
            built_at=data.get("built_at", datetime.now(timezone.utc).isoformat()),
        )
        for node_data in data.get("nodes", []):
            graph.add_node(LineageNode.from_dict(node_data))
        for edge_data in data.get("edges", []):
            graph.add_edge(LineageEdge.from_dict(edge_data))
        return graph
    
    def to_mermaid(self, highlight_ids: Optional[Set[str]] = None) -> str:
        lines = ["flowchart LR"]
        highlight_ids = highlight_ids or set()
        
        ws_nodes: Dict[str, List[LineageNode]] = {}
        for node in self.nodes.values():
            ws = node.workspace_name or "default"
            if ws not in ws_nodes:
                ws_nodes[ws] = []
            ws_nodes[ws].append(node)
        
        for ws_name, nodes in ws_nodes.items():
            safe_ws = ws_name.replace(" ", "_").replace("-", "_")
            lines.append(f"    subgraph {safe_ws}[\"{ws_name}\"]")
            for node in nodes:
                shape = self._get_mermaid_shape(node.asset_type, node.name)
                lines.append(f"        {node.id}{shape}")
            lines.append("    end")
        
        for edge in self.edges:
            label = edge.dependency_type.value.replace("_", " ")
            lines.append(f"    {edge.source_id} -->|{label}| {edge.target_id}")
        
        for node_id in highlight_ids:
            lines.append(f"    style {node_id} fill:#e74c3c,color:#fff")
        
        return "\n".join(lines)
    
    def _get_mermaid_shape(self, asset_type: AssetType, name: str) -> str:
        safe_name = name.replace('"', "'").replace("[", "(").replace("]", ")")[:30]
        shapes = {
            AssetType.LAKEHOUSE: f'[("{safe_name}")]',
            AssetType.TABLE: f'["{safe_name}"]',
            AssetType.SHORTCUT: f'{{{{"{safe_name}"}}}}',
            AssetType.NOTEBOOK: f'[["{safe_name}"]]',
            AssetType.PIPELINE: f'(["{safe_name}"])',
            AssetType.SEMANTIC_MODEL: f'[/"{safe_name}"/]',
            AssetType.MEASURE: f'(("{safe_name}"))',
            AssetType.REPORT: f'>"{safe_name}"]',
        }
        return shapes.get(asset_type, f'["{safe_name}"]')


@dataclass
class ImpactReport:
    """Complete impact analysis report for a proposed change."""
    
    report_id: str = field(default_factory=lambda: f"impact_{uuid4().hex[:12]}")
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    source_asset: Optional[LineageNode] = None
    change_type: str = ""
    change_description: str = ""
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    risk_level: RiskLevel = RiskLevel.NONE
    risk_score: int = 0
    
    affected_tables: List[ImpactedAsset] = field(default_factory=list)
    affected_shortcuts: List[ImpactedAsset] = field(default_factory=list)
    affected_notebooks: List[ImpactedAsset] = field(default_factory=list)
    affected_pipelines: List[ImpactedAsset] = field(default_factory=list)
    affected_models: List[ImpactedAsset] = field(default_factory=list)
    affected_measures: List[ImpactedAsset] = field(default_factory=list)
    affected_reports: List[ImpactedAsset] = field(default_factory=list)
    affected_dashboards: List[ImpactedAsset] = field(default_factory=list)
    affected_workspaces: List[str] = field(default_factory=list)
    
    warnings: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    can_auto_fix: bool = True
    auto_fix_count: int = 0
    manual_fix_count: int = 0
    lineage_graph: Optional[LineageGraph] = None
    
    @property
    def total_affected(self) -> int:
        return sum(len(x) for x in [
            self.affected_tables, self.affected_shortcuts, self.affected_notebooks,
            self.affected_pipelines, self.affected_models, self.affected_measures,
            self.affected_reports, self.affected_dashboards,
        ])
    
    @property
    def all_affected(self) -> List[ImpactedAsset]:
        return (
            self.affected_tables + self.affected_shortcuts + self.affected_notebooks +
            self.affected_pipelines + self.affected_models + self.affected_measures +
            self.affected_reports + self.affected_dashboards
        )
    
    @property
    def direct_count(self) -> int:
        return sum(1 for a in self.all_affected if a.impact_type == "direct")
    
    @property
    def breaking_count(self) -> int:
        return sum(1 for a in self.all_affected if a.breaking)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "report_id": self.report_id,
            "generated_at": self.generated_at,
            "source_asset": self.source_asset.to_dict() if self.source_asset else None,
            "change_type": self.change_type,
            "risk_level": self.risk_level.value,
            "risk_score": self.risk_score,
            "summary": {
                "total_affected": self.total_affected,
                "direct_count": self.direct_count,
                "breaking_count": self.breaking_count,
                "workspaces": len(self.affected_workspaces),
            },
            "warnings": self.warnings,
            "recommendations": self.recommendations,
            "auto_fix_count": self.auto_fix_count,
            "manual_fix_count": self.manual_fix_count,
        }
    
    def to_markdown(self) -> str:
        lines = []
        source_name = self.source_asset.name if self.source_asset else "Unknown"
        
        lines.append(f"# 🔍 Impact Analysis: `{source_name}`")
        lines.append("")
        lines.append(f"**Risk Level:** {self.risk_level.emoji} **{self.risk_level.value.upper()}** (Score: {self.risk_score}/100)")
        lines.append("")
        lines.append("## 📊 Impact Summary")
        lines.append("")
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        lines.append(f"| Total Affected | {self.total_affected} |")
        lines.append(f"| Direct Impact | {self.direct_count} |")
        lines.append(f"| Workspaces | {len(self.affected_workspaces)} |")
        lines.append(f"| Auto-Fixable | {self.auto_fix_count} |")
        lines.append("")
        
        if self.warnings:
            lines.append("## ⚠️ Warnings")
            for w in self.warnings:
                lines.append(f"- {w}")
            lines.append("")
        
        if self.recommendations:
            lines.append("## 💡 Recommendations")
            for r in self.recommendations:
                lines.append(f"- {r}")
        
        return "\n".join(lines)


class LineageEngine:
    """Unified lineage engine for Microsoft Fabric."""
    
    def __init__(self, fabric_client: "FabricApiClient"):
        self.client = fabric_client
        self._graph: Optional[LineageGraph] = None
        self._workspace_cache: Dict[str, Dict[str, Any]] = {}
    
    @property
    def graph(self) -> Optional[LineageGraph]:
        return self._graph
    
    async def build_graph(
        self,
        workspace_ids: List[str],
        include_measures: bool = True,
        include_pipelines: bool = True,
    ) -> LineageGraph:
        logger.info(f"Building lineage graph for {len(workspace_ids)} workspaces")
        
        graph = LineageGraph(workspace_ids=list(workspace_ids))
        
        for ws_id in workspace_ids:
            await self._build_workspace_graph(graph, ws_id, include_measures, include_pipelines)
        
        await self._build_cross_workspace_edges(graph)
        self._graph = graph
        
        logger.info(f"Graph built: {graph.stats['node_count']} nodes, {graph.stats['edge_count']} edges")
        return graph
    
    async def trace_forward(self, source: LineageNode, max_depth: int = 10) -> LineageGraph:
        if self._graph is None:
            raise RuntimeError("Graph not built. Call build_graph() first.")
        
        result = LineageGraph(root_id=source.id)
        result.add_node(source)
        
        downstream = self._graph.get_all_downstream(source.id, max_depth)
        for node_id in downstream:
            node = self._graph.get_node(node_id)
            if node:
                result.add_node(node)
        
        relevant_ids = {source.id} | set(downstream.keys())
        for edge in self._graph.edges:
            if edge.source_id in relevant_ids and edge.target_id in relevant_ids:
                result.add_edge(edge)
        
        return result
    
    async def trace_backward(self, target: LineageNode, max_depth: int = 10) -> LineageGraph:
        if self._graph is None:
            raise RuntimeError("Graph not built. Call build_graph() first.")
        
        result = LineageGraph(root_id=target.id)
        result.add_node(target)
        
        upstream = self._graph.get_all_upstream(target.id, max_depth)
        for node_id in upstream:
            node = self._graph.get_node(node_id)
            if node:
                result.add_node(node)
        
        relevant_ids = {target.id} | set(upstream.keys())
        for edge in self._graph.edges:
            if edge.source_id in relevant_ids and edge.target_id in relevant_ids:
                result.add_edge(edge)
        
        return result
    
    async def analyze_impact(
        self,
        source: LineageNode,
        change_type: str,
        change_description: str = "",
        old_value: Optional[str] = None,
        new_value: Optional[str] = None,
    ) -> ImpactReport:
        logger.info(f"Analyzing impact: {change_type} on {source.name}")
        
        report = ImpactReport(
            source_asset=source,
            change_type=change_type,
            change_description=change_description or f"{change_type} {source.name}",
            old_value=old_value,
            new_value=new_value,
        )
        
        if self._graph is None and source.workspace_id:
            await self.build_graph([source.workspace_id])
        
        lineage = await self.trace_forward(source, max_depth=10)
        report.lineage_graph = lineage
        
        workspaces: Set[str] = set()
        downstream = self._graph.get_all_downstream(source.id, max_depth=10) if self._graph else {}
        
        for node_id, depth in downstream.items():
            node = lineage.get_node(node_id)
            if not node:
                continue
            
            impact_type = "direct" if depth == 1 else "transitive"
            risk = self._calculate_node_risk(node, depth, change_type)
            can_auto_fix, fix_desc = self._can_auto_fix(node, change_type)
            breaking = self._is_breaking_change(node, change_type)
            
            impacted = ImpactedAsset(
                node=node,
                impact_type=impact_type,
                depth=depth,
                risk_level=risk,
                can_auto_fix=can_auto_fix,
                fix_description=fix_desc,
                breaking=breaking,
            )
            
            self._categorize_impacted_asset(report, impacted)
            
            if node.workspace_name:
                workspaces.add(node.workspace_name)
        
        report.affected_workspaces = sorted(workspaces)
        report.risk_score = self._calculate_overall_risk(report)
        report.risk_level = RiskLevel.from_score(report.risk_score)
        report.auto_fix_count = sum(1 for a in report.all_affected if a.can_auto_fix)
        report.manual_fix_count = len(report.all_affected) - report.auto_fix_count
        report.can_auto_fix = report.manual_fix_count == 0
        
        self._generate_warnings(report)
        self._generate_recommendations(report)
        
        return report
    
    async def _build_workspace_graph(
        self, graph: LineageGraph, workspace_id: str,
        include_measures: bool, include_pipelines: bool,
    ) -> None:
        logger.info(f"Building graph for workspace {workspace_id}")
        
        try:
            ws_info = await self.client.get(f"/workspaces/{workspace_id}")
            ws_name = ws_info.get("displayName", workspace_id)
        except Exception:
            ws_name = workspace_id
        
        ws_node = LineageNode(
            id=workspace_id, name=ws_name, asset_type=AssetType.WORKSPACE,
            workspace_id=workspace_id, workspace_name=ws_name,
        )
        graph.add_node(ws_node)
        
        try:
            items_resp = await self.client.get(f"/workspaces/{workspace_id}/items")
            items = self._extract_items(items_resp)
        except Exception as e:
            logger.warning(f"Could not get items: {e}")
            return
        
        for item in items:
            await self._process_item(graph, workspace_id, ws_name, item, include_measures, include_pipelines)
    
    async def _process_item(
        self, graph: LineageGraph, workspace_id: str, workspace_name: str,
        item: Dict[str, Any], include_measures: bool, include_pipelines: bool,
    ) -> None:
        item_id = item.get("id", "")
        item_name = item.get("displayName", "")
        item_type = item.get("type", "")
        asset_type = AssetType.from_fabric_type(item_type)
        
        node = LineageNode(
            id=item_id, name=item_name, asset_type=asset_type,
            workspace_id=workspace_id, workspace_name=workspace_name,
            parent_id=workspace_id, metadata={"fabric_type": item_type},
        )
        graph.add_node(node)
        graph.add_edge(LineageEdge(source_id=workspace_id, target_id=item_id, dependency_type=DependencyType.CONTAINS))
        
        if asset_type == AssetType.SEMANTIC_MODEL and include_measures:
            await self._process_semantic_model(graph, workspace_id, workspace_name, item)
        elif asset_type == AssetType.LAKEHOUSE:
            await self._process_lakehouse(graph, workspace_id, workspace_name, item)
        elif asset_type == AssetType.REPORT:
            await self._process_report(graph, workspace_id, workspace_name, item)
    
    async def _process_semantic_model(
        self, graph: LineageGraph, workspace_id: str, workspace_name: str, item: Dict[str, Any],
    ) -> None:
        """
        Parse a semantic model's TMDL definition to discover measures AND source table names.

        FAANG PARALLEL: This is identical to what LinkedIn's DataHub does when crawling Power BI
        datasets — it reads the dataset definition to extract both computed metrics (measures)
        and the raw tables they join.  The table names are stored in node metadata so that
        _build_model_source_edges() can wire the table → model edges after all nodes exist.
        """
        model_id = item.get("id", "")
        model_name = item.get("displayName", "")
        source_table_names: List[str] = []

        try:
            definition = await self.client.post_with_lro(
                f"/workspaces/{workspace_id}/semanticModels/{model_id}/getDefinition",
                params={"format": "TMDL"},
            )
            measures = self._parse_measures_from_definition(definition)
            source_table_names = self._parse_table_names_from_definition(definition)

            for measure in measures:
                measure_node = LineageNode(
                    id=f"{model_id}:{measure['name']}",
                    name=measure["name"],
                    asset_type=AssetType.MEASURE,
                    workspace_id=workspace_id,
                    workspace_name=workspace_name,
                    parent_id=model_id,
                    metadata={"expression": measure.get("expression", ""), "model_name": model_name},
                )
                graph.add_node(measure_node)
                graph.add_edge(LineageEdge(
                    source_id=model_id, target_id=measure_node.id,
                    dependency_type=DependencyType.CONTAINS,
                ))

                for ref in self._parse_measure_references(measure.get("expression", "")):
                    ref_id = f"{model_id}:{ref}"
                    graph.add_edge(LineageEdge(
                        source_id=ref_id, target_id=measure_node.id,
                        dependency_type=DependencyType.REFERENCES_MEASURE,
                    ))
        except Exception as e:
            logger.debug(f"Could not process semantic model {model_name}: {e}")

        # Store source table names for _build_model_source_edges() deferred pass
        model_node = graph.get_node(model_id)
        if model_node is not None:
            model_node.metadata["source_table_names"] = source_table_names

    async def _process_report(
        self, graph: LineageGraph, workspace_id: str, workspace_name: str, item: Dict[str, Any],
    ) -> None:
        """
        Discover the report → semantic model edge via the Power BI REST API.

        The Fabric REST API (/v1) does not expose a report's bound dataset directly,
        but the Power BI REST API (api.powerbi.com/v1.0/myorg) returns `datasetId`
        on GET .../reports/{id}.  Both APIs accept the same Entra Bearer token.

        FAANG PARALLEL: Meta's Superset scanner does the same thing — each chart
        references a dataset/datasource, so scanning the chart metadata gives you
        the full presentation-to-storage lineage graph edge.
        """
        report_id = item.get("id", "")
        report_name = item.get("displayName", "")

        try:
            headers = await self.client._get_headers()
            resp = await self.client._client.get(
                f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}",
                headers=headers,
            )
            if resp.status_code == 200:
                dataset_id = resp.json().get("datasetId", "")
                if dataset_id and dataset_id in graph.nodes:
                    # semantic_model (upstream) → report (downstream)
                    graph.add_edge(LineageEdge(
                        source_id=dataset_id,
                        target_id=report_id,
                        dependency_type=DependencyType.USES_MODEL,
                        metadata={"report_name": report_name, "cross_api": "powerbi"},
                    ))
                    logger.debug(f"Report→model edge: {report_name} → {dataset_id}")
        except Exception as e:
            logger.debug(f"Could not get report→model link for {report_name}: {e}")
    
    async def _process_lakehouse(
        self, graph: LineageGraph, workspace_id: str, workspace_name: str, item: Dict[str, Any],
    ) -> None:
        """
        Discover tables and shortcuts inside a lakehouse and add them as graph nodes.

        FAANG PARALLEL: Google Cloud Dataplex scans storage assets (GCS buckets, BigQuery tables,
        and external table references) as part of its data catalog. Shortcuts in Microsoft Fabric
        are the equivalent of BigQuery external tables or Hive EXTERNAL TABLE definitions —
        they are pointers to data stored in another location.  Capturing them as nodes here is
        what enables downstream cross-workspace blast radius analysis.
        """
        lakehouse_id = item.get("id", "")
        lakehouse_name = item.get("displayName", "")

        # ── Tables ────────────────────────────────────────────────────────────
        try:
            tables_resp = await self.client.get(
                f"/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables",
                params={"maxResults": 100},
            )
            for table in self._extract_items(tables_resp):
                table_name = table.get("name", "")
                table_node = LineageNode(
                    id=f"{lakehouse_id}:table:{table_name}",
                    name=table_name,
                    asset_type=AssetType.TABLE,
                    workspace_id=workspace_id,
                    workspace_name=workspace_name,
                    parent_id=lakehouse_id,
                    metadata={"lakehouse": lakehouse_name},
                )
                graph.add_node(table_node)
                graph.add_edge(LineageEdge(
                    source_id=lakehouse_id, target_id=table_node.id,
                    dependency_type=DependencyType.CONTAINS,
                ))
        except Exception as e:
            logger.debug(f"Could not process tables for lakehouse {lakehouse_name}: {e}")

        # ── Shortcuts ─────────────────────────────────────────────────────────
        # GET /v1/workspaces/{ws}/items/{lh}/shortcuts
        # Each shortcut is a virtual pointer to a table in another workspace/lakehouse.
        # We add a SHORTCUT node so _build_cross_workspace_edges() can later wire
        # the cross-workspace edge: source_table → shortcut.
        try:
            sc_resp = await self.client.get(
                f"/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts"
            )
            for sc in self._extract_items(sc_resp):
                sc_name = sc.get("name", "") or sc.get("shortcutName", "")
                if not sc_name:
                    continue
                shortcut_node = LineageNode(
                    id=f"{lakehouse_id}:shortcut:{sc_name}",
                    name=sc_name,
                    asset_type=AssetType.SHORTCUT,
                    workspace_id=workspace_id,
                    workspace_name=workspace_name,
                    parent_id=lakehouse_id,
                    metadata={
                        "target": sc.get("target", {}),
                        "path": sc.get("path", ""),
                        "lakehouse_name": lakehouse_name,
                        "shortcut_id": sc.get("id", ""),
                    },
                )
                graph.add_node(shortcut_node)
                graph.add_edge(LineageEdge(
                    source_id=lakehouse_id, target_id=shortcut_node.id,
                    dependency_type=DependencyType.CONTAINS,
                ))
        except Exception as e:
            logger.debug(f"Could not get shortcuts for lakehouse {lakehouse_name}: {e}")
    
    async def _build_cross_workspace_edges(self, graph: LineageGraph) -> None:
        """
        Wire deferred cross-asset edges after all nodes are in the graph:
        1. source_table → shortcut  (cross-workspace data flow)
        2. table/shortcut → semantic_model  (model reads from table/shortcut)

        FAANG PARALLEL: Airbnb's Minerva runs a similar "link pass" after scanning all datasets —
        it matches dataset consumers to their source tables using a reverse-index of table names.
        Running this as a second pass (rather than inline) avoids ordering issues where the
        source node doesn't exist yet when the consumer node is processed.
        """
        # ── 1. Shortcut → source_table cross-workspace edges ──────────────────
        for shortcut in graph.get_nodes_by_type(AssetType.SHORTCUT):
            target = shortcut.metadata.get("target", {}).get("oneLake", {})
            if target:
                target_item_id = target.get("itemId", "")
                target_path = target.get("path", "")
                if target_path and "Tables/" in target_path:
                    table_name = target_path.split("Tables/")[-1]
                    target_node_id = f"{target_item_id}:table:{table_name}"
                    if target_node_id in graph.nodes:
                        graph.add_edge(LineageEdge(
                            source_id=target_node_id, target_id=shortcut.id,
                            dependency_type=DependencyType.SHORTCUT_TO,
                            metadata={"cross_workspace": True},
                        ))

        # ── 2. Table/shortcut → semantic_model edges ──────────────────────────
        self._build_model_source_edges(graph)

    def _build_model_source_edges(self, graph: LineageGraph) -> None:
        """
        For each semantic model whose TMDL was parsed, find matching table/shortcut nodes
        (by name, in the same workspace) and add a READS_FROM edge.

        This covers both:
        - DataPlatform_Sales_Model reading local Silver_Curated tables
        - Enterprise_Sales_Model / Finance_Model reading shortcut-backed tables

        The table names come from metadata["source_table_names"] set during
        _process_semantic_model().
        """
        for model_node in graph.get_nodes_by_type(AssetType.SEMANTIC_MODEL):
            table_names: List[str] = model_node.metadata.get("source_table_names", [])
            if not table_names:
                continue

            ws_id = model_node.workspace_id
            # Build a name→node_id index for tables and shortcuts in the same workspace
            name_index: Dict[str, str] = {}
            for node in graph.nodes.values():
                if node.workspace_id == ws_id and node.asset_type in (AssetType.TABLE, AssetType.SHORTCUT):
                    name_index[node.name.lower()] = node.id

            for tname in table_names:
                source_id = name_index.get(tname.lower())
                if source_id:
                    # Avoid duplicate edges
                    already = any(
                        e.source_id == source_id and e.target_id == model_node.id
                        for e in graph.edges
                    )
                    if not already:
                        graph.add_edge(LineageEdge(
                            source_id=source_id,
                            target_id=model_node.id,
                            dependency_type=DependencyType.READS_FROM,
                            metadata={"table_name": tname},
                        ))
                        logger.debug(f"Model source edge: {tname} → {model_node.name}")
    
    def _parse_table_names_from_definition(self, definition: Dict[str, Any]) -> List[str]:
        """Extract table names declared in TMDL parts (e.g. 'table fact_sales')."""
        tables: List[str] = []
        for part in definition.get("definition", {}).get("parts", []):
            payload = part.get("payload", "")
            if not payload:
                continue
            try:
                content = base64.b64decode(payload).decode("utf-8", errors="replace")
                # Matches: "table fact_sales" or "table 'fact sales'"
                for match in re.finditer(r"^table\s+'?([^'\n]+?)'?\s*$", content, re.MULTILINE):
                    name = match.group(1).strip()
                    if name:
                        tables.append(name)
            except Exception:
                continue
        return tables

    def _parse_measures_from_definition(self, definition: Dict[str, Any]) -> List[Dict[str, Any]]:
        measures = []
        for part in definition.get("definition", {}).get("parts", []):
            payload = part.get("payload", "")
            if not payload:
                continue
            try:
                content = base64.b64decode(payload).decode("utf-8", errors="replace")
                pattern = r"measure\s+'([^']+)'\s*=\s*(.+?)(?=\n\s*measure|\n\s*$|\Z)"
                for name, expression in re.findall(pattern, content, re.DOTALL | re.MULTILINE):
                    measures.append({"name": name.strip(), "expression": expression.strip()})
            except Exception:
                continue
        return measures
    
    def _parse_measure_references(self, expression: str) -> List[str]:
        matches = re.findall(r'\[([^\]]+)\]', expression)
        non_measures = {"Date", "Year", "Month", "Day", "Value", "Amount"}
        return [m for m in matches if m not in non_measures and not m.startswith("@")]
    
    def _extract_items(self, response: Any) -> List[Dict[str, Any]]:
        if isinstance(response, dict):
            for key in ["value", "data", "items"]:
                if key in response and isinstance(response[key], list):
                    return response[key]
        return []
    
    def _calculate_node_risk(self, node: LineageNode, depth: int, change_type: str) -> RiskLevel:
        score = 40 if depth == 1 else (25 if depth <= 3 else 10)
        type_scores = {AssetType.DASHBOARD: 30, AssetType.REPORT: 25, AssetType.MEASURE: 20}
        score += type_scores.get(node.asset_type, 10)
        if change_type == "delete":
            score += 20
        return RiskLevel.from_score(score)
    
    def _can_auto_fix(self, node: LineageNode, change_type: str) -> Tuple[bool, Optional[str]]:
        if change_type == "rename":
            if node.asset_type == AssetType.MEASURE:
                return True, "Update measure reference"
            elif node.asset_type == AssetType.NOTEBOOK:
                return False, "Manual code review required"
        return True, None
    
    def _is_breaking_change(self, node: LineageNode, change_type: str) -> bool:
        return change_type == "delete" or (change_type == "rename" and node.asset_type in {AssetType.MEASURE, AssetType.REPORT})
    
    def _categorize_impacted_asset(self, report: ImpactReport, asset: ImpactedAsset) -> None:
        mapping = {
            AssetType.TABLE: report.affected_tables,
            AssetType.SHORTCUT: report.affected_shortcuts,
            AssetType.NOTEBOOK: report.affected_notebooks,
            AssetType.PIPELINE: report.affected_pipelines,
            AssetType.SEMANTIC_MODEL: report.affected_models,
            AssetType.MEASURE: report.affected_measures,
            AssetType.REPORT: report.affected_reports,
            AssetType.DASHBOARD: report.affected_dashboards,
        }
        target_list = mapping.get(asset.node.asset_type)
        if target_list is not None:
            target_list.append(asset)
    
    def _calculate_overall_risk(self, report: ImpactReport) -> int:
        score = min(report.total_affected * 2, 30)
        score += min(report.direct_count * 5, 20)
        score += min(len(report.affected_reports) * 8, 20)
        score += min(len(report.affected_workspaces) * 5, 15)
        score += min(report.breaking_count * 3, 15)
        return min(score, 100)
    
    def _generate_warnings(self, report: ImpactReport) -> None:
        if len(report.affected_measures) > 10:
            report.warnings.append(f"High number of affected measures ({len(report.affected_measures)})")
        if report.affected_dashboards:
            report.warnings.append(f"Executive dashboards affected ({len(report.affected_dashboards)})")
        if len(report.affected_workspaces) > 1:
            report.warnings.append(f"Cross-workspace impact ({len(report.affected_workspaces)} workspaces)")
        if report.manual_fix_count > 0:
            report.warnings.append(f"{report.manual_fix_count} assets require manual intervention")
    
    def _generate_recommendations(self, report: ImpactReport) -> None:
        report.recommendations.append("Create checkpoint before applying changes")
        if report.risk_score >= 60:
            report.recommendations.append("Schedule change during off-peak hours")
            report.recommendations.append("Notify affected report owners")
        if report.affected_pipelines:
            report.recommendations.append("Pause affected pipelines before making changes")
        if report.manual_fix_count > 0:
            report.recommendations.append("Review manual fix items with domain experts")
