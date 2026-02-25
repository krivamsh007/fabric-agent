"""
Workspace Graph Builder
=======================

Builds a comprehensive dependency graph for a Fabric workspace, tracking:
- Item-level dependencies (Report → SemanticModel, Pipeline → Notebook, etc.)
- Measure-level dependencies within Semantic Models
- Cross-item impact analysis for refactoring operations

The graph can be used for:
- Impact analysis before making changes
- Visualizing workspace structure
- Identifying critical dependencies
"""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from loguru import logger

if TYPE_CHECKING:
    from fabric_agent.api.fabric_client import FabricApiClient


class NodeType(str, Enum):
    """Types of nodes in the workspace graph."""
    WORKSPACE = "workspace"
    SEMANTIC_MODEL = "semantic_model"
    REPORT = "report"
    NOTEBOOK = "notebook"
    PIPELINE = "pipeline"
    LAKEHOUSE = "lakehouse"
    DATAFLOW = "dataflow"
    SHORTCUT = "shortcut"
    MEASURE = "measure"
    TABLE = "table"
    COLUMN = "column"
    DATASET = "dataset"
    UNKNOWN = "unknown"

    @classmethod
    def from_fabric_type(cls, fabric_type: str) -> "NodeType":
        """Convert Fabric item type to NodeType."""
        mapping = {
            "semanticmodel": cls.SEMANTIC_MODEL,
            "report": cls.REPORT,
            "notebook": cls.NOTEBOOK,
            "pipeline": cls.PIPELINE,
            "lakehouse": cls.LAKEHOUSE,
            "dataflow": cls.DATAFLOW,
            "shortcut": cls.SHORTCUT,
            "datapipeline": cls.PIPELINE,
            "dataset": cls.DATASET,
        }
        return mapping.get(fabric_type.lower().replace(" ", ""), cls.UNKNOWN)


class EdgeType(str, Enum):
    """Types of edges (dependencies) in the graph."""
    USES_MODEL = "uses_model"          # Report uses SemanticModel
    REFERENCES_MEASURE = "ref_measure"  # Measure references another measure
    REFERENCES_TABLE = "ref_table"      # Item references a table
    REFERENCES_COLUMN = "ref_column"    # Item references a column
    EXECUTES = "executes"               # Pipeline executes Notebook
    READS_FROM = "reads_from"           # Notebook reads from Lakehouse
    WRITES_TO = "writes_to"             # Notebook writes to Lakehouse
    TRIGGERS = "triggers"               # Pipeline triggers another pipeline
    CONTAINS = "contains"               # Model contains Table/Measure


@dataclass
class GraphNode:
    """A node in the workspace graph."""
    id: str
    name: str
    node_type: NodeType
    parent_id: Optional[str] = None  # For measures, this is the model ID
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "type": self.node_type.value,
            "parent_id": self.parent_id,
            "metadata": self.metadata,
        }


@dataclass
class GraphEdge:
    """An edge (dependency) in the workspace graph."""
    source_id: str
    target_id: str
    edge_type: EdgeType
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source_id,
            "target": self.target_id,
            "type": self.edge_type.value,
            "metadata": self.metadata,
        }


class WorkspaceGraphBuilder:
    """
    Builds a comprehensive dependency graph for a Fabric workspace.
    
    The graph includes:
    - All workspace items (Reports, Semantic Models, Notebooks, Pipelines, etc.)
    - Measure-level dependencies within Semantic Models
    - Cross-item relationships
    
    Usage:
        >>> builder = WorkspaceGraphBuilder()
        >>> graph = await builder.build(client, workspace_id, workspace_name)
        >>> print(f"Nodes: {len(graph['nodes'])}, Edges: {len(graph['edges'])}")
    """
    
    def __init__(
        self,
        include_measure_graph: bool = True,
        max_measure_nodes_per_model: int = 600,
    ):
        """
        Initialize the graph builder.
        
        Args:
            include_measure_graph: Whether to include measure-level nodes and edges.
            max_measure_nodes_per_model: Maximum measures to include per model (for performance).
        """
        self.include_measure_graph = include_measure_graph
        self.max_measure_nodes = max_measure_nodes_per_model
        
        self._nodes: Dict[str, GraphNode] = {}
        self._edges: List[GraphEdge] = []
        self._warnings: List[str] = []

    async def _resolve_workspace_id_by_name(
        self,
        client: "FabricApiClient",
        workspace_name: str,
    ) -> Optional[str]:
        """Resolve workspace ID by display name."""
        try:
            resp = await client.get("/workspaces")
            workspaces = resp.get("value", []) if isinstance(resp, dict) else []
            for ws in workspaces:
                if (ws.get("displayName") or "").lower() == workspace_name.lower():
                    return ws.get("id")
        except Exception:
            return None
        return None
    
    async def build(
        self,
        client: "FabricApiClient",
        workspace_id: str,
        workspace_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Build the complete workspace dependency graph.
        
        Args:
            client: Initialized FabricApiClient.
            workspace_id: Workspace ID to analyze.
            workspace_name: Workspace name (for display).
        
        Returns:
            Dictionary with nodes, edges, stats, and warnings.
        """
        logger.info(f"Building workspace graph for {workspace_id}")
        
        # Reset state
        self._nodes.clear()
        self._edges.clear()
        self._warnings.clear()
        
        # Add workspace root node
        self._add_node(GraphNode(
            id=workspace_id,
            name=workspace_name or workspace_id,
            node_type=NodeType.WORKSPACE,
        ))
        
        # Get all items in workspace
        items = await self._get_workspace_items(client, workspace_id)
        logger.info(f"Found {len(items)} items in workspace")
        
        # Categorize items
        semantic_models = []
        reports = []
        notebooks = []
        pipelines = []
        lakehouses = []
        dataflows = []
        others = []
        
        for item in items:
            item_type = (item.get("type") or "").lower().replace(" ", "")
            
            if "semanticmodel" in item_type or item_type == "dataset":
                semantic_models.append(item)
            elif item_type == "report":
                reports.append(item)
            elif item_type == "notebook":
                notebooks.append(item)
            elif item_type in ("pipeline", "datapipeline"):
                pipelines.append(item)
            elif item_type == "lakehouse":
                lakehouses.append(item)
            elif item_type == "dataflow":
                dataflows.append(item)
            else:
                others.append(item)
        
        # Add item nodes
        for item in items:
            self._add_item_node(item, workspace_id)

        # Build lakehouse internals first (tables, shortcuts) so downstream
        # dependency builders can connect to concrete table nodes.
        for lakehouse in lakehouses:
            await self._build_lakehouse_dependencies(client, workspace_id, lakehouse)
        
        # Build semantic model internals (measures, tables)
        for model in semantic_models:
            await self._build_model_graph(client, workspace_id, model)
        
        # Build report dependencies
        for report in reports:
            await self._build_report_dependencies(client, workspace_id, report)
        
        # Build notebook dependencies
        for notebook in notebooks:
            await self._build_notebook_dependencies(client, workspace_id, notebook, lakehouses)
        
        # Build pipeline dependencies
        for pipeline in pipelines:
            await self._build_pipeline_dependencies(client, workspace_id, pipeline, notebooks)
        
        # Build dataflow dependencies
        for dataflow in dataflows:
            await self._build_dataflow_dependencies(client, workspace_id, dataflow)
        
        # Compile result
        graph = {
            "workspace_id": workspace_id,
            "workspace_name": workspace_name,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "nodes": [n.to_dict() for n in self._nodes.values()],
            "edges": [e.to_dict() for e in self._edges],
            "stats": {
                "nodes": len(self._nodes),
                "edges": len(self._edges),
                "semantic_models": len(semantic_models),
                "reports": len(reports),
                "notebooks": len(notebooks),
                "pipelines": len(pipelines),
                "lakehouses": len(lakehouses),
                "warnings": len(self._warnings),
            },
            "warnings": self._warnings,
        }
        
        logger.info(
            f"Graph built: {len(self._nodes)} nodes, {len(self._edges)} edges, "
            f"{len(self._warnings)} warnings"
        )
        
        return graph
    
    def _add_node(self, node: GraphNode) -> None:
        """Add a node if it doesn't exist."""
        if node.id not in self._nodes:
            self._nodes[node.id] = node
    
    def _add_edge(self, edge: GraphEdge) -> None:
        """Add an edge."""
        self._edges.append(edge)
    
    def _add_item_node(self, item: Dict[str, Any], workspace_id: str) -> None:
        """Add a workspace item as a node."""
        item_id = item.get("id", "")
        item_name = item.get("displayName") or item.get("name", "Unknown")
        item_type = item.get("type", "Unknown")
        
        self._add_node(GraphNode(
            id=item_id,
            name=item_name,
            node_type=NodeType.from_fabric_type(item_type),
            parent_id=workspace_id,
            metadata={
                "description": item.get("description"),
                "last_modified": item.get("lastModifiedDateTime"),
                "fabric_type": item_type,
            },
        ))
        
        # Add contains edge from workspace
        self._add_edge(GraphEdge(
            source_id=workspace_id,
            target_id=item_id,
            edge_type=EdgeType.CONTAINS,
        ))
    
    async def _get_workspace_items(
        self,
        client: "FabricApiClient",
        workspace_id: str,
    ) -> List[Dict[str, Any]]:
        """Get all items in the workspace."""
        try:
            data = await client.get(f"/workspaces/{workspace_id}/items")
            return data.get("value", []) if isinstance(data, dict) else []
        except Exception as e:
            self._warnings.append(f"Failed to get workspace items: {e}")
            return []
    
    async def _build_model_graph(
        self,
        client: "FabricApiClient",
        workspace_id: str,
        model: Dict[str, Any],
    ) -> None:
        """Build the internal graph for a semantic model (tables, measures)."""
        model_id = model.get("id", "")
        model_name = model.get("displayName") or model.get("name", "Unknown")
        
        try:
            # Get model definition
            definition = await self._get_model_definition(client, workspace_id, model_id)
            if not definition:
                return
            
            # Extract tables and measures
            measures = self._extract_measures_from_definition(definition)
            model_tables = self._extract_model_tables_from_definition(definition)
            
            if self.include_measure_graph:
                # Limit measures per model
                measure_names = list(measures.keys())[:self.max_measure_nodes]
                
                # Add measure nodes
                for measure_name in measure_names:
                    measure_id = f"{model_id}:measure:{measure_name}"
                    self._add_node(GraphNode(
                        id=measure_id,
                        name=measure_name,
                        node_type=NodeType.MEASURE,
                        parent_id=model_id,
                        metadata={
                            "expression": measures[measure_name][:500] if measures[measure_name] else None,
                        },
                    ))
                    
                    # Add contains edge from model
                    self._add_edge(GraphEdge(
                        source_id=model_id,
                        target_id=measure_id,
                        edge_type=EdgeType.CONTAINS,
                    ))
                
                # Build measure-to-measure dependencies
                for measure_name in measure_names:
                    expression = measures.get(measure_name, "")
                    refs = self._find_measure_references(expression, measure_names)
                    
                    source_id = f"{model_id}:measure:{measure_name}"
                    for ref_name in refs:
                        if ref_name != measure_name:
                            target_id = f"{model_id}:measure:{ref_name}"
                            self._add_edge(GraphEdge(
                                source_id=source_id,
                                target_id=target_id,
                                edge_type=EdgeType.REFERENCES_MEASURE,
                            ))

            # Add model -> table edges when table names match discovered lakehouse tables.
            if model_tables:
                table_name_index: Dict[str, List[GraphNode]] = {}
                for node in self._nodes.values():
                    if node.node_type == NodeType.TABLE:
                        table_name_index.setdefault(node.name.lower(), []).append(node)

                for table_name in model_tables:
                    for table_node in table_name_index.get(table_name.lower(), []):
                        self._add_edge(GraphEdge(
                            source_id=model_id,
                            target_id=table_node.id,
                            edge_type=EdgeType.REFERENCES_TABLE,
                            metadata={"table_name": table_name, "source": "semantic_model_definition"},
                        ))
            
            if self.include_measure_graph and len(measures) > self.max_measure_nodes:
                self._warnings.append(
                    f"Model '{model_name}' has {len(measures)} measures, "
                    f"only showing first {self.max_measure_nodes}"
                )
                
        except Exception as e:
            self._warnings.append(f"Failed to build model graph for '{model_name}': {e}")

    async def _build_lakehouse_dependencies(
        self,
        client: "FabricApiClient",
        workspace_id: str,
        lakehouse: Dict[str, Any],
    ) -> None:
        """Build lakehouse internals (tables + shortcuts)."""
        lakehouse_id = lakehouse.get("id", "")
        lakehouse_name = lakehouse.get("displayName") or lakehouse.get("name", "Unknown")

        if not lakehouse_id:
            return

        try:
            tables = await self._get_lakehouse_tables(client, workspace_id, lakehouse_id)

            for table in tables:
                table_name = (
                    table.get("name")
                    or table.get("displayName")
                    or table.get("tableName")
                    or table.get("table")
                )
                if not table_name:
                    continue

                table_id = f"{lakehouse_id}:table:{table_name}"
                self._add_node(GraphNode(
                    id=table_id,
                    name=str(table_name),
                    node_type=NodeType.TABLE,
                    parent_id=lakehouse_id,
                    metadata={
                        "workspace_id": workspace_id,
                        "lakehouse_id": lakehouse_id,
                        "lakehouse_name": lakehouse_name,
                        "schema": table.get("columns") or table.get("schema") or [],
                        "last_refresh": (
                            table.get("lastRefreshTime")
                            or table.get("lastUpdatedTime")
                            or table.get("updatedDateTime")
                            or table.get("lastModifiedDateTime")
                        ),
                        "pipeline_id": table.get("pipelineId"),
                        "pipeline_name": table.get("pipelineName"),
                    },
                ))
                self._add_edge(GraphEdge(
                    source_id=lakehouse_id,
                    target_id=table_id,
                    edge_type=EdgeType.CONTAINS,
                ))

            shortcuts = await self._get_lakehouse_shortcuts(client, workspace_id, lakehouse_id)
            for shortcut in shortcuts:
                shortcut_name = shortcut.get("name") or shortcut.get("displayName")
                if not shortcut_name:
                    continue

                shortcut_id = f"{lakehouse_id}:shortcut:{shortcut_name}"
                self._add_node(GraphNode(
                    id=shortcut_id,
                    name=str(shortcut_name),
                    node_type=NodeType.SHORTCUT,
                    parent_id=lakehouse_id,
                    metadata=shortcut,
                ))
                self._add_edge(GraphEdge(
                    source_id=lakehouse_id,
                    target_id=shortcut_id,
                    edge_type=EdgeType.CONTAINS,
                ))

                target_table_id = self._resolve_shortcut_target_table_id(shortcut)
                if target_table_id:
                    self._add_edge(GraphEdge(
                        source_id=shortcut_id,
                        target_id=target_table_id,
                        edge_type=EdgeType.REFERENCES_TABLE,
                            metadata={"source": "shortcut_target"},
                    ))

                    # Add a consumer-side table node linked to the shortcut target.
                    # This makes reverse traversals include consumer models/reports
                    # that reference the shortcut-projected table by name.
                    target_table_name = target_table_id.split(":table:", 1)[-1]
                    consumer_table_id = f"{lakehouse_id}:table:{target_table_name}"
                    self._add_node(GraphNode(
                        id=consumer_table_id,
                        name=str(target_table_name),
                        node_type=NodeType.TABLE,
                        parent_id=lakehouse_id,
                        metadata={
                            "workspace_id": workspace_id,
                            "lakehouse_id": lakehouse_id,
                            "lakehouse_name": lakehouse_name,
                            "source": "shortcut_projection",
                        },
                    ))
                    self._add_edge(GraphEdge(
                        source_id=lakehouse_id,
                        target_id=consumer_table_id,
                        edge_type=EdgeType.CONTAINS,
                        metadata={"source": "shortcut_projection"},
                    ))
                    self._add_edge(GraphEdge(
                        source_id=consumer_table_id,
                        target_id=shortcut_id,
                        edge_type=EdgeType.REFERENCES_TABLE,
                        metadata={"source": "shortcut_projection"},
                    ))

        except Exception as e:
            self._warnings.append(f"Failed to build lakehouse dependencies for '{lakehouse_name}': {e}")
    
    async def _build_report_dependencies(
        self,
        client: "FabricApiClient",
        workspace_id: str,
        report: Dict[str, Any],
    ) -> None:
        """Build dependencies for a report (which models it uses)."""
        report_id = report.get("id", "")
        report_name = report.get("displayName") or report.get("name", "Unknown")
        
        try:
            # Get report definition to find linked semantic model
            definition = await self._get_report_definition(client, workspace_id, report_id)
            if not definition:
                return
            
            # Extract semantic model reference
            model_id = self._extract_model_reference_from_report(definition)
            
            if model_id:
                # Check if model exists in our nodes
                if model_id in self._nodes:
                    self._add_edge(GraphEdge(
                        source_id=report_id,
                        target_id=model_id,
                        edge_type=EdgeType.USES_MODEL,
                    ))
                else:
                    # Model might be external - add as external reference
                    self._add_node(GraphNode(
                        id=model_id,
                        name=f"External Model ({model_id[:8]}...)",
                        node_type=NodeType.SEMANTIC_MODEL,
                        metadata={"external": True},
                    ))
                    self._add_edge(GraphEdge(
                        source_id=report_id,
                        target_id=model_id,
                        edge_type=EdgeType.USES_MODEL,
                    ))
                    
        except Exception as e:
            self._warnings.append(f"Failed to build report dependencies for '{report_name}': {e}")
    
    async def _build_notebook_dependencies(
        self,
        client: "FabricApiClient",
        workspace_id: str,
        notebook: Dict[str, Any],
        lakehouses: List[Dict[str, Any]],
    ) -> None:
        """Build dependencies for a notebook (which lakehouses it uses)."""
        notebook_id = notebook.get("id", "")
        notebook_name = notebook.get("displayName") or notebook.get("name", "Unknown")
        
        try:
            # Get notebook definition
            definition = await self._get_notebook_definition(client, workspace_id, notebook_id)
            if not definition:
                return
            
            # Extract lakehouse references from notebook content
            content = self._extract_notebook_content(definition)
            
            # Look for lakehouse references in the code
            for lakehouse in lakehouses:
                lh_name = lakehouse.get("displayName") or lakehouse.get("name", "")
                lh_id = lakehouse.get("id", "")
                
                if lh_name and (lh_name in content or lh_id in content):
                    self._add_edge(GraphEdge(
                        source_id=notebook_id,
                        target_id=lh_id,
                        edge_type=EdgeType.READS_FROM,
                        metadata={"lakehouse_name": lh_name},
                    ))

            # Add notebook -> table edges when table names appear in notebook code.
            table_nodes = [n for n in self._nodes.values() if n.node_type == NodeType.TABLE]
            content_lower = content.lower()
            for table_node in table_nodes:
                table_name = table_node.name
                if table_name and table_name.lower() in content_lower:
                    self._add_edge(GraphEdge(
                        source_id=notebook_id,
                        target_id=table_node.id,
                        edge_type=EdgeType.READS_FROM,
                        metadata={"table_name": table_name, "source": "notebook_content"},
                    ))
                    
        except Exception as e:
            self._warnings.append(f"Failed to build notebook dependencies for '{notebook_name}': {e}")
    
    async def _build_pipeline_dependencies(
        self,
        client: "FabricApiClient",
        workspace_id: str,
        pipeline: Dict[str, Any],
        notebooks: List[Dict[str, Any]],
    ) -> None:
        """Build dependencies for a pipeline (which notebooks/pipelines it executes)."""
        pipeline_id = pipeline.get("id", "")
        pipeline_name = pipeline.get("displayName") or pipeline.get("name", "Unknown")
        
        try:
            # Get pipeline definition
            definition = await self._get_pipeline_definition(client, workspace_id, pipeline_id)
            if not definition:
                return
            
            # Extract activities that reference notebooks
            activities = self._extract_pipeline_activities(definition)
            
            for activity in activities:
                activity_type = activity.get("type", "").lower()
                
                if activity_type == "tridentnotebook" or "notebook" in activity_type:
                    # Find referenced notebook
                    notebook_ref = activity.get("typeProperties", {}).get("notebookPath")
                    if notebook_ref:
                        # Try to match notebook by name
                        for nb in notebooks:
                            nb_name = nb.get("displayName") or nb.get("name", "")
                            if nb_name and nb_name in notebook_ref:
                                self._add_edge(GraphEdge(
                                    source_id=pipeline_id,
                                    target_id=nb.get("id", ""),
                                    edge_type=EdgeType.EXECUTES,
                                    metadata={"activity": activity.get("name")},
                                ))
                                break
                
                elif activity_type == "executepipeline":
                    # Pipeline triggers another pipeline
                    ref_pipeline = activity.get("typeProperties", {}).get("pipeline", {})
                    ref_name = ref_pipeline.get("referenceName")
                    if ref_name:
                        # Try to find the referenced pipeline
                        for node in self._nodes.values():
                            if node.node_type == NodeType.PIPELINE and node.name == ref_name:
                                self._add_edge(GraphEdge(
                                    source_id=pipeline_id,
                                    target_id=node.id,
                                    edge_type=EdgeType.TRIGGERS,
                                ))
                                break

            # Add pipeline -> table edges from content scan for table-name references.
            definition_text = self._extract_definition_text(definition)
            if definition_text:
                content_lower = definition_text.lower()
                for node in self._nodes.values():
                    if node.node_type != NodeType.TABLE:
                        continue
                    if node.name.lower() in content_lower:
                        self._add_edge(GraphEdge(
                            source_id=pipeline_id,
                            target_id=node.id,
                            edge_type=EdgeType.REFERENCES_TABLE,
                            metadata={"table_name": node.name, "source": "pipeline_definition"},
                        ))
                    
        except Exception as e:
            self._warnings.append(f"Failed to build pipeline dependencies for '{pipeline_name}': {e}")
    
    async def _build_dataflow_dependencies(
        self,
        client: "FabricApiClient",
        workspace_id: str,
        dataflow: Dict[str, Any],
    ) -> None:
        """Build dependencies for a dataflow."""
        # Dataflow dependencies would require parsing M queries
        # For now, we just add a placeholder
        pass
    
    async def _get_model_definition(
        self,
        client: "FabricApiClient",
        workspace_id: str,
        model_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get semantic model definition."""
        try:
            response = await client.post_with_lro(
                f"/workspaces/{workspace_id}/semanticModels/{model_id}/getDefinition"
            )
            return response
        except Exception:
            return None
    
    async def _get_report_definition(
        self,
        client: "FabricApiClient",
        workspace_id: str,
        report_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get report definition."""
        try:
            response = await client.post_with_lro(
                f"/workspaces/{workspace_id}/reports/{report_id}/getDefinition"
            )
            return response
        except Exception:
            return None
    
    async def _get_notebook_definition(
        self,
        client: "FabricApiClient",
        workspace_id: str,
        notebook_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get notebook definition."""
        try:
            response = await client.post_with_lro(
                f"/workspaces/{workspace_id}/notebooks/{notebook_id}/getDefinition"
            )
            return response
        except Exception:
            return None
    
    async def _get_pipeline_definition(
        self,
        client: "FabricApiClient",
        workspace_id: str,
        pipeline_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get pipeline definition."""
        try:
            response = await client.post_with_lro(
                f"/workspaces/{workspace_id}/dataPipelines/{pipeline_id}/getDefinition"
            )
            return response
        except Exception:
            return None

    async def _get_lakehouse_tables(
        self,
        client: "FabricApiClient",
        workspace_id: str,
        lakehouse_id: str,
    ) -> List[Dict[str, Any]]:
        """Get lakehouse table list."""
        try:
            resp = await client.get(
                f"/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables",
                params={"maxResults": 200},
            )
            if isinstance(resp, dict):
                return (
                    resp.get("value")
                    or resp.get("data")
                    or resp.get("tables")
                    or []
                )
            return []
        except Exception:
            return []

    async def _get_lakehouse_shortcuts(
        self,
        client: "FabricApiClient",
        workspace_id: str,
        lakehouse_id: str,
    ) -> List[Dict[str, Any]]:
        """Get shortcut list for a lakehouse item."""
        try:
            resp = await client.get(
                f"/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts"
            )
            if isinstance(resp, dict):
                return (
                    resp.get("value")
                    or resp.get("data")
                    or resp.get("shortcuts")
                    or []
                )
            return []
        except Exception:
            return []
    
    def _extract_measures_from_definition(
        self,
        definition: Dict[str, Any],
    ) -> Dict[str, str]:
        """Extract measures from model definition (TMDL format)."""
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
                
                current_measure = None
                current_expr = []
                
                for line in text.split("\n"):
                    measure_match = re.match(r"\s*measure\s+['\"]?(.+?)['\"]?\s*=", line, re.IGNORECASE)
                    if measure_match:
                        if current_measure and current_expr:
                            measures[current_measure] = "\n".join(current_expr)
                        
                        current_measure = measure_match.group(1).strip()
                        current_expr = [line.split("=", 1)[-1].strip()]
                    elif current_measure:
                        if line.strip().startswith(("formatString:", "displayFolder:", "description:")):
                            if current_expr:
                                measures[current_measure] = "\n".join(current_expr)
                            current_measure = None
                            current_expr = []
                        elif line.strip() and not line.strip().startswith("//"):
                            current_expr.append(line)
                
                if current_measure and current_expr:
                    measures[current_measure] = "\n".join(current_expr)
                    
            except Exception:
                pass
        
        return measures

    def _extract_model_tables_from_definition(
        self,
        definition: Dict[str, Any],
    ) -> Set[str]:
        """Extract semantic-model table names from TMDL parts."""
        table_names: Set[str] = set()
        parts = (definition.get("definition") or {}).get("parts", [])

        for part in parts:
            path = str(part.get("path", ""))
            path_lower = path.lower()
            if not path_lower.endswith(".tmdl"):
                continue

            # Common TMDL path pattern includes /tables/<TableName>.tmdl
            path_match = re.search(r"(?:^|/|\\\\)tables(?:/|\\\\)([^/\\\\]+?)\.tmdl$", path_lower, re.IGNORECASE)
            if path_match:
                table_names.add(path_match.group(1))

            if str(part.get("payloadType", "")).lower() != "inlinebase64":
                continue

            try:
                payload = part.get("payload", "")
                text = base64.b64decode(payload).decode("utf-8", errors="replace")
                for line in text.split("\n"):
                    match = re.match(r"\s*table\s+['\"]?(.+?)['\"]?\s*$", line, re.IGNORECASE)
                    if match:
                        table_names.add(match.group(1).strip())
            except Exception:
                continue

        return table_names
    
    def _find_measure_references(
        self,
        expression: str,
        all_measures: List[str],
    ) -> Set[str]:
        """Find measure references in a DAX expression."""
        refs = set()
        
        for measure_name in all_measures:
            # Look for [MeasureName] pattern
            pattern = rf"\[{re.escape(measure_name)}\]"
            if re.search(pattern, expression, re.IGNORECASE):
                refs.add(measure_name)
        
        return refs
    
    def _extract_model_reference_from_report(
        self,
        definition: Dict[str, Any],
    ) -> Optional[str]:
        """Extract semantic model ID from report definition."""
        parts = (definition.get("definition") or {}).get("parts", [])
        
        for part in parts:
            path = str(part.get("path", "")).lower()
            
            # Look for definition.pbir which contains model reference
            if path == "definition.pbir":
                if str(part.get("payloadType", "")).lower() == "inlinebase64":
                    try:
                        decoded = base64.b64decode(part.get("payload", ""))
                        content = json.loads(decoded.decode("utf-8", errors="replace"))
                        
                        # Extract datasetReference
                        dataset_ref = content.get("datasetReference", {})
                        by_path = dataset_ref.get("byPath", {})
                        
                        # Try to get the model ID
                        path_val = by_path.get("path")
                        if path_val and "/" in path_val:
                            # Path format: ../../SemanticModel/ModelName.SemanticModel
                            # or similar - extract what we can
                            pass
                        
                        # Try byConnection
                        by_connection = dataset_ref.get("byConnection", {})
                        conn_string = by_connection.get("connectionString")
                        if conn_string:
                            # Modern report bindings often store explicit semantic model ID.
                            sem_model_match = re.search(r"semanticmodelid=([^;]+)", conn_string, re.IGNORECASE)
                            if sem_model_match:
                                return sem_model_match.group(1).strip()

                            # Parse connection string for dataset ID
                            match = re.search(r"Data Source=.*?;Initial Catalog=([^;]+)", conn_string)
                            if match:
                                return match.group(1)
                        
                    except Exception:
                        pass
        
        return None
    
    def _extract_notebook_content(self, definition: Dict[str, Any]) -> str:
        """Extract notebook content as text."""
        parts = (definition.get("definition") or {}).get("parts", [])
        content = []
        
        for part in parts:
            if str(part.get("payloadType", "")).lower() == "inlinebase64":
                try:
                    decoded = base64.b64decode(part.get("payload", ""))
                    text = decoded.decode("utf-8", errors="replace")
                    content.append(text)
                except Exception:
                    pass
        
        return "\n".join(content)

    def _extract_definition_text(self, definition: Dict[str, Any]) -> str:
        """Decode all inline-base64 definition parts into text for broad scans."""
        parts = (definition.get("definition") or {}).get("parts", [])
        decoded_parts: List[str] = []
        for part in parts:
            if str(part.get("payloadType", "")).lower() != "inlinebase64":
                continue
            try:
                decoded = base64.b64decode(part.get("payload", ""))
                decoded_parts.append(decoded.decode("utf-8", errors="replace"))
            except Exception:
                continue
        return "\n".join(decoded_parts)

    def _resolve_shortcut_target_table_id(self, shortcut: Dict[str, Any]) -> Optional[str]:
        """Resolve shortcut target to a local table node id when possible."""
        target = shortcut.get("target", {})
        one_lake = target.get("oneLake", {}) if isinstance(target, dict) else {}
        source_item_id = one_lake.get("itemId")
        source_path = one_lake.get("path", "") or ""
        table_name = None
        if isinstance(source_path, str):
            match = re.search(r"tables[/\\\\]([^/\\\\]+)$", source_path, re.IGNORECASE)
            if match:
                table_name = match.group(1)

        if source_item_id and table_name:
            return f"{source_item_id}:table:{table_name}"
        return None


class MultiWorkspaceGraphBuilder:
    """
    Build a unified dependency graph across multiple Fabric workspaces.

    WHY:
    - A single workspace graph cannot fully model producer/consumer lineage
      through shortcuts.
    - This builder merges per-workspace graphs into one graph so impact analysis
      can traverse cross-workspace dependencies.
    """

    def __init__(
        self,
        include_measure_graph: bool = False,
        max_measure_nodes_per_model: int = 600,
    ):
        self.include_measure_graph = include_measure_graph
        self.max_measure_nodes = max_measure_nodes_per_model

    async def _list_workspaces(
        self,
        client: "FabricApiClient",
        workspace_names: Optional[List[str]] = None,
        workspace_prefixes: Optional[List[str]] = None,
        workspace_ids: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        resp = await client.get("/workspaces")
        all_ws = resp.get("value", []) if isinstance(resp, dict) else []

        names_l = {(x or "").lower() for x in (workspace_names or []) if x}
        prefixes_l = [(x or "").lower() for x in (workspace_prefixes or []) if x]
        ids_l = {str(x) for x in (workspace_ids or []) if x}

        if not names_l and not prefixes_l and not ids_l:
            return all_ws

        selected: List[Dict[str, Any]] = []
        seen: Set[str] = set()

        for ws in all_ws:
            ws_id = str(ws.get("id") or "")
            ws_name = (ws.get("displayName") or ws.get("name") or "").lower()
            if not ws_id:
                continue
            hit = False
            if ws_id in ids_l:
                hit = True
            if ws_name in names_l:
                hit = True
            if any(ws_name.startswith(p) for p in prefixes_l):
                hit = True
            if hit and ws_id not in seen:
                selected.append(ws)
                seen.add(ws_id)
        return selected

    async def build(
        self,
        client: "FabricApiClient",
        workspace_names: Optional[List[str]] = None,
        workspace_prefixes: Optional[List[str]] = None,
        workspace_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        workspaces = await self._list_workspaces(
            client,
            workspace_names=workspace_names,
            workspace_prefixes=workspace_prefixes,
            workspace_ids=workspace_ids,
        )

        merged_nodes: Dict[str, Dict[str, Any]] = {}
        merged_edges: List[Dict[str, Any]] = []
        edge_seen: Set[Tuple[str, str, str, str]] = set()
        warnings: List[str] = []

        for ws in workspaces:
            ws_id = ws.get("id")
            ws_name = ws.get("displayName") or ws.get("name") or str(ws_id)
            if not ws_id:
                continue

            builder = WorkspaceGraphBuilder(
                include_measure_graph=self.include_measure_graph,
                max_measure_nodes_per_model=self.max_measure_nodes,
            )
            graph = await builder.build(client, ws_id, str(ws_name))

            for node in graph.get("nodes", []):
                node_id = node.get("id")
                if not node_id:
                    continue
                if node_id not in merged_nodes:
                    merged_nodes[node_id] = node

            for edge in graph.get("edges", []):
                src = str(edge.get("source") or "")
                tgt = str(edge.get("target") or "")
                typ = str(edge.get("type") or "")
                meta = json.dumps(edge.get("metadata", {}), sort_keys=True)
                if not src or not tgt or not typ:
                    continue
                key = (src, tgt, typ, meta)
                if key in edge_seen:
                    continue
                edge_seen.add(key)
                merged_edges.append(edge)

            for warn in graph.get("warnings", []):
                warnings.append(f"[{ws_name}] {warn}")

        return {
            "graph_scope": "multi_workspace",
            "workspace_count": len(workspaces),
            "workspaces": [
                {
                    "id": ws.get("id"),
                    "name": ws.get("displayName") or ws.get("name"),
                }
                for ws in workspaces
            ],
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "nodes": list(merged_nodes.values()),
            "edges": merged_edges,
            "stats": {
                "nodes": len(merged_nodes),
                "edges": len(merged_edges),
                "workspaces": len(workspaces),
                "warnings": len(warnings),
            },
            "warnings": warnings,
        }
    
    def _extract_pipeline_activities(
        self,
        definition: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Extract activities from pipeline definition."""
        activities = []
        parts = (definition.get("definition") or {}).get("parts", [])
        
        for part in parts:
            path = str(part.get("path", "")).lower()
            
            if "pipeline-content.json" in path or path.endswith(".json"):
                if str(part.get("payloadType", "")).lower() == "inlinebase64":
                    try:
                        decoded = base64.b64decode(part.get("payload", ""))
                        content = json.loads(decoded.decode("utf-8", errors="replace"))
                        
                        # Extract activities
                        if isinstance(content, dict):
                            activities.extend(content.get("activities", []))
                            
                    except Exception:
                        pass
        
        return activities


# ============================================================================
# Impact Analysis using Graph
# ============================================================================

class GraphImpactAnalyzer:
    """
    Analyzes impact of changes using the workspace graph.
    
    This provides a more comprehensive impact analysis by leveraging
    the pre-built dependency graph.
    """
    
    def __init__(self, graph: Dict[str, Any]):
        """
        Initialize with a pre-built graph.
        
        Args:
            graph: Output from WorkspaceGraphBuilder.build()
        """
        self.graph = graph
        self._nodes_by_id: Dict[str, Dict[str, Any]] = {
            n["id"]: n for n in graph.get("nodes", [])
        }
        self._edges = graph.get("edges", [])
        
        # Build reverse dependency index
        self._dependents: Dict[str, List[Dict[str, Any]]] = {}
        for edge in self._edges:
            target = edge.get("target")
            if target not in self._dependents:
                self._dependents[target] = []
            self._dependents[target].append(edge)
    
    def find_dependents(
        self,
        node_id: str,
        max_depth: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Find all items that depend on the given node.
        
        Args:
            node_id: Node ID to analyze.
            max_depth: Maximum depth to traverse.
        
        Returns:
            List of dependent nodes with path information.
        """
        dependents = []
        visited = set()
        
        def traverse(current_id: str, depth: int, path: List[str]):
            if depth > max_depth or current_id in visited:
                return
            
            visited.add(current_id)
            
            for edge in self._dependents.get(current_id, []):
                source_id = edge.get("source")
                source_node = self._nodes_by_id.get(source_id)
                
                if source_node and source_id not in visited:
                    dependents.append({
                        "node": source_node,
                        "edge_type": edge.get("type"),
                        "path": path + [source_id],
                        "depth": depth,
                    })
                    traverse(source_id, depth + 1, path + [source_id])
        
        traverse(node_id, 0, [node_id])
        return dependents
    
    def analyze_measure_rename_impact(
        self,
        model_id: str,
        measure_name: str,
    ) -> Dict[str, Any]:
        """
        Analyze the impact of renaming a measure.
        
        Args:
            model_id: Semantic model ID.
            measure_name: Name of the measure to rename.
        
        Returns:
            Impact analysis with affected items.
        """
        measure_id = f"{model_id}:measure:{measure_name}"
        
        # Find direct dependents (other measures)
        dependents = self.find_dependents(measure_id)
        
        # Categorize impacts
        affected_measures = []
        affected_reports = []
        
        for dep in dependents:
            node_type = dep["node"].get("type")
            if node_type == "measure":
                affected_measures.append(dep["node"])
            elif node_type == "report":
                affected_reports.append(dep["node"])
        
        # Also check if model is used by reports
        model_dependents = self.find_dependents(model_id, max_depth=1)
        for dep in model_dependents:
            if dep["node"].get("type") == "report":
                if dep["node"] not in affected_reports:
                    affected_reports.append(dep["node"])
        
        total_impact = len(affected_measures) + len(affected_reports)
        
        return {
            "measure_name": measure_name,
            "model_id": model_id,
            "total_impact": total_impact,
            "affected_measures": affected_measures,
            "affected_reports": affected_reports,
            "risk_level": self._calculate_risk_level(total_impact),
        }
    
    def analyze_notebook_change_impact(
        self,
        notebook_id: str,
    ) -> Dict[str, Any]:
        """
        Analyze the impact of changes to a notebook.
        
        Args:
            notebook_id: Notebook ID.
        
        Returns:
            Impact analysis with affected pipelines.
        """
        dependents = self.find_dependents(notebook_id)
        
        affected_pipelines = [
            dep["node"] for dep in dependents
            if dep["node"].get("type") == "pipeline"
        ]
        
        return {
            "notebook_id": notebook_id,
            "affected_pipelines": affected_pipelines,
            "total_impact": len(affected_pipelines),
            "risk_level": self._calculate_risk_level(len(affected_pipelines)),
        }
    
    def analyze_lakehouse_change_impact(
        self,
        lakehouse_id: str,
    ) -> Dict[str, Any]:
        """
        Analyze the impact of changes to a lakehouse.
        
        Args:
            lakehouse_id: Lakehouse ID.
        
        Returns:
            Impact analysis with affected shortcuts, models, reports,
            notebooks, and pipelines.
        """
        # Table-aware impact gives a more complete business view (shortcuts/models/reports).
        table_nodes = [
            node for node in self._nodes_by_id.values()
            if node.get("type") == "table" and node.get("parent_id") == lakehouse_id
        ]

        affected_shortcuts: Dict[str, Dict[str, Any]] = {}
        affected_models: Dict[str, Dict[str, Any]] = {}
        affected_notebooks: Dict[str, Dict[str, Any]] = {}
        affected_pipelines: Dict[str, Dict[str, Any]] = {}
        affected_reports: Dict[str, Dict[str, Any]] = {}

        for table in table_nodes:
            table_impact = self.analyze_table_change_impact(table["id"])
            for item in table_impact.get("affected_shortcuts", []):
                affected_shortcuts[item["id"]] = item
            for item in table_impact.get("affected_models", []):
                affected_models[item["id"]] = item
            for item in table_impact.get("affected_notebooks", []):
                affected_notebooks[item["id"]] = item
            for item in table_impact.get("affected_pipelines", []):
                affected_pipelines[item["id"]] = item
            for item in table_impact.get("affected_reports", []):
                affected_reports[item["id"]] = item

        # Backward-compatible fallback when no table nodes are present in the graph.
        if not table_nodes:
            dependents = self.find_dependents(lakehouse_id)
            for dep in dependents:
                node = dep["node"]
                node_id = node.get("id")
                if not node_id:
                    continue
                if node.get("type") == "notebook":
                    affected_notebooks[node_id] = node
                elif node.get("type") == "pipeline":
                    affected_pipelines[node_id] = node

        total_impact = (
            len(affected_shortcuts)
            + len(affected_models)
            + len(affected_notebooks)
            + len(affected_pipelines)
            + len(affected_reports)
        )

        return {
            "lakehouse_id": lakehouse_id,
            "table_count_scanned": len(table_nodes),
            "affected_shortcuts": list(affected_shortcuts.values()),
            "affected_models": list(affected_models.values()),
            "affected_notebooks": list(affected_notebooks.values()),
            "affected_pipelines": list(affected_pipelines.values()),
            "affected_reports": list(affected_reports.values()),
            "total_impact": total_impact,
            "risk_level": self._calculate_risk_level(total_impact),
            "business_summary": {
                "upstream_data_objects": len(table_nodes),
                "consumer_shortcuts": len(affected_shortcuts),
                "consumer_models": len(affected_models),
                "consumer_reports": len(affected_reports),
                "operational_notebooks": len(affected_notebooks),
                "operational_pipelines": len(affected_pipelines),
            },
        }

    def analyze_table_change_impact(
        self,
        table_id: str,
    ) -> Dict[str, Any]:
        """
        Analyze the impact of changes to a table.

        This includes downstream shortcuts, semantic models, notebooks, pipelines,
        and reports reachable through the dependency graph.

        Args:
            table_id: Table node ID.

        Returns:
            Impact analysis with affected dependent assets.
        """
        dependents = self.find_dependents(table_id)

        affected_shortcuts: Dict[str, Dict[str, Any]] = {}
        affected_models: Dict[str, Dict[str, Any]] = {}
        affected_notebooks: Dict[str, Dict[str, Any]] = {}
        affected_pipelines: Dict[str, Dict[str, Any]] = {}
        affected_reports: Dict[str, Dict[str, Any]] = {}

        for dep in dependents:
            node = dep["node"]
            node_id = node.get("id")
            node_type = node.get("type")
            if not node_id:
                continue

            if node_type == "shortcut":
                affected_shortcuts[node_id] = node
            elif node_type == "semantic_model":
                affected_models[node_id] = node
            elif node_type == "notebook":
                affected_notebooks[node_id] = node
            elif node_type == "pipeline":
                affected_pipelines[node_id] = node
            elif node_type == "report":
                affected_reports[node_id] = node

        total_impact = (
            len(affected_shortcuts)
            + len(affected_models)
            + len(affected_notebooks)
            + len(affected_pipelines)
            + len(affected_reports)
        )

        return {
            "table_id": table_id,
            "affected_shortcuts": list(affected_shortcuts.values()),
            "affected_models": list(affected_models.values()),
            "affected_notebooks": list(affected_notebooks.values()),
            "affected_pipelines": list(affected_pipelines.values()),
            "affected_reports": list(affected_reports.values()),
            "total_impact": total_impact,
            "risk_level": self._calculate_risk_level(total_impact),
        }
    
    def _calculate_risk_level(self, impact_count: int) -> str:
        """Calculate risk level based on impact count."""
        if impact_count == 0:
            return "safe"
        elif impact_count <= 3:
            return "low_risk"
        elif impact_count <= 10:
            return "medium_risk"
        elif impact_count <= 25:
            return "high_risk"
        else:
            return "critical"
