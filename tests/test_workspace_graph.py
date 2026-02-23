"""
Tests for Workspace Graph Builder
=================================

Tests for workspace graph building and impact analysis.
"""

import pytest
from fabric_agent.tools.workspace_graph import (
    NodeType,
    EdgeType,
    GraphNode,
    GraphEdge,
    WorkspaceGraphBuilder,
    GraphImpactAnalyzer,
)


class TestNodeType:
    """Tests for NodeType enum."""
    
    def test_from_fabric_type_semantic_model(self):
        """Test semantic model type conversion."""
        assert NodeType.from_fabric_type("SemanticModel") == NodeType.SEMANTIC_MODEL
        assert NodeType.from_fabric_type("semanticmodel") == NodeType.SEMANTIC_MODEL
    
    def test_from_fabric_type_report(self):
        """Test report type conversion."""
        assert NodeType.from_fabric_type("Report") == NodeType.REPORT
        assert NodeType.from_fabric_type("report") == NodeType.REPORT
    
    def test_from_fabric_type_notebook(self):
        """Test notebook type conversion."""
        assert NodeType.from_fabric_type("Notebook") == NodeType.NOTEBOOK
    
    def test_from_fabric_type_pipeline(self):
        """Test pipeline type conversion."""
        assert NodeType.from_fabric_type("Pipeline") == NodeType.PIPELINE
        assert NodeType.from_fabric_type("DataPipeline") == NodeType.PIPELINE
    
    def test_from_fabric_type_unknown(self):
        """Test unknown type conversion."""
        assert NodeType.from_fabric_type("SomeUnknownType") == NodeType.UNKNOWN


class TestGraphNode:
    """Tests for GraphNode dataclass."""
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        node = GraphNode(
            id="test-id",
            name="Test Node",
            node_type=NodeType.REPORT,
            parent_id="parent-id",
            metadata={"key": "value"},
        )
        
        result = node.to_dict()
        
        assert result["id"] == "test-id"
        assert result["name"] == "Test Node"
        assert result["type"] == "report"
        assert result["parent_id"] == "parent-id"
        assert result["metadata"]["key"] == "value"
    
    def test_to_dict_without_optional(self):
        """Test conversion without optional fields."""
        node = GraphNode(
            id="test-id",
            name="Test Node",
            node_type=NodeType.NOTEBOOK,
        )
        
        result = node.to_dict()
        
        assert result["id"] == "test-id"
        assert result["parent_id"] is None


class TestGraphEdge:
    """Tests for GraphEdge dataclass."""
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        edge = GraphEdge(
            source_id="source",
            target_id="target",
            edge_type=EdgeType.USES_MODEL,
            metadata={"activity": "test"},
        )
        
        result = edge.to_dict()
        
        assert result["source"] == "source"
        assert result["target"] == "target"
        assert result["type"] == "uses_model"
        assert result["metadata"]["activity"] == "test"


class TestGraphImpactAnalyzer:
    """Tests for GraphImpactAnalyzer."""
    
    @pytest.fixture
    def sample_graph(self):
        """Create a sample graph for testing."""
        return {
            "workspace_id": "ws-1",
            "workspace_name": "Test Workspace",
            "nodes": [
                {"id": "ws-1", "name": "Test Workspace", "type": "workspace"},
                {"id": "model-1", "name": "Sales Model", "type": "semantic_model", "parent_id": "ws-1"},
                {"id": "model-1:measure:Sales", "name": "Sales", "type": "measure", "parent_id": "model-1"},
                {"id": "model-1:measure:Profit", "name": "Profit", "type": "measure", "parent_id": "model-1"},
                {"id": "model-1:measure:Margin", "name": "Margin", "type": "measure", "parent_id": "model-1"},
                {"id": "report-1", "name": "Sales Dashboard", "type": "report", "parent_id": "ws-1"},
                {"id": "notebook-1", "name": "ETL Notebook", "type": "notebook", "parent_id": "ws-1"},
                {"id": "pipeline-1", "name": "Daily Pipeline", "type": "pipeline", "parent_id": "ws-1"},
                {"id": "lakehouse-1", "name": "Bronze Lake", "type": "lakehouse", "parent_id": "ws-1"},
            ],
            "edges": [
                {"source": "ws-1", "target": "model-1", "type": "contains"},
                {"source": "model-1", "target": "model-1:measure:Sales", "type": "contains"},
                {"source": "model-1", "target": "model-1:measure:Profit", "type": "contains"},
                {"source": "model-1", "target": "model-1:measure:Margin", "type": "contains"},
                {"source": "model-1:measure:Margin", "target": "model-1:measure:Sales", "type": "ref_measure"},
                {"source": "model-1:measure:Margin", "target": "model-1:measure:Profit", "type": "ref_measure"},
                {"source": "report-1", "target": "model-1", "type": "uses_model"},
                {"source": "pipeline-1", "target": "notebook-1", "type": "executes"},
                {"source": "notebook-1", "target": "lakehouse-1", "type": "reads_from"},
            ],
        }
    
    def test_find_dependents_measure(self, sample_graph):
        """Test finding dependents of a measure."""
        analyzer = GraphImpactAnalyzer(sample_graph)
        
        dependents = analyzer.find_dependents("model-1:measure:Sales")
        
        # Margin depends on Sales
        names = [d["node"]["name"] for d in dependents]
        assert "Margin" in names
    
    def test_analyze_measure_rename_impact(self, sample_graph):
        """Test measure rename impact analysis."""
        analyzer = GraphImpactAnalyzer(sample_graph)
        
        impact = analyzer.analyze_measure_rename_impact("model-1", "Sales")
        
        assert impact["measure_name"] == "Sales"
        assert impact["model_id"] == "model-1"
        assert impact["total_impact"] >= 1  # At least Margin depends on it
        assert "risk_level" in impact
    
    def test_analyze_notebook_change_impact(self, sample_graph):
        """Test notebook change impact analysis."""
        analyzer = GraphImpactAnalyzer(sample_graph)
        
        impact = analyzer.analyze_notebook_change_impact("notebook-1")
        
        assert impact["notebook_id"] == "notebook-1"
        # Daily Pipeline executes this notebook
        assert len(impact["affected_pipelines"]) >= 1
    
    def test_analyze_lakehouse_change_impact(self, sample_graph):
        """Test lakehouse change impact analysis."""
        analyzer = GraphImpactAnalyzer(sample_graph)
        
        impact = analyzer.analyze_lakehouse_change_impact("lakehouse-1")
        
        assert impact["lakehouse_id"] == "lakehouse-1"
        # ETL Notebook reads from this lakehouse
        assert len(impact["affected_notebooks"]) >= 1
        # Pipeline runs the notebook
        assert len(impact["affected_pipelines"]) >= 1
    
    def test_risk_level_safe(self, sample_graph):
        """Test safe risk level calculation."""
        analyzer = GraphImpactAnalyzer(sample_graph)
        
        # Create a measure with no dependents
        assert analyzer._calculate_risk_level(0) == "safe"
    
    def test_risk_level_low(self, sample_graph):
        """Test low risk level calculation."""
        analyzer = GraphImpactAnalyzer(sample_graph)
        
        assert analyzer._calculate_risk_level(1) == "low_risk"
        assert analyzer._calculate_risk_level(3) == "low_risk"
    
    def test_risk_level_medium(self, sample_graph):
        """Test medium risk level calculation."""
        analyzer = GraphImpactAnalyzer(sample_graph)
        
        assert analyzer._calculate_risk_level(4) == "medium_risk"
        assert analyzer._calculate_risk_level(10) == "medium_risk"
    
    def test_risk_level_high(self, sample_graph):
        """Test high risk level calculation."""
        analyzer = GraphImpactAnalyzer(sample_graph)
        
        assert analyzer._calculate_risk_level(11) == "high_risk"
        assert analyzer._calculate_risk_level(25) == "high_risk"
    
    def test_risk_level_critical(self, sample_graph):
        """Test critical risk level calculation."""
        analyzer = GraphImpactAnalyzer(sample_graph)
        
        assert analyzer._calculate_risk_level(26) == "critical"
        assert analyzer._calculate_risk_level(100) == "critical"


class TestWorkspaceGraphBuilder:
    """Tests for WorkspaceGraphBuilder initialization."""
    
    def test_init_defaults(self):
        """Test default initialization."""
        builder = WorkspaceGraphBuilder()
        
        assert builder.include_measure_graph is True
        assert builder.max_measure_nodes == 600
    
    def test_init_custom(self):
        """Test custom initialization."""
        builder = WorkspaceGraphBuilder(
            include_measure_graph=False,
            max_measure_nodes_per_model=100,
        )
        
        assert builder.include_measure_graph is False
        assert builder.max_measure_nodes == 100
    
    def test_find_measure_references(self):
        """Test measure reference detection in DAX."""
        builder = WorkspaceGraphBuilder()
        
        expression = "DIVIDE([Sales], [Cost])"
        measures = ["Sales", "Cost", "Profit"]
        
        refs = builder._find_measure_references(expression, measures)
        
        assert "Sales" in refs
        assert "Cost" in refs
        assert "Profit" not in refs
    
    def test_find_measure_references_case_insensitive(self):
        """Test case-insensitive measure reference detection."""
        builder = WorkspaceGraphBuilder()
        
        expression = "DIVIDE([SALES], [cost])"
        measures = ["Sales", "Cost"]
        
        refs = builder._find_measure_references(expression, measures)
        
        assert "Sales" in refs
        assert "Cost" in refs
