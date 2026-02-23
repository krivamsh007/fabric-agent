"""
Tests for Tool Models
=====================

Tests for Pydantic input/output validation.
"""

import pytest
from pydantic import ValidationError

from fabric_agent.tools.models import (
    ListWorkspacesInput,
    SetWorkspaceInput,
    AnalyzeImpactInput,
    RenameMeasureInput,
    TargetType,
    ImpactSeverity,
)


class TestWorkspaceModels:
    """Tests for workspace-related models."""
    
    def test_list_workspaces_input_empty(self):
        """Test empty input is valid."""
        input_model = ListWorkspacesInput()
        assert input_model is not None
    
    def test_set_workspace_input_valid(self):
        """Test valid workspace name."""
        input_model = SetWorkspaceInput(workspace_name="My Workspace")
        assert input_model.workspace_name == "My Workspace"
    
    def test_set_workspace_input_strips_whitespace(self):
        """Test whitespace is stripped."""
        input_model = SetWorkspaceInput(workspace_name="  My Workspace  ")
        assert input_model.workspace_name == "My Workspace"
    
    def test_set_workspace_input_empty_fails(self):
        """Test empty name fails validation."""
        with pytest.raises(ValidationError):
            SetWorkspaceInput(workspace_name="")
    
    def test_set_workspace_input_too_long_fails(self):
        """Test name exceeding max length fails."""
        with pytest.raises(ValidationError):
            SetWorkspaceInput(workspace_name="x" * 300)


class TestImpactModels:
    """Tests for impact analysis models."""
    
    def test_analyze_impact_input_valid(self):
        """Test valid impact analysis input."""
        input_model = AnalyzeImpactInput(
            target_type=TargetType.MEASURE,
            target_name="Sales",
            model_name="Analytics",
        )
        
        assert input_model.target_type == TargetType.MEASURE
        assert input_model.target_name == "Sales"
        assert input_model.model_name == "Analytics"
    
    def test_analyze_impact_input_string_enum(self):
        """Test string converted to enum."""
        input_model = AnalyzeImpactInput(
            target_type="measure",  # type: ignore
            target_name="Sales",
        )
        
        assert input_model.target_type == TargetType.MEASURE
    
    def test_analyze_impact_invalid_type_fails(self):
        """Test invalid target type fails."""
        with pytest.raises(ValidationError):
            AnalyzeImpactInput(
                target_type="invalid",  # type: ignore
                target_name="Sales",
            )
    
    def test_impact_severity_values(self):
        """Test impact severity enum values."""
        assert ImpactSeverity.NONE.value == "none"
        assert ImpactSeverity.LOW.value == "low"
        assert ImpactSeverity.MEDIUM.value == "medium"
        assert ImpactSeverity.HIGH.value == "high"
        assert ImpactSeverity.CRITICAL.value == "critical"


class TestRefactorModels:
    """Tests for refactoring models."""
    
    def test_rename_measure_input_valid(self):
        """Test valid rename input."""
        input_model = RenameMeasureInput(
            model_name="Analytics",
            old_name="Sales",
            new_name="Revenue",
            dry_run=True,
        )
        
        assert input_model.model_name == "Analytics"
        assert input_model.old_name == "Sales"
        assert input_model.new_name == "Revenue"
        assert input_model.dry_run is True
    
    def test_rename_measure_default_dry_run(self):
        """Test dry_run defaults to True."""
        input_model = RenameMeasureInput(
            model_name="Analytics",
            old_name="Sales",
            new_name="Revenue",
        )
        
        assert input_model.dry_run is True
    
    def test_rename_measure_same_name_fails(self):
        """Test same old/new name fails."""
        with pytest.raises(ValidationError):
            RenameMeasureInput(
                model_name="Analytics",
                old_name="Sales",
                new_name="Sales",
            )
    
    def test_rename_measure_case_insensitive_same_fails(self):
        """Test case-insensitive same name fails."""
        with pytest.raises(ValidationError):
            RenameMeasureInput(
                model_name="Analytics",
                old_name="Sales",
                new_name="SALES",
            )
    
    def test_rename_measure_empty_names_fail(self):
        """Test empty names fail validation."""
        with pytest.raises(ValidationError):
            RenameMeasureInput(
                model_name="",
                old_name="Sales",
                new_name="Revenue",
            )
        
        with pytest.raises(ValidationError):
            RenameMeasureInput(
                model_name="Analytics",
                old_name="",
                new_name="Revenue",
            )
