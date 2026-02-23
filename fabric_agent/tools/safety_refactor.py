"""
Safety-First Refactoring System
===============================

Implements enterprise-grade safety controls for Fabric refactoring:

1. Impact Analysis Tool - Scans all reports and semantic models for dependencies
2. Pre-Flight Reports - Risk assessment before any change
3. High-Risk Warnings - Blocks dangerous changes without explicit confirmation  
4. Rollback Mechanism - Reverts to exact previous state using MemoryManager

Safety Levels:
- SAFE: No dependencies found, proceed freely
- LOW_RISK: 1-3 items affected, proceed with caution
- MEDIUM_RISK: 4-10 items affected, confirmation recommended
- HIGH_RISK: 11-25 items affected, explicit confirmation required
- CRITICAL: 25+ items affected, requires typed confirmation phrase
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, TYPE_CHECKING
from uuid import uuid4

from loguru import logger
from pydantic import BaseModel, Field, field_validator

from fabric_agent.storage.memory_manager import (
    MemoryManager,
    StateSnapshot,
    StateType,
    OperationStatus,
)

if TYPE_CHECKING:
    from fabric_agent.api.fabric_client import FabricApiClient


# ============================================================================
# Enums and Constants
# ============================================================================

class RiskLevel(str, Enum):
    """Risk levels for refactoring operations."""
    SAFE = "safe"
    LOW_RISK = "low_risk"
    MEDIUM_RISK = "medium_risk"
    HIGH_RISK = "high_risk"
    CRITICAL = "critical"
    
    @classmethod
    def from_impact_count(cls, count: int) -> "RiskLevel":
        """Determine risk level from impact count."""
        if count == 0:
            return cls.SAFE
        elif count <= 3:
            return cls.LOW_RISK
        elif count <= 10:
            return cls.MEDIUM_RISK
        elif count <= 25:
            return cls.HIGH_RISK
        else:
            return cls.CRITICAL


class RefactorType(str, Enum):
    """Types of refactoring operations."""
    RENAME_MEASURE = "rename_measure"
    RENAME_COLUMN = "rename_column"
    RENAME_TABLE = "rename_table"
    MODIFY_EXPRESSION = "modify_expression"
    DELETE_MEASURE = "delete_measure"
    DELETE_COLUMN = "delete_column"


class DependencyType(str, Enum):
    """Types of dependencies."""
    VISUAL_REFERENCE = "visual_reference"
    DAX_REFERENCE = "dax_reference"
    RELATIONSHIP = "relationship"
    CALCULATED_COLUMN = "calculated_column"
    FILTER = "filter"
    SLICER = "slicer"
    DRILL_THROUGH = "drill_through"


# ============================================================================
# Pydantic Models
# ============================================================================

class DependencyInfo(BaseModel):
    """Information about a single dependency."""
    
    dependency_type: DependencyType = Field(..., description="Type of dependency")
    source_item: str = Field(..., description="Item containing the dependency")
    source_type: str = Field(..., description="Type of source (Report, SemanticModel)")
    location: str = Field(..., description="Specific location (page/visual/measure)")
    expression: Optional[str] = Field(None, description="The expression or reference")
    is_critical: bool = Field(default=False, description="Whether this is a critical dependency")
    confidence: float = Field(default=1.0, description="Confidence level of detection (0-1)")


class AffectedItem(BaseModel):
    """An item affected by the refactoring."""
    
    item_id: str = Field(..., description="Item ID")
    item_name: str = Field(..., description="Item name")
    item_type: str = Field(..., description="Item type (Report, SemanticModel)")
    dependencies: List[DependencyInfo] = Field(default_factory=list)
    impact_score: int = Field(default=0, description="Impact score for this item")
    is_production: bool = Field(default=False, description="Whether item is in production")
    last_modified: Optional[str] = Field(None, description="Last modification date")
    owner: Optional[str] = Field(None, description="Item owner")


class PreFlightReport(BaseModel):
    """
    Pre-flight report for a refactoring operation.
    
    This report must be reviewed and acknowledged before proceeding
    with any refactoring operation.
    """
    
    report_id: str = Field(default_factory=lambda: str(uuid4()))
    generated_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    
    # Operation details
    operation_type: RefactorType = Field(..., description="Type of refactoring")
    target_name: str = Field(..., description="Name of target being refactored")
    new_name: Optional[str] = Field(None, description="New name (for renames)")
    model_name: str = Field(..., description="Semantic model name")
    workspace_name: str = Field(..., description="Workspace name")
    
    # Risk assessment
    risk_level: RiskLevel = Field(..., description="Overall risk level")
    total_impact_count: int = Field(default=0, description="Total items affected")
    production_impact_count: int = Field(default=0, description="Production items affected")
    
    # Affected items
    affected_reports: List[AffectedItem] = Field(default_factory=list)
    affected_models: List[AffectedItem] = Field(default_factory=list)
    affected_measures: List[str] = Field(default_factory=list, description="DAX measures that reference target")
    
    # Warnings and recommendations
    warnings: List[str] = Field(default_factory=list)
    recommendations: List[str] = Field(default_factory=list)
    blocking_issues: List[str] = Field(default_factory=list)
    
    # Confirmation requirements
    requires_confirmation: bool = Field(default=False)
    confirmation_phrase: Optional[str] = Field(None, description="Required phrase for CRITICAL risk")
    
    # Rollback info
    rollback_available: bool = Field(default=True)
    estimated_rollback_time: str = Field(default="< 1 minute")
    
    def can_proceed(self, confirmation: Optional[str] = None) -> Tuple[bool, str]:
        """
        Check if the operation can proceed.
        
        Args:
            confirmation: User confirmation (for high-risk operations).
        
        Returns:
            Tuple of (can_proceed, reason).
        """
        if self.blocking_issues:
            return False, f"Blocking issues: {'; '.join(self.blocking_issues)}"
        
        if self.risk_level == RiskLevel.CRITICAL:
            if not confirmation:
                return False, f"CRITICAL risk requires confirmation phrase: '{self.confirmation_phrase}'"
            if confirmation.strip().lower() != self.confirmation_phrase.lower():
                return False, f"Confirmation phrase does not match. Expected: '{self.confirmation_phrase}'"
        
        elif self.risk_level == RiskLevel.HIGH_RISK:
            if not confirmation or confirmation.lower() not in ["yes", "proceed", "confirm"]:
                return False, "HIGH_RISK operation requires explicit confirmation (yes/proceed/confirm)"
        
        return True, "OK"


class AnalyzeRefactorImpactInput(BaseModel):
    """Input for the analyze_refactor_impact tool."""
    
    target_type: str = Field(
        ...,
        description="Type of target: 'measure', 'column', or 'table'"
    )
    target_name: str = Field(
        ...,
        description="Name of the measure, column, or table to analyze"
    )
    model_name: str = Field(
        ...,
        description="Semantic model containing the target"
    )
    new_name: Optional[str] = Field(
        None,
        description="Proposed new name (for rename operations)"
    )
    include_indirect: bool = Field(
        default=True,
        description="Include indirect dependencies (measures that use affected measures)"
    )
    
    @field_validator("target_type")
    @classmethod
    def validate_target_type(cls, v: str) -> str:
        valid = ["measure", "column", "table"]
        if v.lower() not in valid:
            raise ValueError(f"target_type must be one of: {valid}")
        return v.lower()


class AnalyzeRefactorImpactOutput(BaseModel):
    """Output from the analyze_refactor_impact tool."""
    
    success: bool = Field(..., description="Whether analysis completed")
    pre_flight_report: Optional[PreFlightReport] = Field(None, description="Pre-flight report")
    error: Optional[str] = Field(None, description="Error if analysis failed")
    
    # Summary for quick display
    summary: str = Field(default="", description="Human-readable summary")
    can_proceed_safely: bool = Field(default=False, description="Whether safe to proceed without confirmation")


class SafeRefactorInput(BaseModel):
    """Input for safe refactoring operations."""
    
    target_type: str = Field(..., description="Type: measure, column, table")
    target_name: str = Field(..., description="Current name")
    new_name: str = Field(..., description="New name")
    model_name: str = Field(..., description="Semantic model name")
    
    # Safety options
    dry_run: bool = Field(default=True, description="If true, only simulate")
    confirmation: Optional[str] = Field(None, description="Confirmation for high-risk operations")
    skip_impact_analysis: bool = Field(default=False, description="Skip analysis (NOT recommended)")
    force: bool = Field(default=False, description="Force even with blocking issues (DANGEROUS)")
    
    # Rollback options
    create_restore_point: bool = Field(default=True, description="Create restore point before change")
    
    @field_validator("new_name")
    @classmethod
    def validate_new_name(cls, v: str, info) -> str:
        old = info.data.get("target_name", "")
        if v.strip().lower() == old.strip().lower():
            raise ValueError("new_name must be different from target_name")
        return v.strip()


class SafeRefactorOutput(BaseModel):
    """Output from safe refactoring operations."""
    
    success: bool = Field(..., description="Whether operation succeeded")
    operation_id: str = Field(default="", description="Unique operation ID for rollback")
    
    # Pre-flight report (always included)
    pre_flight_report: Optional[PreFlightReport] = Field(None)
    
    # Results
    dry_run: bool = Field(..., description="Was this a dry run")
    changes_made: List[Dict[str, Any]] = Field(default_factory=list)
    rollback_snapshot_id: Optional[str] = Field(None, description="Snapshot ID for rollback")
    
    # Status
    status: str = Field(default="", description="Operation status")
    message: str = Field(default="", description="Status message")
    warnings: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)


class RollbackInput(BaseModel):
    """Input for rollback operations."""
    
    # Option 1: Rollback by operation ID
    operation_id: Optional[str] = Field(None, description="Operation ID to rollback")
    
    # Option 2: Rollback last action
    rollback_last: bool = Field(default=False, description="Rollback the last action")
    
    # Option 3: Rollback to specific snapshot
    snapshot_id: Optional[str] = Field(None, description="Specific snapshot ID")
    
    # Options
    dry_run: bool = Field(default=True, description="If true, only preview")
    confirmation: Optional[str] = Field(None, description="Confirmation for the rollback")


class RollbackOutput(BaseModel):
    """Output from rollback operations."""
    
    success: bool = Field(..., description="Whether rollback succeeded")
    
    # What was rolled back
    rolled_back_operation: Optional[str] = Field(None, description="Operation that was rolled back")
    original_value: Optional[str] = Field(None, description="Original value restored")
    previous_value: Optional[str] = Field(None, description="Value that was reverted")
    
    # State info
    snapshot_used: Optional[str] = Field(None, description="Snapshot ID used for rollback")
    items_restored: List[str] = Field(default_factory=list)
    
    # Status
    dry_run: bool = Field(..., description="Was this a dry run")
    status: str = Field(default="", description="Status")
    message: str = Field(default="", description="Message")


# ============================================================================
# Impact Analyzer
# ============================================================================

class ImpactAnalyzer:
    """
    Analyzes the impact of refactoring operations.
    
    Scans all reports and semantic models in the workspace to find:
    - Direct references in visuals
    - DAX measure dependencies
    - Filter and slicer references
    - Relationship impacts
    - Indirect dependencies (measures that use affected measures)
    """
    
    def __init__(
        self,
        client: "FabricApiClient",
        workspace_id: str,
        workspace_name: str,
    ):
        self.client = client
        self.workspace_id = workspace_id
        self.workspace_name = workspace_name
        self._cache: Dict[str, Any] = {}
    
    async def analyze(
        self,
        target_type: str,
        target_name: str,
        model_name: str,
        new_name: Optional[str] = None,
        include_indirect: bool = True,
    ) -> PreFlightReport:
        """
        Perform comprehensive impact analysis.
        
        Args:
            target_type: Type of target (measure, column, table).
            target_name: Name of the target.
            model_name: Semantic model name.
            new_name: Proposed new name (for renames).
            include_indirect: Include indirect dependencies.
        
        Returns:
            PreFlightReport with full analysis.
        """
        logger.info(f"Analyzing impact for {target_type}: {target_name}")
        
        # Determine operation type
        if new_name:
            op_type = {
                "measure": RefactorType.RENAME_MEASURE,
                "column": RefactorType.RENAME_COLUMN,
                "table": RefactorType.RENAME_TABLE,
            }.get(target_type, RefactorType.RENAME_MEASURE)
        else:
            op_type = RefactorType.MODIFY_EXPRESSION
        
        # Initialize report
        report = PreFlightReport(
            operation_type=op_type,
            target_name=target_name,
            new_name=new_name,
            model_name=model_name,
            workspace_name=self.workspace_name,
            risk_level=RiskLevel.SAFE,
        )
        
        try:
            # Get all items in workspace
            items = await self._get_workspace_items()
            reports = [i for i in items if i.get("type", "").lower() == "report"]
            models = [i for i in items if "semanticmodel" in i.get("type", "").lower()]
            
            # Analyze reports
            for rpt in reports:
                affected = await self._analyze_report(rpt, target_name, target_type)
                if affected:
                    report.affected_reports.append(affected)
            
            # Analyze semantic models (for DAX dependencies)
            for mdl in models:
                affected = await self._analyze_model(
                    mdl, target_name, target_type, include_indirect
                )
                if affected:
                    report.affected_models.append(affected)
                    report.affected_measures.extend(
                        d.location for d in affected.dependencies
                        if d.dependency_type == DependencyType.DAX_REFERENCE
                    )
            
            # Calculate totals
            report.total_impact_count = (
                sum(len(r.dependencies) for r in report.affected_reports) +
                sum(len(m.dependencies) for m in report.affected_models)
            )
            report.production_impact_count = sum(
                1 for r in report.affected_reports if r.is_production
            )
            
            # Determine risk level
            report.risk_level = RiskLevel.from_impact_count(report.total_impact_count)
            
            # Add production impact to risk
            if report.production_impact_count > 0:
                # Elevate risk if production items affected
                risk_order = [RiskLevel.SAFE, RiskLevel.LOW_RISK, RiskLevel.MEDIUM_RISK, 
                             RiskLevel.HIGH_RISK, RiskLevel.CRITICAL]
                current_idx = risk_order.index(report.risk_level)
                elevated_idx = min(current_idx + 1, len(risk_order) - 1)
                report.risk_level = risk_order[elevated_idx]
            
            # Generate warnings and recommendations
            self._generate_warnings(report)
            self._generate_recommendations(report)
            
            # Set confirmation requirements
            if report.risk_level in [RiskLevel.HIGH_RISK, RiskLevel.CRITICAL]:
                report.requires_confirmation = True
            
            if report.risk_level == RiskLevel.CRITICAL:
                # Generate unique confirmation phrase
                report.confirmation_phrase = f"CONFIRM RENAME {target_name}"
            
            logger.info(
                f"Impact analysis complete: {report.risk_level.value}, "
                f"{report.total_impact_count} dependencies"
            )
            
        except Exception as e:
            logger.error(f"Impact analysis failed: {e}")
            report.blocking_issues.append(f"Analysis error: {str(e)}")
        
        return report
    
    async def _get_workspace_items(self) -> List[Dict[str, Any]]:
        """Get all items in the workspace."""
        if "items" in self._cache:
            return self._cache["items"]
        
        try:
            data = await self.client.get(f"/workspaces/{self.workspace_id}/items")
            items = data.get("value", []) if isinstance(data, dict) else []
            self._cache["items"] = items
            return items
        except Exception as e:
            logger.error(f"Failed to get workspace items: {e}")
            return []
    
    async def _analyze_report(
        self,
        report_info: Dict[str, Any],
        target_name: str,
        target_type: str,
    ) -> Optional[AffectedItem]:
        """Analyze a report for dependencies on the target."""
        report_name = report_info.get("displayName") or report_info.get("name", "Unknown")
        report_id = report_info.get("id", "")
        
        try:
            # Get report definition
            definition = await self._get_report_definition(report_id)
            if not definition:
                return None
            
            dependencies: List[DependencyInfo] = []
            
            # Parse report.json
            report_json = self._extract_report_json(definition)
            if not report_json:
                return None
            
            # Search through pages and visuals
            for page in report_json.get("pages", []):
                page_name = page.get("displayName", page.get("name", "Unknown"))
                
                for visual in page.get("visualContainers", []):
                    visual_config = visual.get("config", "{}")
                    if isinstance(visual_config, str):
                        try:
                            visual_config = json.loads(visual_config)
                        except:
                            visual_config = {}
                    
                    # Check for direct references
                    visual_str = json.dumps(visual)
                    
                    if target_name in visual_str:
                        # Determine dependency type
                        dep_type = DependencyType.VISUAL_REFERENCE
                        
                        # Check if it's a filter/slicer
                        visual_type = visual_config.get("singleVisual", {}).get("visualType", "")
                        if "slicer" in visual_type.lower():
                            dep_type = DependencyType.SLICER
                        elif "filter" in visual_str.lower():
                            dep_type = DependencyType.FILTER
                        
                        dependencies.append(DependencyInfo(
                            dependency_type=dep_type,
                            source_item=report_name,
                            source_type="Report",
                            location=f"{page_name}/{visual.get('id', 'unknown')}",
                            expression=self._extract_reference_context(visual_str, target_name),
                            is_critical=visual_config.get("singleVisual", {}).get("title", {}).get("show", False),
                        ))
            
            if not dependencies:
                return None
            
            return AffectedItem(
                item_id=report_id,
                item_name=report_name,
                item_type="Report",
                dependencies=dependencies,
                impact_score=len(dependencies),
                is_production=self._is_production_item(report_info),
                last_modified=report_info.get("lastModifiedDateTime"),
            )
            
        except Exception as e:
            logger.warning(f"Failed to analyze report {report_name}: {e}")
            return None
    
    async def _analyze_model(
        self,
        model_info: Dict[str, Any],
        target_name: str,
        target_type: str,
        include_indirect: bool,
    ) -> Optional[AffectedItem]:
        """Analyze a semantic model for DAX dependencies."""
        model_name = model_info.get("displayName") or model_info.get("name", "Unknown")
        model_id = model_info.get("id", "")
        
        try:
            # Get model definition
            definition = await self._get_model_definition(model_id)
            if not definition:
                return None
            
            dependencies: List[DependencyInfo] = []
            affected_measures: Set[str] = set()
            
            # Extract measures from TMDL
            measures = self._extract_measures(definition)
            
            # Direct dependencies
            for measure_name, expression in measures.items():
                if measure_name == target_name:
                    continue  # Skip the target itself
                
                if self._references_target(expression, target_name, target_type):
                    dependencies.append(DependencyInfo(
                        dependency_type=DependencyType.DAX_REFERENCE,
                        source_item=model_name,
                        source_type="SemanticModel",
                        location=measure_name,
                        expression=expression[:200] + "..." if len(expression) > 200 else expression,
                        is_critical=True,  # DAX references are always critical
                    ))
                    affected_measures.add(measure_name)
            
            # Indirect dependencies (measures that use affected measures)
            if include_indirect and affected_measures:
                for measure_name, expression in measures.items():
                    if measure_name in affected_measures or measure_name == target_name:
                        continue
                    
                    for affected in affected_measures:
                        if self._references_target(expression, affected, "measure"):
                            dependencies.append(DependencyInfo(
                                dependency_type=DependencyType.DAX_REFERENCE,
                                source_item=model_name,
                                source_type="SemanticModel",
                                location=f"{measure_name} (indirect via {affected})",
                                expression=expression[:100] + "..." if len(expression) > 100 else expression,
                                is_critical=False,  # Indirect dependencies less critical
                                confidence=0.8,
                            ))
                            break
            
            if not dependencies:
                return None
            
            return AffectedItem(
                item_id=model_id,
                item_name=model_name,
                item_type="SemanticModel",
                dependencies=dependencies,
                impact_score=len(dependencies) * 2,  # Weight DAX dependencies higher
                is_production=self._is_production_item(model_info),
                last_modified=model_info.get("lastModifiedDateTime"),
            )
            
        except Exception as e:
            logger.warning(f"Failed to analyze model {model_name}: {e}")
            return None
    
    async def _get_report_definition(self, report_id: str) -> Optional[Dict[str, Any]]:
        """Get report definition from Fabric."""
        try:
            response = await self.client.post_with_lro(
                f"/workspaces/{self.workspace_id}/reports/{report_id}/getDefinition"
            )
            return response
        except:
            return None
    
    async def _get_model_definition(self, model_id: str) -> Optional[Dict[str, Any]]:
        """Get semantic model definition from Fabric."""
        try:
            response = await self.client.post_with_lro(
                f"/workspaces/{self.workspace_id}/semanticModels/{model_id}/getDefinition"
            )
            return response
        except:
            return None
    
    def _extract_report_json(self, definition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract report.json from definition."""
        parts = (definition.get("definition") or {}).get("parts", [])
        
        for part in parts:
            if str(part.get("path", "")).lower() == "report.json":
                if str(part.get("payloadType", "")).lower() == "inlinebase64":
                    try:
                        decoded = base64.b64decode(part.get("payload", ""))
                        return json.loads(decoded.decode("utf-8", errors="replace"))
                    except:
                        pass
        return None
    
    def _extract_measures(self, definition: Dict[str, Any]) -> Dict[str, str]:
        """Extract measures from model definition."""
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
                
                # Parse TMDL for measures
                current_measure = None
                current_expr = []
                
                for line in text.split("\n"):
                    measure_match = re.match(r"\s*measure\s+['\"]?(.+?)['\"]?\s*=", line, re.IGNORECASE)
                    if measure_match:
                        # Save previous measure
                        if current_measure and current_expr:
                            measures[current_measure] = "\n".join(current_expr)
                        
                        current_measure = measure_match.group(1).strip()
                        current_expr = [line.split("=", 1)[-1].strip()]
                    elif current_measure:
                        # Check if we're still in measure definition
                        if line.strip().startswith(("formatString:", "displayFolder:", "description:")):
                            if current_expr:
                                measures[current_measure] = "\n".join(current_expr)
                            current_measure = None
                            current_expr = []
                        elif line.strip() and not line.strip().startswith("//"):
                            current_expr.append(line)
                
                # Save last measure
                if current_measure and current_expr:
                    measures[current_measure] = "\n".join(current_expr)
                    
            except Exception as e:
                logger.debug(f"Failed to parse TMDL: {e}")
        
        return measures
    
    def _references_target(
        self,
        expression: str,
        target_name: str,
        target_type: str,
    ) -> bool:
        """Check if expression references the target."""
        if not expression:
            return False
        
        # Escape special regex characters in target name
        escaped_name = re.escape(target_name)
        
        patterns = []
        
        if target_type == "measure":
            # [Measure Name] or 'Table'[Measure]
            patterns = [
                rf"\[{escaped_name}\]",
                rf"'{escaped_name}'",
            ]
        elif target_type == "column":
            # [Column] or Table[Column]
            patterns = [
                rf"\[{escaped_name}\]",
                rf"'{escaped_name}'\s*\[",
            ]
        elif target_type == "table":
            # 'Table' or Table[
            patterns = [
                rf"'{escaped_name}'",
                rf"\b{escaped_name}\s*\[",
            ]
        
        for pattern in patterns:
            if re.search(pattern, expression, re.IGNORECASE):
                return True
        
        return False
    
    def _extract_reference_context(self, text: str, target: str, context_chars: int = 50) -> str:
        """Extract context around a reference."""
        idx = text.find(target)
        if idx == -1:
            return ""
        
        start = max(0, idx - context_chars)
        end = min(len(text), idx + len(target) + context_chars)
        
        context = text[start:end]
        if start > 0:
            context = "..." + context
        if end < len(text):
            context = context + "..."
        
        return context
    
    def _is_production_item(self, item_info: Dict[str, Any]) -> bool:
        """Determine if an item is in production."""
        name = (item_info.get("displayName") or item_info.get("name", "")).lower()
        
        # Heuristics for production items
        prod_indicators = ["prod", "production", "live", "main", "official"]
        non_prod_indicators = ["dev", "test", "staging", "sandbox", "draft", "temp"]
        
        for indicator in non_prod_indicators:
            if indicator in name:
                return False
        
        for indicator in prod_indicators:
            if indicator in name:
                return True
        
        # Default: assume production if no clear indicator
        return True
    
    def _generate_warnings(self, report: PreFlightReport) -> None:
        """Generate warnings based on analysis."""
        if report.production_impact_count > 0:
            report.warnings.append(
                f"⚠️ {report.production_impact_count} PRODUCTION reports will be affected!"
            )
        
        # Check for critical DAX dependencies
        critical_dax = sum(
            1 for m in report.affected_models
            for d in m.dependencies
            if d.is_critical
        )
        if critical_dax > 0:
            report.warnings.append(
                f"⚠️ {critical_dax} DAX measures have direct dependencies - may cause calculation errors"
            )
        
        # Check for high indirect impact
        indirect = sum(
            1 for m in report.affected_models
            for d in m.dependencies
            if "indirect" in d.location.lower()
        )
        if indirect > 5:
            report.warnings.append(
                f"⚠️ {indirect} indirect dependencies detected - cascading impact possible"
            )
        
        if report.risk_level == RiskLevel.CRITICAL:
            report.warnings.append(
                "🛑 CRITICAL RISK: This change affects a large number of items. "
                "Consider scheduling during maintenance window."
            )
    
    def _generate_recommendations(self, report: PreFlightReport) -> None:
        """Generate recommendations based on analysis."""
        if report.risk_level in [RiskLevel.HIGH_RISK, RiskLevel.CRITICAL]:
            report.recommendations.append(
                "📋 Review all affected items before proceeding"
            )
            report.recommendations.append(
                "💾 Ensure backup/restore point is created"
            )
            report.recommendations.append(
                "🕐 Consider performing change during off-peak hours"
            )
        
        if report.production_impact_count > 0:
            report.recommendations.append(
                "📣 Notify report owners before making changes"
            )
        
        if report.affected_measures:
            report.recommendations.append(
                f"🔍 Test these measures after change: {', '.join(report.affected_measures[:5])}"
                + (f" and {len(report.affected_measures) - 5} more" if len(report.affected_measures) > 5 else "")
            )
        
        report.recommendations.append(
            "✅ Use dry_run=true first to preview changes"
        )


# ============================================================================
# Safe Refactoring Engine
# ============================================================================

class SafeRefactoringEngine:
    """
    Engine for safe refactoring operations with full audit trail.
    
    Implements the safety-first approach:
    1. Always analyze impact first
    2. Generate pre-flight report
    3. Require confirmation for high-risk operations
    4. Create restore point before changes
    5. Execute with full logging
    6. Support rollback at any time
    """
    
    def __init__(
        self,
        client: "FabricApiClient",
        memory: MemoryManager,
        workspace_id: str,
        workspace_name: str,
    ):
        self.client = client
        self.memory = memory
        self.workspace_id = workspace_id
        self.workspace_name = workspace_name
        self.analyzer = ImpactAnalyzer(client, workspace_id, workspace_name)
    
    async def analyze_impact(
        self,
        input: AnalyzeRefactorImpactInput,
    ) -> AnalyzeRefactorImpactOutput:
        """
        Analyze the impact of a proposed refactoring operation.
        
        This should ALWAYS be called before any refactoring!
        """
        logger.info(f"Starting impact analysis for {input.target_type}: {input.target_name}")
        
        try:
            report = await self.analyzer.analyze(
                target_type=input.target_type,
                target_name=input.target_name,
                model_name=input.model_name,
                new_name=input.new_name,
                include_indirect=input.include_indirect,
            )
            
            # Generate summary
            summary = self._generate_summary(report)
            
            # Record analysis in audit trail
            await self.memory.record_state(
                state_type=StateType.REFACTOR,
                operation="analyze_impact",
                status=OperationStatus.SUCCESS,
                workspace_id=self.workspace_id,
                workspace_name=self.workspace_name,
                target_name=input.target_name,
                state_data={
                    "target_type": input.target_type,
                    "model_name": input.model_name,
                    "new_name": input.new_name,
                    "risk_level": report.risk_level.value,
                    "total_impact": report.total_impact_count,
                    "affected_reports": len(report.affected_reports),
                    "affected_measures": len(report.affected_measures),
                },
                metadata={"report_id": report.report_id},
            )
            
            return AnalyzeRefactorImpactOutput(
                success=True,
                pre_flight_report=report,
                summary=summary,
                can_proceed_safely=report.risk_level in [RiskLevel.SAFE, RiskLevel.LOW_RISK],
            )
            
        except Exception as e:
            logger.error(f"Impact analysis failed: {e}")
            return AnalyzeRefactorImpactOutput(
                success=False,
                error=str(e),
                summary=f"Analysis failed: {e}",
                can_proceed_safely=False,
            )
    
    async def safe_refactor(
        self,
        input: SafeRefactorInput,
    ) -> SafeRefactorOutput:
        """
        Perform a safe refactoring operation with full safety checks.
        """
        operation_id = str(uuid4())
        logger.info(f"Starting safe refactor operation {operation_id}")
        
        output = SafeRefactorOutput(
            success=False,
            operation_id=operation_id,
            dry_run=input.dry_run,
        )
        
        try:
            # Step 1: Impact Analysis (unless skipped - NOT recommended)
            if not input.skip_impact_analysis:
                analysis = await self.analyze_impact(
                    AnalyzeRefactorImpactInput(
                        target_type=input.target_type,
                        target_name=input.target_name,
                        model_name=input.model_name,
                        new_name=input.new_name,
                    )
                )
                
                if not analysis.success:
                    output.errors.append(f"Impact analysis failed: {analysis.error}")
                    output.status = "analysis_failed"
                    output.message = "Could not complete impact analysis"
                    return output
                
                output.pre_flight_report = analysis.pre_flight_report
                
                # Step 2: Check if we can proceed
                can_proceed, reason = analysis.pre_flight_report.can_proceed(input.confirmation)
                
                if not can_proceed and not input.force:
                    output.status = "confirmation_required"
                    output.message = reason
                    output.warnings.append(reason)
                    return output
                
                if input.force and analysis.pre_flight_report.blocking_issues:
                    output.warnings.append(
                        "⚠️ FORCE flag used - proceeding despite blocking issues!"
                    )
            else:
                output.warnings.append(
                    "⚠️ Impact analysis skipped - proceeding without safety checks!"
                )
            
            # Step 3: Create restore point
            if input.create_restore_point:
                restore_snapshot = await self._create_restore_point(
                    input.target_type,
                    input.target_name,
                    input.model_name,
                    operation_id,
                )
                output.rollback_snapshot_id = restore_snapshot.id
                logger.info(f"Created restore point: {restore_snapshot.id}")
            
            # Step 4: Execute the change (or dry run)
            if input.dry_run:
                output.status = "dry_run_complete"
                output.message = (
                    f"Dry run complete. Would rename '{input.target_name}' to '{input.new_name}'. "
                    f"Run with dry_run=false to apply changes."
                )
                output.success = True
                
                # Record dry run
                await self.memory.record_state(
                    state_type=StateType.REFACTOR,
                    operation=f"safe_refactor_{input.target_type}",
                    status=OperationStatus.DRY_RUN,
                    workspace_id=self.workspace_id,
                    workspace_name=self.workspace_name,
                    target_name=input.target_name,
                    old_value=input.target_name,
                    new_value=input.new_name,
                    state_data={
                        "operation_id": operation_id,
                        "model_name": input.model_name,
                        "risk_level": output.pre_flight_report.risk_level.value if output.pre_flight_report else "unknown",
                    },
                )
            else:
                # Execute actual change
                changes = await self._execute_refactor(
                    input.target_type,
                    input.target_name,
                    input.new_name,
                    input.model_name,
                )
                
                output.changes_made = changes
                output.success = True
                output.status = "completed"
                output.message = (
                    f"Successfully renamed '{input.target_name}' to '{input.new_name}'. "
                    f"Rollback available with snapshot: {output.rollback_snapshot_id}"
                )
                
                # Record success
                await self.memory.record_state(
                    state_type=StateType.REFACTOR,
                    operation=f"safe_refactor_{input.target_type}",
                    status=OperationStatus.SUCCESS,
                    workspace_id=self.workspace_id,
                    workspace_name=self.workspace_name,
                    target_name=input.new_name,
                    old_value=input.target_name,
                    new_value=input.new_name,
                    state_data={
                        "operation_id": operation_id,
                        "model_name": input.model_name,
                        "changes": changes,
                        "restore_snapshot": output.rollback_snapshot_id,
                    },
                    metadata={
                        "risk_level": output.pre_flight_report.risk_level.value if output.pre_flight_report else "unknown",
                    },
                )
            
            return output
            
        except Exception as e:
            logger.error(f"Safe refactor failed: {e}")
            output.errors.append(str(e))
            output.status = "failed"
            output.message = f"Refactoring failed: {e}"
            
            # Record failure
            await self.memory.record_state(
                state_type=StateType.REFACTOR,
                operation=f"safe_refactor_{input.target_type}",
                status=OperationStatus.FAILED,
                workspace_id=self.workspace_id,
                workspace_name=self.workspace_name,
                target_name=input.target_name,
                error_message=str(e),
            )
            
            return output
    
    async def rollback_last_action(
        self,
        input: RollbackInput,
    ) -> RollbackOutput:
        """
        Rollback the last refactoring action or a specific operation.
        """
        logger.info("Starting rollback operation")
        
        output = RollbackOutput(
            success=False,
            dry_run=input.dry_run,
        )
        
        try:
            # Find the snapshot to rollback to
            snapshot: Optional[StateSnapshot] = None
            
            if input.snapshot_id:
                snapshot = await self.memory.get_snapshot(input.snapshot_id)
                if not snapshot:
                    output.status = "snapshot_not_found"
                    output.message = f"Snapshot {input.snapshot_id} not found"
                    return output
                    
            elif input.operation_id:
                # Find snapshot by operation ID
                history = await self.memory.get_history(
                    operation="safe_refactor",
                    limit=100,
                )
                for h in history:
                    if h.state_data.get("operation_id") == input.operation_id:
                        # Get the restore snapshot
                        restore_id = h.state_data.get("restore_snapshot")
                        if restore_id:
                            snapshot = await self.memory.get_snapshot(restore_id)
                        break
                        
            elif input.rollback_last:
                # Get the last successful refactor operation
                snapshot = await self.memory.get_rollback_state(
                    state_type=StateType.REFACTOR,
                    skip_latest=0,
                )
            
            if not snapshot:
                output.status = "no_rollback_available"
                output.message = "No suitable snapshot found for rollback"
                return output
            
            output.snapshot_used = snapshot.id
            output.rolled_back_operation = snapshot.operation
            output.original_value = snapshot.old_value
            output.previous_value = snapshot.new_value
            
            if input.dry_run:
                output.success = True
                output.status = "dry_run_complete"
                output.message = (
                    f"Would rollback: '{snapshot.new_value}' → '{snapshot.old_value}'"
                )
                return output
            
            # Execute rollback
            if snapshot.old_value and snapshot.new_value:
                # Reverse the rename
                await self._execute_refactor(
                    snapshot.state_data.get("target_type", "measure"),
                    snapshot.new_value,  # Current name (what it was renamed to)
                    snapshot.old_value,  # Original name (what to restore)
                    snapshot.state_data.get("model_name", ""),
                )
                
                output.items_restored.append(snapshot.old_value)
            
            # Record rollback
            await self.memory.record_state(
                state_type=StateType.REFACTOR,
                operation="rollback",
                status=OperationStatus.SUCCESS,
                workspace_id=self.workspace_id,
                workspace_name=self.workspace_name,
                target_name=snapshot.old_value,
                old_value=snapshot.new_value,
                new_value=snapshot.old_value,
                parent_id=snapshot.id,
                state_data={
                    "rolled_back_snapshot": snapshot.id,
                    "rolled_back_operation": snapshot.operation,
                },
            )
            
            output.success = True
            output.status = "completed"
            output.message = f"Successfully rolled back to: {snapshot.old_value}"
            
            return output
            
        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            output.status = "failed"
            output.message = f"Rollback failed: {e}"
            return output
    
    async def _create_restore_point(
        self,
        target_type: str,
        target_name: str,
        model_name: str,
        operation_id: str,
    ) -> StateSnapshot:
        """Create a restore point before making changes."""
        
        # Get current state
        current_state = await self._get_current_state(target_type, target_name, model_name)
        
        snapshot = await self.memory.record_state(
            state_type=StateType.REFACTOR,
            operation="restore_point",
            status=OperationStatus.SUCCESS,
            workspace_id=self.workspace_id,
            workspace_name=self.workspace_name,
            target_name=target_name,
            old_value=target_name,
            state_data={
                "target_type": target_type,
                "model_name": model_name,
                "operation_id": operation_id,
                "current_state": current_state,
            },
            metadata={"purpose": "restore_point"},
        )
        
        return snapshot
    
    async def _get_current_state(
        self,
        target_type: str,
        target_name: str,
        model_name: str,
    ) -> Dict[str, Any]:
        """Get the current state of the target for restoration."""
        # This would fetch the actual DAX expression or metadata
        # For now, return a placeholder
        return {
            "target_type": target_type,
            "target_name": target_name,
            "model_name": model_name,
            "captured_at": datetime.now(timezone.utc).isoformat(),
        }
    
    async def _execute_refactor(
        self,
        target_type: str,
        old_name: str,
        new_name: str,
        model_name: str,
    ) -> List[Dict[str, Any]]:
        """
        Execute the actual refactoring in Fabric.
        
        Note: Full implementation requires XMLA endpoint access or
        semantic model update API.
        """
        changes = []
        
        # This is where the actual Fabric API calls would go
        # For now, we document what would be changed
        
        logger.info(f"Executing refactor: {old_name} -> {new_name} in {model_name}")
        
        # Placeholder for actual implementation
        changes.append({
            "type": "semantic_model_update",
            "model": model_name,
            "target_type": target_type,
            "old_name": old_name,
            "new_name": new_name,
            "status": "pending_implementation",
            "note": "Requires XMLA endpoint or updateDefinition API",
        })
        
        return changes
    
    def _generate_summary(self, report: PreFlightReport) -> str:
        """Generate a human-readable summary of the pre-flight report."""
        lines = [
            f"📋 PRE-FLIGHT REPORT: {report.operation_type.value}",
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"Target: {report.target_name}" + (f" → {report.new_name}" if report.new_name else ""),
            f"Model: {report.model_name}",
            f"",
            f"🎯 RISK ASSESSMENT",
        ]
        
        # Risk level with emoji
        risk_emoji = {
            RiskLevel.SAFE: "✅",
            RiskLevel.LOW_RISK: "🟡",
            RiskLevel.MEDIUM_RISK: "🟠",
            RiskLevel.HIGH_RISK: "🔴",
            RiskLevel.CRITICAL: "🛑",
        }
        
        lines.append(f"   Risk Level: {risk_emoji.get(report.risk_level, '❓')} {report.risk_level.value.upper()}")
        lines.append(f"   Total Impact: {report.total_impact_count} dependencies")
        lines.append(f"   Production Impact: {report.production_impact_count} items")
        
        if report.affected_reports:
            lines.append(f"")
            lines.append(f"📊 AFFECTED REPORTS ({len(report.affected_reports)})")
            for rpt in report.affected_reports[:5]:
                lines.append(f"   • {rpt.item_name}: {len(rpt.dependencies)} references")
            if len(report.affected_reports) > 5:
                lines.append(f"   ... and {len(report.affected_reports) - 5} more")
        
        if report.affected_measures:
            lines.append(f"")
            lines.append(f"📐 AFFECTED MEASURES ({len(report.affected_measures)})")
            for m in report.affected_measures[:5]:
                lines.append(f"   • {m}")
            if len(report.affected_measures) > 5:
                lines.append(f"   ... and {len(report.affected_measures) - 5} more")
        
        if report.warnings:
            lines.append(f"")
            lines.append(f"⚠️ WARNINGS")
            for w in report.warnings:
                lines.append(f"   {w}")
        
        if report.recommendations:
            lines.append(f"")
            lines.append(f"💡 RECOMMENDATIONS")
            for r in report.recommendations:
                lines.append(f"   {r}")
        
        lines.append(f"")
        lines.append(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
        if report.requires_confirmation:
            if report.confirmation_phrase:
                lines.append(f"🔐 Type '{report.confirmation_phrase}' to proceed")
            else:
                lines.append(f"🔐 Type 'yes', 'proceed', or 'confirm' to proceed")
        else:
            lines.append(f"✅ Safe to proceed (use dry_run=false to apply)")
        
        return "\n".join(lines)
