"""
MCP Tools Implementation
========================

Async MCP tools with Pydantic validation, detailed docstrings,
and full audit trail integration.
"""

from __future__ import annotations

import base64
import json
import re
import time
from datetime import datetime, timezone
from uuid import uuid4
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from loguru import logger
from pydantic import ValidationError

from fabric_agent.refactor.orchestrator import RefactorOrchestrator, RefactorJsonLog, format_markdown_table

from fabric_agent.tools.models import (
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
    ImpactSeverity,
    TargetType,
    # Refactor
    RenameMeasureInput,
    RenameMeasureOutput,
    SmartRenameMeasureInput,
    SmartRenameMeasureOutput,
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
    # Healing
    ScanWorkspaceHealthInput,
    ScanWorkspaceHealthOutput,
    AnomalyInfo,
    BuildHealingPlanInput,
    BuildHealingPlanOutput,
    HealActionInfo,
    ExecuteHealingPlanInput,
    ExecuteHealingPlanOutput,
    # Memory
    FindSimilarOperationsInput,
    FindSimilarOperationsOutput,
    SimilarOperationInfo,
    GetRiskContextInput,
    RiskContextOutput,
    MemoryStatsInput,
    MemoryStatsOutput,
    ReindexMemoryInput,
    ReindexMemoryOutput,
    GetSessionSummaryInput,
    GetSessionSummaryOutput,
)
from fabric_agent.storage.memory_manager import (
    MemoryManager,
    StateSnapshot,
    StateType,
    OperationStatus,
)

if TYPE_CHECKING:
    from fabric_agent.api.fabric_client import FabricApiClient


class FabricTools:
    """
    Collection of async MCP tools for Fabric operations.
    
    All tools:
    - Are async
    - Use Pydantic models for input validation
    - Include comprehensive docstrings
    - Integrate with the audit trail via MemoryManager
    
    Example:
        >>> tools = FabricTools(client, memory_manager)
        >>> result = await tools.list_workspaces(ListWorkspacesInput())
    """
    
    def __init__(
        self,
        client: "FabricApiClient",
        memory_manager: MemoryManager,
    ):
        """
        Initialize the tools collection.
        
        Args:
            client: Initialized FabricApiClient.
            memory_manager: Initialized MemoryManager for audit trail.
        """
        self.client = client
        self.memory = memory_manager
        self.workspace_id: Optional[str] = None
        self.workspace_name: Optional[str] = None

        # Phase E: shortcut cascade plans keyed by plan_id.
        # Each entry is (plan, expires_at) — plans expire after 1 hour to prevent
        # unbounded memory growth in long-running MCP server processes.
        # MCP servers are single-process/single-session by design, so no locking needed.
        _PLAN_TTL_SECONDS = 3600
        self._shortcut_plans: Dict[str, Tuple[Any, float]] = {}
        self._plan_ttl: float = _PLAN_TTL_SECONDS

        logger.debug("FabricTools initialized")
    
    # ========================================================================
    # Workspace Tools
    # ========================================================================
    
    async def list_workspaces(
        self,
        input: ListWorkspacesInput,
    ) -> ListWorkspacesOutput:
        """
        List all accessible Fabric workspaces.
        
        This tool retrieves all workspaces the authenticated user has access to.
        Use this as the first step to explore available resources.
        
        Args:
            input: ListWorkspacesInput (no parameters required).
        
        Returns:
            ListWorkspacesOutput with workspaces list and count.
        
        Example:
            >>> result = await tools.list_workspaces(ListWorkspacesInput())
            >>> for ws in result.workspaces:
            ...     print(f"{ws.name} ({ws.id})")
        """
        logger.info("Listing Fabric workspaces")
        
        try:
            data = await self.client.get("/workspaces")
            rows = data.get("value", []) if isinstance(data, dict) else []
            
            workspaces = [
                WorkspaceInfo(
                    id=ws.get("id", ""),
                    name=ws.get("displayName") or ws.get("name", "Unknown"),
                    type=ws.get("type", "Workspace"),
                )
                for ws in rows
            ]
            
            logger.info(f"Found {len(workspaces)} workspaces")
            
            return ListWorkspacesOutput(
                workspaces=workspaces,
                count=len(workspaces),
            )
            
        except Exception as e:
            logger.error(f"Failed to list workspaces: {e}")
            return ListWorkspacesOutput(workspaces=[], count=0)
    
    async def set_workspace(
        self,
        input: SetWorkspaceInput,
    ) -> SetWorkspaceOutput:
        """
        Set the active workspace for subsequent operations.
        
        Most operations require an active workspace. Call this after
        list_workspaces to select which workspace to work with.
        
        Args:
            input: SetWorkspaceInput with workspace_name.
        
        Returns:
            SetWorkspaceOutput with workspace_id and workspace_name.
        
        Raises:
            ValueError: If workspace not found.
        
        Example:
            >>> result = await tools.set_workspace(
            ...     SetWorkspaceInput(workspace_name="Sales Analytics")
            ... )
            >>> print(f"Active workspace: {result.workspace_name}")
        """
        logger.info(f"Setting workspace to: {input.workspace_name}")
        
        data = await self.client.get("/workspaces")
        rows = data.get("value", []) if isinstance(data, dict) else []
        
        target = input.workspace_name.lower()
        match = next(
            (
                w for w in rows
                if (w.get("displayName") or w.get("name", "")).lower() == target
            ),
            None,
        )
        
        if not match:
            raise ValueError(f"Workspace '{input.workspace_name}' not found")
        
        self.workspace_id = match.get("id")
        self.workspace_name = match.get("displayName") or match.get("name")
        
        # Record to audit trail
        await self.memory.record_state(
            state_type=StateType.WORKSPACE,
            operation="set_workspace",
            status=OperationStatus.SUCCESS,
            workspace_id=self.workspace_id,
            workspace_name=self.workspace_name,
            target_name=self.workspace_name,
            new_value=self.workspace_name,
        )
        
        logger.info(f"Workspace set: {self.workspace_name} ({self.workspace_id})")
        
        return SetWorkspaceOutput(
            workspace_id=self.workspace_id,
            workspace_name=self.workspace_name,
        )
    
    async def list_items(
        self,
        input: ListItemsInput,
    ) -> ListItemsOutput:
        """
        List items in the current workspace.
        
        Returns reports, semantic models, lakehouses, notebooks, and other
        items in the active workspace. Optionally filter by item type.
        
        Args:
            input: ListItemsInput with optional item_type filter.
        
        Returns:
            ListItemsOutput with items list and count.
        
        Raises:
            RuntimeError: If no workspace is selected.
        
        Example:
            >>> # List all items
            >>> result = await tools.list_items(ListItemsInput())
            >>> 
            >>> # List only reports
            >>> result = await tools.list_items(
            ...     ListItemsInput(item_type="Report")
            ... )
        """
        if not self.workspace_id:
            raise RuntimeError("No workspace selected. Call set_workspace first.")
        
        logger.info(f"Listing items in workspace {self.workspace_name}")
        
        data = await self.client.get(f"/workspaces/{self.workspace_id}/items")
        rows = data.get("value", []) if isinstance(data, dict) else []
        
        # Apply filter if specified
        if input.item_type:
            filter_lower = input.item_type.lower()
            rows = [
                r for r in rows
                if filter_lower in str(r.get("type", "")).lower()
            ]
        
        items = [
            ItemInfo(
                id=r.get("id", ""),
                name=r.get("displayName") or r.get("name", "Unknown"),
                type=r.get("type", "Unknown"),
            )
            for r in rows
        ]
        
        logger.info(f"Found {len(items)} items")
        
        return ListItemsOutput(items=items, count=len(items))
    
    # ========================================================================
    # Semantic Model Tools
    # ========================================================================
    
    async def get_semantic_model(
        self,
        input: GetSemanticModelInput,
    ) -> GetSemanticModelOutput:
        """
        Get details of a semantic model including tables, columns, and measures.
        
        Retrieves the full definition of a semantic model in TMDL format,
        and extracts measure information for analysis.
        
        Args:
            input: GetSemanticModelInput with model_name.
        
        Returns:
            GetSemanticModelOutput with model details and measures.
        
        Raises:
            RuntimeError: If no workspace selected.
            ValueError: If model not found.
        
        Example:
            >>> result = await tools.get_semantic_model(
            ...     GetSemanticModelInput(model_name="Sales Analytics")
            ... )
            >>> for measure in result.measures:
            ...     print(f"{measure.name}: {measure.expression}")
        """
        if not self.workspace_id:
            raise RuntimeError("No workspace selected")
        
        logger.info(f"Getting semantic model: {input.model_name}")
        
        # Find the model
        items_data = await self.client.get(f"/workspaces/{self.workspace_id}/items")
        rows = items_data.get("value", []) if isinstance(items_data, dict) else []
        
        target = input.model_name.lower()
        match = next(
            (
                r for r in rows
                if (r.get("displayName") or r.get("name", "")).lower() == target
                and "semanticmodel" in str(r.get("type", "")).lower()
            ),
            None,
        )
        
        if not match:
            raise ValueError(f"Semantic model '{input.model_name}' not found")
        
        model_id = match.get("id")
        
        # Get model definition (may be LRO)
        definition = await self.client.post_with_lro(
            f"/workspaces/{self.workspace_id}/semanticModels/{model_id}/getDefinition"
        )
        
        # Extract measures from TMDL
        measures = []
        parts = (definition.get("definition") or {}).get("parts", [])
        
        for part in parts:
            path = str(part.get("path", ""))
            if not path.endswith(".tmdl"):
                continue
            if str(part.get("payloadType", "")).lower() != "inlinebase64":
                continue
            
            payload = part.get("payload", "")
            try:
                text = base64.b64decode(payload).decode("utf-8", errors="replace")
            except Exception:
                continue
            
            # Extract table name from path
            table_name = None
            table_match = re.search(r"tables/([^/]+)\.tmdl$", path)
            if table_match:
                table_name = table_match.group(1)
            
            # Find measures in TMDL
            for m in re.finditer(r"(?im)^\s*measure\s+(.+?)\s*=", text):
                raw_name = m.group(1).strip().strip("'\"")
                measures.append(MeasureInfo(name=raw_name, table=table_name))
        
        # Record to audit trail
        await self.memory.record_state(
            state_type=StateType.SEMANTIC_MODEL,
            operation="get_semantic_model",
            status=OperationStatus.SUCCESS,
            workspace_id=self.workspace_id,
            workspace_name=self.workspace_name,
            target_id=model_id,
            target_name=input.model_name,
            state_data={
                "model_id": model_id,
                "measure_count": len(measures),
                "parts_count": len(parts),
            },
        )
        
        logger.info(f"Model {input.model_name}: {len(measures)} measures, {len(parts)} parts")
        
        return GetSemanticModelOutput(
            model_id=model_id,
            model_name=match.get("displayName") or input.model_name,
            measures=measures,
            measure_count=len(measures),
            parts_count=len(parts),
            definition=definition,
        )
    
    async def get_measures(
        self,
        input: GetMeasuresInput,
    ) -> GetMeasuresOutput:
        """
        Get all DAX measures from a semantic model.
        
        Convenience wrapper around get_semantic_model that returns
        only the measures.
        
        Args:
            input: GetMeasuresInput with model_name.
        
        Returns:
            GetMeasuresOutput with measures list and count.
        
        Example:
            >>> result = await tools.get_measures(
            ...     GetMeasuresInput(model_name="Sales Analytics")
            ... )
            >>> print(f"Found {result.count} measures")
        """
        model_input = GetSemanticModelInput(model_name=input.model_name)
        model_output = await self.get_semantic_model(model_input)
        
        return GetMeasuresOutput(
            model_name=input.model_name,
            measures=model_output.measures,
            count=len(model_output.measures),
        )
    
    # ========================================================================
    # Report Tools
    # ========================================================================
    
    async def get_report_definition(
        self,
        input: GetReportDefinitionInput,
    ) -> GetReportDefinitionOutput:
        """
        Get the PBIR definition of a report including pages and visuals.
        
        Retrieves the full report definition and extracts structural
        information like pages and visual containers.
        
        Args:
            input: GetReportDefinitionInput with report_name.
        
        Returns:
            GetReportDefinitionOutput with report structure.
        
        Raises:
            RuntimeError: If no workspace selected.
            ValueError: If report not found.
        
        Example:
            >>> result = await tools.get_report_definition(
            ...     GetReportDefinitionInput(report_name="Sales Dashboard")
            ... )
            >>> print(f"Pages: {result.pages}")
            >>> print(f"Visuals: {result.visual_count}")
        """
        if not self.workspace_id:
            raise RuntimeError("No workspace selected")
        
        logger.info(f"Getting report definition: {input.report_name}")
        
        # Find the report
        items_data = await self.client.get(f"/workspaces/{self.workspace_id}/items")
        rows = items_data.get("value", []) if isinstance(items_data, dict) else []
        
        target = input.report_name.lower()
        match = next(
            (
                r for r in rows
                if (r.get("displayName") or r.get("name", "")).lower() == target
                and str(r.get("type", "")).lower() == "report"
            ),
            None,
        )
        
        if not match:
            raise ValueError(f"Report '{input.report_name}' not found")
        
        report_id = match.get("id")
        
        # Get report definition (may be LRO)
        definition_resp = await self.client.post_with_lro(
            f"/workspaces/{self.workspace_id}/reports/{report_id}/getDefinition"
        )
        
        # Decode report.json
        report_json = None
        parts = (definition_resp.get("definition") or {}).get("parts", [])
        
        for part in parts:
            if (
                str(part.get("path", "")).lower() == "report.json"
                and str(part.get("payloadType", "")).lower() == "inlinebase64"
            ):
                try:
                    decoded = base64.b64decode(part.get("payload", ""))
                    report_json = json.loads(decoded.decode("utf-8", errors="replace"))
                except Exception:
                    pass
                break
        
        # Extract pages and visuals
        pages = []
        visuals = []
        
        if isinstance(report_json, dict):
            for page in report_json.get("pages", []):
                page_name = page.get("displayName", page.get("name", "Unknown"))
                pages.append(page_name)
                
                for visual in page.get("visualContainers", []):
                    visuals.append(VisualInfo(
                        id=visual.get("id"),
                        name=visual.get("name"),
                        type=visual.get("visualType"),
                        page=page_name,
                    ))
        
        logger.info(f"Report {input.report_name}: {len(pages)} pages, {len(visuals)} visuals")
        
        return GetReportDefinitionOutput(
            report_id=report_id,
            report_name=match.get("displayName") or input.report_name,
            pages=pages,
            visuals=visuals,
            visual_count=len(visuals),
            definition=report_json,
            definition_parts=parts,
            parts_count=len(parts),
        )
    
    # ========================================================================
    # Impact Analysis Tools
    # ========================================================================
    
    async def analyze_impact(
        self,
        input: AnalyzeImpactInput,
    ) -> AnalyzeImpactOutput:
        """
        Analyze the impact of renaming a measure or column.
        
        This is THE key function for safe refactoring. It scans all reports
        and semantic models to find dependencies on the target.
        
        ALWAYS call this before any rename operation!
        
        Args:
            input: AnalyzeImpactInput with target_type, target_name, and optional model_name.
        
        Returns:
            AnalyzeImpactOutput with affected items and severity assessment.
        
        Example:
            >>> impact = await tools.analyze_impact(
            ...     AnalyzeImpactInput(
            ...         target_type=TargetType.MEASURE,
            ...         target_name="Sales",
            ...         model_name="Sales Analytics"
            ...     )
            ... )
            >>> if not impact.safe_to_rename:
            ...     print(f"WARNING: {impact.severity} impact!")
            ...     for visual in impact.affected_visuals:
            ...         print(f"  - {visual.report}/{visual.page}")
        """
        if not self.workspace_id:
            raise RuntimeError("No workspace selected")
        
        logger.info(f"Analyzing impact for {input.target_type.value}: {input.target_name}")
        
        # Get all reports
        items_data = await self.client.get(f"/workspaces/{self.workspace_id}/items")
        rows = items_data.get("value", []) if isinstance(items_data, dict) else []
        reports = [r for r in rows if str(r.get("type", "")).lower() == "report"]
        
        affected_reports: List[str] = []
        affected_visuals: List[AffectedVisual] = []
        affected_measures: List[AffectedMeasure] = []
        
        # Scan each report
        for report in reports:
            report_name = report.get("displayName") or report.get("name", "Unknown")
            
            try:
                report_def = await self.get_report_definition(
                    GetReportDefinitionInput(report_name=report_name)
                )
            except Exception as e:
                logger.warning(f"Could not analyze report {report_name}: {e}")
                continue
            
            definition = report_def.definition or {}
            
            # Search for references in visuals
            for page in definition.get("pages", []):
                page_name = page.get("displayName", page.get("name", "Unknown"))
                
                for visual in page.get("visualContainers", []):
                    visual_json = json.dumps(visual)
                    
                    if input.target_name in visual_json:
                        affected_visuals.append(AffectedVisual(
                            report=report_name,
                            page=page_name,
                            visual_id=visual.get("id"),
                            visual_name=visual.get("name"),
                            visual_type=visual.get("visualType"),
                        ))
                        
                        if report_name not in affected_reports:
                            affected_reports.append(report_name)
        
        # Check DAX dependencies if analyzing a measure
        if input.target_type == TargetType.MEASURE and input.model_name:
            try:
                model_output = await self.get_semantic_model(
                    GetSemanticModelInput(model_name=input.model_name)
                )
                
                for measure in model_output.measures:
                    expr = measure.expression or ""
                    if input.target_name in expr and measure.name != input.target_name:
                        affected_measures.append(AffectedMeasure(
                            measure_name=measure.name,
                            expression=expr,
                            dependency_type="uses",
                        ))
            except Exception as e:
                logger.warning(f"Could not analyze DAX dependencies: {e}")
        
        # Calculate severity
        total_impact = len(affected_visuals) + len(affected_measures)
        
        if total_impact == 0:
            severity = ImpactSeverity.NONE
        elif total_impact <= 3:
            severity = ImpactSeverity.LOW
        elif total_impact <= 10:
            severity = ImpactSeverity.MEDIUM
        elif total_impact <= 25:
            severity = ImpactSeverity.HIGH
        else:
            severity = ImpactSeverity.CRITICAL
        
        # Record to audit trail
        await self.memory.record_state(
            state_type=StateType.REFACTOR,
            operation="analyze_impact",
            status=OperationStatus.SUCCESS,
            workspace_id=self.workspace_id,
            workspace_name=self.workspace_name,
            target_name=input.target_name,
            state_data={
                "target_type": input.target_type.value,
                "severity": severity.value,
                "total_impact": total_impact,
                "affected_reports": affected_reports,
            },
        )
        
        logger.info(
            f"Impact analysis: {severity.value} "
            f"({total_impact} items, {len(affected_reports)} reports)"
        )
        
        return AnalyzeImpactOutput(
            target_type=input.target_type.value,
            target_name=input.target_name,
            severity=severity,
            total_impact=total_impact,
            affected_reports=affected_reports,
            affected_visuals=affected_visuals,
            affected_measures=affected_measures,
            safe_to_rename=total_impact == 0,
        )
    
    # ========================================================================
    # Refactoring Tools
    # ========================================================================
    
    async def smart_rename_measure(
        self,
        input: SmartRenameMeasureInput,
    ) -> SmartRenameMeasureOutput:
        """
        Smart, transactional measure rename.

        What it does:
        1) Builds a DAX dependency graph (SemPy-first) to find measures that reference the target.
        2) Updates *child* measures' DAX expressions so references don't break.
        3) Renames the target measure.
        4) Persists an append-only log to ./memory/refactor_log.json.
        5) Records a chained audit trail in MemoryManager (single transaction via operation_id).

        Notes:
        - Applying changes requires SemPy (semantic-link-sempy) with XMLA write enabled.
        - If SemPy is not available and dry_run=True, the tool falls back to TMDL parsing for a best-effort plan.

        Returns a professional GitHub-flavored Markdown summary in `message`.
        """
        if not self.workspace_id or not self.workspace_name:
            raise RuntimeError("No workspace selected. Call set_workspace first.")

        reasoning = input.reasoning or f"Rename measure '{input.old_name}' -> '{input.new_name}'"

        # Fetch semantic model definition (TMDL) for fallback planning (dry-run only)
        definition = None
        try:
            # Find model id
            items_data = await self.client.get(f"/workspaces/{self.workspace_id}/items")
            rows = items_data.get("value", []) if isinstance(items_data, dict) else []
            target = input.model_name.lower()
            match = next(
                (
                    r for r in rows
                    if (r.get("displayName") or r.get("name", "")).lower() == target
                    and "semanticmodel" in str(r.get("type", "")).lower()
                ),
                None,
            )
            if match and match.get("id"):
                model_id = match["id"]
                definition = await self.client.post_with_lro(
                    f"/workspaces/{self.workspace_id}/semanticModels/{model_id}/getDefinition"
                )
        except Exception as e:
            logger.debug(f"Could not fetch model definition for fallback planning: {e}")

        orchestrator = RefactorOrchestrator(self.memory, json_log=RefactorJsonLog())
        plan, snapshot_ids = await orchestrator.apply_smart_rename(
            workspace_id=self.workspace_id,
            workspace_name=self.workspace_name,
            model_name=input.model_name,
            old_name=input.old_name,
            new_name=input.new_name,
            reasoning=reasoning,
            dry_run=input.dry_run,
            definition_fallback=definition,
        )

        # Build change details
        changes: List[ChangeDetail] = []
        for ch in plan.changes:
            if ch.change_type == "rename_measure":
                details = f"Rename measure **{ch.old_value}** → **{ch.new_value}**"
                changes.append(ChangeDetail(file=f"{input.model_name}", change_type="rename_measure", details=details))
            else:
                details = f"Update expression in **{ch.target}**"
                changes.append(ChangeDetail(file=f"{input.model_name}", change_type="update_expression", details=details))

        # Markdown summary
        summary_tbl = format_markdown_table(
            ["Field", "Value"],
            [
                ["Operation", "smart_rename_measure"],
                ["Workspace", self.workspace_name],
                ["Model", input.model_name],
                ["Old Measure", input.old_name],
                ["New Measure", input.new_name],
                ["Direct references", str(len(plan.referenced_by))],
                ["Planned changes", str(len(plan.changes))],
                ["Dry run", str(input.dry_run)],
                ["Operation ID", plan.operation_id],
                ["Audit snapshots", " → ".join(snapshot_ids) if snapshot_ids else ""],
            ],
        )

        refs_tbl = format_markdown_table(
            ["#", "Referencing measure"],
            [[i + 1, name] for i, name in enumerate(plan.referenced_by)] or [["-", "(none)"]],
        )

        changes_tbl = format_markdown_table(
            ["Seq", "Target", "Change", "Reasoning"],
            [[c.seq, c.target, c.change_type, c.reasoning] for c in plan.changes],
        )

        message = "\n\n".join(
            [
                "### ✅ Smart Rename Measure",
                summary_tbl,
                "### 🔎 Direct References (measures that will be updated)",
                refs_tbl,
                "### 🧾 Planned / Applied Changes",
                changes_tbl,
                "",
                "**Rollback:** use `rollback` with `operation_id` and `model_name` to preview/execute rollback.",
            ]
        )

        return SmartRenameMeasureOutput(
            operation_id=plan.operation_id,
            model_name=input.model_name,
            old_name=input.old_name,
            new_name=input.new_name,
            dry_run=input.dry_run,
            referenced_by=plan.referenced_by,
            changes=changes,
            status="success" if (input.dry_run or snapshot_ids) else "unknown",
            message=message,
            snapshot_ids=snapshot_ids,
        )

    async def rename_measure(
        self,
        input: RenameMeasureInput,
    ) -> RenameMeasureOutput:
        """
        Backwards-compatible wrapper around smart_rename_measure.

        This tool returns the legacy RenameMeasureOutput schema, but uses the
        orchestration layer to update dependent measures and create an audit log.

        If you want the full markdown safety UI + operation_id rollback, prefer
        `smart_rename_measure`.
        """
        # Run the smart operation
        smart = await self.smart_rename_measure(
            SmartRenameMeasureInput(
                model_name=input.model_name,
                old_name=input.old_name,
                new_name=input.new_name,
                dry_run=input.dry_run,
                reasoning="Legacy rename_measure wrapper",
            )
        )

        # Optional: include the existing impact analyzer
        impact = None
        try:
            impact = await self.analyze_impact(
                AnalyzeImpactInput(
                    model_name=input.model_name,
                    target_type=input.target_type,
                    target_name=input.old_name,
                )
            )
        except Exception as e:
            logger.debug(f"Impact analysis skipped/failed: {e}")

        return RenameMeasureOutput(
            old_name=input.old_name,
            new_name=input.new_name,
            dry_run=input.dry_run,
            impact=impact,
            changes=smart.changes,
            status="success",
            message=smart.message,
            snapshot_id=smart.snapshot_ids[0] if smart.snapshot_ids else None,
        )


    async def get_refactor_history(
        self,
        input: GetRefactorHistoryInput,
    ) -> GetRefactorHistoryOutput:
        """
        Get history of refactoring operations from the audit trail.
        
        Retrieves past operations with their status, timestamps, and details.
        Useful for auditing and identifying rollback targets.
        
        Args:
            input: GetRefactorHistoryInput with limit and optional operation_type filter.
        
        Returns:
            GetRefactorHistoryOutput with events list and count.
        
        Example:
            >>> history = await tools.get_refactor_history(
            ...     GetRefactorHistoryInput(limit=10)
            ... )
            >>> for event in history.events:
            ...     print(f"{event.timestamp}: {event.operation} ({event.status})")
        """
        logger.info(f"Getting refactor history (limit={input.limit})")
        
        snapshots = await self.memory.get_history(
            operation=input.operation_type,
            limit=input.limit,
        )
        
        events = [
            RefactorEvent(
                id=s.id,
                timestamp=s.timestamp.isoformat(),
                operation=s.operation,
                target=s.target_name,
                old_value=s.old_value,
                new_value=s.new_value,
                affected_items=s.state_data.get("affected_reports", []),
                status=s.status.value,
                user=s.user,
            )
            for s in snapshots
        ]
        
        return GetRefactorHistoryOutput(events=events, count=len(events))
    
    async def rollback(
        self,
        input: RollbackInput,
    ) -> RollbackOutput:
        """
        Roll back a previous change.

        Preferred mode (transactional):
        - Provide `operation_id` + `model_name` (created by smart_rename_measure)
          to preview/execute a full transaction rollback using memory/refactor_log.json.

        Legacy mode:
        - Provide `snapshot_id` to preview a rollback from the MemoryManager audit trail.
          (Execution for legacy snapshot rollbacks is still limited.)

        Output message is formatted as GitHub-flavored Markdown tables.
        """
        if not self.workspace_id or not self.workspace_name:
            raise RuntimeError("No workspace selected. Call set_workspace first.")

        changes: List[ChangeDetail] = []

        # Transaction rollback by operation_id
        if input.operation_id:
            orchestrator = RefactorOrchestrator(self.memory, json_log=RefactorJsonLog())
            result = await orchestrator.rollback_operation(
                operation_id=input.operation_id,
                workspace_id=self.workspace_id,
                workspace_name=self.workspace_name,
                model_name=input.model_name or "",
                dry_run=input.dry_run,
            )

            # Convert preview changes to ChangeDetail
            for c in result.get("changes", []):
                changes.append(
                    ChangeDetail(
                        file=str(c.get("target_name") or ""),
                        change_type=str(c.get("action") or "rollback"),
                        details=f"{c.get('old_value')} → {c.get('new_value')}",
                    )
                )

            summary_tbl = format_markdown_table(
                ["Field", "Value"],
                [
                    ["Operation", "rollback"],
                    ["Workspace", self.workspace_name],
                    ["Model", input.model_name],
                    ["Operation ID", input.operation_id],
                    ["Dry run", str(input.dry_run)],
                    ["Planned rollback steps", str(len(changes))],
                ],
            )

            steps_tbl = format_markdown_table(
                ["#", "Action", "Target", "Revert"],
                [
                    [i + 1, c.get("action"), c.get("target_name"), f"{c.get('old_value')} → {c.get('new_value')}"]
                    for i, c in enumerate(result.get("changes", []))
                ] or [["-", "-", "-", "-"]],
            )

            message = "\n\n".join(
                [
                    "### ↩️ Rollback",
                    summary_tbl,
                    "### 🧾 Steps",
                    steps_tbl,
                    result.get("message", ""),
                ]
            )

            return RollbackOutput(
                snapshot_id=input.operation_id,
                dry_run=input.dry_run,
                changes=changes,
                status="dry_run" if input.dry_run else ("success" if result.get("success") else "failed"),
                message=message,
            )

        # Legacy snapshot rollback (preview only, unless restore logic implemented)
        if not input.snapshot_id:
            raise ValueError("Provide either operation_id or snapshot_id")

        logger.info(f"Rollback to snapshot {input.snapshot_id} (dry_run={input.dry_run})")
        snapshot = await self.memory.get_snapshot(input.snapshot_id)
        if not snapshot:
            raise ValueError(f"Snapshot {input.snapshot_id} not found")

        if input.dry_run:
            message = f"Would rollback {snapshot.operation} from {snapshot.timestamp}"
            if snapshot.old_value and snapshot.new_value:
                changes.append(
                    ChangeDetail(
                        file=snapshot.target_name or "unknown",
                        change_type="revert",
                        details=f"Revert {snapshot.new_value} -> {snapshot.old_value}",
                    )
                )
        else:
            message = "Legacy snapshot rollback execution is not implemented. Use operation_id rollback."
            await self.memory.record_state(
                state_type=snapshot.state_type,
                operation="rollback",
                status=OperationStatus.FAILED,
                workspace_id=snapshot.workspace_id,
                workspace_name=snapshot.workspace_name,
                target_name=snapshot.target_name,
                old_value=snapshot.new_value,
                new_value=snapshot.old_value,
                parent_id=input.snapshot_id,
                metadata={"rolled_back_from": input.snapshot_id},
                error_message="Legacy snapshot rollback not implemented",
            )

        return RollbackOutput(
            snapshot_id=input.snapshot_id,
            dry_run=input.dry_run,
            changes=changes,
            status="dry_run" if input.dry_run else "failed",
            message=message,
        )


    async def get_connection_status(
        self,
        input: GetConnectionStatusInput,
    ) -> GetConnectionStatusOutput:
        """
        Check the current connection status to Fabric.
        
        Returns information about the current connection state,
        active workspace, and authentication mode.
        
        Args:
            input: GetConnectionStatusInput (no parameters).
        
        Returns:
            GetConnectionStatusOutput with connection details.
        
        Example:
            >>> status = await tools.get_connection_status(
            ...     GetConnectionStatusInput()
            ... )
            >>> if status.connected:
            ...     print(f"Connected to {status.workspace_name}")
        """
        try:
            # Try a simple API call to verify connection
            await self.client.get("/workspaces?$top=1")
            connected = True
            error = None
        except Exception as e:
            connected = False
            error = str(e)
        
        return GetConnectionStatusOutput(
            connected=connected,
            workspace_id=self.workspace_id,
            workspace_name=self.workspace_name,
            auth_mode=self.client.auth_config.auth_mode.value if self.client else None,
            error=error,
        )
    
    # ========================================================================
    # Enterprise Demo Tool
    # ========================================================================
    
    async def setup_enterprise_demo(
        self,
        input: "SetupEnterpriseDemoInput",
    ) -> "SetupEnterpriseDemoOutput":
        """
        Create a complete enterprise Star Schema demo environment.
        
        This tool programmatically creates:
        1. Mock data (7 tables, 500K+ transactions)
        2. Lakehouse in Fabric
        3. Data pipeline for CSV to Delta conversion
        4. Semantic Model with 80+ complex DAX measures
        
        The DAX measures include nested dependencies for testing
        impact analysis - measures that call other measures.
        
        Args:
            input: SetupEnterpriseDemoInput with configuration.
        
        Returns:
            SetupEnterpriseDemoOutput with results and statistics.
        
        Example:
            >>> # Dry run first to preview
            >>> result = await tools.setup_enterprise_demo(
            ...     SetupEnterpriseDemoInput(
            ...         demo_name="Sales_Demo",
            ...         num_transactions=100000,
            ...         dry_run=True
            ...     )
            ... )
            >>> print(f"Would create {result.measure_stats['total_measures']} measures")
            >>> 
            >>> # Then execute for real
            >>> result = await tools.setup_enterprise_demo(
            ...     SetupEnterpriseDemoInput(
            ...         demo_name="Sales_Demo",
            ...         dry_run=False
            ...     )
            ... )
        """
        from fabric_agent.tools.setup_enterprise_demo import (
            SetupEnterpriseDemoTool,
            SetupEnterpriseDemoInput as DemoInput,
            SetupEnterpriseDemoOutput as DemoOutput,
        )
        
        if not self.workspace_id:
            raise RuntimeError("No workspace selected. Call set_workspace first.")
        
        logger.info(f"Setting up enterprise demo: {input.demo_name}")
        
        tool = SetupEnterpriseDemoTool(
            client=self.client,
            memory_manager=self.memory,
            workspace_id=self.workspace_id,
            workspace_name=self.workspace_name,
        )
        
        return await tool.execute(input)
    
    # ========================================================================
    # Safety-First Refactoring Tools
    # ========================================================================
    
    async def analyze_refactor_impact(
        self,
        input: "AnalyzeRefactorImpactInput",
    ) -> "AnalyzeRefactorImpactOutput":
        """
        Analyze the impact of a proposed refactoring operation.
        
        This is the FIRST tool to call before any rename or modification.
        It scans all reports and semantic models to find dependencies
        and generates a comprehensive pre-flight report.
        
        Args:
            input: AnalyzeRefactorImpactInput with target details.
        
        Returns:
            AnalyzeRefactorImpactOutput with pre-flight report.
        
        Example:
            >>> # Always analyze before refactoring!
            >>> result = await tools.analyze_refactor_impact(
            ...     AnalyzeRefactorImpactInput(
            ...         target_type="measure",
            ...         target_name="Sales",
            ...         model_name="Sales Analytics",
            ...         new_name="Net_Revenue"
            ...     )
            ... )
            >>> 
            >>> # Check the risk
            >>> print(result.summary)
            >>> if not result.can_proceed_safely:
            ...     print("⚠️ Review pre-flight report before proceeding!")
        """
        from fabric_agent.tools.safety_refactor import (
            SafeRefactoringEngine,
            AnalyzeRefactorImpactInput as ImpactInput,
        )
        
        if not self.workspace_id:
            raise RuntimeError("No workspace selected. Call set_workspace first.")
        
        logger.info(f"Analyzing refactor impact for: {input.target_name}")
        
        engine = SafeRefactoringEngine(
            client=self.client,
            memory=self.memory,
            workspace_id=self.workspace_id,
            workspace_name=self.workspace_name,
        )
        
        return await engine.analyze_impact(input)
    
    async def safe_refactor(
        self,
        input: "SafeRefactorInput",
    ) -> "SafeRefactorOutput":
        """
        Perform a SAFE refactoring operation with full safety checks.
        
        This tool implements the safety-first approach:
        1. Automatically runs impact analysis (unless skipped)
        2. Generates pre-flight report with risk assessment
        3. Requires confirmation for HIGH_RISK and CRITICAL operations
        4. Creates a restore point before making changes
        5. Supports full rollback if anything goes wrong
        
        ALWAYS use dry_run=true first to preview changes!
        
        Args:
            input: SafeRefactorInput with operation details.
        
        Returns:
            SafeRefactorOutput with results and rollback snapshot.
        
        Example:
            >>> # Step 1: Dry run first (ALWAYS!)
            >>> result = await tools.safe_refactor(
            ...     SafeRefactorInput(
            ...         target_type="measure",
            ...         target_name="Sales",
            ...         new_name="Net_Revenue",
            ...         model_name="Sales Analytics",
            ...         dry_run=True
            ...     )
            ... )
            >>> 
            >>> # Step 2: Review the pre-flight report
            >>> if result.pre_flight_report.risk_level == "critical":
            ...     print("⚠️ CRITICAL RISK - Review carefully!")
            >>> 
            >>> # Step 3: Execute with confirmation (if required)
            >>> result = await tools.safe_refactor(
            ...     SafeRefactorInput(
            ...         target_type="measure",
            ...         target_name="Sales",
            ...         new_name="Net_Revenue",
            ...         model_name="Sales Analytics",
            ...         dry_run=False,
            ...         confirmation="CONFIRM RENAME Sales"  # If critical
            ...     )
            ... )
            >>> 
            >>> # Save the rollback ID in case you need it!
            >>> rollback_id = result.rollback_snapshot_id
        """
        from fabric_agent.tools.safety_refactor import (
            SafeRefactoringEngine,
            SafeRefactorInput as RefactorInput,
        )
        
        if not self.workspace_id:
            raise RuntimeError("No workspace selected. Call set_workspace first.")
        
        logger.info(f"Starting safe refactor: {input.target_name} -> {input.new_name}")
        
        engine = SafeRefactoringEngine(
            client=self.client,
            memory=self.memory,
            workspace_id=self.workspace_id,
            workspace_name=self.workspace_name,
        )
        
        return await engine.safe_refactor(input)
    
    async def rollback_last_action(
        self,
        input: "RollbackLastActionInput",
    ) -> "RollbackOutput":
        """
        Rollback the last refactoring action or a specific operation.
        
        Uses the MemoryManager to find the exact previous state and
        reverts the change in Fabric. This is your safety net!
        
        Three ways to rollback:
        1. rollback_last=True - Rolls back the most recent operation
        2. operation_id="..." - Rolls back a specific operation
        3. snapshot_id="..." - Rolls back to a specific snapshot
        
        ALWAYS use dry_run=true first to preview what will be restored!
        
        Args:
            input: RollbackInput with rollback target.
        
        Returns:
            RollbackOutput with restoration details.
        
        Example:
            >>> # Rollback the last action
            >>> result = await tools.rollback_last_action(
            ...     RollbackLastActionInput(
            ...         rollback_last=True,
            ...         dry_run=True  # Preview first!
            ...     )
            ... )
            >>> print(f"Would restore: {result.original_value}")
            >>> 
            >>> # Execute the rollback
            >>> result = await tools.rollback_last_action(
            ...     RollbackLastActionInput(
            ...         rollback_last=True,
            ...         dry_run=False,
            ...         confirmation="yes"
            ...     )
            ... )
        """
        from fabric_agent.tools.safety_refactor import (
            SafeRefactoringEngine,
            RollbackInput,
        )
        
        if not self.workspace_id:
            raise RuntimeError("No workspace selected. Call set_workspace first.")
        
        logger.info("Starting rollback operation")
        
        # Convert input to RollbackInput
        rollback_input = RollbackInput(
            operation_id=input.operation_id,
            rollback_last=input.rollback_last,
            snapshot_id=input.snapshot_id,
            dry_run=input.dry_run,
            confirmation=input.confirmation,
        )
        
        engine = SafeRefactoringEngine(
            client=self.client,
            memory=self.memory,
            workspace_id=self.workspace_id,
            workspace_name=self.workspace_name,
        )
        
        return await engine.rollback_last_action(rollback_input)

    # ========================================================================
    # Self-Healing Infrastructure Tools
    # ========================================================================

    async def scan_workspace_health(
        self,
        input: ScanWorkspaceHealthInput,
    ) -> ScanWorkspaceHealthOutput:
        """
        Scan workspace(s) for infrastructure anomalies.

        WHAT: Runs AnomalyDetector.scan() across the given workspace IDs,
        checking for broken shortcuts, orphaned assets, schema drift,
        stale tables, and failed pipelines.

        WHY (FAANG PATTERN): At scale, humans can't monitor every item.
        This is the "observe" phase of the self-healing observe→decide→act loop,
        same as Google SRE's Monarch alerting and Meta ODS anomaly detection.

        ALWAYS call this before execute_healing_plan to review what will be fixed!

        Args:
            input: ScanWorkspaceHealthInput with workspace_ids.

        Returns:
            ScanWorkspaceHealthOutput with anomalies ranked by severity.

        Example:
            >>> result = await tools.scan_workspace_health(
            ...     ScanWorkspaceHealthInput(workspace_ids=["ws-123"])
            ... )
            >>> for anomaly in result.anomalies:
            ...     print(f"{anomaly.severity}: {anomaly.details}")
        """
        from fabric_agent.healing.detector import AnomalyDetector
        from fabric_agent.healing.models import RiskLevel

        logger.info(f"Scanning workspace health: {input.workspace_ids}")

        detector = AnomalyDetector(
            client=self.client,
            stale_hours=input.stale_hours,
        )
        anomalies = await detector.scan(input.workspace_ids)

        anomaly_infos = [
            AnomalyInfo(
                anomaly_id=a.anomaly_id,
                anomaly_type=a.anomaly_type.value,
                severity=a.severity.value,
                asset_id=a.asset_id,
                asset_name=a.asset_name,
                workspace_id=a.workspace_id,
                details=a.details,
                can_auto_heal=a.can_auto_heal,
                heal_action=a.heal_action,
                detected_at=a.detected_at,
            )
            for a in anomalies
        ]

        counts = {s.value: 0 for s in RiskLevel}
        for a in anomalies:
            counts[a.severity.value] += 1

        return ScanWorkspaceHealthOutput(
            workspace_ids=input.workspace_ids,
            anomalies=anomaly_infos,
            total_found=len(anomaly_infos),
            critical_count=counts.get("critical", 0),
            high_count=counts.get("high", 0),
            medium_count=counts.get("medium", 0),
            low_count=counts.get("low", 0),
            auto_healable_count=sum(1 for a in anomalies if a.can_auto_heal),
            message=(
                f"Found {len(anomalies)} anomalies across {len(input.workspace_ids)} workspace(s). "
                f"{sum(1 for a in anomalies if a.can_auto_heal)} can be auto-healed."
            ),
        )

    async def build_healing_plan(
        self,
        input: BuildHealingPlanInput,
    ) -> BuildHealingPlanOutput:
        """
        Build a healing plan from workspace anomalies without applying changes.

        Scans for anomalies then categorises remediation actions into:
        - auto_actions: safe to apply immediately
        - manual_actions: require human review/approval

        Use this to preview what execute_healing_plan would do.

        Args:
            input: BuildHealingPlanInput with workspace_ids.

        Returns:
            BuildHealingPlanOutput with categorised actions.

        Example:
            >>> plan = await tools.build_healing_plan(
            ...     BuildHealingPlanInput(workspace_ids=["ws-123"])
            ... )
            >>> print(f"{plan.auto_action_count} auto + {plan.manual_action_count} manual")
        """
        from fabric_agent.healing.detector import AnomalyDetector
        from fabric_agent.healing.healer import SelfHealer

        logger.info(f"Building healing plan for: {input.workspace_ids}")

        detector = AnomalyDetector(client=self.client)
        healer = SelfHealer(client=self.client, memory=self.memory)

        anomalies = await detector.scan(input.workspace_ids)
        plan = healer.build_plan(anomalies)

        def _action_info(action) -> HealActionInfo:
            return HealActionInfo(
                action_id=action.action_id,
                anomaly_id=action.anomaly_id,
                action_type=action.action_type,
                description=action.description,
                requires_approval=action.requires_approval,
                status=action.status.value,
            )

        return BuildHealingPlanOutput(
            plan_id=plan.plan_id,
            anomaly_count=len(plan.anomalies),
            auto_action_count=plan.auto_action_count,
            manual_action_count=plan.manual_action_count,
            auto_actions=[_action_info(a) for a in plan.auto_actions],
            manual_actions=[_action_info(a) for a in plan.manual_actions],
            message=(
                f"Plan {plan.plan_id}: {plan.auto_action_count} auto-fix + "
                f"{plan.manual_action_count} manual review actions."
            ),
        )

    async def execute_healing_plan(
        self,
        input: ExecuteHealingPlanInput,
    ) -> ExecuteHealingPlanOutput:
        """
        Scan, plan, and execute self-healing for workspace anomalies.

        WORKFLOW:
            1. AnomalyDetector.scan() → find anomalies
            2. SelfHealer.build_plan() → categorise actions
            3. SelfHealer.execute_healing_plan() → apply safe auto-fixes

        ALWAYS use dry_run=True first to preview what will be changed!

        Args:
            input: ExecuteHealingPlanInput with workspace_ids and dry_run.

        Returns:
            ExecuteHealingPlanOutput with applied/failed/skipped counts.

        Example:
            >>> # Preview first
            >>> result = await tools.execute_healing_plan(
            ...     ExecuteHealingPlanInput(workspace_ids=["ws-123"], dry_run=True)
            ... )
            >>> print(f"Would apply {result.applied} fixes")
            >>>
            >>> # Execute for real
            >>> result = await tools.execute_healing_plan(
            ...     ExecuteHealingPlanInput(workspace_ids=["ws-123"], dry_run=False)
            ... )
        """
        from fabric_agent.healing.monitor import SelfHealingMonitor

        logger.info(
            f"Executing healing plan | workspaces={input.workspace_ids} | "
            f"dry_run={input.dry_run}"
        )

        monitor = SelfHealingMonitor(client=self.client, memory=self.memory)
        report = await monitor.run_once(input.workspace_ids, dry_run=input.dry_run)

        result = report.healing_result
        plan = report.healing_plan

        if result is None:
            return ExecuteHealingPlanOutput(
                plan_id=plan.plan_id if plan else "N/A",
                applied=0,
                failed=0,
                skipped=0,
                dry_run=input.dry_run,
                success=True,
                message="No anomalies found — workspace is healthy.",
            )

        return ExecuteHealingPlanOutput(
            plan_id=result.plan_id,
            applied=result.applied,
            failed=result.failed,
            skipped=result.skipped,
            dry_run=result.dry_run,
            success=result.success,
            errors=result.errors,
            message=(
                f"{'[DRY RUN] ' if input.dry_run else ''}"
                f"Applied {result.applied}, failed {result.failed}, "
                f"skipped {result.skipped}."
            ),
        )

    # ========================================================================
    # Memory / RAG Tools
    # ========================================================================

    async def find_similar_operations(
        self,
        input: FindSimilarOperationsInput,
    ) -> FindSimilarOperationsOutput:
        """
        Find past operations semantically similar to a proposed change.

        WHAT: Uses vector similarity (RAG) to search the operation history
              for operations that are conceptually close to the proposed one.

        WHY: "Revenue YTD rename" will surface lessons from "Sales YTD rename"
             even if the exact names differ — pure SQL WHERE clauses can't do that.

        FAANG PARALLEL: Same pattern as GitHub Copilot context retrieval and
        Airbnb Minerva historical refactor lookup.

        Args:
            input: FindSimilarOperationsInput with proposed_change and top_k.

        Returns:
            FindSimilarOperationsOutput with ranked similar operations.

        Example:
            >>> result = await tools.find_similar_operations(
            ...     FindSimilarOperationsInput(
            ...         proposed_change="rename Total Revenue to Gross Revenue",
            ...         top_k=5
            ...     )
            ... )
            >>> for op in result.similar_operations:
            ...     print(f"{op.similarity:.2f} | {op.operation} | {op.outcome}")
        """
        logger.info(f"Finding similar operations for: {input.proposed_change[:60]}...")

        op_memory = getattr(self.memory, "_operation_memory", None)
        if op_memory is None:
            return FindSimilarOperationsOutput(
                proposed_change=input.proposed_change,
                similar_operations=[],
                total_found=0,
                message="Operation memory not configured. Initialize MemoryManager with an OperationMemory instance.",
            )

        similar = await op_memory.find_similar_operations(
            proposed_change=input.proposed_change,
            change_type=input.change_type,
            top_k=input.top_k,
        )

        ops = [
            SimilarOperationInfo(
                snapshot_id=s.snapshot_id,
                operation=s.operation,
                target_name=s.target_name,
                old_value=s.old_value,
                new_value=s.new_value,
                similarity=round(s.similarity, 4),
                outcome=s.outcome,
                key_lesson=s.key_lesson,
                timestamp=s.timestamp,
            )
            for s in similar
        ]

        return FindSimilarOperationsOutput(
            proposed_change=input.proposed_change,
            similar_operations=ops,
            total_found=len(ops),
            message=f"Found {len(ops)} similar past operations.",
        )

    async def get_risk_context(
        self,
        input: GetRiskContextInput,
    ) -> RiskContextOutput:
        """
        Get risk context for a proposed change using historical operation data.

        Synthesises past similar operations into actionable risk signals:
        - Historical failure rate for similar changes
        - Common failure reasons / anti-patterns
        - Concrete recommendations based on what worked / didn't

        Args:
            input: GetRiskContextInput with proposed_change.

        Returns:
            RiskContextOutput with failure rates and recommendations.

        Example:
            >>> ctx = await tools.get_risk_context(
            ...     GetRiskContextInput(
            ...         proposed_change="rename Total Revenue to Gross Revenue",
            ...         top_k=10
            ...     )
            ... )
            >>> if ctx.is_high_risk:
            ...     print(f"⚠️ {ctx.historical_failure_rate:.0%} failure rate historically!")
            ...     for rec in ctx.recommendations:
            ...         print(f"  • {rec}")
        """
        logger.info(f"Getting risk context for: {input.proposed_change[:60]}...")

        op_memory = getattr(self.memory, "_operation_memory", None)
        if op_memory is None:
            return RiskContextOutput(
                proposed_change=input.proposed_change,
                similar_operations=[],
                historical_failure_rate=0.0,
                historical_rollback_rate=0.0,
                recommendations=["Operation memory not configured — no historical context available."],
                confidence=0.0,
                sample_size=0,
                risk_summary="No historical data",
                is_high_risk=False,
            )

        ctx = await op_memory.get_risk_context(
            proposed_change=input.proposed_change,
            top_k=input.top_k,
        )

        ops = [
            SimilarOperationInfo(
                snapshot_id=s.snapshot_id,
                operation=s.operation,
                target_name=s.target_name,
                old_value=s.old_value,
                new_value=s.new_value,
                similarity=round(s.similarity, 4),
                outcome=s.outcome,
                key_lesson=s.key_lesson,
                timestamp=s.timestamp,
            )
            for s in ctx.similar_operations
        ]

        return RiskContextOutput(
            proposed_change=input.proposed_change,
            similar_operations=ops,
            historical_failure_rate=ctx.historical_failure_rate,
            historical_rollback_rate=ctx.historical_rollback_rate,
            recommendations=ctx.recommendations,
            confidence=ctx.confidence,
            sample_size=ctx.sample_size,
            risk_summary=ctx.risk_summary,
            is_high_risk=ctx.is_high_risk,
        )

    async def memory_stats(
        self,
        input: MemoryStatsInput,
    ) -> MemoryStatsOutput:
        """
        Get statistics about the operation memory vector store.

        Returns total operations indexed, last indexed timestamp,
        and ChromaDB collection size.

        Args:
            input: MemoryStatsInput (no required parameters).

        Returns:
            MemoryStatsOutput with vector store statistics.

        Example:
            >>> stats = await tools.memory_stats(MemoryStatsInput())
            >>> print(f"Indexed: {stats.total_indexed} operations")
            >>> print(f"Vector store: {stats.collection_size} vectors")
        """
        logger.info("Getting memory statistics")

        op_memory = getattr(self.memory, "_operation_memory", None)
        if op_memory is None:
            return MemoryStatsOutput(
                vector_store_backend="not_configured",
                vector_store_count=0,
                vector_store_path="N/A",
                audit_trail_count=0,
                embedding_backend="not_configured",
                embedding_dimension=0,
            )

        stats = await op_memory.get_statistics()
        embedder = getattr(op_memory, "_embedding_client", None)

        return MemoryStatsOutput(
            vector_store_backend=stats.get("backend", "ChromaDB"),
            vector_store_count=stats.get("collection_size", 0),
            vector_store_path=stats.get("vector_store_path", "unknown"),
            audit_trail_count=stats.get("total_indexed", 0),
            embedding_backend=stats.get("embedding_model", "unknown"),
            embedding_dimension=getattr(embedder, "dimension", 0) if embedder else 0,
        )

    async def reindex_operation_memory(
        self,
        input: ReindexMemoryInput,
    ) -> ReindexMemoryOutput:
        """
        Rebuild the vector index from the existing SQLite audit trail.

        Use this after:
        - First-time setup (to index historical operations)
        - Upgrading the embedding model
        - Recovering from a corrupted vector store

        Args:
            input: ReindexMemoryInput (no required parameters).

        Returns:
            ReindexMemoryOutput with count of reindexed operations.

        Example:
            >>> result = await tools.reindex_operation_memory(ReindexMemoryInput())
            >>> print(f"Reindexed {result.reindexed_count} operations")
        """
        logger.info("Reindexing operation memory from audit trail")

        op_memory = getattr(self.memory, "_operation_memory", None)
        if op_memory is None:
            return ReindexMemoryOutput(
                operations_indexed=0,
                success=False,
                message="Operation memory not configured.",
            )

        count = await op_memory.reindex_all()

        return ReindexMemoryOutput(
            operations_indexed=count,
            success=True,
            message=f"Successfully reindexed {count} operations into vector store.",
        )

    async def get_session_summary(
        self,
        input: GetSessionSummaryInput,
    ) -> GetSessionSummaryOutput:
        """
        Get a human-readable summary of the last agent session.

        WHAT: Retrieves persisted cross-session memory so the agent knows
              what happened in previous conversations.

        WHY: Each MCP session starts fresh. Without this, Claude forgets
             what it renamed 3 sessions ago. This bridges the gap.

        FAANG PARALLEL: Same pattern as ChatGPT memory feature, but for
        data infrastructure operations instead of conversations.

        Args:
            input: GetSessionSummaryInput with optional workspace_id.

        Returns:
            GetSessionSummaryOutput with last session summary text.

        Example:
            >>> summary = await tools.get_session_summary(
            ...     GetSessionSummaryInput(workspace_id="my-workspace-id")
            ... )
            >>> print(summary.summary)
            # "Last session: renamed 3 measures, 1 rollback on Total Revenue"
        """
        from fabric_agent.memory.session_context import SessionContext

        logger.info("Getting session summary")

        workspace_id = input.workspace_id or self.workspace_id
        session_ctx = SessionContext(self.memory)

        summary = await session_ctx.get_last_session_summary(workspace_id or "default")

        has_prev = bool(summary and "No previous sessions" not in summary)
        return GetSessionSummaryOutput(
            has_previous_session=has_prev,
            summary=summary if has_prev else None,
            session_id=None,  # SessionContext.get_last_session_summary returns text, not ID
        )

    # ========================================================================
    # Phase E — Shortcut Cascade Tools
    # ========================================================================

    async def scan_shortcut_cascade(
        self,
        input: "ScanShortcutCascadeInput",
    ) -> "ScanShortcutCascadeOutput":
        """
        Discover all broken OneLake shortcuts + their cascade of impacted assets.

        WHAT: Queries the Fabric REST API for all shortcuts in each workspace,
        verifies each source is accessible, and traverses the lineage graph
        downstream from each broken shortcut to find impacted tables, semantic
        models, pipelines, and reports.

        WHEN TO USE: Run this before build_shortcut_healing_plan to get a
        preview of what's broken and how much downstream damage there is.

        FAANG PARALLEL:
            Meta's blast radius analysis — before any infra remediation,
            automatically compute all downstream effects and show operators
            exactly what's broken and why.

        Args:
            input.workspace_ids: List of workspace IDs to scan.

        Returns:
            broken_shortcut_count, total_cascade_impact, broken_shortcuts,
            cascade_impacts (one per broken shortcut).
        """
        from fabric_agent.healing.shortcut_manager import ShortcutCascadeManager
        from fabric_agent.tools.models import (
            ScanShortcutCascadeOutput,
            ShortcutDefinitionInfo,
            CascadeImpactInfo,
            FixSuggestionInfo,
        )

        manager = ShortcutCascadeManager(
            client=self.client,
            memory=self.memory,
        )

        plan = await manager.build_cascade_plan(input.workspace_ids)

        broken_infos = [
            ShortcutDefinitionInfo(
                shortcut_id=s.shortcut_id,
                name=s.name,
                workspace_id=s.workspace_id,
                lakehouse_id=s.lakehouse_id,
                source_workspace_id=s.source_workspace_id,
                source_lakehouse_id=s.source_lakehouse_id,
                source_path=s.source_path,
                is_healthy=s.is_healthy,
            )
            for s in plan.broken_shortcuts
        ]

        cascade_infos = []
        for ci in plan.cascade_impacts:
            sc_name = ci.shortcut.name if ci.shortcut else "unknown"
            cascade_infos.append(
                CascadeImpactInfo(
                    shortcut_name=sc_name,
                    impacted_count=ci.total_impacted,
                    impacted_tables=ci.impacted_tables,
                    impacted_semantic_models=ci.impacted_semantic_models,
                    impacted_pipelines=ci.impacted_pipelines,
                    impacted_reports=ci.impacted_reports,
                    fix_suggestions=[
                        FixSuggestionInfo(
                            asset_id=fs.asset_id,
                            asset_name=fs.asset_name,
                            asset_type=fs.asset_type,
                            suggestion=fs.suggestion,
                            action_type=fs.action_type,
                            auto_applicable=fs.auto_applicable,
                        )
                        for fs in ci.fix_suggestions
                    ],
                )
            )

        total_impact = sum(ci.total_impacted for ci in plan.cascade_impacts)
        msg = (
            f"Found {len(plan.broken_shortcuts)} broken shortcut(s) "
            f"with {total_impact} downstream asset(s) impacted."
            if plan.broken_shortcuts
            else "No broken shortcuts found."
        )

        return ScanShortcutCascadeOutput(
            broken_shortcut_count=len(plan.broken_shortcuts),
            total_cascade_impact=total_impact,
            broken_shortcuts=broken_infos,
            cascade_impacts=cascade_infos,
            message=msg,
        )

    async def build_shortcut_healing_plan(
        self,
        input: "BuildShortcutHealingPlanInput",
    ) -> "BuildShortcutHealingPlanOutput":
        """
        Build a cascade healing plan with auto and manual action splits.

        WHAT: Scans workspaces for broken shortcuts, analyzes the full cascade
        of impacted assets, and creates a plan that splits actions into:
            auto_actions:   safe to apply immediately (shortcut recreation + model refresh)
            manual_actions: require human approval (pipeline config, cross-workspace)

        APPROVAL GATE: If approval_required=True, call approve_shortcut_healing()
        with the returned plan_id before execute_shortcut_healing().

        This method stores the plan in memory so it can be retrieved by
        approve_shortcut_healing() and execute_shortcut_healing().

        Args:
            input.workspace_ids: List of workspace IDs to include.

        Returns:
            plan_id, approval_required, auto/manual action counts and details.
        """
        from fabric_agent.healing.shortcut_manager import ShortcutCascadeManager
        from fabric_agent.tools.models import (
            BuildShortcutHealingPlanOutput,
            FixSuggestionInfo,
        )

        manager = ShortcutCascadeManager(
            client=self.client,
            memory=self.memory,
        )

        plan = await manager.build_cascade_plan(input.workspace_ids)

        # Store plan with TTL; prune expired entries to prevent memory leak
        self._shortcut_plans[plan.plan_id] = (plan, time.monotonic() + self._plan_ttl)
        expired = [pid for pid, (_, exp) in self._shortcut_plans.items() if time.monotonic() > exp]
        for pid in expired:
            del self._shortcut_plans[pid]

        def _to_fix_info(fs) -> FixSuggestionInfo:
            return FixSuggestionInfo(
                asset_id=fs.asset_id,
                asset_name=fs.asset_name,
                asset_type=fs.asset_type,
                suggestion=fs.suggestion,
                action_type=fs.action_type,
                auto_applicable=fs.auto_applicable,
            )

        msg = (
            f"Plan {plan.plan_id}: "
            f"{len(plan.auto_actions)} auto action(s), "
            f"{len(plan.manual_actions)} manual action(s). "
            + (
                "Approval required before executing manual actions."
                if plan.approval_required
                else "All actions are safe to auto-apply."
            )
        )

        return BuildShortcutHealingPlanOutput(
            plan_id=plan.plan_id,
            approval_required=plan.approval_required,
            auto_action_count=len(plan.auto_actions),
            manual_action_count=len(plan.manual_actions),
            auto_actions=[_to_fix_info(a) for a in plan.auto_actions],
            manual_actions=[_to_fix_info(a) for a in plan.manual_actions],
            message=msg,
        )

    async def approve_shortcut_healing(
        self,
        input: "ApproveShortcutHealingInput",
    ) -> "ApproveShortcutHealingOutput":
        """
        Approve a pending shortcut healing plan.

        WHAT: Records the approver's identity on the plan so that
        execute_shortcut_healing() will run the manual_actions in addition
        to the auto_actions.

        Human-in-the-loop gate: This is the approval step. The plan must be
        reviewed (check build_shortcut_healing_plan output for manual_actions)
        before calling this.

        After calling this, run execute_shortcut_healing(plan_id, dry_run=False)
        to apply the changes.

        Args:
            input.plan_id:     Plan ID from build_shortcut_healing_plan.
            input.approved_by: Name/email of the approver.

        Returns:
            Confirmation with plan_id and approver info.
        """
        from datetime import datetime, timezone
        from fabric_agent.tools.models import ApproveShortcutHealingOutput

        entry = self._shortcut_plans.get(input.plan_id)
        plan = entry[0] if entry and time.monotonic() <= entry[1] else None
        if plan is None:
            return ApproveShortcutHealingOutput(
                plan_id=input.plan_id,
                approved=False,
                approved_by=input.approved_by,
                message=(
                    f"Plan '{input.plan_id}' not found. "
                    "Call build_shortcut_healing_plan first."
                ),
            )

        plan.approved_by = input.approved_by
        plan.approved_at = datetime.now(timezone.utc).isoformat()

        logger.info(
            f"Shortcut healing plan {input.plan_id} approved by {input.approved_by}"
        )

        return ApproveShortcutHealingOutput(
            plan_id=input.plan_id,
            approved=True,
            approved_by=input.approved_by,
            message=(
                f"Plan '{input.plan_id}' approved by {input.approved_by}. "
                f"Call execute_shortcut_healing(plan_id='{input.plan_id}', dry_run=False) "
                "to apply changes."
            ),
        )

    async def execute_shortcut_healing(
        self,
        input: "ExecuteShortcutHealingInput",
    ) -> "ExecuteShortcutHealingOutput":
        """
        Execute a shortcut healing plan — recreate shortcuts + refresh models.

        WHAT: Runs the approved healing plan:
            1. Recreates all broken shortcuts via POST /items/{lh_id}/shortcuts
            2. Triggers Full refresh on all impacted semantic models
            3. Skips manual_actions if plan is not approved

        ALWAYS run with dry_run=True first to preview what will happen.

        APPROVAL GATE: manual_actions are only executed if approve_shortcut_healing()
        was called first. auto_actions always run regardless of approval.

        Args:
            input.plan_id:  Plan ID from build_shortcut_healing_plan.
            input.dry_run:  True = preview only. False = apply. Default: True.

        Returns:
            applied/failed/skipped counts and any errors.
        """
        from fabric_agent.healing.shortcut_manager import ShortcutCascadeManager
        from fabric_agent.tools.models import ExecuteShortcutHealingOutput

        entry = self._shortcut_plans.get(input.plan_id)
        plan = entry[0] if entry and time.monotonic() <= entry[1] else None
        if plan is None:
            return ExecuteShortcutHealingOutput(
                plan_id=input.plan_id,
                applied=0,
                failed=0,
                skipped=0,
                dry_run=input.dry_run,
                success=False,
                errors=[f"Plan '{input.plan_id}' not found. Call build_shortcut_healing_plan first."],
                message=f"Plan '{input.plan_id}' not found.",
            )

        manager = ShortcutCascadeManager(
            client=self.client,
            memory=self.memory,
        )

        result = await manager.execute_plan(plan, dry_run=input.dry_run)

        msg = (
            f"{'[DRY RUN] ' if input.dry_run else ''}"
            f"Applied: {result.applied}, "
            f"Failed: {result.failed}, "
            f"Skipped: {result.skipped}."
        )
        if result.errors:
            msg += f" Errors: {'; '.join(result.errors[:3])}"

        return ExecuteShortcutHealingOutput(
            plan_id=input.plan_id,
            applied=result.applied,
            failed=result.failed,
            skipped=result.skipped,
            dry_run=input.dry_run,
            success=result.success,
            errors=result.errors,
            message=msg,
        )

    async def get_enterprise_blast_radius(
        self,
        input: "EnterpriseBlastRadiusInput",
    ) -> "EnterpriseBlastRadiusOutput":
        """
        Compute the full cross-workspace cascade blast radius for a source asset change.

        WHAT IT DOES:
            Builds a complete multi-workspace lineage graph, identifies all downstream
            assets, classifies the required healing action for each, and returns:
            - Per-workspace breakdown (source + consumer workspaces)
            - Topologically ordered healing plan (execute in listed order)
            - Overall risk level (low / medium / high / critical)

        ENTERPRISE SCENARIO:
            When fact_sales schema changes in ENT_DataPlatform_DEV, this tool will
            identify the full 3-workspace blast radius:

            DataPlatform_DEV (source):
              [depth 1] fact_sales → DataPlatform_Sales_Model  [refresh_model, auto]
              [depth 2] DataPlatform_Sales_Model → DataPlatform_Sales_Report  [notify_owner]

            SalesAnalytics_DEV (consumer 1):
              [depth 1] fact_sales shortcut → verify/recreate  [auto]
              [depth 2] shortcut → PL_SalesAnalytics_Consumer_Refresh  [trigger_pipeline]
              [depth 2] shortcut → Enterprise_Sales_Model  [refresh_model]
              [depth 3] Enterprise_Sales_Model → 4 reports  [notify_owner]

            Finance_DEV (consumer 2):
              [depth 1] fact_sales shortcut → verify/recreate  [auto]
              [depth 2] shortcut → PL_Finance_Consumer_Refresh  [trigger_pipeline]
              [depth 2] shortcut → Finance_Model  [refresh_model]
              [depth 3] Finance_Model → PnL_Dashboard, Budget_vs_Actual  [notify_owner]

        FAANG PARALLEL:
            - LinkedIn DataHub: getLineage() + impactAnalysis() for schema changes
            - Airbnb Minerva: ChangeImpact with per-team consumer notifications
            - Google Dataplex: DataQualityRule impact with workspace-level blast radius

        Args:
            input.source_asset_name:  Name of the changed asset (e.g. 'fact_sales').
            input.workspace_ids:      All workspace IDs to scan.
            input.change_description: What changed (e.g. 'column added: customer_tier').

        Returns:
            EnterpriseBlastRadiusOutput with per_workspace details + ordered_healing_steps.
        """
        from fabric_agent.healing.shortcut_manager import ShortcutCascadeManager
        from fabric_agent.tools.models import (
            EnterpriseBlastRadiusOutput,
            ImpactedAssetInfo,
            WorkspaceImpactInfo,
        )

        manager = ShortcutCascadeManager(client=self.client, memory=self.memory)

        blast = await manager.analyze_enterprise_blast_radius(
            source_asset_name=input.source_asset_name,
            workspace_ids=input.workspace_ids,
            change_description=input.change_description,
        )

        def _to_asset_info(d) -> ImpactedAssetInfo:
            return ImpactedAssetInfo(
                asset_id=d.asset_id,
                asset_name=d.asset_name,
                asset_type=d.asset_type,
                workspace_id=d.workspace_id,
                workspace_name=d.workspace_name,
                impact_depth=d.impact_depth,
                impact_path=d.impact_path,
                action_type=d.action_type,
                action_description=d.action_description,
                auto_applicable=d.auto_applicable,
                urgency=d.urgency,
            )

        per_workspace = [
            WorkspaceImpactInfo(
                workspace_id=w.workspace_id,
                workspace_name=w.workspace_name,
                role=w.role,
                asset_count=len(w.impacted_assets),
                auto_count=w.auto_count,
                manual_count=w.manual_count,
                impacted_assets=[_to_asset_info(a) for a in w.impacted_assets],
            )
            for w in blast.per_workspace
        ]

        msg = (
            f"Blast radius for '{blast.source_asset_name}': "
            f"{blast.total_workspaces_impacted} workspaces, "
            f"{blast.total_assets_impacted} assets impacted. "
            f"Risk: {blast.risk_level.upper()}."
        )

        return EnterpriseBlastRadiusOutput(
            source_asset_name=blast.source_asset_name,
            source_workspace_name=blast.source_workspace_name,
            change_description=blast.change_description,
            total_workspaces_impacted=blast.total_workspaces_impacted,
            total_assets_impacted=blast.total_assets_impacted,
            risk_level=blast.risk_level,
            generated_at=blast.generated_at,
            per_workspace=per_workspace,
            ordered_healing_steps=[_to_asset_info(s) for s in blast.ordered_healing_steps],
            message=msg,
        )

    # ── Guards: Freshness + Maintenance ──────────────────────────────────

    async def scan_freshness(
        self,
        input: "ScanFreshnessInput",
    ) -> "ScanFreshnessOutput":
        """
        Scan SQL Endpoint sync freshness across workspaces.

        WHAT: Triggers ``refreshMetadata`` on every SQL Endpoint in the given
        workspaces, then compares each table's ``lastSuccessfulSyncDateTime``
        against configurable SLA thresholds (fnmatch patterns → hours).

        WHY (FAANG PATTERN): SQL Endpoint sync lag is invisible — tables can be
        days stale with no Fabric alert. This is the data-freshness equivalent of
        Google BigQuery slot replication lag monitoring in Monarch.

        Args:
            input: ScanFreshnessInput with workspace_ids, optional sla_thresholds.

        Returns:
            ScanFreshnessOutput with violations and per-table status.
        """
        from fabric_agent.guards.freshness_guard import FreshnessGuard
        from fabric_agent.tools.models import (
            ScanFreshnessOutput,
            FreshnessViolationInfo,
            TableSyncInfo,
        )

        guard = FreshnessGuard(
            client=self.client,
            sla_thresholds=input.sla_thresholds,
            memory=self.memory,
            lro_timeout_secs=input.lro_timeout_secs,
        )

        result = await guard.scan(input.workspace_ids)

        violations = [
            FreshnessViolationInfo(
                violation_id=v.violation_id,
                workspace_id=v.workspace_id,
                workspace_name=v.workspace_name,
                sql_endpoint_id=v.sql_endpoint_id,
                sql_endpoint_name=v.sql_endpoint_name,
                table_status=TableSyncInfo(
                    table_name=v.table_status.table_name,
                    last_successful_sync_dt=v.table_status.last_successful_sync_dt,
                    sync_status=v.table_status.sync_status,
                    hours_since_sync=v.table_status.hours_since_sync,
                    freshness_status=v.table_status.freshness_status.value,
                    sla_threshold_hours=v.table_status.sla_threshold_hours,
                    sla_pattern=v.table_status.sla_pattern,
                ) if v.table_status else TableSyncInfo(
                    table_name="unknown",
                    sync_status="unknown",
                    freshness_status="unknown",
                    sla_threshold_hours=24.0,
                    sla_pattern="*",
                ),
                detected_at=v.detected_at,
            )
            for v in result.violations
        ]

        msg = (
            f"Freshness scan complete: {result.total_tables} tables, "
            f"{result.violation_count} violations, "
            f"{result.healthy_count} healthy. "
            f"Duration: {result.scan_duration_ms}ms."
        )

        return ScanFreshnessOutput(
            scan_id=result.scan_id,
            workspace_ids=result.workspace_ids,
            scanned_at=result.scanned_at,
            total_tables=result.total_tables,
            healthy_count=result.healthy_count,
            violation_count=result.violation_count,
            violations=violations,
            errors=result.errors,
            scan_duration_ms=result.scan_duration_ms,
            message=msg,
        )

    async def run_table_maintenance(
        self,
        input: "RunTableMaintenanceInput",
    ) -> "RunTableMaintenanceOutput":
        """
        Validate and optionally submit TableMaintenance jobs.

        WHAT: Discovers all lakehouses in the given workspaces, validates table
        names against the Delta registry (rejecting control chars, schema-qualified
        names, unregistered tables), checks queue pressure, and submits maintenance
        jobs sequentially (Trial capacity safe).

        WHY (FAANG PATTERN): Fabric TableMaintenance API accepts ANY string as
        tableName (HTTP 202), then fails asynchronously — wasting 4-9 min of
        Spark compute per bad name. This is pre-submission validation at the edge,
        same as Stripe's API gateway schema validation.

        ALWAYS use dry_run=True first to preview what would be submitted.

        Args:
            input: RunTableMaintenanceInput with workspace_ids, dry_run, etc.

        Returns:
            RunTableMaintenanceOutput with validation + job audit trail.
        """
        from fabric_agent.guards.maintenance_guard import MaintenanceGuard
        from fabric_agent.tools.models import (
            RunTableMaintenanceOutput,
            MaintenanceJobInfo,
            TableValidationInfo,
        )

        guard = MaintenanceGuard(
            client=self.client,
            memory=self.memory,
            queue_pressure_threshold=input.queue_pressure_threshold,
            dry_run=input.dry_run,
        )

        result = await guard.run_maintenance(
            input.workspace_ids,
            table_filter=input.table_filter,
        )

        job_records = [
            MaintenanceJobInfo(
                job_id=r.job_id,
                workspace_id=r.workspace_id,
                lakehouse_id=r.lakehouse_id,
                lakehouse_name=r.lakehouse_name,
                table_name=r.table_name,
                validation=TableValidationInfo(
                    table_name=r.validation.table_name,
                    is_valid=r.validation.is_valid,
                    rejection_reason=r.validation.rejection_reason,
                    resolved_name=r.validation.resolved_name,
                ) if r.validation else TableValidationInfo(
                    table_name=r.table_name,
                    is_valid=False,
                    rejection_reason="validation not run",
                ),
                fabric_job_id=r.fabric_job_id,
                status=r.status.value,
                submitted_at=r.submitted_at,
                completed_at=r.completed_at,
                error=r.error,
                dry_run=r.dry_run,
            )
            for r in result.job_records
        ]

        mode = "DRY RUN" if result.dry_run else "LIVE"
        msg = (
            f"[{mode}] Maintenance complete: {result.total_tables} tables, "
            f"{result.validated} valid, {result.rejected} rejected"
        )
        if not result.dry_run:
            msg += (
                f", {result.submitted} submitted, "
                f"{result.succeeded} succeeded, {result.failed} failed"
            )
        msg += "."

        return RunTableMaintenanceOutput(
            run_id=result.run_id,
            workspace_ids=result.workspace_ids,
            dry_run=result.dry_run,
            total_tables=result.total_tables,
            validated=result.validated,
            rejected=result.rejected,
            submitted=result.submitted,
            succeeded=result.succeeded,
            failed=result.failed,
            skipped_queue=result.skipped_queue,
            job_records=job_records,
            errors=result.errors,
            message=msg,
        )


# Import for type hints
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from fabric_agent.tools.setup_enterprise_demo import (
        SetupEnterpriseDemoInput,
        SetupEnterpriseDemoOutput,
    )
    from fabric_agent.tools.safety_refactor import (
        AnalyzeRefactorImpactInput,
        AnalyzeRefactorImpactOutput,
        SafeRefactorInput,
        SafeRefactorOutput,
        RollbackInput as RollbackLastActionInput,
        RollbackOutput,
    )
    from fabric_agent.tools.models import (
        EnterpriseBlastRadiusInput,
        EnterpriseBlastRadiusOutput,
        ScanFreshnessInput,
        ScanFreshnessOutput,
        RunTableMaintenanceInput,
        RunTableMaintenanceOutput,
    )
