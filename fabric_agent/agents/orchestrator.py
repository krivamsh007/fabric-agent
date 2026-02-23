"""
Agent Orchestrator & Deployment Pipeline Integration
=====================================================

Coordinates multiple agents for complex workflows and integrates
with Fabric Deployment Pipelines for human-in-the-loop approval.

Author: Fabric Agent Team
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4

from loguru import logger

from fabric_agent.agents.base import (
    SharedMemory,
    AgentResult,
    TaskStatus,
    AgentMessage,
    MessageType,
)
from fabric_agent.agents.specialized import (
    DiscoveryAgent,
    ImpactAgent,
    RefactorAgent,
)
from fabric_agent.agents.validation import ValidationAgent


# =============================================================================
# Workflow Models
# =============================================================================

class WorkflowStatus(str, Enum):
    """Status of a workflow."""
    PENDING = "pending"
    DISCOVERING = "discovering"
    ANALYZING = "analyzing"
    AWAITING_APPROVAL = "awaiting_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXECUTING = "executing"
    VALIDATING = "validating"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class ApprovalStatus(str, Enum):
    """Status of an approval request."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"


@dataclass
class ApprovalRequest:
    """A request for human approval."""
    id: str
    workflow_id: str
    change_summary: str
    risk_level: str
    impact_report: Dict[str, Any]
    requested_at: str
    requested_by: str = "system"
    approvers: List[str] = field(default_factory=list)
    status: ApprovalStatus = ApprovalStatus.PENDING
    approved_by: Optional[str] = None
    approved_at: Optional[str] = None
    rejection_reason: Optional[str] = None
    deployment_pipeline_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "change_summary": self.change_summary,
            "risk_level": self.risk_level,
            "impact_report": self.impact_report,
            "requested_at": self.requested_at,
            "status": self.status.value,
            "approved_by": self.approved_by,
            "approved_at": self.approved_at,
            "rejection_reason": self.rejection_reason,
        }


@dataclass
class WorkflowResult:
    """Result of a workflow execution."""
    workflow_id: str
    workflow_type: str
    status: WorkflowStatus
    started_at: str
    completed_at: Optional[str] = None
    agent_results: Dict[str, AgentResult] = field(default_factory=dict)
    approval_request: Optional[ApprovalRequest] = None
    final_result: Optional[Dict] = None
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "workflow_type": self.workflow_type,
            "status": self.status.value,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "agent_results": {k: v.to_dict() for k, v in self.agent_results.items()},
            "approval_request": self.approval_request.to_dict() if self.approval_request else None,
            "final_result": self.final_result,
            "error": self.error,
        }


# =============================================================================
# Deployment Pipeline Integration
# =============================================================================

class DeploymentPipelineManager:
    """
    Integrates with Fabric Deployment Pipelines for human approval.
    
    Fabric Deployment Pipelines provide:
    - Built-in approval UI
    - DEV → TEST → PROD promotion
    - Audit trail
    - Rollback capability
    """
    
    def __init__(self, fabric_client: Any):
        self.client = fabric_client
        self._pipelines: Dict[str, Dict] = {}
    
    async def get_or_create_pipeline(
        self,
        name: str,
        source_workspace_id: str,
        target_workspace_id: str,
    ) -> Dict:
        """Get existing or create new deployment pipeline."""
        
        # List existing pipelines
        try:
            response = await self.client.get("/deploymentPipelines")
            pipelines = response.get("value", [])
            
            for pipeline in pipelines:
                if pipeline.get("displayName") == name:
                    self._pipelines[name] = pipeline
                    return pipeline
        except Exception as e:
            logger.warning(f"Could not list pipelines: {e}")
        
        # Create new pipeline
        try:
            pipeline = await self.client.post(
                "/deploymentPipelines",
                json={
                    "displayName": name,
                    "description": f"Deployment pipeline for refactoring operations",
                }
            )
            
            # Assign workspaces to stages
            pipeline_id = pipeline["id"]
            
            # Stage 0: Development
            await self.client.post(
                f"/deploymentPipelines/{pipeline_id}/stages/0/assignWorkspace",
                json={"workspaceId": source_workspace_id}
            )
            
            # Stage 1: Production (or Test)
            await self.client.post(
                f"/deploymentPipelines/{pipeline_id}/stages/1/assignWorkspace",
                json={"workspaceId": target_workspace_id}
            )
            
            self._pipelines[name] = pipeline
            return pipeline
            
        except Exception as e:
            logger.error(f"Could not create pipeline: {e}")
            raise
    
    async def create_deployment_request(
        self,
        pipeline_id: str,
        source_stage: int = 0,
        target_stage: int = 1,
        items: Optional[List[Dict]] = None,
        note: str = "",
    ) -> Dict:
        """
        Create a deployment request (triggers approval flow).
        
        Args:
            pipeline_id: Deployment pipeline ID
            source_stage: Source stage order (0=DEV)
            target_stage: Target stage order (1=PROD)
            items: Specific items to deploy (None = all)
            note: Deployment note
        """
        payload = {
            "sourceStageOrder": source_stage,
            "isBackwardDeployment": False,
            "newWorkspace": {
                "name": None,  # Use existing
            },
            "options": {
                "allowCreateArtifact": False,
                "allowOverwriteArtifact": True,
                "allowOverwriteTargetArtifactLabel": False,
                "allowPurgeData": False,
                "allowSkipTilesWithMissingPrerequisites": True,
                "allowTakeOver": False,
            },
            "note": note,
        }
        
        if items:
            payload["items"] = items
        
        try:
            response = await self.client.post(
                f"/deploymentPipelines/{pipeline_id}/deploy",
                json=payload
            )
            return response
        except Exception as e:
            logger.error(f"Deployment request failed: {e}")
            raise
    
    async def get_deployment_status(
        self,
        pipeline_id: str,
        operation_id: str,
    ) -> Dict:
        """Get status of a deployment operation."""
        try:
            response = await self.client.get(
                f"/deploymentPipelines/{pipeline_id}/operations/{operation_id}"
            )
            return response
        except Exception as e:
            logger.error(f"Could not get deployment status: {e}")
            raise
    
    async def wait_for_deployment(
        self,
        pipeline_id: str,
        operation_id: str,
        timeout_seconds: int = 3600,
        poll_interval: int = 30,
    ) -> Dict:
        """
        Wait for deployment to complete.
        
        Note: Fabric Deployment Pipelines handle approval in their UI.
        This polls until the deployment is approved and completed.
        """
        import time
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            status = await self.get_deployment_status(pipeline_id, operation_id)
            state = status.get("status", "Unknown")
            
            if state == "Succeeded":
                return {"status": "approved", "result": status}
            elif state == "Failed":
                return {"status": "failed", "result": status}
            elif state == "Cancelled":
                return {"status": "rejected", "result": status}
            
            logger.info(f"Deployment status: {state}. Waiting...")
            await asyncio.sleep(poll_interval)
        
        return {"status": "timeout", "result": None}


# =============================================================================
# Simple Approval Manager (No Fabric Pipeline)
# =============================================================================

class SimpleApprovalManager:
    """
    Simple in-memory approval manager for testing.
    
    In production, replace with:
    - DeploymentPipelineManager (Fabric)
    - SlackApprovalManager
    - ServiceNowApprovalManager
    """
    
    def __init__(self):
        self._requests: Dict[str, ApprovalRequest] = {}
    
    async def create_request(
        self,
        workflow_id: str,
        change_summary: str,
        risk_level: str,
        impact_report: Dict,
    ) -> ApprovalRequest:
        """Create an approval request."""
        request = ApprovalRequest(
            id=str(uuid4()),
            workflow_id=workflow_id,
            change_summary=change_summary,
            risk_level=risk_level,
            impact_report=impact_report,
            requested_at=datetime.now(timezone.utc).isoformat(),
        )
        
        self._requests[request.id] = request
        
        logger.info(f"Approval request created: {request.id}")
        logger.info(f"  Change: {change_summary}")
        logger.info(f"  Risk: {risk_level}")
        
        return request
    
    async def approve(
        self,
        request_id: str,
        approved_by: str = "user",
    ) -> ApprovalRequest:
        """Approve a request."""
        request = self._requests.get(request_id)
        if not request:
            raise ValueError(f"Request not found: {request_id}")
        
        request.status = ApprovalStatus.APPROVED
        request.approved_by = approved_by
        request.approved_at = datetime.now(timezone.utc).isoformat()
        
        logger.info(f"Request approved: {request_id} by {approved_by}")
        
        return request
    
    async def reject(
        self,
        request_id: str,
        rejected_by: str = "user",
        reason: str = "",
    ) -> ApprovalRequest:
        """Reject a request."""
        request = self._requests.get(request_id)
        if not request:
            raise ValueError(f"Request not found: {request_id}")
        
        request.status = ApprovalStatus.REJECTED
        request.approved_by = rejected_by
        request.approved_at = datetime.now(timezone.utc).isoformat()
        request.rejection_reason = reason
        
        logger.info(f"Request rejected: {request_id} by {rejected_by}")
        
        return request
    
    async def get_status(self, request_id: str) -> ApprovalStatus:
        """Get approval status."""
        request = self._requests.get(request_id)
        if not request:
            raise ValueError(f"Request not found: {request_id}")
        return request.status
    
    def get_pending_requests(self) -> List[ApprovalRequest]:
        """Get all pending approval requests."""
        return [r for r in self._requests.values() if r.status == ApprovalStatus.PENDING]


# =============================================================================
# Agent Orchestrator
# =============================================================================

class AgentOrchestrator:
    """
    Coordinates multiple agents for complex workflows.
    
    Implements workflows like:
    - Safe Refactor: Discover → Analyze → Approve → Execute → Validate
    - Workspace Audit: Discover → Analyze → Report
    
    Integrates with approval systems for human-in-the-loop.
    """
    
    def __init__(
        self,
        fabric_client: Any,
        workspace_name: str,
        workspace_id: Optional[str] = None,
        use_deployment_pipelines: bool = False,
    ):
        """
        Initialize the orchestrator.
        
        Args:
            fabric_client: FabricApiClient instance
            workspace_name: Default workspace name
            workspace_id: Default workspace ID
            use_deployment_pipelines: If True, use Fabric pipelines for approval
        """
        self.fabric_client = fabric_client
        self.workspace_name = workspace_name
        self.workspace_id = workspace_id
        
        # Shared memory for all agents
        self.memory = SharedMemory()
        
        # Initialize agents
        self.discovery = DiscoveryAgent(
            memory=self.memory,
            fabric_client=fabric_client,
        )
        
        self.impact = ImpactAgent(memory=self.memory)
        
        self.refactor = RefactorAgent(
            memory=self.memory,
            workspace_name=workspace_name,
        )
        
        self.validation = ValidationAgent(workspace_name=workspace_name)
        
        # Approval manager
        if use_deployment_pipelines:
            self.approval = DeploymentPipelineManager(fabric_client)
        else:
            self.approval = SimpleApprovalManager()
        
        # Active workflows
        self._workflows: Dict[str, WorkflowResult] = {}

        # Continuation params for AWAITING_APPROVAL → EXECUTING resume.
        # When safe_refactor_workflow() pauses for approval, Steps 4+5 params
        # are stored here keyed by workflow_id. approve_workflow() retrieves
        # them to resume execution without re-running Steps 1+2.
        self._pending_continuations: Dict[str, Dict[str, Any]] = {}
    
    async def safe_refactor_workflow(
        self,
        refactor_type: str,
        model_name: str,
        old_name: str,
        new_name: str,
        skip_approval: bool = False,
        dry_run: bool = True,
    ) -> WorkflowResult:
        """
        Execute the safe refactor workflow.
        
        Steps:
        1. Discovery: Scan workspace, build dependency graph
        2. Impact: Analyze proposed change, calculate risk
        3. Approval: Request human approval (if high risk)
        4. Execution: Apply changes (with checkpoint)
        5. Validation: Test before/after
        6. Rollback: If validation fails
        
        Args:
            refactor_type: "rename_measure" etc.
            model_name: Target semantic model
            old_name: Current name
            new_name: New name
            skip_approval: If True, skip approval for high-risk changes
            dry_run: If True, only simulate execution
        """
        workflow_id = str(uuid4())
        workflow = WorkflowResult(
            workflow_id=workflow_id,
            workflow_type="safe_refactor",
            status=WorkflowStatus.PENDING,
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        self._workflows[workflow_id] = workflow
        
        logger.info(f"Starting safe_refactor workflow: {workflow_id}")
        logger.info(f"  Refactor: {refactor_type}")
        logger.info(f"  Target: {model_name}.[{old_name}] → [{new_name}]")
        logger.info(f"  Dry Run: {dry_run}")
        
        try:
            # =================================================================
            # Step 1: Discovery
            # =================================================================
            workflow.status = WorkflowStatus.DISCOVERING
            logger.info("[1/5] Running Discovery Agent...")
            
            # Resolve workspace ID if needed
            if not self.workspace_id:
                workspaces = await self.discovery._list_workspaces()
                for ws in workspaces:
                    if ws["name"].lower() == self.workspace_name.lower():
                        self.workspace_id = ws["id"]
                        break
            
            if not self.workspace_id:
                raise ValueError(f"Workspace not found: {self.workspace_name}")
            
            discovery_result = await self.discovery.discover_workspace(
                workspace_id=self.workspace_id,
                workspace_name=self.workspace_name,
                include_measures=True,
            )
            workflow.agent_results["discovery"] = discovery_result
            
            if discovery_result.status == TaskStatus.FAILED:
                raise Exception(f"Discovery failed: {discovery_result.error}")
            
            # Find model ID
            model_id = None
            graph = self.memory.get(f"dependency_graph_{self.workspace_id}")
            if graph:
                for node in graph.get("nodes", []):
                    if node.get("type") == "semantic_model" and node.get("name") == model_name:
                        model_id = node["id"]
                        break
            
            if not model_id:
                raise ValueError(f"Model not found: {model_name}")
            
            # =================================================================
            # Step 2: Impact Analysis
            # =================================================================
            workflow.status = WorkflowStatus.ANALYZING
            logger.info("[2/5] Running Impact Agent...")
            
            impact_result = await self.impact.analyze_change(
                change_type=refactor_type,
                workspace_id=self.workspace_id,
                model_id=model_id,
                measure_name=old_name,
                new_name=new_name,
            )
            workflow.agent_results["impact"] = impact_result
            
            if impact_result.status == TaskStatus.FAILED:
                raise Exception(f"Impact analysis failed: {impact_result.error}")
            
            impact_report = impact_result.result
            risk_level = impact_report.get("risk_level", "unknown")
            
            logger.info(f"  Risk Level: {risk_level}")
            logger.info(f"  Total Impact: {impact_report.get('total_impact', 0)}")
            
            # =================================================================
            # Step 3: Approval (if needed)
            # =================================================================
            requires_approval = risk_level in ("high_risk", "critical") and not skip_approval
            
            if requires_approval:
                workflow.status = WorkflowStatus.AWAITING_APPROVAL
                logger.info("[3/5] Requesting approval (HIGH RISK)...")
                
                approval_request = await self.approval.create_request(
                    workflow_id=workflow_id,
                    change_summary=f"Rename {model_name}.[{old_name}] → [{new_name}]",
                    risk_level=risk_level,
                    impact_report=impact_report,
                )
                workflow.approval_request = approval_request
                
                # For simple approval, we need manual approval
                # In production with Fabric pipelines, this would wait for UI approval
                if isinstance(self.approval, SimpleApprovalManager):
                    logger.warning("=" * 60)
                    logger.warning("APPROVAL REQUIRED")
                    logger.warning(f"Request ID: {approval_request.id}")
                    logger.warning(f"Risk Level: {risk_level}")
                    logger.warning("Call: orchestrator.approve_workflow(request_id)")
                    logger.warning("=" * 60)

                    # Save everything Steps 4+5 need — approve_workflow() will
                    # retrieve these and call _execute_steps_4_and_5() to complete
                    # the workflow after human approval.
                    self._pending_continuations[workflow_id] = {
                        "refactor_type": refactor_type,
                        "model_name": model_name,
                        "old_name": old_name,
                        "new_name": new_name,
                        "dry_run": dry_run,
                        "model_id": model_id,
                    }

                    # Return workflow in awaiting state
                    return workflow
            else:
                logger.info("[3/5] Approval not required (low risk or skipped)")
                workflow.status = WorkflowStatus.APPROVED
            
            # =================================================================
            # Step 4: Execution
            # =================================================================
            workflow.status = WorkflowStatus.EXECUTING
            logger.info(f"[4/5] Running Refactor Agent... (dry_run={dry_run})")
            
            # Capture before state for validation
            if not dry_run:
                # Generate test queries for validation
                test_queries = self.validation.generate_tests_for_measure(new_name)
                before_state = await self.validation.capture_state(model_name, test_queries)
                self.memory.set("validation_before_state", before_state)
                self.memory.set("validation_tests", test_queries)
            
            refactor_result = await self.refactor.execute_refactor(
                refactor_type=refactor_type,
                model_name=model_name,
                old_name=old_name,
                new_name=new_name,
                dry_run=dry_run,
            )
            workflow.agent_results["refactor"] = refactor_result
            
            if refactor_result.status == TaskStatus.FAILED:
                raise Exception(f"Refactor failed: {refactor_result.error}")
            
            # =================================================================
            # Step 5: Validation
            # =================================================================
            if not dry_run:
                workflow.status = WorkflowStatus.VALIDATING
                logger.info("[5/5] Running Validation Agent...")
                
                before_state = self.memory.get("validation_before_state")
                test_queries = self.memory.get("validation_tests")
                
                if before_state and test_queries:
                    validation_report = await self.validation.validate(
                        model_name=model_name,
                        tests=test_queries,
                        before_results=before_state,
                        operation_id=workflow_id,
                    )
                    
                    workflow.agent_results["validation"] = AgentResult(
                        agent_name="ValidationAgent",
                        task="validate",
                        status=TaskStatus.COMPLETED if validation_report.overall_passed else TaskStatus.FAILED,
                        result=validation_report.__dict__,
                    )
                    
                    if not validation_report.overall_passed:
                        logger.warning("Validation FAILED - initiating rollback")
                        
                        checkpoint_id = refactor_result.result.get("checkpoint_id")
                        if checkpoint_id:
                            rollback_result = await self.refactor.execute_refactor(
                                refactor_type="rollback",
                                checkpoint_id=checkpoint_id,
                                dry_run=False,
                            )
                            workflow.agent_results["rollback"] = rollback_result
                            workflow.status = WorkflowStatus.ROLLED_BACK
                            workflow.error = "Validation failed, changes rolled back"
                        else:
                            workflow.status = WorkflowStatus.FAILED
                            workflow.error = "Validation failed, no checkpoint for rollback"
                        
                        return workflow
            else:
                logger.info("[5/5] Validation skipped (dry run)")
            
            # =================================================================
            # Complete
            # =================================================================
            workflow.status = WorkflowStatus.COMPLETED
            workflow.completed_at = datetime.now(timezone.utc).isoformat()
            workflow.final_result = refactor_result.result
            
            logger.info("=" * 60)
            logger.info("WORKFLOW COMPLETED SUCCESSFULLY")
            logger.info(f"  Workflow ID: {workflow_id}")
            logger.info(f"  Status: {workflow.status.value}")
            logger.info(f"  Dry Run: {dry_run}")
            logger.info("=" * 60)
            
            return workflow
            
        except Exception as e:
            logger.error(f"Workflow failed: {e}")
            workflow.status = WorkflowStatus.FAILED
            workflow.error = str(e)
            workflow.completed_at = datetime.now(timezone.utc).isoformat()
            return workflow
    
    async def _execute_steps_4_and_5(
        self,
        workflow: "WorkflowResult",
        params: Dict[str, Any],
    ) -> "WorkflowResult":
        """
        Execute Steps 4 (Refactor) and 5 (Validation) of safe_refactor_workflow.

        Extracted so that approve_workflow() can resume a paused workflow without
        re-running discovery or impact analysis (Steps 1+2 already completed).

        WHY THIS MATTERS:
          Before this fix, approve_workflow() set status=APPROVED and returned —
          the actual refactoring never happened. High-risk workflows were permanently
          stuck in AWAITING_APPROVAL after approval.

        Args:
            workflow: The WorkflowResult object (already in APPROVED state).
            params:   Dict with refactor_type, model_name, old_name, new_name,
                      dry_run, model_id — all captured in safe_refactor_workflow().

        Returns:
            Updated WorkflowResult with COMPLETED, ROLLED_BACK, or FAILED status.
        """
        refactor_type = params["refactor_type"]
        model_name = params["model_name"]
        old_name = params["old_name"]
        new_name = params["new_name"]
        dry_run = params["dry_run"]

        try:
            # =========================================================
            # Step 4: Execution
            # =========================================================
            workflow.status = WorkflowStatus.EXECUTING
            logger.info(f"[4/5] Running Refactor Agent... (dry_run={dry_run})")

            if not dry_run:
                test_queries = self.validation.generate_tests_for_measure(new_name)
                before_state = await self.validation.capture_state(model_name, test_queries)
                self.memory.set("validation_before_state", before_state)
                self.memory.set("validation_tests", test_queries)

            refactor_result = await self.refactor.execute_refactor(
                refactor_type=refactor_type,
                model_name=model_name,
                old_name=old_name,
                new_name=new_name,
                dry_run=dry_run,
            )
            workflow.agent_results["refactor"] = refactor_result

            if refactor_result.status == TaskStatus.FAILED:
                raise Exception(f"Refactor failed: {refactor_result.error}")

            # =========================================================
            # Step 5: Validation
            # =========================================================
            if not dry_run:
                workflow.status = WorkflowStatus.VALIDATING
                logger.info("[5/5] Running Validation Agent...")

                before_state = self.memory.get("validation_before_state")
                test_queries = self.memory.get("validation_tests")

                if before_state and test_queries:
                    validation_report = await self.validation.validate(
                        model_name=model_name,
                        tests=test_queries,
                        before_results=before_state,
                        operation_id=workflow.workflow_id,
                    )

                    workflow.agent_results["validation"] = AgentResult(
                        agent_name="ValidationAgent",
                        task="validate",
                        status=(
                            TaskStatus.COMPLETED
                            if validation_report.overall_passed
                            else TaskStatus.FAILED
                        ),
                        result=validation_report.__dict__,
                    )

                    if not validation_report.overall_passed:
                        logger.warning("Validation FAILED — initiating rollback")
                        checkpoint_id = refactor_result.result.get("checkpoint_id")
                        if checkpoint_id:
                            rollback_result = await self.refactor.execute_refactor(
                                refactor_type="rollback",
                                checkpoint_id=checkpoint_id,
                                dry_run=False,
                            )
                            workflow.agent_results["rollback"] = rollback_result
                            workflow.status = WorkflowStatus.ROLLED_BACK
                            workflow.error = "Validation failed, changes rolled back"
                        else:
                            workflow.status = WorkflowStatus.FAILED
                            workflow.error = "Validation failed, no checkpoint for rollback"
                        workflow.completed_at = datetime.now(timezone.utc).isoformat()
                        return workflow
            else:
                logger.info("[5/5] Validation skipped (dry run)")

            workflow.status = WorkflowStatus.COMPLETED
            workflow.completed_at = datetime.now(timezone.utc).isoformat()
            workflow.final_result = refactor_result.result

            logger.info("=" * 60)
            logger.info("WORKFLOW COMPLETED SUCCESSFULLY (post-approval)")
            logger.info(f"  Workflow ID: {workflow.workflow_id}")
            logger.info(f"  Dry Run: {dry_run}")
            logger.info("=" * 60)

            return workflow

        except Exception as exc:
            logger.error(f"Workflow failed in Steps 4+5: {exc}")
            workflow.status = WorkflowStatus.FAILED
            workflow.error = str(exc)
            workflow.completed_at = datetime.now(timezone.utc).isoformat()
            return workflow

    async def approve_workflow(
        self,
        request_id: str,
        approved_by: str = "user",
    ) -> WorkflowResult:
        """
        Approve a pending workflow and continue execution.

        Call this after receiving approval for a high-risk change.
        Retrieves the saved continuation params and resumes Steps 4+5.
        """
        if isinstance(self.approval, SimpleApprovalManager):
            request = await self.approval.approve(request_id, approved_by)
            workflow_id = request.workflow_id

            workflow = self._workflows.get(workflow_id)
            if not workflow:
                raise ValueError(f"Workflow not found: {workflow_id}")

            if workflow.status != WorkflowStatus.AWAITING_APPROVAL:
                raise ValueError(f"Workflow not awaiting approval: {workflow.status}")

            workflow.status = WorkflowStatus.APPROVED
            logger.info(f"Workflow approved by {approved_by!r} — resuming execution: {workflow_id}")

            # Retrieve the continuation params saved when workflow paused
            params = self._pending_continuations.pop(workflow_id, None)
            if params is None:
                # Should not happen, but degrade gracefully rather than silently
                logger.error(
                    f"No continuation params found for workflow {workflow_id}. "
                    "Cannot resume Steps 4+5."
                )
                workflow.status = WorkflowStatus.FAILED
                workflow.error = "Missing continuation params — cannot resume after approval"
                workflow.completed_at = datetime.now(timezone.utc).isoformat()
                return workflow

            # Resume Steps 4 (Execution) + 5 (Validation)
            return await self._execute_steps_4_and_5(workflow, params)

        else:
            logger.warning("approve_workflow called with non-SimpleApprovalManager — no-op")
            return None

    async def reject_workflow(
        self,
        request_id: str,
        rejected_by: str = "user",
        reason: str = "",
    ) -> WorkflowResult:
        """Reject a pending workflow."""
        if isinstance(self.approval, SimpleApprovalManager):
            request = await self.approval.reject(request_id, rejected_by, reason)
            workflow_id = request.workflow_id
            
            workflow = self._workflows.get(workflow_id)
            if workflow:
                workflow.status = WorkflowStatus.REJECTED
                workflow.completed_at = datetime.now(timezone.utc).isoformat()
                workflow.error = f"Rejected by {rejected_by}: {reason}"
            
            return workflow
        else:
            logger.warning("reject_workflow called with non-SimpleApprovalManager — no-op")
            return None
    
    def get_workflow(self, workflow_id: str) -> Optional[WorkflowResult]:
        """Get a workflow by ID."""
        return self._workflows.get(workflow_id)
    
    def get_pending_approvals(self) -> List[ApprovalRequest]:
        """Get all pending approval requests."""
        if isinstance(self.approval, SimpleApprovalManager):
            return self.approval.get_pending_requests()
        return []
