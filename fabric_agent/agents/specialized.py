"""
Specialized Agents
==================

Implements the specialized agents for the multi-agent system:
- DiscoveryAgent: Scans workspaces, builds dependency graphs
- ImpactAgent: Analyzes changes, scores risk
- RefactorAgent: Executes changes with safety checks

Author: Fabric Agent Team
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from loguru import logger

from fabric_agent.agents.base import (
    BaseAgent,
    SimpleAgent,
    AgentRole,
    AgentResult,
    ToolDefinition,
    SharedMemory,
    LLMClient,
    TaskStatus,
)


# =============================================================================
# Discovery Agent
# =============================================================================

class DiscoveryAgent(SimpleAgent):
    """
    Agent that discovers and maps workspace structure.
    
    Responsibilities:
    - Scan workspaces for items
    - Build dependency graphs
    - Detect changes since last scan
    - Identify anomalies (orphans, cycles)
    
    This agent doesn't need LLM reasoning - it's deterministic.
    """
    
    def __init__(
        self,
        memory: SharedMemory,
        fabric_client: Any,  # FabricApiClient
    ):
        self.fabric_client = fabric_client
        
        tools = [
            ToolDefinition(
                name="list_workspaces",
                description="List all accessible workspaces",
                parameters={},
                handler=self._list_workspaces,
            ),
            ToolDefinition(
                name="scan_workspace",
                description="Scan a workspace and list all items",
                parameters={
                    "workspace_id": {"type": "string", "description": "Workspace ID"},
                },
                handler=self._scan_workspace,
            ),
            ToolDefinition(
                name="build_dependency_graph",
                description="Build dependency graph for a workspace",
                parameters={
                    "workspace_id": {"type": "string", "description": "Workspace ID"},
                    "workspace_name": {"type": "string", "description": "Workspace name"},
                    "include_measures": {"type": "boolean", "description": "Include measure-level dependencies"},
                },
                handler=self._build_graph,
            ),
            ToolDefinition(
                name="get_model_definition",
                description="Get semantic model definition (TMDL)",
                parameters={
                    "workspace_id": {"type": "string", "description": "Workspace ID"},
                    "model_id": {"type": "string", "description": "Model ID"},
                },
                handler=self._get_model_definition,
            ),
        ]
        
        super().__init__(
            name="DiscoveryAgent",
            role=AgentRole.DISCOVERY,
            memory=memory,
            tools=tools,
        )
    
    async def _list_workspaces(self) -> List[Dict]:
        """List all workspaces."""
        response = await self.fabric_client.get("/workspaces")
        workspaces = response.get("value", [])
        
        # Cache in memory
        self.memory.set("workspaces", workspaces)
        
        return [{"id": w["id"], "name": w.get("displayName", "")} for w in workspaces]
    
    async def _scan_workspace(self, workspace_id: str) -> Dict:
        """Scan a workspace for items."""
        response = await self.fabric_client.get(f"/workspaces/{workspace_id}/items")
        items = response.get("value", [])
        
        # Categorize items
        by_type = {}
        for item in items:
            item_type = item.get("type", "Unknown")
            if item_type not in by_type:
                by_type[item_type] = []
            by_type[item_type].append({
                "id": item["id"],
                "name": item.get("displayName", ""),
                "type": item_type,
            })
        
        result = {
            "workspace_id": workspace_id,
            "total_items": len(items),
            "by_type": by_type,
        }
        
        # Cache in memory
        self.memory.set(f"workspace_items_{workspace_id}", result)
        
        return result
    
    async def _build_graph(
        self,
        workspace_id: str,
        workspace_name: str,
        include_measures: bool = True,
    ) -> Dict:
        """Build dependency graph for workspace."""
        from fabric_agent.tools.workspace_graph import WorkspaceGraphBuilder
        
        builder = WorkspaceGraphBuilder(include_measure_graph=include_measures)
        graph = await builder.build(
            client=self.fabric_client,
            workspace_id=workspace_id,
            workspace_name=workspace_name,
        )
        
        # Cache in memory
        self.memory.set(f"dependency_graph_{workspace_id}", graph)
        
        return {
            "workspace_id": workspace_id,
            "nodes": len(graph.get("nodes", [])),
            "edges": len(graph.get("edges", [])),
            "stats": graph.get("stats", {}),
        }
    
    async def _get_model_definition(self, workspace_id: str, model_id: str) -> Dict:
        """Get semantic model definition."""
        response = await self.fabric_client.post(
            f"/workspaces/{workspace_id}/semanticModels/{model_id}/getDefinition",
            json={"format": "TMDL"}
        )
        return response
    
    async def discover_workspace(
        self,
        workspace_id: str,
        workspace_name: str,
        include_measures: bool = True,
    ) -> AgentResult:
        """
        Full discovery of a workspace.
        
        Combines scanning + graph building.
        """
        import time
        start = time.time()
        
        try:
            # Scan items
            items = await self._scan_workspace(workspace_id)
            
            # Build graph
            graph = await self._build_graph(workspace_id, workspace_name, include_measures)
            
            result = {
                "workspace_id": workspace_id,
                "workspace_name": workspace_name,
                "items": items,
                "graph": graph,
            }
            
            return AgentResult(
                agent_name=self.name,
                task="discover_workspace",
                status=TaskStatus.COMPLETED,
                result=result,
                execution_time_ms=int((time.time() - start) * 1000),
            )
            
        except Exception as e:
            return AgentResult(
                agent_name=self.name,
                task="discover_workspace",
                status=TaskStatus.FAILED,
                result=None,
                error=str(e),
                execution_time_ms=int((time.time() - start) * 1000),
            )


# =============================================================================
# Impact Agent
# =============================================================================

class ImpactAgent(SimpleAgent):
    """
    Agent that analyzes the impact of proposed changes.

    Responsibilities:
    - Analyze dependencies for a proposed change
    - Calculate risk scores (data-driven: calibrated by real failure history)
    - Find affected items (reports, measures)
    - Generate impact reports
    - Suggest mitigations

    DATA-DRIVEN RISK SCORING:
      When operation_memory is provided, _calculate_risk_score() retrieves
      historical context for the proposed change via vector similarity search.
      If similar past operations had a high failure rate (> 30%), the base
      score is boosted proportionally and specific recommendations are returned.

      Without history (cold start): falls back to the algorithmic base score.
      With history (warm): leverages past outcomes to prevent repeat failures.

    FAANG PARALLEL:
      This is the same pattern as:
      - Google's SRE change risk analysis (uses incident history)
      - Netflix's deployment risk scoring (uses past failure correlation)
      - Stripe's fraud scoring (historical patterns boost baseline score)
    """

    def __init__(self, memory: SharedMemory, operation_memory=None):
        """
        Args:
            memory:           Shared in-memory state (workspace graphs, etc.).
            operation_memory: Optional OperationMemory instance for historical
                              risk calibration. If None, base score only.
        """
        self._operation_memory = operation_memory
        tools = [
            ToolDefinition(
                name="analyze_measure_rename",
                description="Analyze impact of renaming a measure",
                parameters={
                    "workspace_id": {"type": "string"},
                    "model_id": {"type": "string"},
                    "measure_name": {"type": "string"},
                    "new_name": {"type": "string"},
                },
                handler=self._analyze_measure_rename,
            ),
            ToolDefinition(
                name="analyze_notebook_change",
                description="Analyze impact of changing a notebook",
                parameters={
                    "workspace_id": {"type": "string"},
                    "notebook_id": {"type": "string"},
                },
                handler=self._analyze_notebook_change,
            ),
            ToolDefinition(
                name="calculate_risk_score",
                description="Calculate risk score for a change",
                parameters={
                    "total_impact": {"type": "integer"},
                    "has_production_reports": {"type": "boolean"},
                    "has_downstream_dependencies": {"type": "boolean"},
                },
                handler=self._calculate_risk_score,
            ),
        ]
        
        super().__init__(
            name="ImpactAgent",
            role=AgentRole.IMPACT,
            memory=memory,
            tools=tools,
        )
    
    async def _analyze_measure_rename(
        self,
        workspace_id: str,
        model_id: str,
        measure_name: str,
        new_name: str,
    ) -> Dict:
        """Analyze impact of measure rename."""
        from fabric_agent.tools.workspace_graph import GraphImpactAnalyzer
        
        # Get cached graph
        graph = self.memory.get(f"dependency_graph_{workspace_id}")
        
        if not graph:
            return {"error": "No cached graph. Run DiscoveryAgent first."}
        
        analyzer = GraphImpactAnalyzer(graph)
        impact = analyzer.analyze_measure_rename_impact(model_id, measure_name)
        
        # Enhance with additional analysis
        impact["proposed_new_name"] = new_name
        impact["requires_approval"] = impact["risk_level"] in ("high_risk", "critical")
        
        return impact
    
    async def _analyze_notebook_change(
        self,
        workspace_id: str,
        notebook_id: str,
    ) -> Dict:
        """Analyze impact of notebook change."""
        from fabric_agent.tools.workspace_graph import GraphImpactAnalyzer
        
        graph = self.memory.get(f"dependency_graph_{workspace_id}")
        
        if not graph:
            return {"error": "No cached graph. Run DiscoveryAgent first."}
        
        analyzer = GraphImpactAnalyzer(graph)
        return analyzer.analyze_notebook_change_impact(notebook_id)
    
    async def _calculate_risk_score(
        self,
        total_impact: int,
        has_production_reports: bool,
        has_downstream_dependencies: bool,
        proposed_change: str = "",
    ) -> Dict:
        """
        Calculate composite risk score — data-driven when history is available.

        STEP 1: Algorithmic base score (always runs):
          base = total_impact × 2
          × 1.5 if production reports are affected
          × 1.3 if downstream dependencies exist

        STEP 2: Historical calibration (when operation_memory is wired in):
          Retrieves top-K similar past operations via vector similarity.
          If historical_failure_rate > 30%: score × (1 + failure_rate)
          Also surfaces past failure reasons as recommendations.

        WHY THIS MATTERS:
          A "rename Revenue → Gross Revenue" might score medium_risk by
          algorithm alone. But if history shows that 5 of the last 7 similar
          renames rolled back due to DAX formula failures, we should treat it
          as high_risk and surface that warning before execution.

        Args:
            total_impact:                 Number of affected items.
            has_production_reports:       True if production reports reference this.
            has_downstream_dependencies:  True if other measures/models depend on it.
            proposed_change:              Natural language description for RAG lookup.
        """
        # --- Step 1: Algorithmic base score (unchanged from original) ---
        base_score = total_impact * 2

        if has_production_reports:
            base_score *= 1.5

        if has_downstream_dependencies:
            base_score *= 1.3

        # --- Step 2: Historical calibration ---
        historical_failure_rate = 0.0
        recommendations: list = []
        similar_op_count = 0

        if self._operation_memory and proposed_change:
            try:
                ctx = await self._operation_memory.get_risk_context(proposed_change)
                historical_failure_rate = ctx.historical_failure_rate
                recommendations = list(ctx.recommendations)
                similar_op_count = len(ctx.similar_operations)

                if historical_failure_rate > 0.3:
                    # History shows > 30% failure rate — boost the score.
                    # A 50% failure rate doubles the score; 100% failure rate triples it.
                    multiplier = 1.0 + historical_failure_rate
                    base_score *= multiplier
                    logger.warning(
                        f"ImpactAgent: historical failure rate {historical_failure_rate:.0%} "
                        f"on similar ops → score boosted by {multiplier:.2f}x"
                    )
            except Exception as exc:
                # Never let history lookup block the analysis — degrade gracefully
                logger.warning(f"ImpactAgent: could not load historical context: {exc}")

        # --- Risk level classification ---
        if base_score == 0:
            level = "safe"
        elif base_score <= 5:
            level = "low_risk"
        elif base_score <= 15:
            level = "medium_risk"
        elif base_score <= 40:
            level = "high_risk"
        else:
            level = "critical"

        return {
            "score": round(base_score, 2),
            "level": level,
            "requires_approval": level in ("high_risk", "critical"),
            "historical_failure_rate": round(historical_failure_rate, 4),
            "similar_operations_found": similar_op_count,
            "recommendations": recommendations,
            "factors": {
                "total_impact": total_impact,
                "has_production_reports": has_production_reports,
                "has_downstream_dependencies": has_downstream_dependencies,
            },
        }
    
    async def analyze_change(
        self,
        change_type: str,
        workspace_id: str,
        **kwargs,
    ) -> AgentResult:
        """
        Analyze a proposed change.
        
        Args:
            change_type: "measure_rename", "notebook_change", etc.
            workspace_id: Target workspace
            **kwargs: Change-specific parameters
        """
        import time
        start = time.time()
        
        try:
            if change_type == "measure_rename":
                impact = await self._analyze_measure_rename(
                    workspace_id=workspace_id,
                    model_id=kwargs.get("model_id", ""),
                    measure_name=kwargs.get("measure_name", ""),
                    new_name=kwargs.get("new_name", ""),
                )
            elif change_type == "notebook_change":
                impact = await self._analyze_notebook_change(
                    workspace_id=workspace_id,
                    notebook_id=kwargs.get("notebook_id", ""),
                )
            else:
                impact = {"error": f"Unknown change type: {change_type}"}
            
            return AgentResult(
                agent_name=self.name,
                task=f"analyze_{change_type}",
                status=TaskStatus.COMPLETED,
                result=impact,
                execution_time_ms=int((time.time() - start) * 1000),
            )
            
        except Exception as e:
            return AgentResult(
                agent_name=self.name,
                task=f"analyze_{change_type}",
                status=TaskStatus.FAILED,
                result=None,
                error=str(e),
                execution_time_ms=int((time.time() - start) * 1000),
            )


# =============================================================================
# Refactor Agent
# =============================================================================

class RefactorAgent(SimpleAgent):
    """
    Agent that executes refactoring operations.
    
    Responsibilities:
    - Create checkpoints before changes
    - Execute refactoring (rename, update expression)
    - Coordinate with Validation Agent
    - Handle rollback on failure
    """
    
    def __init__(
        self,
        memory: SharedMemory,
        workspace_name: str,
    ):
        self.workspace_name = workspace_name
        self._executor = None
        
        tools = [
            ToolDefinition(
                name="rename_measure",
                description="Rename a measure with automatic reference updates",
                parameters={
                    "model_name": {"type": "string"},
                    "old_name": {"type": "string"},
                    "new_name": {"type": "string"},
                    "dry_run": {"type": "boolean"},
                },
                handler=self._rename_measure,
            ),
            ToolDefinition(
                name="rollback",
                description="Rollback to a checkpoint",
                parameters={
                    "checkpoint_id": {"type": "string"},
                    "dry_run": {"type": "boolean"},
                },
                handler=self._rollback,
            ),
        ]
        
        super().__init__(
            name="RefactorAgent",
            role=AgentRole.REFACTOR,
            memory=memory,
            tools=tools,
        )
    
    def _get_executor(self):
        """Lazy load executor."""
        if self._executor is None:
            from fabric_agent.refactor.executor import RefactorExecutor
            self._executor = RefactorExecutor(workspace_name=self.workspace_name)
        return self._executor
    
    async def _rename_measure(
        self,
        model_name: str,
        old_name: str,
        new_name: str,
        dry_run: bool = True,
    ) -> Dict:
        """Execute measure rename."""
        executor = self._get_executor()
        result = await executor.rename_measure(
            model_name=model_name,
            old_name=old_name,
            new_name=new_name,
            update_references=True,
            dry_run=dry_run,
        )
        return result.to_dict()
    
    async def _rollback(
        self,
        checkpoint_id: str,
        dry_run: bool = True,
    ) -> Dict:
        """Rollback to checkpoint."""
        executor = self._get_executor()
        result = await executor.rollback(
            checkpoint_id=checkpoint_id,
            dry_run=dry_run,
        )
        return result.to_dict()
    
    async def execute_refactor(
        self,
        refactor_type: str,
        dry_run: bool = True,
        **kwargs,
    ) -> AgentResult:
        """
        Execute a refactoring operation.
        
        Args:
            refactor_type: "rename_measure", "rollback"
            dry_run: If True, only simulate
            **kwargs: Refactor-specific parameters
        """
        import time
        start = time.time()
        
        try:
            if refactor_type == "rename_measure":
                result = await self._rename_measure(
                    model_name=kwargs.get("model_name", ""),
                    old_name=kwargs.get("old_name", ""),
                    new_name=kwargs.get("new_name", ""),
                    dry_run=dry_run,
                )
            elif refactor_type == "rollback":
                result = await self._rollback(
                    checkpoint_id=kwargs.get("checkpoint_id", ""),
                    dry_run=dry_run,
                )
            else:
                result = {"error": f"Unknown refactor type: {refactor_type}"}
            
            # Store checkpoint in memory for potential rollback
            if result.get("checkpoint_id"):
                self.memory.set("last_checkpoint_id", result["checkpoint_id"])
            
            return AgentResult(
                agent_name=self.name,
                task=f"execute_{refactor_type}",
                status=TaskStatus.COMPLETED if result.get("success", False) else TaskStatus.FAILED,
                result=result,
                execution_time_ms=int((time.time() - start) * 1000),
            )
            
        except Exception as e:
            return AgentResult(
                agent_name=self.name,
                task=f"execute_{refactor_type}",
                status=TaskStatus.FAILED,
                result=None,
                error=str(e),
                execution_time_ms=int((time.time() - start) * 1000),
            )


# =============================================================================
# Healer Agent
# =============================================================================


class HealerAgent(SimpleAgent):
    """
    Agent that autonomously detects and heals Fabric infrastructure anomalies.

    WHAT: Runs the detect → plan → execute loop using AnomalyDetector
    and SelfHealer. Exposes results as structured AgentResult for the
    orchestrator or direct MCP tool use.

    WHY: At FAANG scale, humans can't monitor every workspace item.
    Self-healing agents reduce MTTR (Mean Time To Repair) from hours
    to minutes by applying safe fixes automatically and routing hard
    cases to the right human.

    TOOLS EXPOSED:
        scan_for_anomalies   → run AnomalyDetector.scan()
        build_healing_plan   → run SelfHealer.build_plan()
        execute_healing_plan → run SelfHealer.execute_healing_plan()
        get_health_report    → run SelfHealingMonitor.run_once()

    USAGE:
        healer = HealerAgent(
            memory=shared_memory,
            fabric_client=client,
            memory_manager=memory_manager,
        )
        result = await healer.run(
            task="health_scan",
            context={"workspace_ids": ["ws-123"], "dry_run": True}
        )
        report = result.result  # HealthReport
    """

    def __init__(
        self,
        memory: SharedMemory,
        fabric_client: Any,               # FabricApiClient
        memory_manager: Optional[Any] = None,  # MemoryManager
        contracts_dir: Optional[str] = None,
        stale_hours: int = 24,
    ):
        self.fabric_client = fabric_client
        self.memory_manager = memory_manager

        tools = [
            ToolDefinition(
                name="scan_for_anomalies",
                description="Scan workspace(s) for infrastructure anomalies",
                parameters={
                    "workspace_ids": {"type": "array", "items": {"type": "string"}},
                },
                handler=self._scan_for_anomalies,
            ),
            ToolDefinition(
                name="build_healing_plan",
                description="Build a healing plan from detected anomalies",
                parameters={
                    "workspace_ids": {"type": "array", "items": {"type": "string"}},
                },
                handler=self._build_healing_plan,
            ),
            ToolDefinition(
                name="execute_healing_plan",
                description="Execute safe auto-fix actions from a healing plan",
                parameters={
                    "workspace_ids": {"type": "array", "items": {"type": "string"}},
                    "dry_run": {"type": "boolean", "default": True},
                },
                handler=self._execute_healing_plan,
            ),
            ToolDefinition(
                name="get_health_report",
                description="Run full scan + heal cycle and return a HealthReport",
                parameters={
                    "workspace_ids": {"type": "array", "items": {"type": "string"}},
                    "dry_run": {"type": "boolean", "default": True},
                },
                handler=self._get_health_report,
            ),
        ]

        super().__init__(
            name="healer",
            role=AgentRole.REFACTOR,   # Closest existing role; healing = proactive refactor
            memory=memory,
            tools=tools,
        )

        # Store config for lazy monitor construction
        self._contracts_dir = contracts_dir
        self._stale_hours = stale_hours
        self._monitor: Optional[Any] = None

    def _get_monitor(self):
        """Lazy-construct SelfHealingMonitor on first use."""
        if self._monitor is None:
            from fabric_agent.healing.monitor import SelfHealingMonitor
            self._monitor = SelfHealingMonitor(
                client=self.fabric_client,
                memory=self.memory_manager,
                contracts_dir=self._contracts_dir,
                stale_hours=self._stale_hours,
            )
        return self._monitor

    async def run(self, task: str, context: Dict[str, Any]) -> AgentResult:
        """
        Execute a healing task.

        Args:
            task: One of "health_scan", "scan_anomalies", "build_plan", "execute_plan".
            context: Must include "workspace_ids". Optional "dry_run" (default True).

        Returns:
            AgentResult with result=HealthReport (or List[Anomaly] / HealingPlan).
        """
        import time
        start = time.time()
        workspace_ids = context.get("workspace_ids", [])
        dry_run = context.get("dry_run", True)

        try:
            if task in ("health_scan", "get_health_report"):
                result = await self._get_health_report(
                    workspace_ids=workspace_ids, dry_run=dry_run
                )
            elif task == "scan_anomalies":
                result = await self._scan_for_anomalies(workspace_ids=workspace_ids)
            elif task == "build_plan":
                result = await self._build_healing_plan(workspace_ids=workspace_ids)
            elif task == "execute_plan":
                result = await self._execute_healing_plan(
                    workspace_ids=workspace_ids, dry_run=dry_run
                )
            else:
                raise ValueError(f"Unknown healer task: {task}")

            return AgentResult(
                agent_name=self.name,
                task=task,
                status=TaskStatus.COMPLETED,
                result=result,
                execution_time_ms=int((time.time() - start) * 1000),
            )
        except Exception as e:
            logger.error(f"HealerAgent.run({task}) failed: {e}")
            return AgentResult(
                agent_name=self.name,
                task=task,
                status=TaskStatus.FAILED,
                result=None,
                error=str(e),
                execution_time_ms=int((time.time() - start) * 1000),
            )

    async def _scan_for_anomalies(self, workspace_ids: List[str], **_) -> List[Dict]:
        """Run anomaly detection and return serializable list."""
        monitor = self._get_monitor()
        anomalies = await monitor.detector.scan(workspace_ids)
        return [a.to_dict() for a in anomalies]

    async def _build_healing_plan(self, workspace_ids: List[str], **_) -> Dict:
        """Scan + build plan, return serializable plan dict."""
        monitor = self._get_monitor()
        anomalies = await monitor.detector.scan(workspace_ids)
        plan = monitor.healer.build_plan(anomalies)
        return plan.to_dict()

    async def _execute_healing_plan(
        self, workspace_ids: List[str], dry_run: bool = True, **_
    ) -> Dict:
        """Scan + build plan + execute, return result dict."""
        monitor = self._get_monitor()
        anomalies = await monitor.detector.scan(workspace_ids)
        plan = monitor.healer.build_plan(anomalies)
        result = await monitor.healer.execute_healing_plan(plan, dry_run=dry_run)
        return result.to_dict()

    async def _get_health_report(
        self, workspace_ids: List[str], dry_run: bool = True, **_
    ) -> Dict:
        """Full scan cycle, return HealthReport as dict."""
        monitor = self._get_monitor()
        report = await monitor.run_once(workspace_ids, dry_run=dry_run)
        return report.to_dict()
