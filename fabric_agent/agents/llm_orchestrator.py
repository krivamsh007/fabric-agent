"""
llm_orchestrator.py — AI-Powered Workflow Orchestrator
=======================================================

The intelligence layer of the Fabric Agent system. An LLM (Ollama/Claude/Azure)
reasons about WHAT to do and in what order, then delegates each step to the
appropriate deterministic sub-agent.

ARCHITECTURE:
  This follows the "Orchestrator-as-Brain" pattern — one LLM-driven agent
  decides the tool call sequence while sub-agents stay deterministic:

    User: "Safely rename Total Revenue → Gross Revenue in Sales Model"
             │
             ▼
    [OrchestratorLLMAgent]  ← LLM decides tool order, adapts to findings
         LLM calls tools in order it determines:
         1. discover_workspace()              → DiscoveryAgent (deterministic)
         2. find_historical_context()         → OperationMemory RAG
         3. analyze_refactor_impact()         → ImpactAgent (deterministic)
         [pause if high_risk → APPROVAL_REQUIRED message]
         4. execute_refactor()                → RefactorAgent (deterministic)
         5. validate_changes()                → ValidationAgent (deterministic)
         [rollback if validation fails]

WHY LLM-DRIVEN ORCHESTRATION:
  Sub-agents are deterministic for a reason — they execute safety-critical ops.
  But the SEQUENCE of which tool to call, and whether to pause for approval,
  benefits from LLM reasoning. The LLM can:
  - Adapt to unexpected findings (model not found → suggest workspace scan)
  - Explain its reasoning in natural language (useful for audit trail)
  - Handle ambiguous user requests (partial names, typos)
  - Know when to escalate vs. proceed

FAANG PARALLEL:
  - LangGraph: stateful multi-agent graph with LLM router (Google DeepMind)
  - AutoGen: multi-agent conversation with LLM orchestrator (Microsoft)
  - CrewAI: role-based agent orchestration with task delegation
  - AWS Bedrock Agents: LLM-driven tool calling over deterministic APIs
  This is the Fabric-native equivalent of all four.

PROVIDER-AGNOSTIC:
  The OrchestratorLLMAgent accepts any LLMClient — Ollama (default),
  Claude, or Azure OpenAI. Switch providers via LLM_PROVIDER in .env.

SYSTEM PROMPT RULES:
  The LLM follows these rules (injected into every call):
  1. Always run discover_workspace BEFORE analyze_refactor_impact
  2. Always run find_historical_context BEFORE executing changes
  3. Always run execute_refactor with dry_run=true BEFORE dry_run=false
  4. If risk_level is "high_risk" or "critical": output APPROVAL_REQUIRED
     with the full risk summary and DO NOT execute. Wait for approval.
  5. If validation fails after live execution: immediately call rollback()

USAGE:
  from fabric_agent.agents.llm_orchestrator import OrchestratorLLMAgent
  from fabric_agent.api.llm_providers import create_llm_client
  from fabric_agent.core.config import AgentConfig

  config = AgentConfig()
  llm = create_llm_client(config)    # Ollama by default
  orchestrator = OrchestratorLLMAgent(llm=llm, fabric_client=client, ...)
  result = await orchestrator.run(
      "Rename Total Revenue to Gross Revenue in Sales Model",
      context={"workspace_id": "ws-123", "workspace_name": "Analytics"}
  )
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from loguru import logger

from fabric_agent.agents.base import (
    BaseAgent,
    AgentRole,
    AgentResult,
    SharedMemory,
    LLMClient,
    ToolDefinition,
    TaskStatus,
)


# =============================================================================
# System Prompt
# =============================================================================

_SYSTEM_PROMPT = """You are the Fabric AI Orchestrator — the AI brain of a Microsoft Fabric
semantic model refactoring system. You coordinate specialized sub-agents to
safely rename measures, fix schema drift, and maintain data consistency.

AVAILABLE TOOLS:
  discover_workspace(workspace_id, workspace_name)
      → Scans the workspace, builds a dependency graph of all semantic models,
        measures, and reports. ALWAYS call this first.

  find_historical_context(proposed_change_description)
      → Searches past operations using vector similarity. Returns similar past
        renames, their outcomes (success/failure/rollback), and recommendations.
        ALWAYS call this before executing any change.

  analyze_refactor_impact(workspace_id, model_id, measure_name, new_name)
      → Calculates risk score, finds affected reports and downstream measures.
        Returns risk_level: safe | low_risk | medium_risk | high_risk | critical.

  execute_refactor(model_name, old_name, new_name, dry_run)
      → Applies the rename. ALWAYS call with dry_run=true first, then dry_run=false.
        Returns a checkpoint_id for rollback if needed.

  rollback(checkpoint_id)
      → Reverts the last change using the checkpoint. Call immediately if
        validation fails.

  scan_workspace_health(workspace_ids, dry_run)
      → Detects broken shortcuts, schema drift, orphan assets, stale tables.

  get_session_summary()
      → Returns a human-readable summary of what was done this session.

RULES (follow strictly):
  1. DISCOVER first: Always call discover_workspace before analyze_refactor_impact.
  2. CHECK HISTORY: Always call find_historical_context before executing.
  3. DRY RUN first: Always call execute_refactor with dry_run=true before false.
  4. APPROVAL GATE: If risk_level is "high_risk" or "critical":
       - Output text starting with "APPROVAL_REQUIRED:"
       - Include the full risk summary and historical context
       - Stop — do NOT call execute_refactor. Wait for human approval.
  5. AUTO-ROLLBACK: If any validation indicates failure after live execution,
       immediately call rollback() with the checkpoint_id.
  6. BE TRANSPARENT: Explain your reasoning at each step in plain English.
     Mention confidence level and any concerns about the proposed change.

You are cautious but decisive. When risk is low and history is clean, proceed
with confidence. When risk is high or history shows failures, escalate clearly.
"""


# =============================================================================
# OrchestratorLLMAgent
# =============================================================================

class OrchestratorLLMAgent(BaseAgent):
    """
    AI-powered workflow orchestrator — the brain of the Fabric Agent system.

    WHAT: Uses an LLM (Ollama/Claude/Azure OpenAI) to reason about which tools
          to call, in what order, based on what it finds in the workspace.

    WHY: The specialized sub-agents (Discovery, Impact, Refactor, Healer) are
         intentionally deterministic — they execute safety-critical operations.
         The OrchestratorLLMAgent adds the INTELLIGENCE layer:
         - Adapts to unexpected findings
         - Decides when to pause for human approval
         - Explains its reasoning in natural language
         - Handles ambiguous or partial user requests

    ARCHITECTURE:
      OrchestratorLLMAgent is a BaseAgent — it uses the full ReAct loop:
      1. LLM reasons → decides tool call
      2. Tool executes (delegates to sub-agent)
      3. LLM observes result → decides next tool
      4. Repeat until done or APPROVAL_REQUIRED

    FAANG PARALLEL:
      - GitHub Copilot Workspace: LLM-driven multi-step code editing
      - AWS Bedrock Agents: tool-augmented LLM for AWS API orchestration
      - Google Gemini Agent: multi-step reasoning with API tool calling
      - Microsoft Copilot Studio: orchestrated agent graphs with LLM router

    TOOLS (exposed to LLM):
      discover_workspace, find_historical_context, analyze_refactor_impact,
      execute_refactor, rollback, scan_workspace_health, get_session_summary

    APPROVAL_REQUIRED SIGNAL:
      If the LLM outputs text starting with "APPROVAL_REQUIRED:", the orchestrator
      returns an AgentResult with status=COMPLETED and a special approval_required
      flag in the result dict. The caller (MCP tool, CLI) should surface this to
      the user and wait for explicit approval before resuming.

    USAGE:
      orchestrator = OrchestratorLLMAgent(
          llm=create_llm_client(config),
          fabric_client=client,
          memory=SharedMemory(),
          workspace_id="ws-123",
          workspace_name="Analytics DEV",
      )
      result = await orchestrator.run(
          "Rename 'Total Revenue' to 'Gross Revenue' in Sales Model"
      )
      if result.result.get("approval_required"):
          print("Human approval needed:", result.result["risk_summary"])
    """

    def __init__(
        self,
        llm: LLMClient,
        fabric_client: Any,
        memory: Optional[SharedMemory] = None,
        workspace_id: Optional[str] = None,
        workspace_name: Optional[str] = None,
        operation_memory: Optional[Any] = None,
        max_iterations: int = 20,
    ):
        """
        Args:
            llm:              LLMClient (OllamaLLMClient, ClaudeLLMClient, etc.)
            fabric_client:    FabricApiClient for Fabric REST API calls.
            memory:           Shared memory (created fresh if not provided).
            workspace_id:     Default workspace ID (can be None — LLM discovers it).
            workspace_name:   Default workspace name.
            operation_memory: Optional OperationMemory for historical RAG context.
            max_iterations:   Max LLM → tool call cycles before stopping.
        """
        super().__init__(
            name="OrchestratorLLMAgent",
            role=AgentRole.ORCHESTRATOR,
            llm=llm,
            memory=memory or SharedMemory(),
            max_iterations=max_iterations,
        )
        self._fabric_client = fabric_client
        self._workspace_id = workspace_id
        self._workspace_name = workspace_name
        self._operation_memory = operation_memory

        # Lazily import sub-agents to avoid circular imports at module load
        self._discovery_agent = None
        self._impact_agent = None
        self._refactor_agent = None
        self._healer_agent = None

    def _get_discovery(self):
        if self._discovery_agent is None:
            from fabric_agent.agents.specialized import DiscoveryAgent
            self._discovery_agent = DiscoveryAgent(
                memory=self.memory,
                fabric_client=self._fabric_client,
            )
        return self._discovery_agent

    def _get_impact(self):
        if self._impact_agent is None:
            from fabric_agent.agents.specialized import ImpactAgent
            self._impact_agent = ImpactAgent(
                memory=self.memory,
                operation_memory=self._operation_memory,
            )
        return self._impact_agent

    def _get_refactor(self):
        if self._refactor_agent is None:
            from fabric_agent.agents.specialized import RefactorAgent
            self._refactor_agent = RefactorAgent(
                memory=self.memory,
                workspace_name=self._workspace_name or "",
            )
        return self._refactor_agent

    def get_system_prompt(self) -> str:
        """Return the orchestrator system prompt with current context injected."""
        context_lines = []
        if self._workspace_id:
            context_lines.append(f"Current workspace ID: {self._workspace_id}")
        if self._workspace_name:
            context_lines.append(f"Current workspace name: {self._workspace_name}")
        context_lines.append(f"Session started: {datetime.now(timezone.utc).isoformat()}")

        context_block = "\n".join(context_lines)
        return f"{_SYSTEM_PROMPT}\n\nCURRENT CONTEXT:\n{context_block}"

    def get_tools(self) -> List[ToolDefinition]:
        """Define all tools exposed to the LLM."""
        return [
            ToolDefinition(
                name="discover_workspace",
                description=(
                    "Scan a Fabric workspace to build a dependency graph of all "
                    "semantic models, measures, reports, and their relationships. "
                    "MUST be called before analyze_refactor_impact."
                ),
                parameters={
                    "workspace_id": {
                        "type": "string",
                        "description": "Fabric workspace ID (GUID). Use the default if known.",
                    },
                    "workspace_name": {
                        "type": "string",
                        "description": "Human-readable workspace name for logging.",
                    },
                },
                handler=self._tool_discover_workspace,
            ),
            ToolDefinition(
                name="find_historical_context",
                description=(
                    "Search the vector database for similar past operations and their outcomes. "
                    "Returns historical failure rate and specific recommendations. "
                    "MUST be called before executing any change."
                ),
                parameters={
                    "proposed_change_description": {
                        "type": "string",
                        "description": (
                            "Natural language description of the proposed change. "
                            "Example: 'rename Total Revenue to Gross Revenue in Sales model'"
                        ),
                    },
                },
                handler=self._tool_find_historical_context,
            ),
            ToolDefinition(
                name="analyze_refactor_impact",
                description=(
                    "Analyze the impact of a proposed rename: how many reports/measures "
                    "are affected, risk level, and whether human approval is required."
                ),
                parameters={
                    "workspace_id": {"type": "string", "description": "Fabric workspace ID."},
                    "model_id": {"type": "string", "description": "Semantic model ID."},
                    "measure_name": {"type": "string", "description": "Current measure name."},
                    "new_name": {"type": "string", "description": "Proposed new name."},
                },
                handler=self._tool_analyze_impact,
            ),
            ToolDefinition(
                name="execute_refactor",
                description=(
                    "Execute a measure rename. ALWAYS call with dry_run=true first, "
                    "then dry_run=false after reviewing dry_run results. "
                    "Returns a checkpoint_id for rollback."
                ),
                parameters={
                    "model_name": {"type": "string", "description": "Semantic model name."},
                    "old_name": {"type": "string", "description": "Current measure name."},
                    "new_name": {"type": "string", "description": "New measure name."},
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, simulate only. If false, apply changes.",
                    },
                },
                handler=self._tool_execute_refactor,
            ),
            ToolDefinition(
                name="rollback",
                description=(
                    "Revert the last change using a checkpoint. Call immediately "
                    "if validation fails or the user requests undo."
                ),
                parameters={
                    "checkpoint_id": {
                        "type": "string",
                        "description": "Checkpoint ID returned by execute_refactor.",
                    },
                },
                handler=self._tool_rollback,
            ),
            ToolDefinition(
                name="scan_workspace_health",
                description=(
                    "Scan workspace(s) for broken shortcuts, schema drift, orphan assets, "
                    "and stale tables. Returns a health report with anomalies and "
                    "auto-healable vs. manual-action items."
                ),
                parameters={
                    "workspace_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of workspace IDs to scan.",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, report anomalies only — do not apply fixes.",
                    },
                },
                handler=self._tool_scan_health,
            ),
            ToolDefinition(
                name="get_session_summary",
                description=(
                    "Return a human-readable summary of what was done in this session: "
                    "operations performed, outcomes, rollbacks, and recommendations."
                ),
                parameters={},
                handler=self._tool_get_session_summary,
            ),
        ]

    # =========================================================================
    # Tool handlers — delegate to deterministic sub-agents
    # =========================================================================

    async def _tool_discover_workspace(
        self,
        workspace_id: Optional[str] = None,
        workspace_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Delegate to DiscoveryAgent."""
        ws_id = workspace_id or self._workspace_id
        ws_name = workspace_name or self._workspace_name or ""

        if not ws_id:
            return {"error": "workspace_id is required. Provide it or set workspace_id in config."}

        logger.info(f"[OrchestratorLLM] discover_workspace: {ws_name} ({ws_id})")
        agent = self._get_discovery()

        try:
            result = await agent.discover_workspace(
                workspace_id=ws_id,
                workspace_name=ws_name,
                include_measures=True,
            )
            if result.status == TaskStatus.FAILED:
                return {"error": result.error, "status": "failed"}

            # Update default workspace context for subsequent tool calls
            if not self._workspace_id:
                self._workspace_id = ws_id
            if not self._workspace_name:
                self._workspace_name = ws_name

            return {
                "status": "success",
                "workspace_id": ws_id,
                "workspace_name": ws_name,
                "result": result.result,
            }
        except Exception as exc:
            logger.error(f"discover_workspace failed: {exc}")
            return {"error": str(exc), "status": "failed"}

    async def _tool_find_historical_context(
        self,
        proposed_change_description: str,
    ) -> Dict[str, Any]:
        """Query OperationMemory for similar past operations."""
        if not self._operation_memory:
            return {
                "status": "no_history",
                "message": (
                    "OperationMemory not configured. Proceeding without historical context. "
                    "This is normal on a fresh deployment."
                ),
                "historical_failure_rate": 0.0,
                "recommendations": [],
            }

        logger.info(f"[OrchestratorLLM] find_historical_context: {proposed_change_description[:80]}")

        try:
            ctx = await self._operation_memory.get_risk_context(proposed_change_description)
            return {
                "status": "success",
                "historical_failure_rate": ctx.historical_failure_rate,
                "similar_operations_count": len(ctx.similar_operations),
                "common_failure_reasons": ctx.common_failure_reasons,
                "recommendations": ctx.recommendations,
                "confidence": ctx.confidence,
                "similar_operations": [
                    {
                        "outcome": op.outcome,
                        "similarity": round(op.similarity, 3),
                        "key_lesson": op.key_lesson,
                    }
                    for op in ctx.similar_operations[:3]  # Top 3 for LLM context
                ],
            }
        except Exception as exc:
            logger.warning(f"find_historical_context failed (non-blocking): {exc}")
            return {
                "status": "error",
                "message": str(exc),
                "historical_failure_rate": 0.0,
                "recommendations": [],
            }

    async def _tool_analyze_impact(
        self,
        workspace_id: str,
        model_id: str,
        measure_name: str,
        new_name: str,
    ) -> Dict[str, Any]:
        """Delegate to ImpactAgent."""
        logger.info(f"[OrchestratorLLM] analyze_impact: [{measure_name}] → [{new_name}]")
        agent = self._get_impact()

        try:
            result = await agent.analyze_change(
                change_type="rename_measure",
                workspace_id=workspace_id,
                model_id=model_id,
                measure_name=measure_name,
                new_name=new_name,
            )
            if result.status == TaskStatus.FAILED:
                return {"error": result.error, "status": "failed"}

            return {"status": "success", "impact": result.result}
        except Exception as exc:
            logger.error(f"analyze_impact failed: {exc}")
            return {"error": str(exc), "status": "failed"}

    async def _tool_execute_refactor(
        self,
        model_name: str,
        old_name: str,
        new_name: str,
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        """Delegate to RefactorAgent."""
        mode = "DRY RUN" if dry_run else "LIVE"
        logger.info(f"[OrchestratorLLM] execute_refactor ({mode}): [{old_name}] → [{new_name}]")
        agent = self._get_refactor()

        try:
            result = await agent.execute_refactor(
                refactor_type="rename_measure",
                model_name=model_name,
                old_name=old_name,
                new_name=new_name,
                dry_run=dry_run,
            )
            if result.status == TaskStatus.FAILED:
                return {"error": result.error, "status": "failed"}

            return {
                "status": "success",
                "dry_run": dry_run,
                "result": result.result,
                "checkpoint_id": result.result.get("checkpoint_id") if result.result else None,
            }
        except Exception as exc:
            logger.error(f"execute_refactor failed: {exc}")
            return {"error": str(exc), "status": "failed"}

    async def _tool_rollback(self, checkpoint_id: str) -> Dict[str, Any]:
        """Rollback using checkpoint_id."""
        logger.warning(f"[OrchestratorLLM] rollback: checkpoint={checkpoint_id}")
        agent = self._get_refactor()

        try:
            result = await agent.execute_refactor(
                refactor_type="rollback",
                checkpoint_id=checkpoint_id,
                dry_run=False,
            )
            return {
                "status": "success" if result.status == TaskStatus.COMPLETED else "failed",
                "result": result.result,
                "error": result.error,
            }
        except Exception as exc:
            logger.error(f"rollback failed: {exc}")
            return {"error": str(exc), "status": "failed"}

    async def _tool_scan_health(
        self,
        workspace_ids: Optional[List[str]] = None,
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        """Delegate to SelfHealingMonitor."""
        ws_ids = workspace_ids or ([self._workspace_id] if self._workspace_id else [])
        if not ws_ids:
            return {"error": "workspace_ids required for health scan"}

        logger.info(f"[OrchestratorLLM] scan_health: {ws_ids}")

        try:
            from fabric_agent.healing.monitor import SelfHealingMonitor
            monitor = SelfHealingMonitor(
                fabric_client=self._fabric_client,
                memory_manager=None,
            )
            report = await monitor.run_once(workspace_ids=ws_ids, dry_run=dry_run)
            return {
                "status": "success",
                "total_assets": report.total_assets,
                "anomalies_found": report.anomalies_found,
                "auto_healed": report.auto_healed,
                "manual_required": report.manual_required,
                "dry_run": dry_run,
            }
        except Exception as exc:
            logger.error(f"scan_health failed: {exc}")
            return {"error": str(exc), "status": "failed"}

    async def _tool_get_session_summary(self) -> Dict[str, Any]:
        """Return a summary of this session's activity."""
        tool_calls = self.memory.get("session_tool_calls", [])
        return {
            "status": "success",
            "session_id": self.memory.get("session_id", str(uuid4())),
            "tool_calls_made": len(tool_calls),
            "tool_sequence": [tc.get("name") for tc in tool_calls],
            "workspace_id": self._workspace_id,
            "workspace_name": self._workspace_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # =========================================================================
    # Post-processing: detect APPROVAL_REQUIRED signal
    # =========================================================================

    def _extract_result(self, text_content: str, context: Optional[Dict]) -> Any:
        """
        Check if the LLM output contains the APPROVAL_REQUIRED signal.

        If the LLM output starts with "APPROVAL_REQUIRED:", extract the risk
        summary and return a structured dict with approval_required=True.
        The caller (MCP tool, CLI, tests) can check this flag and pause.
        """
        if text_content and text_content.strip().startswith("APPROVAL_REQUIRED:"):
            risk_summary = text_content.strip()[len("APPROVAL_REQUIRED:"):].strip()
            logger.warning(f"[OrchestratorLLM] APPROVAL_REQUIRED: {risk_summary[:200]}")
            return {
                "approval_required": True,
                "risk_summary": risk_summary,
                "status": "awaiting_approval",
            }

        return {
            "approval_required": False,
            "summary": text_content.strip() if text_content else "Completed.",
            "status": "completed",
        }


# =============================================================================
# Factory
# =============================================================================

def create_llm_orchestrator(
    config: Any,
    fabric_client: Any,
    memory: Optional[SharedMemory] = None,
    workspace_id: Optional[str] = None,
    workspace_name: Optional[str] = None,
    operation_memory: Optional[Any] = None,
) -> OrchestratorLLMAgent:
    """
    Factory: build an OrchestratorLLMAgent from config.

    Reads llm_provider from config to select the right LLM client,
    then wires everything into OrchestratorLLMAgent.

    Args:
        config:           AgentConfig with llm_provider + provider-specific fields.
        fabric_client:    Initialized FabricApiClient.
        memory:           Shared memory (created fresh if None).
        workspace_id:     Default workspace ID.
        workspace_name:   Default workspace name.
        operation_memory: Optional OperationMemory for RAG context.

    Returns:
        OrchestratorLLMAgent ready to call .run(task).

    Example:
        config = AgentConfig()
        orchestrator = create_llm_orchestrator(config, fabric_client,
                                               workspace_name="Analytics DEV")
        result = await orchestrator.run("Rename Total Revenue to Gross Revenue")
    """
    from fabric_agent.api.llm_providers import create_llm_client
    llm = create_llm_client(config)

    return OrchestratorLLMAgent(
        llm=llm,
        fabric_client=fabric_client,
        memory=memory,
        workspace_id=workspace_id,
        workspace_name=workspace_name,
        operation_memory=operation_memory,
        max_iterations=getattr(config, "max_agent_iterations", 20),
    )
