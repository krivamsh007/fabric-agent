"""
Fabric Agent - Multi-Agent System
==================================

A coordinated multi-agent system for safe refactoring operations:

Agents:
- DiscoveryAgent: Scans workspaces, builds dependency graphs
- ImpactAgent: Analyzes changes, scores risk
- RefactorAgent: Executes changes with checkpoints
- ValidationAgent: Tests DAX queries before/after

Orchestration:
- AgentOrchestrator: Coordinates workflows
- DeploymentPipelineManager: Fabric pipeline integration
- SimpleApprovalManager: In-memory approval for testing

Author: Fabric Agent Team
"""

# Base classes
from fabric_agent.agents.base import (
    BaseAgent,
    SimpleAgent,
    AgentRole,
    AgentResult,
    AgentMessage,
    AgentThought,
    MessageType,
    TaskStatus,
    ToolDefinition,
    ToolCall,
    SharedMemory,
    LLMClient,
    ClaudeLLMClient,
    MockLLMClient,
)

# Validation Agent
from fabric_agent.agents.validation import (
    ValidationAgent,
    ValidationTest,
    ValidationReport,
    QueryResult,
    TestComparison,
    validate_refactor,
)

# Specialized Agents
from fabric_agent.agents.specialized import (
    DiscoveryAgent,
    ImpactAgent,
    RefactorAgent,
)

# Orchestration
from fabric_agent.agents.orchestrator import (
    AgentOrchestrator,
    WorkflowStatus,
    WorkflowResult,
    ApprovalRequest,
    ApprovalStatus,
    DeploymentPipelineManager,
    SimpleApprovalManager,
)

__all__ = [
    # Base
    "BaseAgent",
    "SimpleAgent",
    "AgentRole",
    "AgentResult",
    "AgentMessage",
    "AgentThought",
    "MessageType",
    "TaskStatus",
    "ToolDefinition",
    "ToolCall",
    "SharedMemory",
    "LLMClient",
    "ClaudeLLMClient",
    "MockLLMClient",
    # Validation
    "ValidationAgent",
    "ValidationTest",
    "ValidationReport",
    "QueryResult",
    "TestComparison",
    "validate_refactor",
    # Specialized
    "DiscoveryAgent",
    "ImpactAgent",
    "RefactorAgent",
    # Orchestration
    "AgentOrchestrator",
    "WorkflowStatus",
    "WorkflowResult",
    "ApprovalRequest",
    "ApprovalStatus",
    "DeploymentPipelineManager",
    "SimpleApprovalManager",
]
