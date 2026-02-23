"""
Fabric Agent - Production-grade Microsoft Fabric MCP Agent
===========================================================

A comprehensive AI agent framework for Microsoft Fabric operations with:
- Full MCP (Model Context Protocol) support
- Pydantic-validated async tools
- SQLite/JSON-backed audit trail and state management
- Rollback capabilities for safe refactoring

Example Usage:
    >>> from fabric_agent import FabricAgent
    >>> agent = FabricAgent()
    >>> await agent.initialize()
    >>> workspaces = await agent.list_workspaces()
"""

__version__ = "1.0.0"
__author__ = "Fabric Agent Team"

from fabric_agent.core.agent import FabricAgent
from fabric_agent.core.config import AgentConfig, FabricAuthConfig
from fabric_agent.storage.memory_manager import MemoryManager, StateSnapshot
from fabric_agent.api.fabric_client import FabricApiClient

__all__ = [
    # Core
    "FabricAgent",
    "AgentConfig",
    "FabricAuthConfig",
    # Storage
    "MemoryManager",
    "StateSnapshot",
    # API
    "FabricApiClient",
    # Version
    "__version__",
]
