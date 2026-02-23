"""
FabricAgent - Main Agent Class
==============================

The FabricAgent is the primary interface for AI agents to interact with
Microsoft Fabric. It provides:

- Automatic initialization and connection management
- Full audit trail with rollback capabilities
- Pydantic-validated async tools
- Comprehensive logging via loguru

Example:
    >>> from fabric_agent import FabricAgent
    >>> 
    >>> async with FabricAgent() as agent:
    ...     workspaces = await agent.list_workspaces()
    ...     await agent.set_workspace("Sales Analytics")
    ...     impact = await agent.analyze_impact("measure", "Sales")
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from loguru import logger

from fabric_agent.core.config import (
    AgentConfig,
    FabricAuthConfig,
    StorageBackend,
    AuthMode,
)
from fabric_agent.api.fabric_client import FabricApiClient
from fabric_agent.storage.memory_manager import (
    MemoryManager,
    StateSnapshot,
    StateType,
    OperationStatus,
)
from fabric_agent.tools.fabric_tools import FabricTools
from fabric_agent.tools.models import (
    ListWorkspacesInput,
    ListWorkspacesOutput,
    SetWorkspaceInput,
    SetWorkspaceOutput,
    ListItemsInput,
    ListItemsOutput,
    GetSemanticModelInput,
    GetSemanticModelOutput,
    GetMeasuresInput,
    GetMeasuresOutput,
    GetReportDefinitionInput,
    GetReportDefinitionOutput,
    AnalyzeImpactInput,
    AnalyzeImpactOutput,
    RenameMeasureInput,
    RenameMeasureOutput,
    SmartRenameMeasureInput,
    SmartRenameMeasureOutput,
    GetRefactorHistoryInput,
    GetRefactorHistoryOutput,
    RollbackInput,
    RollbackOutput,
    GetConnectionStatusInput,
    GetConnectionStatusOutput,
    TargetType,
)


def _configure_logging(
    level: str = "INFO",
    log_file: Optional[Path] = None,
) -> None:
    """
    Configure loguru logging.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_file: Optional file path for log output.
    """
    # Remove default handler
    logger.remove()
    
    # Add stderr handler with formatting
    logger.add(
        sys.stderr,
        level=level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        ),
        colorize=True,
    )
    
    # Add file handler if specified
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        logger.add(
            str(log_file),
            level=level,
            format=(
                "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
                "{name}:{function}:{line} | {message}"
            ),
            rotation="10 MB",
            retention="1 week",
            compression="zip",
        )


class FabricAgent:
    """
    Main agent class for Microsoft Fabric operations.
    
    The FabricAgent provides a high-level interface for AI agents to:
    - Connect to Microsoft Fabric
    - Browse workspaces and items
    - Analyze semantic models and reports
    - Perform safe refactoring with impact analysis
    - Maintain an audit trail with rollback capabilities
    
    All operations are async and use Pydantic for input validation.
    
    Attributes:
        config: Agent configuration.
        client: Fabric API client.
        memory: Memory manager for audit trail.
        tools: Collection of MCP tools.
    
    Example:
        >>> # Using async context manager (recommended)
        >>> async with FabricAgent() as agent:
        ...     workspaces = await agent.list_workspaces()
        ...     print(f"Found {workspaces.count} workspaces")
        >>> 
        >>> # Manual initialization
        >>> agent = FabricAgent()
        >>> await agent.initialize()
        >>> try:
        ...     workspaces = await agent.list_workspaces()
        ... finally:
        ...     await agent.close()
    """
    
    def __init__(
        self,
        config: Optional[AgentConfig] = None,
        auth_config: Optional[FabricAuthConfig] = None,
        storage_path: Optional[Path] = None,
        storage_backend: str = "sqlite",
        log_level: str = "INFO",
        log_file: Optional[Path] = None,
    ):
        """
        Initialize the FabricAgent.
        
        Args:
            config: Full agent configuration. If not provided, will be
                   created from other parameters.
            auth_config: Authentication configuration. If not provided,
                        will be loaded from environment variables.
            storage_path: Path for audit trail storage.
            storage_backend: "sqlite" or "json".
            log_level: Logging level.
            log_file: Optional file for logs.
        
        Example:
            >>> # Default (loads auth from environment)
            >>> agent = FabricAgent()
            >>> 
            >>> # With explicit configuration
            >>> agent = FabricAgent(
            ...     auth_config=FabricAuthConfig(
            ...         tenant_id="...",
            ...         client_id="...",
            ...         auth_mode=AuthMode.INTERACTIVE
            ...     ),
            ...     storage_path=Path("./data/audit.db"),
            ...     log_level="DEBUG"
            ... )
        """
        # Configure logging first
        _configure_logging(level=log_level, log_file=log_file)
        logger.info("Initializing FabricAgent")
        
        # Build config
        if config:
            self.config = config
        else:
            self.config = AgentConfig(
                auth=auth_config,
                storage_backend=StorageBackend(storage_backend),
                storage_path=storage_path or Path("./data/fabric_agent.db"),
                log_level=log_level,
            )
        
        # Load auth from environment if not provided
        self.config.load_auth_from_env()
        
        # Initialize components (will be created in initialize())
        self.client: Optional[FabricApiClient] = None
        self.memory: Optional[MemoryManager] = None
        self.tools: Optional[FabricTools] = None
        
        self._initialized = False
        
        logger.debug(f"Config: storage={self.config.storage_backend}, path={self.config.storage_path}")
    
    async def initialize(self) -> "FabricAgent":
        """
        Initialize the agent and all its components.
        
        This must be called before using any agent methods.
        Alternatively, use the async context manager.
        
        Returns:
            Self for method chaining.
        
        Raises:
            ValueError: If authentication configuration is missing.
        
        Example:
            >>> agent = FabricAgent()
            >>> await agent.initialize()
            >>> # Now ready to use
        """
        if self._initialized:
            logger.debug("Agent already initialized")
            return self
        
        logger.info("Starting agent initialization")
        
        if not self.config.auth:
            raise ValueError(
                "Authentication configuration required. "
                "Set AZURE_TENANT_ID and AZURE_CLIENT_ID environment variables, "
                "or provide auth_config parameter."
            )
        
        # Initialize Fabric API client
        self.client = FabricApiClient(
            auth_config=self.config.auth,
            base_url=self.config.fabric_base_url,
            api_version=self.config.api_version,
            timeout_seconds=self.config.timeout_seconds,
        )
        await self.client.initialize()
        logger.info("Fabric API client initialized")
        
        # Initialize Memory Manager
        self.memory = MemoryManager(
            storage_backend=self.config.storage_backend.value,
            storage_path=self.config.storage_path,
            max_states=self.config.max_rollback_states,
        )
        await self.memory.initialize()
        logger.info("Memory manager initialized")
        
        # Initialize tools
        self.tools = FabricTools(
            client=self.client,
            memory_manager=self.memory,
        )
        logger.info("Tools initialized")
        
        # Record initialization in audit trail
        if self.config.enable_audit_trail:
            await self.memory.record_state(
                state_type=StateType.CONNECTION,
                operation="agent_initialized",
                status=OperationStatus.SUCCESS,
                metadata={
                    "auth_mode": self.config.auth.auth_mode.value,
                    "storage_backend": self.config.storage_backend.value,
                },
            )
        
        self._initialized = True
        logger.info("FabricAgent initialization complete")
        
        return self
    
    async def close(self) -> None:
        """
        Close the agent and release resources.
        
        Always call this when done, or use the async context manager.
        """
        logger.info("Closing FabricAgent")
        
        if self.client:
            await self.client.close()
            self.client = None
        
        self.tools = None
        self._initialized = False
        
        logger.info("FabricAgent closed")
    
    async def __aenter__(self) -> "FabricAgent":
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()
    
    def _ensure_initialized(self) -> None:
        """Ensure the agent is initialized."""
        if not self._initialized or not self.tools:
            raise RuntimeError(
                "FabricAgent not initialized. "
                "Call initialize() or use async context manager."
            )
    
    # ========================================================================
    # Workspace Methods
    # ========================================================================
    
    async def list_workspaces(self) -> ListWorkspacesOutput:
        """
        List all accessible Fabric workspaces.
        
        Returns:
            ListWorkspacesOutput with workspaces and count.
        
        Example:
            >>> result = await agent.list_workspaces()
            >>> for ws in result.workspaces:
            ...     print(f"{ws.name} ({ws.id})")
        """
        self._ensure_initialized()
        return await self.tools.list_workspaces(ListWorkspacesInput())
    
    async def set_workspace(self, workspace_name: str) -> SetWorkspaceOutput:
        """
        Set the active workspace for subsequent operations.
        
        Args:
            workspace_name: Name of the workspace.
        
        Returns:
            SetWorkspaceOutput with workspace_id and workspace_name.
        
        Example:
            >>> await agent.set_workspace("Sales Analytics")
        """
        self._ensure_initialized()
        return await self.tools.set_workspace(
            SetWorkspaceInput(workspace_name=workspace_name)
        )
    
    async def list_items(
        self,
        item_type: Optional[str] = None,
    ) -> ListItemsOutput:
        """
        List items in the current workspace.
        
        Args:
            item_type: Optional filter (Report, SemanticModel, etc.)
        
        Returns:
            ListItemsOutput with items and count.
        
        Example:
            >>> reports = await agent.list_items("Report")
        """
        self._ensure_initialized()
        return await self.tools.list_items(ListItemsInput(item_type=item_type))
    
    # ========================================================================
    # Semantic Model Methods
    # ========================================================================
    
    async def get_semantic_model(
        self,
        model_name: str,
    ) -> GetSemanticModelOutput:
        """
        Get details of a semantic model.
        
        Args:
            model_name: Name of the semantic model.
        
        Returns:
            GetSemanticModelOutput with model details.
        
        Example:
            >>> model = await agent.get_semantic_model("Sales Analytics")
            >>> print(f"Measures: {model.measure_count}")
        """
        self._ensure_initialized()
        return await self.tools.get_semantic_model(
            GetSemanticModelInput(model_name=model_name)
        )
    
    async def get_measures(self, model_name: str) -> GetMeasuresOutput:
        """
        Get all measures from a semantic model.
        
        Args:
            model_name: Name of the semantic model.
        
        Returns:
            GetMeasuresOutput with measures.
        
        Example:
            >>> measures = await agent.get_measures("Sales Analytics")
            >>> for m in measures.measures:
            ...     print(m.name)
        """
        self._ensure_initialized()
        return await self.tools.get_measures(
            GetMeasuresInput(model_name=model_name)
        )
    
    # ========================================================================
    # Report Methods
    # ========================================================================
    
    async def get_report_definition(
        self,
        report_name: str,
    ) -> GetReportDefinitionOutput:
        """
        Get the definition of a report.
        
        Args:
            report_name: Name of the report.
        
        Returns:
            GetReportDefinitionOutput with report structure.
        
        Example:
            >>> report = await agent.get_report_definition("Sales Dashboard")
            >>> print(f"Pages: {report.pages}")
        """
        self._ensure_initialized()
        return await self.tools.get_report_definition(
            GetReportDefinitionInput(report_name=report_name)
        )
    
    # ========================================================================
    # Impact Analysis Methods
    # ========================================================================
    
    async def analyze_impact(
        self,
        target_type: str,
        target_name: str,
        model_name: Optional[str] = None,
    ) -> AnalyzeImpactOutput:
        """
        Analyze the impact of renaming a measure or column.
        
        ALWAYS call this before any rename operation!
        
        Args:
            target_type: "measure" or "column".
            target_name: Name of the target.
            model_name: Semantic model name (for DAX analysis).
        
        Returns:
            AnalyzeImpactOutput with affected items.
        
        Example:
            >>> impact = await agent.analyze_impact("measure", "Sales", "Analytics")
            >>> if not impact.safe_to_rename:
            ...     print(f"Would affect {impact.total_impact} items!")
        """
        self._ensure_initialized()
        return await self.tools.analyze_impact(
            AnalyzeImpactInput(
                target_type=TargetType(target_type),
                target_name=target_name,
                model_name=model_name,
            )
        )
    
    # ========================================================================
    # Refactoring Methods
    # ========================================================================
    
    async def rename_measure(
        self,
        model_name: str,
        old_name: str,
        new_name: str,
        dry_run: bool = True,
    ) -> RenameMeasureOutput:
        """
        Rename a measure across semantic model and reports.
        
        Use dry_run=True first to preview changes!
        
        Args:
            model_name: Semantic model containing the measure.
            old_name: Current measure name.
            new_name: New measure name.
            dry_run: If True, only simulate (default True).
        
        Returns:
            RenameMeasureOutput with operation results.
        
        Example:
            >>> # Preview first
            >>> result = await agent.rename_measure(
            ...     "Analytics", "Sales", "Net_Revenue", dry_run=True
            ... )
            >>> print(f"Would affect {result.impact.total_impact} items")
            >>> 
            >>> # Then execute
            >>> result = await agent.rename_measure(
            ...     "Analytics", "Sales", "Net_Revenue", dry_run=False
            ... )
        """
        self._ensure_initialized()
        return await self.tools.rename_measure(
            RenameMeasureInput(
                model_name=model_name,
                old_name=old_name,
                new_name=new_name,
                dry_run=dry_run,
            )
        )
    

    async def smart_rename_measure(
        self,
        model_name: str,
        old_name: str,
        new_name: str,
        dry_run: bool = True,
        reasoning: Optional[str] = None,
    ) -> SmartRenameMeasureOutput:
        """
        Hero refactor: rename a measure and update dependent measures safely.

        This is the recommended API for measure renames because it:
        - Uses SemPy dependency mapping where available
        - Updates referencing measures' DAX so nothing breaks
        - Persists an append-only log to memory/refactor_log.json
        - Produces a Markdown safety UI

        Args:
            model_name: Semantic model containing the measure.
            old_name: Current measure name.
            new_name: New measure name.
            dry_run: If True, only preview (default True).
            reasoning: Reason for this change (stored in log).

        Returns:
            SmartRenameMeasureOutput with operation_id for rollback.
        """
        self._ensure_initialized()

        return await self.tools.smart_rename_measure(
            SmartRenameMeasureInput(
                model_name=model_name,
                old_name=old_name,
                new_name=new_name,
                dry_run=dry_run,
                reasoning=reasoning,
            )
        )

    # ========================================================================
    # History & Rollback Methods
    # ========================================================================
    
    async def get_history(
        self,
        limit: int = 10,
        operation_type: Optional[str] = None,
    ) -> GetRefactorHistoryOutput:
        """
        Get history of operations from the audit trail.
        
        Args:
            limit: Maximum events to return.
            operation_type: Filter by operation type.
        
        Returns:
            GetRefactorHistoryOutput with events.
        
        Example:
            >>> history = await agent.get_history(limit=5)
            >>> for event in history.events:
            ...     print(f"{event.timestamp}: {event.operation}")
        """
        self._ensure_initialized()
        return await self.tools.get_refactor_history(
            GetRefactorHistoryInput(
                limit=limit,
                operation_type=operation_type,
            )
        )
    
    async def rollback(
        self,
        snapshot_id: str,
        dry_run: bool = True,
    ) -> RollbackOutput:
        """
        Rollback to a previous state.
        
        Args:
            snapshot_id: ID of the snapshot to rollback to.
            dry_run: If True, only preview (default True).
        
        Returns:
            RollbackOutput with rollback results.
        
        Example:
            >>> # Get history to find snapshot ID
            >>> history = await agent.get_history()
            >>> snapshot_id = history.events[0].id
            >>> 
            >>> # Preview rollback
            >>> result = await agent.rollback(snapshot_id, dry_run=True)
        """
        self._ensure_initialized()
        return await self.tools.rollback(
            RollbackInput(
                snapshot_id=snapshot_id,
                dry_run=dry_run,
            )
        )
    
    # ========================================================================
    # Status Methods
    # ========================================================================
    
    async def get_status(self) -> GetConnectionStatusOutput:
        """
        Get current connection status.
        
        Returns:
            GetConnectionStatusOutput with connection details.
        
        Example:
            >>> status = await agent.get_status()
            >>> if status.connected:
            ...     print(f"Connected to {status.workspace_name}")
        """
        self._ensure_initialized()
        return await self.tools.get_connection_status(GetConnectionStatusInput())
    
    async def get_audit_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the audit trail.
        
        Returns:
            Dictionary with snapshot counts by type and status.
        
        Example:
            >>> stats = await agent.get_audit_statistics()
            >>> print(f"Total snapshots: {stats['total_snapshots']}")
        """
        self._ensure_initialized()
        return await self.memory.get_statistics()
    
    async def verify_audit_integrity(self) -> Dict[str, Any]:
        """
        Verify integrity of all audit trail entries.
        
        Returns:
            Dictionary with valid/corrupted counts.
        
        Example:
            >>> result = await agent.verify_audit_integrity()
            >>> if result["corrupted_count"] > 0:
            ...     print("Warning: Some snapshots corrupted!")
        """
        self._ensure_initialized()
        return await self.memory.verify_all_integrity()
