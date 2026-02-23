"""
Memory Manager - Audit Trail and Rollback System
=================================================

The MemoryManager provides persistent state tracking for all agent operations.
It serves as the source of truth for:
- Audit Trail: Every state change is logged with full context
- Rollback: Previous states can be restored on demand
- Analytics: Query historical operations and patterns

Supports both SQLite (recommended) and JSON backends.
"""

from __future__ import annotations

import json
import sqlite3
import hashlib
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Type, TypeVar
from uuid import uuid4

from pydantic import BaseModel, Field, field_serializer
from loguru import logger


class StateType(str, Enum):
    """Types of state snapshots that can be tracked."""
    
    WORKSPACE = "workspace"
    """Workspace selection/configuration state."""
    
    SEMANTIC_MODEL = "semantic_model"
    """Semantic model definition state."""
    
    REPORT = "report"
    """Report definition state."""
    
    MEASURE = "measure"
    """Individual measure state."""
    
    REFACTOR = "refactor"
    """Refactoring operation state."""
    
    CONNECTION = "connection"
    """Connection/authentication state."""
    
    CUSTOM = "custom"
    """Custom state type for extensions."""


class OperationStatus(str, Enum):
    """Status of an operation."""
    
    PENDING = "pending"
    """Operation is queued but not started."""
    
    IN_PROGRESS = "in_progress"
    """Operation is currently executing."""
    
    SUCCESS = "success"
    """Operation completed successfully."""
    
    FAILED = "failed"
    """Operation failed with an error."""
    
    ROLLED_BACK = "rolled_back"
    """Operation was rolled back."""
    
    DRY_RUN = "dry_run"
    """Operation was a simulation only."""


class StateSnapshot(BaseModel):
    """
    Immutable snapshot of agent state at a point in time.
    
    This is the core data structure for audit trail and rollback.
    Each snapshot captures the complete state needed to restore
    or understand what happened at that moment.
    
    Attributes:
        id: Unique identifier for this snapshot.
        timestamp: When this snapshot was created (UTC).
        state_type: Category of state being captured.
        operation: Name of the operation that created this state.
        status: Current status of the operation.
        workspace_id: Associated workspace ID (if applicable).
        workspace_name: Associated workspace name (if applicable).
        target_id: ID of the primary affected item.
        target_name: Name of the primary affected item.
        old_value: Previous value (for change tracking).
        new_value: New value (for change tracking).
        state_data: Full state data as JSON-serializable dict.
        metadata: Additional context and metadata.
        checksum: SHA-256 hash of state_data for integrity verification.
        parent_id: ID of the parent snapshot (for operation chains).
        user: User who initiated the operation (if known).
        error_message: Error details if operation failed.
    
    Example:
        >>> snapshot = StateSnapshot(
        ...     state_type=StateType.MEASURE,
        ...     operation="rename_measure",
        ...     status=OperationStatus.SUCCESS,
        ...     target_name="Sales",
        ...     old_value="Sales",
        ...     new_value="Net_Revenue",
        ...     state_data={"model": "SalesAnalytics", "expression": "..."}
        ... )
    """
    
    id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    state_type: StateType
    operation: str
    status: OperationStatus = OperationStatus.PENDING
    workspace_id: Optional[str] = None
    workspace_name: Optional[str] = None
    target_id: Optional[str] = None
    target_name: Optional[str] = None
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    state_data: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    checksum: Optional[str] = None
    parent_id: Optional[str] = None
    user: Optional[str] = None
    error_message: Optional[str] = None
    
    def model_post_init(self, __context: Any) -> None:
        """Compute checksum after initialization."""
        if self.checksum is None and self.state_data:
            self.checksum = self._compute_checksum()
    
    def _compute_checksum(self) -> str:
        """Compute SHA-256 checksum of state_data."""
        data_str = json.dumps(self.state_data, sort_keys=True, default=str)
        return hashlib.sha256(data_str.encode()).hexdigest()
    
    def verify_integrity(self) -> bool:
        """
        Verify the state_data hasn't been tampered with.
        
        Returns:
            True if checksum matches, False otherwise.
        """
        if not self.checksum:
            return True
        return self._compute_checksum() == self.checksum
    
    @field_serializer("timestamp")
    def serialize_timestamp(self, v: datetime) -> str:
        """Serialize timestamp to ISO format."""
        return v.isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return self.model_dump(mode="json")
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StateSnapshot":
        """Create from dictionary."""
        # Handle timestamp conversion
        if isinstance(data.get("timestamp"), str):
            data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        return cls(**data)


class StorageBackendBase(ABC):
    """Abstract base class for storage backends."""
    
    @abstractmethod
    async def initialize(self) -> None:
        """Initialize the storage backend."""
        pass
    
    @abstractmethod
    async def save_snapshot(self, snapshot: StateSnapshot) -> None:
        """Save a state snapshot."""
        pass
    
    @abstractmethod
    async def get_snapshot(self, snapshot_id: str) -> Optional[StateSnapshot]:
        """Retrieve a snapshot by ID."""
        pass
    
    @abstractmethod
    async def get_snapshots(
        self,
        state_type: Optional[StateType] = None,
        operation: Optional[str] = None,
        status: Optional[OperationStatus] = None,
        workspace_id: Optional[str] = None,
        target_name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order_desc: bool = True,
    ) -> List[StateSnapshot]:
        """Query snapshots with filters."""
        pass
    
    @abstractmethod
    async def get_latest_snapshot(
        self,
        state_type: StateType,
        target_id: Optional[str] = None,
        target_name: Optional[str] = None,
    ) -> Optional[StateSnapshot]:
        """Get the most recent snapshot matching criteria."""
        pass
    
    @abstractmethod
    async def delete_snapshot(self, snapshot_id: str) -> bool:
        """Delete a snapshot by ID."""
        pass
    
    @abstractmethod
    async def count_snapshots(
        self,
        state_type: Optional[StateType] = None,
        operation: Optional[str] = None,
    ) -> int:
        """Count snapshots matching criteria."""
        pass
    
    @abstractmethod
    async def prune_old_snapshots(self, keep_count: int) -> int:
        """Delete old snapshots, keeping only the most recent N."""
        pass


class SQLiteStorageBackend(StorageBackendBase):
    """
    SQLite-based storage backend for production use.
    
    Features:
    - ACID transactions
    - Efficient queries with indexes
    - Full-text search on state_data
    - Automatic schema migrations
    """
    
    SCHEMA_VERSION = 1
    
    def __init__(self, db_path: Path):
        """
        Initialize SQLite storage.
        
        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = db_path
        self._connection: Optional[sqlite3.Connection] = None
    
    @contextmanager
    def _get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Get a database connection with proper settings."""
        conn = sqlite3.connect(
            str(self.db_path),
            detect_types=sqlite3.PARSE_DECLTYPES,
            isolation_level="DEFERRED",
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    async def initialize(self) -> None:
        """Create database schema if not exists."""
        logger.info(f"Initializing SQLite storage at {self.db_path}")
        
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        with self._get_connection() as conn:
            # Create main snapshots table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS state_snapshots (
                    id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    state_type TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    status TEXT NOT NULL,
                    workspace_id TEXT,
                    workspace_name TEXT,
                    target_id TEXT,
                    target_name TEXT,
                    old_value TEXT,
                    new_value TEXT,
                    state_data TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    checksum TEXT,
                    parent_id TEXT,
                    user TEXT,
                    error_message TEXT,
                    FOREIGN KEY (parent_id) REFERENCES state_snapshots(id)
                )
            """)
            
            # Create indexes for common queries
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp 
                ON state_snapshots(timestamp DESC)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_state_type 
                ON state_snapshots(state_type)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_operation 
                ON state_snapshots(operation)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_workspace 
                ON state_snapshots(workspace_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_target 
                ON state_snapshots(target_name)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_status 
                ON state_snapshots(status)
            """)
            
            # Create schema version table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY
                )
            """)
            
            # Set schema version
            conn.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)",
                (self.SCHEMA_VERSION,)
            )
        
        logger.info("SQLite storage initialized successfully")
    
    async def save_snapshot(self, snapshot: StateSnapshot) -> None:
        """Save a state snapshot to the database."""
        logger.debug(f"Saving snapshot {snapshot.id} ({snapshot.operation})")
        
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO state_snapshots (
                    id, timestamp, state_type, operation, status,
                    workspace_id, workspace_name, target_id, target_name,
                    old_value, new_value, state_data, metadata, checksum,
                    parent_id, user, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                snapshot.id,
                snapshot.timestamp.isoformat(),
                snapshot.state_type.value,
                snapshot.operation,
                snapshot.status.value,
                snapshot.workspace_id,
                snapshot.workspace_name,
                snapshot.target_id,
                snapshot.target_name,
                snapshot.old_value,
                snapshot.new_value,
                json.dumps(snapshot.state_data),
                json.dumps(snapshot.metadata),
                snapshot.checksum,
                snapshot.parent_id,
                snapshot.user,
                snapshot.error_message,
            ))
        
        logger.debug(f"Snapshot {snapshot.id} saved successfully")
    
    def _row_to_snapshot(self, row: sqlite3.Row) -> StateSnapshot:
        """Convert a database row to a StateSnapshot."""
        return StateSnapshot(
            id=row["id"],
            timestamp=datetime.fromisoformat(row["timestamp"]),
            state_type=StateType(row["state_type"]),
            operation=row["operation"],
            status=OperationStatus(row["status"]),
            workspace_id=row["workspace_id"],
            workspace_name=row["workspace_name"],
            target_id=row["target_id"],
            target_name=row["target_name"],
            old_value=row["old_value"],
            new_value=row["new_value"],
            state_data=json.loads(row["state_data"]),
            metadata=json.loads(row["metadata"]),
            checksum=row["checksum"],
            parent_id=row["parent_id"],
            user=row["user"],
            error_message=row["error_message"],
        )
    
    async def get_snapshot(self, snapshot_id: str) -> Optional[StateSnapshot]:
        """Retrieve a snapshot by ID."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM state_snapshots WHERE id = ?",
                (snapshot_id,)
            )
            row = cursor.fetchone()
            if row:
                return self._row_to_snapshot(row)
        return None
    
    async def get_snapshots(
        self,
        state_type: Optional[StateType] = None,
        operation: Optional[str] = None,
        status: Optional[OperationStatus] = None,
        workspace_id: Optional[str] = None,
        target_name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order_desc: bool = True,
    ) -> List[StateSnapshot]:
        """Query snapshots with filters."""
        conditions = []
        params: List[Any] = []
        
        if state_type:
            conditions.append("state_type = ?")
            params.append(state_type.value)
        if operation:
            conditions.append("operation = ?")
            params.append(operation)
        if status:
            conditions.append("status = ?")
            params.append(status.value)
        if workspace_id:
            conditions.append("workspace_id = ?")
            params.append(workspace_id)
        if target_name:
            conditions.append("target_name = ?")
            params.append(target_name)
        
        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
        
        order = "DESC" if order_desc else "ASC"
        
        query = f"""
            SELECT * FROM state_snapshots
            {where_clause}
            ORDER BY timestamp {order}
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        
        with self._get_connection() as conn:
            cursor = conn.execute(query, params)
            return [self._row_to_snapshot(row) for row in cursor.fetchall()]
    
    async def get_latest_snapshot(
        self,
        state_type: StateType,
        target_id: Optional[str] = None,
        target_name: Optional[str] = None,
    ) -> Optional[StateSnapshot]:
        """Get the most recent snapshot matching criteria."""
        conditions = ["state_type = ?"]
        params: List[Any] = [state_type.value]
        
        if target_id:
            conditions.append("target_id = ?")
            params.append(target_id)
        if target_name:
            conditions.append("target_name = ?")
            params.append(target_name)
        
        query = f"""
            SELECT * FROM state_snapshots
            WHERE {" AND ".join(conditions)}
            ORDER BY timestamp DESC
            LIMIT 1
        """
        
        with self._get_connection() as conn:
            cursor = conn.execute(query, params)
            row = cursor.fetchone()
            if row:
                return self._row_to_snapshot(row)
        return None
    
    async def delete_snapshot(self, snapshot_id: str) -> bool:
        """Delete a snapshot by ID."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "DELETE FROM state_snapshots WHERE id = ?",
                (snapshot_id,)
            )
            return cursor.rowcount > 0
    
    async def count_snapshots(
        self,
        state_type: Optional[StateType] = None,
        operation: Optional[str] = None,
    ) -> int:
        """Count snapshots matching criteria."""
        conditions = []
        params: List[Any] = []
        
        if state_type:
            conditions.append("state_type = ?")
            params.append(state_type.value)
        if operation:
            conditions.append("operation = ?")
            params.append(operation)
        
        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
        
        query = f"SELECT COUNT(*) FROM state_snapshots {where_clause}"
        
        with self._get_connection() as conn:
            cursor = conn.execute(query, params)
            return cursor.fetchone()[0]
    
    async def prune_old_snapshots(self, keep_count: int) -> int:
        """Delete old snapshots, keeping only the most recent N."""
        with self._get_connection() as conn:
            # Get the timestamp of the Nth most recent snapshot
            cursor = conn.execute("""
                SELECT timestamp FROM state_snapshots
                ORDER BY timestamp DESC
                LIMIT 1 OFFSET ?
            """, (keep_count - 1,))
            
            row = cursor.fetchone()
            if not row:
                return 0
            
            cutoff_timestamp = row[0]
            
            # Delete snapshots older than the cutoff
            cursor = conn.execute("""
                DELETE FROM state_snapshots
                WHERE timestamp < ?
            """, (cutoff_timestamp,))
            
            deleted = cursor.rowcount
            logger.info(f"Pruned {deleted} old snapshots")
            return deleted


class JSONStorageBackend(StorageBackendBase):
    """
    JSON file-based storage backend for simple deployments.
    
    Good for development and small-scale use. For production,
    prefer SQLiteStorageBackend.
    """
    
    def __init__(self, json_path: Path):
        """
        Initialize JSON storage.
        
        Args:
            json_path: Path to the JSON file.
        """
        self.json_path = json_path
        self._data: Dict[str, Any] = {"version": "1.0", "snapshots": []}
    
    def _load(self) -> None:
        """Load data from JSON file."""
        if self.json_path.exists():
            with open(self.json_path, "r", encoding="utf-8") as f:
                self._data = json.load(f)
    
    def _save(self) -> None:
        """Save data to JSON file."""
        self.json_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.json_path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, default=str)
    
    async def initialize(self) -> None:
        """Initialize JSON storage."""
        logger.info(f"Initializing JSON storage at {self.json_path}")
        
        if self.json_path.exists():
            self._load()
        else:
            self._save()
        
        logger.info("JSON storage initialized successfully")
    
    async def save_snapshot(self, snapshot: StateSnapshot) -> None:
        """Save a state snapshot."""
        self._load()
        self._data["snapshots"].append(snapshot.to_dict())
        self._data["last_updated"] = datetime.now(timezone.utc).isoformat()
        self._save()
        logger.debug(f"Snapshot {snapshot.id} saved to JSON")
    
    async def get_snapshot(self, snapshot_id: str) -> Optional[StateSnapshot]:
        """Retrieve a snapshot by ID."""
        self._load()
        for snap_dict in self._data["snapshots"]:
            if snap_dict["id"] == snapshot_id:
                return StateSnapshot.from_dict(snap_dict)
        return None
    
    async def get_snapshots(
        self,
        state_type: Optional[StateType] = None,
        operation: Optional[str] = None,
        status: Optional[OperationStatus] = None,
        workspace_id: Optional[str] = None,
        target_name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order_desc: bool = True,
    ) -> List[StateSnapshot]:
        """Query snapshots with filters."""
        self._load()
        
        results = []
        for snap_dict in self._data["snapshots"]:
            if state_type and snap_dict["state_type"] != state_type.value:
                continue
            if operation and snap_dict["operation"] != operation:
                continue
            if status and snap_dict["status"] != status.value:
                continue
            if workspace_id and snap_dict.get("workspace_id") != workspace_id:
                continue
            if target_name and snap_dict.get("target_name") != target_name:
                continue
            results.append(StateSnapshot.from_dict(snap_dict))
        
        # Sort by timestamp
        results.sort(key=lambda x: x.timestamp, reverse=order_desc)
        
        # Apply pagination
        return results[offset:offset + limit]
    
    async def get_latest_snapshot(
        self,
        state_type: StateType,
        target_id: Optional[str] = None,
        target_name: Optional[str] = None,
    ) -> Optional[StateSnapshot]:
        """Get the most recent snapshot matching criteria."""
        snapshots = await self.get_snapshots(
            state_type=state_type,
            target_name=target_name,
            limit=1,
            order_desc=True,
        )
        
        if target_id:
            snapshots = [s for s in snapshots if s.target_id == target_id]
        
        return snapshots[0] if snapshots else None
    
    async def delete_snapshot(self, snapshot_id: str) -> bool:
        """Delete a snapshot by ID."""
        self._load()
        original_len = len(self._data["snapshots"])
        self._data["snapshots"] = [
            s for s in self._data["snapshots"] if s["id"] != snapshot_id
        ]
        if len(self._data["snapshots"]) < original_len:
            self._save()
            return True
        return False
    
    async def count_snapshots(
        self,
        state_type: Optional[StateType] = None,
        operation: Optional[str] = None,
    ) -> int:
        """Count snapshots matching criteria."""
        self._load()
        count = 0
        for snap_dict in self._data["snapshots"]:
            if state_type and snap_dict["state_type"] != state_type.value:
                continue
            if operation and snap_dict["operation"] != operation:
                continue
            count += 1
        return count
    
    async def prune_old_snapshots(self, keep_count: int) -> int:
        """Delete old snapshots, keeping only the most recent N."""
        self._load()
        
        if len(self._data["snapshots"]) <= keep_count:
            return 0
        
        # Sort by timestamp descending and keep only the most recent
        sorted_snapshots = sorted(
            self._data["snapshots"],
            key=lambda x: x["timestamp"],
            reverse=True,
        )
        
        deleted = len(sorted_snapshots) - keep_count
        self._data["snapshots"] = sorted_snapshots[:keep_count]
        self._save()
        
        logger.info(f"Pruned {deleted} old snapshots from JSON storage")
        return deleted


class MemoryManager:
    """
    High-level interface for state management and audit trail.

    The MemoryManager is the central component for tracking all agent
    state changes. It provides:

    1. **Audit Trail**: Every operation is logged with full context
    2. **Rollback**: Previous states can be queried and restored
    3. **Integrity**: Checksums verify data hasn't been corrupted
    4. **Analytics**: Query patterns and operation history

    Example:
        >>> from fabric_agent.storage import MemoryManager, StorageBackend
        >>>
        >>> manager = MemoryManager(
        ...     storage_backend=StorageBackend.SQLITE,
        ...     storage_path=Path("./data/agent.db")
        ... )
        >>> await manager.initialize()
        >>>
        >>> # Record a state change
        >>> snapshot = await manager.record_state(
        ...     state_type=StateType.MEASURE,
        ...     operation="rename_measure",
        ...     target_name="Sales",
        ...     old_value="Sales",
        ...     new_value="Net_Revenue",
        ...     state_data={"model": "Analytics", "expression": "SUM(...)"}
        ... )
        >>>
        >>> # Get audit history
        >>> history = await manager.get_history(limit=10)
        >>>
        >>> # Rollback to a previous state
        >>> previous = await manager.get_rollback_state(
        ...     state_type=StateType.MEASURE,
        ...     target_name="Net_Revenue"
        ... )
    """

    def __init__(
        self,
        storage_backend: str = "sqlite",
        storage_path: Optional[Path] = None,
        max_states: int = 100,
        # Back-compat alias used by some scripts/tests
        backend: Optional[str] = None,
        # Optional: wire in OperationMemory for vector RAG auto-indexing
        operation_memory: Optional[Any] = None,
    ):
        """
        Initialize the MemoryManager.

        Args:
            storage_backend: "sqlite" or "json"
            storage_path: Path to storage file
            max_states: Maximum states to retain (older ones are pruned)
            operation_memory: Optional OperationMemory instance. When provided,
                every record_state() call will also trigger async vector indexing
                so the operation is searchable by semantic similarity.
                Pass an OperationMemory from fabric_agent.memory to enable
                Context Memory (Use Case 2).
        """
        from fabric_agent.core.config import StorageBackend

        # Allow `backend=` as an alias for `storage_backend=`
        if backend is not None:
            storage_backend = backend

        self.storage_backend_type = str(storage_backend).lower()
        self.storage_path = storage_path or Path("./data/fabric_agent.db")
        self.max_states = max_states
        self._backend: Optional[StorageBackendBase] = None
        self._initialized = False
        # OperationMemory for vector RAG (optional, zero-cost if not set)
        self._operation_memory: Optional[Any] = operation_memory

        logger.info(
            f"MemoryManager created with {self.storage_backend_type} backend "
            f"at {self.storage_path}"
            + (" + OperationMemory (vector RAG enabled)" if operation_memory else "")
        )

    async def close(self) -> None:
        """Close/cleanup the underlying storage backend (if any).

        Notes:
            - The SQLite backend opens short-lived connections per operation,
              so this is mostly a no-op today.
            - We still provide it to support test suites and to make future
              backends (HTTP clients, long-lived DB pools) easy to manage.
        """
        if self._backend is not None:
            closer = getattr(self._backend, "close", None)
            if closer is not None:
                result = closer()
                # Support async or sync close()
                import asyncio

                if asyncio.iscoroutine(result):
                    await result

        self._backend = None
        self._initialized = False

    async def __aenter__(self) -> "MemoryManager":
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def initialize(self) -> None:
        """
        Initialize the storage backend.

        Must be called before any other operations.
        """
        if self._initialized:
            logger.debug("MemoryManager already initialized")
            return

        if self.storage_backend_type == "sqlite":
            self._backend = SQLiteStorageBackend(self.storage_path)
        elif self.storage_backend_type == "json":
            json_path = self.storage_path.with_suffix(".json")
            self._backend = JSONStorageBackend(json_path)
        else:
            raise ValueError(f"Unknown storage backend: {self.storage_backend_type}")

        await self._backend.initialize()
        self._initialized = True

        # Prune old states if needed
        count = await self._backend.count_snapshots()
        if count > self.max_states:
            await self._backend.prune_old_snapshots(self.max_states)

        logger.info("MemoryManager initialized successfully")

    def _ensure_initialized(self) -> None:
        """Ensure the manager is initialized."""
        if not self._initialized or not self._backend:
            raise RuntimeError("MemoryManager not initialized. Call initialize() first.")

    async def record_state(
        self,
        state_type: StateType,
        operation: str,
        status: OperationStatus = OperationStatus.SUCCESS,
        workspace_id: Optional[str] = None,
        workspace_name: Optional[str] = None,
        target_id: Optional[str] = None,
        target_name: Optional[str] = None,
        old_value: Optional[str] = None,
        new_value: Optional[str] = None,
        state_data: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        parent_id: Optional[str] = None,
        user: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> StateSnapshot:
        """
        Record a new state snapshot.

        This is the primary method for creating audit trail entries.
        """
        self._ensure_initialized()

        snapshot = StateSnapshot(
            state_type=state_type,
            operation=operation,
            status=status,
            workspace_id=workspace_id,
            workspace_name=workspace_name,
            target_id=target_id,
            target_name=target_name,
            old_value=old_value,
            new_value=new_value,
            state_data=state_data or {},
            metadata=metadata or {},
            parent_id=parent_id,
            user=user,
            error_message=error_message,
        )

        await self._backend.save_snapshot(snapshot)

        logger.info(
            f"Recorded state: {operation} [{state_type.value}] "
            f"status={status.value} target={target_name}"
        )

        # Auto-index into vector store for semantic search (non-blocking)
        # This is the bridge between the SQL audit trail and the RAG memory layer.
        # If OperationMemory is not configured, this is a no-op (zero overhead).
        if self._operation_memory is not None:
            import asyncio
            asyncio.ensure_future(
                self._operation_memory.index_operation(snapshot)
            )

        return snapshot

    async def get_history(
        self,
        state_type: Optional[StateType] = None,
        operation: Optional[str] = None,
        status: Optional[OperationStatus] = None,
        workspace_id: Optional[str] = None,
        target_name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[StateSnapshot]:
        """Get audit trail history."""
        self._ensure_initialized()

        return await self._backend.get_snapshots(
            state_type=state_type,
            operation=operation,
            status=status,
            workspace_id=workspace_id,
            target_name=target_name,
            limit=limit,
            offset=offset,
            order_desc=True,
        )

    async def get_snapshot(self, snapshot_id: str) -> Optional[StateSnapshot]:
        """Get a specific snapshot by ID."""
        self._ensure_initialized()
        return await self._backend.get_snapshot(snapshot_id)

    async def get_rollback_state(
        self,
        state_type: StateType,
        target_id: Optional[str] = None,
        target_name: Optional[str] = None,
        skip_latest: int = 1,
    ) -> Optional[StateSnapshot]:
        """Get a previous state for rollback purposes."""
        self._ensure_initialized()

        snapshots = await self._backend.get_snapshots(
            state_type=state_type,
            target_name=target_name,
            limit=skip_latest + 1,
            order_desc=True,
        )

        if target_id:
            snapshots = [s for s in snapshots if s.target_id == target_id]

        if len(snapshots) > skip_latest:
            return snapshots[skip_latest]

        return None

    async def get_operation_chain(self, snapshot_id: str) -> List[StateSnapshot]:
        """Get the full chain of related operations (oldest first)."""
        self._ensure_initialized()

        chain: List[StateSnapshot] = []
        current_id: Optional[str] = snapshot_id

        while current_id:
            snapshot = await self._backend.get_snapshot(current_id)
            if snapshot:
                chain.append(snapshot)
                current_id = snapshot.parent_id
            else:
                break

        return list(reversed(chain))

    async def verify_all_integrity(self) -> Dict[str, Any]:
        """Verify integrity of all stored snapshots."""
        self._ensure_initialized()

        all_snapshots = await self._backend.get_snapshots(limit=10000)
        valid = 0
        corrupted = []

        for snapshot in all_snapshots:
            if snapshot.verify_integrity():
                valid += 1
            else:
                corrupted.append(snapshot.id)

        return {
            "total": len(all_snapshots),
            "valid": valid,
            "corrupted_count": len(corrupted),
            "corrupted_ids": corrupted,
        }

    async def get_statistics(self) -> Dict[str, Any]:
        """Get storage statistics."""
        self._ensure_initialized()

        total = await self._backend.count_snapshots()

        stats: Dict[str, Any] = {
            "total_snapshots": total,
            "by_type": {},
            "by_status": {},
        }

        for state_type in StateType:
            count = await self._backend.count_snapshots(state_type=state_type)
            if count > 0:
                stats["by_type"][state_type.value] = count

        all_snapshots = await self._backend.get_snapshots(limit=total)
        for snapshot in all_snapshots:
            status = snapshot.status.value
            stats["by_status"][status] = stats["by_status"].get(status, 0) + 1

        return stats

    async def cleanup(self) -> int:
        """Clean up old snapshots exceeding max_states."""
        self._ensure_initialized()
        return await self._backend.prune_old_snapshots(self.max_states)
