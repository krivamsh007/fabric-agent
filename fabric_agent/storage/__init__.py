"""
Storage Module
==============

Provides persistent state management, audit trail, and rollback capabilities.
"""

from fabric_agent.storage.memory_manager import (
    MemoryManager,
    StateSnapshot,
    StateType,
    OperationStatus,
    StorageBackendBase,
    SQLiteStorageBackend,
    JSONStorageBackend,
)

__all__ = [
    "MemoryManager",
    "StateSnapshot",
    "StateType",
    "OperationStatus",
    "StorageBackendBase",
    "SQLiteStorageBackend",
    "JSONStorageBackend",
]
