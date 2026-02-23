"""
Tests for MemoryManager
=======================

Tests for audit trail and rollback functionality.
"""

import asyncio
import tempfile
from pathlib import Path

import pytest

from fabric_agent.storage.memory_manager import (
    MemoryManager,
    StateSnapshot,
    StateType,
    OperationStatus,
    SQLiteStorageBackend,
    JSONStorageBackend,
)


@pytest.fixture
def temp_db_path():
    """Create a temporary database path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "test.db"


@pytest.fixture
def temp_json_path():
    """Create a temporary JSON path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "test.json"


class TestStateSnapshot:
    """Tests for StateSnapshot model."""
    
    def test_create_snapshot(self):
        """Test creating a basic snapshot."""
        snapshot = StateSnapshot(
            state_type=StateType.MEASURE,
            operation="rename_measure",
            target_name="Sales",
            old_value="Sales",
            new_value="Net_Revenue",
        )
        
        assert snapshot.state_type == StateType.MEASURE
        assert snapshot.operation == "rename_measure"
        assert snapshot.target_name == "Sales"
        assert snapshot.status == OperationStatus.PENDING
        assert snapshot.id is not None
        assert snapshot.timestamp is not None
    
    def test_snapshot_with_data(self):
        """Test snapshot with state_data."""
        state_data = {"model": "Analytics", "expression": "SUM(Sales)"}
        
        snapshot = StateSnapshot(
            state_type=StateType.MEASURE,
            operation="test",
            state_data=state_data,
        )
        
        assert snapshot.state_data == state_data
        assert snapshot.checksum is not None
    
    def test_verify_integrity_valid(self):
        """Test integrity verification with valid data."""
        snapshot = StateSnapshot(
            state_type=StateType.MEASURE,
            operation="test",
            state_data={"key": "value"},
        )
        
        assert snapshot.verify_integrity() is True
    
    def test_verify_integrity_tampered(self):
        """Test integrity verification with tampered data."""
        snapshot = StateSnapshot(
            state_type=StateType.MEASURE,
            operation="test",
            state_data={"key": "value"},
        )
        
        # Tamper with data
        snapshot.state_data["key"] = "tampered"
        
        assert snapshot.verify_integrity() is False
    
    def test_to_dict_and_from_dict(self):
        """Test serialization roundtrip."""
        original = StateSnapshot(
            state_type=StateType.REFACTOR,
            operation="rename_measure",
            status=OperationStatus.SUCCESS,
            workspace_id="ws-123",
            target_name="Sales",
            old_value="Sales",
            new_value="Revenue",
            state_data={"model": "Analytics"},
            metadata={"user": "test"},
        )
        
        data = original.to_dict()
        restored = StateSnapshot.from_dict(data)
        
        assert restored.id == original.id
        assert restored.state_type == original.state_type
        assert restored.operation == original.operation
        assert restored.status == original.status
        assert restored.target_name == original.target_name
        assert restored.old_value == original.old_value
        assert restored.new_value == original.new_value


class TestSQLiteStorageBackend:
    """Tests for SQLite storage backend."""
    
    @pytest.mark.asyncio
    async def test_initialize(self, temp_db_path):
        """Test database initialization."""
        backend = SQLiteStorageBackend(temp_db_path)
        await backend.initialize()
        
        assert temp_db_path.exists()
    
    @pytest.mark.asyncio
    async def test_save_and_get_snapshot(self, temp_db_path):
        """Test saving and retrieving a snapshot."""
        backend = SQLiteStorageBackend(temp_db_path)
        await backend.initialize()
        
        snapshot = StateSnapshot(
            state_type=StateType.MEASURE,
            operation="test",
            target_name="Test",
        )
        
        await backend.save_snapshot(snapshot)
        
        retrieved = await backend.get_snapshot(snapshot.id)
        
        assert retrieved is not None
        assert retrieved.id == snapshot.id
        assert retrieved.operation == snapshot.operation
    
    @pytest.mark.asyncio
    async def test_get_snapshots_with_filter(self, temp_db_path):
        """Test querying snapshots with filters."""
        backend = SQLiteStorageBackend(temp_db_path)
        await backend.initialize()
        
        # Create multiple snapshots
        for i in range(5):
            snapshot = StateSnapshot(
                state_type=StateType.MEASURE if i % 2 == 0 else StateType.REPORT,
                operation=f"test_{i}",
            )
            await backend.save_snapshot(snapshot)
        
        # Filter by type
        measures = await backend.get_snapshots(state_type=StateType.MEASURE)
        assert len(measures) == 3
        
        reports = await backend.get_snapshots(state_type=StateType.REPORT)
        assert len(reports) == 2
    
    @pytest.mark.asyncio
    async def test_get_latest_snapshot(self, temp_db_path):
        """Test getting the most recent snapshot."""
        backend = SQLiteStorageBackend(temp_db_path)
        await backend.initialize()
        
        # Create snapshots
        for i in range(3):
            snapshot = StateSnapshot(
                state_type=StateType.MEASURE,
                operation=f"test_{i}",
                target_name="Sales",
            )
            await backend.save_snapshot(snapshot)
            await asyncio.sleep(0.01)  # Ensure different timestamps
        
        latest = await backend.get_latest_snapshot(
            state_type=StateType.MEASURE,
            target_name="Sales",
        )
        
        assert latest is not None
        assert latest.operation == "test_2"
    
    @pytest.mark.asyncio
    async def test_delete_snapshot(self, temp_db_path):
        """Test deleting a snapshot."""
        backend = SQLiteStorageBackend(temp_db_path)
        await backend.initialize()
        
        snapshot = StateSnapshot(
            state_type=StateType.MEASURE,
            operation="to_delete",
        )
        await backend.save_snapshot(snapshot)
        
        deleted = await backend.delete_snapshot(snapshot.id)
        assert deleted is True
        
        retrieved = await backend.get_snapshot(snapshot.id)
        assert retrieved is None
    
    @pytest.mark.asyncio
    async def test_prune_old_snapshots(self, temp_db_path):
        """Test pruning old snapshots."""
        backend = SQLiteStorageBackend(temp_db_path)
        await backend.initialize()
        
        # Create many snapshots
        for i in range(10):
            snapshot = StateSnapshot(
                state_type=StateType.MEASURE,
                operation=f"test_{i}",
            )
            await backend.save_snapshot(snapshot)
            await asyncio.sleep(0.01)
        
        count_before = await backend.count_snapshots()
        assert count_before == 10
        
        deleted = await backend.prune_old_snapshots(keep_count=5)
        assert deleted == 5
        
        count_after = await backend.count_snapshots()
        assert count_after == 5


class TestJSONStorageBackend:
    """Tests for JSON storage backend."""
    
    @pytest.mark.asyncio
    async def test_initialize(self, temp_json_path):
        """Test JSON file initialization."""
        backend = JSONStorageBackend(temp_json_path)
        await backend.initialize()
        
        assert temp_json_path.exists()
    
    @pytest.mark.asyncio
    async def test_save_and_get_snapshot(self, temp_json_path):
        """Test saving and retrieving a snapshot."""
        backend = JSONStorageBackend(temp_json_path)
        await backend.initialize()
        
        snapshot = StateSnapshot(
            state_type=StateType.MEASURE,
            operation="test",
        )
        
        await backend.save_snapshot(snapshot)
        
        retrieved = await backend.get_snapshot(snapshot.id)
        
        assert retrieved is not None
        assert retrieved.id == snapshot.id


class TestMemoryManager:
    """Tests for MemoryManager high-level interface."""
    
    @pytest.mark.asyncio
    async def test_initialize_sqlite(self, temp_db_path):
        """Test initialization with SQLite backend."""
        manager = MemoryManager(
            storage_backend="sqlite",
            storage_path=temp_db_path,
        )
        await manager.initialize()
        
        assert manager._initialized
    
    @pytest.mark.asyncio
    async def test_initialize_json(self, temp_json_path):
        """Test initialization with JSON backend."""
        manager = MemoryManager(
            storage_backend="json",
            storage_path=temp_json_path,
        )
        await manager.initialize()
        
        assert manager._initialized
    
    @pytest.mark.asyncio
    async def test_record_state(self, temp_db_path):
        """Test recording a state."""
        manager = MemoryManager(
            storage_backend="sqlite",
            storage_path=temp_db_path,
        )
        await manager.initialize()
        
        snapshot = await manager.record_state(
            state_type=StateType.MEASURE,
            operation="rename_measure",
            status=OperationStatus.SUCCESS,
            target_name="Sales",
            old_value="Sales",
            new_value="Revenue",
        )
        
        assert snapshot.id is not None
        assert snapshot.operation == "rename_measure"
    
    @pytest.mark.asyncio
    async def test_get_history(self, temp_db_path):
        """Test getting operation history."""
        manager = MemoryManager(
            storage_backend="sqlite",
            storage_path=temp_db_path,
        )
        await manager.initialize()
        
        # Record multiple states
        for i in range(5):
            await manager.record_state(
                state_type=StateType.MEASURE,
                operation=f"test_{i}",
            )
        
        history = await manager.get_history(limit=3)
        assert len(history) == 3
    
    @pytest.mark.asyncio
    async def test_get_rollback_state(self, temp_db_path):
        """Test getting a state for rollback."""
        manager = MemoryManager(
            storage_backend="sqlite",
            storage_path=temp_db_path,
        )
        await manager.initialize()
        
        # Record states
        for i in range(3):
            await manager.record_state(
                state_type=StateType.MEASURE,
                operation=f"test_{i}",
                target_name="Sales",
            )
            await asyncio.sleep(0.01)
        
        # Get previous state (skip latest)
        rollback = await manager.get_rollback_state(
            state_type=StateType.MEASURE,
            target_name="Sales",
            skip_latest=1,
        )
        
        assert rollback is not None
        assert rollback.operation == "test_1"
    
    @pytest.mark.asyncio
    async def test_get_statistics(self, temp_db_path):
        """Test getting statistics."""
        manager = MemoryManager(
            storage_backend="sqlite",
            storage_path=temp_db_path,
        )
        await manager.initialize()
        
        # Record states
        await manager.record_state(
            state_type=StateType.MEASURE,
            operation="test1",
            status=OperationStatus.SUCCESS,
        )
        await manager.record_state(
            state_type=StateType.REPORT,
            operation="test2",
            status=OperationStatus.FAILED,
        )
        
        stats = await manager.get_statistics()
        
        assert stats["total_snapshots"] == 2
        assert StateType.MEASURE.value in stats["by_type"]
        assert StateType.REPORT.value in stats["by_type"]
