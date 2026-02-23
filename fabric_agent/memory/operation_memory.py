"""
Operation Memory — Semantic RAG Over Audit History
===================================================

WHAT: The central class that bridges the SQLite audit trail (what happened)
      with the ChromaDB vector store (find similar things that happened) to
      answer: "what should I watch out for when I try this operation?"

WHY: The existing MemoryManager stores every operation in SQLite — perfect
     for exact lookups ("show me all renames on Total Revenue"). But it can't
     answer: "find operations semantically similar to renaming Gross Revenue
     in a model with 50+ dependencies" — that requires vector similarity.

     OperationMemory adds that semantic search layer on top, without touching
     the existing SQLite schema. It's a pure addition, not a modification.

FAANG PARALLEL:
     - Airbnb Minerva: "here's what happened last time someone refactored
       a metric in a high-traffic model" — used to guide safe changes
     - Meta's data catalog: semantic search over petabytes of table metadata
     - GitHub Copilot: "here's a similar code block from your repo"
     - Google's BORG: "this job has 87% similarity to one that OOM'd last week"

HOW IT WORKS:
     1. Every time MemoryManager.record_state() is called, it triggers
        OperationMemory.index_operation() (auto-wired in memory_manager.py)
     2. index_operation() creates a rich text description of the operation,
        embeds it, and stores it in ChromaDB with metadata for filtering
     3. When a new operation is proposed, find_similar_operations() embeds
        the description and queries ChromaDB for the K nearest neighbors
     4. The results include outcomes (success/failed/rolled_back), enabling
        the agent to say "33% of similar renames in high-dependency models failed"

DEMO SCENARIO (shows in the tutorial notebook):
     After 20+ operations, ask:
       context = await memory.get_risk_context(
           "rename Total Revenue to Gross Revenue in Enterprise_Sales_Model"
       )
       # Returns:
       # similar_operations: [rename Sales → Revenue (success, sim=0.92),
       #                       rename Revenue YTD (rolled_back, sim=0.88), ...]
       # historical_failure_rate: 0.25
       # recommendations: ["This model had rollbacks before — create checkpoint",
       #                    "High dependency count (42 measures) — use dry_run first"]

USAGE:
     from fabric_agent.memory import OperationMemory, get_default_memory

     # Quick start with defaults
     memory = get_default_memory(memory_manager)
     await memory.initialize()

     # Index a new operation (auto-called by MemoryManager)
     await memory.index_operation(snapshot)

     # Find similar operations
     results = await memory.find_similar_operations(
         "rename Total Revenue to Gross Revenue", top_k=5
     )

     # Get full risk context
     context = await memory.get_risk_context("rename Total Revenue")
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from loguru import logger

from fabric_agent.memory.embedding_client import (
    EmbeddingClient,
    LocalEmbeddingClient,
    MockEmbeddingClient,
)
from fabric_agent.memory.vector_store import (
    VectorStore,
    ChromaVectorStore,
    InMemoryVectorStore,
    SimilarResult,
)

if TYPE_CHECKING:
    from fabric_agent.storage.memory_manager import MemoryManager, StateSnapshot


# =============================================================================
# Data Models
# =============================================================================

@dataclass
class SimilarOperation:
    """
    A past operation that is semantically similar to the proposed change.

    Used by agents to reason about risk: "this operation failed with similar
    characteristics — watch out for the same issue."
    """
    snapshot_id: str
    operation: str
    target_name: str
    workspace_name: Optional[str]
    old_value: Optional[str]
    new_value: Optional[str]
    status: str          # success | failed | rolled_back
    timestamp: str
    similarity: float    # 0.0 to 1.0
    key_lesson: str      # auto-generated one-line takeaway


@dataclass
class FailurePattern:
    """
    A recurring failure pattern extracted from operation history.

    Identifies "when you do X in context Y, it tends to fail because Z."
    """
    pattern_type: str          # e.g., "high_dependency_rename_failure"
    description: str
    occurrence_count: int
    failure_rate: float        # 0.0 to 1.0
    common_contexts: List[str] = field(default_factory=list)
    mitigation: str = ""


@dataclass
class RiskContext:
    """
    Full risk context for a proposed operation.

    Synthesizes similar past operations into actionable risk intelligence.
    This is what the agent uses to decide whether to warn the user,
    require approval, or proceed autonomously.
    """
    proposed_change: str
    similar_operations: List[SimilarOperation]
    historical_failure_rate: float        # fraction of similar ops that failed
    historical_rollback_rate: float       # fraction that required rollback
    common_failure_reasons: List[str]
    recommendations: List[str]
    confidence: float                     # how confident we are (based on sample size)
    sample_size: int                      # number of similar ops found

    @property
    def risk_summary(self) -> str:
        """One-line risk summary for logging / CLI output."""
        if self.sample_size == 0:
            return "No historical data — proceed with standard caution"
        fail_pct = int(self.historical_failure_rate * 100)
        return (
            f"{self.sample_size} similar ops found: "
            f"{fail_pct}% failure rate, "
            f"{int(self.historical_rollback_rate * 100)}% rollback rate"
        )

    @property
    def is_high_risk(self) -> bool:
        """True if history suggests >30% failure rate."""
        return self.historical_failure_rate > 0.30 and self.sample_size >= 3


# =============================================================================
# OperationMemory
# =============================================================================

class OperationMemory:
    """
    Semantic memory layer: SQLite audit trail + ChromaDB vector search.

    See module docstring for full explanation.
    """

    def __init__(
        self,
        memory_manager: "MemoryManager",
        vector_store: VectorStore,
        embedding_client: EmbeddingClient,
    ):
        self._manager = memory_manager
        self._store = vector_store
        self._embedder = embedding_client
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize the vector store. Must be called before any other method."""
        if not self._initialized:
            await self._store.initialize()
            self._initialized = True
            count = await self._store.count()
            logger.info(f"OperationMemory initialized. Vector store has {count} documents.")

    # -------------------------------------------------------------------------
    # Indexing
    # -------------------------------------------------------------------------

    async def index_operation(self, snapshot: "StateSnapshot") -> None:
        """
        Embed and index a single StateSnapshot.

        Called automatically by MemoryManager after every record_state().
        Runs as a background task (non-blocking) so it doesn't slow down
        the main operation.

        Args:
            snapshot: The StateSnapshot to index.
        """
        if not self._initialized:
            await self.initialize()

        try:
            text = self._snapshot_to_text(snapshot)
            embedding = await self._embedder.embed(text)

            metadata = {
                "operation": snapshot.operation,
                "status": snapshot.status if isinstance(snapshot.status, str) else snapshot.status.value,
                "state_type": snapshot.state_type if isinstance(snapshot.state_type, str) else snapshot.state_type.value,
                "workspace_name": snapshot.workspace_name or "",
                "target_name": snapshot.target_name or "",
                "old_value": (snapshot.old_value or "")[:200],
                "new_value": (snapshot.new_value or "")[:200],
                "timestamp": snapshot.timestamp if isinstance(snapshot.timestamp, str) else str(snapshot.timestamp),
            }

            await self._store.upsert(
                doc_id=snapshot.id,
                embedding=embedding,
                metadata=metadata,
                text=text,
            )
            logger.debug(f"Indexed operation: {snapshot.id} ({snapshot.operation})")

        except Exception as e:
            # Never let indexing failure break the main operation
            logger.warning(f"Failed to index operation {snapshot.id}: {e}")

    async def reindex_all(self) -> int:
        """
        Rebuild the entire vector index from SQLite history.

        Use this when:
        - The vector store is new/empty but SQLite has history
        - The embedding model was changed (re-embed everything)
        - The vector store was deleted and needs to be rebuilt

        Returns:
            Number of operations indexed.
        """
        if not self._initialized:
            await self.initialize()

        logger.info("Reindexing all operations from SQLite history...")
        snapshots = await self._manager.get_history(limit=10_000)

        indexed = 0
        batch_size = 50

        for i in range(0, len(snapshots), batch_size):
            batch = snapshots[i : i + batch_size]
            texts = [self._snapshot_to_text(s) for s in batch]
            embeddings = await self._embedder.embed_batch(texts)

            for snapshot, embedding in zip(batch, embeddings):
                metadata = {
                    "operation": snapshot.operation,
                    "status": snapshot.status if isinstance(snapshot.status, str) else snapshot.status.value,
                    "state_type": snapshot.state_type if isinstance(snapshot.state_type, str) else snapshot.state_type.value,
                    "workspace_name": snapshot.workspace_name or "",
                    "target_name": snapshot.target_name or "",
                    "old_value": (snapshot.old_value or "")[:200],
                    "new_value": (snapshot.new_value or "")[:200],
                    "timestamp": snapshot.timestamp if isinstance(snapshot.timestamp, str) else str(snapshot.timestamp),
                }
                await self._store.upsert(snapshot.id, embedding, metadata, texts[0])
                indexed += 1

            logger.info(f"Reindexed {min(i + batch_size, len(snapshots))}/{len(snapshots)} operations")

        logger.info(f"Reindex complete. {indexed} operations indexed.")
        return indexed

    # -------------------------------------------------------------------------
    # Querying
    # -------------------------------------------------------------------------

    async def find_similar_operations(
        self,
        proposed_change: str,
        change_type: Optional[str] = None,
        top_k: int = 5,
    ) -> List[SimilarOperation]:
        """
        Find past operations semantically similar to the proposed change.

        Args:
            proposed_change: Natural language description of the proposed operation
                           e.g., "rename Total Revenue to Gross Revenue"
            change_type:   Optional filter to restrict to a specific operation type
                           e.g., "rename_measure", "schema_drift"
            top_k:         Number of results to return

        Returns:
            List of SimilarOperation sorted by similarity (highest first).

        Example:
            ops = await memory.find_similar_operations(
                "rename Total Revenue to Gross Revenue",
                change_type="rename_measure",
                top_k=5
            )
            for op in ops:
                print(f"{op.similarity:.2f} | {op.status} | {op.target_name}")
        """
        if not self._initialized:
            await self.initialize()

        embedding = await self._embedder.embed(proposed_change)
        where = {"operation": change_type} if change_type else None

        raw_results = await self._store.query(embedding, top_k=top_k, where=where)
        return [self._result_to_similar_op(r) for r in raw_results]

    async def get_failure_patterns(
        self,
        asset_type: str,
        change_type: str,
    ) -> List[FailurePattern]:
        """
        Identify recurring failure patterns for a given asset + change combination.

        Args:
            asset_type:  e.g., "measure", "table", "shortcut"
            change_type: e.g., "rename_measure", "schema_drift"

        Returns:
            List of FailurePattern, sorted by occurrence count.
        """
        # Get all failed operations of this type
        failed = await self._manager.get_history(
            operation=change_type,
            limit=200,
        )
        failed_ops = [s for s in failed if self._is_failed(s)]

        if not failed_ops:
            return []

        # Simple pattern: count by workspace (high-dep workspaces fail more)
        patterns: Dict[str, int] = {}
        for op in failed_ops:
            key = f"{op.workspace_name or 'unknown'}"
            patterns[key] = patterns.get(key, 0) + 1

        total = len(failed_ops)
        result = []
        for ctx, count in sorted(patterns.items(), key=lambda x: -x[1]):
            result.append(FailurePattern(
                pattern_type=f"{change_type}_failure_in_{ctx}",
                description=f"{change_type} operations in {ctx} fail frequently",
                occurrence_count=count,
                failure_rate=count / total,
                common_contexts=[ctx],
                mitigation="Use dry_run first and create a checkpoint",
            ))

        return result[:5]  # top 5 patterns

    async def get_risk_context(self, proposed_change: str, top_k: int = 10) -> RiskContext:
        """
        Full risk context for a proposed operation.

        Synthesizes similar past operations into:
        - failure rate (e.g., 25% of similar ops failed)
        - rollback rate (e.g., 10% required rollback)
        - common failure reasons
        - actionable recommendations

        This is the main method used by agents before executing any operation.

        Args:
            proposed_change: Natural language description of the proposed operation.
            top_k:          How many similar operations to use for context.

        Returns:
            RiskContext with synthesized risk intelligence.
        """
        if not self._initialized:
            await self.initialize()

        similar = await self.find_similar_operations(proposed_change, top_k=top_k)

        # Calculate failure and rollback rates from similar ops
        total = len(similar)
        if total == 0:
            return RiskContext(
                proposed_change=proposed_change,
                similar_operations=[],
                historical_failure_rate=0.0,
                historical_rollback_rate=0.0,
                common_failure_reasons=[],
                recommendations=["No historical data — proceed with standard caution"],
                confidence=0.0,
                sample_size=0,
            )

        failed = sum(1 for op in similar if op.status in ("failed", "FAILED"))
        rolled_back = sum(1 for op in similar if op.status in ("rolled_back", "ROLLED_BACK"))

        failure_rate = failed / total
        rollback_rate = rolled_back / total

        # Extract common failure reasons from similar failed ops
        failure_reasons = [
            op.key_lesson for op in similar
            if op.status in ("failed", "FAILED", "rolled_back", "ROLLED_BACK")
        ][:3]

        # Generate recommendations based on history
        recommendations = self._generate_recommendations(
            similar, failure_rate, rollback_rate
        )

        # Confidence: scales with sample size (diminishing returns above 10)
        confidence = min(1.0, total / 10.0)

        return RiskContext(
            proposed_change=proposed_change,
            similar_operations=similar[:5],  # return top 5 for display
            historical_failure_rate=round(failure_rate, 2),
            historical_rollback_rate=round(rollback_rate, 2),
            common_failure_reasons=failure_reasons,
            recommendations=recommendations,
            confidence=round(confidence, 2),
            sample_size=total,
        )

    async def get_statistics(self) -> Dict[str, Any]:
        """Return statistics about the vector store."""
        if not self._initialized:
            await self.initialize()
        store_stats = await self._store.get_statistics()
        sql_stats = await self._manager.get_statistics()
        n = store_stats.get("total_vectors", store_stats.get("document_count", 0))
        return {
            "vector_store": store_stats,
            "audit_trail": sql_stats,
            "embedding_dimension": self._embedder.dimension,
            "embedding_backend": type(self._embedder).__name__,
            # Top-level aliases for CLI display and direct test assertions
            "total_indexed": n,
            "collection_size": n,
            "embedding_model": type(self._embedder).__name__,
            "vector_store_path": store_stats.get("path", "in-memory"),
        }

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _snapshot_to_text(self, snapshot: "StateSnapshot") -> str:
        """
        Convert a StateSnapshot to a rich text description for embedding.

        The text is designed to capture the semantic meaning of the operation:
        - What type of change (rename, schema drift, rollback, etc.)
        - What was changed (asset name, old/new value)
        - What context it was in (workspace, model)
        - What happened (success, failed, rolled_back)

        More context → better embedding → more accurate similarity search.
        """
        parts = []

        op = snapshot.operation or "unknown_operation"
        parts.append(f"Operation: {op.replace('_', ' ')}")

        if snapshot.target_name:
            parts.append(f"Target: {snapshot.target_name}")

        if snapshot.old_value and snapshot.new_value:
            parts.append(f"Change: from '{snapshot.old_value}' to '{snapshot.new_value}'")
        elif snapshot.old_value:
            parts.append(f"Previous value: {snapshot.old_value}")
        elif snapshot.new_value:
            parts.append(f"New value: {snapshot.new_value}")

        if snapshot.workspace_name:
            parts.append(f"Workspace: {snapshot.workspace_name}")

        status = snapshot.status if isinstance(snapshot.status, str) else snapshot.status.value
        parts.append(f"Outcome: {status}")

        state_type = snapshot.state_type if isinstance(snapshot.state_type, str) else snapshot.state_type.value
        parts.append(f"Asset type: {state_type}")

        return ". ".join(parts)

    def _result_to_similar_op(self, result: SimilarResult) -> SimilarOperation:
        """Convert a vector store SimilarResult to a SimilarOperation."""
        meta = result.metadata
        status = meta.get("status", "unknown")

        key_lesson = self._generate_lesson(
            operation=meta.get("operation", ""),
            status=status,
            target=meta.get("target_name", ""),
            old_val=meta.get("old_value", ""),
            new_val=meta.get("new_value", ""),
        )

        return SimilarOperation(
            snapshot_id=result.doc_id,
            operation=meta.get("operation", "unknown"),
            target_name=meta.get("target_name", ""),
            workspace_name=meta.get("workspace_name") or None,
            old_value=meta.get("old_value") or None,
            new_value=meta.get("new_value") or None,
            status=status,
            timestamp=meta.get("timestamp", ""),
            similarity=result.similarity,
            key_lesson=key_lesson,
        )

    def _generate_lesson(
        self,
        operation: str,
        status: str,
        target: str,
        old_val: str,
        new_val: str,
    ) -> str:
        """Generate a one-line lesson from a historical operation."""
        op_name = operation.replace("_", " ")
        if status in ("failed", "FAILED"):
            return f"{op_name} on '{target}' failed — review dependencies before retrying"
        elif status in ("rolled_back", "ROLLED_BACK"):
            return f"{op_name} on '{target}' was rolled back — validation failed post-change"
        elif status in ("success", "SUCCESS"):
            if old_val and new_val:
                return f"Successfully renamed '{old_val}' → '{new_val}' — safe pattern"
            return f"{op_name} on '{target}' succeeded — similar change is safe"
        return f"{op_name} on '{target}': {status}"

    def _is_failed(self, snapshot: "StateSnapshot") -> bool:
        """Check if a snapshot represents a failed operation."""
        status = snapshot.status if isinstance(snapshot.status, str) else snapshot.status.value
        return status.lower() in ("failed", "rolled_back")

    def _generate_recommendations(
        self,
        similar: List[SimilarOperation],
        failure_rate: float,
        rollback_rate: float,
    ) -> List[str]:
        """Generate actionable recommendations based on historical context."""
        recs = []

        if failure_rate > 0.5:
            recs.append(
                f"HIGH RISK: {int(failure_rate*100)}% of similar operations failed. "
                "Use dry_run=True first and create a checkpoint."
            )
        elif failure_rate > 0.2:
            recs.append(
                f"MODERATE RISK: {int(failure_rate*100)}% of similar operations failed. "
                "Recommended: run with dry_run=True first."
            )

        if rollback_rate > 0.15:
            recs.append(
                f"{int(rollback_rate*100)}% of similar operations required rollback. "
                "Ensure the validation agent is active before executing."
            )

        rolled_back_ops = [op for op in similar if op.status in ("rolled_back", "ROLLED_BACK")]
        if rolled_back_ops:
            recs.append(
                f"Lesson from similar op: {rolled_back_ops[0].key_lesson}"
            )

        if not recs:
            recs.append("Historical data shows similar operations succeeded. Proceed with standard checks.")

        return recs


# =============================================================================
# Factory
# =============================================================================

def get_default_memory(
    memory_manager: "MemoryManager",
    use_mock: bool = False,
) -> OperationMemory:
    """
    Create an OperationMemory with sensible defaults.

    Uses:
    - LocalEmbeddingClient (offline, no API key needed) OR MockEmbeddingClient in tests
    - ChromaVectorStore (local) OR InMemoryVectorStore in tests
    - Path auto-resolved by fabric_env (local: data/vector_store, Fabric: /lakehouse/...)

    Args:
        memory_manager: Existing MemoryManager instance.
        use_mock: Use in-memory backends (for unit tests).

    Returns:
        OperationMemory ready to use (still needs await memory.initialize()).

    Example:
        memory = get_default_memory(manager)
        await memory.initialize()
        context = await memory.get_risk_context("rename Total Revenue")
    """
    if use_mock:
        embedder = MockEmbeddingClient()
        store = InMemoryVectorStore()
    else:
        embedder = LocalEmbeddingClient()
        store = ChromaVectorStore()  # auto-resolves path via fabric_env

    return OperationMemory(
        memory_manager=memory_manager,
        vector_store=store,
        embedding_client=embedder,
    )
