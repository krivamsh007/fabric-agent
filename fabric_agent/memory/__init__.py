"""
Context Memory Module
=====================

Semantic memory layer built on top of the existing SQLite audit trail.

Provides vector-similarity search over operation history so agents can
find "what happened last time I tried something like this" — even when
the names are different.

Modules:
    embedding_client   — Abstract EmbeddingClient + LocalEmbeddingClient (offline)
    vector_store       — Abstract VectorStore + ChromaVectorStore
    operation_memory   — OperationMemory: bridges SQLite + ChromaDB
    session_context    — SessionContext: cross-session state handoff

Quick start:
    from fabric_agent.memory import OperationMemory, get_default_memory

    memory = get_default_memory(memory_manager)
    await memory.initialize()

    context = await memory.get_risk_context("rename Total Revenue to Gross Revenue")
    print(context.historical_failure_rate)  # e.g. 0.33
    print(context.recommendations)
"""

from fabric_agent.memory.embedding_client import (
    EmbeddingClient,
    LocalEmbeddingClient,
    get_embedding_client,
)
from fabric_agent.memory.vector_store import (
    VectorStore,
    ChromaVectorStore,
    SimilarResult,
)
from fabric_agent.memory.operation_memory import (
    OperationMemory,
    SimilarOperation,
    FailurePattern,
    RiskContext,
    get_default_memory,
)
from fabric_agent.memory.session_context import SessionContext

__all__ = [
    # Embedding
    "EmbeddingClient",
    "LocalEmbeddingClient",
    "get_embedding_client",
    # Vector Store
    "VectorStore",
    "ChromaVectorStore",
    "SimilarResult",
    # Operation Memory
    "OperationMemory",
    "SimilarOperation",
    "FailurePattern",
    "RiskContext",
    "get_default_memory",
    # Session
    "SessionContext",
]
