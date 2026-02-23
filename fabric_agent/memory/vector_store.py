"""
Vector Store
============

WHAT: Stores and retrieves embedding vectors with metadata, enabling
      nearest-neighbor search over operation history.

WHY: We need to answer "find the 5 most similar past operations" in < 100ms
     even with thousands of operations in history. A regular SQL LIKE query
     can't do this. A vector store does it in O(log n) via ANN (Approximate
     Nearest Neighbors) indexing.

FAANG PARALLEL:
     - Facebook FAISS: billion-scale vector similarity search
     - Pinecone: managed vector DB used by OpenAI, Notion, Shopify
     - Google Vertex AI: vector search in BigQuery
     - Weaviate: open-source vector DB used at Apple, Red Hat
     This module provides the same abstraction with ChromaDB as the default
     (runs locally with zero infra — perfect for Fabric Notebook deployment).

HOW IT WORKS:
     1. Insert: text → embedding vector → stored in ChromaDB collection
     2. Query:  query text → embedding vector → cosine similarity scan → top-K
     3. Each document has metadata (operation_type, status, timestamp) for filtering

DEPLOYMENT:
     Local:   ChromaDB stores to disk at data/vector_store/
     Fabric:  ChromaDB stores to /lakehouse/default/Files/vector_store/
              (same code path — resolved by fabric_env.py)

USAGE:
     from fabric_agent.memory.vector_store import ChromaVectorStore
     from fabric_agent.memory.embedding_client import LocalEmbeddingClient

     store = ChromaVectorStore()
     await store.initialize()

     # Store an operation
     await store.upsert(
         doc_id="op-001",
         embedding=[0.1, 0.2, ...],  # 384 floats
         metadata={"operation": "rename_measure", "status": "success", "target": "Total Revenue"}
     )

     # Find similar
     query_vector = await embedder.embed("rename Gross Revenue")
     results = await store.query(query_vector, top_k=5)
     for r in results:
         print(r.doc_id, r.similarity, r.metadata)
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from loguru import logger

from fabric_agent.core.fabric_env import get_env


# =============================================================================
# Data Models
# =============================================================================

@dataclass
class SimilarResult:
    """
    A single result from a vector similarity search.

    Attributes:
        doc_id:     The document ID (maps to a StateSnapshot.id)
        similarity: Cosine similarity in [0, 1]. 1.0 = identical.
        distance:   Cosine distance (1 - similarity). Lower = more similar.
        metadata:   Stored metadata dict (operation_type, status, timestamp, etc.)
    """
    doc_id: str
    similarity: float
    distance: float
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_high_similarity(self) -> bool:
        """True if similarity >= 0.85 (strong semantic match)."""
        return self.similarity >= 0.85

    @property
    def outcome(self) -> str:
        """Extract outcome from metadata."""
        return self.metadata.get("status", "unknown")


# =============================================================================
# Abstract Interface
# =============================================================================

class VectorStore(ABC):
    """
    Abstract vector store interface.

    All implementations must support:
    - initialize(): Set up storage (create collection, open DB, etc.)
    - upsert(): Store or update a document
    - query(): Find similar documents by vector
    - delete(): Remove a document
    - count(): Number of stored documents
    """

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize the vector store (create collections, indices, etc.)."""

    @abstractmethod
    async def upsert(
        self,
        doc_id: str,
        embedding: List[float],
        metadata: Dict[str, Any],
        text: Optional[str] = None,
    ) -> None:
        """
        Store or update a document.

        Args:
            doc_id:    Unique identifier (use StateSnapshot.id)
            embedding: Vector from EmbeddingClient.embed()
            metadata:  Filterable metadata dict
            text:      Original text (stored for debugging, not required)
        """

    @abstractmethod
    async def query(
        self,
        embedding: List[float],
        top_k: int = 5,
        where: Optional[Dict[str, Any]] = None,
    ) -> List[SimilarResult]:
        """
        Find the top-K most similar documents.

        Args:
            embedding: Query vector from EmbeddingClient.embed()
            top_k:     Number of results to return
            where:     Optional metadata filter (e.g., {"status": "failed"})

        Returns:
            List of SimilarResult, sorted by similarity descending.
        """

    @abstractmethod
    async def delete(self, doc_id: str) -> bool:
        """Remove a document from the store. Returns True if deleted, False if not found."""

    @abstractmethod
    async def count(self) -> int:
        """Return the number of documents stored."""

    @abstractmethod
    async def get_statistics(self) -> Dict[str, Any]:
        """Return store statistics (count, size, collection info)."""


# =============================================================================
# ChromaDB Implementation (default)
# =============================================================================

class ChromaVectorStore(VectorStore):
    """
    Vector store backed by ChromaDB.

    ChromaDB is an open-source embedding database that runs locally with
    no external services required. It persists to disk automatically.

    Why ChromaDB for this project?
    - Zero setup: no Docker, no cloud account, no API key
    - Runs inside Fabric Notebooks (persists to Lakehouse Files)
    - Production-capable up to ~1M documents (sufficient for audit history)
    - Same API whether local or in Fabric (path is the only difference)

    Collection name: "fabric_operations"
    Distance metric: cosine (standard for text embeddings)

    Install: pip install chromadb
    """

    COLLECTION_NAME = "fabric_operations"

    def __init__(self, persist_path: Optional[str] = None):
        """
        Args:
            persist_path: Directory for ChromaDB storage.
                         If None, auto-resolved via fabric_env:
                           Local:  data/vector_store/
                           Fabric: /lakehouse/default/Files/vector_store/
        """
        if persist_path is None:
            persist_path = get_env().vector_store_path()
        self._persist_path = persist_path
        self._client = None
        self._collection = None
        logger.debug(f"ChromaVectorStore will persist to: {persist_path}")

    async def initialize(self) -> None:
        """Initialize ChromaDB client and get/create the operations collection."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._init_sync)

    def _init_sync(self) -> None:
        try:
            import chromadb
        except ImportError:
            raise ImportError(
                "chromadb is required for ChromaVectorStore.\n"
                "Install it with: pip install chromadb\n"
                "Or: pip install -e '.[memory]'"
            )

        self._client = chromadb.PersistentClient(path=self._persist_path)
        self._collection = self._client.get_or_create_collection(
            name=self.COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},  # cosine similarity metric
        )
        logger.info(
            f"ChromaDB initialized at {self._persist_path}. "
            f"Collection '{self.COLLECTION_NAME}' has {self._collection.count()} documents."
        )

    def _ensure_initialized(self) -> None:
        if self._collection is None:
            raise RuntimeError(
                "ChromaVectorStore not initialized. Call await store.initialize() first."
            )

    async def upsert(
        self,
        doc_id: str,
        embedding: List[float],
        metadata: Dict[str, Any],
        text: Optional[str] = None,
    ) -> None:
        self._ensure_initialized()
        loop = asyncio.get_event_loop()

        # ChromaDB requires string metadata values
        clean_meta = {k: str(v) if not isinstance(v, (str, int, float, bool)) else v
                      for k, v in metadata.items() if v is not None}

        documents = [text or doc_id]

        await loop.run_in_executor(
            None,
            lambda: self._collection.upsert(
                ids=[doc_id],
                embeddings=[embedding],
                metadatas=[clean_meta],
                documents=documents,
            ),
        )

    async def query(
        self,
        embedding: List[float],
        top_k: int = 5,
        where: Optional[Dict[str, Any]] = None,
    ) -> List[SimilarResult]:
        self._ensure_initialized()
        loop = asyncio.get_event_loop()

        def _query_sync():
            kwargs: Dict[str, Any] = {
                "query_embeddings": [embedding],
                "n_results": min(top_k, max(self._collection.count(), 1)),
                "include": ["distances", "metadatas", "documents"],
            }
            if where:
                kwargs["where"] = where
            return self._collection.query(**kwargs)

        try:
            raw = await loop.run_in_executor(None, _query_sync)
        except Exception as e:
            logger.warning(f"Vector query failed: {e}")
            return []

        results = []
        ids = raw.get("ids", [[]])[0]
        distances = raw.get("distances", [[]])[0]
        metadatas = raw.get("metadatas", [[]])[0]

        for doc_id, distance, meta in zip(ids, distances, metadatas):
            similarity = max(0.0, 1.0 - distance)  # cosine: distance = 1 - similarity
            results.append(SimilarResult(
                doc_id=doc_id,
                similarity=round(similarity, 4),
                distance=round(distance, 4),
                metadata=meta or {},
            ))

        return sorted(results, key=lambda r: r.similarity, reverse=True)

    async def delete(self, doc_id: str) -> bool:
        self._ensure_initialized()
        loop = asyncio.get_event_loop()
        # ChromaDB get() returns existing IDs; check before deleting
        existing = await loop.run_in_executor(None, lambda: self._collection.get(ids=[doc_id]))
        if not existing["ids"]:
            return False
        await loop.run_in_executor(None, lambda: self._collection.delete(ids=[doc_id]))
        return True

    async def count(self) -> int:
        self._ensure_initialized()
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._collection.count)

    async def get_statistics(self) -> Dict[str, Any]:
        self._ensure_initialized()
        n = await self.count()
        return {
            "backend": "ChromaDB",
            "collection": self.COLLECTION_NAME,
            "persist_path": self._persist_path,
            "document_count": n,
            "distance_metric": "cosine",
        }


# =============================================================================
# In-Memory Implementation (for tests — zero disk I/O)
# =============================================================================

class InMemoryVectorStore(VectorStore):
    """
    In-memory vector store for unit testing.

    Uses brute-force cosine similarity (O(n) per query). Fast enough for
    small test datasets (<1000 documents). No disk I/O — perfect for tests.

    Do NOT use in production — not persistent, not scalable.
    """

    def __init__(self) -> None:
        self._docs: Dict[str, Dict[str, Any]] = {}  # id → {embedding, metadata, text}

    async def initialize(self) -> None:
        pass  # Nothing to set up

    async def upsert(
        self,
        doc_id: str,
        embedding: List[float],
        metadata: Dict[str, Any],
        text: Optional[str] = None,
    ) -> None:
        self._docs[doc_id] = {
            "embedding": embedding,
            "metadata": metadata,
            "text": text or doc_id,
        }

    async def query(
        self,
        embedding: List[float],
        top_k: int = 5,
        where: Optional[Dict[str, Any]] = None,
    ) -> List[SimilarResult]:
        import math

        def cosine_similarity(a: List[float], b: List[float]) -> float:
            dot = sum(x * y for x, y in zip(a, b))
            mag_a = math.sqrt(sum(x * x for x in a))
            mag_b = math.sqrt(sum(x * x for x in b))
            if mag_a == 0 or mag_b == 0:
                return 0.0
            return dot / (mag_a * mag_b)

        results = []
        for doc_id, doc in self._docs.items():
            meta = doc["metadata"]
            if where:
                if not all(meta.get(k) == v for k, v in where.items()):
                    continue
            sim = cosine_similarity(embedding, doc["embedding"])
            results.append(SimilarResult(
                doc_id=doc_id,
                similarity=round(sim, 4),
                distance=round(1.0 - sim, 4),
                metadata=meta,
            ))

        results.sort(key=lambda r: r.similarity, reverse=True)
        return results[:top_k]

    async def delete(self, doc_id: str) -> bool:
        existed = doc_id in self._docs
        self._docs.pop(doc_id, None)
        return existed

    async def count(self) -> int:
        return len(self._docs)

    async def get_statistics(self) -> Dict[str, Any]:
        n = len(self._docs)
        return {
            "backend": "InMemory",
            "document_count": n,
            # Aliases used by tests and CLI display
            "total_vectors": n,
            "collection_size": n,
        }
