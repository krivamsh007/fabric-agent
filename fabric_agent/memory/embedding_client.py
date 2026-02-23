"""
Embedding Client
================

WHAT: Converts text (operation descriptions) into numerical vectors so they
      can be stored in a vector database and searched by similarity.

WHY: To find "operations similar to this one", we need a way to measure
     semantic similarity between text strings — not just string matching.
     Two operations can be conceptually identical but use different names:
       "rename Total Revenue" ≈ "rename Gross Revenue"  (vector similarity: 0.94)
       "rename Total Revenue" ≈ "delete Total Revenue"  (vector similarity: 0.31)

FAANG PARALLEL:
     - Google Search: query embedding for semantic retrieval
     - GitHub Copilot: code embedding for similar-code retrieval
     - Meta FAISS: embedding index for billion-scale search
     This is the same embedding abstraction, applied to operation history.

HOW IT WORKS:
     Text → Embedding Model → 384-dimensional float vector → stored in ChromaDB
     Query text → same embedding → cosine similarity search → top-K results

MODELS (in order of preference for this project):
     1. all-MiniLM-L6-v2  (default) — 90MB, fast, good quality, works offline
     2. text-embedding-ada-002      — Azure OpenAI, best quality, requires API key
     3. MockEmbeddingClient         — For tests, zero dependencies

USAGE:
     # Default: works offline, no API keys
     embedder = LocalEmbeddingClient()
     vector = await embedder.embed("rename Total Revenue to Gross Revenue")
     # Returns: List[float] of length 384

     # Batch embed for efficiency
     vectors = await embedder.embed_batch(["op1 description", "op2 description"])

     # Auto-select based on config:
     from fabric_agent.memory.embedding_client import get_embedding_client
     embedder = get_embedding_client(config)
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import List, Optional

from loguru import logger


# =============================================================================
# Abstract Interface
# =============================================================================

class EmbeddingClient(ABC):
    """
    Abstract base for all embedding implementations.

    Implementations must be thread-safe and support async calls.
    The embedding dimension must be consistent per instance.
    """

    @property
    @abstractmethod
    def dimension(self) -> int:
        """Size of the embedding vectors this client produces."""

    @abstractmethod
    async def embed(self, text: str) -> List[float]:
        """
        Embed a single text string into a float vector.

        Args:
            text: Text to embed. Can be any length, will be truncated if needed.

        Returns:
            List of floats with length == self.dimension.
        """

    @abstractmethod
    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Embed multiple texts efficiently.

        Batching is faster than calling embed() in a loop because most
        embedding models can process multiple inputs in a single forward pass.

        Args:
            texts: List of texts to embed.

        Returns:
            List of embedding vectors, same order as input.
        """


# =============================================================================
# Local Implementation (sentence-transformers — works offline)
# =============================================================================

class LocalEmbeddingClient(EmbeddingClient):
    """
    Offline embedding using sentence-transformers.

    Uses the all-MiniLM-L6-v2 model (90MB download on first use).
    Zero API keys required. Works in Fabric Notebooks and local dev.

    Why all-MiniLM-L6-v2?
    - 384-dimension vectors (compact, fast to store and search)
    - Excellent semantic similarity performance
    - MIT licensed (usable in any project)
    - Supported by the sentence-transformers library (maintained by HuggingFace)

    Install: pip install sentence-transformers
    """

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self._model_name = model_name
        self._model = None  # Lazy-loaded on first embed() call
        self._dim = 384  # all-MiniLM-L6-v2 output dimension

    @property
    def dimension(self) -> int:
        return self._dim

    def _get_model(self):
        """Lazy-load the model on first use (avoids slow import at startup)."""
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
            except ImportError:
                raise ImportError(
                    "sentence-transformers is required for LocalEmbeddingClient.\n"
                    "Install it with: pip install sentence-transformers\n"
                    "Or install the memory extras: pip install -e '.[memory]'"
                )
            logger.info(f"Loading embedding model: {self._model_name}")
            self._model = SentenceTransformer(self._model_name)
            self._dim = self._model.get_sentence_embedding_dimension()
            logger.info(f"Embedding model loaded. Dimension: {self._dim}")
        return self._model

    async def embed(self, text: str) -> List[float]:
        """Embed a single text. Runs model in thread pool to avoid blocking."""
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, self._embed_sync, [text])
        return result[0]

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Embed multiple texts in a single model forward pass."""
        if not texts:
            return []
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._embed_sync, texts)

    def _embed_sync(self, texts: List[str]) -> List[List[float]]:
        """Synchronous embedding (runs in thread pool)."""
        model = self._get_model()
        embeddings = model.encode(texts, convert_to_numpy=True, show_progress_bar=False)
        return embeddings.tolist()


# =============================================================================
# Azure OpenAI Implementation (optional, requires API key)
# =============================================================================

class AzureOpenAIEmbeddingClient(EmbeddingClient):
    """
    Embedding using Azure OpenAI text-embedding-ada-002.

    Higher quality than local model but requires Azure OpenAI endpoint + key.
    Use this when you need maximum similarity quality and have Azure access.

    Dimension: 1536 (larger than local model, stored separately in ChromaDB)

    Config: Set AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_KEY in .env
    """

    def __init__(self, endpoint: str, api_key: str, model: str = "text-embedding-ada-002"):
        self._endpoint = endpoint
        self._api_key = api_key
        self._model = model
        self._dim = 1536  # ada-002 output dimension

    @property
    def dimension(self) -> int:
        return self._dim

    async def embed(self, text: str) -> List[float]:
        return (await self.embed_batch([text]))[0]

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        try:
            import httpx
        except ImportError:
            raise ImportError("httpx is required. Install with: pip install httpx")

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self._endpoint}/openai/deployments/{self._model}/embeddings?api-version=2023-05-15",
                headers={"api-key": self._api_key, "Content-Type": "application/json"},
                json={"input": texts},
                timeout=30.0,
            )
            response.raise_for_status()
            data = response.json()
            return [item["embedding"] for item in data["data"]]


# =============================================================================
# Mock Implementation (for tests — zero dependencies)
# =============================================================================

class MockEmbeddingClient(EmbeddingClient):
    """
    Deterministic mock embedding for unit tests.

    Produces reproducible vectors based on text hash — no model download,
    no GPU, no internet. Fast enough for test suites.

    The vectors are NOT semantically meaningful but are consistent,
    so similarity searches in tests will produce deterministic results.
    """

    def __init__(self, dimension: int = 384):
        self._dim = dimension

    @property
    def dimension(self) -> int:
        return self._dim

    async def embed(self, text: str) -> List[float]:
        """Produce a deterministic vector from text hash."""
        import hashlib
        hash_bytes = hashlib.sha256(text.encode()).digest()
        # Expand to target dimension using repeated hashing
        extended = hash_bytes
        while len(extended) < self._dim * 4:
            extended += hashlib.sha256(extended).digest()
        # Convert bytes to floats in [-1, 1]
        floats = []
        for i in range(0, self._dim * 4, 4):
            val = int.from_bytes(extended[i:i+4], "big", signed=True)
            floats.append(val / (2**31))
        return floats[:self._dim]

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        results = []
        for text in texts:
            results.append(await self.embed(text))
        return results


# =============================================================================
# Factory
# =============================================================================

def get_embedding_client(
    azure_openai_endpoint: Optional[str] = None,
    azure_openai_key: Optional[str] = None,
    local_model: str = "all-MiniLM-L6-v2",
    use_mock: bool = False,
) -> EmbeddingClient:
    """
    Factory function: returns the best available embedding client.

    Selection order:
    1. MockEmbeddingClient   — if use_mock=True (for tests)
    2. AzureOpenAIEmbeddingClient — if Azure credentials provided
    3. LocalEmbeddingClient  — default (offline, no API key needed)

    Args:
        azure_openai_endpoint: Azure OpenAI endpoint URL (optional)
        azure_openai_key: Azure OpenAI API key (optional)
        local_model: Sentence-transformers model name (default: all-MiniLM-L6-v2)
        use_mock: Force mock client (for tests)

    Returns:
        EmbeddingClient instance ready to use.

    Example:
        embedder = get_embedding_client()  # local, offline
        vector = await embedder.embed("rename Total Revenue")
    """
    if use_mock:
        logger.debug("Using MockEmbeddingClient (test mode)")
        return MockEmbeddingClient()

    if azure_openai_endpoint and azure_openai_key:
        logger.info("Using AzureOpenAIEmbeddingClient (text-embedding-ada-002)")
        return AzureOpenAIEmbeddingClient(
            endpoint=azure_openai_endpoint,
            api_key=azure_openai_key,
        )

    logger.info(f"Using LocalEmbeddingClient (model: {local_model})")
    return LocalEmbeddingClient(model_name=local_model)
