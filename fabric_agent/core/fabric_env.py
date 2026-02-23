"""
Fabric Environment Detection & Path Resolution
===============================================

WHAT: Detects whether the code is running inside a Microsoft Fabric Notebook
      or in a local development environment, and resolves file paths accordingly.

WHY: The same Python package needs to run in two contexts:
     1. Local dev: paths are relative to the repo root (data/, etc.)
     2. Fabric Notebook: paths use the OneLake mount (/lakehouse/default/Files/)

     Without this abstraction, you'd have two codebases. With it, one codebase
     runs everywhere — the environment figures itself out.

FAANG PARALLEL:
     - Meta's PyTorch: automatically detects CUDA vs CPU
     - Google's TF: detects TPU vs GPU vs CPU
     - Airbnb's Airflow: detects local vs Kubernetes executor
     This is the same "environment-adaptive" pattern applied to data infra.

HOW IT WORKS:
     1. Try to import `notebookutils` — only available inside Fabric
     2. If import succeeds → we're in Fabric → use /lakehouse/ paths
     3. If import fails → we're local → use repo-relative paths
     4. The `FabricEnvironment` singleton caches the detection result

USAGE:
     from fabric_agent.core.fabric_env import get_env

     env = get_env()
     vector_store_path = env.vector_store_path()   # auto-correct for both envs
     health_log_path   = env.health_log_table()    # Delta table path

     # Or use convenience functions:
     from fabric_agent.core.fabric_env import is_fabric_notebook, get_lakehouse_path
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

from loguru import logger


# =============================================================================
# Core Detection
# =============================================================================

@lru_cache(maxsize=1)
def is_fabric_notebook() -> bool:
    """
    Detect if code is running inside a Microsoft Fabric Notebook.

    Fabric Notebooks expose `notebookutils` (also `mssparkutils`) as a
    built-in — it's never available in local Python environments.

    Returns:
        True if inside Fabric Notebook, False otherwise.

    Example:
        >>> if is_fabric_notebook():
        ...     print("Running in Fabric — use OneLake paths")
        ... else:
        ...     print("Running locally — use repo paths")
    """
    try:
        import notebookutils  # noqa: F401  — Fabric-only built-in
        return True
    except ImportError:
        pass

    try:
        import mssparkutils  # noqa: F401  — alternative Fabric built-in
        return True
    except ImportError:
        pass

    # Also check env var (useful for CI/CD that mocks Fabric)
    return os.environ.get("FABRIC_NOTEBOOK_ENV", "").lower() == "true"


def is_spark_available() -> bool:
    """Check if PySpark is available (Fabric Notebooks always have it)."""
    try:
        import pyspark  # noqa: F401
        return True
    except ImportError:
        return False


# =============================================================================
# Path Resolution
# =============================================================================

class FabricEnvironment:
    """
    Unified environment abstraction for local dev and Fabric Notebooks.

    Resolves all storage paths correctly regardless of where code runs.
    Use get_env() to get the singleton instance.

    Architecture:
        Local:  data/vector_store/        → ChromaDB
                data/fabric_agent.db      → SQLite audit trail
                data/health_logs/         → JSON health logs

        Fabric: /lakehouse/default/Files/vector_store/  → ChromaDB (persistent)
                /lakehouse/default/Files/fabric_agent.db → SQLite
                /lakehouse/default/Tables/health_log     → Delta table (queryable!)

    The Delta table path is key — in Fabric, health logs are a proper
    queryable Delta table, enabling Power BI reports on top of them.
    """

    def __init__(
        self,
        lakehouse_name: str = "default",
        local_data_root: Optional[Path] = None,
    ):
        self._is_fabric = is_fabric_notebook()
        self._lakehouse_name = lakehouse_name
        self._local_root = local_data_root or (Path(__file__).resolve().parents[2] / "data")

        logger.info(
            f"FabricEnvironment initialized: "
            f"{'Fabric Notebook' if self._is_fabric else 'Local Dev'}"
        )

    @property
    def is_fabric(self) -> bool:
        """True if running inside Microsoft Fabric."""
        return self._is_fabric

    @property
    def environment_name(self) -> str:
        """Human-readable environment name."""
        return "Microsoft Fabric" if self._is_fabric else "Local Development"

    # -------------------------------------------------------------------------
    # Storage Paths
    # -------------------------------------------------------------------------

    def files_root(self) -> Path:
        """
        Root path for file storage.

        Fabric:  /lakehouse/{name}/Files/
        Local:   data/
        """
        if self._is_fabric:
            return Path(f"/lakehouse/{self._lakehouse_name}/Files")
        return self._local_root

    def vector_store_path(self) -> str:
        """
        Path for ChromaDB vector store persistence.

        ChromaDB's PersistentClient accepts this path directly.

        Fabric:  /lakehouse/default/Files/vector_store
        Local:   data/vector_store
        """
        return str(self.files_root() / "vector_store")

    def sqlite_db_path(self) -> Path:
        """
        Path for the SQLite audit trail database.

        Fabric:  /lakehouse/default/Files/fabric_agent.db
        Local:   data/fabric_agent.db
        """
        return self.files_root() / "fabric_agent.db"

    def wheels_path(self) -> Path:
        """
        Path where Python wheels are stored for Fabric Environments.

        Fabric:  /lakehouse/default/Files/wheels/
        Local:   data/wheels/
        """
        path = self.files_root() / "wheels"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def session_store_path(self) -> Path:
        """
        Path for cross-session context storage.

        Fabric:  /lakehouse/default/Files/sessions/
        Local:   data/sessions/
        """
        path = self.files_root() / "sessions"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def health_logs_path(self) -> Path:
        """
        Path for health report JSON logs (local fallback).

        In Fabric, use health_log_delta_table() instead for queryable Delta.
        """
        path = self.files_root() / "health_logs"
        path.mkdir(parents=True, exist_ok=True)
        return path

    # -------------------------------------------------------------------------
    # Fabric-specific: Delta Table paths
    # -------------------------------------------------------------------------

    def health_log_delta_table(self) -> Optional[str]:
        """
        Delta table path for health reports (Fabric only).

        In Fabric, writing to /lakehouse/default/Tables/ creates a proper
        Delta table that is immediately queryable via PySpark and visible
        as a lakehouse table in the Fabric UI.

        Returns:
            Delta table path if in Fabric, None if local.

        Example (Spark write in Fabric Notebook):
            path = env.health_log_delta_table()
            if path:
                df.write.format("delta").mode("append").save(path)
        """
        if not self._is_fabric:
            return None
        return f"/lakehouse/{self._lakehouse_name}/Tables/fabric_agent_health_log"

    def vector_store_delta_table(self) -> Optional[str]:
        """
        Delta table path for vector store embeddings (Fabric only).

        Used as an alternative to ChromaDB when running at Spark scale.
        """
        if not self._is_fabric:
            return None
        return f"/lakehouse/{self._lakehouse_name}/Tables/fabric_agent_embeddings"

    # -------------------------------------------------------------------------
    # Utility
    # -------------------------------------------------------------------------

    def ensure_directories(self) -> None:
        """Create all required local directories if they don't exist."""
        if not self._is_fabric:
            for path in [
                self._local_root,
                self._local_root / "vector_store",
                self._local_root / "wheels",
                self._local_root / "sessions",
                self._local_root / "health_logs",
            ]:
                path.mkdir(parents=True, exist_ok=True)

    def describe(self) -> dict:
        """Return a description of the current environment for logging/debugging."""
        return {
            "environment": self.environment_name,
            "is_fabric": self.is_fabric,
            "is_spark_available": is_spark_available(),
            "files_root": str(self.files_root()),
            "vector_store_path": self.vector_store_path(),
            "sqlite_db_path": str(self.sqlite_db_path()),
            "health_log_delta_table": self.health_log_delta_table(),
        }


# =============================================================================
# Singleton Access
# =============================================================================

_env_instance: Optional[FabricEnvironment] = None


def get_env(
    lakehouse_name: str = "default",
    local_data_root: Optional[Path] = None,
) -> FabricEnvironment:
    """
    Get the global FabricEnvironment singleton.

    Call this from anywhere — it caches after first call.

    Args:
        lakehouse_name: Name of the Fabric lakehouse to mount paths under.
                        Defaults to "default" (the notebook's default lakehouse).
        local_data_root: Override the local data root path (useful for tests).

    Returns:
        FabricEnvironment instance.

    Example:
        >>> from fabric_agent.core.fabric_env import get_env
        >>> env = get_env()
        >>> env.vector_store_path()
        'data/vector_store'   # local
        '/lakehouse/default/Files/vector_store'  # Fabric
    """
    global _env_instance
    if _env_instance is None:
        _env_instance = FabricEnvironment(
            lakehouse_name=lakehouse_name,
            local_data_root=local_data_root,
        )
        _env_instance.ensure_directories()
    return _env_instance


def reset_env() -> None:
    """Reset the singleton (used in tests to switch between local/fabric modes)."""
    global _env_instance
    _env_instance = None
    is_fabric_notebook.cache_clear()


# =============================================================================
# Convenience Functions
# =============================================================================

def get_lakehouse_path(subdirectory: str = "", lakehouse_name: str = "default") -> str:
    """
    Get the correct base path for the given subdirectory.

    Convenience wrapper around FabricEnvironment.files_root().

    Args:
        subdirectory: Sub-path within Files/ (e.g., "vector_store", "wheels")
        lakehouse_name: Which lakehouse to use

    Returns:
        Absolute path string, correct for the current environment.

    Example:
        >>> get_lakehouse_path("vector_store")
        'data/vector_store'              # local
        '/lakehouse/default/Files/vector_store'  # Fabric
    """
    env = get_env(lakehouse_name=lakehouse_name)
    base = env.files_root()
    if subdirectory:
        return str(base / subdirectory)
    return str(base)
