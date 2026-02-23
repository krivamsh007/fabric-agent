"""
Configuration Management
========================

Pydantic-based configuration models for the Fabric Agent.
Supports environment variables, JSON config files, and programmatic configuration.
"""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import Optional, List

from typing import Literal

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings
from loguru import logger
from pathlib import Path

def _try_load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except Exception:
        return

    # Try repo root relative to this file
    repo_root = Path(__file__).resolve().parents[2]  # fabric_agent/core/config.py -> repo root
    load_dotenv(repo_root / ".env", override=False)

    # Also try current working directory (when running from repo root)
    load_dotenv(Path.cwd() / ".env", override=False)

_try_load_dotenv()


class AuthMode(str, Enum):
    """Authentication mode for Azure/Fabric APIs."""
    
    INTERACTIVE = "interactive"
    """Browser-based interactive authentication."""
    
    SERVICE_PRINCIPAL = "service_principal"
    """App-only authentication with client secret."""
    
    DEFAULT = "default"
    """DefaultAzureCredential chain (CLI, VS Code, managed identity, etc.)."""


class StorageBackend(str, Enum):
    """Backend for the MemoryManager audit trail."""
    
    SQLITE = "sqlite"
    """SQLite database (recommended for production)."""
    
    JSON = "json"
    """JSON file (simpler, good for development)."""


class FabricAuthConfig(BaseModel):
    """
    Azure/Fabric authentication configuration.
    
    Attributes:
        tenant_id: Azure AD tenant ID.
        client_id: Azure AD application (client) ID.
        client_secret: Client secret for service principal auth (optional).
        auth_mode: Authentication mode to use.
        scopes: OAuth scopes for Fabric API access.
    
    Example:
        >>> config = FabricAuthConfig(
        ...     tenant_id="your-tenant-id",
        ...     client_id="your-client-id",
        ...     auth_mode=AuthMode.INTERACTIVE
        ... )
    """
    
    tenant_id: str = Field(
        ...,
        description="Azure AD tenant ID",
        min_length=36,
        max_length=36,
    )
    client_id: str = Field(
        ...,
        description="Azure AD application (client) ID",
        min_length=36,
        max_length=36,
    )
    client_secret: Optional[SecretStr] = Field(
        default=None,
        description="Client secret for service principal authentication",
    )
    auth_mode: AuthMode = Field(
        default=AuthMode.INTERACTIVE,
        description="Authentication mode",
    )
    scopes: List[str] = Field(
        default_factory=lambda: ["https://api.fabric.microsoft.com/Workspace.Read.All"],
        description="OAuth scopes for Fabric API",
    )
    
    @model_validator(mode="after")
    def validate_auth_requirements(self) -> "FabricAuthConfig":
        """Validate that service principal auth has a client secret."""
        if self.auth_mode == AuthMode.SERVICE_PRINCIPAL and not self.client_secret:
            raise ValueError(
                "client_secret is required when using SERVICE_PRINCIPAL auth mode"
            )
        return self
    
    @classmethod
    def from_env(cls) -> "FabricAuthConfig":
        """
        Load configuration from environment variables.
        
        Environment Variables:
            AZURE_TENANT_ID: Required
            AZURE_CLIENT_ID: Required
            AZURE_CLIENT_SECRET: Optional
            USE_INTERACTIVE_AUTH: If 'true', use interactive auth (default: true)
            FABRIC_SCOPES: Space-separated list of scopes
        
        Returns:
            FabricAuthConfig instance.
        
        Raises:
            ValueError: If required environment variables are missing.
        """
        tenant_id = os.getenv("AZURE_TENANT_ID", "").strip()
        client_id = os.getenv("AZURE_CLIENT_ID", "").strip()
        client_secret = os.getenv("AZURE_CLIENT_SECRET", "").strip() or None
        
        if not tenant_id or not client_id:
            raise ValueError(
                "AZURE_TENANT_ID and AZURE_CLIENT_ID environment variables are required"
            )
        
        # Determine auth mode
        use_interactive = os.getenv("USE_INTERACTIVE_AUTH", "true").lower() in {
            "1", "true", "yes", "y", "on"
        }
        
        if use_interactive:
            auth_mode = AuthMode.INTERACTIVE
        elif client_secret:
            auth_mode = AuthMode.SERVICE_PRINCIPAL
        else:
            auth_mode = AuthMode.DEFAULT
        
        # Parse scopes
        scopes_raw = os.getenv(
            "FABRIC_SCOPES",
            "https://api.fabric.microsoft.com/Workspace.Read.All"
        )
        scopes = [s.strip() for s in scopes_raw.split() if s.strip()]
        
        return cls(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=SecretStr(client_secret) if client_secret else None,
            auth_mode=auth_mode,
            scopes=scopes,
        )


class AgentConfig(BaseSettings):
    """
    Main configuration for the Fabric Agent.
    
    Attributes:
        auth: Authentication configuration.
        storage_backend: Backend for audit trail storage.
        storage_path: Path to storage file (SQLite or JSON).
        fabric_base_url: Base URL for Fabric REST API.
        api_version: Fabric API version prefix.
        timeout_seconds: HTTP request timeout.
        log_level: Logging level.
        enable_audit_trail: Whether to track state changes.
        max_rollback_states: Maximum number of states to keep for rollback.
    
    Example:
        >>> config = AgentConfig(
        ...     auth=FabricAuthConfig.from_env(),
        ...     storage_backend=StorageBackend.SQLITE,
        ...     log_level="DEBUG"
        ... )
    """
    
    auth: Optional[FabricAuthConfig] = Field(
        default=None,
        description="Authentication configuration (loaded from env if not provided)",
    )
    storage_backend: StorageBackend = Field(
        default=StorageBackend.SQLITE,
        description="Storage backend for audit trail",
    )
    storage_path: Path = Field(
        default=Path("./data/fabric_agent.db"),
        description="Path to storage file",
    )
    fabric_base_url: str = Field(
        default="https://api.fabric.microsoft.com",
        description="Base URL for Fabric REST API",
    )
    api_version: str = Field(
        default="/v1",
        description="Fabric API version prefix",
    )
    timeout_seconds: int = Field(
        default=30,
        description="HTTP request timeout in seconds",
        ge=1,
        le=300,
    )
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    enable_audit_trail: bool = Field(
        default=True,
        description="Enable state tracking and audit trail",
    )
    max_rollback_states: int = Field(
        default=100,
        description="Maximum number of states to retain for rollback",
        ge=1,
        le=10000,
    )

    # -------------------------------------------------------------------------
    # LLM Provider Configuration
    # -------------------------------------------------------------------------
    llm_provider: Literal["ollama", "claude", "azure_openai"] = Field(
        default="ollama",
        description=(
            "LLM provider to use for agent reasoning. "
            "'ollama' (default) runs fully locally — no API key needed. "
            "'claude' uses Anthropic API (best quality). "
            "'azure_openai' uses Azure OpenAI (enterprise)."
        ),
    )

    # Ollama settings (default provider — works out of the box)
    ollama_base_url: str = Field(
        default="http://localhost:11434",
        description="Ollama server base URL. Change if running Ollama on a remote host.",
    )
    ollama_model: str = Field(
        default="llama3.1",
        description=(
            "Ollama model name. Tool-calling capable models: "
            "llama3.1, qwen2.5:14b, mistral, phi3.5"
        ),
    )

    # Claude / Anthropic settings (optional override)
    anthropic_api_key: Optional[SecretStr] = Field(
        default=None,
        description="Anthropic API key. Required when llm_provider='claude'.",
    )
    anthropic_model: str = Field(
        default="claude-sonnet-4-6",
        description="Anthropic model ID. Default: claude-sonnet-4-6 (latest Sonnet).",
    )

    # Azure OpenAI settings (enterprise optional override)
    azure_openai_endpoint: Optional[str] = Field(
        default=None,
        description=(
            "Azure OpenAI endpoint URL. "
            "Required when llm_provider='azure_openai'. "
            "Example: https://my-resource.openai.azure.com"
        ),
    )
    azure_openai_deployment: str = Field(
        default="gpt-4o",
        description="Azure OpenAI deployment name (set in Azure portal).",
    )
    azure_openai_api_key: Optional[SecretStr] = Field(
        default=None,
        description=(
            "Azure OpenAI API key. Optional — if not set, uses DefaultAzureCredential "
            "(managed identity / az login). Leave empty in Fabric notebooks."
        ),
    )

    # -------------------------------------------------------------------------
    # Resilience Configuration
    # -------------------------------------------------------------------------
    max_retries: int = Field(
        default=3,
        description=(
            "Maximum retry attempts for transient Fabric API failures "
            "(429, 500/502/503/504). Uses exponential backoff with full jitter."
        ),
        ge=0,
        le=10,
    )
    circuit_breaker_threshold: int = Field(
        default=5,
        description=(
            "Number of consecutive failures before the circuit breaker opens. "
            "Once open, all calls fast-fail immediately until reset_timeout expires."
        ),
        ge=1,
        le=50,
    )
    circuit_breaker_reset_seconds: float = Field(
        default=60.0,
        description=(
            "Seconds to wait in OPEN state before allowing a probe request. "
            "If the probe succeeds, the circuit closes. If it fails, it resets the timer."
        ),
        ge=5.0,
        le=3600.0,
    )

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is a valid logging level."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return v_upper
    
    @field_validator("storage_path")
    @classmethod
    def ensure_storage_dir(cls, v: Path) -> Path:
        """Ensure the storage directory exists."""
        v.parent.mkdir(parents=True, exist_ok=True)
        return v
    
    def load_auth_from_env(self) -> "AgentConfig":
        """
        Load auth configuration from environment if not already set.
        
        Returns:
            Self with auth loaded.
        """
        if self.auth is None:
            try:
                self.auth = FabricAuthConfig.from_env()
                logger.info("Loaded authentication config from environment")
            except ValueError as e:
                logger.warning(f"Could not load auth from environment: {e}")
        return self
    
    class Config:
        env_prefix = "FABRIC_AGENT_"
        env_nested_delimiter = "__"
