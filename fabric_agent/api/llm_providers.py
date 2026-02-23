"""
llm_providers.py — Multi-Provider LLM Client System
====================================================

Three concrete LLMClient implementations so any org can run the system
without external API keys (Ollama default) or switch to Claude / Azure OpenAI
with a single .env change.

  OllamaLLMClient      — DEFAULT. Local open-source LLMs (zero API key, zero cost).
  ClaudeLLMClient      — Optional override: best reasoning quality.
  AzureOpenAILLMClient — Optional override: enterprise Azure tenants.

FAANG PARALLEL:
  This is the same provider-agnostic pattern used by:
  - LangChain BaseLLM: single interface, 50+ provider backends
  - LlamaIndex LLM class: swap OpenAI → Llama → Bedrock by config
  - Semantic Kernel ITextGenerationService: kernel-level abstraction
  - Microsoft Guidance: provider-agnostic constrained generation

  The key insight: callers (BaseAgent, OrchestratorLLMAgent) never import
  a specific provider. They receive an LLMClient via dependency injection.
  The factory `create_llm_client(config)` is the only place that knows
  which provider is running.

WHY OLLAMA-FIRST:
  Most enterprise orgs can't use external LLM APIs for sensitive data
  (PII, financials, internal measure names). Ollama runs entirely
  on-premises — data never leaves the org. Models like Llama 3.1 and
  Qwen 2.5 support full tool-calling at near-GPT-4 quality for code tasks.

RESPONSE NORMALIZATION:
  Ollama and Azure OpenAI use the OpenAI API format.
  Claude uses the Anthropic API format.
  BaseAgent expects Anthropic format (content blocks with type="text"|"tool_use").
  This module handles both directions of conversion transparently.

SETUP (Ollama — default):
  1. Install: https://ollama.ai → one-click installer
  2. Pull model: ollama pull llama3.1       (8B, tool-calling capable)
               ollama pull qwen2.5:14b      (14B, stronger reasoning)
  3. Verify: curl http://localhost:11434/v1/models

USAGE:
  from fabric_agent.api.llm_providers import create_llm_client
  from fabric_agent.core.config import AgentConfig

  config = AgentConfig()            # reads LLM_PROVIDER from .env
  llm = create_llm_client(config)   # returns OllamaLLMClient by default
  response = await llm.complete(messages=[...], tools=[...])

  # Switch to Claude: set LLM_PROVIDER=claude in .env
  # Switch to Azure:  set LLM_PROVIDER=azure_openai in .env
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from uuid import uuid4

import httpx
from loguru import logger

from fabric_agent.agents.base import LLMClient


# =============================================================================
# Message Format Converters
# =============================================================================
# BaseAgent uses Anthropic-format message history internally. Ollama and Azure
# OpenAI use the OpenAI API format. These helpers convert both ways so that
# each LLM client is a clean adapter with no conversion logic leaking into callers.

def _to_openai_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert Anthropic-format message history to OpenAI format.

    Anthropic format (what BaseAgent stores):
      assistant: {"role": "assistant", "content": [TextBlock|dict, ToolUseBlock|dict, ...]}
      tool results: {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "...", "content": "..."}]}

    OpenAI format (what Ollama/Azure OpenAI expect):
      assistant: {"role": "assistant", "content": "text", "tool_calls": [...]}
      tool results: {"role": "tool", "tool_call_id": "...", "content": "..."}
    """
    result: List[Dict[str, Any]] = []

    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")

        # Simple string content — compatible as-is
        if isinstance(content, str):
            result.append({"role": role, "content": content})
            continue

        # List of content blocks
        if isinstance(content, list):
            if role == "user":
                # Check if this is a list of tool results (Anthropic format)
                tool_results = [
                    b for b in content
                    if isinstance(b, dict) and b.get("type") == "tool_result"
                ]
                if tool_results:
                    # Convert each to a separate OpenAI tool message
                    for tr in tool_results:
                        result.append({
                            "role": "tool",
                            "tool_call_id": tr.get("tool_use_id", ""),
                            "content": str(tr.get("content", "")),
                        })
                else:
                    # Regular user content blocks → flatten to text
                    text_parts = []
                    for block in content:
                        text = _extract_text(block)
                        if text:
                            text_parts.append(text)
                    result.append({"role": "user", "content": "\n".join(text_parts)})

            elif role == "assistant":
                text_parts = []
                tool_calls = []

                for block in content:
                    block_type, text, tool_id, tool_name, tool_input = _parse_block(block)

                    if block_type == "text" and text:
                        text_parts.append(text)
                    elif block_type == "tool_use":
                        tool_calls.append({
                            "id": tool_id,
                            "type": "function",
                            "function": {
                                "name": tool_name,
                                "arguments": json.dumps(tool_input),
                            },
                        })

                openai_msg: Dict[str, Any] = {
                    "role": "assistant",
                    "content": "\n".join(text_parts) if text_parts else None,
                }
                if tool_calls:
                    openai_msg["tool_calls"] = tool_calls
                result.append(openai_msg)
        else:
            # Fallback: pass through unchanged
            result.append(msg)

    return result


def _to_openai_tools(tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert Anthropic tool schemas to OpenAI function calling format.

    Anthropic: {"name": "...", "description": "...", "input_schema": {...}}
    OpenAI:    {"type": "function", "function": {"name": "...", "description": "...", "parameters": {...}}}
    """
    result = []
    for tool in tools:
        result.append({
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool.get("description", ""),
                "parameters": tool.get("input_schema", {"type": "object", "properties": {}}),
            },
        })
    return result


def _from_openai_response(response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert an OpenAI-format chat completion response to Anthropic format.

    OpenAI response:
      choices[0].message.content: "text" | None
      choices[0].message.tool_calls: [{"id": "...", "function": {"name": "...", "arguments": "{...}"}}]
      choices[0].finish_reason: "stop" | "tool_calls" | "length"
      usage.prompt_tokens / completion_tokens

    Anthropic format (what BaseAgent expects):
      content: [{"type": "text", "text": "..."}, {"type": "tool_use", "id": "...", "name": "...", "input": {...}}]
      stop_reason: "end_turn" | "tool_use" | "max_tokens"
      usage: {"input_tokens": N, "output_tokens": N}
    """
    choices = response.get("choices", [{}])
    choice = choices[0] if choices else {}
    message = choice.get("message", {})
    finish_reason = choice.get("finish_reason", "stop")
    usage = response.get("usage", {})

    content: List[Dict[str, Any]] = []

    # Text content
    text = message.get("content")
    if text:
        content.append({"type": "text", "text": text})

    # Tool calls
    for tc in message.get("tool_calls") or []:
        func = tc.get("function", {})
        try:
            input_data = json.loads(func.get("arguments", "{}"))
        except (json.JSONDecodeError, TypeError):
            input_data = {}

        content.append({
            "type": "tool_use",
            "id": tc.get("id", str(uuid4())),
            "name": func.get("name", ""),
            "input": input_data,
        })

    # Map finish_reason → stop_reason
    _finish_map = {
        "stop": "end_turn",
        "tool_calls": "tool_use",
        "length": "max_tokens",
    }
    stop_reason = _finish_map.get(finish_reason, "end_turn")

    return {
        "content": content,
        "stop_reason": stop_reason,
        "usage": {
            "input_tokens": usage.get("prompt_tokens", 0),
            "output_tokens": usage.get("completion_tokens", 0),
        },
    }


def _extract_text(block: Any) -> str:
    """Extract text string from a content block (dict or Anthropic object)."""
    if isinstance(block, dict):
        return block.get("text", "") if block.get("type") == "text" else ""
    if hasattr(block, "type") and block.type == "text":
        return getattr(block, "text", "")
    return ""


def _parse_block(block: Any):
    """
    Parse a content block into (type, text, tool_id, tool_name, tool_input).
    Handles both dict format and Anthropic API object format.
    """
    if isinstance(block, dict):
        btype = block.get("type", "")
        return (
            btype,
            block.get("text", "") if btype == "text" else "",
            block.get("id", ""),
            block.get("name", ""),
            block.get("input", {}),
        )
    elif hasattr(block, "type"):
        btype = block.type
        if btype == "text":
            return btype, getattr(block, "text", ""), "", "", {}
        elif btype == "tool_use":
            return (
                btype, "",
                getattr(block, "id", ""),
                getattr(block, "name", ""),
                getattr(block, "input", {}),
            )
    return ("", "", "", "", {})


# =============================================================================
# Ollama LLM Client (DEFAULT)
# =============================================================================

class OllamaLLMClient(LLMClient):
    """
    Local open-source LLMs via Ollama — the default provider.

    WHAT: Calls Ollama's OpenAI-compatible REST API at /v1/chat/completions.
          Supports tool calling with Llama 3.1, Mistral, Qwen 2.5, Phi-3.5.

    WHY OLLAMA-FIRST:
      - Zero API keys required — anyone can clone and run immediately
      - Data never leaves the machine — critical for regulated industries
        (PII, financial data, internal business logic)
      - Free for any org size, no rate limits
      - Supports full tool calling (function calling) with capable models

    FAANG PARALLEL:
      Meta runs Llama internally across their entire engineering org —
      same principle: full LLM capability with zero external dependency.
      Uber's internal LLM platform uses locally-hosted models for
      latency-sensitive pipelines.

    TOOL CALLING MODELS (recommended):
      llama3.1      — 8B, great tool calling, fast on CPU+GPU
      llama3.1:70b  — 70B, near-GPT-4 quality, needs GPU
      qwen2.5:14b   — 14B, excellent coding, tool calling
      mistral       — 7B, fast, reliable tool calling
      phi3.5        — 3.8B, runs on laptop CPU, decent quality

    SETUP:
      1. Install Ollama: https://ollama.ai (Windows/Mac/Linux)
      2. Pull a model:   ollama pull llama3.1
      3. Start server:   ollama serve   (usually auto-started)
      4. Verify:         curl http://localhost:11434/v1/models

    PARAMETERS:
      base_url: Ollama server URL (default: http://localhost:11434)
      model:    Model name (default: llama3.1)
      timeout:  HTTP timeout in seconds (default: 120 — local models are slow)
    """

    def __init__(
        self,
        base_url: str = "http://localhost:11434",
        model: str = "llama3.1",
        timeout: float = 120.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout
        self._endpoint = f"{self.base_url}/v1/chat/completions"
        logger.debug(f"OllamaLLMClient created: model={model}, endpoint={self._endpoint}")

    async def complete(
        self,
        messages: List[Dict[str, Any]],
        tools: Optional[List[Dict[str, Any]]] = None,
        system_prompt: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Send a chat completion request to Ollama.

        Converts Anthropic-format messages/tools → OpenAI format → sends to Ollama
        → converts response back to Anthropic format for BaseAgent compatibility.

        Args:
            messages:      Chat history in Anthropic format.
            tools:         Tool schemas in Anthropic format (input_schema key).
            system_prompt: System instruction injected as the first message.

        Returns:
            Response dict in Anthropic format:
              {"content": [...], "stop_reason": "end_turn"|"tool_use", "usage": {...}}

        Raises:
            RuntimeError: If Ollama is not running or returns an error.
        """
        # Build message list in OpenAI format
        openai_messages: List[Dict[str, Any]] = []

        if system_prompt:
            openai_messages.append({"role": "system", "content": system_prompt})

        openai_messages.extend(_to_openai_messages(messages))

        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": openai_messages,
            "stream": False,
        }

        if tools:
            payload["tools"] = _to_openai_tools(tools)
            payload["tool_choice"] = "auto"

        logger.debug(f"OllamaLLMClient: POST {self._endpoint} model={self.model}")

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                resp = await client.post(
                    self._endpoint,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
            except httpx.ConnectError as exc:
                raise RuntimeError(
                    f"Cannot connect to Ollama at {self.base_url}. "
                    "Is Ollama running? Run: ollama serve"
                ) from exc
            except httpx.TimeoutException as exc:
                raise RuntimeError(
                    f"Ollama request timed out after {self.timeout}s. "
                    "Try a smaller model or increase timeout."
                ) from exc

        if resp.status_code != 200:
            raise RuntimeError(
                f"Ollama returned HTTP {resp.status_code}: {resp.text[:500]}"
            )

        raw = resp.json()
        result = _from_openai_response(raw)
        logger.debug(
            f"OllamaLLMClient: stop_reason={result['stop_reason']}, "
            f"content_blocks={len(result['content'])}"
        )
        return result


# =============================================================================
# Azure OpenAI LLM Client (enterprise optional)
# =============================================================================

class AzureOpenAILLMClient(LLMClient):
    """
    Azure OpenAI Service — for enterprises already on Azure.

    WHAT: Uses the `openai` Python SDK with Azure-specific configuration.
          Supports GPT-4o, GPT-4o-mini, and other Azure-deployed models.

    WHY AZURE OPENAI vs. OPENAI DIRECT:
      - Enterprises already have Azure agreements → no new vendor
      - Data residency guarantees (data stays in your Azure region)
      - Same DefaultAzureCredential chain as FabricApiClient
      - SLA and compliance (SOC 2, ISO 27001, HIPAA)
      - Can use managed identity — no API key needed

    FAANG PARALLEL:
      Microsoft's own internal use: GitHub Copilot uses Azure OpenAI.
      Most Fortune 500 AI deployments: Azure OpenAI for data sovereignty.

    AUTHENTICATION:
      Option A (API key): Set AZURE_OPENAI_API_KEY in .env
      Option B (managed identity): Leave api_key=None → uses DefaultAzureCredential
                                   → works in Fabric notebooks automatically

    PARAMETERS:
      endpoint:    Azure OpenAI endpoint (e.g., https://my-resource.openai.azure.com)
      deployment:  Deployment name (not model name) — set in Azure portal
      api_version: API version (default: 2024-10-01-preview)
      api_key:     Optional API key (uses DefaultAzureCredential if None)
    """

    def __init__(
        self,
        endpoint: str,
        deployment: str = "gpt-4o",
        api_version: str = "2024-10-01-preview",
        api_key: Optional[str] = None,
    ):
        self.endpoint = endpoint.rstrip("/")
        self.deployment = deployment
        self.api_version = api_version
        self.api_key = api_key
        self._client = None
        logger.debug(f"AzureOpenAILLMClient: endpoint={endpoint}, deployment={deployment}")

    def _get_client(self):
        """Lazy-initialize the AzureOpenAI client."""
        if self._client is not None:
            return self._client

        try:
            from openai import AsyncAzureOpenAI
        except ImportError:
            raise RuntimeError(
                "openai package not installed. "
                "Install: pip install openai"
            )

        if self.api_key:
            self._client = AsyncAzureOpenAI(
                azure_endpoint=self.endpoint,
                api_key=self.api_key,
                api_version=self.api_version,
            )
        else:
            # Use Azure AD token via DefaultAzureCredential
            try:
                from azure.identity import DefaultAzureCredential, get_bearer_token_provider
                token_provider = get_bearer_token_provider(
                    DefaultAzureCredential(),
                    "https://cognitiveservices.azure.com/.default",
                )
                self._client = AsyncAzureOpenAI(
                    azure_endpoint=self.endpoint,
                    azure_ad_token_provider=token_provider,
                    api_version=self.api_version,
                )
            except ImportError:
                raise RuntimeError(
                    "azure-identity package not installed. "
                    "Install: pip install azure-identity"
                )

        return self._client

    async def complete(
        self,
        messages: List[Dict[str, Any]],
        tools: Optional[List[Dict[str, Any]]] = None,
        system_prompt: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Send a chat completion request to Azure OpenAI.

        Same message normalization as OllamaLLMClient — Anthropic format in,
        Anthropic format out. The caller never knows the difference.

        Args:
            messages:      Chat history in Anthropic format.
            tools:         Tool schemas in Anthropic format.
            system_prompt: System instruction.

        Returns:
            Response dict in Anthropic format.

        Raises:
            RuntimeError: If openai package is missing or API call fails.
        """
        client = self._get_client()

        openai_messages: List[Dict[str, Any]] = []
        if system_prompt:
            openai_messages.append({"role": "system", "content": system_prompt})
        openai_messages.extend(_to_openai_messages(messages))

        kwargs: Dict[str, Any] = {
            "model": self.deployment,
            "messages": openai_messages,
        }
        if tools:
            kwargs["tools"] = _to_openai_tools(tools)
            kwargs["tool_choice"] = "auto"

        logger.debug(f"AzureOpenAILLMClient: deployment={self.deployment}")

        try:
            response = await client.chat.completions.create(**kwargs)
        except Exception as exc:
            raise RuntimeError(f"Azure OpenAI request failed: {exc}") from exc

        # Convert SDK response object to dict for _from_openai_response
        raw = response.model_dump()
        result = _from_openai_response(raw)
        logger.debug(
            f"AzureOpenAILLMClient: stop_reason={result['stop_reason']}, "
            f"content_blocks={len(result['content'])}"
        )
        return result


# =============================================================================
# Factory
# =============================================================================

def create_llm_client(config: Any) -> LLMClient:
    """
    Factory function — returns the correct LLMClient based on config.

    WHAT: Single entry point for all LLM provider construction. Callers
          never import OllamaLLMClient or ClaudeLLMClient directly.

    WHY FACTORY PATTERN:
      - Config is the single source of truth for which provider to use
      - Adding a new provider requires one elif here — zero changes to callers
      - Unit tests can inject MockLLMClient without touching factory at all

    FAANG PARALLEL:
      - AWS SDK provider chain (S3, DynamoDB adapters via same interface)
      - Google Cloud ADC (Application Default Credentials) factory
      - Spring's @Bean factory methods for dependency injection

    PROVIDER SELECTION (via LLM_PROVIDER env var):
      "ollama"       → OllamaLLMClient  (DEFAULT — no API key needed)
      "claude"       → ClaudeLLMClient  (best reasoning, needs ANTHROPIC_API_KEY)
      "azure_openai" → AzureOpenAILLMClient (enterprise Azure, needs endpoint)

    Args:
        config: AgentConfig instance with llm_provider + provider-specific fields.

    Returns:
        Configured LLMClient ready to call.

    Raises:
        ValueError: If provider is unknown.
        RuntimeError: If required config fields are missing for the provider.

    Example:
        config = AgentConfig()             # LLM_PROVIDER=ollama in .env
        llm = create_llm_client(config)    # → OllamaLLMClient(model="llama3.1")
    """
    provider = getattr(config, "llm_provider", "ollama")

    if provider == "ollama":
        model = getattr(config, "ollama_model", "llama3.1")
        base_url = getattr(config, "ollama_base_url", "http://localhost:11434")
        logger.info(f"LLM provider: Ollama (model={model}, url={base_url})")
        return OllamaLLMClient(base_url=base_url, model=model)

    elif provider == "claude":
        from fabric_agent.agents.base import ClaudeLLMClient

        api_key_secret = getattr(config, "anthropic_api_key", None)
        if api_key_secret is None:
            raise RuntimeError(
                "LLM_PROVIDER=claude but ANTHROPIC_API_KEY is not set. "
                "Add it to .env or switch to LLM_PROVIDER=ollama."
            )
        api_key = (
            api_key_secret.get_secret_value()
            if hasattr(api_key_secret, "get_secret_value")
            else str(api_key_secret)
        )
        model = getattr(config, "anthropic_model", "claude-sonnet-4-6")
        logger.info(f"LLM provider: Claude (model={model})")
        return ClaudeLLMClient(api_key=api_key, model=model)

    elif provider == "azure_openai":
        endpoint = getattr(config, "azure_openai_endpoint", None)
        if not endpoint:
            raise RuntimeError(
                "LLM_PROVIDER=azure_openai but AZURE_OPENAI_ENDPOINT is not set."
            )
        deployment = getattr(config, "azure_openai_deployment", "gpt-4o")
        api_key_secret = getattr(config, "azure_openai_api_key", None)
        api_key = None
        if api_key_secret:
            api_key = (
                api_key_secret.get_secret_value()
                if hasattr(api_key_secret, "get_secret_value")
                else str(api_key_secret)
            )
        logger.info(f"LLM provider: Azure OpenAI (deployment={deployment})")
        return AzureOpenAILLMClient(
            endpoint=endpoint,
            deployment=deployment,
            api_key=api_key,
        )

    else:
        raise ValueError(
            f"Unknown LLM provider: {provider!r}. "
            "Valid options: 'ollama', 'claude', 'azure_openai'"
        )
