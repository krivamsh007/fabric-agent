"""
fabric_client.py

Async + Sync Microsoft Fabric REST clients with:
- Azure AD auth (Service Principal / Azure CLI / DefaultAzureCredential)
- Automatic token refresh
- Long Running Operation (LRO) helper:
    * Prefer x-ms-operation-id when available
    * Extract opId from Location if needed
    * Poll /operations/{opId} on api.fabric.microsoft.com
    * If /result returns 400/404 (no result endpoint), return final operation payload instead
"""

from __future__ import annotations

import asyncio
import re
import time
from typing import Any, Dict, Optional, Union

import httpx
from loguru import logger

from fabric_agent.core.config import FabricAuthConfig, AuthMode
from fabric_agent.api.resilience import RetryPolicy, CircuitBreaker, CircuitOpenError


# =============================================================================
# Exceptions
# =============================================================================

class LROTimeoutError(Exception):
    """Raised when a Long Running Operation times out."""


class FabricApiError(Exception):
    """Raised when a Fabric API call fails."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_body: Optional[Any] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


# =============================================================================
# Response wrapper (dict-like + httpx style fields)
# =============================================================================

class FabricResponse(dict):
    """
    Dict-like response that also exposes httpx-style fields.
    Supports:
      - dict access: resp.get("id")
      - httpx access: resp.status_code, resp.content, resp.json()
    """

    def __init__(self, raw: httpx.Response):
        self.raw = raw

        data: Dict[str, Any] = {}
        if raw.content:
            try:
                parsed = raw.json()
            except Exception:
                parsed = {}

            if parsed is None:
                data = {}
            elif isinstance(parsed, dict):
                data = parsed
            elif isinstance(parsed, list):
                data = {"value": parsed}
            else:
                data = {"value": parsed}

        super().__init__(data)

    @property
    def status_code(self) -> int:
        return self.raw.status_code

    @property
    def headers(self) -> httpx.Headers:
        return self.raw.headers

    @property
    def content(self) -> bytes:
        return self.raw.content

    @property
    def text(self) -> str:
        return self.raw.text

    def json(self) -> Dict[str, Any]:
        return dict(self)

    def raise_for_status(self) -> None:
        self.raw.raise_for_status()


# =============================================================================
# Async client
# =============================================================================

class FabricApiClient:
    """
    Async REST client for Microsoft Fabric APIs.
    """

    def __init__(
        self,
        auth_config: FabricAuthConfig,
        base_url: str = "https://api.fabric.microsoft.com",
        api_version: str = "/v1",
        timeout_seconds: int = 30,
        max_retries: int = 3,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_reset_seconds: float = 60.0,
    ):
        self.auth_config = auth_config
        self.base_url = base_url.rstrip("/") + api_version
        self.timeout = httpx.Timeout(timeout_seconds)

        self._token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._credential: Any = None
        self._client: Optional[httpx.AsyncClient] = None
        self._initialized = False

        # Resilience primitives — every HTTP call goes through both.
        # RetryPolicy:    backs off on 429/5xx with full jitter.
        # CircuitBreaker: stops hammering a degraded Fabric API after
        #                 `circuit_breaker_threshold` consecutive failures.
        self._retry_policy = RetryPolicy(max_retries=max_retries)
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=circuit_breaker_threshold,
            reset_timeout=circuit_breaker_reset_seconds,
        )

        logger.debug(f"FabricApiClient created for {self.base_url}")

    async def initialize(self) -> None:
        if self._initialized:
            return

        logger.info("Initializing Fabric API client")
        self._credential = await self._get_credential()
        await self._refresh_token()

        self._client = httpx.AsyncClient(
            timeout=self.timeout,
            headers={"Content-Type": "application/json"},
        )
        self._initialized = True
        logger.info("Fabric API client initialized successfully")

    async def _get_credential(self):
        from azure.identity.aio import AzureCliCredential, ClientSecretCredential, DefaultAzureCredential

        # Service Principal
        if self.auth_config.client_secret:
            return ClientSecretCredential(
                tenant_id=self.auth_config.tenant_id,
                client_id=self.auth_config.client_id,
                client_secret=self.auth_config.client_secret.get_secret_value(),
            )

        # Interactive mode -> rely on `az login`
        if self.auth_config.auth_mode == AuthMode.INTERACTIVE:
            return AzureCliCredential()

        # Default chain (exclude browser)
        return DefaultAzureCredential(exclude_interactive_browser_credential=True)

    async def _refresh_token(self) -> None:
        # refresh if expiring within 60 seconds
        if self._token and time.time() < self._token_expiry - 60:
            return

        logger.debug("Refreshing access token")
        scopes = ["https://api.fabric.microsoft.com/.default"]
        token_response = await self._credential.get_token(*scopes)
        self._token = token_response.token
        self._token_expiry = float(token_response.expires_on)
        logger.debug(f"Token refreshed, expires at {self._token_expiry}")

    def _ensure_initialized(self) -> None:
        if not self._initialized or not self._client:
            raise RuntimeError("FabricApiClient not initialized. Call initialize() first.")

    async def _get_headers(self) -> Dict[str, str]:
        await self._refresh_token()
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    async def request(
        self,
        method: str,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> httpx.Response:
        """
        Execute an HTTP request with automatic retry + circuit breaking.

        All GET/POST/PUT/DELETE calls route through here, so resilience is
        applied uniformly — no per-call retry logic needed at call sites.

        RetryPolicy:    Retries 429/5xx with exponential backoff + full jitter.
        CircuitBreaker: Fast-fails once `failure_threshold` consecutive failures
                        occur, giving the Fabric API time to recover.

        FAANG PARALLEL:
          AWS SDK: every API call goes through the retry handler middleware.
          Google gRPC: retry interceptor wraps all unary calls.
          Stripe API client: automatic retry with idempotency keys.
        """
        self._ensure_initialized()

        url = self.base_url + (path if path.startswith("/") else f"/{path}")

        async def _do_request() -> httpx.Response:
            headers = await self._get_headers()
            logger.debug(f"{method.upper()} {url}")
            resp = await self._client.request(
                method=method.upper(),
                url=url,
                headers=headers,
                json=json_data,
                params=params,
            )
            logger.debug(f"Response: {resp.status_code}")

            # Raise structured error so RetryPolicy can inspect status_code
            if resp.status_code >= 400:
                try:
                    body = resp.json()
                except Exception:
                    body = {"raw_text": resp.text}
                raise FabricApiError(
                    f"Fabric API error {resp.status_code}: {resp.text[:200]}",
                    status_code=resp.status_code,
                    response_body=body,
                )

            return resp

        # CircuitBreaker wraps RetryPolicy: if circuit is OPEN, no retries happen.
        try:
            return await self._circuit_breaker.call(
                self._retry_policy.execute, _do_request
            )
        except CircuitOpenError:
            raise  # Re-raise — callers can catch CircuitOpenError specifically
        except FabricApiError:
            raise  # Already structured — re-raise as-is

    # -------------------------
    # RAW helpers (httpx.Response)
    # -------------------------

    async def get_raw(self, path: str, params: Optional[Dict[str, Any]] = None) -> httpx.Response:
        return await self.request("GET", path, params=params)

    async def post_raw(
        self,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,  # alias
    ) -> httpx.Response:
        if json is not None and json_data is None:
            json_data = json
        return await self.request("POST", path, json_data=json_data, params=params)

    async def put_raw(
        self,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,  # alias
    ) -> httpx.Response:
        if json is not None and json_data is None:
            json_data = json
        return await self.request("PUT", path, json_data=json_data)

    async def delete_raw(self, path: str) -> httpx.Response:
        return await self.request("DELETE", path)

    # -------------------------
    # Convenience methods (FabricResponse)
    # -------------------------

    async def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> FabricResponse:
        resp = await self.get_raw(path, params=params)
        if resp.status_code != 200:
            raise FabricApiError(
                f"GET {path} failed with status {resp.status_code}",
                status_code=resp.status_code,
                response_body=resp.json() if resp.content else None,
            )
        return FabricResponse(resp)

    async def post(
        self,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,  # alias
    ) -> FabricResponse:
        resp = await self.post_raw(path, json_data=json_data, params=params, json=json)
        if resp.status_code not in (200, 201, 202):
            raise FabricApiError(
                f"POST {path} failed with status {resp.status_code}",
                status_code=resp.status_code,
                response_body=resp.json() if resp.content else None,
            )
        return FabricResponse(resp)

    async def put(
        self,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,  # alias
    ) -> FabricResponse:
        resp = await self.put_raw(path, json_data=json_data, json=json)
        if resp.status_code not in (200, 201, 202):
            raise FabricApiError(
                f"PUT {path} failed with status {resp.status_code}",
                status_code=resp.status_code,
                response_body=resp.json() if resp.content else None,
            )
        return FabricResponse(resp)

    async def put_binary(
        self,
        path: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> httpx.Response:
        """
        Upload raw bytes to a Fabric endpoint (e.g., OneLake Files API).

        Used by build_and_upload.py and bootstrap to push .whl files into
        Lakehouse Files/wheels/ so Notebooks can %pip install them.

        Standard REST pattern: PUT with Content-Type: application/octet-stream.
        The regular put() method serialises JSON — this bypasses that and
        streams raw bytes instead.

        FAANG PATTERN:
            Same as AWS S3 PutObject — direct binary streaming upload.
            Compute reads from where data lives (OneLake), not from the
            developer's machine.

        Args:
            path: API path, e.g. /workspaces/{ws}/lakehouses/{lh}/files/...
            data: Raw bytes to upload (e.g., wheel file contents).
            content_type: MIME type (default: application/octet-stream).

        Returns:
            httpx.Response (caller may inspect status_code).

        Raises:
            FabricApiError: If the server returns a non-2xx status.
        """
        self._ensure_initialized()
        url = self.base_url + (path if path.startswith("/") else f"/{path}")
        await self._refresh_token()
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": content_type,
        }

        logger.debug(f"PUT (binary) {url} [{len(data):,} bytes]")
        resp = await self._client.put(url, content=data, headers=headers)
        if resp.status_code not in (200, 201, 202, 204):
            raise FabricApiError(
                f"PUT binary {path} failed with status {resp.status_code}",
                status_code=resp.status_code,
                response_body=resp.json() if resp.content else None,
            )
        logger.debug(f"PUT binary response: {resp.status_code}")
        return resp

    async def delete(self, path: str) -> FabricResponse:
        resp = await self.delete_raw(path)
        if resp.status_code not in (200, 202, 204):
            raise FabricApiError(
                f"DELETE {path} failed with status {resp.status_code}",
                status_code=resp.status_code,
                response_body=resp.json() if resp.content else None,
            )
        return FabricResponse(resp)

    # -------------------------
    # LRO support (FIXED)
    # -------------------------

    @staticmethod
    def _extract_operation_id(location: Optional[str]) -> Optional[str]:
        if not location:
            return None
        m = re.search(r"/operations/([0-9a-fA-F-]{36})", location)
        return m.group(1) if m else None

    async def wait_for_lro(
        self,
        initial_response: Union[httpx.Response, FabricResponse],
        poll_interval: float = 1.5,
        timeout: int = 300,
    ) -> Dict[str, Any]:
        raw = initial_response.raw if isinstance(initial_response, FabricResponse) else initial_response

        # Not an LRO
        if raw.status_code in (200, 201):
            return raw.json() if raw.content else {}

        # Unexpected status
        if raw.status_code != 202:
            raw.raise_for_status()
            return raw.json() if raw.content else {}

        # LRO headers
        location = raw.headers.get("Location") or raw.headers.get("location")
        op_id = raw.headers.get("x-ms-operation-id") or raw.headers.get("x-ms-operationid")

        # Try to extract from Location if missing
        if not op_id:
            op_id = self._extract_operation_id(location)

        # Prefer documented operations endpoint on api.fabric.microsoft.com
        op_url = f"{self.base_url}/operations/{op_id}" if op_id else location
        if not op_url:
            raise FabricApiError("LRO response did not include Location or x-ms-operation-id")

        logger.info(f"Waiting for LRO: {op_url}")

        # Honor Retry-After if present
        retry_after = raw.headers.get("Retry-After")
        if retry_after:
            try:
                poll_interval = max(poll_interval, float(retry_after))
            except ValueError:
                pass

        start_time = time.time()
        headers = await self._get_headers()

        last_data: Dict[str, Any] = {}
        last_status: str = ""

        # Poll until terminal state
        while True:
            if time.time() - start_time > timeout:
                raise LROTimeoutError(f"LRO did not complete within {timeout} seconds (opId={op_id})")

            self._ensure_initialized()
            resp = await self._client.get(op_url, headers=headers)
            resp.raise_for_status()

            data = resp.json() if resp.content else {}
            if isinstance(data, dict):
                last_data = data

            # Fabric commonly uses "status" (Succeeded/Failed/Running) for operations
            status = (data.get("status") or data.get("state") or "").strip()
            last_status = status.lower()

            if last_status in {"succeeded", "success", "completed"}:
                break

            if last_status in {"failed", "canceled", "cancelled", "error"}:
                # Preserve the server-provided payload (this is what you need to debug updateDefinition failures)
                raise FabricApiError(
                    f"LRO failed (opId={op_id}) status={status}",
                    response_body=data if isinstance(data, dict) else {"raw": data},
                )

            ra = resp.headers.get("Retry-After")
            sleep_time = poll_interval
            if ra:
                try:
                    sleep_time = max(sleep_time, float(ra))
                except ValueError:
                    pass

            await asyncio.sleep(sleep_time)

        # Try to fetch operation result:
        # IMPORTANT: Not every Fabric operation has a /result endpoint.
        # Some return 400/404 here even though the operation succeeded.
        if op_id:
            result_url = f"{self.base_url}/operations/{op_id}/result"
        else:
            result_url = op_url.rstrip("/")
            if not result_url.endswith("/result"):
                result_url = f"{result_url}/result"

        self._ensure_initialized()
        result_resp = await self._client.get(result_url, headers=headers)

        if result_resp.status_code in (400, 404):
            # No result endpoint (common). Return final operation payload.
            logger.warning(
                f"LRO succeeded but /result not available (status={result_resp.status_code}). "
                f"Returning final operation state instead. opId={op_id}"
            )
            return last_data

        result_resp.raise_for_status()
        if not result_resp.content:
            return last_data

        try:
            return result_resp.json()
        except Exception:
            return last_data

    async def post_with_lro(
        self,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,  # alias
        *,
        lro_poll_seconds: float = 1.5,
        max_polls: int = 200,
        timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        POST and wait for Long Running Operation completion.

        - lro_poll_seconds: poll interval
        - max_polls: used to derive timeout if timeout is not provided
        """
        resp = await self.post_raw(path, json_data=json_data, params=params, json=json)

        poll_interval = float(lro_poll_seconds)
        if timeout is None:
            timeout = int(max(30.0, poll_interval * float(max_polls)))

        if resp.status_code == 202:
            return await self.wait_for_lro(resp, poll_interval=poll_interval, timeout=timeout)

        # Non-LRO create/update responses
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

        if self._credential and hasattr(self._credential, "close"):
            await self._credential.close()

        self._initialized = False
        logger.info("Fabric API client closed")

    # -------------------------
    # Async context manager
    # -------------------------

    async def __aenter__(self) -> "FabricApiClient":
        """
        Support ``async with FabricApiClient(...) as client:`` usage.

        Calls initialize() so callers don't need a separate await step,
        and guarantees close() is called even if the body raises.

        FAANG PATTERN:
            boto3 Session, google-cloud clients, and httpx.AsyncClient all
            expose async context managers for the same reason — guaranteed
            cleanup without try/finally boilerplate at every call site.
        """
        await self.initialize()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Close the underlying HTTP client and release the Azure credential."""
        await self.close()


# =============================================================================
# Sync client (kept for legacy imports)
# =============================================================================

class SyncFabricApiClient:
    """
    Synchronous wrapper used by some older scripts/modules in this repo.
    """

    def __init__(
        self,
        auth_config: FabricAuthConfig,
        base_url: str = "https://api.fabric.microsoft.com",
        api_version: str = "/v1",
        timeout_seconds: int = 30,
    ):
        import requests

        self.auth_config = auth_config
        self.base_url = base_url.rstrip("/") + api_version
        self.timeout = timeout_seconds

        self._token: Optional[str] = None
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    def _get_token_sync(self) -> str:
        from azure.identity import AzureCliCredential, ClientSecretCredential, DefaultAzureCredential

        if self.auth_config.client_secret:
            credential = ClientSecretCredential(
                tenant_id=self.auth_config.tenant_id,
                client_id=self.auth_config.client_id,
                client_secret=self.auth_config.client_secret.get_secret_value(),
            )
        else:
            if self.auth_config.auth_mode == AuthMode.INTERACTIVE:
                credential = AzureCliCredential()
            else:
                credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)

        scopes = ["https://api.fabric.microsoft.com/.default"]
        return credential.get_token(*scopes).token

    def initialize(self) -> None:
        self._token = self._get_token_sync()
        self._session.headers.update({"Authorization": f"Bearer {self._token}"})

    def request(self, method: str, path: str, **kwargs: Any):
        url = self.base_url + (path if path.startswith("/") else f"/{path}")
        return self._session.request(method=method.upper(), url=url, timeout=self.timeout, **kwargs)

    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        resp = self.request("GET", path, params=params)
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    def post_json(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        resp = self.request("POST", path, json=payload)
        resp.raise_for_status()
        return resp.json() if resp.content else {}


def build_local_fabric_client() -> SyncFabricApiClient:
    cfg = FabricAuthConfig.from_env()
    c = SyncFabricApiClient(cfg)
    c.initialize()
    return c


__all__ = [
    "FabricApiClient",
    "SyncFabricApiClient",
    "FabricResponse",
    "FabricApiError",
    "LROTimeoutError",
    "build_local_fabric_client",
]
