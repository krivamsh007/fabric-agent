"""
resilience.py â€” Production Resilience Primitives
==================================================

Provides two battle-tested patterns for calling external APIs reliably:

  RetryPolicy   â€” exponential backoff for transient failures (429, 5xx)
  CircuitBreaker â€” fast-fail when a service is degraded (stops hammering it)

FAANG PARALLEL:
  AWS SDK:       exponential jitter backoff, same retryable/non-retryable split
  Google Cloud:  gRPC retry interceptor with Retry-After header support
  Netflix:       Hystrix circuit breaker (the canonical circuit breaker paper)
  Uber:          TChannel with adaptive timeouts + circuit breaking

WHY BOTH PATTERNS TOGETHER:
  RetryPolicy alone can make a bad situation worse â€” if the service is down,
  retrying 3x just multiplies the load. CircuitBreaker stops retries entirely
  once the failure count crosses a threshold, giving the service time to recover.

  Together: retry transient blips (429, 503), open the circuit on sustained
  outages (5 consecutive failures), then probe once after 60s to confirm recovery.

USAGE:
    from fabric_agent.api.resilience import RetryPolicy, CircuitBreaker

    policy = RetryPolicy(max_retries=3, backoff_base=2.0)
    cb = CircuitBreaker(failure_threshold=5, reset_timeout=60.0)

    # Both together (recommended):
    result = await cb.call(policy.execute, client.get, "/workspaces")

    # RetryPolicy alone:
    result = await policy.execute(client.get, "/workspaces")

    # CircuitBreaker alone:
    result = await cb.call(client.get, "/workspaces")

WIRING INTO FabricApiClient:
    Added to FabricApiClient.__init__() and FabricApiClient.request().
    All HTTP calls automatically benefit â€” no change needed at call sites.
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Callable, FrozenSet, Optional

from loguru import logger


# =============================================================================
# Retryable HTTP status codes
# =============================================================================

#: Status codes that indicate a transient failure worth retrying.
#: 429 = rate limit (wait and retry), 5xx = server errors (may self-heal).
RETRYABLE_STATUS_CODES: FrozenSet[int] = frozenset({429, 500, 502, 503, 504})

#: Status codes that are client errors â€” retrying will not help.
#: 400 = bad request, 401 = auth expired (need token refresh, not retry),
#: 403 = forbidden (permissions issue), 404 = not found.
NON_RETRYABLE_STATUS_CODES: FrozenSet[int] = frozenset({400, 401, 403, 404, 409, 422})


# =============================================================================
# Exceptions
# =============================================================================

class RetryExhaustedError(Exception):
    """Raised when all retry attempts are exhausted."""

    def __init__(self, message: str, last_status_code: Optional[int] = None):
        super().__init__(message)
        self.last_status_code = last_status_code


class CircuitOpenError(Exception):
    """
    Raised when the circuit breaker is OPEN.

    Fast-fail response â€” the service is considered degraded and calls
    are blocked until the reset timeout expires and a probe succeeds.
    """

    def __init__(self, message: str, failure_count: int, reset_at: float):
        super().__init__(message)
        self.failure_count = failure_count
        self.reset_at = reset_at
        self.seconds_until_reset = max(0.0, reset_at - time.monotonic())


# =============================================================================
# RetryPolicy
# =============================================================================

class RetryPolicy:
    """
    Exponential backoff with full jitter for transient API failures.

    WHAT: Automatically retries failed calls using exponentially increasing
          delays, with random jitter to prevent thundering-herd problems.

    WHY FULL JITTER: Without jitter, all clients retry at the same time after
    a rate limit, creating a new spike. Jitter spreads retries over time.
    See: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

    RETRIES: 429 (rate limited), 500/502/503/504 (server errors)
    NEVER RETRIES: 400/401/403/404/409/422 (client errors â€” fix the request)
    NEVER RETRIES: Non-HTTP exceptions (connection errors retry immediately)

    FAANG PARALLEL: AWS SDK default retry policy, Google Cloud retry library.

    PARAMETERS:
        max_retries:   Maximum number of retry attempts (not counting first try).
        backoff_base:  Base for exponential backoff (seconds). Default 2.0.
        max_wait:      Maximum wait between retries (seconds). Default 30.0.
        jitter:        If True, add random jitter to prevent thundering herd.

    EXAMPLE:
        policy = RetryPolicy(max_retries=3, backoff_base=2.0, max_wait=30.0)
        result = await policy.execute(client.get, "/workspaces")
        # Retry delays (approx): 2s, 4s, 8s (with jitter)
    """

    def __init__(
        self,
        max_retries: int = 3,
        backoff_base: float = 2.0,
        max_wait: float = 30.0,
        jitter: bool = True,
    ):
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.max_wait = max_wait
        self.jitter = jitter

    def _wait_time(self, attempt: int) -> float:
        """
        Calculate wait time for attempt N (0-indexed).

        Formula: min(max_wait, backoff_base^attempt) * random(0.5, 1.5) if jitter
        """
        exponential = min(self.max_wait, self.backoff_base ** attempt)
        if self.jitter:
            return exponential * random.uniform(0.5, 1.5)
        return exponential

    def _is_retryable(self, exc: Exception) -> bool:
        """Return True if this exception should trigger a retry."""
        # Import here to avoid circular dependency
        try:
            from fabric_agent.api.fabric_client import FabricApiError
            if isinstance(exc, FabricApiError):
                status = exc.status_code
                if status in NON_RETRYABLE_STATUS_CODES:
                    return False
                if status in RETRYABLE_STATUS_CODES:
                    return True
                # Unknown 4xx â€” don't retry
                if status and 400 <= status < 500:
                    return False
                # Unknown 5xx or no status â€” retry
                return True
        except ImportError:
            pass

        # For non-FabricApiError exceptions (connection errors, timeouts) â€” retry
        return True

    async def execute(self, coro_func: Callable, *args: Any, **kwargs: Any) -> Any:
        """
        Execute `coro_func(*args, **kwargs)` with retry on transient failures.

        Args:
            coro_func: Async callable to execute.
            *args, **kwargs: Arguments forwarded to coro_func.

        Returns:
            Result from coro_func on success.

        Raises:
            RetryExhaustedError: If all retry attempts are exhausted.
            Exception: Re-raises non-retryable exceptions immediately.
        """
        last_exc: Optional[Exception] = None
        last_status: Optional[int] = None

        for attempt in range(self.max_retries + 1):
            try:
                return await coro_func(*args, **kwargs)

            except Exception as exc:
                last_exc = exc

                # Extract status code for logging
                try:
                    from fabric_agent.api.fabric_client import FabricApiError
                    if isinstance(exc, FabricApiError):
                        last_status = exc.status_code
                except ImportError:
                    pass

                if not self._is_retryable(exc):
                    logger.debug(
                        f"RetryPolicy: non-retryable error (status={last_status}), "
                        f"raising immediately"
                    )
                    raise

                if attempt == self.max_retries:
                    break

                wait = self._wait_time(attempt)
                logger.warning(
                    f"RetryPolicy: attempt {attempt + 1}/{self.max_retries + 1} failed "
                    f"(status={last_status}, error={exc!r}). "
                    f"Retrying in {wait:.1f}s..."
                )
                await asyncio.sleep(wait)

        raise RetryExhaustedError(
            f"All {self.max_retries + 1} attempts failed. Last error: {last_exc!r}",
            last_status_code=last_status,
        ) from last_exc


# =============================================================================
# CircuitBreaker
# =============================================================================

class CircuitBreaker:
    """
    Prevents cascading failures by stopping calls to a degraded service.

    STATES:
        CLOSED    â€” Normal operation. Calls pass through.
        OPEN      â€” Service is considered down. Calls fast-fail immediately.
        HALF_OPEN â€” Timeout expired. One probe call is allowed through.
                    If it succeeds: back to CLOSED. If it fails: back to OPEN.

    FAANG PARALLEL:
        Netflix Hystrix: the canonical circuit breaker for microservices.
        Google SRE Book: circuit breaking as a load-shedding mechanism.
        AWS App Mesh: circuit breaking for service meshes.

    WHY THIS MATTERS FOR FABRIC AGENT:
        The Fabric API has rate limits and occasional service degradation.
        Without a circuit breaker, a scan of 10 workspaces would make 50+
        API calls during an outage â€” all failing â€” while the user waits.
        With circuit breaking: after 5 failures, remaining calls fail fast
        and the user sees a clear "service degraded" message.

    PARAMETERS:
        failure_threshold: Number of consecutive failures before opening.
        reset_timeout:     Seconds to wait in OPEN state before probing.

    USAGE:
        cb = CircuitBreaker(failure_threshold=5, reset_timeout=60.0)
        try:
            result = await cb.call(client.get, "/workspaces")
        except CircuitOpenError as e:
            print(f"Service degraded. Retry in {e.seconds_until_reset:.0f}s")
        except Exception as e:
            print(f"Call failed: {e}")

    MONITORING:
        cb.state          â†’ "closed" | "open" | "half_open"
        cb.failure_count  â†’ current consecutive failure count
    """

    _STATE_CLOSED = "closed"
    _STATE_OPEN = "open"
    _STATE_HALF_OPEN = "half_open"

    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout: float = 60.0,
    ):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout

        self._failure_count: int = 0
        self._state: str = self._STATE_CLOSED
        self._opened_at: float = 0.0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> str:
        """Current circuit state: 'closed', 'open', or 'half_open'."""
        if self._state == self._STATE_OPEN:
            if time.monotonic() >= self._opened_at + self.reset_timeout:
                return self._STATE_HALF_OPEN
        return self._state

    @property
    def failure_count(self) -> int:
        """Current consecutive failure count."""
        return self._failure_count

    def _record_success(self) -> None:
        """Reset to CLOSED state after a successful call."""
        self._failure_count = 0
        self._state = self._STATE_CLOSED
        logger.debug("CircuitBreaker: call succeeded â†’ state=CLOSED")

    def _record_failure(self) -> None:
        """Increment failure count and open circuit if threshold reached."""
        self._failure_count += 1
        if self._failure_count >= self.failure_threshold:
            self._state = self._STATE_OPEN
            self._opened_at = time.monotonic()
            logger.warning(
                f"CircuitBreaker: {self._failure_count} consecutive failures â†’ "
                f"state=OPEN (reset in {self.reset_timeout:.0f}s)"
            )
        else:
            logger.debug(
                f"CircuitBreaker: failure {self._failure_count}/{self.failure_threshold}"
            )

    async def call(self, coro_func: Callable, *args: Any, **kwargs: Any) -> Any:
        """
        Execute `coro_func(*args, **kwargs)` with circuit breaking.

        Args:
            coro_func: Async callable to execute.
            *args, **kwargs: Arguments forwarded to coro_func.

        Returns:
            Result from coro_func on success.

        Raises:
            CircuitOpenError: If circuit is OPEN (service considered down).
            Exception: If call fails (and circuit was CLOSED or HALF_OPEN).
        """
        async with self._lock:
            current_state = self.state

            if current_state == self._STATE_OPEN:
                raise CircuitOpenError(
                    f"Circuit is OPEN after {self._failure_count} consecutive failures. "
                    f"Service is considered degraded.",
                    failure_count=self._failure_count,
                    reset_at=self._opened_at + self.reset_timeout,
                )

            if current_state == self._STATE_HALF_OPEN:
                logger.info(
                    "CircuitBreaker: state=HALF_OPEN - sending probe request"
                )

        try:
            result = await coro_func(*args, **kwargs)

            async with self._lock:
                if self.state in (self._STATE_HALF_OPEN, self._STATE_OPEN):
                    logger.info(
                        "CircuitBreaker: probe succeeded â†’ state=CLOSED, "
                        f"failure count reset from {self._failure_count}"
                    )
                self._record_success()

            return result

        except CircuitOpenError:
            raise

        except Exception:
            async with self._lock:
                self._record_failure()
            raise

