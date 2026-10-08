"""Retry decisions and exponential backoff with jitter."""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

from ._http import is_retryable_transport_error
from .exceptions import ServerError, TransportError

if TYPE_CHECKING:
    from .config import RetryConfig

__all__ = ["RetryPolicy"]


class RetryPolicy:
    def __init__(self, config: RetryConfig) -> None:
        self.config = config
        self._random = random.Random()

    def is_retryable(self, error: BaseException) -> bool:
        """Transient failures: no response (network/timeout) or a retryable HTTP status.

        Retrying a write is safe because line protocol writes are idempotent for points with
        explicit timestamps (``write.auto_timestamp`` ensures every point has one).
        """
        if isinstance(error, TransportError):
            return is_retryable_transport_error(error)
        if isinstance(error, ServerError):
            return error.status in self.config.retry_on_status
        return False

    def delay(self, attempt: int, error: BaseException | None = None) -> float:
        """Seconds to wait before attempt ``attempt + 1`` (``attempt`` counts from 1)."""
        cfg = self.config
        base = min(cfg.max_delay, cfg.initial_delay * cfg.multiplier ** (attempt - 1))
        if cfg.jitter:
            base *= 1.0 + cfg.jitter * self._random.uniform(-1.0, 1.0)
        server_hint = getattr(error, "retry_after", None)
        if isinstance(server_hint, int | float) and server_hint > base:
            return float(server_hint)
        return max(0.0, base)

    def reason(self, error: BaseException) -> str:
        """Short label for the retry metric."""
        if isinstance(error, ServerError):
            return f"http_{error.status}"
        if isinstance(error, TransportError):
            return "timeout" if isinstance(error, TimeoutError) else "connection"
        return type(error).__name__
