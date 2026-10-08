"""Logging helpers.

influxkit logs under the ``influxkit`` logger hierarchy (``influxkit.write``,
``influxkit.query``, ``influxkit.transport``, ``influxkit.config``...) and, like any
library, only attaches a ``NullHandler``: by default its records propagate to the
application's handlers. :func:`configure_logging` (or ``[logging] configure = true``) is for
applications without logging setup: it gives the ``influxkit`` logger its own stderr handler
and stops propagation, so records are not printed twice.

Log records carry structured context in ``record.influx`` (a dict: client, database, points,
error, status, code, measurement...), which :class:`JsonFormatter` emits as top-level keys
together with any ``extra=`` attributes. Tokens never reach log records.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import UTC, datetime
from typing import Any, Literal

from . import _fork
from .config import LoggingConfig

__all__ = ["JsonFormatter", "RateLimitedLog", "configure_logging"]

ROOT = "influxkit"
_HANDLER_MARK = "_influxkit_handler"
# Shared by all RateLimitedLog instances: held only for a dict update.
_RATE_LOCK = threading.Lock()


def _new_rate_lock() -> None:
    global _RATE_LOCK
    _RATE_LOCK = threading.Lock()


_fork.on_child(locks=_new_rate_lock)


# Attributes every LogRecord has; anything else was passed with extra=.
_STANDARD_ATTRS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    """One JSON object per line: time, level, logger, message, structured context, exception.

    Structured context is ``record.influx`` (set by influxkit) plus any other ``extra=``
    attributes, as top-level keys.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(
            (key, value)
            for key, value in vars(record).items()
            if key not in _STANDARD_ATTRS and key != "influx" and not key.startswith("_")
        )
        context = getattr(record, "influx", None)
        if isinstance(context, dict):
            payload.update({key: value for key, value in context.items() if value is not None})
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class _TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        context = getattr(record, "influx", None)
        if isinstance(context, dict) and context:
            text += " | " + " ".join(f"{key}={value}" for key, value in context.items())
        return text


def configure_logging(
    config: LoggingConfig | None = None,
    *,
    level: str | None = None,
    format: Literal["text", "json"] | None = None,
    stream: Any = None,
) -> logging.Logger:
    """Give the ``influxkit`` logger its own handler (idempotent); it stops propagating.

    For applications that do not configure logging themselves. With your own logging setup,
    leave this off: influxkit's records then propagate to your handlers.

    Args:
        config: A ``[logging]`` section; ``level`` and ``format`` override its values.
        level: Level name, e.g. ``"DEBUG"`` (default INFO).
        format: ``"text"`` or ``"json"`` (one JSON object per line).
        stream: Where to write (default stderr).
    """
    config = config or LoggingConfig(configure=True)
    updates: dict[str, Any] = {}
    if level is not None:
        updates["level"] = level
    if format is not None:
        updates["format"] = format
    if updates:
        config = LoggingConfig.model_validate({**config.model_dump(), **updates})
    logger = logging.getLogger(ROOT)
    logger.setLevel(config.level)
    for handler in list(logger.handlers):
        if getattr(handler, _HANDLER_MARK, False):
            logger.removeHandler(handler)
    handler = logging.StreamHandler(stream)
    setattr(handler, _HANDLER_MARK, True)
    if config.format == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(_TextFormatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s"))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


class RateLimitedLog:
    """Emit at most one record per key per interval; report how many were suppressed.

    Protects applications from log floods when, say, every point of a hot loop is invalid.
    """

    def __init__(self, logger: logging.Logger, interval: float = 10.0) -> None:
        self.logger = logger
        self.interval = interval
        # key: (time last emitted, suppressed since, latest suppressed (level, message, args, kwargs))
        self._state: dict[
            str, tuple[float, int, tuple[int, str, tuple[Any, ...], dict[str, Any]] | None]
        ] = {}

    def log(self, key: str, level: int, message: str, *args: Any, **kwargs: Any) -> bool:
        """Log unless ``key`` was logged within ``interval``; True if the record was emitted."""
        if not self.logger.isEnabledFor(level):
            return False
        now = time.monotonic()
        with _RATE_LOCK:
            state = self._state.get(key)
            if state is not None and now - state[0] < self.interval:
                # Keep the latest suppressed record, so flush() can show what was hidden.
                self._state[key] = (state[0], state[1] + 1, (level, message, args, kwargs))
                return False
            suppressed = state[1] if state is not None else 0
            self._state[key] = (now, 0, None)
        if suppressed:
            message += " (%d similar messages suppressed)"
            args = (*args, suppressed)
        self.logger.log(level, message, *args, **kwargs)
        return True

    def flush(self) -> None:
        """Log the last suppressed record of each key with the suppressed count (e.g. at close)."""
        with _RATE_LOCK:
            pending = [(count, last) for _, count, last in self._state.values() if count and last]
            self._state.clear()
        for count, (level, message, args, kwargs) in pending:
            self.logger.log(level, message + " (%d similar messages suppressed)", *args, count, **kwargs)


logging.getLogger(ROOT).addHandler(logging.NullHandler())
