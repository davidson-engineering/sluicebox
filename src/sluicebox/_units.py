"""Parsing helpers for human-friendly durations and byte sizes in configuration."""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Annotated, Any

from pydantic import BeforeValidator, ByteSize

__all__ = ["Bytes", "Seconds", "parse_duration"]

_DURATION_PART = re.compile(r"(\d+(?:\.\d+)?)\s*(ns|us|µs|ms|s|m|h|d|w)", re.IGNORECASE)
_UNIT_SECONDS = {
    "ns": 1e-9,
    "us": 1e-6,
    "µs": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
    "d": 86400.0,
    "w": 604800.0,
}


def parse_duration(value: Any) -> Any:
    """Convert ``"250ms"``, ``"1h30m"``, ``"2d"``, a number of seconds or a timedelta to seconds.

    Anything else is passed through so pydantic reports a normal type error.
    """
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        try:
            return float(text)
        except ValueError:
            pass
        position = 0
        total = 0.0
        for match in _DURATION_PART.finditer(text):
            if text[position : match.start()].strip():
                break
            total += float(match.group(1)) * _UNIT_SECONDS[match.group(2).lower()]
            position = match.end()
        else:
            if position and not text[position:].strip():
                return total
        raise ValueError(f"invalid duration {value!r}; use seconds or a string like '500ms', '30s', '1h30m'")
    return value


#: A duration in seconds; accepts numbers or strings such as ``"500ms"`` and ``"1h30m"``.
Seconds = Annotated[float, BeforeValidator(parse_duration)]

#: A byte size; accepts integers or strings such as ``"8MiB"`` and ``"512KB"``.
Bytes = ByteSize
