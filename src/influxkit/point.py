"""A lightweight point builder."""

from __future__ import annotations

from typing import Any, Self

__all__ = ["Point"]


class Point:
    """One point: measurement, tags, fields and an optional timestamp.

    Plain dicts (``{"measurement", "tags", "fields", "time"}``) are accepted everywhere a
    Point is; use whichever is more convenient. A Point is a passive container: all
    validation, tag injection and serialization happen when it is written.

    >>> Point("cpu").tag("host", "a").field("usage", 0.5).time(1_700_000_000_000_000_000)
    Point('cpu', tags={'host': 'a'}, fields={'usage': 0.5}, time=1700000000000000000)
    """

    __slots__ = ("fields", "measurement", "tags", "timestamp")

    def __init__(
        self,
        measurement: str,
        tags: dict[str, Any] | None = None,
        fields: dict[str, Any] | None = None,
        time: Any = None,
    ) -> None:
        self.measurement = measurement
        self.tags: dict[str, Any] = tags if tags is not None else {}
        self.fields: dict[str, Any] = fields if fields is not None else {}
        self.timestamp: Any = time

    def tag(self, key: str, value: Any) -> Self:
        """Set a tag (``None`` or ``""`` values are omitted when written)."""
        self.tags[key] = value
        return self

    def field(self, key: str, value: Any) -> Self:
        """Set a field (``None`` values are omitted when written)."""
        self.fields[key] = value
        return self

    def time(self, value: Any) -> Self:
        """Set the timestamp: an int in the write precision, a datetime, an ISO 8601 string,
        a float of epoch seconds, or a numpy/pandas timestamp."""
        self.timestamp = value
        return self

    def __repr__(self) -> str:
        parts = f"tags={self.tags!r}, fields={self.fields!r}, time={self.timestamp!r}"
        return f"Point({self.measurement!r}, {parts})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Point):
            return NotImplemented
        return (self.measurement, self.tags, self.fields, self.timestamp) == (
            other.measurement,
            other.tags,
            other.fields,
            other.timestamp,
        )

    __hash__ = None  # type: ignore[assignment]  # mutable
