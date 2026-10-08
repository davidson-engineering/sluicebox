"""Shared enums and small value types."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

__all__ = ["FieldType", "Precision", "PRECISION_DIVISORS"]

#: Timestamp precision of written data, using InfluxDB's short names.
Precision = Literal["ns", "us", "ms", "s"]

#: Divisor that converts nanoseconds to each precision.
PRECISION_DIVISORS: dict[str, int] = {"ns": 1, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000}


class FieldType(StrEnum):
    """InfluxDB field value types."""

    FLOAT = "float"
    INTEGER = "integer"
    UINTEGER = "uinteger"
    STRING = "string"
    BOOLEAN = "boolean"

    @classmethod
    def parse(cls, value: str | FieldType) -> FieldType:
        """Parse a type name, accepting aliases such as ``int``, ``i64``, ``double``, ``str`` and ``bool``."""
        if isinstance(value, FieldType):
            return value
        try:
            return _ALIASES[value.strip().lower()]
        except KeyError:
            choices = ", ".join(sorted(_ALIASES))
            raise ValueError(f"unknown field type {value!r}; expected one of: {choices}") from None


_ALIASES: dict[str, FieldType] = {
    "float": FieldType.FLOAT,
    "double": FieldType.FLOAT,
    "f64": FieldType.FLOAT,
    "float64": FieldType.FLOAT,
    "integer": FieldType.INTEGER,
    "int": FieldType.INTEGER,
    "i64": FieldType.INTEGER,
    "int64": FieldType.INTEGER,
    "uinteger": FieldType.UINTEGER,
    "uint": FieldType.UINTEGER,
    "u64": FieldType.UINTEGER,
    "uint64": FieldType.UINTEGER,
    "unsigned": FieldType.UINTEGER,
    "string": FieldType.STRING,
    "str": FieldType.STRING,
    "boolean": FieldType.BOOLEAN,
    "bool": FieldType.BOOLEAN,
}
