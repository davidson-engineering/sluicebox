"""Typed records: write dataclasses and pydantic models directly.

Decorate a dataclass or pydantic model with :func:`measurement`; mark tags with
``Annotated[str, Tag]`` and the timestamp with ``Annotated[datetime, Timestamp]`` (an
attribute named ``time`` or ``timestamp`` is used automatically). Every other attribute
is a field, and its annotation becomes the field's **declared type** - so a model can
never write an inconsistent type::

    @measurement("cpu")
    @dataclass
    class CpuSample:
        host: Annotated[str, Tag]
        usage: float
        cores: int
        time: datetime

    client.write([CpuSample("web-1", 0.42, 8, now)])
"""

from __future__ import annotations

import dataclasses
import operator
import types
import typing
from datetime import datetime
from decimal import Decimal
from enum import Enum, IntEnum
from typing import Annotated, Any, TypeVar

from ._lineprotocol import UInt
from .exceptions import ConfigurationError
from .types import FieldType

if typing.TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["Field", "Tag", "Timestamp", "measurement"]

T = TypeVar("T", bound=type)


class Tag:
    """Marks an attribute as a tag: ``Annotated[str, Tag]`` (or ``Tag(name="key")`` to rename)."""

    __slots__ = ("name",)

    def __init__(self, name: str | None = None) -> None:
        self.name = name


class Field:
    """Optional field options: ``Annotated[int, Field(name="cores", type="float")]``."""

    __slots__ = ("name", "type")

    def __init__(self, name: str | None = None, type: str | FieldType | None = None) -> None:
        self.name = name
        self.type = FieldType.parse(type) if type is not None else None


class Timestamp:
    """Marks the timestamp attribute: ``Annotated[datetime, Timestamp]``."""

    __slots__ = ()


_TIME_NAMES = ("time", "timestamp")


def _unwrap(annotation: Any) -> tuple[Any, list[Any]]:
    """Strip Annotated/Optional; return the base type and Annotated metadata."""
    metadata: list[Any] = []
    while True:
        origin = typing.get_origin(annotation)
        if origin is Annotated:
            metadata.extend(annotation.__metadata__)
            annotation = annotation.__origin__
            continue
        if origin in (typing.Union, types.UnionType):
            args = [arg for arg in typing.get_args(annotation) if arg is not type(None)]
            if len(args) == 1:
                annotation = args[0]
                continue
        return annotation, metadata


def _field_type(annotation: Any) -> FieldType | None:
    if not isinstance(annotation, type):
        return None
    if issubclass(annotation, bool):
        return FieldType.BOOLEAN
    if issubclass(annotation, UInt):
        return FieldType.UINTEGER
    if issubclass(annotation, IntEnum | int):
        return FieldType.INTEGER
    if issubclass(annotation, float | Decimal):
        return FieldType.FLOAT
    if issubclass(annotation, str):
        return FieldType.STRING
    if issubclass(annotation, Enum):
        return None
    return None


class ModelSpec:
    """How to turn an instance of a decorated class into (measurement, tags, fields, time)."""

    def __init__(
        self,
        measurement: str,
        tags: list[tuple[str, str]],
        fields: list[tuple[str, str]],
        time_attr: str | None,
        field_types: dict[str, FieldType],
    ) -> None:
        self.measurement = measurement
        self.field_types = field_types
        attrs = [attr for attr, _ in tags] + [attr for attr, _ in fields]
        if time_attr:
            attrs.append(time_attr)
        self._get: Callable[[Any], Any] = operator.attrgetter(*attrs)
        self._single = len(attrs) == 1
        self._tags = [(key, index) for index, (_, key) in enumerate(tags)]
        offset = len(tags)
        self._fields = [(key, offset + index) for index, (_, key) in enumerate(fields)]
        self._time = len(attrs) - 1 if time_attr else None

    def extract(self, obj: Any) -> tuple[str, dict[str, Any], dict[str, Any], Any]:
        values = self._get(obj)
        if self._single:
            values = (values,)
        tags = {key: values[index] for key, index in self._tags}
        fields = {key: values[index] for key, index in self._fields}
        stamp = values[self._time] if self._time is not None else None
        return self.measurement, tags, fields, stamp


def measurement(
    name: str | None = None,
    *,
    tags: tuple[str, ...] = (),
    timestamp: str | None = None,
) -> Callable[[T], T]:
    """Class decorator making dataclass / pydantic model instances writable.

    Args:
        name: Measurement name (default: the class name).
        tags: Attribute names to treat as tags, as an alternative to ``Annotated[..., Tag]``.
        timestamp: Attribute holding the timestamp (default: the ``Timestamp``-annotated
            attribute, else one named ``time`` or ``timestamp``).
    """

    def decorate(cls: T) -> T:
        spec = _build_spec(cls, name or cls.__name__, set(tags), timestamp)
        cls.__sluicebox_model__ = spec  # type: ignore[attr-defined]
        return cls

    return decorate


def _build_spec(cls: type, name: str, tag_names: set[str], timestamp: str | None) -> ModelSpec:
    hints = typing.get_type_hints(cls, include_extras=True)
    if dataclasses.is_dataclass(cls):
        attributes = [f.name for f in dataclasses.fields(cls)]
    elif hasattr(cls, "model_fields"):  # pydantic v2
        attributes = list(cls.model_fields)
        for attr, info in cls.model_fields.items():
            if info.metadata:
                hints[attr] = Annotated[(hints.get(attr, info.annotation), *info.metadata)]
    else:
        raise ConfigurationError(f"@measurement supports dataclasses and pydantic models, not {cls.__name__}")
    tag_specs: list[tuple[str, str]] = []
    field_specs: list[tuple[str, str]] = []
    field_types: dict[str, FieldType] = {}
    time_attr = timestamp
    unknown = tag_names - set(attributes)
    if unknown:
        raise ConfigurationError(f"@measurement tags {sorted(unknown)} are not attributes of {cls.__name__}")
    for attr in attributes:
        base, metadata = _unwrap(hints.get(attr, Any))
        marker_tag = next((m for m in metadata if m is Tag or isinstance(m, Tag)), None)
        marker_field = next((m for m in metadata if isinstance(m, Field)), None)
        is_time = any(m is Timestamp or isinstance(m, Timestamp) for m in metadata)
        if is_time:
            if time_attr and time_attr != attr:
                raise ConfigurationError(f"{cls.__name__} has more than one timestamp attribute")
            time_attr = attr
            continue
        if attr == time_attr:
            continue
        if marker_tag is not None or attr in tag_names:
            key = marker_tag.name if isinstance(marker_tag, Tag) and marker_tag.name else attr
            tag_specs.append((attr, key))
            continue
        if (
            time_attr is None
            and timestamp is None
            and attr in _TIME_NAMES
            and (base in (datetime, int, str, Any))
        ):
            time_attr = attr
            continue
        key = marker_field.name if marker_field and marker_field.name else attr
        field_specs.append((attr, key))
        declared = marker_field.type if marker_field and marker_field.type else _field_type(base)
        if declared is not None:
            field_types[key] = declared
    if not field_specs:
        raise ConfigurationError(f"{cls.__name__} has no fields; InfluxDB points need at least one")
    return ModelSpec(name, tag_specs, field_specs, time_attr, field_types)
