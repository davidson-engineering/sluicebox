"""Record -> line protocol, with validation, type locking and tag injection in a single pass.

This is the hot path of every write, so the common case is inlined into one loop:

* records are unpacked by exact type (dict, Point, @measurement model) without calls;
* each (database, measurement) gets a cached plan holding the escaped measurement, schema,
  matching tag rules and per-field ``(escaped key, locked type)``;
* complete serialized tag sets are cached per plan (adaptively disabled when the tag
  cardinality is too high for the cache to pay off), falling back to a cache of escaped
  ``key=value`` fragments;
* field values are dispatched on ``type(v) is ...``; unusual values (numpy scalars, enums,
  Decimal, str/int subclasses...) and first sightings take the slower general paths.
"""

from __future__ import annotations

import logging
import math
import operator
import re
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from ._lineprotocol import STRING_ESCAPES, Dialect, LineSyntaxError, UInt, parse_line
from .exceptions import ValidationError
from .log import RateLimitedLog
from .point import Point
from .tags import _CONTEXT_TAGS, TagInjector
from .types import PRECISION_DIVISORS, FieldType

if TYPE_CHECKING:
    from .config import MeasurementSchema, ValidationConfig

__all__ = ["SerializedChunk", "Serializer"]

FLOAT = FieldType.FLOAT
INTEGER = FieldType.INTEGER
UINTEGER = FieldType.UINTEGER
STRING = FieldType.STRING
BOOLEAN = FieldType.BOOLEAN

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
#: Integer timestamps before 1973 (1e17 ns) are almost always in the wrong unit: epoch seconds,
#: milliseconds or microseconds written with precision "ns" land in January 1970.
_PLAUSIBLE_NS = 10**17
_PRECISION_NAMES = {divisor: name for name, divisor in PRECISION_DIVISORS.items()}
_NAT = _INT64_MIN  # numpy's "not a time"
_UINT64_MAX = 2**64 - 1
_FLOAT_EXACT_INT = 2**53
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_ONE_US = timedelta(microseconds=1)
_DICT_KEYS = frozenset({"measurement", "tags", "fields", "time"})
_REQUIRED_KEYS = ("measurement", "fields")
_ISO_FRACTION = re.compile(r"^(.*?[T ]\d{2}:\d{2}:\d{2})[.,](\d+)(.*)$", re.IGNORECASE)
_FRAGMENT_CACHE_LIMIT = 200_000
_TAGSET_CACHE_LIMIT = 50_000
_TAGSET_CACHE_FILLS = 3
_NO_COERCION: Any = object()

_isfinite = math.isfinite


@dataclass(slots=True)
class SerializedChunk:
    """Lines produced from one chunk of input records."""

    lines: list[str]
    nbytes: int
    dropped: int
    #: Why records were dropped (``on_invalid = "drop"``), in input order.
    rejected: list[ValidationError] = field(default_factory=list)


class _Plan:
    """Everything known about one (database, measurement) pair."""

    __slots__ = (
        "allowed_tags",
        "declared",
        "fields",
        "forbid_extra",
        "measurement",
        "prefix",
        "required_fields",
        "required_tags",
        "rules",
        "static_tagsets",
        "tag_keys",
        "tagset_fills",
        "tagsets",
        "types",
    )

    def __init__(self, measurement: str, prefix: str, schema: MeasurementSchema | None, rules: Any) -> None:
        self.measurement = measurement
        self.prefix = prefix
        #: Declared + locked field types; grows (setdefault) as new fields are seen.
        self.types: dict[str, FieldType] = dict(schema.fields) if schema else {}
        self.declared: frozenset[str] = frozenset(schema.fields) if schema else frozenset()
        #: Validated field key -> ("escaped_key=", expected type or None).
        self.fields: dict[str, tuple[str, FieldType | None]] = {}
        #: Tag keys seen (tracked only where tag/field name overlap is forbidden).
        self.tag_keys: set[str] = set()
        #: tuple(final tags.items()) -> serialized "measurement,k=v,..." head (None = disabled).
        self.tagsets: dict[tuple[tuple[str, Any], ...], str] | None = {}
        #: Same, keyed by the point's own tags when only (constant) static tags are injected.
        self.static_tagsets: dict[tuple[tuple[str, Any], ...], str] | None = {}
        self.tagset_fills = 0
        self.rules = rules
        self.allowed_tags = schema.tags if schema else None
        self.required_tags = schema.required_tags if schema else frozenset()
        self.required_fields = schema.required_fields if schema else frozenset()
        self.forbid_extra = bool(schema and schema.extra_fields == "forbid")


class Serializer:
    """Converts records to line protocol for one client. Thread-safe."""

    def __init__(
        self,
        *,
        dialect: Dialect,
        validation: ValidationConfig,
        schemas: Mapping[str, MeasurementSchema],
        injector: TagInjector,
        auto_timestamp: bool,
        on_drop: Callable[[ValidationError, int], None] | None = None,
    ) -> None:
        self.dialect = dialect
        self.validation = validation
        self.schemas = dict(schemas)
        self.injector = injector
        self.auto_timestamp = auto_timestamp
        self.on_drop = on_drop
        self._raise = validation.on_invalid == "raise"
        self._lock_types = validation.type_lock
        self._coerce = validation.coerce
        self._int_as_float = validation.int_as_float
        self._skip_non_finite = validation.non_finite == "skip"
        self._naive_utc = validation.naive_datetime == "utc"
        self._max_string = int(validation.max_string_bytes)
        self._max_string_chars_fast = self._max_string // 4  # UTF-8 uses at most 4 bytes per char
        self._validate_raw = validation.raw_lines == "validate"
        self._check_overlap = dialect.forbid_tag_field_overlap
        self._plans: dict[str, dict[str, _Plan]] = {}
        self._fragments: dict[tuple[str, str], str] = {}
        self._last_datetime: tuple[datetime | None, int] = (None, 0)
        self._seeded_models: set[tuple[str, type]] = set()
        self._warnings = RateLimitedLog(logging.getLogger("sluicebox.validation"))

    # ------------------------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------------------------

    def serialize(
        self,
        records: Iterable[Any],
        *,
        database: str,
        precision: str,
        start_index: int = 0,
        now_ns: int | None = None,
    ) -> SerializedChunk:
        """Serialize ``records``; invalid records raise or are dropped per ``on_invalid``.

        ``now_ns`` is the timestamp given to points without one (when ``auto_timestamp``);
        callers pass the same value for every chunk of one ``write()``.
        """
        divisor = PRECISION_DIVISORS[precision]
        # Integer timestamps in this range take the fast path; others are checked in _timestamp.
        stamp_lo = _PLAUSIBLE_NS // divisor
        stamp_hi = _INT64_MAX // divisor
        now = None
        if self.auto_timestamp:
            if now_ns is None:
                now_ns = time.time_ns()
            now = now_ns // divisor
        lines: list[str] = []
        append = lines.append
        nbytes = 0
        dropped = 0
        rejected: list[ValidationError] = []
        index = start_index - 1
        # Untimed points of one call share the write() time: detect ones that would overwrite
        # each other (same series and field). Tracking starts lazily at the second such point.
        first_untimed: tuple[str, Any] | None = None
        untimed: dict[str, set[Any]] | None = None
        overwritten = 0
        # Plan and cache entries the current record created, undone if it is then rejected
        # (a rejected record must not lock field types or register tag keys).
        journal: list[tuple[Any, Any]] = []

        # Hot-loop locals.
        db_plans = self._plans.get(database)
        if db_plans is None:
            db_plans = self._plans.setdefault(database, {})  # atomic: concurrent callers share one
        plans_get = db_plans.get
        injector = self.injector
        enrich = bool(injector.enrichers)
        static = bool(injector.static)
        context_get = _CONTEXT_TAGS.get
        int_as_float = self._int_as_float
        skip_non_finite = self._skip_non_finite
        max_chars = self._max_string_chars_fast
        escapes = STRING_ESCAPES
        # With type locking every known field has a concrete expected type; without it,
        # undeclared fields have None and are written as their natural type.
        int_untyped_ok = not int_as_float

        for record in records:
            index += 1
            try:
                # -- unpack ---------------------------------------------------------------------
                kind = type(record)
                if kind is dict:
                    if not _DICT_KEYS.issuperset(record):
                        self._bad_dict_keys(record)
                    measurement = record["measurement"]
                    tags = record.get("tags")
                    fields = record["fields"]
                    timestamp = record.get("time")
                elif kind is Point:
                    measurement = record.measurement
                    tags = record.tags
                    fields = record.fields
                    timestamp = record.timestamp
                elif kind is str or kind is bytes:
                    raw = self._raw_lines(record, database, precision, now_ns, now, index)
                    for line in raw.lines:
                        append(line)
                    nbytes += raw.nbytes
                    dropped += raw.dropped
                    rejected += raw.rejected
                    continue
                else:
                    measurement, tags, fields, timestamp = self._unpack_other(record, database)

                # -- plan ------------------------------------------------------------------------------
                plan = plans_get(measurement) if type(measurement) is str else None
                if plan is None:
                    plan = self._plan(database, measurement)
                    measurement = plan.measurement
                if type(fields) is not dict:
                    fields = self._as_mapping(fields, "fields", measurement)
                if tags is not None and type(tags) is not dict:
                    tags = self._as_mapping(tags, "tags", measurement)

                # -- tags: injection, then the cached serialized tag set ------------------------------
                context = context_get()
                if plan.rules or enrich or context is not None:
                    # Content-dependent injection: cache by the final tags.
                    tags = injector.apply(measurement, tags, fields, plan.rules, context)
                    cache = plan.tagsets
                    injected = False
                elif static:
                    # Only constant static tags: cache by the point's own tags (skips the merge).
                    cache = plan.static_tagsets
                    injected = True
                else:
                    cache = plan.tagsets
                    injected = False
                if tags or injected:
                    head = None
                    signature: tuple[tuple[str, Any], ...] | None = tuple(tags.items()) if tags else ()
                    if cache is not None:
                        try:
                            head = cache.get(signature)  # type: ignore[arg-type]
                        except TypeError:  # unhashable tag value: the slow path reports it
                            signature = None
                    if head is None:
                        final = injector.apply(measurement, tags, fields, (), None) if injected else tags
                        head = self._tagset(plan, final or {}, signature, cache, injected, journal)
                elif plan.required_tags:
                    head = self._tagset(plan, {}, None, None, False, journal)
                else:
                    head = plan.prefix

                # -- fields ---------------------------------------------------------------------------
                field_info = plan.fields
                out = []
                for key, value in fields.items():
                    info = field_info.get(key)
                    if info is not None:
                        prefix, expected = info
                        vt = type(value)
                        if vt is float:
                            if value - value == 0.0:  # finite (inf - inf and nan - nan are nan)
                                if expected is FLOAT or expected is None:
                                    out.append(prefix + repr(value))
                                    continue
                            elif skip_non_finite:
                                continue
                        elif vt is int:
                            if (expected is INTEGER or (expected is None and int_untyped_ok)) and (
                                _INT64_MIN <= value <= _INT64_MAX
                            ):
                                out.append(f"{prefix}{value}i")
                                continue
                        elif vt is str:
                            if (expected is STRING or expected is None) and len(value) <= max_chars:
                                out.append(f'{prefix}"{value.translate(escapes)}"')
                                continue
                        elif vt is bool:
                            if expected is BOOLEAN or expected is None:
                                out.append(prefix + ("true" if value else "false"))
                                continue
                        elif value is None:
                            continue
                    elif value is None:
                        continue
                    text = self._field(plan, key, value, tags, journal)
                    if text is not None:
                        out.append(text)
                if not out:
                    raise ValidationError(
                        "record has no fields (all were None, NaN or missing)",
                        code="no_fields",
                        measurement=measurement,
                    )
                if plan.required_fields:
                    self._check_required_fields(plan, out)

                # -- timestamp -------------------------------------------------------------------------
                if timestamp is None:
                    if now is None:
                        line = f"{head} {','.join(out)}"
                    else:
                        line = f"{head} {','.join(out)} {now}"
                        if first_untimed is None:
                            first_untimed = (head, fields)
                        else:
                            if untimed is None:
                                untimed = {first_untimed[0]: _written_keys(first_untimed[1])}
                            seen = untimed.get(head)
                            if seen is None:
                                untimed[head] = _written_keys(fields)
                            else:
                                written = _written_keys(fields)
                                if not seen.isdisjoint(written):
                                    overwritten += 1
                                seen.update(written)
                elif type(timestamp) is int and stamp_lo <= timestamp <= stamp_hi:
                    line = f"{head} {','.join(out)} {timestamp}"
                else:
                    line = f"{head} {','.join(out)} {self._timestamp(timestamp, divisor, measurement)}"
                append(line)
                nbytes += (len(line) if line.isascii() else len(line.encode("utf-8"))) + 1
                if journal:
                    journal.clear()
            except ValidationError as error:
                if journal:
                    _undo(journal)
                if error.index is None:
                    error.index = index
                if self._raise:
                    raise
                dropped += 1
                rejected.append(error)
                if self.on_drop is not None:
                    self.on_drop(error, 1)
            except KeyError as error:
                if journal:
                    _undo(journal)
                key = error.args[0] if error.args else None
                if not (
                    isinstance(record, Mapping) and key in ("measurement", "fields") and key not in record
                ):
                    raise  # a KeyError from elsewhere (e.g. an enricher) is a bug, not invalid data
                invalid = ValidationError(
                    f"record is missing required key {key!r}", code="malformed_record", index=index
                )
                if self._raise:
                    raise invalid from None
                dropped += 1
                rejected.append(invalid)
                if self.on_drop is not None:
                    self.on_drop(invalid, 1)
        if overwritten:
            self._warnings.log(
                "untimed_overwrite",
                logging.WARNING,
                "%d points without a timestamp in one write() repeat the series and field of another point "
                "in the same call; they all get the write() time, so InfluxDB keeps only the last of each. "
                "Give such points timestamps (or distinguishing tags)",
                overwritten,
            )
        return SerializedChunk(lines, nbytes, dropped, rejected)

    def seed_types(self, database: str, measurement: str, types: Mapping[str, FieldType]) -> None:
        """Lock field types learned elsewhere (e.g. from the server's schema)."""
        plan = self._plan(database, measurement)
        for key, kind in types.items():
            plan.types.setdefault(key, kind)

    def seed_tag_keys(self, database: str, measurement: str, keys: Iterable[str]) -> None:
        """Record tag keys known to exist (used to detect tag/field name clashes)."""
        plan = self._plan(database, measurement)
        plan.tag_keys.update(keys)

    def relock(self, database: str, measurement: str, key: str, kind: FieldType) -> FieldType | None:
        """Lock ``key`` to the type the server stores; returns the previous lock if it changed.

        Declared schema types are never changed (the declaration wins).
        """
        if not self._lock_types:
            return None
        plan = self._plan(database, measurement)
        if key in plan.declared:
            return None
        previous = plan.types.get(key)
        if previous is kind:
            return None
        plan.types[key] = kind
        info = plan.fields.get(key)
        if info is not None:
            plan.fields[key] = (info[0], kind)
        return previous

    def locked_types(self, database: str, measurement: str) -> dict[str, FieldType]:
        """Field types currently declared or locked for a measurement."""
        plan = self._plans.get(database, {}).get(measurement)
        return dict(plan.types) if plan else {}

    # ------------------------------------------------------------------------------------
    # Plans and tags
    # ------------------------------------------------------------------------------------

    def _plan(self, database: str, measurement: Any) -> _Plan:
        if type(measurement) is not str:
            if isinstance(measurement, Enum):
                measurement = measurement.value  # str() of a (str, Enum) member is "Class.MEMBER"
            if not isinstance(measurement, str):
                raise ValidationError(
                    f"measurement must be a string, got {type(measurement).__name__}", code="invalid_name"
                )
            measurement = str(measurement)  # other str subclasses
            existing = self._plans.get(database, {}).get(measurement)
            if existing is not None:
                return existing
        problem = self.dialect.measurement_problem(measurement)
        if problem:
            raise ValidationError(f"measurement name {problem}", code="invalid_name", measurement=measurement)
        schema = self.schemas.get(measurement)
        if schema is None and self.validation.unknown_measurements == "reject":
            raise ValidationError(
                "measurement is not declared under [measurements] "
                "and validation.unknown_measurements = 'reject'",
                code="unknown_measurement",
                measurement=measurement,
            )
        plan = _Plan(
            measurement,
            self.dialect.escape_measurement(measurement),
            schema,
            self.injector.rules_for(measurement),
        )
        # setdefault is atomic, so concurrent first writers of a measurement share one plan.
        return self._plans.setdefault(database, {}).setdefault(measurement, plan)

    def _tagset(
        self,
        plan: _Plan,
        tags: Mapping[str, Any],
        signature: tuple[tuple[str, Any], ...] | None,
        cache: dict[tuple[tuple[str, Any], ...], str] | None,
        static_cache: bool,
        journal: list[tuple[Any, Any]] | None = None,
    ) -> str:
        """Serialize and validate a complete tag set, caching it under ``signature``."""
        fragments = self._fragments
        parts = [plan.prefix]
        present: list[str] = []
        cacheable = signature is not None and cache is not None
        try:
            ordered = sorted(tags) if len(tags) > 1 else list(tags)
        except TypeError:
            raise ValidationError(
                "tag keys must be strings", code="invalid_name", measurement=plan.measurement
            ) from None
        for key in ordered:
            value = tags[key]
            if type(value) is not str:
                if value is None:
                    continue
                cacheable = False  # 1 == 1.0 == True would collide in the cache
                value = self._tag_value_to_str(plan, key, value)
            if not value:
                continue
            fragment = fragments.get((key, value))
            if fragment is None:
                fragment = self._new_fragment(plan, key, value, journal)
            elif self._check_overlap and key not in plan.tag_keys:
                self._register_tag_key(plan, key, journal)
            parts.append(fragment)
            present.append(key)
        if plan.allowed_tags is not None or plan.required_tags:
            self._check_tag_schema(plan, present)
        head = "".join(parts)
        if cacheable:
            assert cache is not None
            if len(cache) >= _TAGSET_CACHE_LIMIT:
                # A full cache means many distinct tag sets. Allow a few refills (the working set
                # may have moved on); after _TAGSET_CACHE_FILLS fills the cardinality is too high
                # to pay off.
                plan.tagset_fills += 1
                if plan.tagset_fills >= _TAGSET_CACHE_FILLS:
                    if static_cache:
                        plan.static_tagsets = None
                    else:
                        plan.tagsets = None
                    self._warnings.log(
                        f"cardinality:{plan.measurement}",
                        logging.WARNING,
                        "measurement %r has had over %d distinct tag sets in this process: every tag "
                        "set is a series, and high series cardinality slows InfluxDB down (InfluxDB 2 "
                        "keeps an index of all series in memory). Store unbounded values such as "
                        "request ids, user ids or timestamps as fields, not tags",
                        plan.measurement,
                        _TAGSET_CACHE_LIMIT * _TAGSET_CACHE_FILLS,
                        extra={"influx": {"measurement": plan.measurement}},
                    )
                    return head
                cache.clear()
            cache[signature] = head  # type: ignore[index]
            if journal is not None:
                journal.append((cache, signature))
        return head

    def _new_fragment(
        self, plan: _Plan, key: Any, value: str, journal: list[tuple[Any, Any]] | None = None
    ) -> str:
        dialect = self.dialect
        if type(key) is not str:
            raise ValidationError(
                f"tag keys must be strings, got {type(key).__name__}",
                code="invalid_name",
                measurement=plan.measurement,
            )
        problem = dialect.identifier_problem(key)
        if problem:
            raise ValidationError(
                f"tag key {problem}", code="invalid_name", measurement=plan.measurement, key=key
            )
        if key in dialect.reserved_tag_keys:
            raise ValidationError(
                f"tag key {key!r} is reserved by InfluxDB {dialect.version}",
                code="reserved_name",
                measurement=plan.measurement,
                key=key,
            )
        problem = dialect.identifier_problem(value)
        if problem:
            raise ValidationError(
                f"tag value {problem}", code="invalid_tag_value", measurement=plan.measurement, key=key
            )
        if self._check_overlap and key not in plan.tag_keys:
            self._register_tag_key(plan, key, journal)
        fragment = f",{dialect.escape_key(key)}={dialect.escape_key(value)}"
        fragments = self._fragments
        if len(fragments) >= _FRAGMENT_CACHE_LIMIT:
            fragments.clear()
        fragments[(key, value)] = fragment
        return fragment

    def _register_tag_key(self, plan: _Plan, key: str, journal: list[tuple[Any, Any]] | None = None) -> None:
        if key in plan.fields or key in plan.declared:
            raise ValidationError(
                f"{key!r} is a field of this measurement; InfluxDB 3 does not allow a tag with the same name",
                code="tag_field_conflict",
                measurement=plan.measurement,
                key=key,
            )
        plan.tag_keys.add(key)
        if journal is not None:
            journal.append((plan.tag_keys, key))

    def _tag_value_to_str(self, plan: _Plan, key: str, value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, Enum):
            return self._tag_value_to_str(plan, key, value.value)
        if isinstance(value, str):
            return str(value)
        if isinstance(value, int):
            return str(int(value))
        if isinstance(value, float):
            if math.isnan(value):  # NaN is how pandas/numpy spell "missing": omit the tag, like None
                return ""
            if not _isfinite(value):
                raise ValidationError(
                    f"tag value {value!r} is not finite",
                    code="invalid_tag_value",
                    measurement=plan.measurement,
                    key=key,
                )
            return repr(float(value))
        item = getattr(value, "item", None)
        if callable(item) and not isinstance(value, Mapping | list | tuple | bytes):
            return self._tag_value_to_str(plan, key, item())  # numpy / pandas scalar
        raise ValidationError(
            f"tag values must be strings (or numbers/booleans/enums), got {type(value).__name__}",
            code="invalid_tag_value",
            measurement=plan.measurement,
            key=key,
        )

    def _check_tag_schema(self, plan: _Plan, written: Iterable[str]) -> None:
        """Check the tag keys actually written (None, "" and NaN values are omitted)."""
        present = set(written)
        if plan.allowed_tags is not None:
            extra = present - plan.allowed_tags
            if extra:
                raise ValidationError(
                    f"tags {sorted(extra)} are not allowed by the schema "
                    f"(allowed: {sorted(plan.allowed_tags)})",
                    code="unexpected_tag",
                    measurement=plan.measurement,
                    key=min(extra),
                )
        missing = plan.required_tags - present
        if missing:
            raise ValidationError(
                f"required tags {sorted(missing)} are missing",
                code="missing_tag",
                measurement=plan.measurement,
                key=min(missing),
            )

    def _check_required_fields(self, plan: _Plan, written: list[str]) -> None:
        """Check the formatted fields (skipped None/NaN values do not count as present)."""
        missing = []
        for key in plan.required_fields:
            info = plan.fields.get(key)
            if info is None or not any(item.startswith(info[0]) for item in written):
                missing.append(key)
        if missing:
            raise ValidationError(
                f"required fields {sorted(missing)} are missing",
                code="missing_field",
                measurement=plan.measurement,
                key=min(missing),
            )

    # ------------------------------------------------------------------------------------
    # Fields
    # ------------------------------------------------------------------------------------

    def _field(
        self,
        plan: _Plan,
        key: Any,
        value: Any,
        tags: Mapping[str, Any] | None,
        journal: list[tuple[Any, Any]] | None = None,
    ) -> str | None:
        """General path for one field: first sightings, coercion, unusual types, errors."""
        info = plan.fields.get(key)
        prefix = self._new_field_key(plan, key, tags) if info is None else info[0]
        text = self._field_value(plan, key, value, journal)
        if text is None:
            return None
        if info is None:
            # Record the key with its expected type so later values take the fast path.
            entry = (prefix, plan.types.get(key))
            if plan.fields.setdefault(key, entry) is entry and journal is not None:
                journal.append((plan.fields, key))
        return prefix + text

    def _new_field_key(self, plan: _Plan, key: Any, tags: Mapping[str, Any] | None) -> str:
        dialect = self.dialect
        if type(key) is not str:
            raise ValidationError(
                f"field keys must be strings, got {type(key).__name__}",
                code="invalid_name",
                measurement=plan.measurement,
            )
        problem = dialect.identifier_problem(key)
        if problem:
            raise ValidationError(
                f"field key {problem}", code="invalid_name", measurement=plan.measurement, key=key
            )
        if key in dialect.reserved_field_keys:
            raise ValidationError(
                f"field key {key!r} is reserved by InfluxDB {dialect.version}",
                code="reserved_name",
                measurement=plan.measurement,
                key=key,
            )
        if plan.forbid_extra and key not in plan.declared:
            raise ValidationError(
                f"field {key!r} is not declared in the schema and extra_fields = 'forbid'",
                code="unexpected_field",
                measurement=plan.measurement,
                key=key,
            )
        if self._check_overlap and (
            key in plan.tag_keys or (tags is not None and tags.get(key) not in (None, ""))
        ):
            raise ValidationError(
                f"{key!r} is a tag of this measurement; InfluxDB 3 does not allow a field with the same name",
                code="tag_field_conflict",
                measurement=plan.measurement,
                key=key,
            )
        return dialect.escape_key(key) + "="

    def _field_value(
        self, plan: _Plan, key: str, value: Any, journal: list[tuple[Any, Any]] | None = None
    ) -> str | None:
        """Format any supported value; None means 'skip this field'."""
        kind = type(value)
        if kind is float:
            if not _isfinite(value):
                self._non_finite(plan, key, value)
                return None
            return self._typed(plan, key, value, FLOAT, journal)
        if kind is int:
            return self._typed(plan, key, value, FLOAT if self._int_as_float else INTEGER, journal)
        if kind is str:
            return self._typed(plan, key, value, STRING, journal)
        if kind is bool:
            return self._typed(plan, key, value, BOOLEAN, journal)
        if value is None:
            return None
        return self._other_field(plan, key, value, journal)

    def _non_finite(self, plan: _Plan, key: str, value: float) -> None:
        """Skip a NaN/inf field (returns) or reject the record (raises), per validation.non_finite."""
        if self._skip_non_finite:
            return
        raise ValidationError(
            f"field value {value!r} is not finite", code="non_finite", measurement=plan.measurement, key=key
        )

    def _typed(
        self,
        plan: _Plan,
        key: str,
        value: Any,
        natural: FieldType,
        journal: list[tuple[Any, Any]] | None = None,
    ) -> str:
        """Format ``value`` (whose natural type is ``natural``) as the field's expected type."""
        expected = plan.types.get(key)
        if expected is None:
            if self._lock_types:
                expected = plan.types.setdefault(key, natural)
                if expected is natural and journal is not None:
                    journal.append((plan.types, key))
            else:
                expected = natural
        if expected is natural:
            return self._format(plan, key, value, natural)
        coerced = self._coerce_value(value, expected) if self._coerce else _NO_COERCION
        if coerced is _NO_COERCION:
            origin = "declared" if key in plan.declared else "locked"
            hint = ""
            if natural is FLOAT and expected is INTEGER:
                hint = "; declare the field as float or set validation.int_as_float = true"
            elif natural is INTEGER and expected is FLOAT and not self._coerce:
                hint = "; set validation.coerce = true to convert ints to floats"
            raise ValidationError(
                f"field type conflict: {value!r} is {natural.value} but the field is {origin} "
                f"as {expected.value}{hint}",
                code="type_conflict",
                measurement=plan.measurement,
                key=key,
            )
        return self._format(plan, key, coerced, expected)

    @staticmethod
    def _coerce_value(value: Any, expected: FieldType) -> Any:
        if isinstance(value, bool):
            return _NO_COERCION  # never between booleans and numbers
        if expected is FLOAT:
            if isinstance(value, int) and -_FLOAT_EXACT_INT <= value <= _FLOAT_EXACT_INT:
                return float(value)
            return _NO_COERCION
        if expected is INTEGER or expected is UINTEGER:
            if isinstance(value, int):
                return int(value)
            if isinstance(value, float) and value.is_integer():
                return int(value)
        return _NO_COERCION

    def _format(self, plan: _Plan, key: str, value: Any, kind: FieldType) -> str:
        if kind is FLOAT:
            if type(value) is int and not -_FLOAT_EXACT_INT <= value <= _FLOAT_EXACT_INT:
                raise ValidationError(
                    f"integer {value} cannot be written exactly as a float (validation.int_as_float); "
                    "declare the field as integer",
                    code="out_of_range",
                    measurement=plan.measurement,
                    key=key,
                )
            return repr(float(value))
        if kind is INTEGER:
            if not _INT64_MIN <= value <= _INT64_MAX:
                raise ValidationError(
                    f"integer {value} does not fit in int64",
                    code="out_of_range",
                    measurement=plan.measurement,
                    key=key,
                )
            return f"{int(value)}i"
        if kind is UINTEGER:
            if not 0 <= value <= _UINT64_MAX:
                raise ValidationError(
                    f"unsigned integer {value} does not fit in uint64",
                    code="out_of_range",
                    measurement=plan.measurement,
                    key=key,
                )
            return f"{int(value)}u"
        if kind is STRING:
            if len(value) > self._max_string_chars_fast and len(value.encode("utf-8")) > self._max_string:
                raise ValidationError(
                    f"string field is longer than {self._max_string} bytes",
                    code="string_too_long",
                    measurement=plan.measurement,
                    key=key,
                )
            return '"' + str(value).translate(STRING_ESCAPES) + '"'
        return "true" if value else "false"

    def _other_field(
        self, plan: _Plan, key: str, value: Any, journal: list[tuple[Any, Any]] | None = None
    ) -> str | None:
        """Unusual value types: UInt, str/int/float subclasses, enums, Decimal, numpy scalars."""
        if isinstance(value, UInt):
            return self._typed(plan, key, int(value), UINTEGER, journal)
        if isinstance(value, Enum):
            value = value.value
        elif isinstance(value, Decimal):
            if not self._coerce:
                raise ValidationError(
                    "Decimal field values need validation.coerce = true (they are written as floats)",
                    code="unsupported_type",
                    measurement=plan.measurement,
                    key=key,
                )
            value = float(value)
        elif not isinstance(value, str | int | float):
            item = getattr(value, "item", None)
            if callable(item) and not isinstance(value, Mapping | list | tuple | bytes):
                value = item()  # numpy / pandas scalar -> Python scalar
        if value is None:
            return None
        if isinstance(value, bool):
            return self._typed(plan, key, bool(value), BOOLEAN, journal)
        if isinstance(value, float):
            value = float(value)
            if not _isfinite(value):
                self._non_finite(plan, key, value)
                return None
            return self._typed(plan, key, value, FLOAT, journal)
        if isinstance(value, int):
            return self._typed(plan, key, int(value), FLOAT if self._int_as_float else INTEGER, journal)
        if isinstance(value, str):
            return self._typed(plan, key, str(value), STRING, journal)
        raise ValidationError(
            f"unsupported field value type {type(value).__name__}; use float, int, str or bool",
            code="unsupported_type",
            measurement=plan.measurement,
            key=key,
        )

    # ------------------------------------------------------------------------------------
    # Timestamps
    # ------------------------------------------------------------------------------------

    def _timestamp(self, value: Any, divisor: int, measurement: str) -> int:
        if isinstance(value, datetime):
            last, last_ns = self._last_datetime
            if value is last:
                return last_ns // divisor
            nanos = getattr(value, "value", None)  # pandas.Timestamp carries exact nanoseconds
            if value.tzinfo is None:
                if not self._naive_utc:
                    raise ValidationError(
                        "naive datetime is ambiguous; use a timezone-aware datetime or set "
                        "validation.naive_datetime = 'utc'",
                        code="naive_datetime",
                        measurement=measurement,
                    )
                if not isinstance(nanos, int):
                    nanos = (value.replace(tzinfo=UTC) - _EPOCH) // _ONE_US * 1000
            elif not isinstance(nanos, int):
                nanos = (value - _EPOCH) // _ONE_US * 1000
            self._check_nanos(nanos, value, measurement)
            self._last_datetime = (value, nanos)  # one atomic store: safe across threads
            return nanos // divisor
        if isinstance(value, str):
            nanos = self._parse_iso(value, measurement)
            self._check_nanos(nanos, value, measurement)
            return nanos // divisor
        if isinstance(value, float):
            if not _isfinite(value):
                raise ValidationError(
                    f"timestamp {value!r} is not finite", code="invalid_time", measurement=measurement
                )
            nanos = round(value * 1e9)
            if not _INT64_MIN <= nanos <= _INT64_MAX:
                raise ValidationError(
                    f"float timestamp {value!r} is out of range: float timestamps are epoch seconds "
                    "(use an int in the write precision for other units)",
                    code="invalid_time",
                    measurement=measurement,
                )
            return nanos // divisor
        if isinstance(value, int) and not isinstance(value, bool):
            return self._int_timestamp(int(value), divisor, measurement)
        if isinstance(value, date):
            raise ValidationError(
                "a date is not a timestamp; pass a timezone-aware datetime",
                code="invalid_time",
                measurement=measurement,
            )
        dtype = getattr(value, "dtype", None)
        kind = getattr(dtype, "kind", None)
        if kind == "M":  # numpy.datetime64 (UTC by convention)
            try:
                # Casting a coarse unit straight to ns can wrap around silently: go through us.
                if str(dtype).endswith(("[ns]", "[ps]", "[fs]", "[as]")):
                    nanos = int(value.astype("datetime64[ns]").astype("int64"))
                else:
                    micros = int(value.astype("datetime64[us]").astype("int64"))
                    nanos = None if micros == _NAT else micros * 1000
            except (OverflowError, ValueError):
                nanos = None
            if nanos is None or nanos == _NAT or not _INT64_MIN < nanos <= _INT64_MAX:
                raise ValidationError(
                    f"timestamp {value} is outside the range InfluxDB can store (years 1677 to 2262)",
                    code="invalid_time",
                    measurement=measurement,
                )
            return nanos // divisor
        if kind in ("i", "u"):  # numpy integer scalar
            return self._int_timestamp(operator.index(value), divisor, measurement)
        raise ValidationError(
            f"unsupported timestamp type {type(value).__name__}", code="invalid_time", measurement=measurement
        )

    @staticmethod
    def _check_nanos(nanos: int, value: Any, measurement: str) -> None:
        if not _INT64_MIN <= nanos <= _INT64_MAX:
            raise ValidationError(
                f"timestamp {value} is outside the range InfluxDB can store (years 1677 to 2262)",
                code="invalid_time",
                measurement=measurement,
            )

    def _int_timestamp(self, stamp: int, divisor: int, measurement: str) -> int:
        """An integer timestamp outside the fast path's range: reject or warn."""
        unit = _PRECISION_NAMES[divisor]
        if not _INT64_MIN // divisor <= stamp <= _INT64_MAX // divisor:
            raise ValidationError(
                f"timestamp {stamp} with precision {unit!r} is outside the range InfluxDB can store; "
                "is the precision right?",
                code="invalid_time",
                measurement=measurement,
            )
        if stamp < _PLAUSIBLE_NS // divisor:
            when = datetime.fromtimestamp(stamp * divisor / 1e9, UTC).isoformat()
            self._warnings.log(
                f"implausible_time:{unit}",
                logging.WARNING,
                "integer timestamp %d with precision %r means %s; if that is not intended the "
                "precision is wrong (e.g. epoch seconds need precision='s')",
                stamp,
                unit,
                when,
            )
        return stamp

    def _parse_iso(self, text: str, measurement: str) -> int:
        fraction_ns = 0
        cleaned = text.strip()
        match = _ISO_FRACTION.match(cleaned)
        if match:
            fraction_ns = int(match.group(2)[:9].ljust(9, "0"))
            cleaned = match.group(1) + match.group(3)
        try:
            parsed = datetime.fromisoformat(cleaned)
        except ValueError:
            raise ValidationError(
                f"timestamp {text!r} is not ISO 8601", code="invalid_time", measurement=measurement
            ) from None
        if parsed.tzinfo is None:
            if not self._naive_utc:
                raise ValidationError(
                    f"timestamp {text!r} has no UTC offset; add 'Z' or an offset, or set "
                    "validation.naive_datetime = 'utc'",
                    code="naive_datetime",
                    measurement=measurement,
                )
            parsed = parsed.replace(tzinfo=UTC)
        return (parsed - _EPOCH) // _ONE_US * 1000 + fraction_ns

    # ------------------------------------------------------------------------------------
    # Records other than dict / Point
    # ------------------------------------------------------------------------------------

    @staticmethod
    def _as_mapping(value: Any, what: str, measurement: Any) -> dict[str, Any]:
        if isinstance(value, Mapping):
            return dict(value)
        raise ValidationError(
            f"{what} must be a mapping, got {type(value).__name__}",
            code="malformed_record",
            measurement=measurement if isinstance(measurement, str) else None,
        )

    @staticmethod
    def _bad_dict_keys(record: Mapping[Any, Any]) -> None:
        unknown = sorted(str(key) for key in record if key not in _DICT_KEYS)
        measurement = record.get("measurement")
        raise ValidationError(
            f"unexpected keys {unknown} in record; expected measurement, tags, fields and time",
            code="malformed_record",
            measurement=measurement if isinstance(measurement, str) else None,
        )

    def _unpack_other(self, record: Any, database: str) -> tuple[Any, Any, Any, Any]:
        spec = getattr(type(record), "__sluicebox_model__", None)
        if spec is not None:
            if (database, type(record)) not in self._seeded_models:
                # A model's annotations declare its field types: lock them before the first write.
                self.seed_types(database, spec.measurement, spec.field_types)
                self._seeded_models.add((database, type(record)))
            result: tuple[Any, Any, Any, Any] = spec.extract(record)
            return result
        if isinstance(record, Point):
            return record.measurement, record.tags, record.fields, record.timestamp
        if isinstance(record, Mapping):
            if not _DICT_KEYS.issuperset(record):
                self._bad_dict_keys(record)
            return record["measurement"], record.get("tags"), record["fields"], record.get("time")
        hint = ""
        if hasattr(record, "__dataclass_fields__") or hasattr(type(record), "model_fields"):
            hint = "; decorate the class with @sluicebox.measurement to write it directly"
        raise ValidationError(
            f"cannot write a {type(record).__name__}: expected Point, dict, line protocol str/bytes "
            f"or a @measurement model{hint}",
            code="unsupported_record",
        )

    def _raw_lines(
        self,
        data: str | bytes,
        database: str,
        precision: str,
        now_ns: int | None,
        now: int | None,
        index: int,
    ) -> SerializedChunk:
        if type(data) is bytes:
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ValidationError(
                    f"line protocol is not valid UTF-8 ({exc.reason} at byte {exc.start})",
                    code="invalid_encoding",
                ) from None
        else:
            text = data  # type: ignore[assignment]
        candidates = self.dialect.split_lines(text)
        if not candidates:
            return SerializedChunk([], 0, 0)
        if not self._validate_raw:
            if now is not None:
                # Untimed lines get the write() time too, so that a retry overwrites them.
                stamp = f" {now}"
                candidates = [
                    line if line[line.rfind(" ") + 1 :].isdigit() else _with_timestamp(line, stamp)
                    for line in candidates
                ]
            if text.isascii():
                nbytes = sum(map(len, candidates))
            else:
                nbytes = sum(len(line.encode("utf-8")) for line in candidates)
            return SerializedChunk(candidates, nbytes + len(candidates), 0)
        records = []
        for line in candidates:
            if not line.strip():
                continue
            try:
                parsed = parse_line(self.dialect, line)
            except LineSyntaxError as exc:
                raise ValidationError(
                    f"invalid line protocol: {exc}: {line[:200]!r}", code="invalid_line"
                ) from None
            records.append(
                {
                    "measurement": parsed.measurement,
                    "tags": parsed.tags,
                    "fields": parsed.fields,
                    "time": parsed.timestamp,
                }
            )
        return self.serialize(
            records, database=database, precision=precision, start_index=index, now_ns=now_ns
        )


def _with_timestamp(line: str, stamp: str) -> str:
    """Raw ``line`` with ``stamp`` (a space and the time) appended unless it has a timestamp.

    The timestamp is the last space-separated token: a field set always contains ``=``, and a
    space inside a string field value is followed by its closing quote.
    """
    body = line.rstrip(" \t")
    tail = body[body.rfind(" ") + 1 :]
    if tail.isdigit() or (tail[:1] == "-" and tail[1:].isdigit()):
        return line
    return body + stamp


def _written_keys(fields: Mapping[str, Any]) -> set[Any]:
    """Field keys that produce output (None and non-finite floats are skipped)."""
    return {
        key
        for key, value in fields.items()
        if value is not None and not (type(value) is float and value - value != 0.0)
    }


def _undo(journal: list[tuple[Any, Any]]) -> None:
    """Remove the plan and cache entries a rejected record created."""
    for container, key in reversed(journal):
        if isinstance(container, set):
            container.discard(key)
        else:
            container.pop(key, None)
    journal.clear()
