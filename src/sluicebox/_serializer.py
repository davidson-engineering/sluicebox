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

Threads. What every thread must agree on - field type locks, and the tag and field keys
written to each measurement - is kept in shared plans (:class:`_Plan`) and changed under one
lock, only once a record that needs the change is written: a rejected record locks nothing,
and no other thread ever sees a change that is taken back. The loop itself runs in a
:class:`_View`, whose caches (:class:`_LocalPlan`) hold what it learned from the shared plans;
on free-threaded Python each thread has a view of its own, so threads serializing at once
share no object on the hot path (see ``_threads``). A change that makes cache entries stale
(a type the server reported) is pushed to every view.
"""

from __future__ import annotations

import logging
import math
import operator
import os
import re
import sys
import threading
import time
import weakref
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from . import _fork, _threads
from . import tags as _tags
from ._lineprotocol import STRING_ESCAPES, Dialect, LineSyntaxError, UInt, line_end_problem, parse_line
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

# The views' field caches hold a field's expected type by name: the enum's value, a string
# constant, which free-threaded Python makes immortal, so threads compare it on every value
# without touching a shared reference count (as they would comparing enum members).
_AS_FLOAT = FLOAT.value
_AS_INTEGER = INTEGER.value
_AS_STRING = STRING.value
_AS_BOOLEAN = BOOLEAN.value

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
_INTEGER_TYPES = (INTEGER, UINTEGER)

# What a record changes once it is written (journal entries, see _View._commit).
_FIELD = 0  # (_FIELD, key, escaped "key=", the type it is written as, whether this locks it)
_TAG_KEY = 1  # (_TAG_KEY, key): a tag key the measurement had not been written with
_TAGSET = 2  # (_TAGSET, cache, signature, serialized head, whether it is the static-tags cache)

_isfinite = math.isfinite

#: Guards changes to the shared plans of every Serializer. They happen only when a thread
#: first meets a measurement, field or tag key (or the server reports a type), so one lock is
#: plenty. Held across fork(), so that a child never inherits half a change.
_SCHEMA_LOCK = threading.Lock()


def _release_schema_lock() -> None:
    _SCHEMA_LOCK.release()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(before=_SCHEMA_LOCK.acquire, after_in_parent=_release_schema_lock)
    # Before the child's state is reset: that may run callbacks that write.
    _fork.on_child(locks=_release_schema_lock)


@dataclass(slots=True)
class SerializedChunk:
    """Lines produced from one chunk of input records."""

    lines: list[str]
    nbytes: int
    dropped: int
    #: Why records were dropped (``on_invalid = "drop"``), in input order.
    rejected: Sequence[ValidationError] = ()


class _Plan:
    """What every thread knows about one (database, measurement).

    ``types``, ``tag_keys`` and ``field_keys`` change under ``_SCHEMA_LOCK`` only; the rest
    never changes.
    """

    __slots__ = (
        "allowed_tags",
        "declared",
        "field_keys",
        "forbid_extra",
        "locals",
        "measurement",
        "prefix",
        "required_fields",
        "required_tags",
        "rules",
        "tag_keys",
        "types",
    )

    def __init__(self, measurement: str, prefix: str, schema: MeasurementSchema | None, rules: Any) -> None:
        self.measurement = measurement
        # Interned: every thread reads it without touching a shared reference count.
        self.prefix = sys.intern(prefix)
        #: Declared + locked field types; grows as new fields are written.
        self.types: dict[str, FieldType] = dict(schema.fields) if schema else {}
        self.declared: frozenset[str] = frozenset(schema.fields) if schema else frozenset()
        #: Tag and field keys written (tracked only where tag/field name overlap is forbidden).
        self.tag_keys: set[str] = set()
        self.field_keys: set[str] = set()
        self.rules = rules
        self.allowed_tags = schema.tags if schema else None
        self.required_tags = schema.required_tags if schema else frozenset()
        self.required_fields = schema.required_fields if schema else frozenset()
        self.forbid_extra = bool(schema and schema.extra_fields == "forbid")
        #: The views' caches of this plan, to drop entries that a change of ``types`` makes stale.
        self.locals: dict[int, weakref.ref[_LocalPlan]] = {}

    def local_plans(self) -> list[_LocalPlan]:
        refs = self.locals.copy()  # one atomic copy: views come and go with their threads
        return [local for ref in refs.values() if (local := ref()) is not None]


class _LocalPlan:
    """A view's cache of one plan: what the hot loop reads, owned by the view's thread."""

    __slots__ = (
        "__weakref__",
        "fields",
        "measurement",
        "plan",
        "prefix",
        "required",
        "requires_tags",
        "rules",
        "static_tagsets",
        "tag_keys",
        "tagset_fills",
        "tagsets",
        "validates_tags",
    )

    def __init__(self, plan: _Plan, dialect: Dialect) -> None:
        self.plan = plan
        self.measurement = plan.measurement
        self.prefix = plan.prefix
        self.rules = plan.rules
        #: Field key -> ("escaped_key=", expected type's name or None if untyped), as of the
        #: shared type locks.
        self.fields: dict[str, tuple[str, str | None]] = {}
        #: Tag keys known to be registered in the shared plan.
        self.tag_keys: set[str] = set()
        #: (*final tag keys, *their values) -> serialized "measurement,k=v,..." head (None = disabled).
        self.tagsets: dict[tuple[Any, ...], str] | None = {}
        #: Same, keyed by the point's own tags when only (constant) static tags are injected.
        self.static_tagsets: dict[tuple[Any, ...], str] | None = {}
        self.tagset_fills = 0
        #: (key, "escaped_key=") of the required fields.
        self.required = tuple((key, dialect.escape_key(key) + "=") for key in sorted(plan.required_fields))
        self.requires_tags = bool(plan.required_tags)
        self.validates_tags = plan.allowed_tags is not None or bool(plan.required_tags)


class Serializer:
    """Converts records to line protocol for one client. Thread-safe.

    Holds the settings and the shared plans; serializing runs in a :meth:`view`.
    """

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
        self._warnings = RateLimitedLog(logging.getLogger("sluicebox.validation"))
        self._local = threading.local()
        self._shared_view = None if _threads.per_thread() else _View(self)

    # ------------------------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------------------------

    def view(self) -> _View:
        """The view the calling thread serializes through (its own on free-threaded Python)."""
        shared = self._shared_view
        if shared is not None:
            return shared
        local = self._local
        try:
            view: _View = local.view
        except AttributeError:
            view = local.view = _View(self)  # created (and so owned) by this thread
        return view

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
        return self.view().serialize(
            records, database=database, precision=precision, start_index=start_index, now_ns=now_ns
        )

    def seed_types(self, database: str, measurement: str, types: Mapping[str, FieldType]) -> None:
        """Lock field types learned elsewhere (e.g. from the server's schema)."""
        plan = self._plan(database, measurement)
        with _SCHEMA_LOCK:
            for key, kind in types.items():
                plan.types.setdefault(key, kind)
            self._forget_fields(plan, types)

    def seed_tag_keys(self, database: str, measurement: str, keys: Iterable[str]) -> None:
        """Record tag keys known to exist (used to detect tag/field name clashes)."""
        plan = self._plan(database, measurement)
        with _SCHEMA_LOCK:
            plan.tag_keys.update(keys)

    def relock(self, database: str, measurement: str, key: str, kind: FieldType) -> FieldType | None:
        """Lock ``key`` to the type the server stores; returns the previous lock if it changed.

        Declared schema types are never changed (the declaration wins).
        """
        if not self._lock_types:
            return None
        plan = self._plan(database, measurement)
        with _SCHEMA_LOCK:
            if key in plan.declared:
                return None
            previous = plan.types.get(key)
            if previous is kind:
                return None
            plan.types[key] = kind
            self._forget_fields(plan, (key,))
        return previous

    def locked_types(self, database: str, measurement: str) -> dict[str, FieldType]:
        """Field types currently declared or locked for a measurement."""
        plan = self._plans.get(database, {}).get(measurement)
        if plan is None:
            return {}
        with _SCHEMA_LOCK:
            return dict(plan.types)

    # ------------------------------------------------------------------------------------
    # Shared plans
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

    @staticmethod
    def _forget_fields(plan: _Plan, keys: Iterable[str]) -> None:
        """Drop the views' cache entries of fields whose type changed (_SCHEMA_LOCK held)."""
        for local in plan.local_plans():
            for key in keys:
                local.fields.pop(key, None)

    def field_prefix(self, plan: _Plan, key: Any, tags: Mapping[str, Any] | None) -> str:
        """Validate a field key new to a caller and return its escaped ``key=``.

        Raises if the key is not allowed, or is a tag of the measurement where tag and field
        names must differ (as far as known now: the commit checks that again).
        """
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

    def check_tag_key(self, plan: _Plan, key: str) -> None:
        """Raise if ``key``, new to a caller, is a field of the measurement (where tag and field
        names must differ; as far as known now: the commit checks that again)."""
        if key in plan.field_keys or key in plan.declared:
            raise ValidationError(
                f"{key!r} is a field of this measurement; InfluxDB 3 does not allow a tag with the same name",
                code="tag_field_conflict",
                measurement=plan.measurement,
                key=key,
            )

    def commit(
        self,
        plan: _Plan,
        locks: Mapping[str, FieldType],
        tag_keys: Iterable[str] = (),
        field_keys: Iterable[str] = (),
    ) -> bool:
        """Lock new field types and register tag and field keys, all or nothing.

        False (and no change) if another thread locked one of the fields to another type, or
        wrote a tag key as a field or the other way round, since the caller looked.
        """
        tag_keys = tuple(tag_keys)
        field_keys = tuple(field_keys)
        with _SCHEMA_LOCK:
            types = plan.types
            for key, kind in locks.items():
                current = types.get(key)
                if current is not None and current is not kind:
                    return False
            if any(key in plan.field_keys or key in plan.declared for key in tag_keys):
                return False
            if any(key in plan.tag_keys for key in field_keys):
                return False
            for key, kind in locks.items():
                types.setdefault(key, kind)
            plan.tag_keys.update(tag_keys)
            plan.field_keys.update(field_keys)
        return True

    def _int_timestamp(self, stamp: int, divisor: int, measurement: str) -> int:
        """An integer timestamp outside the fast path's range: reject or warn."""
        unit = _PRECISION_NAMES[divisor]
        lowest, highest = self.dialect.time_range(divisor)
        if not lowest <= stamp <= highest:
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


class _View:
    """How one thread (or, with the GIL, every thread) serializes through a :class:`Serializer`.

    The hot loop reads only objects this view's thread owns or immortal ones: its own caches,
    and private copies of the settings and constants it needs.
    """

    __slots__ = (
        "_auto_timestamp",
        "_check_overlap",
        "_coerce",
        "_dialect",
        "_dict_keys",
        "_enrich",
        "_escapes",
        "_fragments",
        "_injector",
        "_int_as_float",
        "_int_max",
        "_int_min",
        "_last_datetime",
        "_lock_types",
        "_max_chars",
        "_max_string",
        "_naive_utc",
        "_plans",
        "_precisions",
        "_raise",
        "_seeded_models",
        "_serializer",
        "_skip_non_finite",
        "_static",
        "_validate_raw",
    )

    def __init__(self, serializer: Serializer) -> None:
        private = _threads.private
        dialect = serializer.dialect
        self._serializer = serializer
        self._dialect = dialect
        self._injector = serializer.injector
        self._raise = serializer._raise
        self._auto_timestamp = serializer.auto_timestamp
        self._lock_types = serializer._lock_types
        self._coerce = serializer._coerce
        self._int_as_float = serializer._int_as_float
        self._skip_non_finite = serializer._skip_non_finite
        self._naive_utc = serializer._naive_utc
        self._validate_raw = serializer._validate_raw
        self._check_overlap = serializer._check_overlap
        self._enrich = bool(serializer.injector.enrichers)
        self._static = bool(serializer.injector.static)
        self._max_string = private(serializer._max_string)
        self._max_chars = private(serializer._max_string_chars_fast)
        self._int_min = private(_INT64_MIN)
        self._int_max = private(_INT64_MAX)
        self._dict_keys = frozenset(list(_DICT_KEYS))
        self._escapes = dict(STRING_ESCAPES)
        #: Per precision: (ns divisor, smallest plausible integer stamp, largest storable stamp).
        self._precisions = {
            name: (private(divisor), _PLAUSIBLE_NS // divisor, private(dialect.time_range(divisor)[1]))
            for name, divisor in PRECISION_DIVISORS.items()
        }
        self._plans: dict[str, dict[str, _LocalPlan]] = {}
        self._fragments: dict[tuple[str, str], str] = {}
        self._last_datetime: tuple[datetime | None, int] = (None, 0)
        self._seeded_models: set[tuple[str, type]] = set()

    def serialize(
        self,
        records: Iterable[Any],
        *,
        database: str,
        precision: str,
        start_index: int = 0,
        now_ns: int | None = None,
    ) -> SerializedChunk:
        """See :meth:`Serializer.serialize`."""
        # Integer timestamps from stamp_lo to stamp_hi take the fast path; others are checked in
        # _timestamp.
        divisor, stamp_lo, stamp_hi = self._precisions[precision]
        now = None
        if self._auto_timestamp:
            if now_ns is None:
                now_ns = time.time_ns()
            now = now_ns // divisor
        lines: list[str] = []
        append = lines.append
        nbytes = 0
        dropped = 0
        rejected: list[ValidationError] | None = None  # a list once a record is dropped
        index = start_index - 1
        # Untimed points of one call share the write() time: detect ones that would overwrite
        # each other (same series and field). Tracking starts lazily at the second such point.
        first_untimed: tuple[str, Any] | None = None
        untimed: dict[str, set[Any]] | None = None
        overwritten = 0
        # What the current record would change - type locks, new tag and field keys, cache
        # entries - applied by _commit once the record is written.
        journal: list[tuple[Any, ...]] = []

        # Hot-loop locals.
        db_plans = self._plans.get(database)
        if db_plans is None:
            db_plans = self._plans.setdefault(database, {})
        plans_get = db_plans.get
        enrich = self._enrich
        static = self._static
        # Context tags cannot change during the call (an enricher's own tag_context ends with it).
        # The context variable is read only once tag_context() was ever used: every thread
        # shares it, and reading it on every write would contend (free-threaded Python).
        context = _CONTEXT_TAGS.get() if _tags._context_used else None
        skip_non_finite = self._skip_non_finite
        max_chars = self._max_chars
        escapes = self._escapes
        dict_keys = self._dict_keys
        int_min = self._int_min
        int_max = self._int_max
        # With type locking every known field has a concrete expected type; without it,
        # undeclared fields are untyped and written as their natural type.
        int_untyped_ok = not self._int_as_float
        # InfluxDB 2 escapes a special character after a literal backslash as "\\ ", which its
        # line splitting reads as an escaped backslash: such lines must be checked.
        check_backslashes = self._dialect.version == 2

        for record in records:
            index += 1
            try:
                # -- unpack ---------------------------------------------------------------------
                kind = type(record)
                if kind is dict:
                    if not dict_keys.issuperset(record):
                        _bad_dict_keys(record)
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
                    lines += raw.lines
                    nbytes += raw.nbytes
                    dropped += raw.dropped
                    if raw.rejected:
                        rejected = [*(rejected or ()), *raw.rejected]
                    continue
                else:
                    measurement, tags, fields, timestamp = self._unpack_other(record, database)

                # -- plan ------------------------------------------------------------------------------
                plan = plans_get(measurement) if type(measurement) is str else None
                if plan is None:
                    plan = self._local_plan(database, measurement)
                    measurement = plan.measurement
                if type(fields) is not dict:
                    fields = _as_mapping(fields, "fields", measurement)
                if tags is not None and type(tags) is not dict:
                    tags = _as_mapping(tags, "tags", measurement)

                # -- tags: injection, then the cached serialized tag set ------------------------------
                rules = plan.rules
                if rules or enrich or context is not None:
                    # Content-dependent injection: cache by the final tags.
                    tags = self._injector.apply(measurement, tags, fields, rules, context)
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
                    # Keys, then values: one tuple, and distinct for distinct tag sets.
                    signature: tuple[Any, ...] | None = (*tags, *tags.values()) if tags else ()
                    if cache is not None:
                        try:
                            head = cache.get(signature)  # type: ignore[arg-type]
                        except TypeError:  # unhashable tag value: the slow path reports it
                            signature = None
                    if head is None:
                        final = (
                            self._injector.apply(measurement, tags, fields, (), None) if injected else tags
                        )
                        head = self._tagset(plan, final or {}, signature, cache, injected, journal)
                elif plan.requires_tags:
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
                                if expected is _AS_FLOAT or expected is None:
                                    out.append(prefix + repr(value))
                                    continue
                            elif skip_non_finite:
                                continue
                        elif vt is int:
                            if (expected is _AS_INTEGER or (expected is None and int_untyped_ok)) and (
                                int_min <= value <= int_max
                            ):
                                out.append(f"{prefix}{value}i")
                                continue
                        elif vt is str:
                            if (expected is _AS_STRING or expected is None) and len(value) <= max_chars:
                                out.append(f'{prefix}"{value.translate(escapes)}"')
                                continue
                        elif vt is bool:
                            if expected is _AS_BOOLEAN or expected is None:
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
                if plan.required:
                    _check_required_fields(plan, out)

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
                if check_backslashes and "\\" in line:
                    _check_v2_line_end(line, measurement)
                size = (len(line) if line.isascii() else _utf8_size(line, fields, measurement)) + 1
                if journal and not self._commit(plan, journal):
                    # Another thread locked a field type or wrote a key this record needed
                    # meanwhile: serialize the record again, against what was decided.
                    retry = self.serialize(
                        (record,), database=database, precision=precision, start_index=index, now_ns=now_ns
                    )
                    lines += retry.lines
                    nbytes += retry.nbytes
                    dropped += retry.dropped
                    if retry.rejected:
                        rejected = [*(rejected or ()), *retry.rejected]
                    continue
                nbytes += size
                append(line)
            except ValidationError as error:
                journal.clear()  # nothing of a rejected record is kept
                if error.index is None:
                    error.index = index
                if self._raise:
                    raise
                dropped += 1
                if rejected is None:
                    rejected = []
                rejected.append(error)
                on_drop = self._serializer.on_drop
                if on_drop is not None:
                    on_drop(error, 1)
            except KeyError as error:
                journal.clear()
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
                if rejected is None:
                    rejected = []
                rejected.append(invalid)
                on_drop = self._serializer.on_drop
                if on_drop is not None:
                    on_drop(invalid, 1)
        if overwritten:
            self._serializer._warnings.log(
                "untimed_overwrite",
                logging.WARNING,
                "%d points without a timestamp in one write() repeat the series and field of another point "
                "in the same call; they all get the write() time, so InfluxDB keeps only the last of each. "
                "Give such points timestamps (or distinguishing tags)",
                overwritten,
            )
        return SerializedChunk(lines, nbytes, dropped, rejected or ())

    def _commit(self, local: _LocalPlan, journal: list[tuple[Any, ...]]) -> bool:
        """Apply what a record that is being written changes; empties ``journal``.

        Shared changes (type locks, tag and field keys) and the field cache entries derived
        from them are made under ``_SCHEMA_LOCK``, all or nothing: False (and no change) if
        another thread decided otherwise since the record looked.
        """
        plan = local.plan
        try:
            if any(entry[0] != _TAGSET for entry in journal):
                check_overlap = self._check_overlap
                with _SCHEMA_LOCK:
                    types = plan.types
                    for entry in journal:
                        kind = entry[0]
                        if kind == _FIELD:
                            _, key, _, written_as, locks = entry
                            current = types.get(key)
                            if current is not written_as and not (locks and current is None):
                                return False  # locked (or relocked) to another type meanwhile
                            if check_overlap and key in plan.tag_keys:
                                return False  # written as a tag meanwhile
                        elif kind == _TAG_KEY and (entry[1] in plan.field_keys or entry[1] in plan.declared):
                            return False  # written as a field meanwhile
                    fields = local.fields
                    for entry in journal:
                        kind = entry[0]
                        if kind == _FIELD:
                            _, key, prefix, written_as, locks = entry
                            if locks:
                                types[key] = written_as
                            if check_overlap:
                                plan.field_keys.add(key)
                            fields[key] = (prefix, None if written_as is None else written_as.value)
                        elif kind == _TAG_KEY:
                            plan.tag_keys.add(entry[1])
                            local.tag_keys.add(entry[1])
            for entry in journal:
                if entry[0] == _TAGSET:
                    self._cache_head(local, *entry[1:])
        finally:
            journal.clear()
        return True

    # ------------------------------------------------------------------------------------
    # Plans and tags
    # ------------------------------------------------------------------------------------

    def _local_plan(self, database: str, measurement: Any) -> _LocalPlan:
        plan = self._serializer._plan(database, measurement)
        db_plans = self._plans.setdefault(database, {})
        local = db_plans.get(plan.measurement)
        if local is None:
            created = _LocalPlan(plan, self._dialect)
            with _SCHEMA_LOCK:  # registered before it caches anything a type change could make stale
                local = db_plans.setdefault(plan.measurement, created)
                if local is created:
                    key = id(local)
                    locals_ = plan.locals
                    locals_[key] = weakref.ref(local, lambda _, key=key: locals_.pop(key, None))  # type: ignore[misc]
        return local

    def _tagset(
        self,
        local: _LocalPlan,
        tags: Mapping[str, Any],
        signature: tuple[Any, ...] | None,
        cache: dict[tuple[Any, ...], str] | None,
        static_cache: bool,
        journal: list[tuple[Any, ...]],
    ) -> str:
        """Serialize and validate a complete tag set, to be cached under ``signature``."""
        fragments = self._fragments
        parts = [local.prefix]
        present: list[str] = []
        cacheable = signature is not None and cache is not None
        try:
            ordered = sorted(tags) if len(tags) > 1 else list(tags)
        except TypeError:
            raise ValidationError(
                "tag keys must be strings", code="invalid_name", measurement=local.measurement
            ) from None
        check_overlap = self._check_overlap
        for key in ordered:
            value = tags[key]
            if type(value) is not str:
                if value is None:
                    continue
                cacheable = False  # 1 == 1.0 == True would collide in the cache
                value = _tag_value_to_str(local.measurement, key, value)
            if not value:
                continue
            fragment = fragments.get((key, value))
            if fragment is None:
                fragment = self._new_fragment(local, key, value)
            if check_overlap and key not in local.tag_keys:
                self._serializer.check_tag_key(local.plan, key)
                journal.append((_TAG_KEY, key))
            parts.append(fragment)
            present.append(key)
        if local.validates_tags:
            _check_tag_schema(local.plan, present)
        head = "".join(parts)
        if cacheable:
            journal.append((_TAGSET, cache, signature, head, static_cache))
        return head

    def _cache_head(
        self,
        local: _LocalPlan,
        cache: dict[tuple[Any, ...], str],
        signature: tuple[Any, ...],
        head: str,
        static_cache: bool,
    ) -> None:
        if cache is not (local.static_tagsets if static_cache else local.tagsets):
            return  # disabled meanwhile
        if len(cache) >= _TAGSET_CACHE_LIMIT:
            # A full cache means many distinct tag sets. Allow a few refills (the working set
            # may have moved on); after _TAGSET_CACHE_FILLS fills the cardinality is too high
            # to pay off.
            local.tagset_fills += 1
            if local.tagset_fills >= _TAGSET_CACHE_FILLS:
                if static_cache:
                    local.static_tagsets = None
                else:
                    local.tagsets = None
                self._serializer._warnings.log(
                    f"cardinality:{local.measurement}",
                    logging.WARNING,
                    "measurement %r has had over %d distinct tag sets in this process: every tag "
                    "set is a series, and high series cardinality slows InfluxDB down (InfluxDB 2 "
                    "keeps an index of all series in memory). Store unbounded values such as "
                    "request ids, user ids or timestamps as fields, not tags",
                    local.measurement,
                    _TAGSET_CACHE_LIMIT * _TAGSET_CACHE_FILLS,
                    extra={"influx": {"measurement": local.measurement}},
                )
                return
            cache.clear()
        cache[signature] = head

    def _new_fragment(self, local: _LocalPlan, key: Any, value: str) -> str:
        """Validate a tag key and value and cache their escaped ``,key=value``."""
        dialect = self._dialect
        if type(key) is not str:
            raise ValidationError(
                f"tag keys must be strings, got {type(key).__name__}",
                code="invalid_name",
                measurement=local.measurement,
            )
        problem = dialect.identifier_problem(key)
        if problem:
            raise ValidationError(
                f"tag key {problem}", code="invalid_name", measurement=local.measurement, key=key
            )
        if key in dialect.reserved_tag_keys:
            raise ValidationError(
                f"tag key {key!r} is reserved by InfluxDB {dialect.version}",
                code="reserved_name",
                measurement=local.measurement,
                key=key,
            )
        problem = dialect.identifier_problem(value)
        if problem:
            raise ValidationError(
                f"tag value {problem}", code="invalid_tag_value", measurement=local.measurement, key=key
            )
        fragment = f",{dialect.escape_key(key)}={dialect.escape_key(value)}"
        fragments = self._fragments
        if len(fragments) >= _FRAGMENT_CACHE_LIMIT:
            fragments.clear()
        fragments[(key, value)] = fragment
        return fragment

    # ------------------------------------------------------------------------------------
    # Fields
    # ------------------------------------------------------------------------------------

    def _field(
        self,
        local: _LocalPlan,
        key: Any,
        value: Any,
        tags: Mapping[str, Any] | None,
        journal: list[tuple[Any, ...]],
    ) -> str | None:
        """General path for one field: first sightings, coercion, unusual types, errors."""
        info = local.fields.get(key)
        if info is not None:
            prefix, name = info
            text = self._field_value(local, key, value, None if name is None else FieldType(name), None)
            return None if text is None else prefix + text
        plan = local.plan
        prefix = self._serializer.field_prefix(plan, key, tags)
        expected = plan.types.get(key)  # as decided so far: _commit checks it again
        lock: list[FieldType] = []  # the type a new field gets locked to
        text = self._field_value(local, key, value, expected, lock)
        if text is None:
            return None
        # Record the key with its expected type so later values take the fast path.
        journal.append((_FIELD, key, prefix, lock[0] if lock else expected, bool(lock)))
        return prefix + text

    def _field_value(
        self,
        local: _LocalPlan,
        key: str,
        value: Any,
        expected: FieldType | None,
        lock: list[FieldType] | None,
    ) -> str | None:
        """Format any supported value as ``expected`` (None: untyped, or a field new to this
        view if ``lock`` is a list); None means 'skip this field'."""
        kind = type(value)
        if kind is float:
            if not _isfinite(value):
                self._non_finite(local, key, value)
                return None
            return self._typed(local, key, value, FLOAT, expected, lock)
        if kind is int:
            return self._typed(local, key, value, self._int_type(expected), expected, lock)
        if kind is str:
            return self._typed(local, key, value, STRING, expected, lock)
        if kind is bool:
            return self._typed(local, key, value, BOOLEAN, expected, lock)
        if value is None:
            return None
        return self._other_field(local, key, value, expected, lock)

    def _int_type(self, expected: FieldType | None) -> FieldType:
        """The type an int is written as: a float with ``int_as_float``, unless the field is an
        integer field (declared, or locked from earlier values or the server's schema)."""
        if self._int_as_float and expected not in _INTEGER_TYPES:
            return FLOAT
        return INTEGER

    def _non_finite(self, local: _LocalPlan, key: str, value: float) -> None:
        """Skip a NaN/inf field (returns) or reject the record (raises), per validation.non_finite."""
        if self._skip_non_finite:
            return
        raise ValidationError(
            f"field value {value!r} is not finite", code="non_finite", measurement=local.measurement, key=key
        )

    def _typed(
        self,
        local: _LocalPlan,
        key: str,
        value: Any,
        natural: FieldType,
        expected: FieldType | None,
        lock: list[FieldType] | None,
    ) -> str:
        """Format ``value`` (whose natural type is ``natural``) as the field's expected type."""
        if expected is None:
            if lock is not None and self._lock_types:
                lock.append(natural)  # a new field: locked to this type once the record is written
            expected = natural
        if expected is natural:
            return self._format(local, key, value, natural)
        coerced = _coerce_value(value, expected) if self._coerce else _NO_COERCION
        if coerced is _NO_COERCION:
            origin = "declared" if key in local.plan.declared else "locked"
            hint = ""
            if natural is FLOAT and expected is INTEGER:
                hint = "; declare the field as float or set validation.int_as_float = true"
            elif natural is INTEGER and expected is FLOAT and not self._coerce:
                hint = "; set validation.coerce = true to convert ints to floats"
            raise ValidationError(
                f"field type conflict: {value!r} is {natural.value} but the field is {origin} "
                f"as {expected.value}{hint}",
                code="type_conflict",
                measurement=local.measurement,
                key=key,
            )
        return self._format(local, key, coerced, expected)

    def _format(self, local: _LocalPlan, key: str, value: Any, kind: FieldType) -> str:
        if kind is FLOAT:
            if type(value) is int and not -_FLOAT_EXACT_INT <= value <= _FLOAT_EXACT_INT:
                raise ValidationError(
                    f"integer {value} cannot be written exactly as a float (validation.int_as_float); "
                    "declare the field as integer",
                    code="out_of_range",
                    measurement=local.measurement,
                    key=key,
                )
            return repr(float(value))
        if kind is INTEGER:
            if not _INT64_MIN <= value <= _INT64_MAX:
                raise ValidationError(
                    f"integer {value} does not fit in int64",
                    code="out_of_range",
                    measurement=local.measurement,
                    key=key,
                )
            return f"{int(value)}i"
        if kind is UINTEGER:
            if not 0 <= value <= _UINT64_MAX:
                raise ValidationError(
                    f"unsigned integer {value} does not fit in uint64",
                    code="out_of_range",
                    measurement=local.measurement,
                    key=key,
                )
            return f"{int(value)}u"
        if kind is STRING:
            if len(value) > self._max_chars and len(value.encode("utf-8")) > self._max_string:
                raise ValidationError(
                    f"string field is longer than {self._max_string} bytes",
                    code="string_too_long",
                    measurement=local.measurement,
                    key=key,
                )
            return '"' + str(value).translate(self._escapes) + '"'
        return "true" if value else "false"

    def _other_field(
        self,
        local: _LocalPlan,
        key: str,
        value: Any,
        expected: FieldType | None,
        lock: list[FieldType] | None,
    ) -> str | None:
        """Unusual value types: UInt, str/int/float subclasses, enums, Decimal, numpy scalars."""
        if isinstance(value, UInt):
            return self._typed(local, key, int(value), UINTEGER, expected, lock)
        if isinstance(value, Enum):
            value = value.value
        elif isinstance(value, Decimal):
            if not self._coerce:
                raise ValidationError(
                    "Decimal field values need validation.coerce = true (they are written as floats)",
                    code="unsupported_type",
                    measurement=local.measurement,
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
            return self._typed(local, key, bool(value), BOOLEAN, expected, lock)
        if isinstance(value, float):
            value = float(value)
            if not _isfinite(value):
                self._non_finite(local, key, value)
                return None
            return self._typed(local, key, value, FLOAT, expected, lock)
        if isinstance(value, int):
            return self._typed(local, key, int(value), self._int_type(expected), expected, lock)
        if isinstance(value, str):
            return self._typed(local, key, str(value), STRING, expected, lock)
        raise ValidationError(
            f"unsupported field value type {type(value).__name__}; use float, int, str or bool",
            code="unsupported_type",
            measurement=local.measurement,
            key=key,
        )

    # ------------------------------------------------------------------------------------
    # Timestamps
    # ------------------------------------------------------------------------------------

    def _timestamp(self, value: Any, divisor: int, measurement: str) -> int:
        dialect = self._dialect
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
            _check_nanos(dialect, nanos, value, measurement)
            self._last_datetime = (value, nanos)  # one atomic store: safe across threads
            return nanos // divisor
        if isinstance(value, str):
            nanos = self._parse_iso(value, measurement)
            _check_nanos(dialect, nanos, value, measurement)
            return nanos // divisor
        if isinstance(value, float):
            if not _isfinite(value):
                raise ValidationError(
                    f"timestamp {value!r} is not finite", code="invalid_time", measurement=measurement
                )
            nanos = round(value * 1e9)
            if not dialect.min_time_ns <= nanos <= dialect.max_time_ns:
                raise ValidationError(
                    f"float timestamp {value!r} is out of range: float timestamps are epoch seconds "
                    "(use an int in the write precision for other units)",
                    code="invalid_time",
                    measurement=measurement,
                )
            return nanos // divisor
        if isinstance(value, int) and not isinstance(value, bool):
            return self._serializer._int_timestamp(int(value), divisor, measurement)
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
            if nanos is None or nanos == _NAT or not dialect.min_time_ns <= nanos <= dialect.max_time_ns:
                raise ValidationError(
                    f"timestamp {value} is outside the range InfluxDB can store (years 1677 to 2262)",
                    code="invalid_time",
                    measurement=measurement,
                )
            return nanos // divisor
        if kind in ("i", "u"):  # numpy integer scalar
            return self._serializer._int_timestamp(operator.index(value), divisor, measurement)
        raise ValidationError(
            f"unsupported timestamp type {type(value).__name__}", code="invalid_time", measurement=measurement
        )

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

    def _unpack_other(self, record: Any, database: str) -> tuple[Any, Any, Any, Any]:
        spec = getattr(type(record), "__sluicebox_model__", None)
        if spec is not None:
            if (database, type(record)) not in self._seeded_models:
                # A model's annotations declare its field types: lock them before the first write.
                self._serializer.seed_types(database, spec.measurement, spec.field_types)
                self._seeded_models.add((database, type(record)))
            result: tuple[Any, Any, Any, Any] = spec.extract(record)
            return result
        if isinstance(record, Point):
            return record.measurement, record.tags, record.fields, record.timestamp
        if isinstance(record, Mapping):
            if not _DICT_KEYS.issuperset(record):
                _bad_dict_keys(record)
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
        candidates = self._dialect.split_lines(text)
        if not candidates:
            return SerializedChunk([], 0, 0)
        if not self._validate_raw:
            if '"' in text or "\\" in text:  # only these can carry a line into the next
                _check_line_ends(self._dialect, candidates, "\n" in text)
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
                try:
                    nbytes = sum(len(line.encode("utf-8")) for line in candidates)
                except UnicodeEncodeError:
                    raise ValidationError(
                        "line protocol must be valid Unicode, but contains an unpaired surrogate",
                        code="invalid_encoding",
                    ) from None
            return SerializedChunk(candidates, nbytes + len(candidates), 0)
        records = []
        for line in candidates:
            if not line.strip():
                continue
            try:
                parsed = parse_line(self._dialect, line)
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


# ------------------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------------------


def _bad_dict_keys(record: Mapping[Any, Any]) -> None:
    unknown = sorted(str(key) for key in record if key not in _DICT_KEYS)
    measurement = record.get("measurement")
    raise ValidationError(
        f"unexpected keys {unknown} in record; expected measurement, tags, fields and time",
        code="malformed_record",
        measurement=measurement if isinstance(measurement, str) else None,
    )


def _as_mapping(value: Any, what: str, measurement: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    raise ValidationError(
        f"{what} must be a mapping, got {type(value).__name__}",
        code="malformed_record",
        measurement=measurement if isinstance(measurement, str) else None,
    )


def _tag_value_to_str(measurement: str, key: str, value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Enum):
        return _tag_value_to_str(measurement, key, value.value)
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
                measurement=measurement,
                key=key,
            )
        return repr(float(value))
    item = getattr(value, "item", None)
    if callable(item) and not isinstance(value, Mapping | list | tuple | bytes):
        return _tag_value_to_str(measurement, key, item())  # numpy / pandas scalar
    raise ValidationError(
        f"tag values must be strings (or numbers/booleans/enums), got {type(value).__name__}",
        code="invalid_tag_value",
        measurement=measurement,
        key=key,
    )


def _check_tag_schema(plan: _Plan, written: Iterable[str]) -> None:
    """Check the tag keys actually written (None, "" and NaN values are omitted)."""
    present = set(written)
    if plan.allowed_tags is not None:
        extra = present - plan.allowed_tags
        if extra:
            raise ValidationError(
                f"tags {sorted(extra)} are not allowed by the schema (allowed: {sorted(plan.allowed_tags)})",
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


def _check_required_fields(local: _LocalPlan, written: list[str]) -> None:
    """Check the formatted fields (skipped None/NaN values do not count as present)."""
    missing = [key for key, prefix in local.required if not any(item.startswith(prefix) for item in written)]
    if missing:
        raise ValidationError(
            f"required fields {sorted(missing)} are missing",
            code="missing_field",
            measurement=local.measurement,
            key=min(missing),
        )


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


def _check_nanos(dialect: Dialect, nanos: int, value: Any, measurement: str) -> None:
    if not dialect.min_time_ns <= nanos <= dialect.max_time_ns:
        raise ValidationError(
            f"timestamp {value} is outside the range InfluxDB can store (years 1677 to 2262)",
            code="invalid_time",
            measurement=measurement,
        )


def _check_line_ends(dialect: Dialect, lines: list[str], multiline: bool) -> None:
    """Reject raw lines that the server would not end where the client does.

    Lines of many ``write()`` calls are joined into one request, so a line that runs on into
    the next (:func:`line_end_problem`) takes another call's line in: the server stores both as
    one point or rejects them as one line, and the other call's point is lost without an error.
    A line holding a newline (``multiline``: the text has one) is one line only because a
    string field value spans the newline. Unless it parses, the value is a string left open
    that a later line closed, swallowing the lines between.
    """
    for line in lines:
        problem = line_end_problem(line)
        if problem is None and multiline and "\n" in line:
            try:
                parse_line(dialect, line)
            except LineSyntaxError as exc:
                problem = f"a string value spans a line break, but the line it makes is invalid ({exc})"
        if problem is not None:
            raise ValidationError(f"invalid line protocol: {problem}: {line[:200]!r}", code="invalid_line")


def _check_v2_line_end(line: str, measurement: str) -> None:
    """Reject a line InfluxDB 2 would read differently, for the backslashes in its names.

    InfluxDB 2 keeps a backslash before another character, so a measurement, tag or field key
    ``a\\ b`` is written ``a\\\\ b``. Its line splitting, though, takes every backslash to
    escape the next character: it reads an escaped backslash, then a space that starts the
    fields. Quotes after it can then leave a string value open (or close one early), and the
    line runs on into the next, or ends at a newline in a string value.
    """
    problem = line_end_problem(line)
    if problem is not None:
        raise ValidationError(
            f"InfluxDB 2 would not read the line as written ({problem}): a backslash before a space, "
            f"',' or '=' in the measurement, a tag or a field key is read as escaping something else; "
            f"line {line[:200]!r}",
            code="invalid_name",
            measurement=measurement,
        )


def _with_timestamp(line: str, stamp: str) -> str:
    """Raw ``line`` with ``stamp`` (a space and the time) appended unless it has a timestamp.

    The timestamp is the last space-separated token: a field set always contains ``=``, and a
    space inside a string field value is followed by its closing quote. A trailing space that a
    backslash escapes is kept: the stamp must not become what the backslash escapes.
    """
    body = line.rstrip(" \t")
    if body[-1:] == "\\" and (len(body) - len(body.rstrip("\\"))) % 2:
        body = line[: len(body) + 1]
    tail = body[body.rfind(" ") + 1 :]
    if tail.isdigit() or (tail[:1] == "-" and tail[1:].isdigit()):
        return line
    return body + stamp


def _utf8_size(line: str, fields: Mapping[str, Any], measurement: str) -> int:
    """UTF-8 size of a non-ASCII ``line``; rejects string field values UTF-8 cannot encode.

    A Python str may hold unpaired surrogates (from ``surrogateescape``, JSON or Windows
    APIs). Names and tag values were checked when first seen, so a string field is the cause.
    """
    try:
        return len(line.encode("utf-8"))
    except UnicodeEncodeError:
        key = next((k for k, v in fields.items() if isinstance(v, str) and not _encodable(v)), None)
        raise ValidationError(
            "string field value must be valid Unicode, but contains an unpaired surrogate",
            code="invalid_encoding",
            measurement=measurement,
            key=key,
        ) from None


def _encodable(text: str) -> bool:
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _written_keys(fields: Mapping[str, Any]) -> set[Any]:
    """Field keys that produce output (None and non-finite floats are skipped)."""
    return {
        key
        for key, value in fields.items()
        if value is not None and not (type(value) is float and value - value != 0.0)
    }
