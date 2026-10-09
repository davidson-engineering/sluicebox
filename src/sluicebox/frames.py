"""DataFrame writes (polars and pandas).

polars frames are serialized with vectorized expressions: escaping, field formatting,
type checks, static/context tag injection and row validation all run inside polars, so
millions of rows per second can be converted without a Python loop. pandas frames are
converted to polars when it is installed.

The row-by-row serializer is used instead (identical results, lower throughput) when
content rules or enrichers apply to the measurement, or for pandas without polars.
"""

from __future__ import annotations

import importlib.util
import logging
import math
import time
from typing import TYPE_CHECKING, Any, cast

from ._serializer import SerializedChunk, _undo
from .exceptions import ConfigurationError, ValidationError
from .point import Point
from .tags import _CONTEXT_TAGS
from .types import PRECISION_DIVISORS, FieldType

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    import polars as pl

    from ._serializer import Serializer, _Plan

__all__ = ["is_dataframe", "to_line_chunks"]

log = logging.getLogger("sluicebox.validation")

_INT64_MAX = 2**63 - 1
_FLOAT_EXACT_INT = 2**53
_CONTROL_PATTERN = r"[\x00-\x1f\x7f]"
#: Per-row errors reported for one chunk of dropped rows (the count is always exact).
_MAX_ROW_ERRORS = 1000


def is_dataframe(obj: Any) -> bool:
    kind = type(obj)
    module = kind.__module__
    return kind.__name__ in ("DataFrame", "LazyFrame") and module.startswith(("polars", "pandas"))


def to_line_chunks(
    frame: Any,
    *,
    serializer: Serializer,
    database: str,
    precision: str,
    measurement: str | None,
    tag_columns: Sequence[str] | None,
    field_columns: Sequence[str] | None,
    time_column: str | None,
    chunk_size: int,
) -> Iterator[SerializedChunk]:
    """Yield one :class:`SerializedChunk` per chunk of ``chunk_size`` rows."""
    if not measurement:
        raise ConfigurationError("writing a DataFrame needs measurement=...")
    module = type(frame).__module__
    if module.startswith("pandas"):
        if importlib.util.find_spec("polars") is None:
            yield from _pandas_rows(
                frame,
                serializer,
                database,
                precision,
                measurement,
                tag_columns,
                field_columns,
                time_column,
                chunk_size,
            )
            return
        converted = _pandas_to_polars(frame, time_column, serializer)
        if converted is None:
            # Columns polars cannot type (e.g. objects mixing numbers and strings): validate the
            # values one by one, so each bad row is reported with its index.
            yield from _pandas_rows(
                frame,
                serializer,
                database,
                precision,
                measurement,
                tag_columns,
                field_columns,
                time_column,
                chunk_size,
            )
            return
        frame, time_column = converted
    elif type(frame).__name__ == "LazyFrame":
        frame = frame.collect()
    yield from _polars_chunks(
        frame,
        serializer,
        database,
        precision,
        measurement,
        tag_columns,
        field_columns,
        time_column,
        chunk_size,
    )


# ---------------------------------------------------------------------------------------------
# Column resolution
# ---------------------------------------------------------------------------------------------


def _resolve_columns(
    columns: Sequence[str],
    tag_columns: Sequence[str] | None,
    field_columns: Sequence[str] | None,
    time_column: str | None,
    timestamp_is_time: bool = False,
) -> tuple[list[str], list[str], str | None]:
    """Tag, field and time columns. Without ``time_column``, a column named "time" is the time
    (and one named "timestamp", if ``timestamp_is_time``: it holds datetimes)."""
    if isinstance(tag_columns, str):
        tag_columns = [tag_columns]
    if isinstance(field_columns, str):
        field_columns = [field_columns]
    tags = list(tag_columns or [])
    if time_column is None:
        if "time" in columns:
            time_column = "time"
        elif "timestamp" in columns and timestamp_is_time:
            time_column = "timestamp"
    if field_columns is None:
        used = set(tags) | ({time_column} if time_column else set())
        fields = [name for name in columns if name not in used]
    else:
        fields = list(field_columns)
    missing = [
        name for name in [*tags, *fields, *([time_column] if time_column else [])] if name not in columns
    ]
    if missing:
        raise ConfigurationError(f"DataFrame has no columns {missing}")
    overlap = set(tags) & set(fields)
    if overlap:
        raise ConfigurationError(f"columns {sorted(overlap)} are listed as both tags and fields")
    if time_column in tags or time_column in fields:
        raise ConfigurationError(f"time column {time_column!r} is also listed as a tag or field")
    if not fields:
        raise ConfigurationError("DataFrame has no field columns")
    return tags, fields, time_column


# ---------------------------------------------------------------------------------------------
# Vectorized polars path
# ---------------------------------------------------------------------------------------------


class _Invalid:
    """Accumulates per-row validation failures as (mask expression, code, message, key)."""

    def __init__(self) -> None:
        self.checks: list[tuple[pl.Expr, str, str, str | None]] = []

    def add(self, mask: pl.Expr, code: str, message: str, key: str | None = None) -> None:
        self.checks.append((mask, code, message, key))


def _polars_chunks(
    frame: pl.DataFrame,
    serializer: Serializer,
    database: str,
    precision: str,
    measurement: str,
    tag_columns: Sequence[str] | None,
    field_columns: Sequence[str] | None,
    time_column: str | None,
    chunk_size: int,
) -> Iterator[SerializedChunk]:
    import polars as pl

    stamp_dtype = frame.schema.get("timestamp")
    tags, fields, time_col = _resolve_columns(
        frame.columns, tag_columns, field_columns, time_column, isinstance(stamp_dtype, pl.Datetime)
    )
    plan = serializer._plan(database, measurement)
    injector = serializer.injector
    # Content rules and enrichers see each point; text timestamps are parsed (and checked
    # for a UTC offset) one by one: both take the row path.
    if plan.rules or injector.enrichers or (time_col is not None and frame.schema[time_col] == pl.String):
        yield from _polars_rows(
            frame, serializer, database, precision, measurement, tags, fields, time_col, chunk_size
        )
        return
    if frame.height == 0:
        return

    # Field type locks and tag/field key registrations made while building the expressions:
    # undone unless a row gets written (a rejected record locks nothing).
    journal: list[tuple[Any, Any]] = []
    try:
        invalid = _Invalid()
        tag_expr = _tag_expression(frame, serializer, plan, tags, fields, invalid, journal)
        field_expr = _field_expression(frame, serializer, plan, fields, invalid, journal)
        stamp = _time_expression(frame, serializer, precision, time_col)
        invalid.add(field_expr == "", "no_fields", "record has no fields (all were null, NaN or missing)")
        for key in plan.required_fields:
            if key in fields:
                invalid.add(pl.col(key).is_null(), "missing_field", f"required field {key!r} is missing", key)
            else:
                invalid.add(pl.lit(True), "missing_field", f"required field {key!r} is missing", key)

        parts = [pl.lit(plan.prefix), tag_expr, pl.lit(" "), field_expr]
        if stamp is not None:
            # A null time (only possible without auto timestamps) means "server time": no timestamp.
            parts.append(pl.concat_str([pl.lit(" "), stamp]).fill_null(""))
        line = pl.concat_str(parts)
        any_invalid = pl.any_horizontal([mask.fill_null(False) for mask, *_ in invalid.checks])

        for offset in range(0, frame.height, chunk_size):
            chunk = frame.slice(offset, chunk_size)
            result = chunk.select(line.alias("line"), any_invalid.alias("invalid"))
            dropped = 0
            rejected: list[ValidationError] = []
            if result["invalid"].any():
                dropped, rejected = _handle_invalid(chunk, serializer, invalid, plan.measurement, offset)
                result = result.filter(~pl.col("invalid"))
            lines_series = result["line"]
            lines = lines_series.to_list()
            nbytes = int(lines_series.str.len_bytes().sum() or 0) + len(lines)
            if lines:
                journal.clear()  # rows are written with these locks: keep them
            yield SerializedChunk(lines, nbytes, dropped, rejected)
    finally:
        if journal:
            _undo(journal)


def _handle_invalid(
    chunk: pl.DataFrame, serializer: Serializer, invalid: _Invalid, measurement: str, offset: int
) -> tuple[int, list[ValidationError]]:
    """Raise for the first invalid row, or drop the invalid rows.

    Returns the number of dropped rows and one error per dropped row (``index`` is its row
    number in the frame), capped at ``_MAX_ROW_ERRORS`` per chunk.
    """
    import polars as pl

    masks = chunk.select(
        [mask.fill_null(False).alias(f"m{i}") for i, (mask, *_) in enumerate(invalid.checks)]
    )
    if not serializer._raise:
        count = 0
        rejected: list[tuple[int, ValidationError]] = []
        claimed = pl.Series([False] * chunk.height)
        for i, (_, code, message, key) in enumerate(invalid.checks):
            first_failure = masks[f"m{i}"] & ~claimed  # count each row once, under its first problem
            hits = int(first_failure.sum())
            if hits:
                claimed = claimed | first_failure
                count += hits
                error = ValidationError(message, code=code, measurement=measurement, key=key)
                if serializer.on_drop is not None:
                    serializer.on_drop(error, hits)
                failing = first_failure.arg_true()[: _MAX_ROW_ERRORS - len(rejected)].to_list()
                rejected.extend(
                    (
                        row,
                        ValidationError(
                            message, code=code, measurement=measurement, key=key, index=offset + row
                        ),
                    )
                    for row in failing
                )
        rejected.sort(key=lambda item: item[0])
        return count, [error for _, error in rejected]
    first_row = None
    first_check = 0
    for i in range(len(invalid.checks)):
        rows = masks[f"m{i}"].arg_true()
        if len(rows) and (first_row is None or rows[0] < first_row):
            first_row = int(rows[0])
            first_check = i
    _, code, message, key = invalid.checks[first_check]
    raise ValidationError(
        message, code=code, measurement=measurement, key=key, index=offset + (first_row or 0)
    )


def _escape_identifier(expr: pl.Expr, version: int) -> pl.Expr:
    if version == 3:
        expr = expr.str.replace_all("\\", "\\\\", literal=True)
    return (
        expr.str.replace_all(",", "\\,", literal=True)
        .str.replace_all("=", "\\=", literal=True)
        .str.replace_all(" ", "\\ ", literal=True)
    )


def _as_tag_strings(frame: pl.DataFrame, name: str, plan: _Plan, invalid: _Invalid) -> pl.Expr:
    """Tag values as strings, null where the tag is absent (as the row serializer does)."""
    import polars as pl

    dtype = frame.schema[name]
    col = pl.col(name)
    if dtype == pl.String:
        return col
    if dtype == pl.Null:
        return pl.lit(None, dtype=pl.String)
    if dtype == pl.Boolean or dtype in (pl.Categorical, pl.Enum) or dtype.is_integer():
        return col.cast(pl.String)  # booleans become "true" / "false"; nulls stay null
    if dtype.is_float():
        invalid.add(col.is_infinite(), "invalid_tag_value", f"tag {name!r} has an infinite value", name)
        # NaN means "missing"; other values match the row serializer's Python repr.
        finite = pl.when(col.is_finite()).then(col.cast(pl.Float64))
        return finite.map_elements(lambda v: repr(float(v)), return_dtype=pl.String, skip_nulls=True)
    raise ValidationError(
        f"tag column dtype {dtype} is not supported; use strings, integers, booleans or categoricals",
        code="invalid_tag_value",
        measurement=plan.measurement,
        key=name,
    )


def _tag_expression(
    frame: pl.DataFrame,
    serializer: Serializer,
    plan: _Plan,
    tags: list[str],
    fields: list[str],
    invalid: _Invalid,
    journal: list[tuple[Any, Any]],
) -> pl.Expr:
    import polars as pl

    dialect = serializer.dialect
    injector = serializer.injector
    context = _CONTEXT_TAGS.get()
    injected: dict[str, str] = {**injector.static, **context} if context else dict(injector.static)
    columns: dict[str, pl.Expr] = {}
    for name in tags:
        _check_tag_key(serializer, plan, name, fields, journal)
        values = _as_tag_strings(frame, name, plan, invalid)
        present = values.is_not_null() & (values != "")
        invalid.add(
            present & (values.str.contains(_CONTROL_PATTERN) | values.str.ends_with("\\")),
            "invalid_tag_value",
            f"tag {name!r} has a value with a control character or a trailing backslash",
            name,
        )
        columns[name] = pl.when(present).then(values)
    for key, value in injected.items():
        problem = dialect.identifier_problem(value) if isinstance(value, str) else "must be a string"
        if problem:
            raise ValidationError(f"injected tag value {problem}", code="invalid_tag_value", key=key)
        literal = pl.lit(value, dtype=pl.String)
        if key in columns:
            existing = columns[key]
            policy = injector.policy
            if policy == "overwrite":
                columns[key] = literal
            elif policy == "keep":
                columns[key] = pl.coalesce(existing, literal)
            else:
                invalid.add(
                    existing.is_not_null() & (existing != value),
                    "tag_conflict",
                    f"injected tag {key!r}={value!r} conflicts with the point's value",
                    key,
                )
                columns[key] = pl.coalesce(existing, literal)
        else:
            _check_tag_key(serializer, plan, key, fields, journal)
            columns[key] = literal
    if plan.allowed_tags is not None:
        for key, expr in columns.items():
            if key not in plan.allowed_tags:
                invalid.add(
                    expr.is_not_null(),
                    "unexpected_tag",
                    f"tag {key!r} is not allowed by the schema (allowed: {sorted(plan.allowed_tags)})",
                    key,
                )
    for key in plan.required_tags:
        if key in columns:
            invalid.add(columns[key].is_null(), "missing_tag", f"required tag {key!r} is missing", key)
        else:
            invalid.add(pl.lit(True), "missing_tag", f"required tag {key!r} is missing", key)
    parts = [
        pl.lit(f",{dialect.escape_key(key)}=") + _escape_identifier(columns[key], dialect.version)
        for key in sorted(columns)
    ]
    if not parts:
        return pl.lit("")
    return pl.concat_str(parts, separator="", ignore_nulls=True)


def _check_tag_key(
    serializer: Serializer, plan: _Plan, key: str, fields: list[str], journal: list[tuple[Any, Any]]
) -> None:
    dialect = serializer.dialect
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
    if dialect.forbid_tag_field_overlap:
        if key in fields:
            raise ValidationError(
                f"{key!r} is both a tag and a field; InfluxDB 3 does not allow that",
                code="tag_field_conflict",
                measurement=plan.measurement,
                key=key,
            )
        if key not in plan.tag_keys:
            serializer._register_tag_key(plan, key, journal)


def _natural_type(dtype: Any, key: str, measurement: str) -> FieldType | None:
    import polars as pl

    if dtype == pl.Null:
        return None
    if dtype.is_float() or isinstance(dtype, pl.Decimal):
        return FieldType.FLOAT
    if dtype == pl.UInt64:
        return FieldType.UINTEGER
    if dtype.is_integer():
        return FieldType.INTEGER
    if dtype == pl.Boolean:
        return FieldType.BOOLEAN
    if dtype == pl.String or dtype in (pl.Categorical, pl.Enum):
        return FieldType.STRING
    raise ValidationError(
        f"field column dtype {dtype} is not supported; use float, integer, boolean or string columns",
        code="unsupported_type",
        measurement=measurement,
        key=key,
    )


def _field_expression(
    frame: pl.DataFrame,
    serializer: Serializer,
    plan: _Plan,
    fields: list[str],
    invalid: _Invalid,
    journal: list[tuple[Any, Any]],
) -> pl.Expr:
    import polars as pl

    fragments = []
    for key in fields:
        dtype = frame.schema[key]
        natural = _natural_type(dtype, key, plan.measurement)
        if natural is None:
            continue  # an all-null column carries no fields
        info = plan.fields.get(key)
        prefix = info[0] if info is not None else serializer._new_field_key(plan, key, None)
        if natural is FieldType.INTEGER and serializer._int_as_float:
            natural = FieldType.FLOAT
        expected = plan.types.get(key)
        if expected is None:
            if serializer._lock_types:
                expected = plan.types.setdefault(key, natural)
                if expected is natural:
                    journal.append((plan.types, key))
            else:
                expected = natural
        col = pl.col(key)
        if isinstance(dtype, pl.Decimal):
            col = col.cast(pl.Float64)
        value, formatted = _format_field(col, natural, expected, key, plan, serializer, invalid)
        if info is None:
            entry = (prefix, plan.types.get(key))
            if plan.fields.setdefault(key, entry) is entry:
                journal.append((plan.fields, key))
        fragments.append(pl.when(value.is_not_null()).then(pl.lit(prefix) + formatted))
    if not fragments:
        return pl.lit("")
    return pl.concat_str(fragments, separator=",", ignore_nulls=True)


def _format_field(
    col: pl.Expr,
    natural: FieldType,
    expected: FieldType,
    key: str,
    plan: _Plan,
    serializer: Serializer,
    invalid: _Invalid,
) -> tuple[pl.Expr, pl.Expr]:
    """Return (value expression whose nulls mean 'skip', formatted text expression)."""
    import polars as pl

    origin = "declared" if key in plan.declared else "locked"

    def conflict(mask: pl.Expr) -> None:
        invalid.add(
            mask,
            "type_conflict",
            f"field type conflict: column {key!r} is {natural.value} "
            f"but the field is {origin} as {expected.value}",
            key,
        )

    coerce = serializer._coerce
    value = col
    if natural is FieldType.FLOAT:
        value = col.cast(pl.Float64)
        finite = value.is_finite()
        if serializer._skip_non_finite:
            value = pl.when(finite).then(value)
        else:
            invalid.add(
                value.is_not_null() & ~finite, "non_finite", f"field {key!r} has a NaN/inf value", key
            )
    if expected is natural:
        target = natural
    elif not coerce:
        conflict(value.is_not_null())
        target = natural
    elif expected is FieldType.FLOAT and natural in (FieldType.INTEGER, FieldType.UINTEGER):
        conflict(value.is_not_null() & (value.cast(pl.Float64).abs() > _FLOAT_EXACT_INT))
        value = value.cast(pl.Float64)
        target = FieldType.FLOAT
    elif expected in (FieldType.INTEGER, FieldType.UINTEGER) and natural is FieldType.FLOAT:
        integral = value == value.round(0)
        upper = _INT64_MAX if expected is FieldType.INTEGER else 2**64 - 1
        lower = -(2**63) if expected is FieldType.INTEGER else 0
        conflict(value.is_not_null() & (~integral | (value < lower) | (value > upper)))
        value = (
            pl.when(integral & (value >= lower) & (value <= upper))
            .then(value)
            .cast(pl.Int64 if expected is FieldType.INTEGER else pl.UInt64)
        )
        target = expected
    elif expected is FieldType.UINTEGER and natural is FieldType.INTEGER:
        conflict(value.is_not_null() & (value < 0))
        value = pl.when(value >= 0).then(value).cast(pl.UInt64)
        target = expected
    elif expected is FieldType.INTEGER and natural is FieldType.UINTEGER:
        conflict(value.is_not_null() & (value > _INT64_MAX))
        value = pl.when(value <= _INT64_MAX).then(value).cast(pl.Int64)
        target = expected
    else:
        conflict(value.is_not_null())
        target = natural

    if target is FieldType.FLOAT:
        text = value.cast(pl.String)
        # polars prints integral floats as "1.0" and keeps exponents; both parse as floats.
    elif target is FieldType.INTEGER:
        text = value.cast(pl.Int64).cast(pl.String) + pl.lit("i")
    elif target is FieldType.UINTEGER:
        text = value.cast(pl.UInt64).cast(pl.String) + pl.lit("u")
    elif target is FieldType.BOOLEAN:
        text = pl.when(value).then(pl.lit("true")).otherwise(pl.lit("false"))
    else:
        strings = value.cast(pl.String)
        invalid.add(
            strings.str.len_bytes() > serializer._max_string,
            "string_too_long",
            f"string field {key!r} is longer than {serializer._max_string} bytes",
            key,
        )
        escaped = strings.str.replace_all("\\", "\\\\", literal=True).str.replace_all(
            '"', '\\"', literal=True
        )
        text = pl.lit('"') + escaped + pl.lit('"')
    return value, text


def _check_integer_times(
    times: pl.Series, divisor: int, precision: str, serializer: Serializer, name: str
) -> None:
    """Reject integer times InfluxDB cannot store; warn about ones in the wrong unit."""
    low, high = cast("int | None", times.min()), cast("int | None", times.max())
    if low is None or high is None:
        return
    if high > _INT64_MAX // divisor or low < -_INT64_MAX // divisor:
        raise ValidationError(
            f"time column {name!r} has values outside the range InfluxDB can store with "
            f"precision {precision!r}; is the precision right?",
            code="invalid_time",
            key=name,
        )
    serializer._int_timestamp(low, divisor, name)  # warns if it lands before 1973


def _time_expression(
    frame: pl.DataFrame, serializer: Serializer, precision: str, time_col: str | None
) -> pl.Expr | None:
    import polars as pl

    divisor = PRECISION_DIVISORS[precision]
    now = time.time_ns() // divisor if serializer.auto_timestamp else None
    if time_col is None:
        return pl.lit(str(now)) if now is not None else None
    dtype = frame.schema[time_col]
    col = pl.col(time_col)
    if isinstance(dtype, pl.Datetime):
        if dtype.time_zone is None and not serializer._naive_utc:
            raise ValidationError(
                f"time column {time_col!r} has no time zone; use a timezone-aware column "
                "(e.g. .dt.replace_time_zone('UTC')) or set validation.naive_datetime = 'utc'",
                code="naive_datetime",
                key=time_col,
            )
        stamp = col.dt.epoch(time_unit=precision)  # type: ignore[arg-type]
    elif dtype.is_integer():
        stamp = col.cast(pl.Int64)
        _check_integer_times(frame[time_col], divisor, precision, serializer, time_col)
    else:
        raise ValidationError(
            f"time column {time_col!r} has dtype {dtype}; "
            "use a Datetime or an integer column in the write precision",
            code="invalid_time",
            key=time_col,
        )
    if now is not None:
        return stamp.fill_null(now).cast(pl.String)
    # Without auto timestamps a null time means "server time": omit it.
    return stamp.cast(pl.String)


# ---------------------------------------------------------------------------------------------
# Row-by-row fallbacks
# ---------------------------------------------------------------------------------------------


def _polars_rows(
    frame: pl.DataFrame,
    serializer: Serializer,
    database: str,
    precision: str,
    measurement: str,
    tags: list[str],
    fields: list[str],
    time_col: str | None,
    chunk_size: int,
) -> Iterator[SerializedChunk]:
    columns = [*tags, *fields, *([time_col] if time_col else [])]
    n_tags = len(tags)
    n_fields = len(fields)
    now_ns = time.time_ns()
    for offset in range(0, frame.height, chunk_size):
        rows = frame.slice(offset, chunk_size).select(columns).iter_rows()
        points = [
            Point(
                measurement,
                dict(zip(tags, row[:n_tags], strict=True)),
                dict(zip(fields, row[n_tags : n_tags + n_fields], strict=True)),
                row[-1] if time_col else None,
            )
            for row in rows
        ]
        chunk = serializer.serialize(
            points, database=database, precision=precision, start_index=offset, now_ns=now_ns
        )
        yield chunk


def _pandas_to_polars(
    frame: Any, time_column: str | None, serializer: Serializer
) -> tuple[pl.DataFrame, str | None] | None:
    """The frame as polars, or None if polars cannot type some column."""
    import pandas as pd
    import polars as pl

    if time_column is None and "time" not in frame.columns and isinstance(frame.index, pd.DatetimeIndex):
        index_name = str(frame.index.name or "time")
        frame = frame.reset_index(names=index_name)
        time_column = index_name
    name = time_column or ("time" if "time" in frame.columns else None)
    if name is not None and name in frame.columns and not serializer._naive_utc:
        series = frame[name]
        if pd.api.types.is_datetime64_dtype(series) and getattr(series.dt, "tz", None) is None:
            raise ValidationError(
                f"time column {name!r} has no time zone; use a timezone-aware column "
                "(e.g. df[col].dt.tz_localize('UTC')) or set validation.naive_datetime = 'utc'",
                code="naive_datetime",
                key=name,
            )
    try:
        return pl.from_pandas(frame), time_column
    except Exception as exc:  # e.g. an object column mixing floats and strings
        log.debug("pandas frame not convertible to polars (%s); validating row by row", exc)
        return None


def _pandas_rows(
    frame: Any,
    serializer: Serializer,
    database: str,
    precision: str,
    measurement: str,
    tag_columns: Sequence[str] | None,
    field_columns: Sequence[str] | None,
    time_column: str | None,
    chunk_size: int,
) -> Iterator[SerializedChunk]:
    import pandas as pd

    use_index = (
        time_column is None and "time" not in frame.columns and isinstance(frame.index, pd.DatetimeIndex)
    )
    tags, fields, time_col = _resolve_columns(
        list(frame.columns),
        tag_columns,
        field_columns,
        time_column,
        "timestamp" in frame.columns and pd.api.types.is_datetime64_any_dtype(frame["timestamp"]),
    )
    now_ns = time.time_ns()
    missing = (pd.NA, pd.NaT)

    def clean(value: Any) -> Any:
        if value is None or any(value is m for m in missing):
            return None
        return None if isinstance(value, float) and math.isnan(value) else value

    for offset in range(0, len(frame), chunk_size):
        part = frame.iloc[offset : offset + chunk_size]
        times = part.index if use_index else (part[time_col] if time_col else None)
        tag_values = [part[c].tolist() for c in tags]
        field_values = [part[c].tolist() for c in fields]
        stamps = list(times) if times is not None else [None] * len(part)
        points = []
        for i in range(len(part)):
            stamp = stamps[i]
            if stamp is pd.NaT:
                stamp = None
            points.append(
                Point(
                    measurement,
                    {name: clean(values[i]) for name, values in zip(tags, tag_values, strict=True)},
                    {name: clean(values[i]) for name, values in zip(fields, field_values, strict=True)},
                    stamp,
                )
            )
        chunk = serializer.serialize(
            points, database=database, precision=precision, start_index=offset, now_ns=now_ns
        )
        yield chunk
