"""The synchronous client facade (whose writes are asynchronous: buffered and batched)."""

from __future__ import annotations

import itertools
import logging
import os
import re
import threading
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self

from . import _fork
from ._engine import EngineStats, WriteEngine, WriteFailure
from ._http import Transport, error_from_response, is_html, unexpected_page_error
from ._lineprotocol import Dialect
from ._serializer import SerializedChunk, Serializer
from .config import InfluxSettings, load_settings
from .exceptions import (
    BufferFullError,
    ClientClosedError,
    ConfigurationError,
    InfluxKitError,
    ValidationError,
)
from .futures import MAX_REJECTED, WriteFuture, warn_if_event_loop
from .log import RateLimitedLog, configure_logging
from .metrics import Metrics, create_metrics
from .point import Point
from .profiling import ProfileReport, StageRecorder, StageStats, profile
from .query import Language, QueryResult, V2QueryBackend, V3QueryBackend, run_query, stream_query
from .tags import Enricher, TagInjector, tag_context
from .types import PRECISION_DIVISORS, FieldType, Precision

if TYPE_CHECKING:
    from contextlib import AbstractContextManager
    from pathlib import Path
    from types import TracebackType

    from prometheus_client import CollectorRegistry

__all__ = ["ClientStats", "InfluxClient", "ServerInfo"]

log = logging.getLogger("influxkit.client")

# Serializes the lazy creation of query backends (rare and quick).
_backend_lock = threading.Lock()


def _new_backend_lock() -> None:
    global _backend_lock
    _backend_lock = threading.Lock()


_fork.on_child(locks=_new_backend_lock)

_SINGLE_RECORD_TYPES = (Mapping, Point, str, bytes)
# Field type names in the servers' type-conflict errors.
_SERVER_TYPES = {
    "float": FieldType.FLOAT,
    "integer": FieldType.INTEGER,
    "uinteger": FieldType.UINTEGER,
    "unsigned": FieldType.UINTEGER,
    "string": FieldType.STRING,
    "boolean": FieldType.BOOLEAN,
}


@dataclass(frozen=True, slots=True)
class ServerInfo:
    """What :meth:`InfluxClient.ping` learned about the server."""

    url: str
    #: Server version without a leading "v", e.g. "2.9.1" or "3.12.0".
    version: str | None
    build: str | None
    latency: float

    @property
    def major(self) -> int | None:
        """Major version (2 or 3), if the server reported one."""
        match = re.match(r"(\d+)", self.version or "")
        return int(match.group(1)) if match else None


@dataclass(frozen=True, slots=True)
class ClientStats:
    """Point-in-time statistics: write engine counters and per-stage timings."""

    write: EngineStats
    stages: dict[str, StageStats]


class InfluxClient:
    """High-throughput client for InfluxDB 2 and InfluxDB 3.

    Writes are **asynchronous**: :meth:`write` validates and serializes on the calling
    thread, buffers the result and returns a :class:`~influxkit.futures.WriteFuture`
    immediately; background threads batch, compress and send. Call ``.result()`` on the
    future (or :meth:`flush`) when you need the server's acknowledgement.

    Create one client per server and share it across threads; it is thread-safe and keeps
    a pool of keep-alive connections. Use it as a context manager (or call :meth:`close`)
    so buffered data is flushed.

    Args:
        settings: Complete settings. If omitted, they are loaded with
            :func:`~influxkit.config.load_settings` and ``overrides``.
        tags: Extra static tags added to every point (on top of ``[tags] static``).
        enrichers: Callables ``(measurement, tags, fields) -> {tag: value} | None`` that add
            tags based on point content.
        on_error: Called (on a sender thread) with a :class:`WriteFailure` for every batch
            that cannot be written - use it for dead-lettering. When set, :meth:`flush` and
            :meth:`close` no longer raise for those failures.
        registry: Prometheus registry for metrics (default: the global registry).
    """

    def __init__(
        self,
        settings: InfluxSettings | None = None,
        *,
        tags: Mapping[str, str] | None = None,
        enrichers: Sequence[Enricher] = (),
        on_error: Callable[[WriteFailure], Any] | None = None,
        registry: CollectorRegistry | None = None,
        **overrides: Any,
    ) -> None:
        _check_static_tags(tags)
        if settings is None:
            settings = load_settings(**overrides)
        elif overrides:
            settings = settings.with_overrides(**overrides)
        self._settings = settings
        if settings.logging.configure:
            configure_logging(settings.logging)
        self._metrics: Metrics = create_metrics(settings.metrics, settings.name, registry)
        self._stages = StageRecorder(settings.profiling.enabled, self._metrics.stage)
        self._dialect = Dialect.for_version(settings.connection.version)
        self._database = settings.connection.database
        self._precision: Precision = settings.write.precision
        self._chunk_size = settings.write.batch_size
        self._drop_log = RateLimitedLog(logging.getLogger("influxkit.validation"))
        self._injector = TagInjector(settings.tags, static=tags, enrichers=enrichers)
        self._serializer = Serializer(
            dialect=self._dialect,
            validation=settings.validation,
            schemas=settings.measurements,
            injector=self._injector,
            auto_timestamp=settings.write.auto_timestamp,
            on_drop=self._on_drop,
        )
        pool_size = settings.connection.pool_size or settings.write.concurrency + 2
        self._transport = Transport(
            settings.connection, settings.token, pool_size=pool_size, token_hint=settings.origin.token_hint()
        )
        self._engine = WriteEngine(
            transport=self._transport,
            settings=settings,
            metrics=self._metrics,
            stages=self._stages,
            on_error=on_error,
            on_type_conflict=self._learn_server_type,
        )
        self._query_backend: V2QueryBackend | V3QueryBackend | None = None
        self._closed = False
        self._metrics.set_info(
            url=settings.connection.url,
            server_version=str(settings.connection.version),
            database=settings.connection.database,
        )
        log.info(
            "influxkit client %r ready: InfluxDB %s at %s, database %r, write endpoint %s",
            settings.name,
            settings.connection.version,
            settings.connection.url,
            settings.connection.database,
            "/api/v3/write_lp" if settings.write_api == "v3" else "/api/v2/write",
        )

    @classmethod
    def from_config(
        cls,
        config_file: str | os.PathLike[str] | None = None,
        *,
        section: str | None = None,
        env_file: str | os.PathLike[str] | None = ".env",
        env_prefix: str = "INFLUXKIT_",
        secrets_dir: str | os.PathLike[str] | None = None,
        tags: Mapping[str, str] | None = None,
        enrichers: Sequence[Enricher] = (),
        on_error: Callable[[WriteFailure], Any] | None = None,
        registry: CollectorRegistry | None = None,
        **overrides: Any,
    ) -> Self:
        """Create a client from a TOML file plus environment/.env secrets (see :func:`load_settings`).

        ``tags`` are static tags for every point; ``**overrides`` are settings, as for
        :func:`load_settings`.
        """
        _check_static_tags(tags)
        settings = load_settings(
            config_file,
            section=section,
            env_file=env_file,
            env_prefix=env_prefix,
            secrets_dir=secrets_dir,
            **overrides,
        )
        return cls(settings, tags=tags, enrichers=enrichers, on_error=on_error, registry=registry)

    @property
    def settings(self) -> InfluxSettings:
        return self._settings

    # ------------------------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------------------------

    def write(
        self,
        data: Any,
        *,
        database: str | None = None,
        precision: Precision | None = None,
        tags: Mapping[str, str] | None = None,
        measurement: str | None = None,
        tag_columns: Sequence[str] | None = None,
        field_columns: Sequence[str] | None = None,
        time_column: str | None = None,
    ) -> WriteFuture:
        """Validate, serialize and buffer ``data``; return without waiting for the server.

        ``data`` may be a :class:`Point`, a record dict (``measurement``, ``tags``, ``fields``,
        ``time``), a ``@measurement`` model instance, line protocol (``str``/``bytes``, one or
        more lines), any iterable of those, or a polars/pandas DataFrame (with
        ``measurement`` and ``tag_columns``/``field_columns``/``time_column``).

        Data is serialized before this returns, so objects may be reused immediately.

        Args:
            database: Target database/bucket (default: ``connection.database``).
            precision: Precision of integer timestamps (default: ``write.precision``).
            tags: Extra tags for every point of this call.

        Returns:
            A :class:`WriteFuture`; call ``.result()`` or ``await`` it to wait for acknowledgement.

        Raises:
            ValidationError: a record is invalid and ``validation.on_invalid = "raise"``.
                ``error.points_enqueued`` tells how many points of this call were already buffered.
            BufferFullError: the buffer is full and ``write.on_full = "raise"`` (or blocking timed out).
            ClientClosedError: the client was closed.
        """
        if self._closed:
            raise ClientClosedError("the client is closed")
        target = database or self._database
        prec = precision or self._precision
        if prec not in PRECISION_DIVISORS:
            raise ValueError(f"precision must be one of 'ns', 'us', 'ms', 's'; got {prec!r}")
        if tags:
            with tag_context(**tags):
                return self._write(data, target, prec, measurement, tag_columns, field_columns, time_column)
        return self._write(data, target, prec, measurement, tag_columns, field_columns, time_column)

    def _write(
        self,
        data: Any,
        database: str,
        precision: Precision,
        measurement: str | None,
        tag_columns: Sequence[str] | None,
        field_columns: Sequence[str] | None,
        time_column: str | None,
    ) -> WriteFuture:
        kind = type(data)
        if (kind is dict or kind is Point) and measurement is None:
            return self._write_one(data, database, precision)
        chunks = self._chunks(data, database, precision, measurement, tag_columns, field_columns, time_column)
        return self._submit(chunks, database, precision)

    def _write_one(self, record: Any, database: str, precision: Precision) -> WriteFuture:
        """Fast path for the common one-record call (no chunking machinery)."""
        started = time.perf_counter()
        try:
            chunk = self._serializer.serialize((record,), database=database, precision=precision)
        except ValidationError as error:
            self._metrics.error("write", error)
            raise
        elapsed = time.perf_counter() - started
        future = WriteFuture(self._engine)
        future.dropped = chunk.dropped
        future.rejected = chunk.rejected
        if chunk.lines and not self._engine.submit(
            database, precision, chunk.lines, chunk.nbytes, future, serialize_time=elapsed
        ):
            future.dropped += len(chunk.lines)
        future._seal()
        return future

    def _chunks(
        self,
        data: Any,
        database: str,
        precision: Precision,
        measurement: str | None,
        tag_columns: Sequence[str] | None,
        field_columns: Sequence[str] | None,
        time_column: str | None,
    ) -> Iterator[SerializedChunk]:
        """Serialized chunks of at most one batch each (lazy)."""
        from . import frames

        if frames.is_dataframe(data):
            return frames.to_line_chunks(
                data,
                serializer=self._serializer,
                database=database,
                precision=precision,
                measurement=measurement,
                tag_columns=tag_columns,
                field_columns=field_columns,
                time_column=time_column,
                chunk_size=self._chunk_size,
            )
        if measurement or tag_columns or field_columns or time_column:
            raise TypeError("measurement/tag_columns/field_columns/time_column only apply to DataFrames")
        return self._record_chunks(data, database, precision)

    def _record_chunks(self, data: Any, database: str, precision: Precision) -> Iterator[SerializedChunk]:
        now_ns = time.time_ns()
        serialize = self._serializer.serialize
        if isinstance(data, _SINGLE_RECORD_TYPES) or hasattr(type(data), "__influxkit_model__"):
            yield serialize((data,), database=database, precision=precision, now_ns=now_ns)
            return
        size = self._chunk_size
        if isinstance(data, list | tuple):
            for start in range(0, len(data), size):
                yield serialize(
                    data[start : start + size],
                    database=database,
                    precision=precision,
                    start_index=start,
                    now_ns=now_ns,
                )
            return
        if not isinstance(data, Iterable):
            raise ValidationError(f"cannot write a {type(data).__name__}", code="unsupported_record")
        iterator = iter(data)
        start = 0
        while True:
            block = list(itertools.islice(iterator, size))
            if not block:
                return
            yield serialize(block, database=database, precision=precision, start_index=start, now_ns=now_ns)
            start += len(block)

    def _submit(self, chunks: Iterator[SerializedChunk], database: str, precision: Precision) -> WriteFuture:
        future = WriteFuture(self._engine)
        started = time.perf_counter()
        try:
            for chunk in chunks:
                elapsed = time.perf_counter() - started
                _add_dropped(future, chunk)
                if chunk.lines and not self._engine.submit(
                    database, precision, chunk.lines, chunk.nbytes, future, serialize_time=elapsed
                ):
                    future.dropped += len(chunk.lines)
                started = time.perf_counter()
        except ValidationError as error:
            error.points_enqueued = future.points
            self._metrics.error("write", error)
            raise
        except (BufferFullError, ClientClosedError) as error:
            # Re-sending the whole call would duplicate these (untimed points get a new time).
            error.points_enqueued = future.points
            raise
        finally:
            future._seal()
        return future

    def flush(self, timeout: float | None = None) -> None:
        """Send everything buffered and wait for the server's answers.

        Raises :class:`~influxkit.exceptions.WriteError` for failed batches whose errors nobody
        retrieved from their futures (unless an ``on_error`` handler is installed).
        """
        warn_if_event_loop("InfluxClient.flush()", "use 'await AsyncInfluxClient.flush()'")
        self._engine.flush(timeout)

    def to_line_protocol(
        self,
        data: Any,
        *,
        database: str | None = None,
        precision: Precision | None = None,
        tags: Mapping[str, str] | None = None,
        measurement: str | None = None,
        tag_columns: Sequence[str] | None = None,
        field_columns: Sequence[str] | None = None,
        time_column: str | None = None,
    ) -> list[str]:
        """The lines :meth:`write` would send for ``data``, without sending them.

        Validation, tag injection (static, context, rules, enrichers) and escaping apply
        exactly as in :meth:`write`, so this shows what tag rules did and why a record is
        rejected. Like :meth:`write`, it locks the types of new fields.
        """
        target = database or self._database
        prec = precision or self._precision
        if prec not in PRECISION_DIVISORS:
            raise ValueError(f"precision must be one of 'ns', 'us', 'ms', 's'; got {prec!r}")
        with tag_context(**tags) if tags else nullcontext():
            chunks = self._chunks(data, target, prec, measurement, tag_columns, field_columns, time_column)
            return [line for chunk in chunks for line in chunk.lines]

    def _on_drop(self, error: ValidationError, count: int = 1) -> None:
        self._metrics.points_dropped(error.code, count)
        self._engine.count_dropped(count)
        context = {
            "client": self._settings.name,
            "code": error.code,
            "measurement": error.measurement,
            "key": error.key,
            "points": count,
        }
        # Rate limited per problem (code, measurement, key), so each distinct one is seen.
        self._drop_log.log(
            f"drop:{error.code}:{error.measurement}:{error.key}",
            logging.WARNING,
            "dropped %d invalid record(s): %s",
            count,
            error,
            extra={"influx": context},
        )

    def _learn_server_type(self, database: str, measurement: str, key: str, server_type: str) -> None:
        """The server reported a field type conflict: lock the type it actually stores."""
        kind = _SERVER_TYPES.get(server_type.lower())
        if kind is None:
            return
        previous = self._serializer.relock(database, measurement, key, kind)
        if previous is not None:
            log.warning(
                "the server stores field %r of %r as %s, but influxkit had locked it as %s from an "
                "earlier value; it is now locked as %s (call sync_schema() at startup to learn "
                "stored types up front)",
                key,
                measurement,
                kind.value,
                previous.value,
                kind.value,
                extra={"influx": {"client": self._settings.name, "measurement": measurement, "key": key}},
            )

    def locked_types(self, measurement: str, *, database: str | None = None) -> dict[str, FieldType]:
        """Field types declared or locked so far for ``measurement``."""
        return self._serializer.locked_types(database or self._database, measurement)

    # ------------------------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------------------------

    def _backend(self) -> V2QueryBackend | V3QueryBackend:
        backend = self._query_backend
        if backend is None or backend.pid != os.getpid():
            with _backend_lock:
                backend = self._query_backend
                # After fork() the parent's backend must not be used (its connections are
                # the parent's): the child creates its own.
                if backend is None or backend.pid != os.getpid():
                    if self._settings.connection.version == 3:
                        backend = V3QueryBackend(self._settings)
                    else:
                        backend = V2QueryBackend(self._settings)
                    self._query_backend = backend
        return backend

    def _language(self, language: Language | None) -> Language:
        chosen = language or self._settings.query_language
        version = self._settings.connection.version
        if version == 2 and chosen != "flux":
            raise ConfigurationError("InfluxDB 2 queries must use Flux (language='flux')")
        if version == 3 and chosen == "flux":
            raise ConfigurationError("InfluxDB 3 does not support Flux; use 'sql' or 'influxql'")
        return chosen

    def query(
        self,
        query: str,
        *,
        language: Language | None = None,
        database: str | None = None,
        params: Mapping[str, Any] | None = None,
        timeout: float | None = None,
    ) -> QueryResult:
        """Run a query and return all rows.

        Args:
            query: SQL or InfluxQL (InfluxDB 3), Flux (InfluxDB 2).
            language: Default ``query.language`` (sql for v3, flux for v2).
            database: InfluxDB 3 database (default ``connection.database``); Flux names its bucket itself.
            params: Bound parameters - ``$name`` placeholders in SQL/InfluxQL, ``params.name`` in Flux
                (bound as escaped literals, so this works on InfluxDB OSS too). Prefer them over
                string formatting to avoid injection.
            timeout: Seconds (InfluxDB 3; InfluxDB 2 uses ``query.timeout``).
        """
        if self._closed:
            raise ClientClosedError("the client is closed")
        warn_if_event_loop("InfluxClient.query()", "use 'await AsyncInfluxClient.query()'")
        chosen = self._language(language)
        started = time.perf_counter()
        try:
            result = run_query(self._backend(), query, chosen, database or self._database, params, timeout)
        except InfluxKitError as error:
            self._metrics.query(chosen, "error", time.perf_counter() - started)
            self._metrics.error("query", error)
            _hint_language(error, query, chosen)
            log.debug("query failed: %s", error)  # raised to the caller: no need to log loudly
            raise
        self._finish_query(result, chosen)
        return result

    def query_stream(
        self,
        query: str,
        *,
        language: Language | None = None,
        database: str | None = None,
        params: Mapping[str, Any] | None = None,
        timeout: float | None = None,
    ) -> Iterator[QueryResult]:
        """Run a query and yield results chunk by chunk (Arrow record batches on InfluxDB 3).

        Memory stays bounded by one chunk, so arbitrarily large results can be processed.
        """
        if self._closed:
            raise ClientClosedError("the client is closed")
        chosen = self._language(language)
        started = time.perf_counter()
        rows = 0
        try:
            for chunk in stream_query(
                self._backend(),
                query,
                chosen,
                database or self._database,
                params,
                timeout,
                self._settings.query.chunk_size,
            ):
                rows += chunk.num_rows
                yield chunk
        except InfluxKitError as error:
            self._metrics.query(chosen, "error", time.perf_counter() - started)
            self._metrics.error("query", error)
            raise
        self._metrics.query(chosen, "success", time.perf_counter() - started, rows)

    def _finish_query(self, result: QueryResult, language: str) -> None:
        self._metrics.query(language, "success", result.duration, result.num_rows)
        self._stages.record("query", result.duration)
        threshold = self._settings.profiling.slow_query_threshold
        if threshold is not None and result.duration > threshold:
            log.warning(
                "slow query: %.2f s, %d rows: %s", result.duration, result.num_rows, _shorten(result.query)
            )
        else:
            log.debug("query returned %d rows in %.1f ms", result.num_rows, result.duration * 1000)

    def sync_schema(
        self, *, database: str | None = None, lookback: str = "30d"
    ) -> dict[str, dict[str, FieldType]]:
        """Lock field types to what the server already stores, so conflicts are caught client-side.

        InfluxDB 3 reads ``information_schema.columns``. InfluxDB 2 inspects the last value of
        every field within ``lookback``. Returns ``{measurement: {field: type}}``.
        """
        target = database or self._database
        if self._settings.connection.version == 3:
            schema, tags = self._v3_schema(target)
        else:
            schema, tags = self._v2_schema(target, lookback)
        for measurement, fields in schema.items():
            try:
                self._serializer.seed_types(target, measurement, fields)
                if measurement in tags:
                    self._serializer.seed_tag_keys(target, measurement, tags[measurement])
            except ValidationError as error:
                log.warning("ignoring server measurement %r: %s", measurement, error)
        log.info("synced schema of %d measurements from %r", len(schema), target)
        return schema

    def _v3_schema(self, database: str) -> tuple[dict[str, dict[str, FieldType]], dict[str, set[str]]]:
        result = self.query(
            "SELECT table_name, column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'iox'",
            database=database,
        )
        schema: dict[str, dict[str, FieldType]] = {}
        tags: dict[str, set[str]] = {}
        for row in result.to_dicts():
            table, column, kind = row["table_name"], row["column_name"], str(row["data_type"])
            if column == "time":
                continue
            if kind.startswith("Dictionary"):
                tags.setdefault(table, set()).add(column)
                continue
            mapped = _ARROW_TYPES.get(kind)
            if mapped is not None:
                schema.setdefault(table, {})[column] = mapped
        return schema, tags

    def _v2_schema(
        self, bucket: str, lookback: str
    ) -> tuple[dict[str, dict[str, FieldType]], dict[str, set[str]]]:
        if not lookback.isalnum():
            raise ValueError(f"invalid lookback {lookback!r}; use a Flux duration such as '30d'")
        backend = self._backend()
        assert isinstance(backend, V2QueryBackend)
        flux = (
            f"from(bucket: {flux_string(bucket)}) |> range(start: -{lookback}) |> last() "
            '|> keep(columns: ["_measurement", "_field", "_value"])'
        )
        tables = backend.tables(flux, None)
        schema: dict[str, dict[str, FieldType]] = {}
        for table in tables:
            value_column = next((c for c in table.columns if c.label == "_value"), None)
            mapped = _FLUX_TYPES.get(value_column.data_type) if value_column is not None else None
            if mapped is None or not table.records:
                continue
            record = table.records[0]
            schema.setdefault(record["_measurement"], {})[record["_field"]] = mapped
        return schema, {}

    # ------------------------------------------------------------------------------------
    # Diagnostics and lifecycle
    # ------------------------------------------------------------------------------------

    def ping(self) -> ServerInfo:
        """Check connectivity (and, on InfluxDB 3, the token); return the server version."""
        started = time.perf_counter()
        response = self._transport.request("GET", "/ping")
        latency = time.perf_counter() - started
        if response.status >= 300:
            error = error_from_response(response)
            self._transport.explain(error)
            raise error
        version = response.headers.get("x-influxdb-version")
        build = response.headers.get("x-influxdb-build")
        if response.body[:1] == b"{":
            import json

            try:
                body = json.loads(response.body)
                version = version or body.get("version")
                build = body.get("product_name") or build
            except ValueError:
                pass
        if version is not None:
            version = version.removeprefix("v")
        return ServerInfo(self._settings.connection.url, version, build, latency)

    def check(self) -> ServerInfo:
        """Verify the settings against the server; call it at startup to fail fast.

        Checks that the server is reachable and runs the configured InfluxDB version, and that
        the token may write to the database/bucket (with an empty write: nothing is stored).
        Without this, such mistakes surface on the first background write.

        Raises:
            ConfigurationError: the server is not the configured InfluxDB version (or not InfluxDB).
            AuthenticationError: the token is missing or invalid.
            PermissionDeniedError: the token may not write to the database/bucket.
            NotFoundError: the bucket or organization does not exist (InfluxDB 2).
            TransportError: the server cannot be reached.
        """
        conn = self._settings.connection
        info = self.ping()
        if info.major is None:
            raise ConfigurationError(
                f"{conn.url} did not identify itself as InfluxDB (no version in its /ping answer): "
                "check connection.url"
            )
        if info.major != conn.version:
            raise ConfigurationError(
                f"connection.version = {conn.version} but {conn.url} runs InfluxDB {info.version}: "
                f"set connection.version = {info.major}"
            )
        # An empty write checks the token, organization and bucket without storing anything.
        path, params = self._engine._endpoint(self._database, self._precision)
        response = self._transport.request("POST", path, params=params, body=b"")
        if response.status == 204 or (response.status == 400 and b"empty" in response.body):
            return info
        if 200 <= response.status < 300 and is_html(response):
            raise ConfigurationError(str(unexpected_page_error(response, path, conn.url, conn.version)))
        error = error_from_response(response)
        self._transport.explain(error)
        error.add_note(f"checked by an empty write to {self._database!r} ({path})")
        raise error

    def stats(self) -> ClientStats:
        """Counters of the write engine and per-stage timings since the client was created."""
        return ClientStats(write=self._engine.stats(), stages=self._stages.snapshot())

    def profile(
        self, path: str | Path | None = None, *, memory: bool = False, log_summary: bool = False
    ) -> AbstractContextManager[ProfileReport]:
        """Profile a block of code with cProfile (see :func:`influxkit.profiling.profile`)."""
        return profile(path, memory=memory, log_summary=log_summary)

    def close(self, timeout: float | None = None) -> None:
        """Flush buffered writes and release resources.

        Waits up to ``timeout`` seconds (default ``write.close_timeout``) for buffered data.

        Raises :class:`~influxkit.exceptions.WriteError` if batches failed and no ``on_error``
        handler is installed. Idempotent.
        """
        if self._closed:
            return
        self._engine.check_wait_allowed("close()")
        warn_if_event_loop("InfluxClient.close()", "use 'await AsyncInfluxClient.close()'")
        self._closed = True
        failure = self._engine.close(timeout)
        self._drop_log.flush()
        self._serializer._warnings.flush()
        backend = self._query_backend
        if backend is not None and backend.pid == os.getpid():  # never the parent's, after fork()
            try:
                backend.close()
            except Exception:  # pragma: no cover - best effort
                log.debug("error closing query backend", exc_info=True)
        log.info("influxkit client %r closed", self._settings.name)
        if failure is not None:
            raise failure

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        if exc is None:
            self.close()
            return
        try:
            self.close()
        except InfluxKitError as close_error:  # do not mask the original exception
            log.error("while closing after an exception: %s", close_error)

    def __repr__(self) -> str:
        conn = self._settings.connection
        state = "closed" if self._closed else "open"
        return (
            f"<InfluxClient {self._settings.name!r} v{conn.version} {conn.url} db={conn.database!r} {state}>"
        )


_ARROW_TYPES: dict[str, FieldType] = {
    "Float64": FieldType.FLOAT,
    "Int64": FieldType.INTEGER,
    "UInt64": FieldType.UINTEGER,
    "Utf8": FieldType.STRING,
    "Utf8View": FieldType.STRING,
    "LargeUtf8": FieldType.STRING,
    "Boolean": FieldType.BOOLEAN,
}
_FLUX_TYPES: dict[str, FieldType] = {
    "double": FieldType.FLOAT,
    "long": FieldType.INTEGER,
    "unsignedLong": FieldType.UINTEGER,
    "string": FieldType.STRING,
    "boolean": FieldType.BOOLEAN,
}


def flux_string(value: str) -> str:
    """Quote ``value`` as a Flux string literal (escaping quotes, backslashes and ``${`` interpolation)."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("${", "\\${")
    return f'"{escaped}"'


def _shorten(text: str, limit: int = 300) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _add_dropped(future: WriteFuture, chunk: SerializedChunk) -> None:
    future.dropped += chunk.dropped
    if chunk.rejected and len(future.rejected) < MAX_REJECTED:
        future.rejected += chunk.rejected[: MAX_REJECTED - len(future.rejected)]


_TAG_SETTINGS = frozenset({"static", "from_env", "rules", "on_conflict"})


def _check_static_tags(tags: Mapping[str, Any] | None) -> None:
    """``tags=`` adds static tags; catch [tags] settings passed there by mistake."""
    if tags and not _TAG_SETTINGS.isdisjoint(tags):
        raise TypeError(
            f"tags= adds static tags to every point, but {sorted(_TAG_SETTINGS & set(tags))} are [tags] "
            "settings: set them in the config file or with load_settings(tags={...})"
        )


def _hint_language(error: BaseException, query: str, language: str) -> None:
    """Point out a query written in another server version's language."""
    text = query.lstrip().lower()
    if language != "flux" and ("|>" in query or text.startswith(("from(", "import "))):
        error.add_note("this looks like Flux: InfluxDB 3 queries use SQL or InfluxQL")
    elif language == "flux" and text.startswith(("select ", "show ", "with ")):
        error.add_note("this looks like SQL/InfluxQL: InfluxDB 2 queries use Flux")
