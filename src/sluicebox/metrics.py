"""Prometheus instrumentation.

Metric families are created once per (registry, namespace) and shared by every client,
which is distinguished by the ``client`` label - so several clients (or a client re-created
in tests) never trigger duplicate-registration errors. Instrumentation is per batch or per
call, never per point, so it costs nothing measurable on the hot path.

Metrics (default namespace ``sluicebox``):

=====================================================  =========  ===========================
name                                                    type       labels
=====================================================  =========  ===========================
``points_written_total``                                counter    client, database
``points_failed_total``                                 counter    client, database
``points_dropped_total``                                counter    client, reason
``write_batches_total``                                 counter    client, database, outcome
``write_bytes_total``                                   counter    client, database, kind
``write_retries_total``                                 counter    client, reason
``write_request_duration_seconds``                      histogram  client, database
``write_batch_duration_seconds``                        histogram  client, database
``write_batch_points``                                  histogram  client
``write_buffer_bytes``                                  gauge      client
``write_buffer_limit_bytes``                            gauge      client
``write_max_batch_bytes``                               gauge      client
``write_last_success_timestamp_seconds``                gauge      client
``write_inflight_requests``                             gauge      client
``queries_total``                                       counter    client, language, outcome
``query_duration_seconds``                              histogram  client, language
``query_rows_total``                                    counter    client, language
``errors_total``                                        counter    client, operation, error
``stage_duration_seconds``                              histogram  client, stage
``client_info``                                         info       client, version, ...
=====================================================  =========  ===========================
"""

from __future__ import annotations

import threading
import time
import weakref
from typing import TYPE_CHECKING, Any

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram, Info, start_http_server

from . import _fork
from ._version import __version__
from .exceptions import ConfigurationError

if TYPE_CHECKING:
    from .config import MetricsConfig

__all__ = ["Metrics", "NullMetrics", "create_metrics"]

_LATENCY_BUCKETS = (
    0.0005,
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
)
_SIZE_BUCKETS = (1, 10, 100, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000)

_families_lock = threading.Lock()


def _new_families_lock() -> None:
    global _families_lock
    _families_lock = threading.Lock()


_fork.on_child(locks=_new_families_lock)
_families: weakref.WeakKeyDictionary[CollectorRegistry, dict[str, _Families]] = weakref.WeakKeyDictionary()
_exporters: dict[tuple[str, int], Any] = {}


class _Families:
    def __init__(self, namespace: str, registry: CollectorRegistry) -> None:
        def counter(name: str, doc: str, labels: tuple[str, ...]) -> Counter:
            return Counter(name, doc, labels, namespace=namespace, registry=registry)

        def histogram(name: str, doc: str, labels: tuple[str, ...], buckets: tuple[float, ...]) -> Histogram:
            return Histogram(name, doc, labels, namespace=namespace, registry=registry, buckets=buckets)

        def gauge(name: str, doc: str, labels: tuple[str, ...]) -> Gauge:
            return Gauge(name, doc, labels, namespace=namespace, registry=registry)

        self.info = Info(
            "client", "sluicebox client information", ("client",), namespace=namespace, registry=registry
        )
        self.points_written = counter(
            "points_written", "Points acknowledged by the server", ("client", "database")
        )
        self.points_failed = counter(
            "points_failed", "Points that could not be written after retries", ("client", "database")
        )
        self.points_dropped = counter(
            "points_dropped", "Points dropped before sending (validation, full buffer)", ("client", "reason")
        )
        self.batches = counter("write_batches", "Write batches by outcome", ("client", "database", "outcome"))
        self.bytes = counter(
            "write_bytes",
            "Line protocol bytes (kind=raw: uncompressed, sent: on the wire)",
            ("client", "database", "kind"),
        )
        self.retries = counter("write_retries", "Write request retries by reason", ("client", "reason"))
        self.request_seconds = histogram(
            "write_request_duration_seconds",
            "Duration of single write requests",
            ("client", "database"),
            _LATENCY_BUCKETS,
        )
        self.batch_seconds = histogram(
            "write_batch_duration_seconds",
            "Time from a batch's first point being buffered until it was acknowledged",
            ("client", "database"),
            _LATENCY_BUCKETS,
        )
        self.batch_points = histogram(
            "write_batch_points", "Points per write batch", ("client",), _SIZE_BUCKETS
        )
        self.buffer_bytes = gauge("write_buffer_bytes", "Bytes buffered or in flight", ("client",))
        self.buffer_limit = gauge(
            "write_buffer_limit_bytes",
            "write.max_pending_bytes (the buffer is full at this size)",
            ("client",),
        )
        self.max_batch_bytes = gauge(
            "write_max_batch_bytes",
            "Request size limit in effect (lowered after HTTP 413 responses)",
            ("client",),
        )
        self.last_success = gauge(
            "write_last_success_timestamp_seconds",
            "Unix time of the last successfully written batch",
            ("client",),
        )
        self.inflight = gauge("write_inflight_requests", "Write requests in flight", ("client",))
        self.queries = counter("queries", "Queries by outcome", ("client", "language", "outcome"))
        self.query_seconds = histogram(
            "query_duration_seconds",
            "Query duration including result transfer",
            ("client", "language"),
            _LATENCY_BUCKETS,
        )
        self.query_rows = counter("query_rows", "Rows returned by queries", ("client", "language"))
        self.errors = counter("errors", "Errors by operation and type", ("client", "operation", "error"))
        self.stage_seconds = histogram(
            "stage_duration_seconds", "Time spent per pipeline stage", ("client", "stage"), _LATENCY_BUCKETS
        )


class Metrics:
    """Instrumentation bound to one client name."""

    enabled = True

    def __init__(self, config: MetricsConfig, client: str, registry: CollectorRegistry | None = None) -> None:
        registry = registry if registry is not None else REGISTRY
        with _families_lock:
            per_registry = _families.setdefault(registry, {})
            families = per_registry.get(config.namespace)
            if families is None:
                families = per_registry[config.namespace] = _Families(config.namespace, registry)
            if config.port is not None:
                key = (config.addr, config.port)
                if key not in _exporters:
                    try:
                        _exporters[key] = start_http_server(config.port, addr=config.addr, registry=registry)
                    except OSError as exc:
                        raise ConfigurationError(
                            f"cannot serve metrics on {config.addr}:{config.port} ([metrics] port): {exc}. "
                            "With several processes, give each its own port or export metrics from the "
                            "application (prometheus_client multiprocess mode) and leave port unset"
                        ) from exc
        self.registry = registry
        self.client = client
        self._f = families
        self._children: dict[tuple[str, tuple[str, ...]], Any] = {}
        self._buffer_bytes = families.buffer_bytes.labels(client)
        self._inflight = families.inflight.labels(client)
        self._batch_points = families.batch_points.labels(client)
        self._last_success = families.last_success.labels(client)

    def prime(self, database: str, buffer_limit: int, max_batch_bytes: int) -> None:
        """Create the series alerts rely on at zero, before the first event."""
        for name, labels in (
            ("points_written", (database,)),
            ("points_failed", (database,)),
            ("batches", (database, "success")),
            ("batches", (database, "failed")),
            ("batches", (database, "partial")),
            ("points_dropped", ("buffer_full",)),
        ):
            self._child(name, *labels)
        self._f.buffer_limit.labels(self.client).set(buffer_limit)
        self.batch_limit(max_batch_bytes)

    def batch_limit(self, nbytes: int) -> None:
        self._f.max_batch_bytes.labels(self.client).set(nbytes)

    def _child(self, name: str, *labels: str) -> Any:
        key = (name, labels)
        child = self._children.get(key)
        if child is None:
            child = getattr(self._f, name).labels(self.client, *labels)
            self._children[key] = child
        return child

    def set_info(self, **info: str) -> None:
        self._f.info.labels(self.client).info({"version": __version__, **info})

    # -- writes -------------------------------------------------------------------------------

    def batch_succeeded(
        self, database: str, points: int, raw_bytes: int, sent_bytes: int, seconds: float
    ) -> None:
        self._child("points_written", database).inc(points)
        self._child("batches", database, "success").inc()
        self._child("bytes", database, "raw").inc(raw_bytes)
        self._child("bytes", database, "sent").inc(sent_bytes)
        self._child("batch_seconds", database).observe(seconds)
        self._batch_points.observe(points)
        self._last_success.set(time.time())

    def batch_failed(self, database: str, points: int, outcome: str, error: BaseException) -> None:
        self._child("points_failed", database).inc(points)
        self._child("batches", database, outcome).inc()
        self._child("errors", "write", type(error).__name__).inc()

    def points_partially_rejected(self, database: str, written: int, rejected: int) -> None:
        if written:
            self._child("points_written", database).inc(written)
        if rejected:
            self._child("points_failed", database).inc(rejected)

    def points_dropped(self, reason: str, count: int = 1) -> None:
        self._child("points_dropped", reason).inc(count)

    def request(self, database: str, seconds: float) -> None:
        self._child("request_seconds", database).observe(seconds)

    def retry(self, reason: str) -> None:
        self._child("retries", reason).inc()

    def buffer(self, nbytes: int) -> None:
        self._buffer_bytes.set(nbytes)

    def inflight(self, count: int) -> None:
        self._inflight.set(count)

    def stage(self, stage: str, seconds: float) -> None:
        self._child("stage_seconds", stage).observe(seconds)

    # -- queries ------------------------------------------------------------------------------

    def query(self, language: str, outcome: str, seconds: float, rows: int = 0) -> None:
        self._child("queries", language, outcome).inc()
        self._child("query_seconds", language).observe(seconds)
        if rows:
            self._child("query_rows", language).inc(rows)

    def error(self, operation: str, error: BaseException) -> None:
        self._child("errors", operation, type(error).__name__).inc()


class NullMetrics(Metrics):
    """Drop-in replacement when metrics are disabled."""

    enabled = False

    def __init__(self) -> None:
        pass

    def set_info(self, **info: str) -> None:
        pass

    def prime(self, database: str, buffer_limit: int, max_batch_bytes: int) -> None:
        pass

    def batch_limit(self, nbytes: int) -> None:
        pass

    def batch_succeeded(
        self, database: str, points: int, raw_bytes: int, sent_bytes: int, seconds: float
    ) -> None:
        pass

    def batch_failed(self, database: str, points: int, outcome: str, error: BaseException) -> None:
        pass

    def points_partially_rejected(self, database: str, written: int, rejected: int) -> None:
        pass

    def points_dropped(self, reason: str, count: int = 1) -> None:
        pass

    def request(self, database: str, seconds: float) -> None:
        pass

    def retry(self, reason: str) -> None:
        pass

    def buffer(self, nbytes: int) -> None:
        pass

    def inflight(self, count: int) -> None:
        pass

    def stage(self, stage: str, seconds: float) -> None:
        pass

    def query(self, language: str, outcome: str, seconds: float, rows: int = 0) -> None:
        pass

    def error(self, operation: str, error: BaseException) -> None:
        pass


def create_metrics(config: MetricsConfig, client: str, registry: CollectorRegistry | None = None) -> Metrics:
    return Metrics(config, client, registry) if config.enabled else NullMetrics()
