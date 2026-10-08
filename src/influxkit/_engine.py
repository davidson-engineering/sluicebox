"""The asynchronous write engine.

``write()`` serializes on the caller's thread and hands lines to the engine, which:

* appends them to an open batch per (database, precision), sealing it when it reaches
  ``batch_size`` lines or ``max_batch_bytes``, or (via the flusher thread) once it is
  ``flush_interval`` old;
* lets ``concurrency`` sender threads gzip and POST sealed batches over a keep-alive
  connection pool, retrying transient failures with backoff and splitting batches the
  server rejects as too large (HTTP 413);
* bounds memory with ``max_pending_bytes`` (block / drop / raise when full);
* resolves each ``write()`` call's :class:`~influxkit.futures.WriteFuture`, attributing
  per-line partial-write errors to the exact call (and line) that produced them.

Threads are daemons, started on first use, re-created after ``fork()``, and the buffer is
flushed at interpreter exit (``write.flush_on_exit``).

Fork safety: a child process starts with only the thread that called ``fork()``. The engine
locks and the futures lock are held across ``fork()``, so the child inherits consistent,
unlocked state; the child then forgets the parent's buffers, threads and connections and
fails the futures of data the parent is sending.
"""

from __future__ import annotations

import atexit
import gzip
import logging
import os
import re
import sys
import threading
import time
import weakref
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from . import _fork
from . import futures as _futures
from ._http import Transport, error_from_response, is_html, unexpected_page_error
from ._lineprotocol import Dialect, LineSyntaxError, parse_line
from ._retry import RetryPolicy
from .exceptions import (
    BufferFullError,
    ClientClosedError,
    InfluxConnectionError,
    InfluxKitError,
    InfluxTimeoutError,
    LineError,
    NotFoundError,
    PartialWriteError,
    PayloadTooLargeError,
    ServerError,
    TransportError,
    WriteError,
)
from .log import RateLimitedLog

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from .config import InfluxSettings
    from .futures import WriteFuture
    from .metrics import Metrics
    from .profiling import StageRecorder

__all__ = ["EngineStats", "WriteEngine", "WriteFailure"]

log = logging.getLogger("influxkit.write")

_V3_PRECISION = {"ns": "nanosecond", "us": "microsecond", "ms": "millisecond", "s": "second"}
_MAX_RECORDED_FAILURES = 100
#: A 413 for a request this small is not about its size (e.g. a proxy refusing all bodies).
_MIN_SPLIT_BYTES = 1024
# Field type conflicts reported by the servers: (field, the type the server stores).
_V3_TYPE_CONFLICT = re.compile(
    r"invalid column type for column '(?P<field>[^']+)', expected iox::column_type::field::(?P<type>\w+)"
)
_V2_TYPE_CONFLICT = re.compile(
    r'field type conflict: input field "(?P<field>[^"]+)" on measurement "(?P<measurement>[^"]+)" '
    r"is type \w+, already exists as type (?P<type>\w+)"
)


@dataclass(frozen=True, slots=True)
class WriteFailure:
    """Lines of one batch that could not be written, passed to the client's ``on_error`` handler.

    ``lines`` are the exact line protocol strings, so a handler can persist them (with
    ``precision``) to a dead-letter file or queue and replay them later with
    ``client.write(lines, database=..., precision=...)``. When the server rejected individual
    lines, ``error`` is a :class:`PartialWriteError` whose ``line_errors`` number ``lines``.
    """

    database: str
    precision: str
    lines: list[str]
    error: BaseException
    #: True if the lines failed for a transient reason (network, timeouts, 429/5xx after all
    #: retries, the client closing): replaying them later should work. False if the server
    #: refused them (invalid data, authentication, missing database): fix that first.
    retryable: bool = False

    @property
    def points(self) -> int:
        return len(self.lines)


@dataclass(frozen=True, slots=True)
class EngineStats:
    """Snapshot of the write engine (since the client was created)."""

    points_written: int
    points_failed: int
    #: Points dropped before sending: invalid records (``on_invalid = "drop"``) and a full
    #: buffer (``on_full = "drop"``); ``points_dropped_total{reason}`` has the breakdown.
    points_dropped: int
    batches_written: int
    batches_failed: int
    retries: int
    bytes_raw: int
    bytes_sent: int
    buffered_bytes: int
    open_batches: int
    queued_batches: int
    inflight_batches: int


class _Batch:
    __slots__ = (
        "created",
        "database",
        "finished",
        "lines",
        "nbytes",
        "precision",
        "sealed",
        "seq",
        "serialize_time",
        "waiters",
    )

    def __init__(self, database: str, precision: str) -> None:
        self.database = database
        self.precision = precision
        self.lines: list[str] = []
        self.nbytes = 0
        #: Caller-thread time spent serializing the lines of this batch (recorded once per batch).
        self.serialize_time = 0.0
        #: (future, first line index in this batch, line count, offset within the future's lines)
        self.waiters: list[tuple[WriteFuture, int, int, int]] = []
        self.created = time.monotonic()
        self.sealed = 0.0
        self.seq = 0
        #: Set (with the engine lock) by whoever reports the outcome: the sender, or close()
        #: when it gives up on a request that is still in flight.
        self.finished = False


@dataclass(slots=True)
class _Failure:
    """Lines ``lo:hi`` of a batch were affected by ``error``.

    ``line`` is set when the server rejected exactly one identified line. ``failed`` is how
    many lines of the range were not written: all of them, except for InfluxDB 2 partial
    writes, which store everything but a reported number of (unidentified) lines.
    """

    lo: int
    hi: int
    error: BaseException
    line: LineError | None = None
    failed: int = -1

    def __post_init__(self) -> None:
        if self.failed < 0:
            self.failed = self.hi - self.lo


@dataclass(slots=True)
class _Unreported:
    """A failed batch that flush()/close() still have to report (unless its futures did)."""

    error: BaseException
    points: int
    #: The futures that received an error from this batch.
    futures: tuple[WriteFuture, ...]


@dataclass(slots=True)
class _Outcome:
    failures: list[_Failure] = field(default_factory=list)
    retries: int = 0
    bytes_sent: int = 0


def _partial_error(
    failures: list[_Failure], numbers: Iterable[int], lines: list[str], scope: str, source: BaseException
) -> PartialWriteError:
    """One PartialWriteError for individually rejected lines (``numbers`` are their 1-based numbers)."""
    assert isinstance(source, ServerError)
    line_errors = tuple(
        LineError(number, f.line.message if f.line else str(f.error), lines[f.lo])
        for f, number in zip(failures, numbers, strict=True)
    )
    return PartialWriteError(
        f"{len(line_errors)} {scope}rejected by the server: "
        + "; ".join(f"line {e.line_number}: {e.message}" for e in line_errors[:3]),
        status=source.status,
        code=source.code,
        request_id=source.request_id,
        line_errors=line_errors,
    )


def _batch_failure(batch: _Batch, failures: list[_Failure], retryable: bool) -> WriteFailure:
    """Everything that failed in one batch, for ``on_error``."""
    lines: list[str] = []
    for failure in failures:
        lines += batch.lines[failure.lo : failure.hi]
    if len(failures) == 1:
        error = failures[0].error
    elif all(failure.line is not None and failure.hi - failure.lo == 1 for failure in failures):
        # Rejected lines: number them within ``lines`` (they are consecutive there).
        error = _partial_error(
            failures, range(1, len(failures) + 1), batch.lines, "lines ", failures[0].error
        )
    else:
        error = _futures._fresh(failures[0].error)  # the futures share the original: note only this copy
        error.add_note(f"{len(failures) - 1} other part(s) of this batch failed too (all lines are included)")
    return WriteFailure(batch.database, batch.precision, lines, error, retryable)


_ENGINES: weakref.WeakSet[WriteEngine] = weakref.WeakSet()
#: ``engine`` is set on sender threads (done-callbacks and ``on_error`` run there).
_SENDER = threading.local()


class WouldBlock(Exception):
    """Raised by ``submit(block=False)`` instead of waiting for buffer space."""


class WriteEngine:
    """Buffers line protocol and sends it from background threads."""

    def __init__(
        self,
        *,
        transport: Transport,
        settings: InfluxSettings,
        metrics: Metrics,
        stages: StageRecorder,
        on_error: Callable[[WriteFailure], Any] | None = None,
        on_type_conflict: Callable[[str, str, str, str], Any] | None = None,
    ) -> None:
        cfg = settings.write
        self.name = settings.name
        self._transport = transport
        self._metrics = metrics
        self._stages = stages
        self._on_error = on_error
        #: Called with (database, measurement, field, server type) when the server reports a
        #: field type conflict, so the client can lock the type the server really stores.
        self._on_type_conflict = on_type_conflict
        self._version = settings.connection.version
        self._dialect = Dialect.for_version(self._version)
        self._org = settings.connection.org
        self._api = settings.write_api
        self._batch_size = cfg.batch_size
        self._max_batch_bytes = int(cfg.max_batch_bytes)
        self._flush_interval = cfg.flush_interval
        self._concurrency = cfg.concurrency
        self._max_pending = int(cfg.max_pending_bytes)
        self._on_full = cfg.on_full
        self._block_timeout = cfg.block_timeout
        self._gzip = cfg.gzip
        self._gzip_level = cfg.gzip_level
        self._gzip_min = int(cfg.gzip_min_bytes)
        self._no_sync = cfg.no_sync
        self._accept_partial = cfg.accept_partial
        self.flush_on_exit = cfg.flush_on_exit
        self.close_timeout = cfg.close_timeout
        self._request_timeout = settings.connection.timeout
        self._retry = RetryPolicy(cfg.retry)
        self._max_attempts = cfg.retry.max_attempts
        self._max_elapsed = cfg.retry.max_elapsed
        self._log_rejected = settings.logging.log_rejected_lines
        self._slow_batch = settings.profiling.slow_batch_threshold
        self._rate_log = RateLimitedLog(log)
        self._dropped_unlogged = 0  # points dropped (buffer full) since the last warning
        self._endpoints: dict[tuple[str, str], tuple[str, dict[str, str]]] = {}
        self._init_state()
        _ENGINES.add(self)
        _register_exit_flush()
        metrics.prime(settings.connection.database, self._max_pending, self._max_batch_bytes)

    def _init_state(self) -> None:
        self._lock = threading.Lock()
        self._has_work = threading.Condition(self._lock)
        self._has_space = threading.Condition(self._lock)
        self._progress = threading.Condition(self._lock)
        self._flusher_wake = threading.Condition(self._lock)
        self._stop = threading.Event()
        self._open: dict[tuple[str, str], _Batch] = {}
        self._queue: deque[_Batch] = deque()
        self._outstanding: set[int] = set()
        self._seq = 0
        self._sending: set[_Batch] = set()
        self._pending_bytes = 0
        self._threads: list[threading.Thread] = []
        self._closing = False
        self._closed = False
        self._deadline: float | None = None
        self._unreported: list[_Unreported] = []
        # Failures beyond _MAX_RECORDED_FAILURES are only counted.
        self._overflow_points = 0
        self._overflow_batches = 0
        self._pid = os.getpid()
        # statistics
        self._points_written = 0
        self._points_failed = 0
        self._points_dropped = 0
        self._batches_written = 0
        self._batches_failed = 0
        self._retries = 0
        self._bytes_raw = 0
        self._bytes_sent = 0

    # ------------------------------------------------------------------------------------
    # Producer side
    # ------------------------------------------------------------------------------------

    def submit(
        self,
        database: str,
        precision: str,
        lines: list[str],
        nbytes: int,
        future: WriteFuture,
        *,
        block: bool = True,
        serialize_time: float = 0.0,
    ) -> bool:
        """Buffer ``lines`` for ``future``. Returns False if they were dropped (buffer full).

        With ``block=False`` a full buffer under the ``block`` policy raises :class:`WouldBlock`
        (nothing is buffered) so asyncio callers can wait elsewhere via :meth:`wait_for_space`.
        """
        if not lines:
            return True
        with self._lock:
            if self._closing:
                raise ClientClosedError("the client is closed; no further writes are accepted")
            if not self._threads:
                self._start_threads()
            if (
                self._pending_bytes + nbytes > self._max_pending
                and self._pending_bytes > 0
                and not self._wait_for_space(nbytes, block)
            ):
                self._points_dropped += len(lines)
                self._dropped_unlogged += len(lines)
                self._metrics.points_dropped("buffer_full", len(lines))
                if self._rate_log.log(
                    "buffer_full",
                    logging.WARNING,
                    "write buffer full (%d bytes pending): dropped %d points since the last report "
                    "(write.on_full = 'drop')",
                    self._pending_bytes,
                    self._dropped_unlogged,
                    extra={"influx": {"client": self.name, "points": self._dropped_unlogged}},
                ):
                    self._dropped_unlogged = 0
                return False
            key = (database, precision)
            count = len(lines)
            batch = self._open.get(key)
            if batch is not None and (
                len(batch.lines) + count > self._batch_size or batch.nbytes + nbytes > self._max_batch_bytes
            ):
                self._seal(key, batch)
                batch = None
            if count > self._batch_size or nbytes > self._max_batch_bytes:
                # Lines are re-measured there: count exactly what the batches hold, since
                # _finish releases batch.nbytes.
                self._pending_bytes += self._submit_split(key, lines, future)
            else:
                self._pending_bytes += nbytes
                if batch is None:
                    batch = self._new_open_batch(key)
                batch.waiters.append((future, len(batch.lines), count, future.points))
                future._attach()
                future.points += count
                batch.lines += lines
                batch.nbytes += nbytes
                batch.serialize_time += serialize_time
                if len(batch.lines) >= self._batch_size or batch.nbytes >= self._max_batch_bytes:
                    self._seal(key, batch)
        return True

    def _new_open_batch(self, key: tuple[str, str]) -> _Batch:
        batch = self._open[key] = _Batch(*key)
        if len(self._open) == 1:
            self._flusher_wake.notify()
        return batch

    def _submit_split(self, key: tuple[str, str], lines: list[str], future: WriteFuture) -> int:
        """Cut an oversized submission into batches that respect both size limits.

        Returns the bytes added to batches (UTF-8, newlines included).
        """
        piece: list[str] = []
        piece_bytes = 0
        total = 0
        for line in lines:
            size = (len(line) if line.isascii() else len(line.encode("utf-8"))) + 1
            if piece and (len(piece) >= self._batch_size or piece_bytes + size > self._max_batch_bytes):
                batch = self._new_open_batch(key)
                self._fill(batch, piece, piece_bytes, future)
                self._seal(key, batch)
                piece = []
                piece_bytes = 0
            piece.append(line)
            piece_bytes += size
            total += size
        if piece:
            batch = self._new_open_batch(key)
            self._fill(batch, piece, piece_bytes, future)
            if len(batch.lines) >= self._batch_size or batch.nbytes >= self._max_batch_bytes:
                self._seal(key, batch)
        return total

    @staticmethod
    def _fill(batch: _Batch, lines: list[str], nbytes: int, future: WriteFuture) -> None:
        batch.waiters.append((future, len(batch.lines), len(lines), future.points))
        future._attach()
        future.points += len(lines)
        batch.lines += lines
        batch.nbytes += nbytes

    def _wait_for_space(self, nbytes: int, block: bool = True) -> bool:
        """Apply the overflow policy with the lock held; False means 'drop'."""
        if self._on_full == "drop":
            return False
        if not block and self._on_full == "block":
            raise WouldBlock
        if self._on_full == "raise" or getattr(_SENDER, "engine", None) is self:
            # Never block a sender thread (e.g. a callback that writes): nothing could drain the buffer.
            raise BufferFullError(
                f"write buffer is full ({self._pending_bytes} of {self._max_pending} bytes pending)"
            )
        started = time.perf_counter()
        deadline = time.monotonic() + self._block_timeout if self._block_timeout else None
        while True:
            # Checked after every wait: close() may have begun meanwhile, and nothing sends a
            # batch opened after it sealed the buffer.
            if self._closing:
                raise ClientClosedError("the client was closed while waiting for buffer space")
            if self._pending_bytes + nbytes <= self._max_pending or self._pending_bytes == 0:
                break
            remaining = None
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BufferFullError(
                        f"write buffer still full after {self._block_timeout} s "
                        f"({self._pending_bytes} of {self._max_pending} bytes pending)"
                    )
            self._seal_all_open()  # make sure everything buffered is on its way
            self._has_space.wait(remaining)
        self._stages.record("backpressure", time.perf_counter() - started)
        return True

    def count_dropped(self, points: int) -> None:
        """Count points dropped before they reached the engine (invalid records)."""
        with self._lock:
            self._points_dropped += points

    def wait_for_space(self, nbytes: int) -> None:
        """Block until ``nbytes`` more fit in the buffer (honouring ``write.block_timeout``)."""
        with self._lock:
            if self._pending_bytes + nbytes > self._max_pending and self._pending_bytes > 0:
                self._wait_for_space(nbytes)

    def _seal(self, key: tuple[str, str], batch: _Batch) -> None:
        """Queue a batch for sending (lock held). Buffer gauges are updated here, per batch."""
        if self._open.get(key) is batch:
            del self._open[key]
        self._metrics.buffer(self._pending_bytes)
        self._seq += 1
        batch.seq = self._seq
        batch.sealed = time.monotonic()
        self._outstanding.add(batch.seq)
        self._queue.append(batch)
        self._has_work.notify()

    def _seal_all_open(self) -> None:
        for key, batch in list(self._open.items()):
            self._seal(key, batch)

    def flush_nowait(self) -> None:
        """Send all buffered data now without waiting for it."""
        with self._lock:
            self._seal_all_open()

    def check_wait_allowed(self, what: str) -> None:
        """Refuse to wait for this engine on one of its own sender threads.

        Done-callbacks and ``on_error`` handlers run on sender threads before their batch is
        released, so waiting there for this engine's writes would wait for itself.
        """
        if getattr(_SENDER, "engine", None) is self:
            raise RuntimeError(
                f"{what} is not possible in a write done-callback or on_error handler of the same "
                "client: they run on its sender threads, which would wait for themselves. "
                "Hand the work to another thread instead."
            )

    def flush(self, timeout: float | None = None) -> None:
        """Send everything buffered so far and wait until the server has answered for it.

        Raises:
            InfluxTimeoutError: not finished within ``timeout`` seconds.
            WriteError: some batches failed since the previous flush and no ``on_error``
                handler is installed (with one, failures are reported only to the handler).
            RuntimeError: called from a done-callback or ``on_error`` handler of this client.
        """
        self.check_wait_allowed("flush()")
        deadline = time.monotonic() + timeout if timeout is not None else None
        with self._lock:
            self._seal_all_open()
            target = self._seq
            while self._outstanding and min(self._outstanding) <= target:
                remaining = None
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        pending = sum(1 for seq in self._outstanding if seq <= target)
                        raise InfluxTimeoutError(
                            f"flush did not complete within {timeout} s ({pending} batches pending)"
                        )
                self._progress.wait(remaining)
            failures = self._take_failures()
        if failures is not None:
            raise failures

    def _take_failures(self) -> WriteError | None:
        """The failures since the last flush that nobody has seen (lock held).

        A failure counts as seen once every write() call it affected has had its error
        retrieved from the future (``result()``, ``exception()``, ``await``, ``errors``).
        """
        pending = [entry for entry in self._unreported if not all(f._observed for f in entry.futures)]
        points = sum(entry.points for entry in pending) + self._overflow_points
        batches = len(pending) + self._overflow_batches
        self._unreported = []
        self._overflow_points = self._overflow_batches = 0
        if not batches:
            return None
        errors = [entry.error for entry in pending]
        first = f"; first error: {errors[0]}" if errors else ""
        error = WriteError(
            f"{batches} write batch(es) failed since the last flush ({points} points not written){first}",
            errors=errors,
            failed_points=points,
            failed_batches=batches,
        )
        if errors:
            error.__cause__ = errors[0]
        return error

    # ------------------------------------------------------------------------------------
    # Threads
    # ------------------------------------------------------------------------------------

    def _start_threads(self) -> None:
        for index in range(self._concurrency):
            thread = threading.Thread(
                target=self._sender_loop, name=f"influxkit-{self.name}-sender-{index}", daemon=True
            )
            thread.start()
            self._threads.append(thread)
        flusher = threading.Thread(
            target=self._flusher_loop, name=f"influxkit-{self.name}-flusher", daemon=True
        )
        flusher.start()
        self._threads.append(flusher)

    def _flusher_loop(self) -> None:
        interval = self._flush_interval
        with self._lock:
            while not self._closing:
                if not self._open:
                    self._flusher_wake.wait()
                    continue
                now = time.monotonic()
                oldest = min(batch.created for batch in self._open.values())
                wait = oldest + interval - now
                if wait > 0:
                    self._flusher_wake.wait(wait)
                    continue
                for key, batch in list(self._open.items()):
                    if batch.created + interval <= now:
                        self._seal(key, batch)

    def _sender_loop(self) -> None:
        _SENDER.engine = self
        while True:
            with self._lock:
                while not self._queue:
                    if self._closed:
                        return
                    self._has_work.wait()
                batch = self._queue.popleft()
                self._sending.add(batch)
                inflight = len(self._sending)
            self._metrics.inflight(inflight)
            outcome: _Outcome | None = None
            try:
                outcome = self._process(batch)
            except BaseException as exc:  # never let a sender thread die
                log.exception("unexpected error while sending a batch")
                outcome = _Outcome(failures=[_Failure(0, len(batch.lines), exc)])
            finally:
                self._finish(batch, outcome)

    # ------------------------------------------------------------------------------------
    # Sending
    # ------------------------------------------------------------------------------------

    def _endpoint(self, database: str, precision: str) -> tuple[str, dict[str, str]]:
        key = (database, precision)
        endpoint = self._endpoints.get(key)
        if endpoint is None:
            if self._api == "v3":
                params = {"db": database, "precision": _V3_PRECISION[precision]}
                if self._no_sync:
                    params["no_sync"] = "true"
                if not self._accept_partial:
                    params["accept_partial"] = "false"
                endpoint = ("/api/v3/write_lp", params)
            else:
                params = {"bucket": database, "precision": precision}
                if self._org:
                    params["org"] = self._org
                endpoint = ("/api/v2/write", params)
            self._endpoints[key] = endpoint
        return endpoint

    def _process(self, batch: _Batch) -> _Outcome:
        started = time.monotonic()
        self._stages.record("queue", started - batch.sealed)
        outcome = _Outcome()
        self._send_range(batch, batch.lines, 0, outcome)
        return outcome

    def _send_range(self, batch: _Batch, lines: list[str], base: int, outcome: _Outcome) -> None:
        """Send ``lines`` (batch lines ``base:base+len``), recording failures in ``outcome``."""
        payload = "\n".join(lines).encode("utf-8")
        try:
            self._post(batch, payload, outcome)
        except PayloadTooLargeError as exc:
            if len(lines) == 1 or len(payload) <= _MIN_SPLIT_BYTES:
                if len(lines) > 1:
                    exc.add_note(
                        f"even a {len(payload)}-byte request was refused: the limit is not about size "
                        "(check the body limit of a proxy in front of the server)"
                    )
                outcome.failures.append(_Failure(base, base + len(lines), exc))
                return
            self._learn_size_limit(len(payload))
            middle = len(lines) // 2
            log.debug("request of %d bytes too large; splitting %d lines in two", len(payload), len(lines))
            self._send_range(batch, lines[:middle], base, outcome)
            self._send_range(batch, lines[middle:], base + middle, outcome)
        except PartialWriteError as exc:
            numbered = [err for err in exc.line_errors if 1 <= err.line_number <= len(lines)]
            if numbered:
                for err in numbered:
                    index = base + err.line_number - 1
                    outcome.failures.append(_Failure(index, index + 1, exc, err))
            else:
                rejected = exc.rejected if exc.rejected else len(lines)
                outcome.failures.append(
                    _Failure(base, base + len(lines), exc, failed=min(rejected, len(lines)))
                )
        except InfluxKitError as exc:
            outcome.failures.append(_Failure(base, base + len(lines), exc))

    def _post(self, batch: _Batch, payload: bytes, outcome: _Outcome) -> None:
        """POST one payload, retrying transient failures. Raises the final error."""
        path, params = self._endpoint(batch.database, batch.precision)
        headers = {"Content-Type": "text/plain; charset=utf-8"}
        body = payload
        if self._gzip and len(payload) >= self._gzip_min:
            started = time.perf_counter()
            body = gzip.compress(payload, compresslevel=self._gzip_level, mtime=0)
            self._stages.record("compress", time.perf_counter() - started)
            headers["Content-Encoding"] = "gzip"
        first_attempt = time.monotonic()
        attempt = 0
        while True:
            attempt += 1
            started = time.perf_counter()
            error: InfluxKitError
            try:
                response = self._transport.request(
                    "POST", path, params=params, body=body, headers=headers, timeout=self._request_timeout
                )
            except TransportError as exc:
                error = exc
            else:
                elapsed = time.perf_counter() - started
                self._metrics.request(batch.database, elapsed)
                self._stages.record("request", elapsed)
                outcome.bytes_sent += len(body)
                if 200 <= response.status < 300:
                    if response.status == 204 or not is_html(response):
                        return
                    error = unexpected_page_error(response, path, self._transport.base_url, self._version)
                else:
                    error = error_from_response(response)
                    self._transport.explain(error)
                    if isinstance(error, NotFoundError) and path == "/api/v3/write_lp":
                        error.add_note(
                            "InfluxDB 3 Cloud Serverless/Dedicated/Clustered only offer /api/v2/write: "
                            "set write.api = 'v2'"
                        )
            if not self._retry.is_retryable(error) or (
                self._max_attempts is not None and attempt >= self._max_attempts
            ):
                if attempt > 1:
                    error.add_note(f"gave up after {attempt} attempts")
                raise error
            delay = self._retry.delay(attempt, error)
            if attempt == 1 and isinstance(error, InfluxConnectionError):
                # Most often a pooled keep-alive connection the server or a middlebox already
                # closed: retry at once on a fresh connection (backoff applies from then on).
                delay = 0.0
            now = time.monotonic()
            if self._max_elapsed is not None and now - first_attempt + delay > self._max_elapsed:
                remaining = first_attempt + self._max_elapsed - now
                if remaining <= 0:
                    error.add_note(f"gave up after {attempt} attempts: retry.max_elapsed reached")
                    raise error
                delay = remaining  # a long backoff or Retry-After: one last attempt at the deadline
            if self._deadline is not None and now + delay > self._deadline:
                error.add_note("gave up retrying: the client is closing")
                raise error
            reason = self._retry.reason(error)
            outcome.retries += 1
            self._metrics.retry(reason)
            self._rate_log.log(
                f"retry:{reason}",
                logging.WARNING,
                "write to %r failed (%s), retrying %s (attempt %d of %s)",
                batch.database,
                error,
                f"in {delay:.2f} s" if delay else "now on a new connection",
                attempt,
                self._max_attempts or "unlimited",
                extra={"influx": {"client": self.name, "database": batch.database, "reason": reason}},
            )
            if self._stop.wait(delay):
                error.add_note("gave up retrying: the client was closed")
                raise error

    def _learn_size_limit(self, rejected_bytes: int) -> None:
        """After an HTTP 413, cap future batches below the rejected size.

        Proxies and load balancers often limit request bodies (nginx defaults to 1 MiB). Without
        this, every oversized batch would be uploaded in full just to be rejected again.
        """
        limit = max(1024, rejected_bytes // 2)
        with self._lock:
            if limit >= self._max_batch_bytes:
                return
            self._max_batch_bytes = limit
        self._metrics.batch_limit(limit)
        log.warning(
            "the server (or a proxy in front of it) rejected a %d-byte request as too large; "
            "limiting batches to %d bytes from now on (set write.max_batch_bytes to avoid the probing)",
            rejected_bytes,
            limit,
        )

    def _finish(self, batch: _Batch, outcome: _Outcome | None) -> None:
        """Report a processed batch, resolve its futures, then release it.

        Futures and ``on_error`` handlers run *before* the batch stops counting as
        outstanding, so a ``flush()`` that returns has seen all of their side effects.
        """
        assert outcome is not None
        with self._lock:
            if batch.finished:  # close() gave up on this request and reported it already
                return
            batch.finished = True
        unreported: _Unreported | None = None
        total = len(batch.lines)
        failed_lines = min(total, sum(failure.failed for failure in outcome.failures))
        written = total - failed_lines
        elapsed = time.monotonic() - batch.created
        raw_bytes = batch.nbytes
        self._stages.record("batch", elapsed)
        if batch.serialize_time:
            self._stages.record("serialize", batch.serialize_time)
        try:
            if not outcome.failures:
                self._metrics.batch_succeeded(batch.database, total, raw_bytes, outcome.bytes_sent, elapsed)
                for future, _start, _count, _offset in batch.waiters:
                    future._batch_done(None)
                if self._slow_batch is not None and elapsed > self._slow_batch:
                    self._rate_log.log(
                        "slow_batch",
                        logging.WARNING,
                        "slow write batch: %d points to %r took %.2f s from buffering to acknowledgement",
                        total,
                        batch.database,
                        elapsed,
                    )
                else:
                    log.debug("wrote %d points to %r in %.1f ms", total, batch.database, elapsed * 1000)
            else:
                first = outcome.failures[0].error
                self._metrics.batch_failed(
                    batch.database, failed_lines, "partial" if written else "failed", first
                )
                if written:
                    self._metrics.points_partially_rejected(batch.database, written, 0)
                handled = self._report(batch, outcome, failed_lines)
                self._learn_types(batch, outcome.failures)
                affected = []
                for future, start, count, offset in batch.waiters:
                    error = self._future_error(batch.lines, outcome.failures, start, count, offset)
                    if error is not None:
                        affected.append(future)
                    future._batch_done(error)
                if not handled:
                    unreported = _Unreported(first, failed_lines, tuple(affected))
        finally:
            with self._lock:
                self._sending.discard(batch)
                self._pending_bytes -= batch.nbytes
                self._outstanding.discard(batch.seq)
                self._points_written += written
                self._points_failed += failed_lines
                self._retries += outcome.retries
                self._bytes_raw += raw_bytes
                self._bytes_sent += outcome.bytes_sent
                if outcome.failures:
                    self._batches_failed += 1
                    if unreported is not None:
                        if len(self._unreported) < _MAX_RECORDED_FAILURES:
                            self._unreported.append(unreported)
                        else:
                            self._overflow_points += unreported.points
                            self._overflow_batches += 1
                else:
                    self._batches_written += 1
                pending = self._pending_bytes
                inflight = len(self._sending)
                self._has_space.notify_all()
                self._progress.notify_all()
            self._metrics.buffer(pending)
            self._metrics.inflight(inflight)

    @staticmethod
    def _future_error(
        lines: list[str], failures: list[_Failure], start: int, count: int, offset: int
    ) -> BaseException | None:
        """The error a future sees for its lines ``start:start+count`` of a batch."""
        end = start + count
        relevant = [f for f in failures if f.lo < end and f.hi > start]
        if not relevant:
            return None
        whole = [f.error for f in relevant if f.line is None]
        if whole:
            return whole[0]
        # Only individual lines were rejected: report them numbered within this write() call.
        numbers = [offset + (f.lo - start) + 1 for f in relevant]
        return _partial_error(relevant, numbers, lines, f"of {count} points ", relevant[0].error)

    def _report(self, batch: _Batch, outcome: _Outcome, failed: int) -> bool:
        """Log a failed batch and pass it to ``on_error``; True if the handler took it."""
        first = outcome.failures[0].error
        context = {
            "client": self.name,
            "database": batch.database,
            "points": failed,
            "error": type(first).__name__,
            "status": getattr(first, "status", None),
        }
        partial = all(f.line is not None or isinstance(f.error, PartialWriteError) for f in outcome.failures)
        if partial:
            samples = [batch.lines[f.lo] for f in outcome.failures if f.line is not None]
            sample_text = ""
            if samples and self._log_rejected:
                sample_text = "; rejected: " + " | ".join(s[:200] for s in samples[: self._log_rejected])
            self._rate_log.log(
                f"partial:{batch.database}",
                logging.WARNING,
                "server rejected %d of %d points written to %r: %s%s",
                failed,
                len(batch.lines),
                batch.database,
                first,
                sample_text,
                extra={"influx": context},
            )
        else:
            self._rate_log.log(
                f"failed:{type(first).__name__}",
                logging.ERROR,
                "failed to write %d points to %r: %s",
                failed,
                batch.database,
                first,
                extra={"influx": context},
            )
        if self._on_error is None:
            return False
        retryable = isinstance(first, ClientClosedError) or self._retry.is_retryable(first)
        try:
            self._on_error(_batch_failure(batch, outcome.failures, retryable))
        except Exception:
            log.exception("on_error handler raised: the failure is reported by flush()/close() instead")
            return False
        return True

    def _learn_types(self, batch: _Batch, failures: list[_Failure]) -> None:
        """Tell the client which type the server stores for fields it reported conflicts on."""
        if self._on_type_conflict is None:
            return
        learned: set[tuple[str, str, str]] = set()
        for failure in failures:
            message = failure.line.message if failure.line is not None else str(failure.error)
            match = _V2_TYPE_CONFLICT.search(message)
            if match is not None:
                measurement = match["measurement"]
            else:
                match = _V3_TYPE_CONFLICT.search(message)
                if match is None or failure.hi - failure.lo != 1:
                    continue
                try:
                    measurement = parse_line(self._dialect, batch.lines[failure.lo]).measurement
                except LineSyntaxError:
                    continue
            key = (measurement, match["field"], match["type"])
            if key not in learned:
                learned.add(key)
                self._on_type_conflict(batch.database, *key)

    # ------------------------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------------------------

    def close(self, timeout: float | None = None) -> WriteError | None:
        """Flush buffered data (up to ``timeout``), stop the threads and return unreported failures."""
        self.check_wait_allowed("close()")
        timeout = self.close_timeout if timeout is None else timeout
        with self._lock:
            if self._closed:
                return None
            self._begin_close(timeout)
            deadline = self._deadline or time.monotonic()
            while (self._queue or self._sending) and self._threads:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._progress.wait(remaining)
            self._seal_all_open()  # nothing can be opened after _closing; this is a safety net
            abandoned = list(self._queue)
            self._queue.clear()
            # Requests still in flight are given up on as well (their senders' late results are
            # ignored), so every buffered point is accounted for when close() returns.
            stuck = [batch for batch in self._sending if not batch.finished]
            # Abandoned batches stay outstanding until _finish has resolved their futures, so a
            # concurrent flush() cannot return before that.
            self._sending.update(abandoned)
            self._closed = True
            self._has_work.notify_all()
            self._progress.notify_all()
        self._stop.set()  # abort retry sleeps of batches still in flight
        lost = sum(len(batch.lines) for batch in abandoned)
        if abandoned:
            error = ClientClosedError(f"client closed before {lost} buffered points could be sent")
            for batch in abandoned:
                self._finish(batch, _Outcome(failures=[_Failure(0, len(batch.lines), error)]))
            log.error(
                "closed with %d points still buffered after waiting %.1f s; they were not written",
                lost,
                timeout,
                extra={"influx": {"client": self.name, "points": lost}},
            )
        if stuck:
            unknown = sum(len(batch.lines) for batch in stuck)
            error = ClientClosedError(
                f"close() stopped waiting after {timeout} s for a write request still in flight; "
                "the server may or may not store its points"
            )
            for batch in stuck:
                self._finish(batch, _Outcome(failures=[_Failure(0, len(batch.lines), error)]))
            log.error(
                "close() timed out with %d write requests (%d points) still in flight; "
                "they are reported as failed but the server may still store them",
                len(stuck),
                unknown,
                extra={"influx": {"client": self.name, "points": unknown}},
            )
        # One deadline for all threads (idle ones exit at once; stuck requests are abandoned).
        join_until = max(deadline, time.monotonic() + 0.5)
        for thread in self._threads:
            thread.join(timeout=max(0.0, join_until - time.monotonic()))
        self._transport.close()
        with self._lock:
            return self._take_failures()

    def _begin_close(self, timeout: float) -> None:
        """Stop accepting writes and send everything buffered (lock held)."""
        if self._closing:
            return
        self._closing = True
        self._deadline = time.monotonic() + timeout
        self._seal_all_open()
        self._has_work.notify_all()
        self._flusher_wake.notify_all()
        self._has_space.notify_all()

    @property
    def closed(self) -> bool:
        return self._closing

    def stats(self) -> EngineStats:
        with self._lock:
            return EngineStats(
                points_written=self._points_written,
                points_failed=self._points_failed,
                points_dropped=self._points_dropped,
                batches_written=self._batches_written,
                batches_failed=self._batches_failed,
                retries=self._retries,
                bytes_raw=self._bytes_raw,
                bytes_sent=self._bytes_sent,
                buffered_bytes=self._pending_bytes,
                open_batches=len(self._open),
                queued_batches=len(self._queue),
                inflight_batches=len(self._sending),
            )

    def _after_fork(self) -> None:
        """In a forked child: forget the parent's buffers, threads and connections."""
        if self._pid == os.getpid():
            return
        inherited = [*self._open.values(), *self._queue, *self._sending]
        self._init_state()
        self._transport.reset_after_fork()
        error = ClientClosedError("buffered in the parent process before fork(); the parent sends it")
        for batch in inherited:
            for future, _start, _count, _offset in batch.waiters:
                future._batch_done(error)


# Locks held across fork(): the engines' (in a fixed order), then the futures lock, which
# engine code takes while holding an engine lock.
_held_across_fork: list[threading.Lock] = []


def _before_fork() -> None:
    _held_across_fork[:] = [engine._lock for engine in list(_ENGINES)]
    _held_across_fork.append(_futures._LOCK)
    for lock in _held_across_fork:
        lock.acquire()


def _after_fork_in_parent() -> None:
    for lock in reversed(_held_across_fork):
        lock.release()
    _held_across_fork.clear()


def _release_in_child() -> None:
    _futures._LOCK.release()  # the engine locks are replaced by _after_fork
    _held_across_fork.clear()


def _reset_engines_in_child() -> None:
    _SENDER.engine = None  # fork() may have been called from a sender thread's callback
    for engine in list(_ENGINES):
        engine._after_fork()


def _register_exit_flush() -> None:
    """(Re-)register the exit flush so it runs before urllib3 tears connection pools down.

    atexit runs the most recently registered handler first. urllib3 closes pooled connections
    from a ``weakref.finalize`` hook, which is registered with atexit when the first finalizer
    is created - at the latest when the client's connection pool was. Registering again after
    that keeps the flush ahead of it; otherwise senders wait forever for a drained pool.
    """
    atexit.unregister(_close_all_at_exit)
    atexit.register(_close_all_at_exit)


def _close_all_at_exit() -> None:
    engines = [engine for engine in list(_ENGINES) if engine.flush_on_exit and not engine.closed]
    for engine in engines:  # start all flushes first, so exit waits for the slowest, not the sum
        with engine._lock:
            engine._begin_close(engine.close_timeout)
    for engine in engines:
        try:
            failure = engine.close()
            if failure is not None:
                log.error("at exit: %s", failure)
                if not _has_handlers(log):
                    # Data was lost and nothing would say so: the library only installs a NullHandler.
                    print(f"influxkit: at exit: {failure}", file=sys.stderr)
        except Exception:
            log.exception("failed to flush influxkit write buffer at exit")


def _has_handlers(logger: logging.Logger) -> bool:
    """Whether records of ``logger`` reach a real handler (not just NullHandler)."""
    current: logging.Logger | None = logger
    while current is not None:
        if any(not isinstance(handler, logging.NullHandler) for handler in current.handlers):
            return True
        current = current.parent if current.propagate else None
    return False


# POSIX only: fork() does not exist elsewhere, so no check is needed on the write path.
if hasattr(os, "register_at_fork"):
    os.register_at_fork(before=_before_fork, after_in_parent=_after_fork_in_parent)
    _fork.on_child(locks=_release_in_child, state=_reset_engines_in_child)
