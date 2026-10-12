"""The asynchronous write engine.

``write()`` serializes on the caller's thread and hands lines to the engine, which:

* appends them to an open batch per (database, precision) in the caller's lane, sealing it
  when it reaches ``batch_size`` lines or ``max_batch_bytes``, or (via the flusher thread)
  once it is ``flush_interval`` old;
* lets ``concurrency`` sender threads gzip and POST sealed batches over a keep-alive
  connection pool, retrying transient failures with backoff, splitting batches the
  server rejects as too large (HTTP 413) and resending the rest of a batch without the
  lines the server refused it for (InfluxDB 2, and InfluxDB 3 on /api/v2/write); a sender
  takes queued batches of the same database and precision along in one request while the
  size limits allow;
* bounds memory with ``max_pending_bytes`` (block / drop / raise when full);
* resolves each ``write()`` call's :class:`~sluicebox.futures.WriteFuture`, attributing
  per-line partial-write errors to the exact call (and line) that produced them.

Lanes: lines wait for their batch to be sealed in a lane, which holds open batches, a lock
and some reserved buffer space. On free-threaded Python every thread that writes has a lane
of its own, so threads writing at once share no lock, list or counter until a batch is
sealed (see ``_threads``); with the GIL and with ``concurrency = 1`` (whose requests must
leave in the order the lines were written) all threads share one lane. A lane's lock also
guards the futures of the writes buffered in it. Locks are taken in one order: a lane's,
then the engine's (``_lock``), and never a lane's while holding the engine's.

Threads are daemons, started on first use, re-created after ``fork()``, and the buffer is
flushed at interpreter exit (``write.flush_on_exit``).

Fork safety: a child process starts with only the thread that called ``fork()``. The engine
locks are held across ``fork()``; batches enter and leave lanes and the queue under them, so
the child inherits consistent state. It gives every lane a fresh lock (another thread may
have held one), forgets the parent's buffers, threads and connections, and fails the futures
of data the parent is sending.
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

from . import _fork, _threads
from . import futures as _futures
from ._http import Transport, error_from_response, is_html, unexpected_page_error
from ._lineprotocol import Dialect, LineSyntaxError, ParsedLine, UInt, parse_line
from ._retry import RetryPolicy
from .exceptions import (
    BadRequestError,
    BufferFullError,
    ClientClosedError,
    InfluxConnectionError,
    InfluxTimeoutError,
    LineError,
    NotFoundError,
    PartialWriteError,
    PayloadTooLargeError,
    ServerError,
    SluiceboxError,
    TransportError,
    WriteError,
)
from .log import RateLimitedLog

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from .config import InfluxSettings
    from .futures import WriteFuture
    from .metrics import Metrics
    from .profiling import StageRecorder

__all__ = ["EngineStats", "WriteEngine", "WriteFailure"]

log = logging.getLogger("sluicebox.write")

_V3_PRECISION = {"ns": "nanosecond", "us": "microsecond", "ms": "millisecond", "s": "second"}
_MAX_RECORDED_FAILURES = 100
#: A 413 for a request this small is not about its size (e.g. a proxy refusing all bodies).
_MIN_SPLIT_BYTES = 1024
#: Buffer space a lane reserves at a time: its thread then buffers that much without the
#: engine lock (at most 1/256 of ``max_pending_bytes``).
_CREDIT = 64 * 1024
#: Futures released per hold of their lane's lock, so a sender never holds up a writing
#: thread for long.
_RELEASE_CHUNK = 512
#: A lane that opened no batch for this long is unregistered (its thread may have ended);
#: it registers again when written to.
_RETIRE_AFTER = 30.0
#: Extra requests a batch may send to single out the lines a server refuses it for (_isolate).
_ISOLATION_REQUESTS = 64
#: How InfluxDB 2 quotes each line it cannot parse in its HTTP 400 answer.
_UNPARSEABLE = "unable to parse '"
#: How InfluxDB 3's /api/v2/write refuses a request for a line it cannot parse or store (it
#: names the reason, but not the line).
_V3_REFUSED = "write buffer error: line protocol parse failed: "
#: A column given another type than InfluxDB 3 has for it (stored, or from an earlier line).
_V3_COLUMN_CONFLICT = re.compile(
    r"invalid column type for column '(?P<column>[^']+)', "
    r"expected iox::column_type::(?P<expected>[\w:]+), got iox::column_type::(?P<got>[\w:]+)"
)
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
    #: refused them (invalid data, authentication, missing database): fix that first. If
    #: some lines failed each way, True: replay them, and the refused ones fail again.
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
    #: Bytes buffered or in flight, plus buffer space that threads writing at that moment
    #: have reserved and not used yet (at most 64 KiB each; none once their batches are sealed).
    buffered_bytes: int
    open_batches: int
    queued_batches: int
    inflight_batches: int


class _Batch:
    __slots__ = (
        "counts",
        "created",
        "database",
        "finished",
        "futures",
        "lane",
        "lines",
        "max_bytes",
        "nbytes",
        "number",
        "offsets",
        "precision",
        "runs",
        "sealed",
        "seqs",
        "serialize_time",
    )

    def __init__(
        self, lane: _Lane, database: str, precision: str, number: int, created: float, max_bytes: int
    ) -> None:
        self.lane = lane
        self.database = database
        self.precision = precision
        #: Tells this batch from later ones of the same lane, database and precision.
        self.number = number
        self.lines: list[str] = []
        self.nbytes = 0
        #: ``max_batch_bytes`` when the batch was opened (a 413 may lower it later).
        self.max_bytes = max_bytes
        #: Caller-thread time spent serializing the lines of this batch (recorded once per batch).
        self.serialize_time = 0.0
        #: The futures of the write() calls with lines here, in line order: each call's lines
        #: follow the previous call's. ``counts`` holds how many lines each has in this batch,
        #: ``offsets`` how many of its lines earlier batches carry. (Three lists rather than a
        #: tuple per call: no object per buffered write for the garbage collector to scan.)
        self.futures: list[WriteFuture] = []
        self.counts: list[int] = []
        self.offsets: list[int] = []
        #: Batches merged from several lanes: (lane, number of futures) for each part, in
        #: order. The lane's lock guards those futures. None: all are ``lane``'s.
        self.runs: list[tuple[_Lane, int]] | None = None
        self.created = created
        #: When the batch was queued for sending (0.0 while open).
        self.sealed = 0.0
        #: Sequence numbers in ``_outstanding``: one per sealed batch merged into this one.
        self.seqs: list[int] = []
        #: Set (with the engine lock) by whoever reports the outcome: the sender, or close()
        #: when it gives up on a request that is still in flight.
        self.finished = False

    def lane_runs(self) -> list[tuple[_Lane, int]]:
        return self.runs if self.runs is not None else [(self.lane, len(self.futures))]


def _fill(batch: _Batch, lines: list[str], nbytes: int, future: WriteFuture, seal: bool = False) -> None:
    """Add a write's lines to an open batch (lock of the batch's lane held)."""
    batch.futures.append(future)
    batch.counts.append(len(lines))
    batch.offsets.append(future.points)
    future._attach(seal)
    future.points += len(lines)
    batch.lines += lines
    batch.nbytes += nbytes


class _Lane:
    """Where writes wait for their batch to be sealed: open batches and reserved buffer space.

    One per thread on free-threaded Python, otherwise one for all threads (see the module
    docstring). ``lock`` guards ``open``, ``credit`` and ``usable``, and the state of the
    futures of the writes buffered here. Batches enter and leave ``open`` with the engine lock
    held as well, so the flusher can see which are open and fork() leaves no batch halfway.
    """

    __slots__ = ("__weakref__", "batch_size", "credit", "engine", "lock", "open", "opened", "seen", "usable")

    def __init__(self, engine: WriteEngine) -> None:
        self.engine = engine
        self.lock = threading.Lock()
        self.open: dict[tuple[str, str], _Batch] = {}
        #: Buffer space reserved from ``max_pending_bytes`` and not used yet. Held only while
        #: the lane has open batches: sealing the last one returns it.
        self.credit = 0
        #: False when submit() must take the slow path first: the lane is not registered with
        #: the engine (new, idle for a while, inherited across fork()) or the engine is closing.
        self.usable = False
        self.batch_size = _threads.private(engine._batch_size)
        #: Batches opened (with the engine lock), and that count when the flusher last looked.
        self.opened = 0
        self.seen = 0

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
        seal: bool = False,
    ) -> bool:
        """Buffer ``lines`` for ``future``, whose lane this must be: see :meth:`WriteEngine.submit`.

        The common case - room in the open batch and in the reserved space - takes only this
        lane's lock.
        """
        if not lines:
            if seal:
                future._seal()
            return True
        with self.lock:
            if self.usable and nbytes <= self.credit:
                key = (database, precision)
                batch = self.open.get(key)
                if (
                    batch is not None
                    and len(batch.lines) + len(lines) <= self.batch_size
                    and batch.nbytes + nbytes <= batch.max_bytes
                ):
                    self.credit -= nbytes
                    _fill(batch, lines, nbytes, future, seal)
                    batch.serialize_time += serialize_time
                    if len(batch.lines) >= self.batch_size or batch.nbytes >= batch.max_bytes:
                        self._seal(key, batch)
                    return True
        return self.engine._submit(
            self, database, precision, lines, nbytes, future, block, serialize_time, seal
        )

    def _append(
        self,
        key: tuple[str, str],
        lines: list[str],
        nbytes: int,
        future: WriteFuture,
        seal: bool,
        serialize_time: float,
    ) -> None:
        """Add lines whose space is already reserved, opening and sealing batches (lock held)."""
        batch = self.open.get(key)
        # Lines too many or too large for one batch get batches of their own. A 413 may have
        # lowered the limit since the open batch was opened, so lines it would still take can
        # be split too: it is sealed first, as opening another batch would replace it.
        split = len(lines) > self.batch_size or nbytes > self.engine._max_batch_bytes
        if batch is not None and (
            split
            or len(batch.lines) + len(lines) > self.batch_size
            or batch.nbytes + nbytes > batch.max_bytes
        ):
            self._seal(key, batch, keep_credit=True)
            batch = None
        if split:
            self._append_split(key, lines, nbytes, future, seal)
            return
        if batch is None:
            batch = self.engine._open_batch(self, key)
        _fill(batch, lines, nbytes, future, seal)
        batch.serialize_time += serialize_time
        if len(batch.lines) >= self.batch_size or batch.nbytes >= batch.max_bytes:
            self._seal(key, batch)

    def _append_split(
        self, key: tuple[str, str], lines: list[str], nbytes: int, future: WriteFuture, seal: bool
    ) -> None:
        """Cut an oversized submission into batches that respect both size limits (lock held)."""
        engine = self.engine
        limit = engine._max_batch_bytes
        piece: list[str] = []
        piece_bytes = 0
        total = 0
        for line in lines:
            size = (len(line) if line.isascii() else len(line.encode("utf-8"))) + 1
            if piece and (len(piece) >= self.batch_size or piece_bytes + size > limit):
                batch = engine._open_batch(self, key)
                _fill(batch, piece, piece_bytes, future)
                self._seal(key, batch, keep_credit=True)
                piece = []
                piece_bytes = 0
            piece.append(line)
            piece_bytes += size
            total += size
        if total != nbytes:
            # The batches hold ``total`` bytes, which _finish releases: account for exactly that.
            engine._adjust_pending(total - nbytes)
        if piece:
            batch = engine._open_batch(self, key)
            _fill(batch, piece, piece_bytes, future, seal)
            if len(batch.lines) >= self.batch_size or batch.nbytes >= batch.max_bytes:
                self._seal(key, batch)

    def _seal(self, key: tuple[str, str], batch: _Batch, keep_credit: bool = False) -> None:
        """Queue an open batch for sending (lock held).

        Sealing the last open batch returns the unused reserved space, unless ``keep_credit``
        (another batch is opened right away).
        """
        returned = 0
        if not keep_credit and len(self.open) == 1:
            returned = self.credit
            self.credit = 0
        self.engine._enqueue(self, key, batch, returned)

    def flush_nowait(self) -> None:
        """Queue this lane's open batches for sending now."""
        with self.lock:
            for key, batch in list(self.open.items()):
                self._seal(key, batch)
            if self.credit:  # not expected (only open batches hold space), but never leak it
                self.engine._adjust_pending(-self.credit)
                self.credit = 0


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
    #: Requests the batch may still send to single out lines the server cannot parse.
    isolation_budget: int = _ISOLATION_REQUESTS


def _fail(outcome: _Outcome, indices: Sequence[int], error: BaseException, failed: int | None = None) -> None:
    """Record that the batch lines ``indices`` were not written (only ``failed`` of them, if given)."""
    left = len(indices) if failed is None else failed
    for lo, hi in _runs(indices):
        count = min(left, hi - lo)
        outcome.failures.append(_Failure(lo, hi, error, failed=count))
        left -= count


def _runs(indices: Sequence[int]) -> list[tuple[int, int]]:
    """Ascending ``indices`` as ``(lo, hi)`` ranges of consecutive ones."""
    if not indices:
        return []
    if isinstance(indices, range):
        return [(indices[0], indices[-1] + 1)]
    runs = []
    lo = previous = indices[0]
    for index in indices[1:]:
        if index != previous + 1:
            runs.append((lo, previous + 1))
            lo = index
        previous = index
    runs.append((lo, previous + 1))
    return runs


def _part(items: Sequence[Any], positions: Sequence[int]) -> Sequence[Any]:
    """The items at ``positions`` (a range of them is sliced)."""
    if isinstance(positions, range):
        return items[positions.start : positions.stop]
    return [items[position] for position in positions]


def _halves(count: int) -> list[Sequence[int]]:
    return [range(count // 2), range(count // 2, count)]


def _iox_type(line: ParsedLine, column: str) -> str | None:
    """The type ``line`` gives ``column`` as InfluxDB 3 names it in errors (``tag``,
    ``field::integer``, ...), or None. A tag and a field of the same name read as the tag."""
    if column in line.tags:
        return "tag"
    if column not in line.fields:
        return None
    value = line.fields[column]
    if isinstance(value, bool):
        return "field::boolean"
    if isinstance(value, UInt):
        return "field::uinteger"
    if isinstance(value, int):
        return "field::integer"
    if isinstance(value, float):
        return "field::float"
    return "field::string"


def _unparseable_lines(message: str, lines: Sequence[str]) -> tuple[dict[int, str], bool]:
    """The lines of a request InfluxDB 2 cannot parse, ``{position in lines: reason}``, and
    whether they account for every line its error quotes.

    The message holds ``unable to parse '<line>': <reason>`` for each such line, one after the
    other on new lines, the line without its leading whitespace. A line can contain newlines
    (in a string field value) and ``': ``, so the reason follows the last ``': ``. No line runs
    on into the next (write() rejects those), so each quote is one line of the request.
    """
    quotes: dict[str, str] = {}
    complete = True
    for entry in message[len(_UNPARSEABLE) :].split("\n" + _UNPARSEABLE):
        text, separator, reason = entry.rpartition("': ")
        if separator:
            quotes[text] = reason
        else:
            complete = False
    found: dict[int, str] = {}
    seen: set[str] = set()
    for position, line in enumerate(lines):
        key = line.lstrip(" \t\0")
        if key in quotes:
            found[position] = quotes[key]
            seen.add(key)
    return found, complete and len(seen) == len(quotes)


def _unparseable_error(
    source: BadRequestError, line_errors: tuple[LineError, ...], verb: str = "unable to parse"
) -> PartialWriteError:
    """The lines of a request the server refused it for, which fail alone: a partial write."""
    details = "; ".join(f"line {e.line_number}: {e.message}" for e in line_errors[:3])
    if len(line_errors) > 3:
        details += f"; ... {len(line_errors) - 3} more"
    return PartialWriteError(
        f"{verb} {len(line_errors)} line{'s' if len(line_errors) > 1 else ''}: {details}",
        status=source.status,
        code=source.code,
        body=source.body,
        request_id=source.request_id,
        line_errors=line_errors,
    )


def _refused_error(source: BadRequestError, line_errors: tuple[LineError, ...]) -> PartialWriteError:
    return _unparseable_error(source, line_errors, "the server refused")


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
    error: BaseException
    if all(failure.line is not None and failure.hi - failure.lo == 1 for failure in failures):
        # Rejected lines: number them within ``lines`` (they are consecutive there).
        scope = "lines " if len(failures) > 1 else "line "
        error = _partial_error(failures, range(1, len(failures) + 1), batch.lines, scope, failures[0].error)
    elif len(failures) == 1:
        error = failures[0].error
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
        self._credit = min(_CREDIT, self._max_pending // 256)
        # Lanes live as long as their thread (or this engine), across fork() too.
        # concurrency = 1 promises requests in the order the lines were written: one lane.
        self._per_thread = _threads.per_thread() and self._concurrency > 1
        self._local = threading.local()
        #: Every lane alive (weakly: a thread's lane goes when the thread does), registered or
        #: not, so that a forked child can replace their locks.
        self._all_lanes: dict[int, weakref.ref[_Lane]] = {}
        self._shared_lane = None if self._per_thread else self._new_lane()
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
        #: Registered lanes: the ones that may hold open batches or reserved space.
        self._lanes: set[_Lane] = set()
        #: Open batches by age, as (lane, key, batch number), for the flusher. Sealed ones are
        #: skipped (and dropped) when they come up.
        self._expiring: deque[tuple[_Lane, tuple[str, str], int, float]] = deque()
        self._open_count = 0
        self._batch_number = 0
        self._queue: deque[_Batch] = deque()
        #: Queued batches per (database, precision), so a sender only looks for batches to
        #: take along when there are any.
        self._queued: dict[tuple[str, str], int] = {}
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

    def lane(self) -> _Lane:
        """The lane the calling thread writes through (see the module docstring)."""
        shared = self._shared_lane
        if shared is not None:
            return shared
        local = self._local
        try:
            lane: _Lane = local.lane
        except AttributeError:
            lane = local.lane = self._new_lane()
        return lane

    def _new_lane(self) -> _Lane:
        lane = _Lane(self)  # created (and so owned) by the thread that is going to write through it
        key = id(lane)
        lanes = self._all_lanes
        lanes[key] = weakref.ref(lane, lambda _, key=key: lanes.pop(key, None))  # type: ignore[misc]
        return lane

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
        seal: bool = False,
    ) -> bool:
        """Buffer ``lines`` for ``future``. Returns False if they were dropped (buffer full).

        The lines go to the future's lane. With ``block=False`` a full buffer under the
        ``block`` policy raises :class:`WouldBlock` (nothing is buffered) so asyncio callers can
        wait elsewhere via :meth:`wait_for_space`. ``seal=True`` marks these as the last lines
        of the call, sealing the future with them (the caller seals it on a False return or an
        exception).
        """
        lane = future._lane
        assert lane is not None, "a completed future cannot take lines"
        assert lane.engine is self, "the future belongs to another client"
        return lane.submit(
            database, precision, lines, nbytes, future, block=block, serialize_time=serialize_time, seal=seal
        )

    def _submit(
        self,
        lane: _Lane,
        database: str,
        precision: str,
        lines: list[str],
        nbytes: int,
        future: WriteFuture,
        block: bool,
        serialize_time: float,
        seal: bool,
    ) -> bool:
        """The rest of :meth:`_Lane.submit`: register the lane, reserve space, open batches."""
        key = (database, precision)
        with lane.lock:
            if not lane.usable:
                self._register(lane)
            if nbytes <= lane.credit:
                lane.credit -= nbytes
                lane._append(key, lines, nbytes, future, seal, serialize_time)
                return True
        # Reserving may wait for space: never with the lane's lock held, since the senders need
        # it to release the futures of the batches whose completion frees the space.
        granted = self._reserve(nbytes, len(lines), block)
        if not granted:
            return False
        try:
            with lane.lock:
                if not lane.usable:
                    self._register(lane)  # raises if close() began meanwhile
                lane.credit += granted - nbytes
                granted = 0
                lane._append(key, lines, nbytes, future, seal, serialize_time)
        finally:
            if granted:
                self._adjust_pending(-granted)
        return True

    def _register(self, lane: _Lane) -> None:
        """Let the flusher, flush() and close() reach a lane about to buffer data (its lock held)."""
        with self._lock:
            if self._closing:
                raise ClientClosedError("the client is closed; no further writes are accepted")
            if not self._threads:
                self._start_threads()
            self._lanes.add(lane)
            lane.usable = True

    def _reserve(self, nbytes: int, points: int, block: bool) -> int:
        """Reserve buffer space for ``nbytes``, and some to spare if there is room.

        Applies the overflow policy when the buffer is full. Returns the bytes reserved, or 0
        if the points are to be dropped. No lane lock may be held.
        """
        with self._lock:
            if self._closing:
                raise ClientClosedError("the client is closed; no further writes are accepted")
            granted = self._grant(nbytes)
        if granted:
            return granted
        self._reclaim_credit()  # space that lanes reserved and have not used
        with self._lock:
            granted = self._grant(nbytes)
            if granted:
                return granted
            if self._on_full == "drop":
                self._count_full_drop(points)
                return 0
            if not block and self._on_full == "block":
                raise WouldBlock
            if self._on_full == "raise" or getattr(_SENDER, "engine", None) is self:
                # Never block a sender thread (e.g. a callback that writes): nothing could drain the buffer.
                raise BufferFullError(
                    f"write buffer is full ({self._pending_bytes} of {self._max_pending} bytes pending)"
                )
        return self._wait_for_space(nbytes, reserve=True)

    def _grant(self, nbytes: int) -> int:
        """Reserve ``nbytes`` and up to one credit more if they fit (engine lock held), else 0.

        A buffer holding nothing takes a write of any size: it could never fit otherwise.
        """
        pending = self._pending_bytes
        if pending + nbytes > self._max_pending and pending > 0:
            return 0
        granted = nbytes + max(0, min(self._credit, self._max_pending - pending - nbytes))
        self._pending_bytes = pending + granted
        return granted

    def _count_full_drop(self, points: int) -> None:
        """Count points dropped because the buffer is full (engine lock held)."""
        self._points_dropped += points
        self._dropped_unlogged += points
        self._metrics.points_dropped("buffer_full", points)
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

    def _wait_for_space(self, nbytes: int, *, reserve: bool) -> int:
        """Wait until ``nbytes`` fit in the buffer (no lock held), reserving them if ``reserve``.

        Returns the bytes reserved (0 without ``reserve``).
        """
        started = time.perf_counter()
        deadline = time.monotonic() + self._block_timeout if self._block_timeout else None
        while True:
            self._seal_lanes()  # make sure everything buffered is on its way
            with self._lock:
                # Checked after every wait: close() may have begun meanwhile, and nothing sends a
                # batch opened after it sealed the lanes.
                if self._closing:
                    raise ClientClosedError("the client was closed while waiting for buffer space")
                if reserve:
                    granted = self._grant(nbytes)
                    if granted:
                        break
                elif self._pending_bytes + nbytes <= self._max_pending or self._pending_bytes == 0:
                    granted = 0
                    break
                remaining = None
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise BufferFullError(
                            f"write buffer still full after {self._block_timeout} s "
                            f"({self._pending_bytes} of {self._max_pending} bytes pending)"
                        )
                self._has_space.wait(remaining)
        self._stages.record("backpressure", time.perf_counter() - started)
        return granted

    def count_dropped(self, points: int) -> None:
        """Count points dropped before they reached the engine (invalid records)."""
        with self._lock:
            self._points_dropped += points

    def wait_for_space(self, nbytes: int) -> None:
        """Block until ``nbytes`` more fit in the buffer (honouring ``write.block_timeout``)."""
        for attempt in range(2):
            with self._lock:
                if self._pending_bytes + nbytes <= self._max_pending or self._pending_bytes == 0:
                    return
            if attempt == 0:
                self._reclaim_credit()
        self._wait_for_space(nbytes, reserve=False)

    def _open_batch(self, lane: _Lane, key: tuple[str, str]) -> _Batch:
        """Open a batch in ``lane`` (its lock held)."""
        assert key not in lane.open, "the open batch would be lost"
        with self._lock:
            self._batch_number += 1
            batch = _Batch(
                lane,
                key[0],
                key[1],
                self._batch_number,
                time.monotonic(),
                _threads.private(self._max_batch_bytes),
            )
            lane.open[key] = batch
            lane.opened += 1
            self._open_count += 1
            self._expiring.append((lane, key, batch.number, batch.created))
            if len(self._expiring) == 1:
                self._flusher_wake.notify()
        return batch

    def _enqueue(self, lane: _Lane, key: tuple[str, str], batch: _Batch, returned: int) -> None:
        """Move a sealed batch from its lane to the send queue, with ``returned`` bytes of the
        lane's reserved space (lane lock held). Buffer gauges are updated here, per batch."""
        with self._lock:
            del lane.open[key]
            self._open_count -= 1
            if returned:
                self._pending_bytes -= returned
                self._has_space.notify_all()
            self._seq += 1
            batch.seqs.append(self._seq)
            batch.sealed = time.monotonic()
            self._outstanding.add(self._seq)
            self._queue.append(batch)
            self._queued[key] = self._queued.get(key, 0) + 1
            self._has_work.notify()
            self._metrics.buffer(self._pending_bytes)

    def _adjust_pending(self, delta: int) -> None:
        with self._lock:
            self._pending_bytes += delta
            if delta < 0:
                self._has_space.notify_all()

    def _registered(self) -> list[_Lane]:
        with self._lock:
            return list(self._lanes)

    def _seal_lanes(self) -> None:
        """Queue every open batch for sending (no lock held)."""
        for lane in self._registered():
            lane.flush_nowait()

    def _reclaim_credit(self) -> None:
        """Take back the buffer space lanes have reserved and not used (no lock held)."""
        for lane in self._registered():
            with lane.lock:
                if lane.credit:
                    self._adjust_pending(-lane.credit)
                    lane.credit = 0

    def flush_nowait(self) -> None:
        """Send all buffered data now without waiting for it."""
        self._seal_lanes()

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
        # Every write that returned before this call is in an open batch of a registered lane
        # or already queued: sealing the lanes gives all of them sequence numbers up to target.
        self._seal_lanes()
        with self._lock:
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
                target=self._sender_loop, name=f"sluicebox-{self.name}-sender-{index}", daemon=True
            )
            thread.start()
            self._threads.append(thread)
        flusher = threading.Thread(
            target=self._flusher_loop, name=f"sluicebox-{self.name}-flusher", daemon=True
        )
        flusher.start()
        self._threads.append(flusher)

    def _flusher_loop(self) -> None:
        interval = self._flush_interval
        next_retirement = time.monotonic() + _RETIRE_AFTER
        while True:
            with self._lock:
                due = self._wait_for_expiry(interval)
            if due is None:
                return
            for lane, key, number in due:
                with lane.lock:
                    batch = lane.open.get(key)
                    if batch is not None and batch.number == number:
                        lane._seal(key, batch)
            if time.monotonic() >= next_retirement:
                next_retirement = time.monotonic() + _RETIRE_AFTER
                self._retire_idle_lanes()

    def _wait_for_expiry(self, interval: float) -> list[tuple[_Lane, tuple[str, str], int]] | None:
        """Wait until open batches are ``interval`` old and return them (engine lock held).

        None means the engine is closing.
        """
        expiring = self._expiring
        while not self._closing:
            while expiring and not self._still_open(*expiring[0][:3]):
                expiring.popleft()  # sealed meanwhile (full, flushed or waited for)
            if not expiring:
                self._flusher_wake.wait()
                continue
            now = time.monotonic()
            wait = expiring[0][3] + interval - now
            if wait > 0:
                self._flusher_wake.wait(wait)
                continue
            due = []
            while expiring and expiring[0][3] + interval <= now:
                lane, key, number, _ = expiring.popleft()
                if self._still_open(lane, key, number):
                    due.append((lane, key, number))
            return due
        return None

    @staticmethod
    def _still_open(lane: _Lane, key: tuple[str, str], number: int) -> bool:
        """Whether batch ``number`` is still open in ``lane`` (engine lock held)."""
        batch = lane.open.get(key)
        return batch is not None and batch.number == number

    def _retire_idle_lanes(self) -> None:
        """Unregister lanes that have opened no batch since the last look (their thread may be
        gone): only registered lanes are visited by flush(), close() and waits for space."""
        for lane in self._registered():
            if lane is self._shared_lane:
                continue
            with lane.lock:
                if lane.open or lane.opened != lane.seen:
                    lane.seen = lane.opened
                    continue
                with self._lock:
                    self._lanes.discard(lane)
                    lane.usable = False
                    if lane.credit:  # not expected: only open batches hold space
                        self._pending_bytes -= lane.credit
                        lane.credit = 0
                        self._has_space.notify_all()

    def _sender_loop(self) -> None:
        _SENDER.engine = self
        while True:
            with self._lock:
                while not self._queue:
                    if self._closed:
                        return
                    self._has_work.wait()
                batch = self._take()
                self._sending.add(batch)
                inflight = len(self._sending)
            # Never let a sender thread die: with all of them gone, nothing would be sent again.
            try:
                self._metrics.inflight(inflight)
                outcome = self._process(batch)
            except BaseException as exc:
                log.exception("unexpected error while sending a batch")
                outcome = _Outcome(failures=[_Failure(0, len(batch.lines), exc)])
            try:
                self._finish(batch, outcome)
            except BaseException:
                log.exception("unexpected error while finishing a write batch")

    def _take(self) -> _Batch:
        """Dequeue the next batch to send (lock held, queue not empty).

        Later queued batches of the same database and precision join it, in order, while
        ``batch_size`` and ``max_batch_bytes`` allow. Small batches pile up in the queue when
        callers wait on their writes, since every wait seals the open batch so that an idle
        client answers at once; sent one per request, they would limit throughput to
        ``concurrency`` points per server round trip.
        """
        batch = self._queue.popleft()
        key = (batch.database, batch.precision)
        others = self._queued[key] - 1
        if others and len(batch.lines) < self._batch_size and batch.nbytes < self._max_batch_bytes:
            skipped: list[_Batch] = []
            while others:
                other = self._queue.popleft()
                if (other.database, other.precision) != key:
                    skipped.append(other)
                    continue
                if (
                    len(batch.lines) + len(other.lines) > self._batch_size
                    or batch.nbytes + other.nbytes > self._max_batch_bytes
                ):
                    skipped.append(other)  # later ones stay behind it, keeping the order of writes
                    break
                self._merge(batch, other)
                others -= 1
            self._queue.extendleft(reversed(skipped))
        if others:
            self._queued[key] = others
        else:
            del self._queued[key]
        return batch

    @staticmethod
    def _merge(batch: _Batch, other: _Batch) -> None:
        """Append the lines of ``other`` (and all its bookkeeping) to ``batch``."""
        runs = batch.lane_runs()
        for lane, count in other.lane_runs():
            if runs[-1][0] is lane:
                runs[-1] = (lane, runs[-1][1] + count)
            else:
                runs.append((lane, count))
        batch.runs = runs
        batch.futures += other.futures
        batch.counts += other.counts
        batch.offsets += other.offsets
        batch.lines += other.lines
        batch.nbytes += other.nbytes
        batch.serialize_time += other.serialize_time
        batch.created = min(batch.created, other.created)
        batch.seqs += other.seqs

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
        self._send(batch, batch.lines, range(len(batch.lines)), outcome)
        return outcome

    def _send(self, batch: _Batch, lines: Sequence[str], indices: Sequence[int], outcome: _Outcome) -> None:
        """Send ``lines`` (batch lines ``indices``), recording failures in ``outcome``.

        ``indices`` is a range, unless the lines are part of a request the server refused.
        """
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
                _fail(outcome, indices, exc)
                return
            self._learn_size_limit(len(payload))
            middle = len(lines) // 2
            log.debug("request of %d bytes too large; splitting %d lines in two", len(payload), len(lines))
            self._send(batch, lines[:middle], indices[:middle], outcome)
            self._send(batch, lines[middle:], indices[middle:], outcome)
        except PartialWriteError as exc:
            numbers = [err.line_number for err in exc.line_errors]
            if numbers and all(1 <= number <= len(lines) for number in numbers):
                reported: set[int] = set()
                for err in exc.line_errors:
                    if err.line_number not in reported:
                        reported.add(err.line_number)
                        index = indices[err.line_number - 1]
                        outcome.failures.append(_Failure(index, index + 1, exc, err))
            else:
                # Not every rejection names a line of this request: report the whole request as
                # failed rather than count unidentified rejected lines as written (replaying
                # stored lines is harmless, as every point carries its timestamp).
                rejected = max(len(exc.line_errors), exc.rejected or 0) or len(lines)
                _fail(outcome, indices, exc, min(rejected, len(lines)))
        except BadRequestError as exc:
            if self._api == "v2" and exc.message.startswith(_UNPARSEABLE):
                self._isolate(batch, lines, indices, exc, outcome)
            elif self._api == "v2" and exc.message.startswith(_V3_REFUSED):
                self._isolate_unnamed(batch, lines, indices, exc, outcome)
            else:
                _fail(outcome, indices, exc)
        except SluiceboxError as exc:
            _fail(outcome, indices, exc)

    def _isolate(
        self,
        batch: _Batch,
        lines: Sequence[str],
        indices: Sequence[int],
        error: BadRequestError,
        outcome: _Outcome,
    ) -> None:
        """Write the lines of a request InfluxDB 2 refused because it cannot parse some of them.

        InfluxDB 2 parses a whole request before it stores any of it: one line it cannot
        parse fails all the others, other write() calls' lines too, and a replay of the
        WriteFailure fails the same way. Its error quotes each such line, so those fail alone
        (as in a partial write) and the others are sent again. If it quotes something that is
        not one of the lines sent, the lines are halved instead, as for HTTP 413, until the
        halves are written or down to single lines (see _resend for what that may cost).
        As the server stored nothing of the request, resending never writes a line twice.
        """
        found, complete = _unparseable_lines(error.message, lines)
        if not found and len(lines) == 1:
            # Alone in its request, the line is the one the server cannot parse, however quoted.
            _, separator, reason = error.message.rpartition("': ")
            found = {0: reason if separator else error.message}
        if found:
            self._reject(lines, indices, found, _unparseable_error, error, outcome)
            keep = [p for p in range(len(lines)) if p not in found]
            if not keep:
                return
            lines = [lines[p] for p in keep]
            indices = [indices[p] for p in keep]
        # The rest goes in one request when the server named every line it cannot parse (or
        # only one is left), else in halves. A sub-request's error names its own bad lines.
        parts: list[Sequence[int]]
        if len(lines) == 1 or (found and complete):
            log.debug("the server cannot parse %d lines; sending the other %d again", len(found), len(lines))
            parts = [range(len(lines))]
        else:
            log.debug("the server cannot parse some of %d lines; sending them in two halves", len(lines))
            parts = _halves(len(lines))
        self._resend(batch, lines, indices, parts, error, outcome)

    def _isolate_unnamed(
        self,
        batch: _Batch,
        lines: Sequence[str],
        indices: Sequence[int],
        error: BadRequestError,
        outcome: _Outcome,
    ) -> None:
        """Write the lines of a request InfluxDB 3 refused on /api/v2/write for one line.

        It stores nothing of a request with a line it cannot parse, or that gives a column
        another type than the table has (stored, or from an earlier line of the request), and
        names the reason but not the line. Halving would find each such line in about
        2 * log2(lines) requests, and every request the server stores takes it up to a second
        (it acknowledges on its WAL flush), refused ones milliseconds. So the lines the reason
        can be about are sent apart from the others, which go in one request: the lines the
        client cannot parse either, or those giving the column the type the server refused,
        a request per table. All lines of a request giving one table's field the refused type
        of another field type fail together: the server's type came from none of them. Only
        when the client cannot tell which lines the reason is about are the lines halved. Each
        request's error is handled the same way.
        """
        reason = error.message[len(_V3_REFUSED) :]
        if len(lines) == 1:
            self._reject(lines, indices, {0: reason}, _refused_error, error, outcome)
            return
        conflict = _V3_COLUMN_CONFLICT.search(reason)
        others: list[int] = []
        suspects: dict[str, list[int]] = {}  # by table ("" for lines the client cannot parse)
        for position, line in enumerate(lines):
            try:
                parsed = parse_line(self._dialect, line.lstrip(" \t"))
            except LineSyntaxError:
                if conflict is None:
                    suspects.setdefault("", []).append(position)
                else:
                    others.append(position)
                continue
            if conflict is not None and _iox_type(parsed, conflict["column"]) == conflict["got"]:
                suspects.setdefault(parsed.measurement, []).append(position)
            else:
                others.append(position)
        if (
            conflict is not None
            and not others
            and len(suspects) == 1
            and conflict["expected"].startswith("field::")
            and conflict["got"].startswith("field::")
        ):
            self._reject(
                lines, indices, dict.fromkeys(range(len(lines)), reason), _refused_error, error, outcome
            )
            return
        parts: list[Sequence[int]] = [part for part in (others, *suspects.values()) if part]
        if len(parts) > 1:
            log.debug(
                "the server refused %d lines (%s); sending them in %d parts", len(lines), reason, len(parts)
            )
        else:
            log.debug("the server refused %d lines (%s); sending them in two halves", len(lines), reason)
            parts = _halves(len(lines))
        self._resend(batch, lines, indices, parts, error, outcome)

    @staticmethod
    def _reject(
        lines: Sequence[str],
        indices: Sequence[int],
        found: dict[int, str],
        make_error: Callable[[BadRequestError, tuple[LineError, ...]], PartialWriteError],
        error: BadRequestError,
        outcome: _Outcome,
    ) -> None:
        """Record that the server refused ``lines`` at the positions of ``found`` (with its reasons)."""
        positions = sorted(found)
        line_errors = tuple(LineError(p + 1, found[p], lines[p]) for p in positions)
        rejected = make_error(error, line_errors)
        for position, line_error in zip(positions, line_errors, strict=True):
            index = indices[position]
            outcome.failures.append(_Failure(index, index + 1, rejected, line_error))

    def _resend(
        self,
        batch: _Batch,
        lines: Sequence[str],
        indices: Sequence[int],
        parts: list[Sequence[int]],
        error: BadRequestError,
        outcome: _Outcome,
    ) -> None:
        """Send ``parts`` (positions in ``lines``) of a request the server stored nothing of, each
        in a request of its own, as far as the batch's budget for that allows.

        A batch sends at most ``_ISOLATION_REQUESTS`` such requests; the parts left over (the
        last ones: put the most promising first) fail with the server's error. Halving costs
        about 2 * log2(lines) requests per line the server refuses, so the budget finds two in
        25,000 lines that way, and many more where the server or the client can tell them.
        """
        affordable = parts[: outcome.isolation_budget]
        outcome.isolation_budget -= len(affordable)
        for part in affordable:
            self._send(batch, _part(lines, part), _part(indices, part), outcome)
        unsent = sorted(position for part in parts[len(affordable) :] for position in part)
        if unsent:
            error.add_note(
                f"{len(unsent)} line(s) that may be valid were not written: the lines a server refuses "
                f"a request for are singled out with at most {_ISOLATION_REQUESTS} extra requests per batch"
            )
            _fail(outcome, _part(indices, unsent), error)

    def _post(self, batch: _Batch, payload: bytes, outcome: _Outcome) -> None:
        """POST one payload, retrying transient failures. Raises the final error."""
        if self._stop.is_set():
            # close() gave up on the requests in flight: send no more of them (a split batch's rest).
            raise ClientClosedError("the client was closed before this request was sent")
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
            error: SluiceboxError
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

        Only a request built under the current limit teaches something new. Requests already
        in flight when a lower limit was learned are rejected too; halving the limit again
        for each of them would shrink it far below the server's.
        """
        limit = max(1024, rejected_bytes // 2)
        with self._lock:
            if rejected_bytes > self._max_batch_bytes or limit >= self._max_batch_bytes:
                return
            self._max_batch_bytes = limit
        self._metrics.batch_limit(limit)
        log.warning(
            "the server (or a proxy in front of it) rejected a %d-byte request as too large; "
            "limiting batches to %d bytes from now on (set write.max_batch_bytes to avoid the probing)",
            rejected_bytes,
            limit,
        )

    def _finish(self, batch: _Batch, outcome: _Outcome) -> None:
        """Report a processed batch, resolve its futures, then release it.

        Futures and ``on_error`` handlers run *before* the batch stops counting as
        outstanding, so a ``flush()`` that returns has seen all of their side effects. A
        failure while reporting (metrics, logging, learning server types) never keeps the
        futures from being resolved or the batch from being released.
        """
        with self._lock:
            if batch.finished:  # close() gave up on this request and reported it already
                return
            batch.finished = True
        unreported: _Unreported | None = None
        total = len(batch.lines)
        failed_lines = min(total, sum(failure.failed for failure in outcome.failures))
        written = total - failed_lines
        finished = time.monotonic()
        elapsed = finished - batch.created
        raw_bytes = batch.nbytes
        try:
            try:
                self._observe(batch, outcome, elapsed, written, failed_lines)
            except Exception:
                log.exception("error while recording the metrics of a write batch")
            errors: list[BaseException | None] | None = None
            if outcome.failures:
                # In line order (lines found unparseable are recorded before the rest of their request).
                outcome.failures.sort(key=lambda failure: failure.lo)
                handled = self._report(batch, outcome, failed_lines)
                self._learn_types(batch, outcome.failures)
                errors = self._future_errors(batch, outcome.failures)
                if not handled:
                    affected = tuple(f for f, e in zip(batch.futures, errors, strict=True) if e is not None)
                    unreported = _Unreported(outcome.failures[0].error, failed_lines, affected)
            self._resolve_futures(batch, errors, finished)
        finally:
            with self._lock:
                self._sending.discard(batch)
                self._pending_bytes -= batch.nbytes
                self._outstanding.difference_update(batch.seqs)
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
                self._has_space.notify_all()
                self._progress.notify_all()
                # Under the lock, so that the last update wins and the gauges end up right.
                self._metrics.buffer(self._pending_bytes)
                self._metrics.inflight(len(self._sending))

    @staticmethod
    def _resolve_futures(batch: _Batch, errors: list[BaseException | None] | None, now: float) -> None:
        """Release a completed batch's futures (``errors``: one per future, or None for none).

        Their state is updated under their lane's lock, a chunk at a time, so that a large
        batch holds up its writing threads only briefly. Waiters are woken and callbacks run
        afterwards, each on its own, so one callback cannot hold up the others.
        """
        futures = batch.futures
        notify: list[WriteFuture] = []
        start = 0
        for lane, count in batch.lane_runs():
            end = start + count
            for low in range(start, end, _RELEASE_CHUNK):
                high = min(end, low + _RELEASE_CHUNK)
                with lane.lock:
                    if errors is None:
                        notify += [future for future in futures[low:high] if future._release(None, now)]
                    else:
                        notify += [
                            future
                            for future, error in zip(futures[low:high], errors[low:high], strict=True)
                            if future._release(error, now)
                        ]
            start = end
        for future in notify:
            try:
                future._complete()
            except BaseException:  # e.g. SystemExit from a callback (Exceptions are caught there)
                log.exception("a write future callback raised")

    def _observe(
        self, batch: _Batch, outcome: _Outcome, elapsed: float, written: int, failed_lines: int
    ) -> None:
        """Stage timings, metrics and the slow-batch log of a processed batch."""
        self._stages.record("batch", elapsed)
        if batch.serialize_time:
            self._stages.record("serialize", batch.serialize_time)
        if outcome.failures:
            first = outcome.failures[0].error
            self._metrics.batch_failed(
                batch.database, failed_lines, "partial" if written else "failed", first
            )
            if written:
                self._metrics.points_partially_rejected(batch.database, written, 0)
            return
        total = len(batch.lines)
        self._metrics.batch_succeeded(batch.database, total, batch.nbytes, outcome.bytes_sent, elapsed)
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

    @staticmethod
    def _future_errors(batch: _Batch, failures: list[_Failure]) -> list[BaseException | None]:
        """The error of each future of a batch (None for success), given its failures in line order.

        One pass over both: a batch can carry thousands of write() calls and rejected lines.
        """
        errors: list[BaseException | None] = []
        first = 0  # the first failure that can still overlap a future's lines
        start = 0
        for count, offset in zip(batch.counts, batch.offsets, strict=True):
            end = start + count
            while first < len(failures) and failures[first].hi <= start:
                first += 1
            last = first
            while last < len(failures) and failures[last].lo < end:
                last += 1
            relevant = failures[first:last]
            errors.append(
                WriteEngine._future_error(batch.lines, relevant, start, count, offset) if relevant else None
            )
            start = end
        return errors

    @staticmethod
    def _future_error(
        lines: list[str], relevant: list[_Failure], start: int, count: int, offset: int
    ) -> BaseException:
        """The error a future sees for its lines ``start:start+count`` of a batch (``relevant``:
        the failures among them)."""
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
        retryable = any(
            isinstance(f.error, ClientClosedError) or self._retry.is_retryable(f.error)
            for f in outcome.failures
        )
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
                try:
                    self._on_type_conflict(batch.database, *key)
                except Exception:
                    log.exception(
                        "could not learn the type the server stores for field %r of %r", key[1], key[0]
                    )

    # ------------------------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------------------------

    def close(self, timeout: float | None = None) -> WriteError | None:
        """Flush buffered data (up to ``timeout``), stop the threads and return unreported failures."""
        self.check_wait_allowed("close()")
        timeout = self.close_timeout if timeout is None else timeout
        self.begin_close(timeout)  # nothing to do if already closed
        with self._lock:
            if self._closed:
                return None
            deadline = self._deadline or time.monotonic()
            while (self._queue or self._sending) and self._threads:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._progress.wait(remaining)
            abandoned = list(self._queue)
            self._queue.clear()
            self._queued.clear()
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

    def begin_close(self, timeout: float) -> None:
        """Stop accepting writes and queue everything buffered for sending, without waiting."""
        with self._lock:
            if not self._closing:
                self._closing = True
                self._deadline = time.monotonic() + timeout
                self._has_work.notify_all()
                self._flusher_wake.notify_all()
                self._has_space.notify_all()
            lanes = list(self._lanes)  # no lane registers once _closing is set
        # A write that held a lane's lock before this has its lines in a batch sealed here; one
        # that takes the lock afterwards finds the lane unusable and the engine closing.
        for lane in lanes:
            with lane.lock:
                lane.usable = False
                for key, batch in list(lane.open.items()):
                    lane._seal(key, batch)

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
                open_batches=self._open_count,
                queued_batches=len(self._queue),
                inflight_batches=len(self._sending),
            )

    def _alive_lanes(self) -> list[_Lane]:
        refs = self._all_lanes.copy()  # one atomic copy: lanes may come and go meanwhile
        return [lane for ref in refs.values() if (lane := ref()) is not None]

    def _after_fork(self) -> None:
        """In a forked child: forget the parent's buffers, threads and connections."""
        if self._pid == os.getpid():
            return
        lanes = self._alive_lanes()
        inherited = [batch for lane in lanes for batch in lane.open.values()]
        inherited += [*self._queue, *self._sending]
        for lane in lanes:
            lane.open = {}
            lane.credit = 0
            lane.usable = False  # registers with the child's state when written to
        self._init_state()
        self._transport.reset_after_fork()
        error = ClientClosedError("buffered in the parent process before fork(); the parent sends it")
        now = time.monotonic()
        for batch in inherited:
            self._resolve_futures(batch, [error] * len(batch.futures), now)


# The engines' locks are held across fork(), in a fixed order.
_held_across_fork: list[threading.Lock] = []


def _before_fork() -> None:
    _held_across_fork[:] = [engine._lock for engine in list(_ENGINES)]
    for lock in _held_across_fork:
        lock.acquire()


def _after_fork_in_parent() -> None:
    for lock in reversed(_held_across_fork):
        lock.release()
    _held_across_fork.clear()


def _release_in_child() -> None:
    _held_across_fork.clear()  # the engine locks are replaced by _after_fork
    for engine in list(_ENGINES):
        for lane in engine._alive_lanes():
            lane.lock = threading.Lock()  # a thread that does not exist here may have held it


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
        engine.begin_close(engine.close_timeout)
    for engine in engines:
        try:
            failure = engine.close()
            if failure is not None:
                log.error("at exit: %s", failure)
                if not _has_handlers(log):
                    # Data was lost and nothing would say so: the library only installs a NullHandler.
                    print(f"sluicebox: at exit: {failure}", file=sys.stderr)
        except Exception:
            log.exception("failed to flush sluicebox write buffer at exit")


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
