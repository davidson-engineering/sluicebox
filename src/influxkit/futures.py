"""Write futures: confirmation for asynchronous (buffered) writes.

``client.write(...)`` returns a :class:`WriteFuture` immediately. Ignore it for
fire-and-forget throughput, or wait for the server's acknowledgement:

* ``future.result(timeout)`` blocks (and flushes the buffer so you never wait for the
  flush interval), returning a :class:`WriteResult` or raising the write's error;
* ``await future`` does the same from asyncio code without blocking the event loop;
* ``future.add_done_callback(fn)`` runs ``fn(future)`` on completion (on a sender thread).
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .exceptions import InfluxTimeoutError, PartialWriteError, ValidationError
from .log import RateLimitedLog

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from ._engine import WriteEngine

__all__ = ["WriteFuture", "WriteResult"]

#: Validation errors kept per write() call (``dropped`` always has the exact count).
MAX_REJECTED = 1000

log = logging.getLogger("influxkit.write")

# One lock for all futures: state transitions are tiny and rare relative to serialization.
# The write engine holds it across fork() (see ``_engine``), so the child inherits it unlocked.
_LOCK = threading.Lock()


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Outcome of one ``write()`` call."""

    #: Points (lines) acknowledged by the server.
    points: int
    #: Records dropped by client-side validation (``on_invalid = "drop"``) or a full buffer.
    dropped: int = 0
    #: Seconds from ``write()`` to acknowledgement.
    duration: float = 0.0
    #: Errors of the batches carrying this call's points; empty on success.
    errors: tuple[BaseException, ...] = field(default=(), repr=False)
    #: Why records were dropped by validation (``on_invalid = "drop"``): each error's ``index``
    #: is the record's (or DataFrame row's) position in the ``write()`` input. Capped at 1000.
    rejected: tuple[ValidationError, ...] = field(default=(), repr=False)


class WriteFuture:
    """Completion handle for one ``write()`` call (possibly spanning several batches)."""

    __slots__ = (
        "_callbacks",
        "_done",
        "_engine",
        "_errors",
        "_event",
        "_finished_at",
        "_observed",
        "_pending",
        "_sealed",
        "_started_at",
        "dropped",
        "points",
        "rejected",
    )

    def __init__(self, engine: WriteEngine | None) -> None:
        self._engine = engine
        #: Lines handed to the engine for this call.
        self.points = 0
        #: Records dropped before sending.
        self.dropped = 0
        #: Validation errors of the dropped records (capped at ``MAX_REJECTED``).
        self.rejected: list[ValidationError] = []
        self._pending = 0  # batches carrying our lines that have not completed
        self._sealed = False  # all of our lines have been handed over
        self._done = False
        self._observed = False  # the outcome was retrieved (flush/close then skip its failures)
        self._errors: list[BaseException] = []
        self._event: threading.Event | None = None
        self._callbacks: list[Callable[[WriteFuture], Any]] | None = None
        self._started_at = time.monotonic()
        self._finished_at = 0.0

    # -- engine side ------------------------------------------------------------------------

    def _attach(self) -> None:
        """A batch now carries some of our lines (called with the engine lock held)."""
        with _LOCK:
            self._pending += 1

    def _batch_done(self, error: BaseException | None) -> None:
        with _LOCK:
            if error is not None:
                self._errors.append(error)
            self._pending -= 1
            finished = self._sealed and self._pending == 0 and not self._done
            if finished:
                self._done = True
                self._finished_at = time.monotonic()
        if finished:
            self._complete()

    def _seal(self) -> None:
        """No more lines will be added for this call."""
        with _LOCK:
            self._sealed = True
            finished = self._pending == 0 and not self._done
            if finished:
                self._done = True
                self._finished_at = time.monotonic()
        if finished:
            self._complete()

    def _complete(self) -> None:
        event = self._event
        if event is not None:
            event.set()
        callbacks = self._callbacks
        if callbacks:
            for callback in callbacks:
                try:
                    callback(self)
                except Exception:
                    log.exception("write future callback %r failed", callback)

    # -- caller side ------------------------------------------------------------------------

    def done(self) -> bool:
        return self._done

    def result(self, timeout: float | None = None) -> WriteResult:
        """Wait for the server to acknowledge this write; raise its first error if it failed.

        Waiting flushes the buffer, so the call returns as soon as the server responds.

        Raises:
            InfluxTimeoutError: not acknowledged within ``timeout`` seconds.
            RuntimeError: called from a done-callback or ``on_error`` handler of the same
                client while the write is pending (that would wait for itself).
        """
        self._wait(timeout)
        self._observed = True
        if self._errors:
            raise self._combined_error()
        return self._result()

    def exception(self, timeout: float | None = None) -> BaseException | None:
        """Like :meth:`result` but returns the write's first error (or None) instead of raising it.

        Raises:
            InfluxTimeoutError: not acknowledged within ``timeout`` seconds.
        """
        self._wait(timeout)
        self._observed = True
        return self._combined_error() if self._errors else None

    @property
    def errors(self) -> tuple[BaseException, ...]:
        """All errors of the batches carrying this call's points (available once done)."""
        if self._done:
            self._observed = True
        return tuple(self._errors)

    def add_done_callback(self, callback: Callable[[WriteFuture], Any]) -> None:
        """Call ``callback(self)`` once complete (immediately if already complete)."""
        with _LOCK:
            if not self._done:
                if self._callbacks is None:
                    self._callbacks = []
                self._callbacks.append(callback)
                return
        callback(self)

    def __await__(self) -> Generator[Any, None, WriteResult]:
        if not self._done:
            loop = asyncio.get_running_loop()
            waiter: asyncio.Future[None] = loop.create_future()

            def wake(_: WriteFuture) -> None:
                loop.call_soon_threadsafe(_resolve, waiter)

            self.add_done_callback(wake)
            if not self._done and self._engine is not None:
                self._engine.flush_nowait()
            yield from waiter.__await__()
        self._observed = True
        if self._errors:
            raise self._combined_error()
        return self._result()

    def __repr__(self) -> str:
        state = "done" if self._done else "pending"
        if self._done and self._errors:
            state = f"failed: {self._errors[0]!r}"
        return f"<WriteFuture points={self.points} dropped={self.dropped} {state}>"

    # -- internals --------------------------------------------------------------------------

    def _wait(self, timeout: float | None) -> None:
        if self._done:
            return
        engine = self._engine
        if engine is not None:
            engine.check_wait_allowed("waiting for a WriteFuture")
        warn_if_event_loop("WriteFuture.result()", "await the future instead")
        event = self._ensure_event()
        if not self._done and engine is not None:
            engine.flush_nowait()
        if not event.wait(timeout):
            raise InfluxTimeoutError(
                f"write not acknowledged within {timeout} s (it is still pending and may succeed later)"
            )

    def _combined_error(self) -> BaseException:
        """The error to raise: one for the whole call, even if several batches failed."""
        errors = self._errors
        if len(errors) == 1:
            return _fresh(errors[0])
        if all(isinstance(error, PartialWriteError) and error.line_errors for error in errors):
            first = errors[0]
            assert isinstance(first, PartialWriteError)
            line_errors = tuple(
                sorted(
                    (line for error in errors for line in error.line_errors),  # type: ignore[attr-defined]
                    key=lambda line: line.line_number,
                )
            )
            return PartialWriteError(
                f"{len(line_errors)} of {self.points} points rejected by the server: "
                + "; ".join(f"line {e.line_number}: {e.message}" for e in line_errors[:3]),
                status=first.status,
                code=first.code,
                request_id=first.request_id,
                line_errors=line_errors,
            )
        error = _fresh(errors[0])
        error.add_note(f"{len(errors) - 1} more error(s) for other parts of this write(): see .errors")
        return error

    def _ensure_event(self) -> threading.Event:
        with _LOCK:
            if self._event is None:
                self._event = threading.Event()
                if self._done:
                    self._event.set()
            return self._event

    def _result(self) -> WriteResult:
        return WriteResult(
            points=self.points,
            dropped=self.dropped,
            duration=max(0.0, self._finished_at - self._started_at),
            errors=tuple(self._errors),
            rejected=tuple(self.rejected),
        )


_loop_warnings = RateLimitedLog(logging.getLogger("influxkit.client"), interval=60.0)


def warn_if_event_loop(call: str, instead: str) -> None:
    """Warn (rate limited) when a blocking call runs on a running asyncio event loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    _loop_warnings.log(
        f"loop:{call}",
        logging.WARNING,
        "%s blocks the running asyncio event loop: %s (or use AsyncInfluxClient)",
        call,
        instead,
    )


def _fresh(error: BaseException) -> BaseException:
    """A copy of ``error`` for one ``raise``.

    The futures of a failed batch share its exception. Raising one instance repeatedly (and
    from several threads) would append every caller's frames to the same traceback.
    """
    cls = type(error)
    try:
        clone = cls.__new__(cls, *error.args)
        clone.args = error.args
        clone.__dict__.update(error.__dict__)  # attributes, including __notes__
    except Exception:  # an exotic exception type: raise the shared instance after all
        return error
    clone.__cause__ = error.__cause__
    clone.__context__ = error.__context__
    clone.__suppress_context__ = error.__suppress_context__
    clone.__traceback__ = error.__traceback__
    return clone


def _resolve(waiter: asyncio.Future[None]) -> None:
    if not waiter.done():
        waiter.set_result(None)


def completed_future(points: int = 0, dropped: int = 0, error: BaseException | None = None) -> WriteFuture:
    """A future that is already complete (used when nothing needs sending)."""
    future = WriteFuture(None)
    future.points = points
    future.dropped = dropped
    if error is not None:
        future._errors.append(error)
    future._seal()
    return future
