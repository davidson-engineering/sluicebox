"""Profiling: always-on stage timings plus an opt-in deep profiler.

Stage timings (``[profiling] enabled``, on by default) cost a few ``perf_counter`` calls per
batch or call and feed the ``stage_duration_seconds`` histogram and ``client.stats()``:

* ``serialize``  - converting records to line protocol (caller thread)
* ``backpressure`` - time callers spent blocked on a full buffer
* ``queue``      - a sealed batch waiting for a free sender
* ``compress``   - gzip
* ``request``    - one HTTP round trip
* ``batch``      - first point buffered -> acknowledged
* ``query``      - a query including result transfer

Batches and queries slower than the configured thresholds are logged with this breakdown.

For a deeper look, :func:`profile` wraps ``cProfile`` (and optionally ``tracemalloc``)
around any block::

    with client.profile("write.prof") as report:
        client.write(points).result()
    print(report.summary(limit=15))
"""

from __future__ import annotations

import cProfile
import io
import logging
import pstats
import threading
import time
import tracemalloc
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from . import _fork

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = ["ProfileReport", "StageStats", "profile"]

log = logging.getLogger("sluicebox.profiling")


@dataclass
class StageStats:
    """Aggregated timings of one stage (seconds)."""

    count: int = 0
    total: float = 0.0
    max: float = 0.0

    @property
    def mean(self) -> float:
        return self.total / self.count if self.count else 0.0

    def add(self, seconds: float) -> None:
        self.count += 1
        self.total += seconds
        self.max = max(self.max, seconds)


# Shared by all recorders: held only to update a few numbers.
_STAGE_LOCK = threading.Lock()


def _new_stage_lock() -> None:
    global _STAGE_LOCK
    _STAGE_LOCK = threading.Lock()


_fork.on_child(locks=_new_stage_lock)


class StageRecorder:
    """Thread-safe accumulator of stage timings, mirrored into Prometheus."""

    def __init__(self, enabled: bool, observe: object) -> None:
        self.enabled = enabled
        self._observe = observe
        self._stages: dict[str, StageStats] = {}

    def record(self, stage: str, seconds: float) -> None:
        if not self.enabled:
            return
        with _STAGE_LOCK:
            stats = self._stages.get(stage)
            if stats is None:
                stats = self._stages[stage] = StageStats()
            stats.add(seconds)
        self._observe(stage, seconds)  # type: ignore[operator]

    def snapshot(self) -> dict[str, StageStats]:
        with _STAGE_LOCK:
            return {name: StageStats(s.count, s.total, s.max) for name, s in self._stages.items()}


@dataclass
class ProfileReport:
    """Result of :func:`profile`, available after the ``with`` block exits."""

    wall_seconds: float = 0.0
    stats: pstats.Stats | None = None
    path: Path | None = None
    #: Peak traced memory in bytes (``memory=True`` only).
    peak_memory: int | None = None
    top_allocations: list[str] = field(default_factory=list)

    def summary(self, limit: int = 20, sort: str = "cumulative") -> str:
        """Top functions by ``sort`` (cumulative, tottime, calls...) as text."""
        lines = [f"wall time: {self.wall_seconds:.3f} s"]
        if self.peak_memory is not None:
            lines.append(f"peak traced memory: {self.peak_memory / 1024 / 1024:.1f} MiB")
            lines.extend(f"  {entry}" for entry in self.top_allocations)
        if self.stats is not None:
            buffer = io.StringIO()
            self.stats.stream = buffer  # type: ignore[attr-defined]
            # Short file names; the saved profile (path) keeps the full ones.
            self.stats.strip_dirs().sort_stats(sort).print_stats(limit)
            lines.append(buffer.getvalue())
        return "\n".join(lines)


@contextmanager
def profile(
    path: str | Path | None = None,
    *,
    memory: bool = False,
    log_summary: bool = False,
    limit: int = 20,
) -> Iterator[ProfileReport]:
    """Profile the enclosed block with ``cProfile`` (current thread).

    Args:
        path: Also dump raw stats here (open with ``snakeviz``/``pstats``).
        memory: Track allocations with ``tracemalloc`` and report the peak and top sites.
        log_summary: Log the summary at INFO on the ``sluicebox.profiling`` logger.
        limit: Number of rows in the logged summary.

    Background sender threads are not included; their cost shows up in stage timings.
    """
    report = ProfileReport(path=Path(path) if path else None)
    profiler = cProfile.Profile()
    started_tracing = False
    if memory and not tracemalloc.is_tracing():
        tracemalloc.start()
        started_tracing = True
    if memory:
        tracemalloc.reset_peak()
    start = time.perf_counter()
    profiler.enable()
    try:
        yield report
    finally:
        profiler.disable()
        report.wall_seconds = time.perf_counter() - start
        report.stats = pstats.Stats(profiler)
        if report.path is not None:
            report.stats.dump_stats(report.path)
        if memory:
            report.peak_memory = tracemalloc.get_traced_memory()[1]
            snapshot = tracemalloc.take_snapshot()
            report.top_allocations = [str(stat) for stat in snapshot.statistics("lineno")[:10]]
            if started_tracing:
                tracemalloc.stop()
        if log_summary:
            log.info("profile summary:\n%s", report.summary(limit))
