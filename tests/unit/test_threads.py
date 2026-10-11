"""Many threads writing through one client: per-thread lanes and serializer views.

Every test runs with each thread on state of its own (as on free-threaded Python) and with
all threads sharing it (as with the GIL), whatever the build. They race writers against
flush(), close(), backpressure, fork() and each other, and check that every write arrives
exactly once and every future resolves.
"""

from __future__ import annotations

import os
import signal
import threading
import time
import warnings
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

import pytest

from sluicebox import (
    ClientClosedError,
    FieldType,
    InfluxClient,
    ValidationError,
    WriteFuture,
    _engine,
    _threads,
)
from sluicebox._lineprotocol import Dialect
from sluicebox._serializer import Serializer
from sluicebox.config import TagsConfig, ValidationConfig
from sluicebox.tags import TagInjector

from .fake_server import FakeInflux, Reply

if TYPE_CHECKING:
    from collections.abc import Callable


@pytest.fixture(params=[True, False], ids=["per-thread", "shared"])
def per_thread(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> bool:
    monkeypatch.setattr(_threads, "PER_THREAD", request.param)
    return bool(request.param)


def record(t: int, i: int, measurement: str = "m") -> dict[str, Any]:
    return {"measurement": measurement, "tags": {"t": str(t)}, "fields": {"v": float(i)}, "time": i}


def line(t: int, i: int, measurement: str = "m") -> str:
    return f"{measurement},t={t} v={float(i)!r} {i}"


def run_threads(count: int, target: Callable[[int], None]) -> list[BaseException]:
    errors: list[BaseException] = []

    def guarded(t: int) -> None:
        try:
            target(t)
        except BaseException as error:  # pragma: no cover - reported by the caller
            errors.append(error)

    threads = [threading.Thread(target=guarded, args=(t,)) for t in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    return errors


def serializer(version: int = 3, **validation: Any) -> Serializer:
    return Serializer(
        dialect=Dialect.for_version(version),
        validation=ValidationConfig(**validation),
        schemas={},
        injector=TagInjector(TagsConfig()),
        auto_timestamp=False,
    )


class TestLifecycle:
    def test_flush_waits_for_every_write_that_returned_before_it(
        self, per_thread: bool, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        client = client_for(write={"batch_size": 500, "concurrency": 4, "flush_interval": 60})
        writers, per_writer = 6, 3_000
        futures: list[list[WriteFuture]] = [[] for _ in range(writers)]
        running = threading.Event()
        running.set()

        def write(t: int) -> None:
            for i in range(per_writer):
                futures[t].append(client.write(record(t, i)))

        threads = [threading.Thread(target=write, args=(t,)) for t in range(writers)]
        for thread in threads:
            thread.start()
        flushes = 0
        while any(thread.is_alive() for thread in threads) or flushes < 3:
            returned = [list(calls) for calls in futures]  # all of these returned before flush()
            client.flush(timeout=30)
            flushes += 1
            assert all(future.done() for calls in returned for future in calls)
        for thread in threads:
            thread.join()
        client.flush(timeout=30)
        assert Counter(fake.lines) == Counter(line(t, i) for t in range(writers) for i in range(per_writer))
        assert all(future.result().points == 1 for calls in futures for future in calls)
        stats = client.stats().write
        assert (stats.buffered_bytes, stats.open_batches, stats.points_written) == (
            0,
            0,
            writers * per_writer,
        )

    def test_close_under_writers_resolves_everything_it_accepted(
        self, per_thread: bool, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        for round_ in range(3):
            measurement = f"m{round_}"
            client = client_for(write={"batch_size": 50, "concurrency": 4, "flush_interval": 0.01})
            accepted: list[list[tuple[WriteFuture, str]]] = [[] for _ in range(6)]
            refused = [0] * 6

            def write(
                t: int,
                client: InfluxClient = client,
                measurement: str = measurement,
                accepted: list[list[tuple[WriteFuture, str]]] = accepted,
                refused: list[int] = refused,
            ) -> None:
                for i in range(100_000):
                    try:
                        future = client.write(record(t, i, measurement))
                    except ClientClosedError:
                        refused[t] += 1
                        return
                    accepted[t].append((future, line(t, i, measurement)))

            threads = [threading.Thread(target=write, args=(t,)) for t in range(6)]
            for thread in threads:
                thread.start()
            time.sleep(0.05)
            client.close(timeout=20)
            for thread in threads:
                thread.join(timeout=30)
            assert refused == [1] * 6  # every writer ran into the closed client
            calls = [call for calls in accepted for call in calls]
            assert all(future.done() for future, _ in calls)
            assert all(future.result().points == 1 for future, _ in calls)  # close() sent them all
            received = Counter(text for text in fake.lines if text.startswith(f"{measurement},"))
            assert received == Counter(text for _, text in calls)
            stats = client.stats().write
            assert (stats.buffered_bytes, stats.open_batches, stats.queued_batches) == (0, 0, 0)

    def test_a_write_that_reserved_space_as_close_began_is_refused(
        self,
        per_thread: bool,
        fake: FakeInflux,
        client_for: Callable[..., InfluxClient],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A write waits for buffer space without its lane's lock; close() may seal the lane
        meanwhile, and nothing would ever send a batch the write opened after that."""
        client = client_for(write={"flush_interval": 60})
        engine = client._engine
        reserve = engine._reserve

        def reserve_as_close_begins(*args: Any) -> int:
            granted = reserve(*args)
            engine.begin_close(5)
            return granted

        monkeypatch.setattr(engine, "_reserve", reserve_as_close_begins)
        with pytest.raises(ClientClosedError):
            client.write(record(0, 0))
        client.close()
        stats = client.stats().write
        assert (stats.buffered_bytes, stats.open_batches) == (0, 0)
        assert fake.lines == []

    @pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
    def test_fork_while_threads_write(
        self, per_thread: bool, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        """A child forked while other threads hold their lanes' locks can still write."""
        client = client_for(write={"batch_size": 100, "concurrency": 4, "flush_interval": 0.005})
        stop = threading.Event()
        written = [0] * 4

        def write(t: int) -> None:
            i = 0
            while not stop.is_set():
                future = client.write(record(t, i))
                if i % 50 == 0:
                    future.result(timeout=10)
                i += 1
            written[t] = i

        threads = [threading.Thread(target=write, args=(t,)) for t in range(4)]
        for thread in threads:
            thread.start()
        try:
            for child in range(5):
                time.sleep(0.02)
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", DeprecationWarning)  # "process is multi-threaded"
                    pid = os.fork()
                if pid == 0:
                    signal.alarm(20)  # a child that deadlocks dies, rather than hang the test
                    code = 1
                    try:
                        if client.write(record(100 + child, 0)).result(timeout=10).points == 1:
                            code = 0
                    finally:
                        os._exit(code)
                _, status = os.waitpid(pid, 0)
                assert os.waitstatus_to_exitcode(status) == 0, "the child failed or deadlocked"
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=30)
        client.flush(timeout=30)
        expected = [line(t, i) for t in range(4) for i in range(written[t])]
        expected += [line(100 + child, 0) for child in range(5)]
        assert Counter(fake.lines) == Counter(expected)


class TestBackpressure:
    def overrides(self, on_full: str) -> dict[str, Any]:
        return {
            "write": {
                "max_pending_bytes": 4096,
                "max_batch_bytes": 1024,
                "batch_size": 20,
                "concurrency": 4,
                "flush_interval": 0.01,
                "on_full": on_full,
            }
        }

    def test_blocked_writers_never_exceed_the_buffer(
        self, per_thread: bool, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        fake.responder = lambda _: Reply(delay=0.005)
        client = client_for(**self.overrides("block"))
        done = threading.Event()
        highest = 0

        def sample() -> None:
            nonlocal highest
            while not done.is_set():
                highest = max(highest, client.stats().write.buffered_bytes)
                time.sleep(0.001)

        sampler = threading.Thread(target=sample)
        sampler.start()
        try:
            errors = run_threads(8, lambda t: [client.write(record(t, i)) for i in range(150)])
        finally:
            done.set()
            sampler.join()
        client.flush(timeout=30)
        assert not errors
        assert 0 < highest <= 4096
        assert Counter(fake.lines) == Counter(line(t, i) for t in range(8) for i in range(150))

    def test_dropped_and_written_add_up(
        self, per_thread: bool, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        fake.responder = lambda _: Reply(delay=0.005)
        client = client_for(**self.overrides("drop"))
        futures: list[list[WriteFuture]] = [[] for _ in range(8)]
        errors = run_threads(8, lambda t: futures[t].extend(client.write(record(t, i)) for i in range(150)))
        client.flush(timeout=30)
        assert not errors
        written = {
            line(t, i) for t in range(8) for i, future in enumerate(futures[t]) if future.result().points
        }
        dropped = sum(future.dropped for calls in futures for future in calls)
        assert dropped > 0
        assert len(written) + dropped == 8 * 150
        assert Counter(fake.lines) == Counter(written)
        assert client.stats().write.points_dropped == dropped

    def test_space_reserved_by_idle_threads_drops_nothing(
        self, per_thread: bool, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        """Each writing thread reserves buffer space ahead; a full buffer first takes back what
        is reserved and unused, so it drops no more than the buffer itself requires."""
        client = client_for(
            write={
                "max_pending_bytes": 256 * 1024,  # 1 KiB reserved at a time
                "max_batch_bytes": 64 * 1024,
                "flush_interval": 60,  # batches stay open, and their reservations with them
                "on_full": "drop",
            }
        )
        futures: list[WriteFuture] = []
        errors = run_threads(400, lambda t: futures.append(client.write(record(t, 0))))  # 400 x 1 KiB
        assert not errors
        assert sum(future.dropped for future in futures) == 0
        client.flush(timeout=30)
        assert Counter(fake.lines) == Counter(line(t, 0) for t in range(400))
        assert client.stats().write.buffered_bytes == 0


class TestLanes:
    def test_idle_lanes_are_unregistered_and_come_back(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(_threads, "PER_THREAD", True)
        monkeypatch.setattr(_engine, "_RETIRE_AFTER", 0.02)
        client = client_for(write={"flush_interval": 0.005})
        engine = client._engine
        assert not run_threads(20, lambda t: client.write(record(t, 0)).result(timeout=10))
        deadline = time.monotonic() + 10
        i = 0
        while len(engine._lanes) > 1:  # the threads are gone: only this thread's lane stays
            assert time.monotonic() < deadline, f"{len(engine._lanes)} lanes still registered"
            client.write(record(99, i))  # keeps the flusher (which unregisters lanes) busy
            i += 1
            time.sleep(0.005)
        lane = client._thread_state()[1]
        with ThreadPoolExecutor(1) as other:  # now another thread writes, and this one idles
            while lane in engine._lanes:
                assert time.monotonic() < deadline, "this thread's lane stayed registered"
                other.submit(client.write, record(98, i)).result()
                i += 1
                time.sleep(0.005)
        assert client.write(record(97, 0)).result(timeout=10).points == 1  # registered again
        assert lane in engine._lanes
        client.flush(timeout=10)
        assert len(fake.lines) == len(set(fake.lines))  # each written once
        assert client.stats().write.buffered_bytes == 0

    def test_concurrency_one_keeps_one_lane(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """concurrency = 1 sends requests in the order lines were written, across threads too."""
        monkeypatch.setattr(_threads, "PER_THREAD", True)
        client = client_for(write={"concurrency": 1, "flush_interval": 60})
        with ThreadPoolExecutor(1) as other:
            other.submit(client.write, record(0, 1)).result()
        client.write(record(1, 2))
        client.flush(timeout=10)
        assert fake.lines == [line(0, 1), line(1, 2)]
        assert len(fake.write_requests) == 1


class TestSerializerViews:
    def test_racing_first_writes_lock_one_type(self, per_thread: bool) -> None:
        """Threads writing a new field with different types at once: one type wins, and no
        line of another type is ever produced."""
        for _ in range(20):
            shared = serializer(on_invalid="drop")
            barrier = threading.Barrier(8)
            outputs: list[list[str]] = [[] for _ in range(8)]

            def write(
                t: int,
                shared: Serializer = shared,
                barrier: threading.Barrier = barrier,
                outputs: list[list[str]] = outputs,
            ) -> None:
                value: Any = 1.5 if t % 2 else "text"
                barrier.wait()
                for i in range(20):
                    outputs[t] += shared.serialize(
                        [{"measurement": "m", "fields": {"x": value}, "time": i}],
                        database="db",
                        precision="ns",
                    ).lines

            assert not run_threads(8, write)
            kinds = {"string" if '"' in text else "float" for lines in outputs for text in lines}
            assert len(kinds) == 1
            assert shared.locked_types("db", "m") == {"x": FieldType(kinds.pop())}

    def test_racing_tag_and_field_of_one_name(self, per_thread: bool) -> None:
        """InfluxDB 3 refuses a tag and a field of the same name: threads writing both at once
        never get both through."""
        for _ in range(20):
            shared = serializer(on_invalid="drop")
            barrier = threading.Barrier(8)
            outputs: list[list[str]] = [[] for _ in range(8)]

            def write(
                t: int,
                shared: Serializer = shared,
                barrier: threading.Barrier = barrier,
                outputs: list[list[str]] = outputs,
            ) -> None:
                if t % 2:
                    point = {"measurement": "m", "tags": {"x": "a"}, "fields": {"v": 1.0}, "time": 1}
                else:
                    point = {"measurement": "m", "fields": {"x": 1.0}, "time": 1}
                barrier.wait()
                for _ in range(20):
                    outputs[t] += shared.serialize([point], database="db", precision="ns").lines

            assert not run_threads(8, write)
            written = {text for lines in outputs for text in lines}
            assert written in ({"m,x=a v=1.0 1"}, {"m x=1.0 1"})

    def test_a_relocked_type_reaches_every_thread(self, per_thread: bool) -> None:
        shared = serializer()

        def write(fields: dict[str, Any]) -> list[str]:
            return shared.serialize(
                [{"measurement": "m", "fields": fields}], database="db", precision="ns"
            ).lines

        with ThreadPoolExecutor(1) as other:  # one thread, whose view caches "v" as a float field
            assert other.submit(write, {"v": 1.5}).result() == ["m v=1.5"]
            assert shared.relock("db", "m", "v", FieldType.INTEGER) is FieldType.FLOAT
            assert other.submit(write, {"v": 2}).result() == ["m v=2i"]
            with pytest.raises(ValidationError, match="locked as integer"):
                other.submit(write, {"v": 2.5}).result()

    def test_types_seeded_later_reach_untyped_fields(self, per_thread: bool) -> None:
        shared = serializer(type_lock=False)

        def write(fields: dict[str, Any]) -> list[str]:
            return shared.serialize(
                [{"measurement": "m", "fields": fields}], database="db", precision="ns"
            ).lines

        with ThreadPoolExecutor(1) as other:
            assert other.submit(write, {"v": 1.5}).result() == ["m v=1.5"]  # no type: as it is
            shared.seed_types("db", "m", {"v": FieldType.STRING})
            with pytest.raises(ValidationError, match="type conflict"):
                other.submit(write, {"v": 2.5}).result()

    def test_a_record_rejected_in_one_thread_locks_nothing_for_others(self, per_thread: bool) -> None:
        shared = serializer(on_invalid="drop")
        with ThreadPoolExecutor(1) as first, ThreadPoolExecutor(1) as second:
            rejected = {"measurement": "m", "fields": {"a": 1.5, "b": [1]}}  # b: unsupported type
            chunk = first.submit(shared.serialize, [rejected], database="db", precision="ns").result()
            assert chunk.dropped == 1
            accepted = {"measurement": "m", "fields": {"a": "text"}}
            assert second.submit(
                shared.serialize, [accepted], database="db", precision="ns"
            ).result().lines == ['m a="text"']
        assert shared.locked_types("db", "m") == {"a": FieldType.STRING}
