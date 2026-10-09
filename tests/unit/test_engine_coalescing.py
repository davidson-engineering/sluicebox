"""Queued batches of waiting callers are sent together, with exact per-call bookkeeping."""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Any

import pytest

from sluicebox import (
    ClientClosedError,
    InfluxClient,
    PartialWriteError,
    WriteError,
    WriteFailure,
    WriteFuture,
)

from .fake_server import FakeInflux, Recorded, Reply

if TYPE_CHECKING:
    from collections.abc import Callable


def point(i: int, measurement: str = "m") -> dict[str, Any]:
    return {"measurement": measurement, "fields": {"v": float(i)}, "time": i}


def line(i: int, measurement: str = "m") -> str:
    return f"{measurement} v={float(i)!r} {i}"


def wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.005)


def send_now(client: InfluxClient, records: Any, **kwargs: Any) -> WriteFuture:
    """write() and seal the batch at once, as waiting on the future does."""
    future = client.write(records, **kwargs)
    client._engine.flush_nowait()
    return future


class Gate:
    """Holds the first request in flight until opened, so the batches sealed meanwhile queue up."""

    def __init__(self, fake: FakeInflux, then: Callable[[Recorded], Reply] = lambda _: Reply()) -> None:
        self.open = threading.Event()
        self._first = True
        self._lock = threading.Lock()
        self._fake = fake
        self._then = then
        fake.responder = self._respond

    def _respond(self, request: Recorded) -> Reply:
        with self._lock:
            first, self._first = self._first, False
        if first:
            self.open.wait(10)
            return Reply()
        return self._then(request)

    def hold(self, client: InfluxClient) -> WriteFuture:
        """Put one point in flight (it waits for the gate) and return its future."""
        future = send_now(client, point(-1, "held"))
        wait_until(lambda: len(self._fake.write_requests) == 1)
        return future


def held_back(fake: FakeInflux) -> list[list[str]]:
    """Lines of each request after the held one."""
    return [r.lines for r in fake.write_requests[1:]]


class TestCoalescing:
    def test_waiting_callers_share_one_request(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        gate = Gate(fake)
        client = client_for(write={"concurrency": 1, "flush_interval": 60})
        held = gate.hold(client)
        results: dict[int, int] = {}

        def caller(i: int) -> None:
            results[i] = client.write(point(i)).result(timeout=10).points

        threads = [threading.Thread(target=caller, args=(i,)) for i in range(20)]
        for thread in threads:
            thread.start()
        # All 20 points sealed (callers can share an open batch, so count bytes, not batches).
        buffered = len("held v=-1.0 -1") + 1 + sum(len(line(i)) + 1 for i in range(20))

        def all_queued() -> bool:
            stats = client.stats().write
            return stats.open_batches == 0 and stats.buffered_bytes == buffered

        wait_until(all_queued)
        gate.open.set()
        for thread in threads:
            thread.join()
        assert held.result(timeout=5).points == 1
        assert results == dict.fromkeys(range(20), 1)
        assert [sorted(lines) for lines in held_back(fake)] == [sorted(line(i) for i in range(20))]
        stats = client.stats()
        write = stats.write
        assert (write.points_written, write.batches_written, write.points_failed) == (21, 2, 0)
        assert (write.buffered_bytes, write.queued_batches, write.inflight_batches) == (0, 0, 0)
        assert write.bytes_raw == sum(len(sent) + 1 for r in fake.write_requests for sent in r.lines)
        # Stages are recorded per request (serialize: the time of all its write() calls together).
        assert stats.stages["batch"].count == stats.stages["queue"].count == 2
        assert stats.stages["serialize"].count == 2

    def test_lines_keep_their_order_and_both_limits_hold(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        gate = Gate(fake)
        client = client_for(write={"concurrency": 1, "flush_interval": 60, "batch_size": 3})
        gate.hold(client)
        futures = [send_now(client, point(i)) for i in range(7)]
        gate.open.set()
        client.flush(timeout=5)  # waits for every batch merged into a request
        assert all(f.done() and not f.errors for f in futures)
        assert held_back(fake) == [[line(0), line(1), line(2)], [line(3), line(4), line(5)], [line(6)]]

    def test_byte_limit_holds(self, fake: FakeInflux, client_for: Callable[..., InfluxClient]) -> None:
        gate = Gate(fake)
        client = client_for(write={"concurrency": 1, "flush_interval": 60, "max_batch_bytes": 1024})
        gate.hold(client)
        pad = "x" * 300  # about 320 bytes per line: three fit in 1024
        for i in range(5):
            send_now(client, {"measurement": "m", "fields": {"s": pad}, "time": i})
        gate.open.set()
        client.flush(timeout=5)
        assert [len(lines) for lines in held_back(fake)] == [3, 2]

    def test_only_the_same_database_and_precision_are_merged(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        gate = Gate(fake)
        client = client_for(write={"concurrency": 1, "flush_interval": 60})
        gate.hold(client)
        send_now(client, point(0))
        send_now(client, point(1), database="other")
        send_now(client, point(2))
        send_now(client, point(3), precision="ms")
        send_now(client, point(4))
        send_now(client, point(5), database="other")
        gate.open.set()
        client.flush(timeout=5)
        sent = [(r.args["db"], r.args["precision"], r.lines) for r in fake.write_requests[1:]]
        assert sent == [
            ("db", "nanosecond", [line(0), line(2), line(4)]),
            ("other", "nanosecond", [line(1), line(5)]),
            ("db", "millisecond", [line(3)]),
        ]
        assert client.stats().write.batches_written == 4

    def test_rejected_lines_are_attributed_to_their_calls(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        def reject_4_and_7(request: Recorded) -> Reply:
            return Reply(
                400,
                {
                    "error": "partial write of line protocol occurred",
                    "data": [
                        {"error_message": f"bad {n}", "line_number": n, "original_line": request.lines[n - 1]}
                        for n in (4, 7)
                    ],
                },
            )

        gate = Gate(fake, then=reject_4_and_7)
        failures: list[WriteFailure] = []
        client = client_for(write={"concurrency": 1, "flush_interval": 60}, on_error=failures.append)
        gate.hold(client)
        first = send_now(client, [point(0), point(1)])
        second = send_now(client, [point(2), point(3), point(4)])  # lines 3-5 of the request
        third = send_now(client, [point(5), point(6)])  # lines 6-7
        gate.open.set()
        assert first.result(timeout=5).points == 2
        for future, expected in ((second, (2, "bad 4", line(3))), (third, (2, "bad 7", line(6)))):
            with pytest.raises(PartialWriteError) as info:
                future.result(timeout=5)
            assert [(e.line_number, e.message, e.line) for e in info.value.line_errors] == [expected]
        assert held_back(fake) == [[line(i) for i in range(7)]]
        [failure] = failures
        assert failure.lines == [line(3), line(6)]
        assert isinstance(failure.error, PartialWriteError)
        assert [(e.line_number, e.line) for e in failure.error.line_errors] == [(1, line(3)), (2, line(6))]
        write = client.stats().write
        assert (write.points_written, write.points_failed) == (1 + 5, 2)
        assert (write.batches_written, write.batches_failed, write.buffered_bytes) == (1, 1, 0)

    def test_a_merged_request_too_large_is_split(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        def limit_to_two_lines(request: Recorded) -> Reply:
            if len(request.lines) > 2:
                return Reply(413, {"code": "request too large", "message": "max"})
            return Reply()

        gate = Gate(fake, then=limit_to_two_lines)
        client = client_for(write={"concurrency": 1, "flush_interval": 60})
        gate.hold(client)
        pad = "x" * 600  # proxies limit bodies to kilobytes at least; tiny 413s are not about size
        futures = [send_now(client, {"measurement": "m", "fields": {"s": pad}, "time": i}) for i in range(5)]
        gate.open.set()
        assert [f.result(timeout=5).points for f in futures] == [1] * 5
        accepted = [line for r in fake.write_requests[1:] if len(r.lines) <= 2 for line in r.lines]
        assert accepted == [f'm s="{pad}" {i}' for i in range(5)]
        write = client.stats().write
        assert (write.points_written, write.batches_written, write.buffered_bytes) == (6, 2, 0)

    def test_close_fails_every_call_of_a_stuck_merged_request(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        late = threading.Event()

        def stuck(_: Recorded) -> Reply:
            late.wait(10)
            return Reply()

        gate = Gate(fake, then=stuck)
        client = client_for(write={"concurrency": 1, "flush_interval": 60})
        held = gate.hold(client)
        futures = [send_now(client, point(i)) for i in range(3)]
        gate.open.set()
        wait_until(lambda: len(fake.write_requests) == 2)  # the merged request is now in flight
        with pytest.raises(WriteError) as info:
            client.close(timeout=0.3)
        assert info.value.failed_points == 3
        assert held.result(timeout=1).points == 1
        for future in futures:
            assert isinstance(future.exception(timeout=1), ClientClosedError)
        late.set()  # the server answers after all: already reported, not counted again
        time.sleep(0.2)
        write = client.stats().write
        assert (write.points_written, write.points_failed, write.batches_failed) == (1, 3, 1)
        assert (write.buffered_bytes, write.inflight_batches) == (0, 0)

    def test_many_waiting_threads_against_a_slow_server(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        """Every write is acknowledged once, and requests carry many points each."""
        fake.responder = lambda _: Reply(delay=0.02)
        client = client_for(write={"concurrency": 2, "flush_interval": 60})
        threads_count, rounds = 32, 10
        errors: list[BaseException] = []

        def caller(t: int) -> None:
            try:
                for r in range(rounds):
                    assert client.write(point(t * rounds + r)).result(timeout=10).points == 1
            except BaseException as error:  # pragma: no cover - reported below
                errors.append(error)

        threads = [threading.Thread(target=caller, args=(t,)) for t in range(threads_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert not errors
        total = threads_count * rounds
        assert sorted(fake.lines) == sorted(line(i) for i in range(total))
        write = client.stats().write
        assert (write.points_written, write.buffered_bytes) == (total, 0)
        assert write.batches_written == len(fake.write_requests) <= total // 4
