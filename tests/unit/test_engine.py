"""Write engine behaviour against a scriptable HTTP server."""

from __future__ import annotations

import os
import threading
import time
import warnings
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from influxkit import (
    AuthenticationError,
    BadRequestError,
    BufferFullError,
    ClientClosedError,
    InfluxClient,
    InfluxConnectionError,
    InfluxTimeoutError,
    PartialWriteError,
    ServerError,
    WriteError,
    WriteFailure,
)
from influxkit._engine import _close_all_at_exit

from .fake_server import FakeInflux, Recorded, Reply, sequence


def points(n: int, start: int = 0, measurement: str = "m") -> list[dict[str, Any]]:
    return [
        {"measurement": measurement, "fields": {"v": float(i)}, "time": i} for i in range(start, start + n)
    ]


def write_many(client: InfluxClient, batches: int) -> None:
    for i in range(batches):
        client.write(points(5, start=i * 5))


def partial_body(*line_numbers: int) -> dict[str, Any]:
    return {
        "error": "partial write of line protocol occurred",
        "data": [
            {"error_message": f"bad line {n}", "line_number": n, "original_line": f"line{n}"}
            for n in line_numbers
        ],
    }


class TestBatching:
    def test_batch_size_splits_requests(self, fake: FakeInflux, client_for: Any) -> None:
        client = client_for(write={"batch_size": 3, "flush_interval": 60})
        client.write(points(7)).result(timeout=5)
        assert sorted(len(r.lines) for r in fake.write_requests) == [1, 3, 3]
        # Batches are sent concurrently, so only their content (not arrival order) is fixed.
        assert sorted(fake.lines) == sorted(f"m v={float(i)!r} {i}" for i in range(7))

    def test_small_writes_coalesce_into_one_batch(self, fake: FakeInflux, client_for: Any) -> None:
        client = client_for(write={"flush_interval": 60})
        futures = [client.write(p) for p in points(50)]
        client.flush(timeout=5)
        assert all(f.done() for f in futures)
        assert len(fake.write_requests) == 1
        assert len(fake.lines) == 50

    def test_flush_interval_sends_partial_batches(self, fake: FakeInflux, client_for: Any) -> None:
        client = client_for(write={"flush_interval": 0.1})
        future = client.write(points(1))
        deadline = time.monotonic() + 3
        while not fake.write_requests and time.monotonic() < deadline:
            time.sleep(0.01)
        assert fake.lines == ["m v=0.0 0"]
        assert future.result(timeout=2).points == 1

    def test_result_flushes_without_waiting_for_interval(self, fake: FakeInflux, client_for: Any) -> None:
        client = client_for(write={"flush_interval": 60})
        started = time.monotonic()
        assert client.write(points(2)).result(timeout=5).points == 2
        assert time.monotonic() - started < 2

    def test_byte_limit_splits_large_lines(self, fake: FakeInflux, client_for: Any) -> None:
        client = client_for(write={"max_batch_bytes": 2048, "batch_size": 10_000, "gzip": False})
        records = [{"measurement": "m", "fields": {"s": "x" * 900}, "time": i} for i in range(5)]
        client.write(records).result(timeout=5)
        assert sorted(len(r.lines) for r in fake.write_requests) == [1, 2, 2]

    def test_request_shape_v3(self, fake: FakeInflux, client_for: Any) -> None:
        client = client_for(write={"no_sync": True, "accept_partial": False})
        client.write(points(1), precision="ms").result(timeout=5)
        request = fake.write_requests[0]
        assert request.path == "/api/v3/write_lp"
        assert request.args == {
            "db": "db",
            "precision": "millisecond",
            "no_sync": "true",
            "accept_partial": "false",
        }
        assert request.headers["Authorization"] == "Bearer test-token"
        assert request.headers["Content-Type"] == "text/plain; charset=utf-8"
        assert request.headers["User-Agent"].startswith("influxkit/")

    def test_request_shape_v2(self, fake: FakeInflux, client_for: Any) -> None:
        client = client_for(version=2)
        client.write(points(1), database="bucket2").result(timeout=5)
        request = fake.write_requests[0]
        assert request.path == "/api/v2/write"
        assert request.args == {"bucket": "bucket2", "org": "org", "precision": "ns"}
        assert request.headers["Authorization"] == "Token test-token"

    def test_v3_with_v2_endpoint(self, fake: FakeInflux, client_for: Any) -> None:
        client = client_for(write={"api": "v2"})
        client.write(points(1)).result(timeout=5)
        assert fake.write_requests[0].path == "/api/v2/write"
        assert fake.write_requests[0].args == {"bucket": "db", "precision": "ns"}

    def test_gzip_threshold(self, fake: FakeInflux, client_for: Any) -> None:
        client = client_for(write={"gzip_min_bytes": 1000})
        client.write(points(1)).result(timeout=5)
        client.write(points(200)).result(timeout=5)
        small, large = fake.write_requests
        assert "Content-Encoding" not in small.headers
        assert large.headers["Content-Encoding"] == "gzip"
        assert len(large.raw) < sum(len(line) for line in large.lines)

    def test_concurrency_limit(self, fake: FakeInflux, client_for: Any) -> None:
        fake.responder = lambda _: Reply(delay=0.1)
        client = client_for(write={"batch_size": 10, "concurrency": 3})
        client.write(points(200)).result(timeout=10)
        assert fake.max_active == 3
        assert len(fake.lines) == 200


class TestFailures:
    def test_retries_honour_retry_after(self, fake: FakeInflux, client_for: Any) -> None:
        fake.responder = sequence(
            Reply(503, {"code": "unavailable", "message": "busy"}, {"Retry-After": "0.2"}),
            Reply(429, {"code": "too many requests", "message": "slow down"}),
            Reply(),
        )
        client = client_for()
        started = time.monotonic()
        assert client.write(points(3)).result(timeout=10).points == 3
        assert time.monotonic() - started >= 0.2
        assert len(fake.write_requests) == 3
        assert client.stats().write.retries == 2
        assert fake.write_requests[0].lines == fake.write_requests[2].lines  # identical retry payload

    def test_retries_exhausted(self, fake: FakeInflux, client_for: Any) -> None:
        fake.responder = lambda _: Reply(500, {"code": "internal error", "message": "boom"})
        client = client_for(write={"retry": {"max_attempts": 3}})
        with pytest.raises(ServerError, match="boom") as info:
            client.write(points(1)).result(timeout=10)
        assert info.value.status == 500
        assert any("3 attempts" in note for note in info.value.__notes__)
        assert len(fake.write_requests) == 3

    def test_bad_request_is_not_retried(self, fake: FakeInflux, client_for: Any) -> None:
        fake.responder = lambda _: Reply(400, {"code": "invalid", "message": "unable to parse"})
        client = client_for()
        with pytest.raises(BadRequestError, match="unable to parse"):
            client.write(points(1)).result(timeout=5)
        assert len(fake.write_requests) == 1

    def test_auth_error_reaches_future_and_flush(self, fake: FakeInflux, client_for: Any) -> None:
        fake.responder = lambda _: Reply(401, {"error": "the request was not authenticated"})
        client = client_for()
        future = client.write(points(2))
        with pytest.raises(AuthenticationError):
            future.result(timeout=5)
        client.flush()  # the caller saw the failure through its future: not reported again
        client.write(points(3))  # fire and forget: nobody looks at this future
        with pytest.raises(WriteError) as info:
            client.flush()
        assert info.value.failed_points == 3
        assert isinstance(info.value.__cause__, AuthenticationError)
        client.flush()  # failures are reported once

    def test_failure_seen_through_exception_or_callback_is_not_reraised(
        self, fake: FakeInflux, client_for: Any
    ) -> None:
        fake.responder = lambda _: Reply(401, {"error": "nope"})
        client = client_for()
        outcomes: list[BaseException | None] = []
        client.write(points(1)).add_done_callback(lambda f: outcomes.append(f.exception()))
        assert isinstance(client.write(points(1)).exception(timeout=5), AuthenticationError)
        client.flush()
        assert len(outcomes) == 1
        assert isinstance(outcomes[0], AuthenticationError)

    def test_on_error_receives_lines_and_silences_flush(self, fake: FakeInflux, client_for: Any) -> None:
        fake.responder = lambda _: Reply(401, {"error": "nope"})
        failures: list[WriteFailure] = []
        client = client_for(on_error=failures.append)
        client.write(points(2))
        client.flush(timeout=5)
        assert len(failures) == 1
        assert failures[0].lines == ["m v=0.0 0", "m v=1.0 1"]
        assert isinstance(failures[0].error, AuthenticationError)

    def test_payload_too_large_splits_until_accepted(self, fake: FakeInflux, client_for: Any) -> None:
        fake.responder = lambda r: (
            Reply(413, {"code": "request too large", "message": "max"}) if len(r.lines) > 2 else Reply()
        )
        client = client_for()
        pad = "x" * 600  # proxies limit bodies to kilobytes at least; tiny 413s are not about size
        records = [{"measurement": "m", "fields": {"s": pad}, "time": i} for i in range(7)]
        assert client.write(records).result(timeout=5).points == 7
        accepted = [line for r in fake.write_requests if len(r.lines) <= 2 for line in r.lines]
        assert sorted(accepted) == sorted(f'm s="{pad}" {i}' for i in range(7))

    def test_413_for_tiny_requests_is_not_split_further(self, fake: FakeInflux, client_for: Any) -> None:
        fake.responder = lambda _: Reply(413, {"code": "request too large", "message": "max"})
        client = client_for()
        with pytest.raises(ServerError) as info:
            client.write(points(200)).result(timeout=5)
        assert info.value.status == 413
        assert any("not about size" in note for note in info.value.__notes__)
        assert len(fake.write_requests) < 10  # no halving down to single lines

    def test_single_line_too_large_fails(self, fake: FakeInflux, client_for: Any) -> None:
        fake.responder = lambda _: Reply(413, {"code": "request too large", "message": "max"})
        client = client_for()
        with pytest.raises(ServerError) as info:
            client.write(points(1)).result(timeout=5)
        assert info.value.status == 413

    def test_partial_write_is_attributed_to_the_right_call(self, fake: FakeInflux, client_for: Any) -> None:
        fake.responder = sequence(Reply(400, partial_body(4)))
        client = client_for(write={"flush_interval": 60})
        first = client.write(points(2))
        second = client.write(points(3, start=2))
        client.flush_nowait = None  # type: ignore[attr-defined]  # (not part of the API; ensure no misuse)
        assert first.result(timeout=5).points == 2  # lines 1-2 were accepted
        with pytest.raises(PartialWriteError) as info:
            second.result(timeout=5)
        error = info.value
        assert error.rejected == 1
        assert [(e.line_number, e.message) for e in error.line_errors] == [(2, "bad line 4")]
        stats = client.stats().write
        assert (stats.points_written, stats.points_failed) == (4, 1)

    def test_v2_partial_write_counts(self, fake: FakeInflux, client_for: Any) -> None:
        message = (
            'partial write: field type conflict: input field "v" on measurement "m" is type float dropped=2'
        )
        fake.responder = sequence(Reply(422, {"code": "unprocessable entity", "message": message}))
        client = client_for(version=2)
        with pytest.raises(PartialWriteError) as info:
            client.write(points(5)).result(timeout=5)
        assert info.value.rejected == 2
        assert (client.stats().write.points_written, client.stats().write.points_failed) == (3, 2)

    def test_connection_refused_is_retried_then_reported(self, client_for: Any, make_settings: Any) -> None:
        settings = make_settings(url="http://127.0.0.1:9", write={"retry": {"max_attempts": 2}})
        client = InfluxClient(settings, registry=CollectorRegistry())
        try:
            with pytest.raises(InfluxConnectionError):
                client.write(points(1)).result(timeout=10)
            assert client.stats().write.retries == 1
        finally:
            client.close()  # the failure was already raised by result()

    def test_read_timeout(self, fake: FakeInflux, client_for: Any) -> None:
        fake.responder = lambda _: Reply(delay=1.0)
        client = client_for(connection={"timeout": 0.2}, write={"retry": {"max_attempts": 1}})
        with pytest.raises(InfluxTimeoutError):
            client.write(points(1)).result(timeout=10)

    def test_not_found_on_v3_endpoint_hints_at_cloud(self, fake: FakeInflux, client_for: Any) -> None:
        fake.responder = lambda _: Reply(404, "not found")
        client = client_for()
        with pytest.raises(ServerError) as info:
            client.write(points(1)).result(timeout=5)
        assert any("write.api = 'v2'" in note for note in info.value.__notes__)


class TestBackpressure:
    def slow(self, fake: FakeInflux) -> threading.Event:
        release = threading.Event()

        def respond(_: Recorded) -> Reply:
            release.wait(10)
            return Reply()

        fake.responder = respond
        return release

    def overrides(self, on_full: str, **extra: Any) -> dict[str, Any]:
        return {
            "write": {
                "max_pending_bytes": 2048,
                "max_batch_bytes": 1024,
                "batch_size": 5,
                "concurrency": 1,
                "on_full": on_full,
                **extra,
            }
        }

    def test_raise_policy(self, fake: FakeInflux, client_for: Any) -> None:
        release = self.slow(fake)
        client = client_for(**self.overrides("raise"))
        with pytest.raises(BufferFullError):
            write_many(client, 200)
        release.set()

    def test_drop_policy(self, fake: FakeInflux, client_for: Any) -> None:
        release = self.slow(fake)
        client = client_for(**self.overrides("drop"))
        futures = [client.write(points(5, start=i * 5)) for i in range(100)]
        release.set()
        client.flush(timeout=10)
        dropped = sum(f.dropped for f in futures)
        written = sum(f.result().points for f in futures)
        assert dropped > 0
        assert written + dropped == 500
        assert client.stats().write.points_dropped == dropped

    def test_block_policy_with_timeout(self, fake: FakeInflux, client_for: Any) -> None:
        release = self.slow(fake)
        client = client_for(**self.overrides("block", block_timeout=0.3))
        started = time.monotonic()
        with pytest.raises(BufferFullError, match="still full"):
            write_many(client, 200)
        assert time.monotonic() - started >= 0.3
        release.set()

    def test_block_policy_waits_for_space(self, fake: FakeInflux, client_for: Any) -> None:
        fake.responder = lambda _: Reply(delay=0.01)
        client = client_for(**self.overrides("block"))
        for i in range(60):
            client.write(points(5, start=i * 5))
        client.flush(timeout=10)
        assert len(fake.lines) == 300


class TestLifecycle:
    def test_close_flushes(self, fake: FakeInflux, client_for: Any) -> None:
        client = client_for(write={"flush_interval": 60})
        future = client.write(points(3))
        client.close()
        assert future.done()
        assert len(fake.lines) == 3
        with pytest.raises(ClientClosedError):
            client.write(points(1))
        client.close()  # idempotent

    def test_close_timeout_abandons_and_reports(self, fake: FakeInflux, client_for: Any) -> None:
        release = threading.Event()
        fake.responder = lambda _: (release.wait(5), Reply())[1]
        client = client_for(write={"batch_size": 1, "concurrency": 1})
        futures = [client.write(p) for p in points(3)]
        with pytest.raises(WriteError):
            client.close(timeout=0.3)
        release.set()
        failed = [f for f in futures if f.done() and f.errors]
        assert any(isinstance(f.errors[0], ClientClosedError) for f in failed)

    def test_context_manager_does_not_mask_exceptions(self, fake: FakeInflux, make_settings: Any) -> None:
        fake.responder = lambda _: Reply(401, {"error": "nope"})
        settings = make_settings(url=fake.url)

        def failing_block() -> None:
            with InfluxClient(settings, registry=CollectorRegistry()) as client:
                client.write(points(1))
                raise RuntimeError("original")

        with pytest.raises(RuntimeError, match="original"):
            failing_block()

    def test_exit_handler_flushes(self, fake: FakeInflux, client_for: Any) -> None:
        client = client_for(write={"flush_interval": 60})
        client.write(points(4))
        _close_all_at_exit()
        assert len(fake.lines) == 4

    @pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
    def test_forked_child_writes_its_own_data_only(self, fake: FakeInflux, client_for: Any) -> None:
        """A real fork: the child gets fresh threads/connections and never re-sends the parent's buffer."""
        client = client_for(write={"flush_interval": 60})
        parent_future = client.write(points(2))  # buffered in the parent, not yet sent
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)  # "process is multi-threaded" (expected here)
            pid = os.fork()
        if pid == 0:  # child
            code = 1
            try:
                client.write(points(1, start=10)).result(timeout=10)
                code = 0
            finally:
                os._exit(code)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        assert fake.lines == ["m v=10.0 10"]  # only the child's point so far
        assert parent_future.result(timeout=5).points == 2
        assert sorted(fake.lines) == ["m v=0.0 0", "m v=1.0 1", "m v=10.0 10"]

    def test_stats_and_futures(self, fake: FakeInflux, client_for: Any) -> None:
        client = client_for()
        result = client.write(points(10)).result(timeout=5)
        assert result.points == 10
        assert result.duration > 0
        stats = client.stats()
        assert stats.write.points_written == 10
        assert stats.write.batches_written == 1
        assert stats.write.buffered_bytes == 0
        assert {"serialize", "queue", "request", "batch"} <= set(stats.stages)


class TestProxy:
    def test_requests_go_through_the_proxy_with_credentials(
        self, fake: FakeInflux, make_settings: Any
    ) -> None:
        # The fake server plays the forward proxy: it sees absolute-URI requests.
        settings = make_settings(
            url="http://influx.example:8181",
            connection={"proxy": fake.url.replace("http://", "http://user:p%40ss@")},
        )
        with InfluxClient(settings, registry=CollectorRegistry()) as client:
            client.write(points(1)).result(timeout=5)
        request = fake.write_requests[0]
        assert request.path == "/api/v3/write_lp"
        assert request.headers["Proxy-Authorization"] == "Basic dXNlcjpwQHNz"  # user:p@ss
        assert request.headers["Host"] == "influx.example:8181"


class TestConcurrency:
    def test_many_threads_share_one_client(self, fake: FakeInflux, client_for: Any) -> None:
        """Every line arrives exactly once and intact, also on free-threaded Python."""
        from influxkit import tag_context

        client = client_for(write={"batch_size": 500, "concurrency": 8, "flush_interval": 0.01})
        threads_count, per_thread = 8, 2_000
        errors: list[BaseException] = []
        barrier = threading.Barrier(threads_count)

        def worker(t: int) -> None:
            try:
                barrier.wait()
                with tag_context(worker=str(t)):
                    for i in range(0, per_thread, 2):
                        client.write({"measurement": "m", "fields": {"v": float(i)}, "time": t * 10**6 + i})
                        client.write(
                            [{"measurement": "m", "fields": {"v": float(i + 1)}, "time": t * 10**6 + i + 1}]
                        )
            except BaseException as error:  # pragma: no cover - reported below
                errors.append(error)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(threads_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        client.flush(timeout=30)
        assert not errors
        expected = sorted(
            f"m,worker={t} v={float(i)!r} {t * 10**6 + i}"
            for t in range(threads_count)
            for i in range(per_thread)
        )
        assert sorted(fake.lines) == expected
        stats = client.stats().write
        assert stats.points_written == threads_count * per_thread
        assert stats.buffered_bytes == 0
