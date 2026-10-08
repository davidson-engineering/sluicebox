"""Write path robustness: lifecycle races, failure reporting, retries, misconfiguration."""

from __future__ import annotations

import logging
import os
import socket
import subprocess
import sys
import textwrap
import threading
import time
import traceback
import warnings
from typing import TYPE_CHECKING, Any

import pytest
from prometheus_client import CollectorRegistry

from influxkit import (
    AuthenticationError,
    ClientClosedError,
    ConfigurationError,
    InfluxClient,
    InfluxConnectionError,
    NotFoundError,
    PartialWriteError,
    ServerError,
    WriteError,
    WriteFailure,
)
from influxkit.types import FieldType

from .fake_server import FakeInflux, Recorded, Reply, sequence

if TYPE_CHECKING:
    from collections.abc import Callable


def points(n: int, start: int = 0, measurement: str = "m") -> list[dict[str, Any]]:
    return [
        {"measurement": measurement, "fields": {"v": float(i)}, "time": 1_700_000_000_000_000_000 + i}
        for i in range(start, start + n)
    ]


def v3_type_conflict(*line_numbers: int, expected: str = "float") -> dict[str, Any]:
    return {
        "error": "partial write of line protocol occurred",
        "data": [
            {
                "error_message": f"invalid column type for column 'v', expected "
                f"iox::column_type::field::{expected}, got iox::column_type::field::string",
                "line_number": n,
                "original_line": "m v=x 1",
            }
            for n in line_numbers
        ],
    }


class TestLifecycle:
    def test_close_accounts_for_writers_blocked_on_a_full_buffer(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        """Writers woken while close() runs must fail, never leave an unsent batch behind."""
        fake.responder = lambda _: Reply(delay=0.05)
        for _ in range(5):
            client = client_for(
                write={"batch_size": 1, "max_batch_bytes": 1024, "max_pending_bytes": 1024, "concurrency": 2}
            )
            futures, errors = _write_concurrently(client, 8)
            stats = client.stats().write
            assert stats.open_batches == 0
            assert stats.buffered_bytes == 0
            assert all(future.done() for future in futures)
            assert len(futures) + len(errors) == 8

    def test_split_non_ascii_writes_leave_no_buffered_bytes(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        client = client_for(write={"max_batch_bytes": 2048, "max_pending_bytes": 8192, "block_timeout": 2})
        lines = [f"m,unit=°C,room=Küche v={i}.5 {1_700_000_000_000_000_000 + i}" for i in range(200)]
        for _ in range(10):  # each write is split; leaked bytes would fill the buffer
            client.write(lines).result(timeout=5)
        assert client.stats().write.buffered_bytes == 0

    def test_close_timeout_bounds_close_and_fails_requests_in_flight(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        fake.responder = lambda _: Reply(delay=3)
        client = client_for(write={"batch_size": 1, "concurrency": 8}, connection={"timeout": 10})
        futures = [client.write(points(1, start=i)) for i in range(8)]
        client.flush_nowait = None  # type: ignore[attr-defined]
        time.sleep(0.2)  # all eight requests in flight
        started = time.monotonic()
        with pytest.raises(WriteError) as info:
            client.close(timeout=0.5)
        assert time.monotonic() - started < 2
        assert info.value.failed_points == 8
        for future in futures:
            assert isinstance(future.exception(timeout=1), ClientClosedError)
        stats = client.stats().write
        assert stats.points_failed == 8
        assert stats.buffered_bytes == 0
        assert stats.inflight_batches == 0

    def test_waiting_inside_a_callback_raises_instead_of_deadlocking(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        client = client_for(write={"concurrency": 1})
        outcome: list[BaseException] = []

        def callback(_: Any) -> None:
            for call in (client.flush, lambda: client.write(points(1, start=5)).result(), client.close):
                try:
                    call()
                except RuntimeError as exc:
                    outcome.append(exc)

        client.write(points(1)).add_done_callback(callback)
        client.flush(timeout=5)
        assert len(outcome) == 3
        assert all("callback" in str(exc) for exc in outcome)
        assert client.write(points(1, start=9)).result(timeout=5).points == 1  # the sender still works

    def test_errors_get_a_fresh_traceback_per_raise(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        fake.responder = lambda _: Reply(401, {"error": "nope"})
        client = client_for(write={"flush_interval": 60})
        futures = [client.write(points(1, start=i)) for i in range(20)]
        lengths = []
        for _ in range(3):
            for future in futures:
                with pytest.raises(AuthenticationError) as info:
                    future.result(timeout=5)
                lengths.append(len(traceback.extract_tb(info.value.__traceback__)))
        assert max(lengths) == min(lengths)  # no growth with every raise
        assert all(isinstance(future.exception(), AuthenticationError) for future in futures)

    def test_exit_without_close_flushes(self, fake: FakeInflux) -> None:
        """End to end: a process that never calls close() still delivers its points at exit."""
        script = textwrap.dedent(
            f"""
            from influxkit import InfluxClient, load_settings
            settings = load_settings(None, env_file=None, token="t",
                connection={{"url": "{fake.url}", "version": 3, "database": "db"}})
            client = InfluxClient(settings)
            client.write([{{"measurement": "m", "fields": {{"v": float(i)}}, "time": i}} for i in range(100)])
            """
        )
        started = time.monotonic()
        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=60, check=False
        )
        assert result.returncode == 0, result.stderr
        assert time.monotonic() - started < 15
        assert len(fake.lines) == 100

    def test_exit_reports_lost_points_without_logging_setup(self) -> None:
        script = textwrap.dedent(
            """
            from influxkit import InfluxClient, load_settings
            settings = load_settings(None, env_file=None, token="t",
                connection={"url": "http://127.0.0.1:9", "version": 3, "database": "db"},
                write={"close_timeout": 0.5, "retry": {"initial_delay": 0.01}})
            InfluxClient(settings).write({"measurement": "m", "fields": {"v": 1.0}, "time": 1})
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=60, check=False
        )
        assert "influxkit: at exit:" in result.stderr
        assert "1 points not written" in result.stderr

    def test_futures_in_flight_at_fork_fail_in_the_child(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        fake.responder = lambda _: Reply(delay=0.5)
        client = client_for()
        future = client.write(points(1))
        client._engine.flush_nowait()
        time.sleep(0.1)  # the request is in flight
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)  # "process is multi-threaded"
            pid = os.fork()
        if pid == 0:  # child: the parent sends it; here the future fails at once
            code = 1
            try:
                if isinstance(future.exception(timeout=2), ClientClosedError):
                    code = 0
            finally:
                os._exit(code)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        assert future.result(timeout=5).points == 1


def _write_concurrently(client: InfluxClient, writers: int) -> tuple[list[Any], list[BaseException]]:
    """Start ``writers`` threads writing large points, then close the client under them."""
    futures: list[Any] = []
    errors: list[BaseException] = []

    def writer(i: int) -> None:
        try:
            futures.append(client.write({"measurement": "m", "fields": {"s": "x" * 400}, "time": i}))
        except ClientClosedError as exc:
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(writers)]
    for thread in threads:
        thread.start()
    time.sleep(0.02)
    client.close(timeout=5)
    for thread in threads:
        thread.join(timeout=5)
    return futures, errors


class TestFailureReporting:
    def test_on_error_gets_each_failed_batch_once_with_the_sent_lines(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        fake.responder = lambda _: Reply(400, v3_type_conflict(1, 2, 3))
        failures: list[WriteFailure] = []
        client = client_for(on_error=failures.append, write={"flush_interval": 60})
        future = client.write(points(5))
        with pytest.raises(PartialWriteError) as info:
            future.result(timeout=5)
        assert [e.line_number for e in info.value.line_errors] == [1, 2, 3]
        assert info.value.line_errors[0].line == "m v=0.0 1700000000000000000"  # the full line we sent
        assert len(failures) == 1
        assert failures[0].lines == [line.line for line in info.value.line_errors]
        assert isinstance(failures[0].error, PartialWriteError)
        assert [e.line_number for e in failures[0].error.line_errors] == [1, 2, 3]
        assert failures[0].retryable is False

    def test_transient_failures_are_marked_retryable(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        fake.responder = lambda _: Reply(503, {"message": "busy"})
        failures: list[WriteFailure] = []
        client = client_for(on_error=failures.append, write={"retry": {"max_attempts": 2}})
        client.write(points(2))
        client.flush(timeout=5)
        assert [f.retryable for f in failures] == [True]

    def test_one_error_for_a_call_spanning_several_batches(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        fake.responder = lambda r: Reply(400, v3_type_conflict(*range(1, len(r.lines) + 1)))
        client = client_for(write={"batch_size": 10})
        future = client.write(points(30))
        with pytest.raises(PartialWriteError) as info:
            future.result(timeout=5)
        assert len(info.value.line_errors) == 30
        assert [e.line_number for e in info.value.line_errors] == list(range(1, 31))
        assert len(future.errors) == 3

    def test_failing_on_error_handler_reports_through_flush(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        fake.responder = lambda _: Reply(401, {"error": "nope"})

        def broken(_: WriteFailure) -> None:
            raise OSError("disk full")

        client = client_for(on_error=broken)
        client.write(points(3))
        with pytest.raises(WriteError) as info:
            client.flush(timeout=5)
        assert info.value.failed_points == 3

    def test_buffer_full_drops_are_counted_in_the_log(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient], caplog: Any
    ) -> None:
        fake.responder = lambda _: Reply(delay=0.3)
        caplog.set_level(logging.WARNING, logger="influxkit.write")
        client = client_for(
            write={"on_full": "drop", "max_pending_bytes": 1024, "max_batch_bytes": 1024, "batch_size": 1}
        )
        dropped = sum(
            client.write({"measurement": "m", "fields": {"s": "x" * 300}}).dropped for _ in range(30)
        )
        assert dropped > 1
        assert client.stats().write.points_dropped == dropped
        messages = [r.getMessage() for r in caplog.records if "buffer full" in r.getMessage()]
        assert messages
        assert "since the last report" in messages[0]


class TestRetries:
    def test_retries_continue_until_max_elapsed(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        fake.responder = sequence(*[Reply(503, {"message": "restarting"})] * 12, Reply())
        client = client_for()  # no max_attempts: an outage is ridden out for retry.max_elapsed
        assert client.write(points(1)).result(timeout=10).points == 1
        assert client.stats().write.retries == 12

    def test_long_retry_after_gets_a_last_attempt_at_the_deadline(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        fake.responder = sequence(Reply(429, {"message": "slow down"}, {"Retry-After": "60"}), Reply())
        client = client_for(write={"retry": {"max_elapsed": 0.5}})
        started = time.monotonic()
        assert client.write(points(1)).result(timeout=10).points == 1
        assert 0.4 < time.monotonic() - started < 3


class TestMisconfiguration:
    def test_html_page_is_not_a_successful_write(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        """An InfluxDB 2 server answers /api/v3/write_lp with its UI (200 text/html)."""
        fake.responder = lambda _: Reply(200, "<!doctype html><html>UI</html>", {"Content-Type": "text/html"})
        client = client_for()
        with pytest.raises(ServerError, match="web page") as info:
            client.write(points(1)).result(timeout=5)
        assert "connection.version = 2" in str(info.value)
        assert client.stats().write.points_written == 0

    def test_check_accepts_a_matching_server(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        def respond(request: Recorded) -> Reply:
            if request.path == "/ping":
                return Reply(200, {"version": "3.12.0"})
            return Reply(400, "incoming write was empty")

        fake.responder = respond
        info = client_for().check()
        assert info.version == "3.12.0"
        assert info.major == 3
        assert fake.write_requests[0].raw == b""  # the probe writes nothing

    def test_check_detects_the_wrong_version(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        fake.responder = lambda _: Reply(204, headers={"X-Influxdb-Version": "v2.9.1"})
        with pytest.raises(ConfigurationError, match=r"runs InfluxDB 2\.9\.1: set connection\.version = 2"):
            client_for().check()

    def test_check_detects_a_missing_bucket(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        def respond(request: Recorded) -> Reply:
            if request.path == "/ping":
                return Reply(204, headers={"X-Influxdb-Version": "v2.9.1"})
            return Reply(404, {"code": "not found", "message": 'bucket "db" not found'})

        fake.responder = respond
        with pytest.raises(NotFoundError, match="bucket") as info:
            client_for(version=2).check()
        assert any("empty write" in note for note in info.value.__notes__)

    def test_check_detects_a_web_page(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        fake.responder = lambda _: Reply(200, "<html>dashboard</html>", {"Content-Type": "text/html"})
        with pytest.raises(ConfigurationError, match="did not identify itself as InfluxDB"):
            client_for().check()

    def test_401_without_a_token_says_so(self, fake: FakeInflux, make_settings: Callable[..., Any]) -> None:
        fake.responder = lambda _: Reply(401, {"error": "the request was not authenticated"})
        settings = make_settings(url=fake.url, token=None)
        with InfluxClient(settings, registry=CollectorRegistry()) as client:
            with pytest.raises(AuthenticationError) as info:
                client.write(points(1)).result(timeout=5)
            assert any("no token is configured: set INFLUXKIT_TOKEN" in note for note in info.value.__notes__)

    def test_url_path_prefix_is_kept(self, fake: FakeInflux, client_for: Callable[..., InfluxClient]) -> None:
        client = client_for(connection={"url": fake.url + "/influx/"})
        client.write(points(1)).result(timeout=5)
        assert fake.write_requests[0].path == "/influx/api/v3/write_lp"

    def test_https_to_a_plain_http_port_hints_at_http(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        client = client_for(connection={"url": fake.url.replace("http://", "https://")})
        with pytest.raises(InfluxConnectionError) as info:
            client.write(points(1)).result(timeout=5)
        assert any("http://" in note for note in info.value.__notes__)

    def test_metrics_port_in_use(self, make_settings: Callable[..., Any]) -> None:
        with socket.socket() as busy:
            busy.bind(("127.0.0.1", 0))
            busy.listen()
            port = busy.getsockname()[1]
            settings = make_settings(metrics={"port": port, "addr": "127.0.0.1"})
            with pytest.raises(ConfigurationError, match=f"cannot serve metrics on 127.0.0.1:{port}"):
                InfluxClient(settings, registry=CollectorRegistry())


class TestServerTypes:
    def test_a_type_conflict_reported_by_the_server_relocks_the_field(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient], caplog: Any
    ) -> None:
        """A new process whose first value has the wrong type must not reject every later value."""
        fake.responder = sequence(Reply(400, v3_type_conflict(1)), Reply())
        client = client_for()
        with pytest.raises(PartialWriteError):
            client.write({"measurement": "m", "fields": {"v": "oops"}, "time": 1}).result(timeout=5)
        assert client.locked_types("m") == {"v": FieldType.FLOAT}
        assert (
            client.write({"measurement": "m", "fields": {"v": 1.5}, "time": 2}).result(timeout=5).points == 1
        )
        assert any("now locked as float" in r.getMessage() for r in caplog.records)

    def test_influxdb2_type_conflicts_are_learned_too(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        message = (
            "failure writing points to database: partial write: field type conflict: input field "
            '"v" on measurement "m" is type string, already exists as type float dropped=1'
        )
        fake.responder = sequence(Reply(422, {"code": "unprocessable entity", "message": message}), Reply())
        client = client_for(version=2)
        with pytest.raises(ServerError):
            client.write({"measurement": "m", "fields": {"v": "oops"}, "time": 1}).result(timeout=5)
        assert client.locked_types("m") == {"v": FieldType.FLOAT}


class TestMetricsForAlerting:
    def test_series_exist_before_the_first_event(
        self, fake: FakeInflux, make_settings: Callable[..., Any]
    ) -> None:
        registry = CollectorRegistry()
        with InfluxClient(make_settings(url=fake.url, name="alerts"), registry=registry) as client:
            labels = {"client": "alerts", "database": "db"}
            assert registry.get_sample_value("influxkit_points_failed_total", labels) == 0
            assert (
                registry.get_sample_value(
                    "influxkit_points_dropped_total", {"client": "alerts", "reason": "buffer_full"}
                )
                == 0
            )
            limit = registry.get_sample_value("influxkit_write_buffer_limit_bytes", {"client": "alerts"})
            assert limit == client.settings.write.max_pending_bytes
            client.write(points(1)).result(timeout=5)
            last = registry.get_sample_value(
                "influxkit_write_last_success_timestamp_seconds", {"client": "alerts"}
            )
            assert last is not None
            assert abs(last - time.time()) < 60
