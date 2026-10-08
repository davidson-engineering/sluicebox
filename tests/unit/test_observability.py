"""Metrics, logging, profiling and the asyncio facade, exercised through the client."""

from __future__ import annotations

import asyncio
import io
import json
import logging
from datetime import UTC, datetime, timedelta, timezone
from importlib.util import find_spec
from typing import TYPE_CHECKING, Any

import pytest
from prometheus_client import CollectorRegistry

from sluicebox import (
    AsyncInfluxClient,
    AuthenticationError,
    InfluxClient,
    JsonFormatter,
    ValidationError,
    WriteError,
    __version__,
    configure_logging,
    profile,
)
from sluicebox.config import LoggingConfig
from sluicebox.log import RateLimitedLog
from sluicebox.query import QueryResult, flux_params

from .fake_server import FakeInflux, Reply

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


@pytest.fixture
def client(
    fake: FakeInflux, make_settings: Callable[..., Any], registry: CollectorRegistry
) -> Iterator[InfluxClient]:
    client = InfluxClient(make_settings(url=fake.url, name="obs"), registry=registry)
    yield client
    client.close()


def sample(registry: CollectorRegistry, name: str, **labels: str) -> float:
    value = registry.get_sample_value(name, labels)
    return 0.0 if value is None else value


# polars publishes no free-threaded (3.14t) wheels; CI runs that build without it.
requires_polars = pytest.mark.skipif(find_spec("polars") is None, reason="polars is not installed")


class TestMetrics:
    def test_write_metrics(self, client: InfluxClient, registry: CollectorRegistry) -> None:
        client.write([{"measurement": "m", "fields": {"v": float(i)}} for i in range(5)]).result(timeout=5)
        assert sample(registry, "sluicebox_points_written_total", client="obs", database="db") == 5
        assert (
            sample(registry, "sluicebox_write_batches_total", client="obs", database="db", outcome="success")
            == 1
        )
        assert sample(registry, "sluicebox_write_bytes_total", client="obs", database="db", kind="raw") > 0
        assert (
            sample(registry, "sluicebox_write_request_duration_seconds_count", client="obs", database="db")
            == 1
        )
        assert (
            sample(registry, "sluicebox_stage_duration_seconds_count", client="obs", stage="serialize") == 1
        )
        assert sample(registry, "sluicebox_write_buffer_bytes", client="obs") == 0
        labels = {
            "client": "obs",
            "version": __version__,
            "url": client.settings.connection.url,
            "server_version": "3",
            "database": "db",
        }
        assert registry.get_sample_value("sluicebox_client_info", labels) == 1.0

    def test_failure_and_drop_metrics(
        self, fake: FakeInflux, make_settings: Callable[..., Any], registry: CollectorRegistry
    ) -> None:
        fake.responder = lambda _: Reply(401, {"error": "nope"})
        settings = make_settings(url=fake.url, name="bad", validation={"on_invalid": "drop"})
        with InfluxClient(settings, registry=registry, on_error=lambda failure: None) as client:
            client.write(
                [{"measurement": "m", "fields": {"v": 1.0}}, {"measurement": "m", "fields": {"v": "x"}}]
            )
            client.flush(timeout=5)
        assert sample(registry, "sluicebox_points_failed_total", client="bad", database="db") == 1
        assert sample(registry, "sluicebox_points_dropped_total", client="bad", reason="type_conflict") == 1
        assert (
            sample(
                registry,
                "sluicebox_errors_total",
                client="bad",
                operation="write",
                error="AuthenticationError",
            )
            == 1
        )

    def test_two_clients_share_families(
        self, fake: FakeInflux, make_settings: Callable[..., Any], registry: CollectorRegistry
    ) -> None:
        a = InfluxClient(make_settings(url=fake.url, name="a"), registry=registry)
        b = InfluxClient(make_settings(url=fake.url, name="b"), registry=registry)
        a.write({"measurement": "m", "fields": {"v": 1.0}}).result(timeout=5)
        b.write({"measurement": "m", "fields": {"v": 1.0}}).result(timeout=5)
        a.close()
        b.close()
        assert sample(registry, "sluicebox_points_written_total", client="a", database="db") == 1
        assert sample(registry, "sluicebox_points_written_total", client="b", database="db") == 1

    def test_disabled(
        self, fake: FakeInflux, make_settings: Callable[..., Any], registry: CollectorRegistry
    ) -> None:
        with InfluxClient(
            make_settings(url=fake.url, metrics={"enabled": False}), registry=registry
        ) as client:
            client.write({"measurement": "m", "fields": {"v": 1.0}}).result(timeout=5)
        assert list(registry.collect()) == []


class TestLogging:
    def test_token_never_logged(
        self, fake: FakeInflux, make_settings: Callable[..., Any], caplog: Any
    ) -> None:
        fake.responder = lambda _: Reply(401, {"error": "nope"})
        caplog.set_level(logging.DEBUG, logger="sluicebox")
        settings = make_settings(url=fake.url, token="super-secret-token")
        with InfluxClient(settings, registry=CollectorRegistry()) as client:
            with pytest.raises(AuthenticationError):
                client.write({"measurement": "m", "fields": {"v": 1.0}}).result(timeout=5)
            assert "super-secret-token" not in repr(client)
            client.write({"measurement": "m", "fields": {"v": 2.0}})
            with pytest.raises(WriteError):
                client.flush()
        assert caplog.records
        assert all("super-secret-token" not in record.getMessage() for record in caplog.records)

    def test_json_formatter(self) -> None:
        record = logging.LogRecord(
            "sluicebox.write", logging.WARNING, __file__, 1, "hello %s", ("world",), None
        )
        record.influx = {"database": "db", "points": 5}
        payload = json.loads(JsonFormatter().format(record))
        assert payload["message"] == "hello world"
        assert payload["level"] == "WARNING"
        assert payload["database"] == "db"
        assert payload["points"] == 5

    def test_configure_logging_is_idempotent(self) -> None:
        stream = io.StringIO()
        logger = configure_logging(LoggingConfig(configure=True, level="DEBUG", format="json"), stream=stream)
        configure_logging(LoggingConfig(configure=True, level="DEBUG", format="json"), stream=stream)
        try:
            marked = [h for h in logger.handlers if getattr(h, "_sluicebox_handler", False)]
            assert len(marked) == 1
            logging.getLogger("sluicebox.test").info("structured")
            assert json.loads(stream.getvalue().splitlines()[-1])["message"] == "structured"
        finally:
            for handler in marked:
                logger.removeHandler(handler)
            logger.propagate = True
            logger.setLevel(logging.NOTSET)

    def test_rate_limited_log(self, caplog: Any) -> None:
        caplog.set_level(logging.WARNING, logger="sluicebox.ratelimit")
        limited = RateLimitedLog(logging.getLogger("sluicebox.ratelimit"), interval=0.2)
        for _ in range(50):
            limited.log("k", logging.WARNING, "boom")
        assert len(caplog.records) == 1
        import time

        time.sleep(0.25)
        limited.log("k", logging.WARNING, "boom")
        assert "49 similar messages suppressed" in caplog.records[-1].getMessage()

    def test_invalid_records_are_logged_once(
        self, fake: FakeInflux, make_settings: Callable[..., Any], caplog: Any
    ) -> None:
        caplog.set_level(logging.WARNING, logger="sluicebox.validation")
        settings = make_settings(url=fake.url, validation={"on_invalid": "drop"})
        with InfluxClient(settings, registry=CollectorRegistry()) as client:
            for _ in range(20):
                client.write({"measurement": "m", "fields": {}})
            messages = [r.getMessage() for r in caplog.records]
            assert len([m for m in messages if "invalid record" in m]) == 1
        # Closing reports what was suppressed.
        last = caplog.records[-1].getMessage()
        assert "invalid record" in last
        assert "(19 similar messages suppressed)" in last


class TestProfiling:
    def test_profile_report(self, client: InfluxClient, tmp_path: Any) -> None:
        path = tmp_path / "write.prof"
        with client.profile(path, memory=True) as report:
            client.write([{"measurement": "m", "fields": {"v": float(i)}} for i in range(1000)]).result(
                timeout=5
            )
        assert path.exists()
        assert report.wall_seconds > 0
        assert report.peak_memory is not None
        assert report.peak_memory > 0
        summary = report.summary(limit=5)
        assert "serialize" in summary
        assert "wall time" in summary

    def test_stage_stats(self, client: InfluxClient) -> None:
        client.write([{"measurement": "m", "fields": {"v": 1.0}}]).result(timeout=5)
        stages = client.stats().stages
        assert stages["request"].count == 1
        assert stages["batch"].max >= stages["request"].max

    def test_profile_helper_without_client(self) -> None:
        with profile() as report:
            sum(range(1000))
        assert report.stats is not None


class TestAsync:
    def test_write_query_flush(self, fake: FakeInflux, make_settings: Callable[..., Any]) -> None:
        async def scenario() -> tuple[int, int]:
            async with AsyncInfluxClient(make_settings(url=fake.url), registry=CollectorRegistry()) as client:
                future = await client.write({"measurement": "m", "fields": {"v": 1.0}})
                result = await future
                big = await client.write(
                    [{"measurement": "m", "fields": {"v": float(i)}} for i in range(5000)]
                )
                await client.flush()
                assert big.done()
                with pytest.raises(ValidationError):
                    await client.write({"measurement": "m", "fields": {"v": "x"}})
                return result.points, client.stats().write.points_written

        assert asyncio.run(scenario()) == (1, 5001)

    def test_async_backpressure_does_not_block_the_loop(
        self, fake: FakeInflux, make_settings: Callable[..., Any]
    ) -> None:
        fake.responder = lambda _: Reply(delay=0.05)
        settings = make_settings(
            url=fake.url,
            write={"max_pending_bytes": 2048, "max_batch_bytes": 1024, "batch_size": 5, "concurrency": 1},
        )

        async def scenario() -> int:
            ticks = 0

            async def ticker() -> None:
                nonlocal ticks
                while True:
                    await asyncio.sleep(0.01)
                    ticks += 1

            task = asyncio.create_task(ticker())
            async with AsyncInfluxClient(settings, registry=CollectorRegistry()) as client:
                for i in range(40):
                    await client.write(
                        [{"measurement": "m", "fields": {"v": float(j)}} for j in range(i * 5, i * 5 + 5)]
                    )
                await client.flush()
            task.cancel()
            return ticks

        assert asyncio.run(scenario()) > 5  # the loop kept running while writers waited for space


class TestQueryHelpers:
    def test_flux_params_escaping(self) -> None:
        text = flux_params(
            {
                "s": 'q"uote ${x} back\\slash\nnl',
                "i": -3,
                "f": 1e300,
                "b": True,
                "t": datetime(2026, 1, 1, 1, tzinfo=UTC),
                "d": timedelta(minutes=-5),
                "a": ["x", 1],
                "r": {"k": 1},
            }
        )
        assert text == (
            'option params = {s: "q\\"uote \\${x} back\\\\slash\\nnl", i: -3, f: float(v: "1e+300"), b: true, '
            't: 2026-01-01T01:00:00.000000000Z, d: -300000000us, a: ["x", 1], r: {k: 1}}\n'
        )

    @pytest.mark.parametrize(
        "bad", [{"bad key": 1}, {"x": float("nan")}, {"x": datetime(2026, 1, 1)}, {"x": object()}]
    )
    def test_flux_params_rejects(self, bad: dict[str, Any]) -> None:
        with pytest.raises(ValueError, match="Flux"):
            flux_params(bad)

    def test_query_result_from_records(self) -> None:
        result = QueryResult(records=[{"a": 1, "b": "x"}, {"a": 2, "c": True}], query="q", language="flux")
        assert len(result) == 2
        assert result.columns == ["a", "b", "c"]
        assert result.to_pandas().shape == (2, 3)
        assert result.to_arrow().num_rows == 2
        assert list(result) == result.to_dicts()

    def test_query_result_from_arrow(self) -> None:
        import pyarrow as pa

        table = pa.table({"time": pa.array([0], pa.timestamp("ns", "UTC")), "v": [1.5]})
        result = QueryResult(table=table, query="q", language="sql")
        assert result.to_dicts() == [{"time": datetime(1970, 1, 1, tzinfo=UTC), "v": 1.5}]
        with pytest.raises(ValueError, match="exactly one"):
            QueryResult()

    @requires_polars
    def test_query_result_to_polars(self) -> None:
        import pyarrow as pa

        records = QueryResult(records=[{"a": 1, "b": "x"}, {"a": 2, "c": True}], query="q", language="flux")
        assert records.to_polars().shape == (2, 3)
        table = QueryResult(table=pa.table({"v": [1.5]}), query="q", language="sql")
        assert table.to_polars()["v"].to_list() == [1.5]
        assert QueryResult(records=[], query="q", language="flux").to_polars().shape == (0, 0)


class TestUsability:
    def test_json_formatter_includes_extra_and_context(self) -> None:
        logger = logging.getLogger("sluicebox.test.json")
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        try:
            logger.warning("hi", extra={"request_id": "r-1", "influx": {"points": 3, "status": None}})
        finally:
            logger.removeHandler(handler)
        payload = json.loads(stream.getvalue())
        assert payload["request_id"] == "r-1"
        assert payload["points"] == 3
        assert "status" not in payload  # empty context values are left out

    def test_configure_logging_keywords(self) -> None:
        stream = io.StringIO()
        logger = configure_logging(level="DEBUG", format="json", stream=stream)
        try:
            logging.getLogger("sluicebox.test").debug("hello")
            assert json.loads(stream.getvalue())["message"] == "hello"
            assert logger.level == logging.DEBUG
        finally:
            configure_logging(LoggingConfig(configure=True), stream=io.StringIO())
            logger.propagate = True
            for handler in list(logger.handlers):
                if not isinstance(handler, logging.NullHandler):
                    logger.removeHandler(handler)
            logger.setLevel(logging.NOTSET)

    def test_rate_limited_log_flush_reports_suppressed(self, caplog: Any) -> None:
        caplog.set_level(logging.INFO, logger="sluicebox.test.rate")
        limiter = RateLimitedLog(logging.getLogger("sluicebox.test.rate"), interval=60)
        for i in range(5):
            limiter.log("k", logging.INFO, "event %d", i)
        limiter.flush()
        assert [r.getMessage() for r in caplog.records] == [
            "event 0",
            "event 4 (4 similar messages suppressed)",
        ]

    def test_blocking_calls_on_an_event_loop_warn(
        self, fake: FakeInflux, make_settings: Callable[..., Any], caplog: Any
    ) -> None:
        caplog.set_level(logging.WARNING, logger="sluicebox.client")

        async def scenario() -> None:
            with InfluxClient(make_settings(url=fake.url), registry=CollectorRegistry()) as client:
                client.write({"measurement": "m", "fields": {"v": 1.0}}).result(timeout=5)

        asyncio.run(scenario())
        assert "WriteFuture.result() blocks the running asyncio event loop" in caplog.text
        assert "InfluxClient.close() blocks the running asyncio event loop" in caplog.text

    def test_async_single_writes_yield_to_the_loop(
        self, fake: FakeInflux, make_settings: Callable[..., Any]
    ) -> None:
        async def scenario() -> int:
            ticks = 0

            async def ticker() -> None:
                nonlocal ticks
                while True:
                    await asyncio.sleep(0)
                    ticks += 1

            async with AsyncInfluxClient(make_settings(url=fake.url), registry=CollectorRegistry()) as client:
                task = asyncio.create_task(ticker())
                await asyncio.sleep(0)
                for i in range(2000):
                    await client.write({"measurement": "m", "fields": {"v": float(i)}})
                task.cancel()
            return ticks

        assert asyncio.run(scenario()) >= 2000 // 32 - 1

    def test_to_line_protocol_shows_what_would_be_sent(
        self, fake: FakeInflux, make_settings: Callable[..., Any]
    ) -> None:
        settings = make_settings(url=fake.url, tags={"static": {"env": "dev"}})
        with InfluxClient(settings, registry=CollectorRegistry()) as client:
            lines = client.to_line_protocol(
                {"measurement": "m", "fields": {"v": 1.0}, "time": 5}, tags={"a": "b"}
            )
            assert lines == ["m,a=b,env=dev v=1.0 5"]
            assert fake.write_requests == []

    def test_invalid_precision(self, client: InfluxClient) -> None:
        with pytest.raises(ValueError, match="precision must be one of"):
            client.write({"measurement": "m", "fields": {"v": 1.0}}, precision="seconds")  # type: ignore[arg-type]

    def test_validation_drops_count_in_stats(
        self, fake: FakeInflux, make_settings: Callable[..., Any]
    ) -> None:
        settings = make_settings(url=fake.url, validation={"on_invalid": "drop"})
        with InfluxClient(settings, registry=CollectorRegistry()) as client:
            result = client.write(
                [{"measurement": "m", "fields": {"v": 1.0}}, {"measurement": "m", "fields": {}}]
            ).result(timeout=5)
            assert result.dropped == 1
            assert [error.code for error in result.rejected] == ["no_fields"]
            assert client.stats().write.points_dropped == 1


class TestQueryUsability:
    def test_empty_influxdb2_result_converts(self) -> None:
        result = QueryResult(records=[], query="q", language="flux")
        assert result.to_pandas().shape == (0, 0)
        assert result.to_arrow().num_rows == 0

    def test_datetime_sql_parameters_are_sent_as_rfc3339(self) -> None:
        from sluicebox.query import _sql_parameter

        moment = datetime(2026, 1, 1, 2, tzinfo=timezone(timedelta(hours=2)))
        assert _sql_parameter(moment) == "2026-01-01T00:00:00Z"
        assert _sql_parameter(5) == 5
        with pytest.raises(ValueError, match="timezone-aware"):
            _sql_parameter(datetime(2026, 1, 1))

    def test_query_language_hints(self) -> None:
        from sluicebox.client import _hint_language
        from sluicebox.exceptions import QueryError

        flux_on_v3 = QueryError("parse error")
        _hint_language(flux_on_v3, 'from(bucket: "b") |> range(start: -1h)', "sql")
        assert "looks like Flux" in flux_on_v3.__notes__[0]
        sql_on_v2 = QueryError("error @1:1")
        _hint_language(sql_on_v2, "SELECT * FROM cpu", "flux")
        assert "looks like SQL" in sql_on_v2.__notes__[0]

    @pytest.mark.parametrize(
        ("status", "kind"), [(404, "QueryError"), (500, "QueryError"), (503, "ServerError")]
    )
    def test_influxdb2_query_errors(self, status: int, kind: str) -> None:
        from sluicebox.query import _map_v2_error

        class ApiError(Exception):
            def __init__(self) -> None:
                super().__init__("boom")
                self.status = status
                self.message = "boom"

        assert type(_map_v2_error(ApiError(), "q")).__name__ == kind
