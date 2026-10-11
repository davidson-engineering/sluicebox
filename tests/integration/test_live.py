"""End-to-end tests against real InfluxDB 2 and InfluxDB 3 servers (see docker-compose.yml).

Every test runs against both servers; they are skipped when the servers are not running.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

import polars as pl
import pytest
from prometheus_client import CollectorRegistry

from sluicebox import (
    AsyncInfluxClient,
    AuthenticationError,
    ConfigurationError,
    InfluxClient,
    NotFoundError,
    PartialWriteError,
    Point,
    QueryError,
    ServerError,
    Tag,
    ValidationError,
    WriteError,
    WriteFailure,
    load_settings,
    measurement,
)
from sluicebox._lineprotocol import Dialect, influxdb2_runs_on
from sluicebox.client import flux_string
from sluicebox.types import FieldType
from tests.servers import V2_BUCKET, V2_ORG, V2_TOKEN, V2_URL, V3_DATABASE, V3_TOKEN, V3_URL
from tests.unit.test_lineprotocol import RUN_ON

pytestmark = pytest.mark.integration

T0 = datetime(2026, 1, 1, tzinfo=UTC)
T0_NS = int(T0.timestamp()) * 10**9
US = 1000  # InfluxDB 2 results come back as datetimes (microsecond resolution): space points by 1 us


def ts(i: int) -> int:
    return T0_NS + i * US


def rows(client: InfluxClient, measurement_name: str) -> list[dict[str, Any]]:
    """All points of a measurement as {"time": ns, **tags, **fields}, ordered by time."""
    if client.settings.connection.version == 3:
        result = client.query(f'SELECT * FROM "{measurement_name}" ORDER BY time')
        out = []
        for row in result.to_dicts():
            stamp = row.pop("time")
            out.append({"time": _ns(stamp), **{k: v for k, v in row.items() if v is not None}})
        return out
    flux = (
        f"from(bucket: {flux_string(V2_BUCKET)}) |> range(start: 0) "
        f"|> filter(fn: (r) => r._measurement == {flux_string(measurement_name)}) "
        '|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value") |> group() '
        '|> sort(columns: ["_time"])'
    )
    out = []
    for row in client.query(flux).to_dicts():
        record = {
            k: v for k, v in row.items() if k not in ("result", "table", "_start", "_stop", "_measurement")
        }
        stamp = record.pop("_time")
        out.append({"time": _ns(stamp), **{k: v for k, v in record.items() if v is not None}})
    return out


def _ns(stamp: Any) -> int:
    if hasattr(stamp, "value"):  # pandas.Timestamp
        return int(stamp.value)
    return (stamp - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1) * 1000


TRICKY_TAGS = [
    "plain",
    "with space",
    "comma,separated",
    "equals=sign",
    r"back\slash",
    r"c:\dir\file",
    r"back\,comma",
    r"\leading",
    "üñíçødé 😀",
    "quote\"in'tag",
    "#hash",
]
TRICKY_STRINGS = ['say "hi"', "back\\slash", "trailing\\", "multi\nline", "tab\there", "😀", ""]


def test_ping(live_client: InfluxClient) -> None:
    info = live_client.ping()
    assert info.version
    assert info.latency > 0


def test_tricky_values_round_trip(live_client: InfluxClient, unique: str) -> None:
    """Escaping is server-specific (backslashes!); every value must come back unchanged."""
    records = [
        {
            "measurement": unique,
            "tags": {"t": tag},
            "fields": {"s": TRICKY_STRINGS[i % len(TRICKY_STRINGS)], "i": i},
            "time": ts(i),
        }
        for i, tag in enumerate(TRICKY_TAGS)
    ]
    assert live_client.write(records).result(timeout=30).points == len(records)
    stored = rows(live_client, unique)
    if live_client.settings.connection.version == 2:
        # InfluxDB 2 stores "\n" but its CSV query output turns it into "\r\n" (Go encoding/csv).
        for row in stored:
            if "s" in row:
                row["s"] = row["s"].replace("\r\n", "\n")
    assert [(r["time"], r["t"], r.get("s", ""), r["i"]) for r in stored] == [
        (rec["time"], rec["tags"]["t"], rec["fields"]["s"], rec["fields"]["i"]) for rec in records
    ]


@pytest.mark.parametrize("raw_lines", ["passthrough", "validate"])
def test_line_protocol_with_multiline_strings_replays(
    make_live_client: Any, unique: str, raw_lines: str
) -> None:
    """to_line_protocol() output (what WriteFailure.lines holds) can be written back as is."""
    client = make_live_client(write={"batch_size": 2}, validation={"raw_lines": raw_lines})
    records = [
        {"measurement": unique, "tags": {"t": "x"}, "fields": {"s": s, "i": i}, "time": ts(i)}
        for i, s in enumerate(TRICKY_STRINGS)
    ]
    text = "\n".join(client.to_line_protocol(records))
    assert client.write(text).result(timeout=30).points == len(records)
    stored = rows(client, unique)
    if client.settings.connection.version == 2:
        for row in stored:  # see test_tricky_values_round_trip
            if "s" in row:
                row["s"] = row["s"].replace("\r\n", "\n")
    assert [(r["time"], r.get("s", ""), r["i"]) for r in stored] == [
        (rec["time"], rec["fields"]["s"], rec["fields"]["i"]) for rec in records
    ]


def test_long_strings_come_back(live_client: InfluxClient, unique: str) -> None:
    """Strings up to the 1 MiB server limit can be read back, also through sync_schema()
    (Flux results are CSV, and Python's csv module refuses fields over 128 KiB by default)."""
    text = "é" * 150_000 + 'with "quotes"'
    # Before the look-back of other tests' sync_schema(), which older versions would fail.
    when = int(datetime(2001, 1, 1, tzinfo=UTC).timestamp()) * 10**9
    live_client.write({"measurement": unique, "fields": {"s": text}, "time": when}).result(timeout=30)
    assert rows(live_client, unique) == [{"time": when, "s": text}]
    assert live_client.sync_schema(lookback="10000d")[unique] == {"s": "string"}


def test_special_measurement_and_keys(live_client: InfluxClient, unique: str) -> None:
    # "=" in measurement names is rejected for InfluxDB 2 (stored but never queryable there).
    name = f"{unique} m,x=y" if live_client.settings.connection.version == 3 else f"{unique} m,x"
    live_client.write(
        {"measurement": name, "tags": {"tag key,=": "v"}, "fields": {"field key,=": 1.5}, "time": T0_NS}
    ).result(timeout=30)
    if live_client.settings.connection.version == 3:
        tables = live_client.query("SHOW TABLES").to_dicts()
        assert name in {t["table_name"] for t in tables}
    stored = rows(live_client, name)
    assert stored == [{"time": T0_NS, "tag key,=": "v", "field key,=": 1.5}]


def test_all_field_types(live_client: InfluxClient, unique: str) -> None:
    from sluicebox import UInt

    live_client.write(
        Point(unique)
        .field("f", -1.25e-7)
        .field("i", -(2**63))
        .field("u", UInt(2**64 - 1))
        .field("b", True)
        .field("s", "x")
        .time(T0)
    ).result(timeout=30)
    (row,) = rows(live_client, unique)
    assert row == {"time": T0_NS, "f": -1.25e-7, "i": -(2**63), "u": 2**64 - 1, "b": True, "s": "x"}


def test_precision(make_live_client: Any, unique: str) -> None:
    client = make_live_client(write={"precision": "s"})
    client.write([{"measurement": unique, "fields": {"v": 1.0}, "time": 1_767_225_600}]).result(timeout=30)
    client.write(
        {"measurement": unique, "fields": {"v": 2.0}, "time": T0 + timedelta(seconds=1, milliseconds=900)}
    ).result(timeout=30)
    assert [r["time"] for r in rows(client, unique)] == [1_767_225_600 * 10**9, 1_767_225_601 * 10**9]


def test_type_lock_stops_conflicts_before_the_server(live_client: InfluxClient, unique: str) -> None:
    live_client.write({"measurement": unique, "fields": {"v": 1}, "time": T0_NS}).result(timeout=30)
    with pytest.raises(ValidationError) as info:
        live_client.write({"measurement": unique, "fields": {"v": 1.5}, "time": ts(1)})
    assert info.value.code == "type_conflict"
    assert [r["v"] for r in rows(live_client, unique)] == [1]


def test_sync_schema_learns_server_types(make_live_client: Any, unique: str) -> None:
    writer = make_live_client()
    writer.write({"measurement": unique, "fields": {"count": 5, "ratio": 0.5}, "time": T0_NS}).result(
        timeout=30
    )
    fresh = make_live_client()
    schema = fresh.sync_schema(lookback="3650d")
    assert schema[unique] == {"count": "integer", "ratio": "float"}
    with pytest.raises(ValidationError, match="type conflict"):
        fresh.write({"measurement": unique, "fields": {"count": 1.5}})
    assert (
        fresh.write({"measurement": unique, "fields": {"ratio": 2}, "time": ts(1)}).result(timeout=30).points
        == 1
    )
    assert [r["ratio"] for r in rows(fresh, unique)] == [0.5, 2.0]


def test_server_rejection_is_attributed_to_the_failing_call(make_live_client: Any, unique: str) -> None:
    """With client-side locking off, the server's own type check rejects lines."""
    client = make_live_client(validation={"type_lock": False}, write={"flush_interval": 60})
    client.write({"measurement": unique, "fields": {"v": 1}, "time": T0_NS}).result(timeout=30)
    good = client.write([{"measurement": unique, "fields": {"v": 2}, "time": ts(1)}])
    bad = client.write(
        [
            {"measurement": unique, "fields": {"v": 3}, "time": ts(2)},
            {"measurement": unique, "fields": {"v": 3.5}, "time": ts(3)},
        ]
    )
    if client.settings.connection.version == 3:
        # InfluxDB 3 names the rejected line: only the failing call fails, at its 2nd line.
        assert good.result(timeout=30).points == 1
    else:
        # InfluxDB 2 reports only a count, so every call in the batch sees the partial write.
        with pytest.raises(PartialWriteError):
            good.result(timeout=30)
    with pytest.raises(PartialWriteError) as info:
        bad.result(timeout=30)
    error = info.value
    if client.settings.connection.version == 3:
        assert [e.line_number for e in error.line_errors] == [2]
        assert "3.5" in (error.line_errors[0].line or "")
        assert [r["v"] for r in rows(client, unique)] == [1, 2, 3]
    else:
        assert error.rejected == 1
        assert [r["v"] for r in rows(client, unique)] == [1, 2, 3]
    stats = client.stats().write
    assert stats.points_failed == 1


def test_a_line_the_server_cannot_parse_fails_alone(make_live_client: Any, unique: str) -> None:
    """Raw line protocol is sent unparsed by default, so the server finds its bad lines. InfluxDB 3
    writes the rest of the request; InfluxDB 2 stores none of it, so the client sends the rest
    again. Either way only the bad line fails: for its write() call, and on_error."""
    failures: list[WriteFailure] = []
    client = make_live_client(write={"flush_interval": 60}, on_error=failures.append)
    bad = f"{unique},t=x {ts(3)}"  # no field set
    first = client.write([f"{unique} v=1i {ts(1)}", f"{unique} v=2i {ts(2)}"])
    second = client.write([f"{unique} v=3i {ts(4)}", bad, f"{unique} v=4i {ts(5)}"])
    third = client.write(f"{unique} v=5i {ts(6)}")
    client.flush(timeout=30)  # the three calls share one request
    assert first.result(timeout=1).points == 2
    assert third.result(timeout=1).points == 1
    with pytest.raises(PartialWriteError) as info:
        second.result(timeout=1)
    assert [(e.line_number, e.line) for e in info.value.line_errors] == [(2, bad)]
    assert [r["v"] for r in rows(client, unique)] == [1, 2, 3, 4, 5]
    [failure] = failures
    assert (failure.lines, failure.retryable) == ([bad], False)
    assert isinstance(failure.error, PartialWriteError)
    assert [(e.line_number, e.line) for e in failure.error.line_errors] == [(1, bad)]
    stats = client.stats().write
    assert (stats.points_written, stats.points_failed) == (5, 1)


def test_influxdb2_lines_it_cannot_parse_fail_alone(
    server_version: int, make_live_client: Any, unique: str
) -> None:
    """InfluxDB 2 refuses a request with lines it cannot parse as a whole and quotes each of them:
    the client takes them out and sends the rest again. Lines that leave a string open run on
    into the next: two of them pair up and would be stored as one point, quoted by nobody, and
    the last hides the line after it from the server until it is gone (three requests here)."""
    if server_version != 2:
        pytest.skip("InfluxDB 2 behaviour")
    failures: list[WriteFailure] = []
    client = make_live_client(write={"flush_interval": 60}, on_error=failures.append)
    lines = [
        f"{unique} v=1i {ts(1)}",
        f"{unique},t=x {ts(2)}",  # no field set
        f'{unique} v=3i,s="two\nlines" {ts(3)}',  # valid: a string may hold a newline
        f"  {unique} v=NaN {ts(4)}",  # quoted without its leading spaces
        f'{unique} s="open {ts(5)}',  # closed by the quote in line 7: the server parses
        f"{unique} v=6i {ts(6)}",  # these three lines as one, without complaint
        f'{unique} s="open {ts(7)}',
        f"{unique} v=8i {ts(8)}",
        f'{unique} s="left open {ts(9)}',  # the server reads on to the end of the request
        f"{unique} v=10i {ts(10)}",
        f"{unique} v=11i {2**63 - 1}",  # beyond the time range InfluxDB 2 stores
        f"{unique} v=12i {ts(12)}",
    ]
    future = client.write(lines)
    other = client.write(f"{unique} v=13i {ts(13)}")
    with pytest.raises(PartialWriteError) as info:
        future.result(timeout=30)
    assert other.result(timeout=30).points == 1
    rejected = (2, 4, 5, 7, 9, 11)
    assert [(e.line_number, e.line) for e in info.value.line_errors] == [(n, lines[n - 1]) for n in rejected]
    reasons = [e.message for e in info.value.line_errors]
    assert reasons[:2] == ["invalid field format", "invalid number"]
    assert reasons[2:5] == [influxdb2_runs_on(lines[4])] * 3
    assert reasons[5].startswith("time outside range")
    assert [r["v"] for r in rows(client, unique)] == [1, 3, 6, 8, 10, 12, 13]
    [failure] = failures
    assert failure.lines == [lines[n - 1] for n in rejected]
    assert failure.retryable is False
    stats = client.stats().write
    assert (stats.points_written, stats.points_failed) == (7, 6)


def _raw_v2_write_to_v3(body: str) -> tuple[int, str]:
    """POST line protocol to InfluxDB 3's /api/v2/write without sluicebox: the status and answer."""
    url = f"{V3_URL}/api/v2/write?bucket={V3_DATABASE}&precision=ns"
    request = urllib.request.Request(
        url, data=body.encode(), method="POST", headers={"Authorization": f"Bearer {V3_TOKEN}"}
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return int(response.status), response.read().decode()
    except urllib.error.HTTPError as error:
        with error:
            return error.code, error.read().decode()


def test_influxdb3_v2_api_stores_nothing_of_a_refused_request(server_version: int, unique: str) -> None:
    """What isolating lines on InfluxDB 3's /api/v2/write relies on: one line it cannot parse, or
    that gives a column another type than its table has (stored or from an earlier line), and it
    stores none of the request, however large, naming the reason but not the line."""
    if server_version != 3:
        pytest.skip("InfluxDB 3 behaviour")
    assert _raw_v2_write_to_v3(f"{unique}_stored v=0i {ts(0)}")[0] == 204
    many = [f"{{m}} v={i}i {ts(i)}" for i in range(1, 20_001)]
    cases = {
        "no_fields": ([f"{{m}} v=1i {ts(1)}", f"{{m}},t=x {ts(2)}", f"{{m}} v=3i {ts(3)}"], "No fields"),
        "stored_type": ([f"{{m}} v=1i {ts(1)}", f"{{m}} v=2.5 {ts(2)}"], "field::integer, got"),
        "request_type": ([f"{{m}} v=1i {ts(1)}", f"{{m}} v=2.5 {ts(2)}"], "field::integer, got"),
        "tag_and_field": ([f"{{m}} v=1i {ts(1)}", f"{{m}},v=a x=1i {ts(2)}"], "got iox::column_type::tag"),
        "large": ([*many[:10_000], f"{{m}} v=1.5 {ts(0)}", *many[10_000:]], "field::integer, got"),
    }
    for name, (lines, reason) in cases.items():
        table = f"{unique}_stored" if name == "stored_type" else f"{unique}_{name}"
        status, answer = _raw_v2_write_to_v3("\n".join(line.format(m=table) for line in lines))
        assert status == 400, (name, answer)
        message = json.loads(answer)["message"]
        assert message.startswith("write buffer error: line protocol parse failed: "), (name, message)
        assert reason in message, (name, message)
    with InfluxClient(
        load_settings(
            None,
            env_file=None,
            token=V3_TOKEN,
            connection={"url": V3_URL, "version": 3, "database": V3_DATABASE},
        ),
        registry=CollectorRegistry(),
    ) as client:
        assert [r["v"] for r in rows(client, f"{unique}_stored")] == [0]
        tables = client.query("SHOW TABLES").to_dicts()
    assert not [
        t
        for t in tables
        if t["table_name"].startswith(f"{unique}_") and not t["table_name"].endswith("_stored")
    ]


def test_influxdb3_v2_api_refused_lines_fail_alone(
    make_live_client: Any, unique: str, server_version: int
) -> None:
    """InfluxDB 3 refuses a whole /api/v2/write request for one line, naming no line: the client
    sends the lines the reason can be about apart from the others, so only they fail. Lines of
    the type the server refused teach the client its type."""
    if server_version != 3:
        pytest.skip("InfluxDB 3 behaviour")
    failures: list[WriteFailure] = []
    client = make_live_client(write={"api": "v2", "flush_interval": 60}, on_error=failures.append)
    client.write(f"{unique} v=0i {ts(0)}").result(timeout=30)
    joined = f"{unique}_joined"
    first, second = f'{joined} s="a {ts(1)}', f'{joined} s=",n=1i {ts(3)}'  # one valid line together
    lines = [
        f"{unique} v=1i {ts(1)}",
        f"{unique},t=x {ts(2)}",  # no field set
        f"{unique} v=3.5 {ts(3)}",  # the server stores integers
        f"{unique} v=4i {ts(4)}",
        first,
        f"{joined} v=5i {ts(5)}",
        second,
        f"{unique},v=a x=6i {ts(6)}",  # a tag named like the field
        f"{unique} v=7.5 {ts(7)}",
        f"{unique} v=8i {ts(8)}",
    ]
    future = client.write(lines)
    other = client.write(f"{unique} v=9i {ts(9)}")
    client.flush(timeout=60)
    assert other.result(timeout=1).points == 1
    with pytest.raises(PartialWriteError) as info:
        future.result(timeout=1)
    rejected = (2, 3, 5, 7, 8, 9)
    assert [(e.line_number, e.line) for e in info.value.line_errors] == [(n, lines[n - 1]) for n in rejected]
    reasons = [e.message for e in info.value.line_errors]
    assert reasons[0] == "No fields were provided"
    assert "expected iox::column_type::field::integer, got iox::column_type::field::float" in reasons[1]
    assert "got iox::column_type::tag" in reasons[4]
    assert [r["v"] for r in rows(client, unique)] == [0, 1, 4, 8, 9]
    assert rows(client, joined) == [{"time": ts(5), "v": 5}]
    [failure] = failures
    assert failure.lines == [lines[n - 1] for n in rejected]
    assert client.stats().write.points_failed == 6
    assert client.locked_types(unique) == {"v": FieldType.INTEGER}


def test_influxdb3_v2_api_isolates_in_a_large_batch(
    make_live_client: Any, unique: str, server_version: int
) -> None:
    """A fresh process writing floats to an integer field: 5,000 such lines among 25,000 cost two
    more requests, not a request per line or halving 25,000 lines for each."""
    if server_version != 3:
        pytest.skip("InfluxDB 3 behaviour")
    make_live_client().write({"measurement": unique, "fields": {"v": 0}, "time": ts(0)}).result(timeout=30)
    client = make_live_client(write={"api": "v2", "flush_interval": 60})
    lines = [f"{unique} v={i}i {ts(i)}" if i % 5 else f"{unique} v={i}.5 {ts(i)}" for i in range(1, 25_001)]
    started = time.monotonic()
    with pytest.raises(PartialWriteError) as info:
        client.write(lines).result(timeout=60)
    elapsed = time.monotonic() - started
    assert len(info.value.line_errors) == 5_000
    count = client.query(f'SELECT count(*) AS n FROM "{unique}"').to_dicts()
    assert count == [{"n": 20_001}]
    assert client.stats().write.points_written == 20_000
    assert elapsed < 10, elapsed  # three requests; one the server stores takes it up to a second


def test_client_reads_lines_as_influxdb2_does(server_version: int, unique: str) -> None:
    """influxdb2_runs_on() knows which lines InfluxDB 2 reads the next line into: each is written
    before a line without fields, which the server quotes on its own unless joined to it."""
    if server_version != 2:
        pytest.skip("InfluxDB 2 behaviour")
    after = f"{unique}_after"
    for line, runs_on in RUN_ON:
        status, answer = _raw_write(2, line.replace("m", unique, 1) + "\n" + after)
        assert status == 400, (line, answer)
        joined = f"\n{after}'" in json.loads(answer)["message"]
        assert joined is runs_on, (line, answer)


def test_waiting_callers_share_requests(make_live_client: Any, unique: str) -> None:
    """Many threads waiting on their own writes: requests carry many points, outcomes stay per call."""
    client = make_live_client(
        validation={"type_lock": False}, write={"flush_interval": 1.0, "concurrency": 2}
    )
    client.write({"measurement": unique, "fields": {"v": 0}, "time": ts(0)}).result(timeout=30)
    threads_count, rounds, bad = 40, 3, (7, 1)  # one call writes a float to the integer field
    outcomes: dict[tuple[int, int], BaseException | None] = {}

    def caller(t: int) -> None:
        for r in range(rounds):
            i = 1 + t * rounds + r
            record = {"measurement": unique, "fields": {"v": 0.5 if (t, r) == bad else i}, "time": ts(i)}
            outcomes[t, r] = client.write(record).exception(timeout=60)

    threads = [threading.Thread(target=caller, args=(t,)) for t in range(threads_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert isinstance(outcomes.pop(bad), PartialWriteError)
    if client.settings.connection.version == 3:
        # InfluxDB 3 names the rejected line: only the failing call fails.
        assert set(outcomes.values()) == {None}
    else:
        # InfluxDB 2 reports only a count: the other calls in that request see the partial write.
        assert all(error is None or isinstance(error, PartialWriteError) for error in outcomes.values())
    total = 1 + threads_count * rounds
    assert len(rows(client, unique)) == total - 1
    stats = client.stats().write
    assert (stats.points_written, stats.points_failed) == (total - 1, 1)
    if client.settings.connection.version == 3:
        # Each request waits for a WAL flush (~1 s), so both senders stay busy and queued writes
        # must share requests. InfluxDB 2 answers in milliseconds: how many writes queue up
        # meanwhile depends on thread scheduling (12 requests here, 33-43 on CI runners), so
        # request sharing is checked against a slow fake server in test_engine_coalescing.
        assert stats.batches_written + stats.batches_failed <= total // 4


def test_wrong_token(make_live_client: Any, unique: str) -> None:
    client = make_live_client(token="definitely-wrong")
    with pytest.raises(AuthenticationError):
        client.write({"measurement": unique, "fields": {"v": 1.0}}).result(timeout=30)
    with pytest.raises(AuthenticationError):
        client.query("SELECT 1" if client.settings.connection.version == 3 else "buckets() |> limit(n: 1)")
    client.flush()  # the failure was raised by result(): not reported again
    client.write({"measurement": unique, "fields": {"v": 2.0}})  # nobody waits for this one
    with pytest.raises(WriteError, match="failed"):
        client.flush()


def test_missing_bucket_or_database(make_live_client: Any, unique: str) -> None:
    client = make_live_client()
    if client.settings.connection.version == 2:
        with pytest.raises(NotFoundError, match="not found"):
            client.write({"measurement": unique, "fields": {"v": 1.0}}, database="no-such-bucket").result(
                timeout=30
            )
        missing = make_live_client(connection={"database": "no-such-bucket"})
        with pytest.raises(NotFoundError, match="no-such-bucket"):
            missing.check()  # caught at startup, before any data is written
    else:
        with pytest.raises(QueryError, match="database not found") as info:
            client.query("SELECT 1", database="no_such_database")
        assert info.value.status == 404


def test_query_parameters_and_errors(live_client: InfluxClient, unique: str) -> None:
    live_client.write(
        [
            {"measurement": unique, "tags": {"host": h}, "fields": {"v": float(i)}, "time": ts(i)}
            for i, h in enumerate("abca")
        ]
    ).result(timeout=30)
    if live_client.settings.connection.version == 3:
        result = live_client.query(
            f'SELECT v FROM "{unique}" WHERE host = $host ORDER BY time', params={"host": "a"}
        )
        assert [r["v"] for r in result] == [0.0, 3.0]
        influxql = live_client.query(f"SELECT v FROM \"{unique}\" WHERE host = 'b'", language="influxql")
        assert [r["v"] for r in influxql] == [1.0]
        with pytest.raises(QueryError):
            live_client.query("SELEC nonsense")
    else:
        flux = (
            "from(bucket: params.bucket) |> range(start: 0) "
            f"|> filter(fn: (r) => r._measurement == {flux_string(unique)}"
            ' and r.host == params.host) |> sort(columns: ["_time"])'
        )
        result = live_client.query(flux, params={"bucket": V2_BUCKET, "host": "a"})
        assert [r["_value"] for r in result] == [0.0, 3.0]
        # Flux wants imports first: the params option goes after them.
        imported = live_client.query(
            f'// comment\nimport "strings"\n{flux.replace("params.host", "strings.toLower(v: params.host)")}',
            params={"bucket": V2_BUCKET, "host": "A"},
        )
        assert [r["_value"] for r in imported] == [0.0, 3.0]
        with pytest.raises(QueryError):
            live_client.query("from(bucket:")


def test_query_stream_and_conversions(make_live_client: Any, unique: str) -> None:
    client = make_live_client(query={"chunk_size": 1000})
    n = 5000
    client.write(
        [
            {"measurement": unique, "tags": {"k": str(i % 7)}, "fields": {"v": float(i)}, "time": ts(i)}
            for i in range(n)
        ]
    ).result(timeout=60)
    query = (
        f'SELECT v FROM "{unique}"'
        if client.settings.connection.version == 3
        else f"from(bucket: {flux_string(V2_BUCKET)}) |> range(start: 0) "
        f"|> filter(fn: (r) => r._measurement == {flux_string(unique)})"
    )
    chunks = list(client.query_stream(query))
    assert sum(len(c) for c in chunks) == n
    assert len(chunks) > 1 or client.settings.connection.version == 3
    full = client.query(query)
    assert full.to_polars().height == n
    assert len(full.to_pandas()) == n
    assert full.to_arrow().num_rows == n


def test_dataframe_round_trip(live_client: InfluxClient, unique: str) -> None:
    frame = pl.DataFrame(
        {
            "host": ["a", "b", None],
            "temp": [21.5, None, 19.0],
            "count": [1, 2, 3],
            "ok": [True, False, True],
            "note": ["x", 'y "q"', None],
            "time": [T0 + timedelta(seconds=i) for i in range(3)],
        }
    )
    assert live_client.write(frame, measurement=unique, tag_columns=["host"]).result(timeout=30).points == 3
    stored = rows(live_client, unique)
    assert stored == [
        {"time": T0_NS, "host": "a", "temp": 21.5, "count": 1, "ok": True, "note": "x"},
        {"time": T0_NS + 10**9, "host": "b", "count": 2, "ok": False, "note": 'y "q"'},
        {"time": T0_NS + 2 * 10**9, "temp": 19.0, "count": 3, "ok": True},
    ]


def test_tag_injection_end_to_end(make_live_client: Any, unique: str) -> None:
    client = make_live_client(
        tags={
            "static": {"env": "test"},
            "rules": [
                {
                    "when": {"tags": {"device": r"^(?P<site>[a-z]+)-\d+$"}},
                    "set": {"site": "{site}"},
                },
                {"when": {"fields": {"temp": {"ge": 30}}}, "set": {"alert": "hot"}},
            ],
        }
    )
    client.write(
        [
            {"measurement": unique, "tags": {"device": "lon-1"}, "fields": {"temp": 35.0}, "time": T0_NS},
            {"measurement": unique, "tags": {"device": "x"}, "fields": {"temp": 20.0}, "time": ts(1)},
        ]
    ).result(timeout=30)
    stored = rows(client, unique)
    assert stored == [
        {"time": T0_NS, "device": "lon-1", "env": "test", "site": "lon", "alert": "hot", "temp": 35.0},
        {"time": ts(1), "device": "x", "env": "test", "temp": 20.0},
    ]


def test_models(live_client: InfluxClient, unique: str) -> None:
    @measurement(unique)
    @dataclass
    class Reading:
        sensor: Annotated[str, Tag]
        value: float
        time: datetime

    live_client.write([Reading("s1", 1, T0), Reading("s2", 2.5, T0 + timedelta(seconds=1))]).result(
        timeout=30
    )
    assert rows(live_client, unique) == [
        {"time": T0_NS, "sensor": "s1", "value": 1.0},
        {"time": T0_NS + 10**9, "sensor": "s2", "value": 2.5},
    ]


def test_large_write(make_live_client: Any, unique: str) -> None:
    client = make_live_client(write={"batch_size": 5000})
    n = 100_000
    records = [
        {"measurement": unique, "tags": {"h": f"h{i % 50}"}, "fields": {"v": float(i)}, "time": ts(i)}
        for i in range(n)
    ]
    result = client.write(records).result(timeout=120)
    assert result.points == n
    if client.settings.connection.version == 3:
        count = client.query(f'SELECT count(*) AS n FROM "{unique}"').to_dicts()[0]["n"]
    else:
        flux = (
            f"from(bucket: {flux_string(V2_BUCKET)}) |> range(start: 0) "
            f"|> filter(fn: (r) => r._measurement == {flux_string(unique)}) |> group() |> count()"
        )
        count = client.query(flux).to_dicts()[0]["_value"]
    assert count == n
    stats = client.stats().write
    assert stats.points_written == n
    assert stats.batches_written >= n // 5000


def test_async_client(server_version: int, unique: str) -> None:
    from tests.servers import server_client

    sync = server_client(server_version, __import__("prometheus_client").CollectorRegistry())

    async def scenario() -> list[dict[str, Any]]:
        async with AsyncInfluxClient.wrap(sync) as client:
            future = await client.write(
                [{"measurement": unique, "fields": {"v": float(i)}, "time": ts(i)} for i in range(3)]
            )
            result = await future
            assert result.points == 3
            many = await client.write(
                [{"measurement": unique, "fields": {"v": float(i)}, "time": ts(i)} for i in range(3, 5003)]
            )
            await many
            await client.flush()
            if server_version == 3:
                query = f'SELECT count(*) AS n FROM "{unique}"'
                rows_ = (await client.query(query)).to_dicts()
                chunks = [chunk async for chunk in client.query_stream(f'SELECT v FROM "{unique}"')]
                assert sum(len(c) for c in chunks) == 5003
                return rows_
            flux = (
                f"from(bucket: {flux_string(V2_BUCKET)}) |> range(start: 0) "
                f"|> filter(fn: (r) => r._measurement == {flux_string(unique)}) |> group() |> count()"
            )
            return [{"n": r["_value"]} for r in (await client.query(flux)).to_dicts()]

    assert asyncio.run(scenario()) == [{"n": 5003}]


def _raw_write(server_version: int, body: str) -> tuple[int, str]:
    """POST line protocol without sluicebox (no client-side validation): the status and answer."""
    if server_version == 3:
        url = f"{V3_URL}/api/v3/write_lp?db={V3_DATABASE}&precision=nanosecond"
        auth = f"Bearer {V3_TOKEN}"
    else:
        url = f"{V2_URL}/api/v2/write?org={V2_ORG}&bucket={V2_BUCKET}&precision=ns"
        auth = f"Token {V2_TOKEN}"
    request = urllib.request.Request(url, data=body.encode(), method="POST", headers={"Authorization": auth})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return int(response.status), response.read().decode()
    except urllib.error.HTTPError as error:
        with error:
            return error.code, error.read().decode()


def test_server_side_raw_write_then_query(server_version: int, unique: str, make_live_client: Any) -> None:
    """Data written by another tool is readable, and the server's types can be synced."""
    assert _raw_write(server_version, f"{unique},host=raw v=7i {T0_NS}")[0] == 204
    client = make_live_client()
    assert rows(client, unique) == [{"time": T0_NS, "host": "raw", "v": 7}]


def test_client_time_range_is_the_servers(server_version: int, unique: str) -> None:
    """The client rejects exactly the timestamps the server refuses (on InfluxDB 2, one such
    line fails its whole request, so a single bad point would take a batch down)."""
    dialect = Dialect.for_version(server_version)
    for stamp in (-(2**63), -(2**63) + 1, -(2**63) + 2, 2**63 - 2, 2**63 - 1):
        storable = dialect.min_time_ns <= stamp <= dialect.max_time_ns
        status, _ = _raw_write(server_version, f"{unique} v=1 {stamp}")
        assert (status == 204) == storable, (stamp, status)


# ---------------------------------------------------------------------------------------------
# First-time-user mistakes, end to end
# ---------------------------------------------------------------------------------------------


def test_check_passes_for_correct_settings(live_client: InfluxClient) -> None:
    info = live_client.check()
    assert info.major == live_client.settings.connection.version
    assert info.version is not None
    assert not info.version.startswith("v")


def test_influxdb3_settings_pointed_at_influxdb2_fail_loudly(unique: str) -> None:
    """InfluxDB 2 answers /api/v3/write_lp with its UI and HTTP 200: that must not look like success."""
    from tests.servers import reachable

    if not reachable(V2_URL):
        pytest.skip("InfluxDB 2 test server not running")
    settings = load_settings(
        None, env_file=None, token=V3_TOKEN, connection={"url": V2_URL, "version": 3, "database": "x"}
    )
    with InfluxClient(settings, registry=CollectorRegistry()) as client:
        with pytest.raises(
            ConfigurationError, match=r"runs InfluxDB 2\.\d+\.\d+: set connection\.version = 2"
        ):
            client.check()
        with pytest.raises(ServerError, match="web page"):
            client.write({"measurement": unique, "fields": {"v": 1.0}}).result(timeout=30)
        assert client.stats().write.points_written == 0


def test_server_field_types_are_learned_from_conflicts(make_live_client: Any, unique: str) -> None:
    """A fresh process whose first value has the wrong type recovers instead of rejecting everything."""
    make_live_client().write({"measurement": unique, "fields": {"v": 1.5}, "time": ts(0)}).result(timeout=30)
    fresh = make_live_client()
    with pytest.raises(ServerError):  # PartialWriteError (v3) / UnprocessableEntityError (v2)
        fresh.write({"measurement": unique, "fields": {"v": "oops"}, "time": ts(1)}).result(timeout=30)
    assert fresh.locked_types(unique) == {"v": FieldType.FLOAT}
    fresh.write({"measurement": unique, "fields": {"v": 2.5}, "time": ts(2)}).result(timeout=30)
    assert [row["v"] for row in rows(fresh, unique)] == [1.5, 2.5]


def test_sql_datetime_parameters(live_client: InfluxClient, unique: str) -> None:
    if live_client.settings.connection.version != 3:
        pytest.skip("SQL is InfluxDB 3 only")
    live_client.write(
        [{"measurement": unique, "fields": {"v": float(i)}, "time": ts(i)} for i in range(5)]
    ).result(timeout=30)
    result = live_client.query(
        f'SELECT v FROM "{unique}" WHERE time >= $start ORDER BY time',
        params={"start": datetime.fromtimestamp(ts(3) / 1e9, UTC)},
    )
    assert [row["v"] for row in result] == [3.0, 4.0]


def test_empty_results_convert_to_frames(live_client: InfluxClient, unique: str) -> None:
    if live_client.settings.connection.version == 3:
        live_client.write({"measurement": unique, "fields": {"v": 1.0}, "time": ts(0)}).result(timeout=30)
        result = live_client.query(f'SELECT * FROM "{unique}" WHERE v > 100')
    else:
        result = live_client.query(
            f"from(bucket: {flux_string(V2_BUCKET)}) |> range(start: -1m) "
            f"|> filter(fn: (r) => r._measurement == {flux_string(unique)})"
        )
    assert result.to_polars().height == 0
    assert len(result.to_pandas()) == 0


def test_exit_without_close_delivers_the_data(server_version: int, unique: str) -> None:
    from tests.servers import reachable

    url, token, database = (
        (V3_URL, V3_TOKEN, V3_DATABASE) if server_version == 3 else (V2_URL, V2_TOKEN, V2_BUCKET)
    )
    if not reachable(url):
        pytest.skip(f"InfluxDB {server_version} test server not running")
    connection = {"url": url, "version": server_version, "database": database, "org": V2_ORG}
    if server_version == 3:
        del connection["org"]
    script = (
        "from sluicebox import InfluxClient, load_settings\n"
        f"settings = load_settings(None, env_file=None, token={token!r}, connection={connection!r})\n"
        "client = InfluxClient(settings)\n"
        f"client.write([{{'measurement': {unique!r}, 'fields': {{'v': float(i)}}, 'time': {T0_NS} + i * 1000}}"
        " for i in range(50)])\n"
    )
    started = time.monotonic()
    done = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=120, check=False
    )
    assert done.returncode == 0, done.stderr
    assert time.monotonic() - started < 20
    with InfluxClient(load_settings(None, env_file=None, token=token, connection=connection)) as reader:
        assert len(rows(reader, unique)) == 50
