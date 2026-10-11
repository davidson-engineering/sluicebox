"""Lines InfluxDB 2 cannot parse fail alone: the rest of their request is sent again and written."""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

import pytest

from sluicebox import (
    BadRequestError,
    ClientClosedError,
    InfluxClient,
    PartialWriteError,
    WriteError,
    WriteFailure,
)
from sluicebox._engine import _ISOLATION_REQUESTS, WriteEngine, _Batch, _Failure, _runs, _unparseable_lines
from sluicebox.exceptions import LineError
from sluicebox.futures import WriteFuture

from .fake_server import FakeInflux, Recorded, Reply
from .test_lineprotocol import scan_lines

if TYPE_CHECKING:
    from collections.abc import Callable

T = 1_700_000_000_000_000_000
BAD = f"m,t=x {T + 99}"  # no field set


def good(i: int) -> str:
    return f"m v={i}i {T + i}"


def wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.005)


def reason(line: str) -> str | None:
    """Why the fake InfluxDB 2 cannot parse ``line`` (None if it can)."""
    if line.count('"') % 2:
        return "unbalanced quotes"
    fields = line.partition(" ")[2].partition(" ")[0]
    return None if "=" in fields else "invalid field format"


class InfluxDB2:
    """The fake server as an InfluxDB 2 write endpoint: a request with lines it cannot parse is
    refused as a whole with HTTP 400, quoting each of them (through ``quote``)."""

    def __init__(self, fake: FakeInflux, quote: Callable[[str], str] = lambda line: line) -> None:
        self.stored: list[str] = []
        self.quote = quote
        self._lock = threading.Lock()
        fake.responder = self

    def __call__(self, request: Recorded) -> Reply:
        lines = scan_lines("\n".join(request.lines))
        errors = [f"unable to parse '{self.quote(line)}': {why}" for line in lines if (why := reason(line))]
        if errors:
            return Reply(400, {"code": "invalid", "message": "\n".join(errors)})
        with self._lock:
            self.stored += lines
        return Reply()


def v2_client(client_for: Callable[..., InfluxClient], **kwargs: object) -> InfluxClient:
    """A client whose write() calls wait in one open batch until flushed."""
    return client_for(version=2, write={"flush_interval": 60}, **kwargs)


class TestIsolation:
    def test_only_the_unparseable_lines_fail(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        server = InfluxDB2(fake)
        failures: list[WriteFailure] = []
        client = v2_client(client_for, on_error=failures.append)
        first = client.write([good(0), good(1)])
        second = client.write([good(2), BAD, good(3)])
        third = client.write([good(4)])
        fourth = client.write([f"  {BAD}", good(5)])  # leading whitespace is not quoted
        client.flush(timeout=5)
        assert first.result(timeout=1).points == 2
        assert third.result(timeout=1).points == 1
        with pytest.raises(PartialWriteError) as info:
            second.result(timeout=1)
        assert [(e.line_number, e.message, e.line) for e in info.value.line_errors] == [
            (2, "invalid field format", BAD)
        ]
        assert info.value.status == 400
        assert (
            str(info.value) == "HTTP 400: 1 of 3 points rejected by the server: line 2: invalid field format"
        )
        with pytest.raises(PartialWriteError) as info:
            fourth.result(timeout=1)
        assert [(e.line_number, e.message, e.line) for e in info.value.line_errors] == [
            (1, "invalid field format", f"  {BAD}"),
        ]
        assert server.stored == [good(i) for i in range(6)]
        # The batch, then everything but the two lines the server quoted.
        assert [len(r.lines) for r in fake.write_requests] == [8, 6]
        [failure] = failures
        assert failure.lines == [BAD, f"  {BAD}"]
        assert failure.retryable is False
        assert isinstance(failure.error, PartialWriteError)
        assert [(e.line_number, e.line) for e in failure.error.line_errors] == [
            (1, BAD),
            (2, f"  {BAD}"),
        ]
        write = client.stats().write
        assert (write.points_written, write.points_failed) == (6, 2)
        assert (write.batches_written, write.batches_failed, write.buffered_bytes) == (0, 1, 0)

    def test_lines_quoted_in_another_form_are_found_by_halving(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        server = InfluxDB2(fake, quote=str.upper)
        client = v2_client(client_for)
        lines = [good(i) for i in range(16)]
        lines[11] = BAD
        with pytest.raises(PartialWriteError) as info:
            client.write(lines).result(timeout=5)
        assert [(e.line_number, e.message, e.line) for e in info.value.line_errors] == [
            (12, "invalid field format", BAD)
        ]
        assert sorted(server.stored) == sorted(good(i) for i in range(16) if i != 11)
        assert len(fake.write_requests) == 1 + 2 * 4  # halved from 16 lines down to 1
        assert client.stats().write.points_written == 15

    def test_isolation_sends_a_bounded_number_of_requests(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        server = InfluxDB2(fake, quote=str.upper)
        client = v2_client(client_for)
        future = client.write([f"m,t={i} {T + i}" for i in range(200)])  # none can be parsed
        error = future.exception(timeout=10)
        assert len(fake.write_requests) == 1 + _ISOLATION_REQUESTS
        assert isinstance(error, BadRequestError)
        assert any("at most 64 extra requests per batch" in note for note in error.__notes__)
        assert len(future.errors) == 1
        assert server.stored == []
        write = client.stats().write
        assert (write.points_written, write.points_failed) == (0, 200)

    def test_a_large_batch_is_isolated_with_one_more_request(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        server = InfluxDB2(fake)
        client = v2_client(client_for)
        lines = [good(i) if i % 100 else f"m,t={i} {T + i}" for i in range(5000)]  # 50 bad lines
        with pytest.raises(PartialWriteError) as info:
            client.write(lines).result(timeout=10)
        assert [e.line_number for e in info.value.line_errors] == list(range(1, 5000, 100))
        assert len(server.stored) == 4950
        assert len(fake.write_requests) == 2

    def test_a_split_request_isolates_within_its_halves(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        server = InfluxDB2(fake)

        def limit(request: Recorded) -> Reply:
            if len(request.lines) > 4:
                return Reply(413, {"code": "request too large", "message": "max"})
            return server(request)

        fake.responder = limit
        client = v2_client(client_for)
        pad = "x" * 300  # proxies limit bodies to kilobytes at least; tiny 413s are not about size
        lines = [f'm s="{pad}" {T + i}' for i in range(8)]
        lines[5] = f'm,t="{pad}" {T + 5}'
        with pytest.raises(PartialWriteError) as info:
            client.write(lines).result(timeout=5)
        assert [e.line_number for e in info.value.line_errors] == [6]
        assert sorted(server.stored) == sorted(line for i, line in enumerate(lines) if i != 5)

    def test_nothing_is_sent_once_close_gave_up(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        """close() stops waiting for the requests in flight: their batches send nothing more."""
        server = InfluxDB2(fake, quote=str.upper)  # found by halving: many requests
        release = threading.Event()

        def hold_the_second(request: Recorded) -> Reply:
            if len(fake.write_requests) == 2:
                release.wait(10)
            return server(request)

        fake.responder = hold_the_second
        client = v2_client(client_for)
        future = client.write([f"m,t={i} {T + i}" for i in range(64)])
        client._engine.flush_nowait()
        wait_until(lambda: len(fake.write_requests) == 2)
        closed: list[BaseException] = []

        def close() -> None:
            try:
                client.close(timeout=0.1)
            except WriteError as error:
                closed.append(error)

        closer = threading.Thread(target=close)
        closer.start()
        wait_until(client._engine._stop.is_set)  # close() gave up on the request in flight
        release.set()  # its answer would have the sender split and resend
        closer.join(10)
        assert len(fake.write_requests) == 2
        assert closed
        assert isinstance(future.exception(timeout=1), ClientClosedError)


class TestNoResend:
    """Only requests the server stored nothing of, for lines it could not parse, are resent."""

    @pytest.mark.parametrize(
        ("version", "api", "status", "body"),
        [
            (2, None, 400, {"code": "invalid", "message": "writing requires points"}),
            (
                2,
                None,
                400,
                {"code": "invalid", "message": f"partial write: unable to parse '{BAD}': x dropped=0"},
            ),
            (
                2,
                None,
                422,
                {
                    "code": "unprocessable entity",
                    "message": "partial write: points beyond retention policy dropped=1",
                },
            ),
            (3, "v3", 400, {"code": "invalid", "message": f"unable to parse '{BAD}': invalid field format"}),
        ],
        ids=["other-400", "influxdb1-partial-write", "v2-partial-write", "v3-api"],
    )
    def test_not_resent(
        self,
        fake: FakeInflux,
        client_for: Callable[..., InfluxClient],
        version: int,
        api: str | None,
        status: int,
        body: dict[str, str],
    ) -> None:
        fake.responder = lambda _: Reply(status, body)
        client = client_for(version=version, write={"flush_interval": 60, "api": api})
        error = client.write([good(0), BAD, good(1)]).exception(timeout=5)
        assert isinstance(error, BadRequestError | PartialWriteError)
        assert len(fake.write_requests) == 1

    def test_a_resent_rest_the_server_stores_in_part_is_not_resent_again(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        server = InfluxDB2(fake)
        conflict = (
            'partial write: field type conflict: input field "v" on measurement "m" is type float, '
            "already exists as type integer dropped=1"
        )

        def respond(request: Recorded) -> Reply:
            if BAD in request.lines:
                return server(request)
            return Reply(422, {"code": "unprocessable entity", "message": conflict})

        fake.responder = respond
        client = v2_client(client_for)
        future = client.write([good(0), BAD, "m v=1.5 1", good(2)])
        error = future.exception(timeout=5)
        assert isinstance(error, PartialWriteError)
        assert error.rejected == 1  # the server's count of the conflict, not resent
        assert [len(r.lines) for r in fake.write_requests] == [4, 3]
        write = client.stats().write
        assert (write.points_written, write.points_failed) == (2, 2)  # BAD, and the conflict

    def test_a_bad_line_alone_is_not_resent(
        self, fake: FakeInflux, client_for: Callable[..., InfluxClient]
    ) -> None:
        InfluxDB2(fake)
        client = v2_client(client_for)
        with pytest.raises(PartialWriteError) as info:
            client.write(BAD).result(timeout=5)
        assert [(e.line_number, e.message, e.line) for e in info.value.line_errors] == [
            (1, "invalid field format", BAD)
        ]
        assert len(fake.write_requests) == 1


class TestQuotedLines:
    def test_lines_are_found_however_they_are_written(self) -> None:
        lines = ["m v=1 1", "  m,t=x 2", 'm s="it\': x" junk 3', 'm s="a\nb",g=zz 4', "m v=5 5", "m,t=x 2"]
        message = "\n".join(
            [
                "unable to parse 'm,t=x 2': invalid field format",
                "unable to parse 'm s=\"it': x\" junk 3': bad timestamp",
                "unable to parse 'm s=\"a\nb\",g=zz 4': invalid boolean",
                "unable to parse 'm,t=x 2': invalid field format",
            ]
        )
        found, complete = _unparseable_lines(message, lines)
        assert found == {
            1: "invalid field format",
            2: "bad timestamp",
            3: "invalid boolean",
            5: "invalid field format",
        }
        assert complete

    @pytest.mark.parametrize(
        ("message", "expected"),
        [
            ("unable to parse 'M,T=X 2': invalid field format", {}),  # not a line that was sent
            (
                "unable to parse 'm,t=x 2': invalid field format\nunable to parse 'cut sh",
                {1: "invalid field format"},
            ),
        ],
    )
    def test_quotes_matching_no_line_leave_it_incomplete(
        self, message: str, expected: dict[int, str]
    ) -> None:
        assert _unparseable_lines(message, ["m v=1 1", "m,t=x 2"]) == (expected, False)


class TestBookkeeping:
    def test_runs(self) -> None:
        assert _runs(range(3, 7)) == [(3, 7)]
        assert _runs(range(0)) == []
        assert _runs([]) == []
        assert _runs([0, 1, 2, 5, 7, 8]) == [(0, 3), (5, 6), (7, 9)]

    def test_each_future_gets_the_failures_among_its_lines(self) -> None:
        batch = _Batch("db", "ns")
        for count in (2, 3, 1, 4, 2):  # five write() calls: lines 0-1, 2-4, 5, 6-9, 10-11
            batch.futures.append(WriteFuture(None))
            batch.counts.append(count)
            batch.offsets.append(0)
            batch.lines += [good(len(batch.lines) + i) for i in range(count)]
        refused = BadRequestError("refused", status=400)
        rejected = PartialWriteError("rejected", status=400)
        failures = [  # in line order, as _finish sorts them
            _Failure(3, 4, rejected, LineError(4, "bad", good(3))),
            _Failure(5, 7, refused),  # the third call, and the first line of the fourth
            _Failure(8, 9, rejected, LineError(9, "bad", good(8))),
            _Failure(11, 12, rejected, LineError(12, "bad", good(11))),
        ]
        errors = WriteEngine._future_errors(batch, failures)
        assert errors[0] is None
        assert isinstance(errors[1], PartialWriteError)
        assert [(e.line_number, e.line) for e in errors[1].line_errors] == [(2, good(3))]
        assert errors[2] is refused
        assert errors[3] is refused  # a failure of whole lines wins over a rejected line
        assert isinstance(errors[4], PartialWriteError)
        assert [(e.line_number, e.line) for e in errors[4].line_errors] == [(2, good(11))]
