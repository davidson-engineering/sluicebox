"""A scriptable fake InfluxDB write endpoint for engine tests."""

from __future__ import annotations

import gzip
import json
import re
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

from pytest_httpserver import HTTPServer
from werkzeug import Request, Response


@dataclass
class Recorded:
    method: str
    path: str
    args: dict[str, str]
    headers: dict[str, str]
    lines: list[str]
    raw: bytes


@dataclass
class Reply:
    status: int = 204
    body: Any = b""
    headers: dict[str, str] = field(default_factory=dict)
    delay: float = 0.0


Responder = Callable[[Recorded], Reply]


class _QuickStopServer(HTTPServer):
    """``stop()`` waits for the serve loop's next poll, 0.5 s apart by default."""

    def thread_target(self) -> None:
        assert self.server is not None
        self.server.serve_forever(poll_interval=0.01)


class FakeInflux:
    """Records every request (gunzipping bodies) and answers via ``responder``."""

    def __init__(self) -> None:
        self.server = _QuickStopServer(threaded=True)
        self.server.expect_request(re.compile(".*")).respond_with_handler(self._handle)
        self.server.start()
        self.requests: list[Recorded] = []
        self.responder: Responder = lambda _: Reply()
        self._lock = threading.Lock()
        self.active = 0
        self.max_active = 0

    @property
    def url(self) -> str:
        return self.server.url_for("").rstrip("/")

    def _handle(self, request: Request) -> Response:
        raw = request.get_data()
        body = gzip.decompress(raw) if request.headers.get("Content-Encoding") == "gzip" else raw
        recorded = Recorded(
            method=request.method,
            path=request.path,
            args=dict(request.args),
            headers=dict(request.headers),
            lines=[line for line in body.decode().split("\n") if line] if body else [],
            raw=raw,
        )
        with self._lock:
            self.requests.append(recorded)
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            reply = self.responder(recorded)
            if reply.delay:
                time.sleep(reply.delay)
            payload = reply.body if isinstance(reply.body, bytes | str) else json.dumps(reply.body)
            return Response(payload, status=reply.status, headers=reply.headers)
        finally:
            with self._lock:
                self.active -= 1

    @property
    def write_requests(self) -> list[Recorded]:
        return [r for r in self.requests if r.method == "POST"]

    @property
    def lines(self) -> list[str]:
        return [line for r in self.write_requests for line in r.lines]

    def stop(self) -> None:
        self.server.clear()
        self.server.stop()


def sequence(*replies: Reply) -> Responder:
    """Answer with ``replies`` in order, then keep repeating the last one."""
    queue: Iterator[Reply] = iter(replies)
    last = [replies[-1]]
    lock = threading.Lock()

    def respond(_: Recorded) -> Reply:
        with lock:
            reply = next(queue, None)
            if reply is not None:
                last[0] = reply
            return last[0]

    return respond
