"""A scriptable fake InfluxDB server for engine tests.

Like InfluxDB, it keeps HTTP/1.1 connections alive: the client reuses its pooled connections
here as it does in production, and thousands of requests do not use up the ephemeral ports.
"""

from __future__ import annotations

import contextlib
import gzip
import json
import socket
import socketserver
import sys
import threading
import traceback
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler
from typing import Any
from urllib.parse import parse_qsl, unquote, urlsplit


@dataclass
class Recorded:
    method: str
    path: str
    args: dict[str, str]
    headers: dict[str, str]
    lines: list[str]
    raw: bytes
    #: The TCP connection that carried the request: 1 for the first one the server accepted.
    connection: int


@dataclass
class Reply:
    status: int = 204
    body: Any = b""
    headers: dict[str, str] = field(default_factory=dict)
    delay: float = 0.0
    #: Close the connection instead of answering, as a server does when its keep-alive timeout
    #: closes a pooled connection just as a request arrives on it.
    disconnect: bool = False


Responder = Callable[[Recorded], Reply]


class FakeInflux:
    """Records every request (gunzipping bodies) and answers via ``responder``."""

    def __init__(self) -> None:
        self.requests: list[Recorded] = []
        self.responder: Responder = lambda _: Reply()
        self._lock = threading.Lock()
        self.active = 0
        self.max_active = 0
        self._stopping = threading.Event()
        self._server = _Server(self._answer)
        # serve_forever() checks for shutdown between polls, 0.5 s apart by default.
        serve = threading.Thread(target=self._server.serve_forever, kwargs={"poll_interval": 0.01})
        serve.daemon = True
        serve.start()

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def _answer(self, recorded: Recorded) -> Reply:
        with self._lock:
            self.requests.append(recorded)
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            reply = self.responder(recorded)
            if reply.delay:
                self._stopping.wait(reply.delay)
            return reply
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
        """Stop serving, cut reply delays short and close every connection."""
        self._server.shutdown()
        self._server.server_close()
        self._stopping.set()
        self._server.close_connections()


class _Server(socketserver.ThreadingTCPServer):
    """Serves each connection on a thread of its own, for as long as the client keeps it open."""

    daemon_threads = True
    allow_reuse_address = True
    # A burst of new connections must not overflow the listen backlog: the kernel then drops
    # the handshake, and the client tries again a second later (werkzeug's backlog is 128 too).
    request_queue_size = 128

    def __init__(self, answer: Callable[[Recorded], Reply]) -> None:
        self.answer = answer
        self._lock = threading.Lock()
        self._accepted = 0
        self._open: dict[socket.socket, int] = {}
        # Not "localhost": that can resolve to ::1 first, where nothing listens.
        super().__init__(("127.0.0.1", 0), _Handler)

    def process_request(self, request: Any, client_address: Any) -> None:
        with self._lock:
            self._accepted += 1
            self._open[request] = self._accepted
        super().process_request(request, client_address)

    def connection_number(self, request: socket.socket) -> int:
        with self._lock:
            return self._open[request]

    def close_request(self, request: Any) -> None:
        with self._lock:  # close_connections() must never shut down a reused descriptor
            self._open.pop(request, None)
        super().close_request(request)

    def close_connections(self) -> None:
        """Close every open connection, idle ones too: their clients see them closed."""
        with self._lock:
            for request in self._open:
                with contextlib.suppress(OSError):
                    request.shutdown(socket.SHUT_RDWR)

    def handle_error(self, request: Any, client_address: Any) -> None:
        # Socket errors: the client went away mid-request, or stop() closed the connection.
        if not isinstance(sys.exception(), OSError):
            super().handle_error(request, client_address)


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keeps connections alive
    # As servers do: a reply's headers and body are separate writes, and Nagle's algorithm
    # could hold the body back until the client's (delayed) acknowledgement of the headers.
    disable_nagle_algorithm = True
    server: _Server

    def setup(self) -> None:
        super().setup()
        self.connection_number = self.server.connection_number(self.request)

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def _serve(self) -> None:
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        try:
            reply = self.server.answer(self._recorded(raw))
        except Exception:  # a failing responder (or an undecodable body) fails the request visibly
            traceback.print_exc()
            reply = Reply(500, traceback.format_exc())
        if reply.disconnect:
            self.close_connection = True
            return
        if isinstance(reply.body, bytes | str):
            content_type = "text/plain; charset=utf-8"
            payload = reply.body.encode() if isinstance(reply.body, str) else reply.body
        else:
            content_type = "application/json"
            payload = json.dumps(reply.body).encode()
        has_body = reply.status not in (204, 304)
        self.send_response(reply.status)
        for name, value in reply.headers.items():
            self.send_header(name, value)
        if has_body:
            if payload and "content-type" not in {name.lower() for name in reply.headers}:
                self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if has_body:
            self.wfile.write(payload)

    def _recorded(self, raw: bytes) -> Recorded:
        # A client talking to a forward proxy sends absolute URIs: "POST http://host:port/path?q".
        target = urlsplit(self.path)
        # Spelled as werkzeug did (urllib3 sends some names in lower case, e.g. proxy-authorization).
        headers = {name.title(): value for name, value in self.headers.items()}
        body = gzip.decompress(raw) if headers.get("Content-Encoding") == "gzip" else raw
        return Recorded(
            method=self.command,
            path=unquote(target.path),
            args=dict(parse_qsl(target.query, keep_blank_values=True)),
            headers=headers,
            lines=[line for line in body.decode().split("\n") if line],
            raw=raw,
            connection=self.connection_number,
        )

    do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = _serve


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
