"""Proxies between the client and the server: a body-limited reverse proxy and a forward proxy.

The nginx test needs ``docker compose --profile network up -d --wait``.
"""

from __future__ import annotations

import http.client
import logging
import select
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import pytest
from prometheus_client import CollectorRegistry

from sluicebox import InfluxClient, load_settings
from sluicebox.client import flux_string
from tests.servers import (
    V2_BUCKET,
    V2_ORG,
    V2_TOKEN,
    V2_URL,
    V3_DATABASE,
    V3_TOKEN,
    V3_URL,
    reachable,
    server_client,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

NGINX = {2: "http://localhost:28087", 3: "http://localhost:28182"}
T0_NS = 1_767_225_600_000_000_000


def make_client(version: int, url: str, **overrides: Any) -> InfluxClient:
    connection: dict[str, Any] = {"url": url, "version": version, **overrides.pop("connection", {})}
    if version == 3:
        connection["database"] = V3_DATABASE
        token = V3_TOKEN
    else:
        connection.update(database=V2_BUCKET, org=V2_ORG)
        token = V2_TOKEN
    settings = load_settings(None, env_file=None, token=token, connection=connection, **overrides)
    return InfluxClient(settings, registry=CollectorRegistry())


def records(measurement: str, n: int) -> list[dict[str, Any]]:
    return [
        {
            "measurement": measurement,
            "tags": {"host": f"h{i % 10}"},
            "fields": {"v": float(i)},
            "time": T0_NS + i * 1000,
        }
        for i in range(n)
    ]


def stored(client: InfluxClient, measurement: str) -> int:
    if client.settings.connection.version == 3:
        return int(client.query(f'SELECT count(*) AS n FROM "{measurement}"').to_dicts()[0]["n"])
    flux = (
        f"from(bucket: {flux_string(V2_BUCKET)}) |> range(start: 0) "
        f"|> filter(fn: (r) => r._measurement == {flux_string(measurement)}) |> group() |> count()"
    )
    return int(client.query(flux).to_dicts()[0]["_value"])


def test_reverse_proxy_body_limit_is_learned(server_version: int, unique: str, caplog: Any) -> None:
    if not reachable(NGINX[server_version]):
        pytest.skip("nginx not running (docker compose --profile network up -d --wait)")
    n = 60_000
    with make_client(server_version, NGINX[server_version], write={"gzip": False}) as client:
        assert client.write(records(unique, n)).result(timeout=120).points == n
        learned = client._engine._max_batch_bytes
        assert learned <= 256 * 1024  # nginx: client_max_body_size 256k
        assert client.write(records(unique + "_again", n)).result(timeout=120).points == n
    assert sum("rejected a" in r.getMessage() for r in caplog.records if r.levelname == "WARNING") <= 6
    with server_client(server_version, CollectorRegistry()) as direct:
        assert stored(direct, unique) == n
        assert stored(direct, unique + "_again") == n


def test_batch_byte_limit_counts_utf8_bytes(server_version: int, unique: str, caplog: Any) -> None:
    """Multi-byte text must not inflate requests past max_batch_bytes (and the proxy's limit)."""
    if not reachable(NGINX[server_version]):
        pytest.skip("nginx not running (docker compose --profile network up -d --wait)")
    caplog.set_level(logging.DEBUG, logger="sluicebox")
    limit = 200_000  # below nginx's 256k, but not if characters were counted as bytes
    text = "漢字" * 70
    write = {"gzip": False, "max_batch_bytes": limit, "flush_interval": 60}
    with make_client(server_version, NGINX[server_version], write=write) as client:
        for i in range(2000):
            client.write({"measurement": unique, "fields": {"s": f"{text}{i}"}, "time": T0_NS + i * 1000})
        client.flush()
        stats = client.stats().write
        assert client._engine._max_batch_bytes == limit  # no 413 to learn from
    assert not [r for r in caplog.records if "too large" in r.getMessage()]
    expected = sum(len(f'{unique} s="{text}{i}" {T0_NS + i * 1000}'.encode()) + 1 for i in range(2000))
    assert stats.bytes_raw == expected
    with server_client(server_version, CollectorRegistry()) as direct:
        assert stored(direct, unique) == 2000


class _ForwardProxy(BaseHTTPRequestHandler):
    """Minimal HTTP forward proxy: absolute-URI forwarding plus CONNECT tunnels (for gRPC)."""

    protocol_version = "HTTP/1.1"
    requests_seen: list[str]

    def log_message(self, *args: Any) -> None:
        pass

    def _forward(self) -> None:
        self.server.requests_seen.append(f"{self.command} {self.path}")  # type: ignore[attr-defined]
        target = urlsplit(self.path)
        body = self.rfile.read(int(self.headers.get("Content-Length", 0) or 0))
        upstream = http.client.HTTPConnection(target.hostname, target.port, timeout=30)
        headers = {
            k: v for k, v in self.headers.items() if k.lower() not in ("proxy-connection", "connection")
        }
        upstream.request(
            self.command, target.path + (f"?{target.query}" if target.query else ""), body, headers
        )
        response = upstream.getresponse()
        payload = response.read()
        self.send_response(response.status, response.reason)
        for key, value in response.getheaders():
            if key.lower() not in ("transfer-encoding", "connection", "content-length"):
                self.send_header(key, value)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
        upstream.close()

    do_GET = do_POST = _forward

    def do_CONNECT(self) -> None:
        self.server.requests_seen.append(f"CONNECT {self.path}")  # type: ignore[attr-defined]
        host, port = self.path.rsplit(":", 1)
        upstream = socket.create_connection((host, int(port)), timeout=30)
        self.send_response(200, "Connection established")
        self.end_headers()
        sockets = [self.connection, upstream]
        try:
            while True:
                readable, _, _ = select.select(sockets, [], [], 30)
                if not readable:
                    return
                for sock in readable:
                    data = sock.recv(65536)
                    if not data:
                        return
                    (upstream if sock is self.connection else self.connection).sendall(data)
        finally:
            upstream.close()


@pytest.fixture
def forward_proxy() -> Iterator[Any]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ForwardProxy)
    server.daemon_threads = True
    server.requests_seen = []  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()


def test_forward_proxy_for_writes_and_queries(server_version: int, unique: str, forward_proxy: Any) -> None:
    url = V3_URL if server_version == 3 else V2_URL
    if not reachable(url):
        pytest.skip("test server not running")
    proxy = f"http://127.0.0.1:{forward_proxy.server_address[1]}"
    with make_client(server_version, url, connection={"proxy": proxy}) as client:
        assert client.write(records(unique, 1000)).result(timeout=60).points == 1000
        assert stored(client, unique) == 1000
    seen = forward_proxy.requests_seen
    assert any(entry.startswith("POST http://") for entry in seen), seen  # writes went through the proxy
    if server_version == 3:
        assert any(entry.startswith("CONNECT ") for entry in seen), seen  # Flight (gRPC) tunnelled
