"""Remote-server network conditions, emulated with Toxiproxy.

Each test writes through a Toxiproxy proxy that injects latency, bandwidth limits, resets,
blackholes or outages, then verifies the stored data over a direct connection. Needs the
``network`` compose profile::

    docker/tls/generate.sh && docker compose --profile network up -d --wait
"""

from __future__ import annotations

import contextlib
import json
import threading
import time
import urllib.request
from typing import TYPE_CHECKING, Any

import pytest
from prometheus_client import CollectorRegistry

from sluicebox import InfluxClient, SluiceboxError, WriteError, load_settings
from sluicebox.client import flux_string
from tests.servers import V2_BUCKET, V2_ORG, V2_TOKEN, V3_DATABASE, V3_TOKEN, reachable, server_client

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

pytestmark = pytest.mark.integration

TOXIPROXY_API = "http://localhost:18474"
PROXIED = {2: ("influxdb2", "http://localhost:28086"), 3: ("influxdb3", "http://localhost:28181")}
T0_NS = 1_767_225_600_000_000_000


class Toxiproxy:
    def __init__(self, api: str) -> None:
        self.api = api

    def _call(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            self.api + path, data=data, method=method, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            text = response.read()
        return json.loads(text) if text else None

    def reset(self) -> None:
        self._call("POST", "/reset")

    def add(
        self,
        proxy: str,
        name: str,
        kind: str,
        stream: str = "downstream",
        toxicity: float = 1.0,
        **attributes: Any,
    ) -> None:
        self._call(
            "POST",
            f"/proxies/{proxy}/toxics",
            {"name": name, "type": kind, "stream": stream, "toxicity": toxicity, "attributes": attributes},
        )

    def remove(self, proxy: str, name: str) -> None:
        self._call("DELETE", f"/proxies/{proxy}/toxics/{name}")

    def enable(self, proxy: str, enabled: bool) -> None:
        self._call("POST", f"/proxies/{proxy}", {"enabled": enabled})


@pytest.fixture
def toxiproxy() -> Iterator[Toxiproxy]:
    if not reachable(TOXIPROXY_API):
        pytest.skip("Toxiproxy not running (docker compose --profile network up -d --wait)")
    proxy = Toxiproxy(TOXIPROXY_API)
    proxy.reset()
    yield proxy
    proxy.reset()


@pytest.fixture
def direct(server_version: int) -> Iterator[InfluxClient]:
    client = server_client(server_version, CollectorRegistry())
    yield client
    client.close()


@pytest.fixture
def proxied(server_version: int, toxiproxy: Toxiproxy) -> Iterator[Callable[..., InfluxClient]]:
    clients: list[InfluxClient] = []
    url = PROXIED[server_version][1]

    def factory(**overrides: Any) -> InfluxClient:
        connection: dict[str, Any] = {"url": url, "version": server_version}
        if server_version == 3:
            connection["database"] = V3_DATABASE
            token = V3_TOKEN
        else:
            connection.update(database=V2_BUCKET, org=V2_ORG)
            token = V2_TOKEN
        connection.update(overrides.pop("connection", {}))
        settings = load_settings(
            None,
            env_file=None,
            name=f"net-v{server_version}",
            token=token,
            connection=connection,
            **overrides,
        )
        client = InfluxClient(settings, registry=CollectorRegistry())
        clients.append(client)
        return client

    yield factory
    toxiproxy.reset()
    for client in clients:
        with contextlib.suppress(WriteError):  # tests that provoke failures assert on them
            client.close(timeout=10)


def proxy_name(server_version: int) -> str:
    return PROXIED[server_version][0]


def points(measurement: str, n: int, *, tags: int = 20) -> list[dict[str, Any]]:
    return [
        {
            "measurement": measurement,
            "tags": {"host": f"h{i % tags}"},
            "fields": {"v": float(i), "n": i},
            "time": T0_NS + i * 1000,
        }
        for i in range(n)
    ]


def count(client: InfluxClient, measurement: str) -> int:
    """Stored points, read over a direct connection (waits briefly for visibility)."""
    deadline = time.monotonic() + 10
    while True:
        if client.settings.connection.version == 3:
            try:
                rows = client.query(f'SELECT count(*) AS n FROM "{measurement}"').to_dicts()
            except SluiceboxError as error:  # table does not exist yet
                if "not found" not in str(error):
                    raise
                rows = [{"n": 0}]
            found = int(rows[0]["n"]) if rows else 0
        else:
            flux = (
                f"from(bucket: {flux_string(V2_BUCKET)}) |> range(start: 0) "
                f'|> filter(fn: (r) => r._measurement == {flux_string(measurement)} and r._field == "v") '
                "|> group() |> count()"
            )
            rows = client.query(flux).to_dicts()
            found = int(rows[0]["_value"]) if rows else 0
        if found or time.monotonic() > deadline:
            return found
        time.sleep(0.3)


def test_latency_is_hidden_by_concurrency(
    proxied: Callable[..., InfluxClient],
    direct: InfluxClient,
    toxiproxy: Toxiproxy,
    server_version: int,
    unique: str,
) -> None:
    """80 ms round trips: requests in flight, not round trips, bound throughput."""
    toxiproxy.add(proxy_name(server_version), "lat_down", "latency", "downstream", latency=40)
    toxiproxy.add(proxy_name(server_version), "lat_up", "latency", "upstream", latency=40)
    n = 10_000
    timings = {}
    for concurrency in (1, 16):
        measurement = f"{unique}_c{concurrency}"
        client = proxied(write={"batch_size": 1000, "concurrency": concurrency})
        started = time.monotonic()
        assert client.write(points(measurement, n)).result(timeout=120).points == n
        timings[concurrency] = time.monotonic() - started
        assert count(direct, measurement) == n
    assert timings[16] < timings[1] / 3, timings


def test_bandwidth_limited_link_needs_gzip(
    proxied: Callable[..., InfluxClient],
    direct: InfluxClient,
    toxiproxy: Toxiproxy,
    server_version: int,
    unique: str,
) -> None:
    """A ~1 Mbit/s uplink (one connection, so the limit is the link's).

    Slow enough that upload time dominates InfluxDB 3's WAL-flush wait.
    """
    toxiproxy.add(proxy_name(server_version), "bw", "bandwidth", "upstream", rate=120)  # KB/s
    n = 10_000
    timings = {}
    for gzip in (True, False):
        measurement = f"{unique}_gz{int(gzip)}"
        client = proxied(write={"gzip": gzip, "concurrency": 1, "batch_size": 5000})
        started = time.monotonic()
        assert client.write(points(measurement, n)).result(timeout=120).points == n
        timings[gzip] = time.monotonic() - started
        assert count(direct, measurement) == n
    assert timings[True] < timings[False] / 2, timings


def test_connection_resets_are_retried_without_loss(
    proxied: Callable[..., InfluxClient],
    direct: InfluxClient,
    toxiproxy: Toxiproxy,
    server_version: int,
    unique: str,
) -> None:
    toxiproxy.add(proxy_name(server_version), "reset", "reset_peer", "upstream", toxicity=0.5, timeout=0)
    n = 30_000
    client = proxied(
        write={"batch_size": 1000, "retry": {"max_attempts": 12, "initial_delay": 0.05, "max_delay": 0.5}}
    )
    assert client.write(points(unique, n)).result(timeout=120).points == n
    assert client.stats().write.retries >= 1
    toxiproxy.reset()
    assert count(direct, unique) == n


def test_lost_responses_do_not_duplicate_points(
    proxied: Callable[..., InfluxClient],
    direct: InfluxClient,
    toxiproxy: Toxiproxy,
    server_version: int,
    unique: str,
) -> None:
    """The server stores the batch but the response never arrives; the retry must not duplicate."""
    proxy = proxy_name(server_version)
    toxiproxy.add(proxy, "blackhole", "timeout", "downstream", timeout=0)
    timer = threading.Timer(3.0, toxiproxy.remove, (proxy, "blackhole"))
    timer.start()
    n = 5_000
    client = proxied(
        connection={"timeout": 1.5},
        write={"retry": {"max_attempts": 20, "initial_delay": 0.2, "max_delay": 1.0}},
    )
    started = time.monotonic()
    try:
        assert client.write(points(unique, n)).result(timeout=120).points == n
    finally:
        timer.cancel()
    elapsed = time.monotonic() - started
    assert 2.5 < elapsed < 60
    assert client.stats().write.retries >= 1
    assert count(direct, unique) == n  # each retry re-wrote identical points: overwritten, not added


def test_outage_then_recovery(
    proxied: Callable[..., InfluxClient],
    direct: InfluxClient,
    toxiproxy: Toxiproxy,
    server_version: int,
    unique: str,
) -> None:
    proxy = proxy_name(server_version)
    client = proxied(write={"retry": {"max_attempts": 30, "initial_delay": 0.1, "max_delay": 0.5}})
    # Warm the connection pool in another table: overwriting rows of the counted one can make
    # InfluxDB 3 briefly count them twice.
    client.write(points(f"{unique}_warmup", 10)).result(timeout=30)
    toxiproxy.enable(proxy, False)
    threading.Timer(2.0, toxiproxy.enable, (proxy, True)).start()
    n = 10_000
    assert client.write(points(unique, n)).result(timeout=120).points == n
    assert client.stats().write.retries >= 1
    assert count(direct, unique) == n


@pytest.mark.parametrize("gzip", [False, True], ids=["plain", "gzip"])
def test_truncated_uploads_are_never_partially_ingested(
    proxied: Callable[..., InfluxClient],
    direct: InfluxClient,
    toxiproxy: Toxiproxy,
    server_version: int,
    unique: str,
    gzip: bool,
) -> None:
    """The connection dies mid-body: the server must not store a prefix of the batch."""
    toxiproxy.add(proxy_name(server_version), "cut", "limit_data", "upstream", bytes=20_000)
    client = proxied(
        write={"gzip": gzip, "gzip_min_bytes": 0, "retry": {"max_attempts": 2, "initial_delay": 0.05}},
        connection={"timeout": 5},
    )
    with pytest.raises(SluiceboxError):
        client.write(points(unique, 20_000)).result(timeout=60)
    toxiproxy.reset()
    time.sleep(1.0)
    assert count(direct, unique) == 0


def test_sliced_tcp_stream(
    proxied: Callable[..., InfluxClient],
    direct: InfluxClient,
    toxiproxy: Toxiproxy,
    server_version: int,
    unique: str,
) -> None:
    """Data arrives in tiny, delayed TCP segments in both directions (writes and queries)."""
    proxy = proxy_name(server_version)
    toxiproxy.add(proxy, "slice_up", "slicer", "upstream", average_size=512, size_variation=256, delay=50)
    toxiproxy.add(proxy, "slice_down", "slicer", "downstream", average_size=64, size_variation=32, delay=50)
    n = 5_000
    client = proxied()
    assert client.write(points(unique, n)).result(timeout=120).points == n
    assert count(client, unique) == n  # the query also runs through the sliced proxy
    assert count(direct, unique) == n


def test_queries_over_latency(
    proxied: Callable[..., InfluxClient],
    direct: InfluxClient,
    toxiproxy: Toxiproxy,
    server_version: int,
    unique: str,
) -> None:
    direct.write(points(unique, 2_000)).result(timeout=60)
    proxy = proxy_name(server_version)
    toxiproxy.add(proxy, "lat_down", "latency", "downstream", latency=50, jitter=20)
    toxiproxy.add(proxy, "lat_up", "latency", "upstream", latency=50, jitter=20)
    client = proxied()
    assert count(client, unique) == 2_000
