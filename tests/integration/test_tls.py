"""HTTPS (and gRPC over TLS for InfluxDB 3 queries) against servers with native TLS.

Needs ``docker/tls/generate.sh && docker compose --profile network up -d --wait``.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from influxkit import InfluxClient, InfluxConnectionError, InfluxKitError, ServerError, load_settings
from tests.servers import V2_BUCKET, V2_ORG, V2_TOKEN, V3_DATABASE, V3_TOKEN, reachable

pytestmark = pytest.mark.integration

TLS_DIR = Path(__file__).resolve().parents[2] / "docker" / "tls"
CA = TLS_DIR / "ca.crt"
WRONG_CA = TLS_DIR / "wrong-ca.crt"
TLS_URL = {2: "https://localhost:18087", 3: "https://localhost:18182"}
T0_NS = 1_767_225_600_000_000_000


def tls_client(version: int, url: str | None = None, **connection: Any) -> InfluxClient:
    target = url or TLS_URL[version]
    if not reachable(target.replace("https://", "http://")) or not CA.exists():
        pytest.skip(
            "TLS test servers not running (docker/tls/generate.sh && docker compose --profile network up -d)"
        )
    conn: dict[str, Any] = {
        "url": target,
        "version": version,
        "connect_timeout": 3,
        "timeout": 10,
        **connection,
    }
    if version == 3:
        conn["database"] = V3_DATABASE
        token = V3_TOKEN
    else:
        conn.update(database=V2_BUCKET, org=V2_ORG)
        token = V2_TOKEN
    settings = load_settings(
        None,
        env_file=None,
        token=token,
        connection=conn,
        write={"retry": {"max_attempts": 3, "initial_delay": 0.05}},
    )
    return InfluxClient(settings, registry=CollectorRegistry())


def stored(client: InfluxClient, measurement: str) -> int:
    if client.settings.connection.version == 3:
        return int(client.query(f'SELECT count(*) AS n FROM "{measurement}"').to_dicts()[0]["n"])
    flux = (
        f'from(bucket: "{V2_BUCKET}") |> range(start: 0) '
        f'|> filter(fn: (r) => r._measurement == "{measurement}") |> group() |> count()'
    )
    return int(client.query(flux).to_dicts()[0]["_value"])


def records(measurement: str, n: int) -> list[dict[str, Any]]:
    return [
        {"measurement": measurement, "fields": {"v": float(i)}, "time": T0_NS + i * 1000} for i in range(n)
    ]


def test_https_with_custom_ca(server_version: int, unique: str) -> None:
    with tls_client(server_version, ca_cert=str(CA)) as client:
        assert client.ping().version
        assert client.write(records(unique, 1000)).result(timeout=30).points == 1000
        assert stored(client, unique) == 1000  # InfluxDB 3: Arrow Flight over gRPC/TLS


def test_verification_can_be_disabled(server_version: int, unique: str) -> None:
    with tls_client(server_version, verify_ssl=False) as client:
        assert client.write(records(unique, 10)).result(timeout=30).points == 10
        assert stored(client, unique) == 10


@pytest.mark.parametrize("ca", [None, WRONG_CA], ids=["system-ca", "wrong-ca"])
def test_untrusted_certificate_fails_fast_without_retries(
    server_version: int, unique: str, ca: Path | None
) -> None:
    client = tls_client(server_version, **({"ca_cert": str(ca)} if ca else {}))
    started = time.monotonic()
    with pytest.raises(InfluxConnectionError, match="certificate verify failed"):
        client.write(records(unique, 1)).result(timeout=30)
    assert time.monotonic() - started < 5
    assert client.stats().write.retries == 0  # a TLS failure will not fix itself
    with pytest.raises(InfluxKitError):
        client.query("SELECT 1" if server_version == 3 else "buckets()")
    client.close()  # the failure was already raised by result(): not reported twice


def test_plain_http_against_a_tls_port_is_reported(server_version: int, unique: str) -> None:
    client = tls_client(server_version, url=TLS_URL[server_version].replace("https://", "http://"))
    with pytest.raises((ServerError, InfluxConnectionError)) as info:
        client.write(records(unique, 1)).result(timeout=30)
    if isinstance(info.value, ServerError):
        assert info.value.status == 400
    client.close()


MTLS_URL = "https://localhost:28443"  # nginx requiring a client certificate, in front of InfluxDB 3


def test_mutual_tls_for_writes_and_flight_queries(unique: str) -> None:
    client = tls_client(
        3,
        url=MTLS_URL,
        ca_cert=str(CA),
        client_cert=str(TLS_DIR / "client.crt"),
        client_key=str(TLS_DIR / "client.key"),
    )
    with client:
        assert client.write(records(unique, 500)).result(timeout=30).points == 500
        assert stored(client, unique) == 500  # Arrow Flight over gRPC with the client certificate


def test_mutual_tls_without_a_client_certificate_is_rejected(unique: str) -> None:
    client = tls_client(3, url=MTLS_URL, ca_cert=str(CA))
    with pytest.raises(ServerError, match="SSL certificate") as info:
        client.write(records(unique, 1)).result(timeout=30)
    assert info.value.status == 400
    client.close()
