"""The docker-compose test servers and helpers to reach them (shared by the test modules)."""

from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from sluicebox import InfluxClient, load_settings

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry

#: Throwaway credentials that `docker compose up` generates for the test servers.
SECRETS_DIR = Path(__file__).resolve().parents[1] / "docker" / "secrets"


def _credential(variable: str, file_name: str) -> str:
    """From the environment (other servers), else from the generated file ("" if absent)."""
    value = os.environ.get(variable)
    if value:
        return value
    try:
        return (SECRETS_DIR / file_name).read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return ""


V2_URL = os.environ.get("SLUICEBOX_TEST_V2_URL", "http://localhost:18086")
V2_TOKEN = _credential("SLUICEBOX_TEST_V2_TOKEN", "influxdb2-token")
V2_ORG = "sluicebox"
V2_BUCKET = "sluicebox"
V3_URL = os.environ.get("SLUICEBOX_TEST_V3_URL", "http://localhost:18181")
V3_TOKEN = _credential("SLUICEBOX_TEST_V3_TOKEN", "influxdb3-token")
# InfluxDB 3 Core allows only 5 databases: integration tests share this one.
V3_DATABASE = "sluicebox_test"


def deep_merge(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def reachable(url: str) -> bool:
    host, port = url.split("://", 1)[1].split(":")
    try:
        with socket.create_connection((host, int(port)), timeout=0.5):
            return True
    except OSError:
        return False


def server_client(version: int, registry: CollectorRegistry, **overrides: Any) -> InfluxClient:
    url = V3_URL if version == 3 else V2_URL
    if not reachable(url):
        pytest.skip(f"InfluxDB {version} test server not running at {url} (docker compose up -d --wait)")
    if not (V3_TOKEN if version == 3 else V2_TOKEN):
        pytest.skip(f"no credentials in {SECRETS_DIR}: start the servers with docker compose up -d --wait")
    connection: dict[str, Any] = {"url": url, "version": version}
    if version == 3:
        connection["database"] = V3_DATABASE
        token = V3_TOKEN
    else:
        connection.update(database=V2_BUCKET, org=V2_ORG)
        token = V2_TOKEN
    base: dict[str, Any] = {
        "name": f"it-v{version}",
        "token": token,
        "connection": connection,
        "write": {"flush_interval": 0.05},
    }
    settings = load_settings(None, env_file=None, **deep_merge(base, overrides))
    return InfluxClient(settings, registry=registry)
