from __future__ import annotations

import contextlib
import os
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from sluicebox import InfluxClient, InfluxSettings, WriteError, load_settings
from tests.servers import deep_merge, server_client


@pytest.fixture(autouse=True)
def _isolated_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """No developer SLUICEBOX_* variables, .env or sluicebox.toml can leak into a test."""
    for name in list(os.environ):
        if name.startswith("SLUICEBOX_") and not name.startswith("SLUICEBOX_TEST_"):
            monkeypatch.delenv(name)
    monkeypatch.chdir(tmp_path)


@pytest.fixture
def registry() -> CollectorRegistry:
    return CollectorRegistry()


SettingsFactory = Callable[..., InfluxSettings]


@pytest.fixture
def make_settings() -> SettingsFactory:
    """Build settings for a (fake or real) server without reading files."""

    def factory(url: str = "http://127.0.0.1:9", version: int = 3, **overrides: Any) -> InfluxSettings:
        base: dict[str, Any] = {
            "token": "test-token",
            "connection": {
                "url": url,
                "version": version,
                "database": "db",
                "timeout": 5,
                "connect_timeout": 2,
            },
            "write": {
                "flush_interval": 0.05,
                "retry": {"initial_delay": 0.01, "max_delay": 0.05, "jitter": 0},
            },
            "metrics": {"enabled": True},
        }
        if version == 2:
            base["connection"]["org"] = "org"
        return load_settings(None, env_file=None, **deep_merge(base, overrides))

    return factory


@pytest.fixture
def unique() -> str:
    """A unique measurement name for one test."""
    return f"t_{uuid.uuid4().hex[:12]}"


@pytest.fixture(params=[3, 2], ids=["influxdb3", "influxdb2"])
def server_version(request: pytest.FixtureRequest) -> int:
    return int(request.param)


@pytest.fixture
def live_client(server_version: int, registry: CollectorRegistry) -> Iterator[InfluxClient]:
    client = server_client(server_version, registry)
    yield client
    client.close()


@pytest.fixture
def make_live_client(
    server_version: int, registry: CollectorRegistry
) -> Iterator[Callable[..., InfluxClient]]:
    clients: list[InfluxClient] = []

    def factory(**overrides: Any) -> InfluxClient:
        client = server_client(server_version, CollectorRegistry(), **overrides)
        clients.append(client)
        return client

    yield factory
    for client in clients:
        # Tests that provoke failures have already asserted on them.
        with contextlib.suppress(WriteError):
            client.close()
