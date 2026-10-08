"""Fixtures shared by the unit tests that talk to the fake server."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

import pytest
from prometheus_client import CollectorRegistry

from sluicebox import InfluxClient, WriteError

from .fake_server import FakeInflux

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


@pytest.fixture
def fake() -> Iterator[FakeInflux]:
    server = FakeInflux()
    yield server
    server.stop()


@pytest.fixture
def client_for(fake: FakeInflux, make_settings: Callable[..., Any]) -> Iterator[Callable[..., InfluxClient]]:
    """Clients of the fake server (closed after the test, failures already asserted on)."""
    clients: list[InfluxClient] = []

    def factory(version: int = 3, *, on_error: Any = None, **overrides: Any) -> InfluxClient:
        settings = make_settings(url=fake.url, version=version, **overrides)
        client = InfluxClient(settings, on_error=on_error, registry=CollectorRegistry())
        clients.append(client)
        return client

    yield factory
    for client in clients:
        with contextlib.suppress(WriteError):
            client.close(timeout=2)
