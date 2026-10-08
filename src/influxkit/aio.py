"""asyncio facade.

:class:`AsyncInfluxClient` shares the thread-based engine of :class:`InfluxClient`: writes
are already asynchronous (buffered, batched and sent by background threads), so asyncio
code only needs non-blocking entry points. Nothing here blocks the event loop:

* ``fut = await client.write(points)`` buffers the data (serializing large inputs in a
  worker thread and waiting for buffer space off-loop) and returns a
  :class:`~influxkit.futures.WriteFuture`; ``await fut`` waits for acknowledgement.
* ``query``, ``flush``, ``ping``, ``sync_schema`` and ``close`` run in worker threads.
* ``query_stream`` is an async iterator.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from contextlib import nullcontext
from typing import TYPE_CHECKING, Any, Self

from ._engine import WouldBlock, WriteFailure
from .client import ClientStats, InfluxClient, ServerInfo, _add_dropped
from .exceptions import BufferFullError, ClientClosedError, ValidationError
from .futures import WriteFuture
from .point import Point
from .tags import Enricher, tag_context
from .types import PRECISION_DIVISORS

if TYPE_CHECKING:
    import os
    from collections.abc import AsyncIterator, Callable, Iterator, Sequence
    from types import TracebackType

    from prometheus_client import CollectorRegistry

    from .config import InfluxSettings
    from .query import Language, QueryResult
    from .types import FieldType, Precision

__all__ = ["AsyncInfluxClient"]

_DONE: Any = object()
#: Writes that never need to suspend still yield to the event loop every this many calls.
_YIELD_EVERY = 32


class AsyncInfluxClient:
    """asyncio-friendly client; see :class:`~influxkit.client.InfluxClient` for the arguments."""

    def __init__(
        self,
        settings: InfluxSettings | None = None,
        *,
        tags: Mapping[str, str] | None = None,
        enrichers: Sequence[Enricher] = (),
        on_error: Callable[[WriteFailure], Any] | None = None,
        registry: CollectorRegistry | None = None,
        **overrides: Any,
    ) -> None:
        self._client = InfluxClient(
            settings, tags=tags, enrichers=enrichers, on_error=on_error, registry=registry, **overrides
        )
        self._unyielded = 0

    @classmethod
    def from_config(
        cls,
        config_file: str | os.PathLike[str] | None = None,
        *,
        section: str | None = None,
        env_file: str | os.PathLike[str] | None = ".env",
        env_prefix: str = "INFLUXKIT_",
        secrets_dir: str | os.PathLike[str] | None = None,
        tags: Mapping[str, str] | None = None,
        enrichers: Sequence[Enricher] = (),
        on_error: Callable[[WriteFailure], Any] | None = None,
        registry: CollectorRegistry | None = None,
        **overrides: Any,
    ) -> Self:
        """Create a client from a TOML file plus environment/.env secrets."""
        sync = InfluxClient.from_config(
            config_file,
            section=section,
            env_file=env_file,
            env_prefix=env_prefix,
            secrets_dir=secrets_dir,
            tags=tags,
            enrichers=enrichers,
            on_error=on_error,
            registry=registry,
            **overrides,
        )
        return cls.wrap(sync)

    @classmethod
    def wrap(cls, client: InfluxClient) -> Self:
        """An asyncio facade over an existing client (sharing its buffer and connections)."""
        wrapper = cls.__new__(cls)
        wrapper._client = client
        wrapper._unyielded = 0
        return wrapper

    @property
    def sync(self) -> InfluxClient:
        """The underlying synchronous client (shares buffers and connections)."""
        return self._client

    @property
    def settings(self) -> InfluxSettings:
        """The settings in effect."""
        return self._client.settings

    async def write(
        self,
        data: Any,
        *,
        database: str | None = None,
        precision: Precision | None = None,
        tags: Mapping[str, str] | None = None,
        measurement: str | None = None,
        tag_columns: Sequence[str] | None = None,
        field_columns: Sequence[str] | None = None,
        time_column: str | None = None,
    ) -> WriteFuture:
        """Buffer ``data`` without blocking the event loop; returns once it is buffered.

        ``await`` the returned future to wait for the server's acknowledgement.
        Arguments and errors are those of :meth:`InfluxClient.write`.
        """
        client = self._client
        if client.closed:
            raise ClientClosedError("the client is closed")
        target = database or client._database
        prec = precision or client._precision
        if prec not in PRECISION_DIVISORS:
            raise ValueError(f"precision must be one of 'ns', 'us', 'ms', 's'; got {prec!r}")
        engine = client._engine
        with tag_context(**tags) if tags else nullcontext():
            chunks = client._chunks(data, target, prec, measurement, tag_columns, field_columns, time_column)
            offload = _is_large(data, client._chunk_size)
            future = WriteFuture(engine)
            try:
                while True:
                    item = await asyncio.to_thread(next, chunks, _DONE) if offload else next(chunks, _DONE)
                    if item is _DONE:
                        break
                    _add_dropped(future, item)
                    while True:
                        try:
                            if not engine.submit(target, prec, item.lines, item.nbytes, future, block=False):
                                future.dropped += len(item.lines)
                            break
                        except WouldBlock:
                            await asyncio.to_thread(engine.wait_for_space, item.nbytes)
            except (ValidationError, BufferFullError, ClientClosedError) as error:
                error.points_enqueued = future.points
                raise
            finally:
                future._seal()
        if not offload:
            # A coroutine that never suspends starves the event loop: yield now and then.
            self._unyielded += 1
            if self._unyielded >= _YIELD_EVERY:
                self._unyielded = 0
                await asyncio.sleep(0)
        return future

    async def flush(self, timeout: float | None = None) -> None:
        """Send everything buffered and wait for the server (see :meth:`InfluxClient.flush`)."""
        await asyncio.to_thread(self._client.flush, timeout)

    async def query(
        self,
        query: str,
        *,
        language: Language | None = None,
        database: str | None = None,
        params: Mapping[str, Any] | None = None,
        timeout: float | None = None,
    ) -> QueryResult:
        """Run a query in a worker thread (see :meth:`InfluxClient.query`)."""
        return await asyncio.to_thread(
            self._client.query, query, language=language, database=database, params=params, timeout=timeout
        )

    async def query_stream(
        self,
        query: str,
        *,
        language: Language | None = None,
        database: str | None = None,
        params: Mapping[str, Any] | None = None,
        timeout: float | None = None,
    ) -> AsyncIterator[QueryResult]:
        """Yield result chunks as they arrive (see :meth:`InfluxClient.query_stream`)."""
        iterator: Iterator[QueryResult] = self._client.query_stream(
            query, language=language, database=database, params=params, timeout=timeout
        )
        try:
            while True:
                chunk = await asyncio.to_thread(next, iterator, _DONE)
                if chunk is _DONE:
                    return
                yield chunk
        finally:
            close = getattr(iterator, "close", None)
            if close is not None:
                await asyncio.to_thread(close)

    async def ping(self) -> ServerInfo:
        """Check connectivity and return the server version (see :meth:`InfluxClient.ping`)."""
        return await asyncio.to_thread(self._client.ping)

    async def check(self) -> ServerInfo:
        """Verify version, token and database at startup (see :meth:`InfluxClient.check`)."""
        return await asyncio.to_thread(self._client.check)

    async def sync_schema(
        self, *, database: str | None = None, lookback: str = "30d"
    ) -> dict[str, dict[str, FieldType]]:
        """Lock field types to what the server stores (see :meth:`InfluxClient.sync_schema`)."""
        return await asyncio.to_thread(self._client.sync_schema, database=database, lookback=lookback)

    def to_line_protocol(self, data: Any, **kwargs: Any) -> list[str]:
        """The lines a write of ``data`` would send (see :meth:`InfluxClient.to_line_protocol`)."""
        return self._client.to_line_protocol(data, **kwargs)

    def stats(self) -> ClientStats:
        """Write counters and stage timings (see :meth:`InfluxClient.stats`)."""
        return self._client.stats()

    def locked_types(self, measurement: str, *, database: str | None = None) -> dict[str, FieldType]:
        """Field types declared or locked so far for ``measurement``."""
        return self._client.locked_types(measurement, database=database)

    async def close(self, timeout: float | None = None) -> None:
        """Flush and release resources without blocking the event loop."""
        await asyncio.to_thread(self._client.close, timeout)

    @property
    def closed(self) -> bool:
        return self._client.closed

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        await asyncio.to_thread(self._client.__exit__, exc_type, exc, tb)

    def __repr__(self) -> str:
        return "<Async" + repr(self._client)[1:]


def _is_large(data: Any, chunk_size: int) -> bool:
    """Whether serializing ``data`` could hold the event loop for more than a few milliseconds."""
    if isinstance(data, str | bytes):
        return len(data) > 100_000
    if isinstance(data, Mapping | Point) or hasattr(type(data), "__influxkit_model__"):
        return False
    if isinstance(data, list | tuple):
        return len(data) > 1_000
    if type(data).__name__ in ("DataFrame", "LazyFrame"):
        return True
    return not hasattr(data, "__len__") or len(data) > min(1_000, chunk_size)
