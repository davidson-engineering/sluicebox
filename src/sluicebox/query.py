"""Queries, delegated to the official clients.

* InfluxDB 3: SQL or InfluxQL over Arrow Flight via ``influxdb3-python`` (the ``v3``
  extra). Results stay columnar (Arrow) end to end, which is by far the fastest way to
  read large result sets.
* InfluxDB 2: Flux via ``influxdb-client`` (the ``v2`` extra).

Both return :class:`QueryResult`, convertible to Arrow, polars, pandas or plain dicts.
"""

from __future__ import annotations

import logging
import math
import os
import re
import time
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal, cast

from .exceptions import (
    AuthenticationError,
    ConfigurationError,
    InfluxConnectionError,
    InfluxTimeoutError,
    PermissionDeniedError,
    QueryError,
    ServerError,
    SluiceboxError,
)

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyarrow as pa

    from .config import InfluxSettings

__all__ = ["QueryResult"]

log = logging.getLogger("sluicebox.query")

Language = Literal["sql", "influxql", "flux"]


class QueryResult:
    """Rows returned by a query (or one chunk of a streamed query).

    The native form is an Arrow table (InfluxDB 3) or a list of dicts (InfluxDB 2);
    conversions are computed on demand.
    """

    __slots__ = ("_records", "_table", "duration", "language", "query")

    def __init__(
        self,
        *,
        table: pa.Table | None = None,
        records: list[dict[str, Any]] | None = None,
        query: str = "",
        language: str = "",
        duration: float = 0.0,
    ) -> None:
        if (table is None) == (records is None):
            raise ValueError("QueryResult needs exactly one of table or records")
        self._table = table
        self._records = records
        self.query = query
        self.language = language
        #: Seconds spent executing the query and transferring this result.
        self.duration = duration

    @property
    def num_rows(self) -> int:
        return self._table.num_rows if self._table is not None else len(self._records or ())

    def __len__(self) -> int:
        return self.num_rows

    @property
    def columns(self) -> list[str]:
        if self._table is not None:
            return list(self._table.column_names)
        names: dict[str, None] = {}
        for record in self._records or ():
            names.update(dict.fromkeys(record))
        return list(names)

    def to_arrow(self) -> pa.Table:
        """As a ``pyarrow.Table`` (zero-copy for InfluxDB 3)."""
        if self._table is not None:
            return self._table
        pyarrow = _require("pyarrow", "v3")
        return pyarrow.Table.from_pylist(self._records or [])

    def to_polars(self) -> pl.DataFrame:
        """As a ``polars.DataFrame``."""
        polars = _require("polars", "polars")
        if self._table is not None:
            return cast("pl.DataFrame", polars.from_arrow(self._table))
        if not self._records:
            return cast("pl.DataFrame", polars.DataFrame())  # no rows (InfluxDB 2 sends no schema then)
        return cast("pl.DataFrame", polars.from_dicts(self._records, infer_schema_length=None))

    def to_pandas(self) -> pd.DataFrame:
        """As a ``pandas.DataFrame``."""
        pandas = _require("pandas", "pandas")
        if self._table is not None:
            return cast("pd.DataFrame", self._table.to_pandas())
        return cast("pd.DataFrame", pandas.DataFrame.from_records(self._records or []))

    def to_dicts(self) -> list[dict[str, Any]]:
        """As a list of ``{column: value}`` dicts."""
        if self._records is not None:
            return self._records
        assert self._table is not None
        return self._table.to_pylist()  # type: ignore[no-any-return]

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self.to_dicts())

    def __repr__(self) -> str:
        return f"<QueryResult rows={self.num_rows} columns={self.columns[:8]} {self.duration * 1000:.1f} ms>"


_SOURCE = "git+https://github.com/davidson-engineering/sluicebox"


def _install_hint(extra: str) -> str:
    return f"install sluicebox with the '{extra}' extra (uv add 'sluicebox[{extra}] @ {_SOURCE}')"


def _require(module: str, extra: str) -> Any:
    try:
        return __import__(module)
    except ImportError:
        raise ConfigurationError(f"{module} is required for this operation: {_install_hint(extra)}") from None


# ---------------------------------------------------------------------------------------------
# InfluxDB 3 (Arrow Flight via influxdb3-python)
# ---------------------------------------------------------------------------------------------


#: The process that created an Arrow Flight (gRPC) client. gRPC does not survive fork(): any
#: Flight query in a child forked after that hangs forever, so it is refused instead.
_flight_pid: int | None = None


class V3QueryBackend:
    def __init__(self, settings: InfluxSettings) -> None:
        global _flight_pid
        try:
            from influxdb_client_3 import InfluxDBClient3
        except ImportError:
            raise ConfigurationError(
                f"querying InfluxDB 3 needs the official client: {_install_hint('v3')}"
            ) from None
        self.pid = os.getpid()
        if _flight_pid is not None and _flight_pid != self.pid:
            raise QueryError(
                "InfluxDB 3 queries cannot run in a process forked after its parent created a query "
                "client: Arrow Flight (gRPC) does not support fork() and the query would hang. Create "
                "clients that query only after forking (e.g. in a gunicorn post_fork hook) or start "
                "workers with multiprocessing's 'spawn' or 'forkserver' method. Writes are not affected."
            )
        _flight_pid = self.pid
        conn = settings.connection
        kwargs: dict[str, Any] = {
            "host": conn.url,
            "database": conn.database,
            "token": settings.token.get_secret_value() if settings.token else "",
            "query_timeout": int(settings.query.timeout * 1000),
            "verify_ssl": conn.verify_ssl,
        }
        if conn.ca_cert:
            kwargs["ssl_ca_cert"] = str(conn.ca_cert)
        if conn.proxy:
            kwargs["proxy"] = conn.proxy
        if conn.org:
            kwargs["org"] = conn.org
        if conn.client_cert:
            # Mutual TLS for the Flight (gRPC) channel; the HTTP side gets it in the transport.
            key_path = conn.client_key or conn.client_cert
            kwargs["flight_client_options"] = {
                "cert_chain": conn.client_cert.read_bytes(),
                "private_key": key_path.read_bytes(),
            }
        self._client = InfluxDBClient3(**kwargs)
        self._timeout = settings.query.timeout

    def query(
        self,
        query: str,
        language: Language,
        database: str,
        params: Mapping[str, Any] | None,
        timeout: float | None,
    ) -> pa.Table:
        kwargs = self._call_options(params, timeout)
        try:
            table = self._client.query(query, language=language, mode="all", database=database, **kwargs)
        except Exception as exc:
            raise _map_v3_error(exc, query) from exc
        return _utc_timestamps(table)

    def stream(
        self,
        query: str,
        language: Language,
        database: str,
        params: Mapping[str, Any] | None,
        timeout: float | None,
    ) -> Iterator[pa.Table]:
        import pyarrow as pa

        kwargs = self._call_options(params, timeout)
        try:
            reader = self._client.query(query, language=language, mode="reader", database=database, **kwargs)
            for batch in reader:
                yield _utc_timestamps(pa.Table.from_batches([batch]))
        except SluiceboxError:
            raise
        except Exception as exc:
            raise _map_v3_error(exc, query) from exc

    def _call_options(self, params: Mapping[str, Any] | None, timeout: float | None) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"timeout": timeout if timeout is not None else self._timeout}
        if params:
            kwargs["query_parameters"] = {key: _sql_parameter(value) for key, value in params.items()}
        return kwargs

    def close(self) -> None:
        self._client.close()


def _sql_parameter(value: Any) -> Any:
    """Parameters travel as JSON: send datetimes as RFC 3339 strings (SQL compares them as times)."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("datetime query parameters must be timezone-aware")
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return value


def _utc_timestamps(table: pa.Table) -> pa.Table:
    """Mark zone-less timestamp columns as UTC (InfluxDB stores UTC; this is metadata only)."""
    import pyarrow as pa

    for index, column in enumerate(table.schema):
        kind = column.type
        if pa.types.is_timestamp(kind) and kind.tz is None:
            utc = table.column(index).cast(pa.timestamp(kind.unit, tz="UTC"))
            table = table.set_column(index, column.name, utc)
    return table


def _map_v3_error(exc: BaseException, query: str) -> SluiceboxError:
    """Translate Arrow Flight errors (possibly wrapped by influxdb3-python)."""
    try:
        from pyarrow import flight
    except ImportError:  # pragma: no cover - influxdb3-python depends on pyarrow
        flight = None
    cause: BaseException = exc
    if (
        flight is not None
        and not isinstance(exc, flight.FlightError)
        and isinstance(exc.__context__, Exception)
    ):
        cause = exc.__context__
    message = _flight_message(cause)
    if flight is not None:
        if isinstance(cause, flight.FlightUnauthenticatedError):
            return AuthenticationError(message, status=401)
        if isinstance(cause, flight.FlightUnauthorizedError):
            return PermissionDeniedError(message, status=403)
        if isinstance(cause, flight.FlightTimedOutError):
            return InfluxTimeoutError(f"query timed out: {message}")
        if isinstance(cause, flight.FlightUnavailableError):
            return InfluxConnectionError(f"query failed, server unavailable: {message}")
        if isinstance(cause, flight.FlightCancelledError):
            return QueryError(f"query cancelled: {message}", query=query)
    lowered = message.lower()
    if "database not found" in lowered or ("not found" in lowered and "database" in lowered):
        return QueryError(message, query=query, status=404)
    return QueryError(message, query=query)


def _flight_message(exc: BaseException) -> str:
    text = str(exc).strip()
    # "Flight returned internal error, with message: ... . gRPC client debug context: ..."
    if "with message:" in text:
        text = text.split("with message:", 1)[1]
    if "gRPC client debug context" in text:
        text = text.split("gRPC client debug context", 1)[0]
    text = text.strip().rstrip(".").strip()
    return text or type(exc).__name__


# ---------------------------------------------------------------------------------------------
# InfluxDB 2 (Flux via influxdb-client)
# ---------------------------------------------------------------------------------------------


class V2QueryBackend:
    def __init__(self, settings: InfluxSettings) -> None:
        try:
            from influxdb_client import InfluxDBClient
        except ImportError:
            raise ConfigurationError(
                f"querying InfluxDB 2 needs the official client: {_install_hint('v2')}"
            ) from None
        self.pid = os.getpid()
        conn = settings.connection
        assert settings.token is not None
        kwargs: dict[str, Any] = {
            "url": conn.url,
            "token": settings.token.get_secret_value(),
            "org": conn.org,
            "timeout": int(settings.query.timeout * 1000),
            "verify_ssl": conn.verify_ssl,
            "retries": False,
        }
        if conn.ca_cert:
            kwargs["ssl_ca_cert"] = str(conn.ca_cert)
        if conn.client_cert:
            kwargs["cert_file"] = str(conn.client_cert)
        if conn.client_key:
            kwargs["cert_key_file"] = str(conn.client_key)
        if conn.proxy:
            kwargs["proxy"] = conn.proxy
        self._client = InfluxDBClient(**kwargs)
        self._api = self._client.query_api()
        self._org = conn.org

    def records(self, query: str, params: Mapping[str, Any] | None) -> Iterator[dict[str, Any]]:
        if params:
            query = flux_params(params) + query
        try:
            for record in self._api.query_stream(query, org=self._org):
                yield record.values
        except SluiceboxError:
            raise
        except Exception as exc:
            raise _map_v2_error(exc, query) from exc

    def tables(self, query: str, params: Mapping[str, Any] | None) -> Any:
        if params:
            query = flux_params(params) + query
        try:
            return self._api.query(query, org=self._org)
        except Exception as exc:
            raise _map_v2_error(exc, query) from exc

    def close(self) -> None:
        self._client.close()


_FLUX_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FLUX_STRING_ESCAPES = str.maketrans({"\\": "\\\\", '"': '\\"', "\n": "\\n", "\r": "\\r", "\t": "\\t"})


def flux_params(params: Mapping[str, Any]) -> str:
    """Render ``params`` as ``option params = {...}`` with safely escaped Flux literals.

    Queries reference them as ``params.name`` - the syntax of InfluxDB Cloud's parameterized
    queries - and the binding also works on InfluxDB OSS, which lacks that API feature.
    Values never become Flux code, so this is injection-safe.
    """
    return f"option params = {_flux_literal(dict(params))}\n"


def _flux_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"Flux parameters cannot be {value!r}")
        return f'float(v: "{value!r}")'
    if isinstance(value, str):
        return '"' + value.translate(_FLUX_STRING_ESCAPES).replace("${", "\\${") + '"'
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("Flux time parameters must be timezone-aware datetimes")
        utc = value.astimezone(UTC)
        nanos = getattr(value, "nanosecond", 0)
        return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond * 1000 + nanos:09d}Z"
    if isinstance(value, timedelta):
        return f"{value // timedelta(microseconds=1)}us"
    if isinstance(value, Mapping):
        items = []
        for key, item in value.items():
            if not isinstance(key, str) or not _FLUX_IDENTIFIER.match(key):
                raise ValueError(f"Flux parameter names must be identifiers, got {key!r}")
            items.append(f"{key}: {_flux_literal(item)}")
        return "{" + ", ".join(items) + "}"
    if isinstance(value, list | tuple):
        return "[" + ", ".join(_flux_literal(item) for item in value) + "]"
    raise ValueError(f"unsupported Flux parameter type {type(value).__name__}")


def _map_v2_error(exc: BaseException, query: str) -> SluiceboxError:
    status = getattr(exc, "status", None)
    message = getattr(exc, "message", None) or str(exc)
    if isinstance(message, bytes):
        message = message.decode("utf-8", errors="replace")
    if isinstance(status, int):
        if status == 401:
            return AuthenticationError(str(message), status=401)
        if status == 403:
            return PermissionDeniedError(str(message), status=403)
        if status in (502, 503, 504):
            return ServerError(str(message), status=status)
        # 404 (unknown bucket) and 500 (Flux runtime errors) are failures of this query.
        return QueryError(str(message), query=query, status=status)
    try:
        import urllib3
    except ImportError:  # pragma: no cover
        urllib3 = None  # type: ignore[assignment]
    if urllib3 is not None:
        if isinstance(exc, urllib3.exceptions.TimeoutError):
            return InfluxTimeoutError(f"query timed out: {exc}")
        if isinstance(exc, urllib3.exceptions.HTTPError | OSError):
            return InfluxConnectionError(f"query failed: {exc}")
    return QueryError(str(message), query=query)


def run_query(
    backend: V2QueryBackend | V3QueryBackend,
    query: str,
    language: Language,
    database: str,
    params: Mapping[str, Any] | None,
    timeout: float | None,
) -> QueryResult:
    started = time.perf_counter()
    if isinstance(backend, V3QueryBackend):
        table = backend.query(query, language, database, params, timeout)
        return QueryResult(
            table=table, query=query, language=language, duration=time.perf_counter() - started
        )
    records = list(backend.records(query, params))
    return QueryResult(
        records=records, query=query, language=language, duration=time.perf_counter() - started
    )


def stream_query(
    backend: V2QueryBackend | V3QueryBackend,
    query: str,
    language: Language,
    database: str,
    params: Mapping[str, Any] | None,
    timeout: float | None,
    chunk_size: int,
) -> Iterator[QueryResult]:
    started = time.perf_counter()
    if isinstance(backend, V3QueryBackend):
        for table in backend.stream(query, language, database, params, timeout):
            yield QueryResult(
                table=table, query=query, language=language, duration=time.perf_counter() - started
            )
            started = time.perf_counter()
        return
    chunk: list[dict[str, Any]] = []
    for record in backend.records(query, params):
        chunk.append(record)
        if len(chunk) >= chunk_size:
            yield QueryResult(
                records=chunk, query=query, language=language, duration=time.perf_counter() - started
            )
            chunk = []
            started = time.perf_counter()
    if chunk:
        yield QueryResult(
            records=chunk, query=query, language=language, duration=time.perf_counter() - started
        )
