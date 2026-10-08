"""influxkit: high-throughput, validated and observable writes and queries for InfluxDB 2 and 3.

Quick start::

    from influxkit import InfluxClient

    with InfluxClient.from_config("influxkit.toml") as client:   # token from .env
        client.write({"measurement": "cpu", "tags": {"host": "a"}, "fields": {"usage": 0.5}})
        rows = client.query("SELECT * FROM cpu LIMIT 10").to_dicts()
"""

from ._engine import EngineStats, WriteFailure
from ._lineprotocol import UInt
from ._version import __version__
from .aio import AsyncInfluxClient
from .client import ClientStats, InfluxClient, ServerInfo
from .config import InfluxSettings, LoggingConfig, MeasurementSchema, load_settings
from .exceptions import (
    AuthenticationError,
    BadRequestError,
    BufferFullError,
    ClientClosedError,
    ConfigurationError,
    InfluxConnectionError,
    InfluxKitError,
    InfluxTimeoutError,
    LineError,
    NotFoundError,
    PartialWriteError,
    PayloadTooLargeError,
    PermissionDeniedError,
    QueryError,
    RateLimitedError,
    ServerError,
    ServiceUnavailableError,
    TransportError,
    UnprocessableEntityError,
    ValidationError,
    WriteError,
)
from .futures import WriteFuture, WriteResult
from .log import JsonFormatter, configure_logging
from .models import Field, Tag, Timestamp, measurement
from .point import Point
from .profiling import ProfileReport, StageStats, profile
from .query import QueryResult
from .tags import Enricher, current_context_tags, tag_context
from .types import FieldType, Precision

__all__ = [
    "__version__",
    # clients
    "AsyncInfluxClient",
    "InfluxClient",
    "ClientStats",
    "EngineStats",
    "ServerInfo",
    # configuration
    "InfluxSettings",
    "MeasurementSchema",
    "LoggingConfig",
    "load_settings",
    # data
    "Point",
    "UInt",
    "FieldType",
    "Precision",
    "measurement",
    "Tag",
    "Field",
    "Timestamp",
    # writes and queries
    "WriteFuture",
    "WriteResult",
    "WriteFailure",
    "QueryResult",
    # tags
    "Enricher",
    "tag_context",
    "current_context_tags",
    # observability
    "configure_logging",
    "JsonFormatter",
    "profile",
    "ProfileReport",
    "StageStats",
    # errors
    "InfluxKitError",
    "ConfigurationError",
    "ValidationError",
    "ClientClosedError",
    "BufferFullError",
    "TransportError",
    "InfluxConnectionError",
    "InfluxTimeoutError",
    "ServerError",
    "BadRequestError",
    "AuthenticationError",
    "PermissionDeniedError",
    "NotFoundError",
    "PayloadTooLargeError",
    "UnprocessableEntityError",
    "RateLimitedError",
    "ServiceUnavailableError",
    "PartialWriteError",
    "LineError",
    "QueryError",
    "WriteError",
]
