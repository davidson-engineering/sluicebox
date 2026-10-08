"""Exception hierarchy.

Every exception raised by influxkit derives from :class:`InfluxKitError`, so callers can
catch one type at the integration boundary and still branch on the specific failure:

* :class:`ConfigurationError` - invalid settings, missing secrets or optional dependencies.
* :class:`ValidationError` - a record was rejected client-side before anything was sent.
* :class:`TransportError` - the server could not be reached (connection, TLS, timeout).
* :class:`ServerError` - the server answered with an HTTP error; subclasses map status codes.
* :class:`QueryError` - a query failed for a reason other than transport or authentication.
* :class:`WriteError` - aggregate of background write failures, raised by ``flush()``/``close()``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

__all__ = [
    "AuthenticationError",
    "BadRequestError",
    "BufferFullError",
    "ClientClosedError",
    "ConfigurationError",
    "InfluxConnectionError",
    "InfluxKitError",
    "InfluxTimeoutError",
    "LineError",
    "NotFoundError",
    "PartialWriteError",
    "PayloadTooLargeError",
    "PermissionDeniedError",
    "QueryError",
    "RateLimitedError",
    "ServerError",
    "ServiceUnavailableError",
    "TransportError",
    "UnprocessableEntityError",
    "ValidationError",
    "WriteError",
]


class InfluxKitError(Exception):
    """Base class for all influxkit errors."""


class ConfigurationError(InfluxKitError):
    """Settings are invalid, a secret is missing or misplaced, or an optional dependency is absent."""


class ClientClosedError(InfluxKitError, RuntimeError):
    """The client (or its write engine) was used after ``close()``."""

    #: Points of the failed ``write()`` call that were buffered before the error (and will be sent).
    points_enqueued = 0


class BufferFullError(InfluxKitError):
    """The write buffer is full and the configured overflow policy is ``raise`` (or blocking timed out)."""

    #: Points of the failed ``write()`` call that were buffered before the error (and will be sent).
    points_enqueued = 0


class ValidationError(InfluxKitError, ValueError):
    """A record failed client-side validation.

    ``code`` is a stable, machine-readable reason (for example ``type_conflict``,
    ``non_finite``, ``invalid_name``, ``missing_tag``) that is also used as the
    ``reason`` label of the dropped-points metric.
    """

    def __init__(
        self,
        message: str,
        *,
        code: str,
        measurement: str | None = None,
        key: str | None = None,
        index: int | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.measurement = measurement
        self.key = key
        self.index = index
        #: Points from the same ``write()`` call that were already handed to the write engine
        #: before this error was found (only non-zero for inputs larger than one batch).
        self.points_enqueued = 0

    def __str__(self) -> str:
        where = []
        if self.index is not None:
            where.append(f"record {self.index}")
        if self.measurement is not None:
            where.append(f"measurement {self.measurement!r}")
        if self.key is not None:
            where.append(f"key {self.key!r}")
        prefix = f"[{self.code}] "
        return prefix + (f"{self.message} ({', '.join(where)})" if where else self.message)


class TransportError(InfluxKitError):
    """The request did not produce an HTTP response (network, TLS or timeout failure)."""


class InfluxConnectionError(TransportError, ConnectionError):
    """Could not connect to the server, or the connection broke mid-request."""


class InfluxTimeoutError(TransportError, TimeoutError):
    """The request timed out (connect, read, or waiting for a pooled connection)."""


@dataclass(frozen=True, slots=True)
class LineError:
    """A single line rejected by the server.

    ``line_number`` is 1-based within the request that was sent; ``line`` is the
    offending line protocol when the server reported it.
    """

    line_number: int
    message: str
    line: str | None = None


class ServerError(InfluxKitError):
    """The server answered with an HTTP error status."""

    def __init__(
        self,
        message: str,
        *,
        status: int,
        code: str | None = None,
        body: bytes = b"",
        request_id: str | None = None,
        retry_after: float | None = None,
        line_errors: tuple[LineError, ...] = (),
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status = status
        self.code = code
        self.body = body
        self.request_id = request_id
        self.retry_after = retry_after
        self.line_errors = line_errors

    def __str__(self) -> str:
        text = f"HTTP {self.status}: {self.message}"
        if self.request_id:
            text += f" (request id {self.request_id})"
        return text

    def __reduce__(self) -> tuple[Any, ...]:
        # Keep keyword-only state when errors cross process/thread boundaries via pickling.
        return (_rebuild_server_error, (type(self), self.message, self.__dict__))


def _rebuild_server_error(cls: type[ServerError], message: str, state: dict[str, Any]) -> ServerError:
    error = cls.__new__(cls)
    Exception.__init__(error, message)
    error.__dict__.update(state)
    return error


class BadRequestError(ServerError):
    """HTTP 400: the request (usually line protocol or a query) was malformed or rejected."""


class AuthenticationError(ServerError):
    """HTTP 401: the token is missing, invalid or expired."""


class PermissionDeniedError(ServerError):
    """HTTP 403: the token is valid but lacks permission for the database/bucket."""


class NotFoundError(ServerError):
    """HTTP 404: the bucket, database, organization or endpoint does not exist."""


class PayloadTooLargeError(ServerError):
    """HTTP 413: the request body exceeded the server's limit (batches are split automatically)."""


class UnprocessableEntityError(ServerError):
    """HTTP 422: the server understood the request but refused (some of) its content."""


class RateLimitedError(ServerError):
    """HTTP 429: the server is throttling requests; ``retry_after`` holds its hint if given."""


class ServiceUnavailableError(ServerError):
    """HTTP 503: the server is temporarily unable to handle requests."""


class PartialWriteError(ServerError):
    """The server wrote part of a batch and rejected the rest.

    ``line_errors`` lists the rejected lines when the server reports them (InfluxDB 3
    ``/api/v3/write_lp``). InfluxDB 2 only reports the first conflict and a count, which
    is exposed as ``rejected``.
    """

    def __init__(self, message: str, *, rejected: int | None = None, **kwargs: Any) -> None:
        super().__init__(message, **kwargs)
        self.rejected = rejected if rejected is not None else len(self.line_errors)


class QueryError(InfluxKitError):
    """A query failed (syntax/planning error, unknown table, ...)."""

    def __init__(self, message: str, *, query: str | None = None, status: int | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.query = query
        self.status = status


class WriteError(InfluxKitError):
    """One or more background write batches failed permanently.

    Raised by ``flush()`` and ``close()`` when no ``on_error`` handler is installed, so
    fire-and-forget callers still learn about lost data. ``errors`` holds the individual
    exceptions (capped), while the counters cover every failure since the previous flush.
    """

    def __init__(self, message: str, *, errors: list[BaseException], failed_points: int, failed_batches: int):
        super().__init__(message)
        self.errors = errors
        self.failed_points = failed_points
        self.failed_batches = failed_batches
