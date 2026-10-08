"""HTTP transport (urllib3) and translation of InfluxDB error responses into exceptions."""

from __future__ import annotations

import json
import logging
import re
import socket
import ssl
import time
import warnings
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote, urlencode

import urllib3
from urllib3.connection import HTTPConnection

from ._version import __version__
from .exceptions import (
    AuthenticationError,
    BadRequestError,
    InfluxConnectionError,
    InfluxTimeoutError,
    LineError,
    NotFoundError,
    PartialWriteError,
    PayloadTooLargeError,
    PermissionDeniedError,
    RateLimitedError,
    ServerError,
    ServiceUnavailableError,
    TransportError,
    UnprocessableEntityError,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from pydantic import SecretStr

    from .config import ConnectionConfig

__all__ = ["Response", "Transport", "error_from_response", "parse_retry_after"]

log = logging.getLogger("sluicebox.transport")

USER_AGENT = f"sluicebox/{__version__}"


def _keepalive_options() -> list[tuple[int, int, int]]:
    """TCP keepalive for pooled connections.

    Probes detect peers that vanished without closing the connection and keep NAT / load
    balancer state alive, so an idle pooled connection does not hang the next write.
    """
    options = [*HTTPConnection.default_socket_options, (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]
    idle = getattr(socket, "TCP_KEEPIDLE", None) or getattr(socket, "TCP_KEEPALIVE", None)  # Linux / macOS
    if idle is not None:
        options.append((socket.IPPROTO_TCP, idle, 30))
    if hasattr(socket, "TCP_KEEPINTVL"):
        options.append((socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10))
    if hasattr(socket, "TCP_KEEPCNT"):
        options.append((socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3))
    return options


_SOCKET_OPTIONS = _keepalive_options()
_HTML_TITLE = re.compile(r"<title>\s*(.*?)\s*</title>", re.IGNORECASE | re.DOTALL)
_V2_DROPPED = re.compile(r"dropped=(\d+)")
_STATUS_ERRORS: dict[int, type[ServerError]] = {
    400: BadRequestError,
    401: AuthenticationError,
    403: PermissionDeniedError,
    404: NotFoundError,
    413: PayloadTooLargeError,
    422: UnprocessableEntityError,
    429: RateLimitedError,
    503: ServiceUnavailableError,
}


@dataclass(frozen=True, slots=True)
class Response:
    status: int
    headers: Mapping[str, str]
    body: bytes


class Transport:
    """A thread-safe keep-alive connection pool to one InfluxDB server."""

    def __init__(
        self,
        config: ConnectionConfig,
        token: SecretStr | None,
        *,
        pool_size: int,
        token_hint: str | None = None,
    ) -> None:
        self.config = config
        self.base_url = config.url
        parsed = urllib3.util.parse_url(config.url)
        # A path in the URL (e.g. https://gateway/influx) prefixes every API path.
        self._path_prefix = (parsed.path or "").rstrip("/")
        self._origin = parsed._replace(path=None, query=None, fragment=None).url
        self._pool_size = pool_size
        self._token_hint = token_hint or "set the token in the environment or a .env file"
        scheme = "Bearer" if config.version == 3 else "Token"
        self._headers = {"User-Agent": USER_AGENT}
        if token is not None:
            self._headers["Authorization"] = f"{scheme} {token.get_secret_value()}"
        self._timeout = urllib3.Timeout(connect=config.connect_timeout, read=config.timeout)
        self._pool_timeout = max(1, round(config.timeout))
        if config.url.startswith("https://") and not config.verify_ssl:
            # The user opted out explicitly: say so once, instead of urllib3 warning on every request.
            host = urllib3.util.parse_url(config.url).host or ""
            warnings.filterwarnings(
                "ignore",
                message=f"Unverified HTTPS request is being made to host '{re.escape(host)}'",
                category=urllib3.exceptions.InsecureRequestWarning,
            )
            log.warning("TLS certificate verification is disabled for %s (verify_ssl = false)", config.url)
        self._pool = self._make_pool()

    def _make_pool(self) -> urllib3.connectionpool.HTTPConnectionPool:
        config = self.config
        options: dict[str, Any] = {
            "maxsize": self._pool_size,
            "block": True,  # wait for a free connection instead of opening throwaway ones
            "timeout": self._timeout,
            "retries": False,
            "socket_options": _SOCKET_OPTIONS,
        }
        if config.url.startswith("https://"):
            context = ssl.create_default_context(cafile=str(config.ca_cert) if config.ca_cert else None)
            if not config.verify_ssl:
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
            if config.client_cert is not None:
                context.load_cert_chain(
                    str(config.client_cert), str(config.client_key) if config.client_key else None
                )
            options["ssl_context"] = context
        manager: urllib3.PoolManager
        if config.proxy:
            # Credentials in the proxy URL become a Proxy-Authorization header; urllib3 never
            # sees them in the URL, so they cannot surface in its error messages.
            proxy = urllib3.util.parse_url(config.proxy)
            headers = urllib3.make_headers(proxy_basic_auth=unquote(proxy.auth)) if proxy.auth else None
            manager = urllib3.ProxyManager(
                proxy._replace(auth=None).url, num_pools=1, proxy_headers=headers, **options
            )
        else:
            manager = urllib3.PoolManager(num_pools=1, **options)
        self._manager = manager
        self._proxied = bool(config.proxy)
        return manager.connection_from_url(self._origin)

    def reset_after_fork(self) -> None:
        """Drop connections inherited from the parent process (sockets must not be shared)."""
        self._pool = self._make_pool()

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        body: bytes | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> Response:
        """Send one request. Raises :class:`TransportError` subclasses when no response arrives."""
        url = self._path_prefix + (f"{path}?{urlencode(params)}" if params else path)
        merged = {**self._headers, **headers} if headers else self._headers
        request_timeout = (
            urllib3.Timeout(connect=self.config.connect_timeout, read=timeout)
            if timeout is not None
            else self._timeout
        )
        try:
            if self._proxied:
                # The proxy manager sends absolute-URI requests (http) or tunnels (https).
                response = self._manager.urlopen(
                    method,
                    self._origin + url,
                    body=body,
                    headers=merged,
                    retries=False,
                    redirect=False,
                    timeout=request_timeout,
                    preload_content=True,
                )
            else:
                response = self._pool.urlopen(
                    method,
                    url,
                    body=body,
                    headers=merged,
                    retries=False,
                    redirect=False,
                    timeout=request_timeout,
                    pool_timeout=self._pool_timeout,  # never wait forever for a pooled connection
                    preload_content=True,
                    assert_same_host=False,
                )
        except urllib3.exceptions.EmptyPoolError as exc:
            raise InfluxTimeoutError(f"no free HTTP connection to {self.base_url} ({exc})") from exc
        except urllib3.exceptions.NewConnectionError as exc:
            # Checked before TimeoutError: urllib3 derives it from ConnectTimeoutError for historical reasons.
            raise InfluxConnectionError(f"cannot connect to {self.base_url}: {_reason(exc)}") from exc
        except urllib3.exceptions.TimeoutError as exc:
            raise InfluxTimeoutError(f"{method} {self.base_url}{path} timed out: {_reason(exc)}") from exc
        except urllib3.exceptions.SSLError as exc:
            reason = _reason(exc)
            error = InfluxConnectionError(f"TLS error talking to {self.base_url}: {reason}")
            if "WRONG_VERSION_NUMBER" in reason or "record layer failure" in reason:
                error.add_note("the server does not seem to speak TLS: try an http:// URL")
            error.retryable = False  # type: ignore[attr-defined]
            raise error from exc
        except (urllib3.exceptions.HTTPError, OSError) as exc:
            raise InfluxConnectionError(f"{method} {self.base_url}{path} failed: {_reason(exc)}") from exc
        return Response(response.status, response.headers, response.data or b"")

    def explain(self, error: BaseException) -> None:
        """Add a note to errors that the client configuration probably caused."""
        if isinstance(error, AuthenticationError) and "Authorization" not in self._headers:
            error.add_note(f"no token is configured: {self._token_hint}")

    def close(self) -> None:
        self._manager.clear()


def _reason(exc: BaseException) -> str:
    reason = getattr(exc, "reason", None)
    return str(reason or exc)


def is_retryable_transport_error(error: TransportError) -> bool:
    return bool(getattr(error, "retryable", True))


def parse_retry_after(value: str | None) -> float | None:
    """Seconds to wait from a ``Retry-After`` header (delta-seconds or HTTP date)."""
    if not value:
        return None
    value = value.strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, when.timestamp() - time.time())


def _parse_line_errors(data: Any) -> tuple[LineError, ...]:
    items = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
    errors = []
    for item in items:
        if not isinstance(item, dict):
            continue
        message = item.get("error_message")
        if not isinstance(message, str) or not message:
            continue
        number = item.get("line_number")
        line = item.get("original_line")
        errors.append(
            LineError(
                line_number=number if isinstance(number, int) else 0,
                message=message,
                line=line if isinstance(line, str) else None,
            )
        )
    return tuple(errors)


def is_html(response: Response) -> bool:
    """Whether a response is a web page (a UI or proxy page, not an InfluxDB API answer)."""
    if "html" in (response.headers.get("content-type") or "").lower():
        return True
    return response.body[:64].lstrip().lower().startswith((b"<!doctype html", b"<html"))


def unexpected_page_error(response: Response, path: str, url: str, version: int) -> ServerError:
    """A 2xx web page where InfluxDB returns an API response: the URL or version is wrong."""
    hint = (
        "an InfluxDB 2 server answers unknown API paths with its UI: if this is InfluxDB 2, "
        "set connection.version = 2"
        if version == 3
        else "check connection.url and connection.version"
    )
    return ServerError(
        f"{url}{path} answered with a web page instead of an InfluxDB {version} response, so nothing "
        f"was written: {hint}",
        status=response.status,
        code="unexpected_response",
        body=response.body[:1024],
    )


def error_from_response(response: Response) -> ServerError:
    """Build the exception for an error response, understanding InfluxDB 2 and 3 bodies."""
    status = response.status
    headers = response.headers
    body = response.body
    request_id = headers.get("x-request-id") or headers.get("request-id") or headers.get("trace-id")
    retry_after = parse_retry_after(headers.get("retry-after"))
    code: str | None = headers.get("x-platform-error-code")
    message = ""
    line_errors: tuple[LineError, ...] = ()
    partial = False
    rejected: int | None = None

    text = body.decode("utf-8", errors="replace").strip() if body else ""
    payload: Any = None
    if text[:1] in ("{", "["):
        try:
            payload = json.loads(text)
        except ValueError:
            payload = None
    if isinstance(payload, dict):
        # InfluxDB 2 and v2-compatible endpoints: {"code": ..., "message": ...}
        if isinstance(payload.get("message"), str):
            message = payload["message"]
            code = payload.get("code") if isinstance(payload.get("code"), str) else code
        # InfluxDB 3: {"error": ..., "data": ...}
        if isinstance(payload.get("error"), str):
            error_text = payload["error"]
            message = message or error_text
            line_errors = _parse_line_errors(payload.get("data"))
            if "partial write" in error_text.lower():
                partial = True
    elif text[:1] == "<":
        # An HTML page from a proxy or load balancer (e.g. nginx's 413): keep just its title.
        match = _HTML_TITLE.search(text)
        message = f"{match.group(1)} (HTML response from a proxy?)" if match else ""
    elif text:
        message = text[:2000]
    if not message:
        message = _REASONS.get(status, "error")
    if line_errors:
        details = "; ".join(f"line {err.line_number}: {err.message}" for err in line_errors[:3]) + (
            f"; ... {len(line_errors) - 3} more" if len(line_errors) > 3 else ""
        )
        message = f"{message}: {details}"
    if status == 422 and "partial write" in message:
        partial = True
        match = _V2_DROPPED.search(message)
        rejected = int(match.group(1)) if match else None

    kwargs: dict[str, Any] = {
        "status": status,
        "code": code,
        "body": body[:65536],
        "request_id": request_id,
        "retry_after": retry_after,
        "line_errors": line_errors,
    }
    if partial:
        return PartialWriteError(message, rejected=rejected, **kwargs)
    cls = _STATUS_ERRORS.get(status, ServerError)
    return cls(message, **kwargs)


_REASONS = {
    400: "bad request",
    401: "unauthorized",
    403: "forbidden",
    404: "not found",
    405: "method not allowed",
    413: "request entity too large",
    422: "unprocessable entity",
    429: "too many requests",
    500: "internal server error",
    502: "bad gateway",
    503: "service unavailable",
    504: "gateway timeout",
}
