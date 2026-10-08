"""Settings: non-secret configuration from TOML, secrets from the environment.

Sources, highest priority first:

1. Keyword overrides passed to :func:`load_settings` / ``InfluxSettings(...)``.
2. Environment variables (``SLUICEBOX_TOKEN``, ``SLUICEBOX_WRITE__BATCH_SIZE``, ...).
3. A ``.env`` file (``SLUICEBOX_*`` keys).
4. A secrets directory (one file per field, e.g. ``/run/secrets/sluicebox_token``).
5. The TOML configuration file.
6. Defaults.

The token is a secret: it is held as :class:`pydantic.SecretStr` (never printed or
logged) and is refused if it appears in the TOML file, which is meant to be committed.
"""

from __future__ import annotations

import difflib
import logging
import os
import re
import string
import tomllib
from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PrivateAttr,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic import (
    ValidationError as PydanticValidationError,
)
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from ._units import Bytes, Seconds
from .exceptions import ConfigurationError
from .types import FieldType, Precision

if TYPE_CHECKING:
    from pydantic.fields import FieldInfo

__all__ = [
    "ConnectionConfig",
    "EnvTag",
    "FieldPredicate",
    "InfluxSettings",
    "LoggingConfig",
    "MeasurementSchema",
    "MetricsConfig",
    "ProfilingConfig",
    "QueryConfig",
    "RetryConfig",
    "RuleCondition",
    "SettingsOrigin",
    "TagRule",
    "TagsConfig",
    "ValidationConfig",
    "WriteConfig",
    "load_settings",
]

DEFAULT_CONFIG_FILE = "sluicebox.toml"
DEFAULT_ENV_PREFIX = "SLUICEBOX_"
#: Variables (after the prefix) that steer load_settings() rather than holding a setting.
_SPECIAL_ENV = ("config", "section", "secrets_dir", "token_file")
_SECRET_KEYS = frozenset({"token"})


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


# ---------------------------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------------------------


def _parse_version(value: Any) -> Any:
    if isinstance(value, str):
        text = value.strip().lower().removeprefix("v")
        if text in {"2", "3"}:
            return int(text)
    return value


class ConnectionConfig(_Model):
    """Where and how to reach the server."""

    url: str = Field(description="Base URL, e.g. http://localhost:8181 (v3) or http://localhost:8086 (v2).")
    version: Annotated[Literal[2, 3], BeforeValidator(_parse_version)] = Field(
        description="Server generation: 2 (InfluxDB 2.x / Cloud TSM) or 3 (InfluxDB 3 Core/Enterprise/Cloud)."
    )
    database: str = Field(
        min_length=1,
        description="Default database (InfluxDB 3) or bucket (InfluxDB 2). Overridable per call.",
    )
    org: str | None = Field(default=None, description="Organization name; required for InfluxDB 2.")
    timeout: Seconds = Field(default=30.0, gt=0, description="Read timeout for a single request.")
    connect_timeout: Seconds = Field(default=5.0, gt=0, description="TCP/TLS connect timeout.")
    verify_ssl: bool = True
    ca_cert: Path | None = Field(
        default=None, description="PEM bundle used to verify the server certificate."
    )
    client_cert: Path | None = Field(default=None, description="Client certificate for mutual TLS.")
    client_key: Path | None = Field(default=None, description="Private key for the client certificate.")
    proxy: str | None = Field(default=None, description="HTTP(S) proxy URL.")
    pool_size: int | None = Field(
        default=None, ge=1, description="Max pooled HTTP connections (default: write concurrency + 2)."
    )

    @field_validator("url")
    @classmethod
    def _check_url(cls, value: str) -> str:
        value = value.strip().rstrip("/")
        if not re.match(r"^https?://[^/\s]+", value):
            raise ValueError("url must start with http:// or https:// followed by a host")
        # A path is kept as a prefix (e.g. behind a path-routing proxy), except an API path
        # pasted by mistake: sluicebox adds /api/v2/... and /api/v3/... itself.
        return re.sub(r"/api/v[23]$", "", value)

    @model_validator(mode="after")
    def _check_version_requirements(self) -> Self:
        if self.version == 2 and not self.org:
            raise ValueError("connection.org is required for InfluxDB 2")
        if self.client_key is not None and self.client_cert is None:
            raise ValueError("connection.client_key requires connection.client_cert")
        return self


# ---------------------------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------------------------


class RetryConfig(_Model):
    """Retry policy for transient write failures (network errors, 429, 5xx)."""

    max_attempts: int | None = Field(
        default=None,
        ge=1,
        description="Total attempts per batch, including the first. Unlimited by default: "
        "retry.max_elapsed bounds how long a batch is retried (ride out outages of that length).",
    )
    initial_delay: Seconds = Field(default=0.5, ge=0)
    max_delay: Seconds = Field(default=30.0, ge=0)
    multiplier: float = Field(default=2.0, ge=1.0)
    jitter: float = Field(
        default=0.2, ge=0.0, le=1.0, description="Random +/- fraction applied to each delay."
    )
    max_elapsed: Seconds | None = Field(
        default=300.0,
        gt=0,
        description="Give up on a batch once this much time has passed since its first attempt.",
    )
    retry_on_status: frozenset[int] = Field(default=frozenset({429, 500, 502, 503, 504}))

    @model_validator(mode="after")
    def _check_delays(self) -> Self:
        if self.max_delay < self.initial_delay:
            raise ValueError("retry.max_delay must be >= retry.initial_delay")
        return self


class WriteConfig(_Model):
    """Batching, buffering and transport options for writes."""

    precision: Precision = "ns"
    batch_size: int = Field(
        default=25_000,
        ge=1,
        description="Maximum lines per HTTP request (large batches amortize per-request cost).",
    )
    max_batch_bytes: Bytes = Field(
        default=Bytes(8 * 1024 * 1024),
        ge=1024,
        description="Maximum uncompressed bytes per request (InfluxDB 3 rejects bodies over 10 MiB).",
    )
    flush_interval: Seconds = Field(
        default=1.0, gt=0, description="Send a partially filled batch once it is this old."
    )
    concurrency: int = Field(
        default=16,
        ge=1,
        le=256,
        description=(
            "Sender threads = max requests in flight. InfluxDB 3 acknowledges writes on its WAL flush "
            "(every 1 s by default), so its throughput scales with requests in flight."
        ),
    )
    max_pending_bytes: Bytes = Field(
        default=Bytes(128 * 1024 * 1024),
        ge=1024,
        description=(
            "Upper bound on buffered + in-flight line protocol before backpressure applies. "
            "Process memory (RSS) can grow by about 3x this while the buffer is full."
        ),
    )
    on_full: Literal["block", "drop", "raise"] = Field(
        default="block",
        description="When the buffer is full: block the caller, drop new data, or raise BufferFullError.",
    )
    block_timeout: Seconds | None = Field(
        default=60.0,
        gt=0,
        description=(
            "With on_full='block', raise BufferFullError after waiting this long (None: wait for as "
            "long as it takes), so an unreachable server cannot stall the application forever."
        ),
    )
    gzip: bool = True
    gzip_level: int = Field(default=1, ge=1, le=9)
    gzip_min_bytes: Bytes = Field(
        default=Bytes(4096), ge=0, description="Smaller payloads are sent uncompressed."
    )
    auto_timestamp: bool = Field(
        default=True,
        description="Stamp points without a timestamp at write() time, so retries can never duplicate them.",
    )
    api: Literal["v2", "v3"] | None = Field(
        default=None,
        description=(
            "Write endpoint. Default: /api/v3/write_lp for InfluxDB 3 (partial writes with per-line errors), "
            "/api/v2/write for InfluxDB 2. Use 'v2' for InfluxDB 3 Cloud Serverless/Dedicated/Clustered."
        ),
    )
    no_sync: bool = Field(
        default=False, description="InfluxDB 3 write_lp: acknowledge before the WAL is persisted."
    )
    accept_partial: bool = Field(
        default=True, description="InfluxDB 3 write_lp: write valid lines even if some lines are rejected."
    )
    flush_on_exit: bool = Field(default=True, description="Flush buffered data when the interpreter exits.")
    close_timeout: Seconds = Field(
        default=20.0,
        gt=0,
        description="How long close()/exit waits for pending data (below Kubernetes' 30 s grace period).",
    )
    retry: RetryConfig = RetryConfig()

    @model_validator(mode="after")
    def _check_sizes(self) -> Self:
        if self.max_pending_bytes < self.max_batch_bytes:
            raise ValueError("write.max_pending_bytes must be >= write.max_batch_bytes")
        return self


# ---------------------------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------------------------


class QueryConfig(_Model):
    """Query defaults."""

    language: Literal["sql", "influxql", "flux"] | None = Field(
        default=None, description="Default query language: sql for InfluxDB 3, flux for InfluxDB 2."
    )
    timeout: Seconds = Field(default=120.0, gt=0)
    chunk_size: int = Field(
        default=10_000, ge=1, description="Rows per chunk yielded by query_stream() on InfluxDB 2."
    )


# ---------------------------------------------------------------------------------------------
# Validation and schemas
# ---------------------------------------------------------------------------------------------


class ValidationConfig(_Model):
    """Client-side validation applied to every record before it is buffered."""

    on_invalid: Literal["raise", "drop"] = Field(
        default="raise",
        description="raise: write() raises ValidationError. drop: skip the record (logged and counted).",
    )
    type_lock: bool = Field(
        default=True,
        description=(
            "Remember the type of every field the first time it is written and reject later values of a "
            "different type - the client-side equivalent of the server's field type conflict, caught "
            "before a whole batch is rejected."
        ),
    )
    unknown_measurements: Literal["allow", "reject"] = Field(
        default="allow", description="reject: only measurements declared under [measurements] may be written."
    )
    coerce: bool = Field(
        default=True,
        description="Safe numeric coercion towards the expected type: int -> float, integral float -> int.",
    )
    int_as_float: bool = Field(
        default=False, description="Write Python ints as floats unless a schema declares the field integer."
    )
    non_finite: Literal["skip", "error"] = Field(
        default="skip",
        description="NaN/inf cannot be stored: skip the field, or treat the record as invalid.",
    )
    naive_datetime: Literal["error", "utc"] = Field(
        default="error", description="Timezone-naive datetimes are ambiguous: reject them, or assume UTC."
    )
    max_string_bytes: Bytes = Field(default=Bytes(1024 * 1024), ge=1, description="Server limit is 1 MiB.")
    raw_lines: Literal["passthrough", "validate"] = Field(
        default="passthrough",
        description="Raw line protocol input: send as-is (fastest), or parse it and apply validation/tags.",
    )


def _parse_field_types(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: FieldType.parse(kind) for key, kind in value.items()}
    return value


class MeasurementSchema(_Model):
    """Declared schema for one measurement (table).

    Declared field types are always enforced; ``tags`` (if given) is the closed set of
    allowed tag keys.
    """

    fields: Annotated[dict[str, FieldType], BeforeValidator(_parse_field_types)] = {}
    tags: frozenset[str] | None = Field(default=None, description="Allowed tag keys; others are rejected.")
    required_tags: frozenset[str] = frozenset()
    required_fields: frozenset[str] = frozenset()
    extra_fields: Literal["allow", "forbid"] = Field(
        default="allow", description="Whether fields that are not declared under 'fields' may be written."
    )

    @model_validator(mode="after")
    def _check_consistency(self) -> Self:
        if self.tags is not None and not self.required_tags <= self.tags:
            missing = sorted(self.required_tags - self.tags)
            raise ValueError(f"required_tags {missing} are not listed in tags")
        undeclared = sorted(self.required_fields - self.fields.keys())
        if undeclared and self.extra_fields == "forbid":
            raise ValueError(f"required_fields {undeclared} are not declared in fields")
        overlap = sorted((self.tags or set()) & self.fields.keys())
        if overlap:
            raise ValueError(f"{overlap} declared as both tag and field")
        return self


# ---------------------------------------------------------------------------------------------
# Tag injection
# ---------------------------------------------------------------------------------------------


class FieldPredicate(_Model):
    """Conditions on a field value; all given operators must hold."""

    gt: float | None = None
    ge: float | None = None
    lt: float | None = None
    le: float | None = None
    eq: float | str | bool | None = None
    ne: float | str | bool | None = None
    in_: list[float | str | bool] | None = Field(default=None, alias="in")
    regex: str | None = None

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    @field_validator("regex")
    @classmethod
    def _compile(cls, value: str | None) -> str | None:
        if value is not None:
            re.compile(value)
        return value


class RuleCondition(_Model):
    """When a tag rule applies. Every given condition must match (logical AND).

    Regular expressions use :func:`re.search` semantics; anchor with ``^``/``$`` for full
    matches. Named groups (``(?P<site>...)``) become template variables for ``set``.
    """

    measurement: str | None = None
    tags: dict[str, str] = {}
    has_tags: frozenset[str] = frozenset()
    missing_tags: frozenset[str] = frozenset()
    has_fields: frozenset[str] = frozenset()
    fields: dict[str, FieldPredicate] = {}

    @field_validator("measurement")
    @classmethod
    def _compile_measurement(cls, value: str | None) -> str | None:
        if value is not None:
            re.compile(value)
        return value

    @field_validator("tags")
    @classmethod
    def _compile_tags(cls, value: dict[str, str]) -> dict[str, str]:
        for pattern in value.values():
            re.compile(pattern)
        return value

    def group_names(self) -> set[str]:
        names: set[str] = set()
        patterns = list(self.tags.values())
        if self.measurement:
            patterns.append(self.measurement)
        for pattern in patterns:
            names.update(re.compile(pattern).groupindex)
        return names


ConflictPolicy = Literal["keep", "overwrite", "error"]


class TagRule(_Model):
    """Add tags to points whose content matches ``when``.

    ``set`` values are templates: ``{name}`` refers to a named regex group, ``{measurement}``
    to the measurement, ``{tags[key]}`` / ``{fields[key]}`` to existing values.
    """

    name: str | None = None
    when: RuleCondition = RuleCondition()
    set: dict[str, str] = Field(min_length=1)
    on_conflict: ConflictPolicy | None = None

    @model_validator(mode="after")
    def _check_templates(self) -> Self:
        known = self.when.group_names() | {"measurement", "tags", "fields"}
        for key, template in self.set.items():
            try:
                parsed = list(string.Formatter().parse(template))
            except ValueError as exc:
                raise ValueError(f"invalid template for tag {key!r}: {exc}") from None
            for _, field_name, _, _ in parsed:
                if field_name is None:
                    continue
                root = re.split(r"[.\[]", field_name, maxsplit=1)[0]
                if not root or root.isdigit():
                    raise ValueError(
                        f"tag {key!r}: positional placeholders are not supported in {template!r}"
                    )
                if root not in known:
                    raise ValueError(
                        f"tag {key!r}: template {template!r} references {{{root}}}, which is neither a "
                        "named group of this rule's patterns nor one of measurement/tags/fields"
                    )
        return self


class EnvTag(_Model):
    """A tag value from an environment variable, with a fallback (``{ var = "...", default = "..." }``)."""

    var: str
    default: str = Field(description='Used when the variable is unset or empty; "" leaves the tag out.')


class TagsConfig(_Model):
    """Tags injected into every point (static, from environment, or by content rules)."""

    static: dict[str, str] = {}
    from_env: dict[str, str | EnvTag] = Field(
        default={},
        description="tag key -> environment variable name (required to be set), or "
        '{ var = "NAME", default = "value" }.',
    )
    on_conflict: ConflictPolicy = Field(
        default="keep",
        description="If an injected tag is already on the point: keep its value, overwrite, or error.",
    )
    rules: list[TagRule] = []


# ---------------------------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------------------------


class MetricsConfig(_Model):
    """Prometheus metrics."""

    enabled: bool = True
    namespace: str = Field(default="sluicebox", pattern=r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
    port: int | None = Field(
        default=None,
        ge=0,
        le=65535,
        description="Start a /metrics HTTP exporter on this port (once per process).",
    )
    addr: str = "0.0.0.0"  # noqa: S104 - an exporter must be reachable by the scraper


class ProfilingConfig(_Model):
    """Stage timing (always cheap) and slow-operation reporting."""

    enabled: bool = Field(default=True, description="Record per-stage timings into metrics and stats().")
    slow_batch_threshold: Seconds | None = Field(
        default=5.0, gt=0, description="Log a warning with a stage breakdown when a batch takes longer."
    )
    slow_query_threshold: Seconds | None = Field(default=10.0, gt=0)


class LoggingConfig(_Model):
    """Logging. sluicebox logs under the ``sluicebox`` logger and installs handlers only when asked."""

    configure: bool = Field(default=False, description="Install a handler on the 'sluicebox' logger.")
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    format: Literal["text", "json"] = "text"
    log_rejected_lines: int = Field(
        default=5, ge=0, description="How many rejected lines to include in partial-write warnings."
    )


# ---------------------------------------------------------------------------------------------
# Root settings
# ---------------------------------------------------------------------------------------------

_config_log = logging.getLogger("sluicebox.config")
_TOML_DATA: ContextVar[Mapping[str, Any] | None] = ContextVar("sluicebox_toml_data", default=None)


class _MappingSource(PydanticBaseSettingsSource):
    """Settings source backed by an already-parsed mapping (the TOML file)."""

    def __init__(self, settings_cls: type[BaseSettings], data: Mapping[str, Any]) -> None:
        super().__init__(settings_cls)
        self._data = data

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:  # noqa: ARG002
        return self._data.get(field_name), field_name, False

    def __call__(self) -> dict[str, Any]:
        return dict(self._data)


class _PrefixedDotEnvSource(PydanticBaseSettingsSource):
    """Wrap the dotenv source so a shared ``.env`` file may hold other applications' keys.

    pydantic-settings reports every unused ``.env`` key as an extra input when
    ``extra="forbid"``. Only keys carrying our prefix are ours to validate, so unknown
    prefixed keys (typos such as ``SLUICEBOX_TOKN``) still fail loudly.
    """

    def __init__(self, inner: PydanticBaseSettingsSource) -> None:
        super().__init__(inner.settings_cls)
        self._inner = inner
        self._prefix = str(getattr(inner, "env_prefix", "") or "").lower()

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        return self._inner.get_field_value(field, field_name)

    def __call__(self) -> dict[str, Any]:
        fields = self.settings_cls.model_fields
        special = {f"{self._prefix}{name}" for name in _SPECIAL_ENV}  # read by load_settings()
        return {
            key: value
            for key, value in self._inner().items()
            if key in fields
            or (self._prefix and key.lower().startswith(self._prefix) and key.lower() not in special)
        }


@dataclass(frozen=True, slots=True)
class SettingsOrigin:
    """Where :func:`load_settings` looked for settings (used to explain missing values)."""

    env_prefix: str = DEFAULT_ENV_PREFIX
    #: Absolute path of the ``.env`` file consulted, if any.
    env_file: Path | None = None
    config_file: Path | None = None

    def token_hint(self) -> str:
        where = "the environment"
        if self.env_file is not None:
            found = "" if self.env_file.is_file() else ", which does not exist"
            where += f" or {self.env_file}{found}"
        return f"set {self.env_prefix}TOKEN in {where}"


class InfluxSettings(BaseSettings):
    """Complete sluicebox configuration. Prefer :func:`load_settings` to build one."""

    model_config = SettingsConfigDict(
        env_prefix=DEFAULT_ENV_PREFIX,
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",
        frozen=True,
        validate_default=True,
    )

    name: str = Field(
        default="default", min_length=1, description="Client name used in metric labels and logs."
    )
    token: SecretStr | None = Field(
        default=None, description="API token. Secret: set SLUICEBOX_TOKEN in the environment or .env file."
    )
    connection: ConnectionConfig
    write: WriteConfig = WriteConfig()
    query: QueryConfig = QueryConfig()
    validation: ValidationConfig = ValidationConfig()
    measurements: dict[str, MeasurementSchema] = Field(
        default={}, description="Declared measurement schemas, keyed by measurement name."
    )
    tags: TagsConfig = TagsConfig()
    metrics: MetricsConfig = MetricsConfig()
    profiling: ProfilingConfig = ProfilingConfig()
    logging: LoggingConfig = LoggingConfig()

    _origin: SettingsOrigin = PrivateAttr(default_factory=SettingsOrigin)

    @property
    def origin(self) -> SettingsOrigin:
        """Where these settings were looked up (environment prefix, ``.env`` and config file)."""
        return self._origin

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        toml = _MappingSource(settings_cls, _TOML_DATA.get() or {})
        dotenv = _PrefixedDotEnvSource(dotenv_settings)
        return (init_settings, env_settings, dotenv, file_secret_settings, toml)

    @model_validator(mode="after")
    def _check_token(self) -> Self:
        if self.connection.version == 2 and self.token is None:
            raise ValueError("a token is required for InfluxDB 2")
        if self.token is not None and not self.token.get_secret_value().strip():
            raise ValueError("token is empty")
        return self

    def with_overrides(self, **overrides: Any) -> InfluxSettings:
        """A copy with ``overrides`` applied (nested tables are merged, as in :func:`load_settings`)."""
        merged = _deep_merge(self.model_dump(), overrides)
        try:
            settings = InfluxSettings(_env_file=None, **merged)  # type: ignore[call-arg]
        except PydanticValidationError as exc:
            raise ConfigurationError(
                f"invalid sluicebox settings (keyword arguments):\n{_format_errors(exc, self._origin, None)}"
            ) from None
        settings._origin = self._origin
        return settings

    @property
    def write_api(self) -> Literal["v2", "v3"]:
        """The write endpoint family in effect."""
        if self.write.api is not None:
            return self.write.api
        return "v3" if self.connection.version == 3 else "v2"

    @property
    def query_language(self) -> Literal["sql", "influxql", "flux"]:
        """The default query language in effect."""
        if self.query.language is not None:
            return self.query.language
        return "sql" if self.connection.version == 3 else "flux"

    def __repr_args__(self) -> Any:
        # SecretStr already masks the token; also hide whether one is set from casual reprs.
        for key, value in super().__repr_args__():
            yield (key, "**********" if key == "token" and value is not None else value)

    @classmethod
    def load(
        cls,
        config_file: str | os.PathLike[str] | None = None,
        *,
        section: str | None = None,
        env_file: str | os.PathLike[str] | None = ".env",
        env_prefix: str = DEFAULT_ENV_PREFIX,
        secrets_dir: str | os.PathLike[str] | None = None,
        **overrides: Any,
    ) -> InfluxSettings:
        """Alias of :func:`load_settings`."""
        return load_settings(
            config_file,
            section=section,
            env_file=env_file,
            env_prefix=env_prefix,
            secrets_dir=secrets_dir,
            **overrides,
        )


def _read_toml(path: Path, section: str | None) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            data: dict[str, Any] = tomllib.load(handle)
    except FileNotFoundError:
        raise ConfigurationError(f"config file not found: {path}") from None
    except tomllib.TOMLDecodeError as exc:
        raise ConfigurationError(f"invalid TOML in {path}: {exc}") from None
    if section:
        for part in section.split("."):
            node = data.get(part)
            if not isinstance(node, dict):
                raise ConfigurationError(f"section [{section}] not found in {path}")
            data = node
    leaked = sorted(_secret_paths(data))
    proxy = data.get("connection", {}).get("proxy") if isinstance(data.get("connection"), dict) else None
    if isinstance(proxy, str) and re.match(r"^[a-z][a-z0-9+.-]*://[^/@]*:[^/@]*@", proxy, re.IGNORECASE):
        leaked.append("connection.proxy (password)")
    if leaked:
        raise ConfigurationError(
            f"{path} contains {leaked}: secrets must not be stored in the config file. "
            f"Put them in the environment or a .env file (e.g. {DEFAULT_ENV_PREFIX}TOKEN=...)."
        )
    return data


def _deep_merge(base: dict[str, Any], extra: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in extra.items():
        if isinstance(value, Mapping) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _secret_paths(data: Mapping[str, Any], prefix: str = "") -> list[str]:
    """Dotted paths of secret keys anywhere in a TOML document (e.g. ``connection.token``)."""
    found = []
    for key, value in data.items():
        if key.lower() in _SECRET_KEYS:
            found.append(prefix + key)
        elif isinstance(value, Mapping):
            found += _secret_paths(value, f"{prefix}{key}.")
    return found


def _resolve_config_file(config_file: str | os.PathLike[str] | None, env_prefix: str) -> Path | None:
    if config_file is not None:
        return Path(config_file)
    from_env = os.environ.get(f"{env_prefix}CONFIG")
    if from_env:
        return Path(from_env)
    default = Path(DEFAULT_CONFIG_FILE)
    return default if default.is_file() else None


def load_settings(
    config_file: str | os.PathLike[str] | None = None,
    *,
    section: str | None = None,
    env_file: str | os.PathLike[str] | None = ".env",
    env_prefix: str = DEFAULT_ENV_PREFIX,
    secrets_dir: str | os.PathLike[str] | None = None,
    **overrides: Any,
) -> InfluxSettings:
    """Load settings from a TOML file, the environment, a ``.env`` file and keyword overrides.

    Args:
        config_file: TOML file. Defaults to ``$SLUICEBOX_CONFIG``, else ``./sluicebox.toml`` if it
            exists, else no file (environment only).
        section: Dotted path of a sub-table when the settings live inside a larger application
            config, e.g. ``"services.influx"``.
        env_file: ``.env`` file with secrets/overrides; ignored if it does not exist. ``None`` disables it.
        env_prefix: Prefix of environment variables; use distinct prefixes for several clients.
        secrets_dir: Directory with one file per secret (Docker/Kubernetes secrets).
        **overrides: Highest-priority values, e.g. ``token=...`` or ``write={"batch_size": 5000}``.

    Raises:
        ConfigurationError: the file is missing/invalid, contains a secret, or validation fails.
    """
    special = _special_variables(env_prefix, env_file)
    if section is None:
        section = special.get("section")
    if secrets_dir is None:
        secrets_dir = special.get("secrets_dir")
    token_file = special.get("token_file")
    if token_file and "token" not in overrides and not os.environ.get(f"{env_prefix}TOKEN"):
        try:
            overrides["token"] = Path(token_file).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise ConfigurationError(f"cannot read {env_prefix}TOKEN_FILE {token_file}: {exc}") from None
    if config_file is None and special.get("config"):
        config_file = special["config"]
    _warn_unknown_env(env_prefix)
    path = _resolve_config_file(config_file, env_prefix)
    data = _read_toml(path, section) if path is not None else {}
    origin = SettingsOrigin(
        env_prefix=env_prefix,
        env_file=Path(env_file).absolute() if env_file is not None else None,
        config_file=path.absolute() if path is not None else None,
    )
    reset = _TOML_DATA.set(data)
    try:
        settings = InfluxSettings(
            _env_file=env_file,  # type: ignore[call-arg]
            _env_prefix=env_prefix,
            _secrets_dir=secrets_dir,
            **overrides,
        )
    except PydanticValidationError as exc:
        sources = [f"config file {path}" + (f" [{section}]" if section else "")] if path else []
        sources.append("environment")
        if env_file is not None:
            sources.append(".env file")
        sources.append("keyword arguments")
        listed = ", ".join(sources)
        raise ConfigurationError(
            f"invalid sluicebox settings (from {listed}):\n{_format_errors(exc, origin, section)}"
        ) from None
    finally:
        _TOML_DATA.reset(reset)
    settings._origin = origin
    return settings


def _format_errors(exc: PydanticValidationError, origin: SettingsOrigin, section: str | None) -> str:
    prefix = origin.env_prefix
    lines = []
    for error in exc.errors(include_url=False, include_input=False):
        location = ".".join(str(part) for part in error["loc"])
        message = str(error["msg"]).removeprefix("Value error, ")
        if error["type"] == "missing" and location == "connection":
            if origin.config_file is None:
                message += (
                    f" and no config file was found (looked for {Path(DEFAULT_CONFIG_FILE).absolute()}): "
                    f"pass the file's path, set {prefix}CONFIG, or set {prefix}CONNECTION__URL, "
                    f"{prefix}CONNECTION__VERSION and {prefix}CONNECTION__DATABASE"
                )
            else:
                table = f"{section}.connection" if section else "connection"
                message += f" (add a [{table}] table to {origin.config_file})"
        elif error["type"] == "extra_forbidden":
            suggestion = _closest_field(error["loc"])
            if suggestion:
                message += f" (did you mean {suggestion!r}?)"
            variable = prefix + "__".join(str(part) for part in error["loc"]).upper()
            if variable in os.environ:
                message += f" [from environment variable {variable}]"
        if location == "token" or "token" in message:
            message += f": {origin.token_hint()}"
        lines.append(f"  - {location}: {message}" if location else f"  - {message}")
    return "\n".join(lines)


def _special_variables(env_prefix: str, env_file: str | os.PathLike[str] | None) -> dict[str, str]:
    """``{prefix}CONFIG``, ``SECTION``, ``SECRETS_DIR`` and ``TOKEN_FILE`` (environment, then ``.env``)."""
    from_file: dict[str, str | None] = {}
    if env_file is not None and Path(env_file).is_file():
        from dotenv import dotenv_values

        from_file = {key.lower(): value for key, value in dotenv_values(env_file).items()}
    found = {}
    for name in _SPECIAL_ENV:
        variable = f"{env_prefix}{name}".upper()
        value = os.environ.get(variable) or from_file.get(variable.lower())
        if value:
            found[name] = value
    return found


def _warn_unknown_env(env_prefix: str) -> None:
    """Log environment variables with our prefix that are no setting (typos are silent otherwise)."""
    prefix = env_prefix.lower()
    for variable in os.environ:
        lowered = variable.lower()
        if not lowered.startswith(prefix) or lowered[len(prefix) :] in _SPECIAL_ENV:
            continue
        path = tuple(lowered[len(prefix) :].split("__"))
        if _is_setting(path) or (len(path) > 1 and path[0] in InfluxSettings.model_fields):
            continue  # a setting, or inside a known table (where unknown keys are errors)
        suggestion = _env_suggestion(path, env_prefix)
        _config_log.warning(
            "environment variable %s is not an sluicebox setting and is ignored%s",
            variable,
            f" (did you mean {suggestion}?)" if suggestion else "",
        )


def _is_setting(path: tuple[str, ...]) -> bool:
    model: Any = InfluxSettings
    for part in path:
        fields = getattr(model, "model_fields", None)
        if fields is None or part not in fields:
            return False
        annotation = fields[part].annotation
        if not (isinstance(annotation, type) and issubclass(annotation, BaseModel)):
            return True  # a leaf, or a free-form table such as measurements
        model = annotation
    return bool(path)  # a whole table given as JSON


def _setting_paths(model: Any = None, prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    """Every leaf setting, as a path of field names."""
    model = model or InfluxSettings
    paths = []
    for name, info in model.model_fields.items():
        annotation = info.annotation
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            paths += _setting_paths(annotation, (*prefix, name))
        else:
            paths.append((*prefix, name))
    return paths


def _env_suggestion(path: tuple[str, ...], env_prefix: str) -> str | None:
    candidates = {"__".join(leaf): leaf for leaf in _setting_paths()}
    # SLUICEBOX_DATABASE -> SLUICEBOX_CONNECTION__DATABASE: same last part, different table.
    same_leaf = [key for key, leaf in candidates.items() if leaf[-1] == path[-1]]
    matches = same_leaf or difflib.get_close_matches("__".join(path), list(candidates), n=1, cutoff=0.7)
    return f"{env_prefix}{matches[0].upper()}" if matches else None


def _closest_field(loc: tuple[int | str, ...]) -> str | None:
    """The settings key closest to an unknown one, for "did you mean" hints."""
    model: Any = InfluxSettings
    for part in loc[:-1]:
        fields = getattr(model, "model_fields", None)
        if fields is None or part not in fields:
            return None
        annotation = fields[part].annotation
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            model = annotation
        else:
            return None  # e.g. the free-form keys of [measurements]
    candidates = list(getattr(model, "model_fields", {}))
    matches = difflib.get_close_matches(str(loc[-1]), candidates, n=1, cutoff=0.6)
    return matches[0] if matches else None
