# sluicebox

High-throughput, validated and observable writes and queries for **InfluxDB 2** and **InfluxDB 3**,
built to drop into an application and push large volumes of data as fast as the server accepts them.

- **Asynchronous writes**: `write()` validates and serializes on the calling thread, buffers, and
  returns at once; background threads batch, gzip and send with up to 16 requests in flight,
  retrying transient failures. Await the returned future when you need the acknowledgement.
- **Fast**: about 1.1 M points/s serialization per thread with full validation, 3x the official
  clients; polars DataFrames are serialized vectorized. End to end, 5-50x the official clients (below).
- **Validation that prevents bad data**: field types are locked on first write and corrected from
  the server's own type errors (no more `field type conflict` batches), schemas can be declared,
  identifiers are checked against what each server version really accepts, NaN/inf, naive
  datetimes, out-of-range values and timestamps in the wrong unit are caught before sending.
- **Content-aware tag injection**: static tags, environment tags, request-scoped context tags, and
  rules that derive tags from a point's measurement, tags and field values (with regex captures).
- **Configuration** in TOML, **secrets** only from the environment / `.env` / secret files (pydantic `SecretStr`).
- **Prometheus metrics**, structured **logging**, per-stage **profiling** and a cProfile helper.
- **Typed exceptions** for every failure, including per-line attribution of partial writes.
- Sync and **asyncio** APIs; thread-safe (tested on free-threaded Python 3.14t); fork-aware.

Queries go through the official clients: Arrow Flight (SQL / InfluxQL) for InfluxDB 3 via
`influxdb3-python`, Flux for InfluxDB 2 via `influxdb-client`. The write path uses its own
urllib3 transport, so ingest-only applications need neither.

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Writing](#writing)
- [Choosing tags](#choosing-tags)
- [Configuration and secrets](#configuration-and-secrets)
- [Validation and schemas](#validation-and-schemas)
- [Tag injection](#tag-injection)
- [Queries](#queries)
- [Errors](#errors)
- [Observability](#observability)
- [Running in production](#running-in-production)
- [Performance and tuning](#performance-and-tuning)
- [Remote servers, TLS and proxies](#remote-servers-tls-and-proxies)
- [Server behaviour handled for you](#server-behaviour-handled-for-you)
- [Development](#development)

## Install

sluicebox is installed from GitHub (it is not on PyPI). Pick the extras you need:

```bash
uv add 'sluicebox @ git+https://github.com/davidson-engineering/sluicebox'           # writes only
uv add 'sluicebox[v3] @ git+https://github.com/davidson-engineering/sluicebox'       # + InfluxDB 3 queries
uv add 'sluicebox[v2] @ git+https://github.com/davidson-engineering/sluicebox'       # + InfluxDB 2 queries
uv add 'sluicebox[polars] @ git+https://github.com/davidson-engineering/sluicebox'   # + polars DataFrames
uv add 'sluicebox[all] @ git+https://github.com/davidson-engineering/sluicebox'      # all, plus pandas
```

| Extra | Adds | For |
| --- | --- | --- |
| (none) | urllib3, pydantic, pydantic-settings, prometheus-client | writes |
| `v3` | influxdb3-python, pyarrow | InfluxDB 3 queries (SQL, InfluxQL) |
| `v2` | influxdb-client | InfluxDB 2 queries (Flux) |
| `polars`, `pandas` | polars / pandas | writing DataFrames, query results as DataFrames |
| `all` | all of the above | |

Append `@<tag-or-commit>` to the URL to pin a version. From a local checkout:
`uv add '/path/to/sluicebox[v3]'`. Python 3.11+; `pip install` takes the same specifiers.

## Quick start

`sluicebox.toml` (non-secret settings; full reference in [`sluicebox.example.toml`](sluicebox.example.toml)):

```toml
[connection]
url = "http://localhost:8181"
version = 3              # or 2 (then also: org = "my-org")
database = "telemetry"   # bucket on InfluxDB 2
```

`.env` (or real environment variables; never commit it):

```bash
SLUICEBOX_TOKEN=apiv3_...
```

```python
from sluicebox import InfluxClient, Point

with InfluxClient.from_config() as client:  # reads ./sluicebox.toml and ./.env
    client.check()  # fail fast: wrong URL, version, token or bucket

    # Asynchronous: returns immediately, sent in the background.
    client.write({"measurement": "cpu", "tags": {"host": "web-1"}, "fields": {"usage": 0.42}})
    client.write(Point("cpu").tag("host", "web-2").field("usage", 0.17))

    # Synchronous: wait for the server's acknowledgement (raises on failure).
    batch = [
        {"measurement": "cpu", "tags": {"host": f"web-{i}"}, "fields": {"usage": i / 10}} for i in range(10)
    ]
    result = client.write(batch).result()

    for row in client.query("SELECT * FROM cpu WHERE time > now() - interval '1 hour'"):
        print(row)  # or .to_polars(), .to_pandas(), .to_arrow() with those extras
# Leaving the block flushes everything buffered.
```

InfluxDB 3 Core acknowledges a write when its write-ahead log is persisted (every second by
default), so a single synchronous write takes up to a second. Throughput comes from many writes
in flight, not from waiting on each; `write.no_sync = true` acknowledges in about a millisecond
with weaker durability.

`client.to_line_protocol(data)` returns the lines `write(data)` would send, with validation and
tag injection applied, without sending them.

## Writing

### Inputs

`write()` accepts any of these, or an iterable mixing them:

| Input | Example |
| --- | --- |
| dict (or any mapping) | `{"measurement": "cpu", "tags": {...}, "fields": {...}, "time": ...}` |
| `Point` | `Point("cpu").tag("host", "a").field("usage", 0.5).time(ts)` |
| `@measurement` model | dataclass or pydantic model (see below) |
| line protocol | `"cpu,host=a usage=0.5 1700000000000000000"` (str or bytes, many lines allowed) |
| polars / pandas DataFrame | `client.write(df, measurement="cpu", tag_columns=["host"])` |

Timestamps may be ints in the write precision, timezone-aware `datetime`s, ISO 8601 strings
(nanoseconds kept), float epoch seconds, or numpy/pandas timestamps and integers. Values
InfluxDB cannot store (before 1677 or after 2262) are rejected; integer timestamps that land
before 1973 log a warning, since that is what epoch seconds written with `precision="ns"` look
like. Points without a timestamp get the time of the `write()` call (`write.auto_timestamp`),
which makes retries idempotent. Untimed points of the same series written in one call therefore
share a timestamp and overwrite each other, as with server-assigned times; sluicebox logs a
warning when that happens.

DataFrames: a column named `time` (or a datetime column named `timestamp`, or a pandas
`DatetimeIndex`) is the timestamp unless `time_column=` says otherwise; it may hold datetimes,
integers in the write precision, or ISO 8601 strings. All other columns that are not
`tag_columns` are fields.

Typed models declare their field types once, so they can never write an inconsistent type:

```python
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated
from sluicebox import Tag, measurement


@measurement("cpu")
@dataclass
class CpuSample:
    host: Annotated[str, Tag]
    usage: float  # declared float: an int value is written as 1.0
    cores: int
    time: datetime  # an attribute named "time"/"timestamp" is the timestamp


client.write([CpuSample("web-1", 1, 8, now)])
```

### Asynchronous and synchronous use

`write()` returns a `WriteFuture` as soon as the data is buffered:

```python
future = client.write(points)  # fire and forget: ignore the future
result = future.result(timeout=30)  # or wait: WriteResult(points=..., dropped=..., duration=...)
await future  # the same from asyncio
future.add_done_callback(lambda f: ack_upstream() if f.exception() is None else nack())  # on a sender thread
```

Waiting flushes the buffer, so a synchronous write returns as soon as the server answers rather
than after `flush_interval`. `client.flush()` waits for everything written so far; `close()`
(or leaving the `with` block, or interpreter exit) flushes before shutting down.

Data is serialized before `write()` returns, so the objects you passed can be reused immediately.

Done-callbacks and `on_error` handlers run on the client's sender threads: they must not wait for
the same client (`flush()`, `close()`, or `result()` of a pending write raise `RuntimeError`
there), and should hand slow work to another thread.

### Failures in the background

A batch that still fails after retries (or that the server rejects) is reported:

- to the futures of the `write()` calls whose points it carried: `result()` / `await` raise
  one error for the call, and `future.errors` lists every batch's error. For InfluxDB 3 partial
  writes only the calls that contained a rejected line fail, and `PartialWriteError.line_errors`
  gives each rejected line (as sent) and its number within that call;
- to `on_error`, if given, once per failed batch, with the exact line protocol, ready to
  dead-letter and replay. `failure.retryable` tells transient failures (network, outage,
  client closing) from data the server refused:

  ```python
  def dead_letter(failure: WriteFailure) -> None:
      if failure.retryable:  # replay later with client.write(lines, precision=failure.precision)
          DEAD_LETTERS.write_text("\n".join(failure.lines))


  client = InfluxClient.from_config(on_error=dead_letter)
  ```
- otherwise by `flush()` / `close()`, which raise a `WriteError` for failures nobody has seen
  (no `result()`, `exception()`, `await` or `errors` on their future), so fire-and-forget
  callers cannot lose data silently and synchronous callers are not told twice. An `on_error`
  handler that raises leaves the failure to `flush()` / `close()` as well;
- always by an ERROR log (rate limited) and the `points_failed_total` metric.

Records rejected by validation with `on_invalid = "drop"` are counted in `result.dropped`, and
`result.rejected` holds their `ValidationError`s (index, code, key, message; DataFrame rows by
row number).

### Backpressure

`write.max_pending_bytes` (128 MiB) bounds buffered plus in-flight data. When the buffer is full,
`write.on_full` decides: `block` (default) waits up to `block_timeout` (60 s, then
`BufferFullError`), `drop` discards new data (counted in `points_dropped_total{reason="buffer_full"}`
and the future's `dropped`), and `raise` raises `BufferFullError` at once. `BufferFullError`,
`ClientClosedError` and `ValidationError` carry `points_enqueued`: the points of that call that
were already buffered and will be sent (do not re-send those).

### asyncio

```python
async with AsyncInfluxClient.from_config() as client:
    future = await client.write(points)  # buffered; never blocks the event loop
    result = await future  # acknowledged
    rows = await client.query("SELECT ...")
    async for chunk in client.query_stream("SELECT ..."):
        ...
```

The asyncio client shares the thread-based engine: large inputs are serialized and backpressure
is awaited in worker threads, and streams of small writes yield to the event loop regularly.
`AsyncInfluxClient.wrap(client)` puts an asyncio facade over an existing `InfluxClient`. Calling
the synchronous client's blocking methods (`result()`, `flush()`, `query()`, `close()`) on a
running event loop logs a warning.

## Choosing tags

Every distinct combination of tag values is a series, and InfluxDB's cost grows with the number
of series (InfluxDB 2 keeps an index of all series in memory). Tag values should come from a
bounded set: host, region, service, endpoint, status class, sensor id. Values that are unique or
unbounded (request ids, user or session ids, trace ids, timestamps, free text, measured values)
belong in fields. sluicebox logs a warning when a measurement exceeds 150,000 distinct tag sets
in one process.

## Configuration and secrets

Settings come from, highest priority first: keyword overrides, environment variables
(`SLUICEBOX_WRITE__BATCH_SIZE=50000`), a `.env` file, a secrets directory (one file per
secret, e.g. Docker/Kubernetes `/run/secrets/sluicebox_token`), the TOML file, then defaults.

```python
load_settings("sluicebox.toml")  # explicit file
load_settings("app.toml", section="services.influx")  # a table inside your app's config
load_settings(env_prefix="ARCHIVE_")  # a second client: ARCHIVE_TOKEN, ...
load_settings(secrets_dir="/run/secrets")
InfluxClient.from_config(write={"batch_size": 50_000})  # same arguments, plus overrides
InfluxClient(settings, write={"concurrency": 4})  # settings plus overrides
settings.with_overrides(query={"timeout": "5m"})  # a modified copy
```

Some environment variables steer loading itself (they may also be in `.env`):

| Variable | Meaning |
| --- | --- |
| `SLUICEBOX_CONFIG` | The TOML file (default `./sluicebox.toml` if it exists) |
| `SLUICEBOX_SECTION` | A table in it, e.g. `prod` (one file with `[dev...]` and `[prod...]` tables) |
| `SLUICEBOX_TOKEN_FILE` | Read the token from this file (a mounted secret) |
| `SLUICEBOX_SECRETS_DIR` | The secrets directory |

The token is a `pydantic.SecretStr`: never printed, logged or put in a repr. sluicebox refuses to
load a TOML file that contains a token (in any table) or a proxy password, since that file is
meant to be committed. Unknown keys in the TOML file, keyword overrides or prefixed `.env`
entries are errors with a "did you mean" suggestion; unknown `SLUICEBOX_*` environment variables
are logged with the setting they probably meant (`SLUICEBOX_DATABASE` -> `SLUICEBOX_CONNECTION__DATABASE`);
other applications' `.env` entries are ignored. Invalid settings raise `ConfigurationError`
listing every problem. Without a token, a 401 error says where the token was looked for.

## Validation and schemas

Every record is checked while it is serialized, before anything is buffered:

| Check | Behaviour |
| --- | --- |
| Field type lock (`type_lock`) | The first type written for a field is kept; later values must match. Safe coercions apply (`coerce`): int to float, integral float to int. Booleans never become numbers. A record that is rejected locks nothing. |
| Server types | When the server rejects a value because it stores the field with another type, the lock follows the server's type. `client.sync_schema()` learns all stored types up front. |
| Declared schemas | `[measurements.<name>]`: field types, allowed and required tags, required fields, `extra_fields = "forbid"`. `unknown_measurements = "reject"` allows only declared measurements. |
| Identifiers | Escaped per server version; names that cannot round-trip are rejected (trailing backslash, control characters, reserved keys, `#`-prefixed measurements, `=` in InfluxDB 2 measurement names, tag/field name clashes on InfluxDB 3). |
| Values | int64/uint64 ranges, 1 MiB strings, NaN/inf (`non_finite`: `skip` the field, like pandas' missing values, or reject the record with `error`), timezone-naive datetimes (`naive_datetime`: reject or treat as UTC), ints that `int_as_float` cannot represent exactly. |
| Timestamps | Range (1677-2262), and a warning for integers that look like the wrong unit. |
| Records | Unknown keys in record dicts (`"tag"` instead of `"tags"`), records without fields. |

`validation.on_invalid = "raise"` (default) raises `ValidationError` with a machine-readable
`code`, the record `index`, `measurement`, `key` and a position-free `message`; `"drop"` skips the
record, logs it (rate limited per code, measurement and key), counts it in
`points_dropped_total{reason=<code>}` and `stats().write.points_dropped`, and returns it in
`WriteResult.rejected`.

Codes: `type_conflict`, `non_finite`, `out_of_range`, `string_too_long`, `invalid_name`,
`reserved_name`, `invalid_tag_value`, `tag_field_conflict`, `unexpected_tag`, `missing_tag`,
`unexpected_field`, `missing_field`, `unknown_measurement`, `no_fields`, `invalid_time`,
`naive_datetime`, `malformed_record`, `unsupported_type`, `unsupported_record`, `invalid_line`,
`invalid_encoding`.

Raw line protocol is passed through unchanged by default (fastest); `raw_lines = "validate"`
parses it so validation and tag injection apply too.

```toml
[validation]
int_as_float = true          # common choice for sensor data: 25 and 25.5 are the same field type

[measurements.cpu]
fields = { usage = "float", cores = "integer" }
tags = ["host", "region"]
required_tags = ["host"]
```

## Tag injection

```toml
[tags]
static = { env = "production" }
from_env = { region = "AWS_REGION", pod = { var = "POD_NAME", default = "local" } }
on_conflict = "keep"           # the point's own value wins (or "overwrite", "error")

[[tags.rules]]                 # tags derived from content; named groups become template values
when.tags = { device = '^(?P<site>[a-z]+)-(?P<rack>\d+)$' }
when.measurement = "^sensor_"
set = { site = "{site}", rack = "{rack}" }

[[tags.rules]]
when.fields = { temperature = { gt = 80 } }   # gt ge lt le eq ne in regex
set = { alert = "overheating" }
```

Rule conditions (all given ones must hold): `measurement` (regex), `tags` (regex per tag),
`has_tags`, `missing_tags`, `has_fields`, `fields` (comparisons per field). A `from_env`
variable given as a plain name must be set; with `{ var, default }` it is optional (an empty
default leaves the tag out).

```python
from sluicebox import tag_context

with tag_context(tenant=req.tenant, region=req.region):  # per thread / asyncio task
    client.write(points)

client = InfluxClient.from_config(
    tags={"host": socket.gethostname()},
    enrichers=[lambda measurement, tags, fields: {"unit": "celsius"} if "temperature" in fields else None],
)
```

Order: context tags, then static tags, then rules (which see the tags added so far), then
enrichers. Templates can also use `{measurement}`, `{tags[key]}` and `{fields[key]}`; a rule
whose template references a missing value does not apply. `client.to_line_protocol(...)` shows
the result. The `tags=` argument adds static tags; `[tags]` settings go in the config file (or
`load_settings(tags={...})`).

## Queries

```python
client.query("SELECT host, usage FROM cpu WHERE host = $host", params={"host": "web-1"})  # InfluxDB 3
client.query("SELECT * FROM cpu WHERE time >= $since", params={"since": since})  # aware datetime
client.query("SELECT * FROM cpu", language="influxql")
client.query("from(bucket: params.bucket) |> range(start: -1h)", params={"bucket": "b"})  # InfluxDB 2
for chunk in client.query_stream("SELECT * FROM big_table"):  # bounded memory
    process(chunk.to_polars())
```

Prefer `params` to string formatting. On InfluxDB 3, datetime parameters are sent as RFC 3339
strings. On InfluxDB 2, Flux parameters are bound as escaped literals in an `option params = {...}`
record, so `params.name` works on InfluxDB OSS too (the official client relies on a Cloud-only
API feature); pass datetimes for Flux times. Writes are asynchronous: `flush()` before querying
data you just wrote.

Results: InfluxDB 3 timestamp columns are timezone-aware (UTC). InfluxDB 2 results come from the
official client's Flux parser: they include Flux's `result`, `table`, `_start`, `_stop` columns,
times have microsecond resolution, and string values containing `\n` come back as `\r\n`. An
empty result converts to an empty DataFrame.

Moving from InfluxDB 2 to 3: writes need no code changes (set `version` and the database).
Queries do: InfluxDB 3 speaks SQL and InfluxQL, InfluxDB 2 Flux, and a query in the other
language fails with a hint saying so.

## Errors

All exceptions derive from `SluiceboxError`:

| Exception | Meaning |
| --- | --- |
| `ConfigurationError` | Invalid settings, secret in the wrong place, wrong server version (`check()`), missing optional dependency |
| `ValidationError` | A record was rejected client-side (`.code`, `.index`, `.measurement`, `.key`, `.message`) |
| `InfluxConnectionError`, `InfluxTimeoutError` | No response (network, TLS, timeout) |
| `ServerError` and subclasses | HTTP errors: `BadRequestError` 400, `AuthenticationError` 401, `PermissionDeniedError` 403, `NotFoundError` 404, `PayloadTooLargeError` 413, `UnprocessableEntityError` 422, `RateLimitedError` 429, `ServiceUnavailableError` 503; also a web page where an API answer was expected |
| `PartialWriteError` | The server stored part of a batch (`.line_errors`, `.rejected`) |
| `QueryError` | The query failed (syntax, planning, unknown table, database or bucket; `.status`) |
| `WriteError` | Background batches failed and nobody saw it (`.errors`, `.failed_points`) |
| `BufferFullError`, `ClientClosedError` | Backpressure (`on_full = "raise"` or `block_timeout`); use after `close()` |

Transient failures (connection errors, timeouts, 429, 5xx) are retried with exponential backoff
and jitter, honouring `Retry-After`, for up to `retry.max_elapsed` (5 minutes, so outages that
long lose nothing); a broken pooled connection is retried immediately. Retries are safe because
every point carries an explicit timestamp: a re-sent point overwrites itself.

## Observability

**Metrics** (Prometheus, namespace `sluicebox`, label `client` = settings `name`):
`points_written_total`, `points_failed_total`, `points_dropped_total{reason}`,
`write_batches_total{outcome}`, `write_bytes_total{kind=raw|sent}`, `write_retries_total{reason}`,
`write_request_duration_seconds`, `write_batch_duration_seconds`, `write_batch_points`,
`write_buffer_bytes`, `write_buffer_limit_bytes`, `write_max_batch_bytes` (lowered after HTTP
413), `write_last_success_timestamp_seconds`, `write_inflight_requests`, `queries_total`,
`query_duration_seconds`, `query_rows_total`, `errors_total{operation,error}`,
`stage_duration_seconds{stage}`, `client_info`. The counters alerts need exist at zero from the
start. Pass `registry=` to use your own registry; set `[metrics] port` to serve `/metrics`
directly (one port per process). Clients with the same name share series.

Alerts worth having: `increase(sluicebox_points_failed_total[5m]) > 0`,
`increase(sluicebox_points_dropped_total[5m]) > 0`,
`sluicebox_write_buffer_bytes / sluicebox_write_buffer_limit_bytes > 0.8`,
`time() - sluicebox_write_last_success_timestamp_seconds > 300`, and a rising
`sluicebox_write_retries_total`.

**Logging** goes to the `sluicebox` logger hierarchy (`sluicebox.write`, `.query`, `.validation`,
`.transport`, `.client`, `.config`); the library only adds a `NullHandler`, so records reach your
own handlers. Records carry structured context (`client`, `database`, `points`, `error`,
`status`, `code`, `measurement`, `key`) in `record.influx`, which `JsonFormatter` emits as JSON
keys along with any `extra=` fields. For applications without logging setup,
`configure_logging(level="INFO", format="json")` or `[logging] configure = true` installs a stderr
handler on the `sluicebox` logger (which then stops propagating). Repeated warnings are rate
limited, and `close()` reports how many were suppressed. Tokens never reach log records. Data
lost at interpreter exit is also printed to stderr when no logging is configured.

**Profiling**: stage timings (`serialize`, `backpressure`, `queue`, `compress`, `request`,
`batch`, `query`) feed `stage_duration_seconds` and `client.stats().stages` (count, total and
max seconds per stage; `serialize` and `batch` are counted per batch). Slow batches and queries
are logged with their duration. For a deep dive:

```python
with client.profile("write.prof", memory=True) as report:
    client.write(points).result()
print(report.summary(limit=15))  # top functions, wall time, peak memory
```

`memory=True` traces every allocation and slows code down many times over; use it on short runs.
`client.stats()` returns counters (points written/failed/dropped, retries, bytes) and buffer state.

## Running in production

- **Check at startup**: `client.check()` verifies the server version, token, organization and
  bucket (with an empty write) so misconfiguration fails the deploy, not the first batch.
- **Shutdown**: call `client.close()` (or use `with`). Python's default SIGTERM handler exits
  without running cleanup, so install one that triggers your normal shutdown. `close_timeout`
  (20 s) stays below Kubernetes' 30 s grace period; data that could not be sent in time is
  reported (log, `on_error`, `WriteError`), never dropped silently.
- **Telemetry that must never slow the application**: `on_full = "drop"` (drops are counted and
  logged). Pipelines that must not lose data keep `block` (with a `block_timeout`).
- **Memory**: process memory can grow by about 3x `max_pending_bytes` while the buffer is full
  (an outage); size it for the container's limit.
- **Outages**: batches are retried for `retry.max_elapsed` (5 minutes); meanwhile the buffer fills
  and backpressure applies.
- **Processes**: create one client per process. With pre-fork servers (gunicorn) or
  `multiprocessing` with `fork`, create clients after the fork (e.g. in `post_fork`); a client
  inherited across `fork()` keeps working in the child, but data buffered before the fork belongs
  to the parent, and InfluxDB 3 queries (gRPC) cannot run in a child forked after the parent
  created a query client (sluicebox raises instead of hanging). For metrics from several
  processes, use prometheus_client's multiprocess mode or a port per process.

## Performance and tuning

Measured on an Apple M5 Pro against local Docker containers (InfluxDB 2.9.1, InfluxDB 3.12 Core
with an in-memory object store), 3 tags and 4 fields per point, full validation on, with every
run verifying the stored point count. `uv run python benchmarks/bench_write.py --help`
reproduces them.

| 1,000,000 points | InfluxDB 3 Core | InfluxDB 2 |
| --- | --- | --- |
| sluicebox, `write(list of dicts)` | 272 k points/s | 440 k points/s |
| sluicebox, `write(polars DataFrame)` | 226 k points/s | 562 k points/s |
| sluicebox, one `write()` per point | 211 k points/s | 217 k points/s |
| official client, batching mode | 5 k points/s | 93 k points/s |
| official client, synchronous 5k-point writes | 5 k points/s | 84 k points/s |

Serialization alone: 1.1 M points/s per thread (dicts, validation and type locking on) versus
0.36-0.38 M points/s for the official clients' `Point` serialization
(`benchmarks/bench_serialize.py`).

What matters most:

- **Requests in flight.** InfluxDB 3 acknowledges a write when its WAL flushes (every second by
  default), so its throughput is roughly `concurrency x batch_size` per second. The defaults
  (16 x 25,000 lines) were 6-7x faster on InfluxDB 3 than 4 x 10,000, and the official clients
  send one request at a time. Over a 40 ms round trip the defaults reached 460 k points/s on
  InfluxDB 3 versus 20 k points/s with 4 x 5,000.
- **gzip** (on by default) barely matters on localhost but is decisive on real links: on a
  1 Mbit/s uplink it was over twice as fast.
- **DataFrames** are serialized vectorized: `write()` returns 4-6x sooner than for the same rows
  as dicts, leaving the server as the only bottleneck.
- `write.no_sync = true` (InfluxDB 3 Core/Enterprise) acknowledges before the WAL is persisted:
  lower latency, weaker durability.
- One client per process, shared by all threads, is the intended setup. Serialization of
  records runs at about 1.1 M points/s on one core; threads do not add to that, not even on
  free-threaded Python 3.14t (measured: allocation-heavy pure-Python code scales poorly there).
  To use more cores for serialization, write DataFrames (vectorized in Rust) or run one client
  per process.

## Remote servers, TLS and proxies

Beyond loopback, the test suite runs every write scenario through Toxiproxy (latency, bandwidth
limits, connection resets, responses lost in a blackhole, outages, truncated uploads, sliced TCP
streams), against servers with native TLS, behind nginx with a 256 KB body limit or mutual TLS,
and through an HTTP forward proxy. The client:

- pools keep-alive connections with TCP keepalive (dead peers and NAT/LB idle drops are detected);
- retries a broken pooled connection immediately, everything else with backoff;
- learns a request size limit from HTTP 413 responses (including a proxy's HTML page) and splits
  batches accordingly, instead of re-uploading oversized bodies (and stops splitting when even
  tiny requests are refused, which is not about size);
- never duplicates data when a response is lost after the server stored the batch, and the
  servers never ingest a truncated upload (both verified).

A path in `connection.url` (e.g. `https://gateway.internal/influx`) prefixes every API path, for
servers behind path-routing proxies. TLS: `connection.ca_cert` for a private CA,
`client_cert`/`client_key` for mutual TLS (applied to writes and to InfluxDB 3's Flight queries;
tested through an nginx that requires client certificates), `verify_ssl = false` to disable
verification (logged once). Python 3.13+ verifies certificates strictly (`VERIFY_X509_STRICT`):
an internal CA certificate needs `basicConstraints` and `keyUsage` extensions. Proxies:
`connection.proxy`, with credentials only via the environment
(`SLUICEBOX_CONNECTION__PROXY=http://user:pass@proxy:3128`); InfluxDB 3 queries tunnel through it.

InfluxDB 3 Cloud Serverless, Dedicated and Clustered only offer the v2-compatible endpoint: set
`write.api = "v2"` (a 404 from `/api/v3/write_lp` says so in the error).

## Server behaviour handled for you

Found by testing against InfluxDB 2.9 and InfluxDB 3.12, and handled or validated:

- An InfluxDB 2 server answers the InfluxDB 3 write endpoint with its web UI and HTTP 200: a web
  page is never taken for a successful write, and `check()` names the version mismatch.
- Backslashes: InfluxDB 3 unescapes `\\` in names and tags, InfluxDB 2 does not. sluicebox
  escapes per version, so values like `c:\dir` round-trip on both.
- InfluxDB 2 silently drops points whose measurement starts with `#` (a comment line), and
  stores but can never query measurement names containing `=`. Both are rejected client-side.
- Names or tag values ending in a backslash, tabs in names (InfluxDB 3), NaN, int64 overflow,
  timestamps beyond 2262 and strings over 1 MiB are rejected by the servers, often taking the
  rest of the batch with them.
- On InfluxDB 3, `/api/v3/write_lp` (the default for version 3) keeps the valid lines of a batch
  and names the rejected ones; `/api/v2/write` rejects the whole batch.
- Flux parameters (`params.x`) are a Cloud-only feature; sluicebox binds them so they work on OSS.

## Development

```bash
uv sync                                         # environment with all extras and dev tools
docker compose up -d --wait                     # InfluxDB 2 (:18086) and 3 Core (:18181)
docker/tls/generate.sh && docker compose --profile network up -d --wait   # + TLS, Toxiproxy, nginx

uv run pytest tests/unit                        # fast, no servers needed
uv run pytest                                   # everything (server tests skip when unavailable)
uv run ruff check . && uv run ruff format --check . && uv run mypy
uv run python benchmarks/bench_write.py --server 3 --points 500000
```

On first start the compose stack generates random throwaway credentials into `docker/secrets/`
(git-ignored), where the tests and benchmarks read them; the servers keep their data in memory.
InfluxDB 3 Core allows five databases, so the tests share one.

## License

MIT; see [LICENSE](LICENSE).
