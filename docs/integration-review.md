# First-time integration review

What it was like to integrate influxkit for the first time, what got in the way, and what was
changed as a result. Reviewed 2026-10-08 against the code as of the initial commit.

## How the review was done

Three independent reviewers who had never seen the library each built a realistic application
from the README, examples and docstrings alone, without reading `src/` unless they were stuck
(which itself counted as a documentation gap). They also deliberately made common mistakes.

| Persona | Application | Server |
| --- | --- | --- |
| Backend developer | FastAPI service: request events, a process-stats sampler, `/metrics`, a p95 query endpoint, graceful shutdown | InfluxDB 3 |
| Data engineer | Batch backfill of 2 M dirty rows (polars, pandas, CSV), exact reject reporting, idempotent re-runs, daily aggregates | InfluxDB 2 and 3 (a migration) |
| Platform / SRE engineer | 16-thread worker with Kubernetes-style config, secrets, strict schemas, alerting, outages, SIGTERM, pre-fork workers | InfluxDB 3 |

An adversarial code review of the write engine, futures, serializer and clients ran alongside.
It reported 14 bugs, each with a reproduction script. Every finding below was reproduced before
it was fixed, and every fix has a regression test. The fixes for data loss also have end-to-end
tests against both real servers.

All three reviewers praised the same things: validation messages that say how to fix the
problem, config errors that list every problem, secrets kept out of the TOML file, typed
exceptions that carry the server's message, no pointless retries of 4xx errors, identical write
code for InfluxDB 2 and 3, and clean `mypy --strict` results.

## Pain points and what changed

Severity is the highest any reviewer gave the finding. "Fixed" means changed in code, with tests.

### Data loss and blockers

| # | Finding | Found by | Status |
| --- | --- | --- | --- |
| 1 | **Exiting without `close()` hung for the full close timeout, then lost every buffered point.** urllib3 drains its connection pools from a `weakref.finalize` hook at exit, and that hook ran before influxkit's flush. | service, SRE | Fixed: the exit flush is registered so it runs first, pool waits are bounded, and data lost at exit is printed to stderr when logging is not configured. Tested end to end on both servers. |
| 2 | **`version = 3` settings pointed at an InfluxDB 2 server reported success and stored nothing.** InfluxDB 2 answers `/api/v3/write_lp` with its web UI and HTTP 200. | batch, SRE | Fixed: a web page is never a successful write; the error names the likely cause. New `client.check()` compares the server's version with the settings. |
| 3 | **A 30 s outage lost 45% of the data.** `max_attempts = 5` gave up after about 7 s, long before `max_elapsed = 5m` mattered. | service, SRE | Fixed: attempts are unlimited by default and `max_elapsed` bounds retrying. A long `Retry-After` gets a final attempt at the deadline instead of ending retries. |
| 4 | **`close(timeout)` silently abandoned requests still in flight.** It overran its timeout (7 s for 5 s; 16 s+ at the default concurrency), and points were neither failed nor counted. | SRE, review | Fixed: in-flight batches are failed and reported (futures, `on_error`, `WriteError`, metrics), and the timeout is one deadline. `close_timeout` default 30 s → 20 s, below Kubernetes' grace period. |
| 5 | **A bad first value poisoned the type lock for the whole process.** One string written to a float field locked it as string, so every later correct float was rejected client-side. | SRE, review | Fixed: when the server reports a field type conflict (InfluxDB 2 and 3 formats), the lock follows the server's type. A rejected record never locks anything. |
| 6 | Writers blocked on a full buffer when `close()` began could add a batch nobody sent; their futures never completed. | review | Fixed. |
| 7 | `fork()` could deadlock the child (futures lock); futures in flight at fork never resolved in the child; InfluxDB 3 queries in a forked child hung forever (gRPC). | review, found while fixing | Fixed: locks are held across `fork()` and renewed in the child, the parent's in-flight futures fail in the child, query backends are per process, and a Flight query in a forked child raises a clear error instead of hanging. |
| 8 | Splitting polars frames with non-ASCII text leaked buffer accounting until writes blocked forever. | review | Fixed. |

### Major

| # | Finding | Found by | Status |
| --- | --- | --- | --- |
| 9 | `flush()`/`close()` re-raised failures the caller had already handled via `result()`, so batch jobs saw every failure twice and lifespan shutdowns failed. | batch, service, SRE | Fixed: only failures nobody retrieved from a future are raised. |
| 10 | No way to learn which records were dropped and why: only a count, one rate-limited log line per error code, and `stats()` excluded validation drops. | batch, SRE | Fixed: `WriteResult.rejected` holds the `ValidationError`s (DataFrame rows by row number). Stats count all drops, logs are rate limited per code, measurement and key, and suppressed counts are reported at close. |
| 11 | Partial failures under-reported: `result()` raised only the first batch's error, `LineError.line` was cut to 20 characters, and `on_error` was called once per rejected line (100,000 calls). | batch | Fixed: one combined `PartialWriteError` per call, the full line as sent, and one `on_error` call per batch with numbered line errors. New `WriteFailure.retryable`. |
| 12 | `on_full = "block"` waited forever on a hung server, stalling every worker thread. | SRE | Fixed: `block_timeout` defaults to 60 s; a production section documents `drop` for telemetry. |
| 13 | Integer seconds written with `precision = "ns"` silently landed in 1970. | batch | Fixed: integer timestamps before 1973 are logged as a likely unit mistake, and out-of-range timestamps (ISO, float, numpy, per precision) are rejected client-side. |
| 14 | `ping()` passed with a wrong token, bucket or version; problems surfaced only on the first background write. | batch, service | Fixed: `client.check()` (version, token, org, bucket via an empty write). |
| 15 | Docs and examples used unbounded tags (`request_id`, a timestamp), with no cardinality guidance or warning. | service | Fixed: a "Choosing tags" section, corrected examples, and a warning after 150,000 tag sets per measurement. |
| 16 | Typo'd `INFLUXKIT_*` environment variables (e.g. `INFLUXKIT_DATABASE`) were silently ignored, while the same typo in `.env` was an error. | SRE | Fixed: they are logged with the setting they probably meant. Nested typos are errors that name the variable. |
| 17 | Logs did not fit a JSON pipeline: `JsonFormatter` dropped `extra=` fields, records carried no structured context, and `configure = true` silently stopped propagation. | SRE | Fixed: extras and structured context (client, database, points, error, status, code, measurement, key); propagation documented; `configure_logging(level=, format=)`. |
| 18 | No signals to alert on: counters had no series until their first event, and there was no buffer capacity, learned size limit or last-success time. | SRE | Fixed: zero-initialized counters, `write_buffer_limit_bytes`, `write_max_batch_bytes`, `write_last_success_timestamp_seconds`, and suggested alerts in the README. |
| 19 | A second process with `[metrics] port` died with a bare `OSError: Address already in use`. | SRE | Fixed: a `ConfigurationError` that explains the multi-process options. |
| 20 | A path in `connection.url` (path-routing proxy) was silently dropped. | SRE | Fixed: the path prefixes API paths; a pasted `/api/v2` is removed. |
| 21 | An empty InfluxDB 2 result crashed `to_polars()`. | batch | Fixed. |
| 22 | Query errors came from two families (an unknown database was `NotFoundError`, a Flux runtime error `ServerError`). SQL rejected `datetime` parameters. | batch, service | Fixed: these are `QueryError` (with `.status`), datetime parameters are sent as RFC 3339, and a query in the other server's language gets a hint. |
| 23 | `await client.write(point)` never yielded to the event loop (150 ms stalls for 50k writes). | service | Fixed: yields every 32 writes, and single `Point`s are no longer handed to a worker thread. |
| 24 | `WriteFuture.exception()` raised instead of returning, breaking the documented callback pattern. A shared exception's traceback grew on every raise. | service, review | Fixed. |

### Minor

| Finding | Status |
| --- | --- |
| Install instructions only worked for a package index | Fixed: path and git installs documented |
| Quick start needed polars with only the `v3` extra; dependency hints said `pip` | Fixed: the quick start iterates rows; hints name uv and pip |
| `influxkit.example.toml` could not be copied as is: an active strict `cpu` schema broke the README quick start, and `from_env = AWS_REGION` failed without that variable | Fixed: optional sections are commented out (a test checks they stay valid) |
| A missing token showed up only as a bare 401; `.env` is looked up relative to the working directory | Fixed: the 401 says no token is configured and where it looked; `INFLUXKIT_TOKEN_FILE` |
| Config errors: generic message for a token under `[connection]`, no "did you mean", keyword typos attributed to the config file, a misleading message when no config file was found | Fixed |
| `InfluxClient(settings, write=...)` raised `TypeError` | Fixed: overrides are merged (`settings.with_overrides()`) |
| `configure_logging` needed the unexported `LoggingConfig` | Fixed: keywords, and `LoggingConfig` and `StageStats` are exported |
| `precision="seconds"` raised a bare `KeyError` | Fixed: `ValueError` listing valid values |
| `tag_columns="site"` was split into characters | Fixed |
| A datetime column named `timestamp` was not the time; ISO string time columns were rejected | Fixed |
| A pandas column mixing floats and strings failed without naming it | Fixed: falls back to row-by-row validation with the row index |
| The pandas naive-datetime hint showed a polars method | Fixed |
| `ServerInfo.version` was `v2.9.1` on InfluxDB 2 but `3.12.0` on InfluxDB 3 | Fixed: normalized, plus `ServerInfo.major` |
| Blocking calls inside `async def` stalled the loop silently | Fixed: logged warning |
| Failed queries were logged at WARNING and raised | Fixed: DEBUG, since they are raised |
| `https://` against a plain-HTTP port gave a bare `WRONG_VERSION_NUMBER` | Fixed: a hint to use `http://` |
| `from_env` had no optional form | Fixed: `{ var = "...", default = "..." }` |
| No environment variable for the config section or secrets directory | Fixed: `INFLUXKIT_SECTION`, `INFLUXKIT_SECRETS_DIR` |
| `from_config(tags={"on_conflict": ...})` silently created a tag named `on_conflict` | Fixed: `TypeError` explaining the difference |
| A proxy refusing every body made the client halve batches down to single lines (387 requests for 200 points) | Fixed: splitting stops at 1 KiB |
| 256 MiB `max_pending_bytes` could mean about 0.8 GiB RSS | Default 128 MiB, and the about-3x factor is documented |
| An `on_error` handler that raised (e.g. disk full) lost track of the data | Fixed: reported by `flush()`/`close()` |
| `result(timeout)` expiring looked like a server timeout | The message now says the write is still pending |
| `profile(memory=True)` was very slow; summaries printed absolute paths; stage stats undocumented | Documented; short paths |
| Synchronous writes take about 1 s on InfluxDB 3 | Documented in the quick start (WAL flush, `no_sync`) |
| Rule conditions `has_tags`, `missing_tags`, `has_fields` undocumented | Documented |
| Async client docstrings were thin; no public way to wrap an existing client | Fixed: docstrings, `AsyncInfluxClient.wrap()` |
| The adversarial review's smaller bugs: `(str, Enum)` measurement names written as `Class.MEMBER`, required tags or fields satisfied by NaN, timestamps beyond 2262 sent to the server, non-dict mappings sent as raw text, inexact `int_as_float`, a `KeyError` in an enricher reported as invalid data, numpy integer timestamps rejected, false untimed-overwrite warnings | Fixed |

## Not changed, and why

| Suggestion | Decision |
| --- | --- |
| Default `non_finite = "error"` | Kept `skip`: NaN is how pandas and numpy spell a missing value, and sparse frames are common. The README now says exactly what happens. |
| Cap `Retry-After` at `max_delay` | The server's request is honoured, but it can no longer end retrying before `max_elapsed`. |
| InfluxQL on InfluxDB 2 (its v1 `/query` API), and normalized Flux result columns | Worth doing for v2 → v3 migrations, but it needs DBRP mappings and is a feature of its own. Possible follow-up. |
| Auto-detect `connection.version` | `check()` verifies it instead; detection would add a network round trip to client creation. |
| `write(..., wait=True)` on the async client, or renaming `WriteFuture.points` | Kept the explicit future. `points` is documented as lines buffered. |
| `TypedDict` records and typed keyword overrides | Possible follow-up; misspelled keys are already caught at runtime with suggestions. |
| Built-in file dead-letter queue and replay CLI, `client.health()` | Left to applications. `on_error`, `WriteFailure.retryable`, `check()` and the metrics provide the parts. |
| Prometheus multiprocess mode | Documented (port per process, or the application's multiprocess registry). |
| Duplicate-timestamp detection for DataFrames, flat-dict rows with `measurement=` | Possible follow-ups. |
| Section inheritance in the TOML file | Not added: environment overrides already layer on one section. |
| A `measurement` label on the drop metric | Not added (cardinality); the logs carry the measurement. |
