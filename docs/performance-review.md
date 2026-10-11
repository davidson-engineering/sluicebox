# Performance and data integrity review

Where the write path spends its time, what was changed to make it faster, and the data
integrity problems found on the way. Reviewed 2026-10-11 against `470fe38` (v0.1.0 and the
fixes after it), on an Apple M5 Pro with CPython 3.14.8, 3.14t (free-threaded) and 3.11.

## How the review was done

- **Profiling.** The serializer is one inlined loop, so cProfile only sees `serialize()`
  itself: the cost of each step was measured by timing it alone over the same records
  (ablation) and with microbenchmarks of the alternatives. A sampling profiler over all
  threads, the per-stage timings of `client.stats()`, process CPU time, `gc.get_stats()` and
  `tracemalloc` covered the rest.
- **Two kinds of end-to-end runs.** Against the real servers (`benchmarks/bench_write.py`),
  which bound throughput (InfluxDB 3 acknowledges on its WAL flush), and against a local sink
  in another process that accepts everything, which shows what the client itself can do. The
  sink checksums every line it receives (order-independent), so each run also proved that no
  point was lost or written twice. It became `benchmarks/bench_client.py`.
- **Integrity probing.** Boundary values were sent raw to both live servers to learn exactly
  what they accept; the vectorized DataFrame path was run against the row path over boundary
  values; futures were stress-tested with `result()`, callbacks and `await` racing batch
  completion from 16 threads, on standard and free-threaded Python.
- Every problem was reproduced before it was fixed, and every code fix has a regression test
  that fails without it. Optimizations were measured before and after; ideas that did not pay
  were dropped.

## Where the time goes

Before this review, per point (3 tags, 4 fields: two floats, an int, a boolean):

| `write(list of dicts)`, 1,117 ns per point | ns |
| --- | --- |
| Field loop (type dispatch, formatting, type locks) | 610 |
| ... of which `repr()` of the two floats | ~300 |
| Tag set: cache key and lookup | 143 |
| Join fields, build the line | 144 |
| Unpack the record dict | 83 |
| Plan lookup, context tags, size, append | ~75 |

| One `write()` per point, 3,155 ns per call | ns |
| --- | --- |
| `serialize()` of one record, of which ~337 ns fixed per call | 1,537 |
| Cyclic garbage collector | ~480 |
| `engine.submit()` | 366 |
| Future: create, attach, seal (two locks) | ~195 |
| `write()` call chain | ~320 |
| Sender share (resolving 25,000 futures: 3.1 ms per batch) | ~125 |

On the senders, a 25,000-line batch cost 0.18 ms to join and encode, 4.4 ms to gzip (outside
the GIL) and 3.1 ms to resolve its futures one by one under a global lock.

The garbage collector was the hidden cost: every buffered single write kept four GC-tracked
objects alive until its batch was acknowledged (the future, its error and rejected-record
lists, a waiter tuple), so collections were frequent and every full one scanned every
buffered write. A buffered single write also took 569 bytes for 121 bytes of line protocol.

## Data integrity: findings and fixes

| # | Finding | Severity | Status |
| --- | --- | --- | --- |
| 1 | **DataFrame times outside 1677-2262 were stamped with the write() time.** Converting a `Datetime[us]` or `[ms]` value (the units of Python datetimes and pandas 2) to nanoseconds overflowed, polars made it null, and the null was filled like a missing time: a point from the year 3000 was stored as now. | Critical: silently wrong data | Fixed: times are range-checked on the column's own integers; out-of-range rows are invalid (`invalid_time`), and drop mode drops just them. |
| 2 | **DataFrame integers beyond 2^53 were rounded** when written as floats (`int_as_float`, or coerced into a float field): 9007199254740993 became 9007199254740992.0. With `int_as_float` and a field declared integer, they went through floats on the way to integers. | High: silently wrong data | Fixed: bounds are checked on the integers; the row path's error codes. A differential test runs both paths over boundary values. |
| 3 | **A server type conflict could kill a sender thread.** Learning the type raised when the server named a measurement the client refuses (written as raw line protocol, e.g. with `unknown_measurements = "reject"`): the futures of the batch never resolved and the thread died. With `concurrency = 1` nothing was ever sent again. | High: writes stop | Fixed: reporting can no longer keep futures from resolving, and sender threads survive any error. |
| 4 | **InfluxDB 2 refuses timestamps at the int64 limits for the whole request.** One point at -2^63, -2^63 + 1 or 2^63 - 1 failed its batch (verified: 0 of 3 stored). The bounds for coarser precisions also let a value through that overflows nanoseconds. | Medium | Fixed: each dialect knows its server's range; an integration test pins it to what the servers accept. |
| 5 | **Unpaired surrogates in strings escaped validation** as `UnicodeEncodeError`: not in the exception hierarchy, not dropped with `on_invalid = "drop"` (one bad record aborted a large write), and the record's type locks stayed. | Medium | Fixed: an invalid record (`invalid_name`, `invalid_tag_value`, `invalid_encoding`). |
| 6 | **A rejected line without a line number counted as written** when another rejection in the same response had one. A line reported twice was counted twice. | Medium | Fixed: the whole request is reported as failed then (replaying stored points is harmless). InfluxDB 3.12 numbers every rejection (checked up to 1,000), so this is defensive. |
| 7 | **InfluxDB 2 queries failed on strings over 128 KiB**, which sluicebox writes (up to the 1 MiB limit): the official client parses Flux results with Python's csv module and its default field limit. `sync_schema()` failed outright as soon as its look-back held one. | Medium | Fixed: the limit is raised (never lowered) when the InfluxDB 2 backend is created. |
| 8 | Decimal DataFrame columns were written as floats despite `coerce = false`; floats of 2^63 or more coerced to integers raised a raw polars error instead of a `ValidationError`. | Medium | Fixed. |
| 9 | With 16 requests in flight and independent retries, two writes of the same point can reach the server in either order: a correction, or a "latest state" kept at a fixed timestamp, could keep the older value. | Medium | Documented, with the two safe ways (wait for the first future, or `concurrency = 1`). |
| 10 | Process memory with a full buffer of single writes reached 4.7x the buffered bytes, against the "about 3x" the README says to size containers by. | Medium | Now 2.95x (see below). |
| 11 | `tag_columns=` and the other DataFrame options were silently ignored for a record, instead of raising `TypeError` as for other inputs. | Low | Fixed. |
| 12 | With `int_as_float` and `coerce = false`, ints were rejected for integer fields ("5 is float ... set int_as_float"), contrary to the documented behaviour. | Low | Fixed in both paths. |

## Performance: what changed

| Change | Effect |
| --- | --- |
| A batch lists futures, line counts and offsets in three lists instead of a tuple per write; futures create their lists only when needed. | GC per single write 480 -> ~85 ns; 569 -> 358 bytes per buffered single write. |
| Futures are guarded by their engine's lock, which `submit()` already holds: the future is attached and sealed in one hold; a batch's futures are released under one hold, and only those with waiters are visited again. | 2 fewer lock round trips per write; 25,000 futures resolved in 1.0 ms instead of 3.1. |
| Records and lists that fit in one batch skip the chunking machinery (generator, ABC checks, separate seal), in the synchronous and asyncio clients. | Fixed cost of a small `write()` halved (1.85 -> 0.9-1.1 us); `await write(record)` 3.9 -> 3.0 us. |
| Serializer: one-tuple tag cache key, tag context read once per call, lazy lists, precomputed precision bounds. | 925 -> 873 ns per point (`bench_serialize.py`). |

`benchmarks/bench_client.py` (local sink, every line verified, 300,000 points):

| Scenario | Before | After |
| --- | --- | --- |
| One `write()` per point | 307 k points/s | 383 k points/s (+25%) |
| One `write()` per point, 8 threads | 302 k points/s | 376 k points/s (+25%) |
| 10 points per `write()` | 704 k points/s | 755 k points/s (+7%) |
| `write(list of dicts)` | 831 k points/s | 866 k points/s (+4%) |
| `write(polars DataFrame)` | sink-bound | unchanged (6.5 M rows/s serialized) |

Against the real servers (`bench_write.py`, 1 M points, stored count verified, before and
after run alternately against the same server), the servers are the bottleneck for lists and
DataFrames (lists +2%). One `write()` per point went from 217 k to 260 k points/s on InfluxDB 2
(+20%), and from 210 k to 217 k on InfluxDB 3, whose WAL bounds it, with the calling thread
busy 22% less.

## Investigated, not changed

- **Float formatting is the floor.** `repr()` of a float costs ~170 ns and is the only exact,
  shortest form; a cache would need special cases (`-0.0` equals `0.0`) and loses on
  continuous data. The current line building was faster than every alternative tried.
- **gzip and the GIL.** zlib releases the GIL while compressing but takes it back for every
  output block, and each time may wait out the 5 ms switch interval while a caller thread
  serializes: compressing a batch took 46 ms instead of 4 under load. With 16 senders this is
  hidden latency; with `concurrency = 1` throughput is ~9% lower than with 16. Changing the
  interpreter's switch interval from a library, or sending multi-member gzip bodies (which
  some decoders silently truncate), would be worse.
- **The DataFrame path** serializes 6.5 M rows/s, far beyond what either server ingests.
- **Queries** are a thin wrapper over the official clients; nothing on that path was hot.
- **Futures that seemed to hang** under a 16-thread stress test turned out to be the unit
  tests' fake server: werkzeug closes every connection, so thousands of requests exhausted
  the ephemeral ports (once a socket even connected to itself). The client retried correctly
  and lost nothing; against a keep-alive sink the same test passed every run, on standard and
  free-threaded Python. The original code behaved the same way.

## Recommendations

1. **Scale writes across threads on free-threaded Python.** Several threads writing through
   one client are slower than one there: one `write()` per point went 401 k points/s with one
   thread, 256 k with two, 189 k with eight (5.2 cores busy). `submit()` alone went from
   1.28 M/s on one thread to 355 k/s on eight: every write takes the engine lock, and the
   batch lists are shared. A per-thread staging buffer, sealed under its own lock by the
   owner and reachable by `flush()`, `close()` and the flusher, would remove most of it. The
   serializer's shared caches cost too (a per-thread serializer scaled where a shared one did
   not). This touches the buffer accounting and flush semantics, so it deserves its own
   change.
2. **Isolate bad lines on InfluxDB 2.** A line the server cannot parse fails its whole request
   with HTTP 400, taking other callers' points with it, and replaying the `WriteFailure`
   fails again. Client validation prevents this for records, but raw line protocol passes
   through unchecked by default. Splitting such a batch in halves (as for HTTP 413) would
   write every valid line and report only the bad ones.
3. **A keep-alive fake server for the unit tests**, so they exercise connection reuse and
   cannot run out of ports (see above).
4. A polars `UInt64` column with `int_as_float` is written as unsigned integers, while the row
   path writes the same values as floats: harmless (type locks catch the mix), but
   inconsistent.
