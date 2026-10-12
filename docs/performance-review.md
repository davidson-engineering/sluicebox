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

## Writes from many threads on free-threaded Python

Recommendation 1 of the review below, done in a follow-up change.

**The problem, reproduced.** On 3.14t, one `write()` per point (`bench_client.py`, local sink)
went 394 k points/s on one thread, 270 k on two, 255 k on four and 198 k on eight.
`submit()` alone (pre-serialized one-line writes, no network) went 1.56 M/s on one thread and
503 k/s on eight; a serializer shared by eight threads 685 k -> 563 k points/s.

**The cause was mostly not the lock.** On free-threaded Python a thread that reads an object it
did not create updates the object's reference count atomically, so threads that read the same
objects on every write fight over their cache lines. Measured in isolation on this machine:

| Per iteration, every thread | 1 thread | 2 | 4 | 8 |
| --- | --- | --- | --- | --- |
| Read an attribute holding a shared object | 8 ns | 123 ns | 505 ns | 1,154 ns |
| The same, holding an object the thread owns | 7 ns | 8 ns | 9 ns | 15 ns |
| Take and release one shared lock | 38 ns | 81 ns | 183 ns | 438 ns |
| Take and release a lock of one's own | 38 ns | 39 ns | 40 ns | 58 ns |

Every write read dozens of shared objects: the engine and its lock, the batch lists, the
serializer's plans, cached tag sets and field prefixes, the `FieldType` members compared for
every field, ints above 256 (`2**63 - 1`, the batch size), the `tag_context` variable.
Functions, classes, modules, `threading.local` objects, interned strings, small ints and
code constants are exempt (immortal or reference counted per thread).

**What changed.**

- *Lanes.* The engine buffers each thread's writes in a lane of its own: its open batches, a
  lock (which also guards the futures of the writes buffered there) and some reserved buffer
  space. A write takes only its lane's lock; the engine lock is taken once per batch, to open
  and to seal it. Batches enter and leave lanes under both locks, so `flush()`, `close()`, the
  flusher and fork handling always find every buffered line, and `close()` marks lanes
  unusable, so a write racing it either has its lines sealed or fails with
  `ClientClosedError`. Merged requests release each lane's futures under that lane's lock,
  512 at a time. A lane that opened no batch for 30 s is unregistered (its thread may be
  gone) and registers again when used. With the GIL, and with `concurrency = 1` (whose
  promise is that requests leave in the order the lines were written), all threads share one
  lane, as before.
- *Backpressure by credit.* A lane reserves buffer space ahead (64 KiB, or 1/256 of
  `max_pending_bytes` when that is smaller), so `max_pending_bytes` still bounds the buffer
  exactly: reserved space counts as used. Before a full buffer drops or refuses a write, the
  space lanes reserved and did not use is taken back, and a lane returns it when its last open
  batch is sealed, so `buffered_bytes` is exact once data is flushed. Waiting for space never
  holds a lane's lock (the senders need it to release the futures that free the space).
- *Serializer views.* Plans (type locks, the tag and field keys written) are shared and change
  under one lock; each thread serializes through a view with caches of its own, keeping the
  field's expected type as an immortal string and field prefixes, tag fragments and tag sets
  as strings it created itself. A record's changes to the shared plans are now made only once
  it is written, and checked again then: if another thread locked the field to another type,
  or wrote a tag key as a field, in the meantime, the record is serialized again against that
  decision. Before, a rejected record's locks were taken back afterwards, and another thread
  could act on them in between. Types the server reports are pushed to every view.
- *The rest of the path.* The client finds the calling thread's view and lane through a
  `threading.local`; the default database and precision are interned; the `tag_context`
  variable is read only once `tag_context()` was ever used.

**Integrity.** New tests (`tests/unit/test_threads.py`, each run with per-thread and with shared
state on every build) race six writers against repeated `flush()` (every write that returned
before a flush is acknowledged when it returns), against `close()` (everything accepted is sent
exactly once, everything else refused), and against `fork()` with writers holding their lanes'
locks; check that blocked writers never exceed `max_pending_bytes`, that drops plus writes add
up, that space reserved by 400 idle threads drops nothing, that lanes are unregistered and come
back, and that racing first writes of a field (or of a tag and field of one name) never let two
types (or both) through. Seven bugs planted in turn (no credit reclaim, no view invalidation, no
lock reset after fork, no commit checks of types or of tag keys, `close()` leaving lanes usable,
`flush()` not sealing lanes) each fail at least one test. The whole suite also runs with
per-thread state on the GIL build, in a new CI job. Every `bench_client.py` run below checked
that each line arrived exactly once.

**Results** (`bench_client.py --threads 2,4,8,16`, 300,000 points, median of 4 runs alternating
before/after):

| One `write()` per point | 3.14t before | 3.14t after | 3.14 before | 3.14 after |
| --- | --- | --- | --- | --- |
| 1 thread | 394 k | 402 k | 363 k | 373 k |
| 2 threads | 270 k | 646 k | 361 k | 373 k |
| 4 threads | 255 k | 1,071 k | 360 k | 368 k |
| 8 threads | 198 k | 1,613 k | 356 k | 368 k |
| 16 threads | 176 k | 1,914 k | 352 k | 362 k |

Lists of dicts, 10 points per call and DataFrames are unchanged on both builds (within the
2% the runs vary by), and so is serialization alone with the GIL (`bench_serialize.py`,
alternating runs). Without the network, `submit()` now goes 1.55 M/s on one thread and 4.1 M/s
on eight, and a shared serializer 661 k -> 3.2 M points/s from one thread to eight. The M5 Pro
has six fast cores, so each thread beyond six adds less.

`bench_client.py`'s sink now accepts 128 pending connections instead of 5: when a flush
releases many batches at once, the client's 16 senders connect together, and the dropped
connection attempts cost a second each in TCP retries (16 threads otherwise measured 250 k).

**Cost.** On free-threaded Python every writing thread keeps serializer caches of its own, so
memory for them grows with the number of threads that write many distinct series (each cache
keeps its limit: 50,000 tag sets per measurement, 200,000 tag fragments).

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

1. **Scale writes across threads on free-threaded Python.** Done: see "Writes from many
   threads on free-threaded Python" above.
2. **Isolate bad lines on InfluxDB 2.** Done: the client takes out the lines InfluxDB 2
   quotes and sends the rest again (halving when the quotes do not match), does the same on
   InfluxDB 3's `/api/v2/write`, and rejects at `write()` lines the server would read into
   the next.
3. **A keep-alive fake server for the unit tests.** Done.
4. A polars `UInt64` column with `int_as_float` is written as unsigned integers, while the row
   path writes the same values as floats: harmless (type locks catch the mix), but
   inconsistent.
