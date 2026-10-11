"""Write throughput of the client itself, against a local sink that accepts everything.

The servers bound end-to-end throughput (bench_write.py); this measures what the client can do
on its own: serializing, buffering, batching, compressing and sending, with the caller's
threads and the senders sharing the interpreter. The sink is a separate process, so its work
does not compete with the client for the GIL. It counts the lines it receives and checks that
every write arrived exactly once (each line is unique, and their order-independent checksum
must match).

Run: uv run python benchmarks/bench_client.py [--points N] [--threads 2,4,8] [--only SUBSTRING]
"""

from __future__ import annotations

import argparse
import functools
import gc
import gzip
import hashlib
import json
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

BASE_NS = 1_767_225_600_000_000_000  # 2026-01-01


def checksum(lines: list[bytes]) -> int:
    """Order-independent: the sum of 64-bit line hashes (a lost or doubled line changes it)."""
    total = 0
    for line in lines:
        total += int.from_bytes(hashlib.blake2b(line, digest_size=8).digest(), "little")
    return total % (1 << 64)


def serve_sink(port: int) -> None:
    received = {"lines": 0, "checksum": 0}
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"  # keep-alive, like the servers

        def log_message(self, *args: Any) -> None:
            pass

        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
            if self.path == "/reset":
                with lock:
                    received.update(lines=0, checksum=0)
            else:
                if self.headers.get("Content-Encoding") == "gzip":
                    body = gzip.decompress(body)
                lines = [line for line in body.split(b"\n") if line]
                total = checksum(lines)
                with lock:
                    received["lines"] += len(lines)
                    received["checksum"] = (received["checksum"] + total) % (1 << 64)
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self) -> None:
            with lock:
                body = json.dumps(received).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    class Server(ThreadingHTTPServer):
        # The client's 16 senders connect at once when a flush releases many batches: the
        # default backlog of 5 drops connection attempts, which TCP retries a second later.
        request_queue_size = 128

    Server(("127.0.0.1", port), Handler).serve_forever()


def make_points(n: int) -> list[dict[str, Any]]:
    return [
        {
            "measurement": "cpu",
            "tags": {"host": f"host-{i % 100}", "region": ("eu-west", "us-east", "ap-south")[i % 3]},
            "fields": {"usage_user": (i % 1000) / 10.0, "usage_system": (i % 333) / 7.0, "load": i % 17},
            "time": BASE_NS + i * 1000,
        }
        for i in range(n)
    ]


def make_lines(n: int, *, strings: bool) -> list[str]:
    """Raw line protocol for points like make_points(), optionally with a string field (quoted)."""
    state = ',state="ok {}"' if strings else ""
    return [
        f"cpu,host=host-{i % 100},region={('eu-west', 'us-east', 'ap-south')[i % 3]} "
        f"usage_user={(i % 1000) / 10.0},usage_system={(i % 333) / 7.0},load={i % 17}i"
        f"{state.format(i % 7)} {BASE_NS + i * 1000}"
        for i in range(n)
    ]


#: What a scenario writes with the client, and the lines it must deliver (None: the points').
Scenario = tuple["Callable[[Any], None]", "list[str] | None"]


def scenarios(points: list[dict[str, Any]], thread_counts: list[int]) -> dict[str, Scenario]:
    def records(client: Any) -> None:
        client.write(points)

    def one_per_call(client: Any, points: list[Any] = points) -> None:
        write = client.write
        for point in points:
            write(point)

    def lists_of_10(client: Any) -> None:
        write = client.write
        for start in range(0, len(points), 10):
            write(points[start : start + 10])

    def threaded(threads: int) -> Callable[[Any], None]:
        def run(client: Any) -> None:
            share = -(-len(points) // threads)
            workers = [
                threading.Thread(target=one_per_call, args=(client, points[i * share : (i + 1) * share]))
                for i in range(threads)
            ]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()

        return run

    found: dict[str, Scenario] = {
        "write(list of dicts)": (records, None),
        "one write() per point": (one_per_call, None),
        "write() of 10 points at a time": (lists_of_10, None),
    }
    for threads in thread_counts:
        found[f"one write() per point, {threads} threads"] = (threaded(threads), None)
    for strings in (False, True):
        lines = make_lines(len(points), strings=strings)
        quoted = ", string field" if strings else ""

        def text(client: Any, lines: list[str] = lines) -> None:
            client.write("\n".join(lines))

        found[f"write(line protocol text){quoted}"] = (text, lines)
        found[f"one write() per line{quoted}"] = (functools.partial(one_per_call, points=lines), lines)
    try:
        import polars as pl
    except ImportError:
        return found

    def frame(client: Any) -> None:
        columns = {
            "host": [p["tags"]["host"] for p in points],
            "region": [p["tags"]["region"] for p in points],
            **{name: [p["fields"][name] for p in points] for name in points[0]["fields"]},
            "time": [p["time"] for p in points],
        }
        client.write(pl.DataFrame(columns), measurement="cpu", tag_columns=["host", "region"])

    found["write(polars DataFrame)"] = (frame, None)
    return found


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--points", type=int, default=300_000)
    parser.add_argument("--threads", default="8", help="thread counts, comma-separated (e.g. 2,4,8)")
    parser.add_argument("--only", default="", help="run the scenarios whose name contains this")
    parser.add_argument("--port", type=int, default=18_998)
    parser.add_argument("--sink", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.sink:
        serve_sink(args.port)
        return

    from prometheus_client import CollectorRegistry

    from sluicebox import InfluxClient, load_settings

    url = f"http://127.0.0.1:{args.port}"
    sink = subprocess.Popen([sys.executable, __file__, "--sink", "--port", str(args.port)])
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen(url, timeout=1).close()
                break
            except OSError:
                time.sleep(0.05)
        settings = load_settings(
            None, env_file=None, token="-", connection={"url": url, "version": 3, "database": "bench"}
        )
        points = make_points(args.points)
        print(f"{args.points:,} points (2 tags, 3 fields) to a local sink, Python {sys.version.split()[0]}\n")
        thread_counts = [int(count) for count in args.threads.split(",")]
        for name, (body, lines) in scenarios(points, thread_counts).items():
            if args.only not in name:
                continue
            urllib.request.urlopen(urllib.request.Request(f"{url}/reset", data=b"", method="POST")).close()
            with InfluxClient(settings, registry=CollectorRegistry()) as client:
                expected = lines or client.to_line_protocol(points)
                gc.collect()
                started = time.perf_counter()
                body(client)
                returned = time.perf_counter() - started
                client.flush()
                total = time.perf_counter() - started
            with urllib.request.urlopen(url) as response:
                received = json.loads(response.read())
            exact = received == {
                "lines": len(expected),
                "checksum": checksum([line.encode() for line in expected]),
            }
            check = (
                ""
                if exact
                else f"  !! received {received['lines']:,} lines, not exactly the {len(expected):,} written"
            )
            print(
                f"  {name:42s} {args.points / total / 1e3:6.0f} k points/s   "
                f"write() returned after {returned:5.2f} s{check}",
                flush=True,
            )
    finally:
        sink.terminate()
        sink.wait()


if __name__ == "__main__":
    main()
