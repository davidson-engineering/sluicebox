"""End-to-end write throughput: influxkit vs the official clients, against real servers.

Start the servers first (``docker compose up -d --wait``), then e.g.::

    uv run python benchmarks/bench_write.py --server 3 --points 500000
    uv run python benchmarks/bench_write.py --server 2 --points 500000 --only influxkit
    uv run python benchmarks/bench_write.py --server 3 --tune     # batch size / concurrency / gzip sweep

Every scenario writes the same points to a fresh measurement and verifies the stored count,
so a fast-but-lossy configuration cannot win.
"""

from __future__ import annotations

import argparse
import gc
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from prometheus_client import CollectorRegistry

from influxkit import InfluxClient, load_settings
from influxkit.client import flux_string

if TYPE_CHECKING:
    from collections.abc import Callable

# Credentials that `docker compose up` generated for the local servers.
SECRETS = Path(__file__).resolve().parents[1] / "docker" / "secrets"


def _token(file_name: str) -> str:
    try:
        return (SECRETS / file_name).read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        raise SystemExit(
            f"no {file_name} in {SECRETS}: start the servers with docker compose up -d --wait"
        ) from None


SERVERS = {
    2: {
        "url": "http://localhost:18086",
        "token_file": "influxdb2-token",
        "database": "influxkit",
        "org": "influxkit",
    },
    3: {
        "url": "http://localhost:18181",
        "token_file": "influxdb3-token",
        "database": "influxkit_bench",
    },
}
BASE_NS = 1_767_225_600_000_000_000  # 2026-01-01


@dataclass
class Outcome:
    name: str
    points: int
    total: float
    producer: float
    stored: int

    @property
    def rate(self) -> float:
        return self.points / self.total


def make_points(n: int, measurement: str) -> list[dict[str, Any]]:
    return [
        {
            "measurement": measurement,
            "tags": {
                "host": f"host-{i % 100}",
                "region": ("eu-west", "us-east", "ap-south")[i % 3],
                "dc": f"dc{i % 7}",
            },
            "fields": {
                "usage_user": (i % 1000) / 10.0,
                "usage_system": (i % 333) / 7.0,
                "load": i % 17,
                "up": True,
            },
            "time": BASE_NS + i * 1000,
        }
        for i in range(n)
    ]


def settings_for(version: int, **overrides: Any) -> Any:
    server = SERVERS[version]
    connection = {"url": server["url"], "version": version, "database": server["database"]}
    if version == 2:
        connection["org"] = server["org"]
    return load_settings(
        None,
        env_file=None,
        token=_token(server["token_file"]),
        connection=connection,
        **overrides,
    )


def count_points(client: InfluxClient, measurement: str) -> int:
    if client.settings.connection.version == 3:
        rows = client.query(f'SELECT count(*) AS n FROM "{measurement}"').to_dicts()
        return int(rows[0]["n"]) if rows else 0
    flux = (
        f"from(bucket: {flux_string(client.settings.connection.database)}) |> range(start: 0) "
        f'|> filter(fn: (r) => r._measurement == {flux_string(measurement)} and r._field == "usage_user") '
        "|> group() |> count()"
    )
    rows = client.query(flux).to_dicts()
    return int(rows[0]["_value"]) if rows else 0


def run(name: str, n: int, body: Callable[[str], float], verifier: InfluxClient) -> Outcome:
    measurement = f"bench_{uuid.uuid4().hex[:10]}"
    gc.collect()
    started = time.perf_counter()
    producer = body(measurement)
    total = time.perf_counter() - started
    stored = count_points(verifier, measurement)
    deadline = time.monotonic() + 15
    while stored != n and time.monotonic() < deadline:  # under load, visibility can lag the ack
        time.sleep(0.5)
        stored = count_points(verifier, measurement)
    outcome = Outcome(name, n, total, producer, stored)
    flag = "" if stored == n else f"  !! stored {stored:,} of {n:,}"
    print(
        f"  {name:58s} {outcome.rate / 1e3:8.0f} k points/s   total {total:6.2f} s   "
        f"write() returned after {producer:6.2f} s{flag}",
        flush=True,
    )
    return outcome


def influxkit_records(version: int, n: int, **overrides: Any) -> Callable[[str], float]:
    def body(measurement: str) -> float:
        points = make_points(n, measurement)
        with InfluxClient(settings_for(version, **overrides), registry=CollectorRegistry()) as client:
            started = time.perf_counter()
            future = client.write(points)
            producer = time.perf_counter() - started
            future.result(timeout=600)
        return producer

    return body


def influxkit_frame(version: int, n: int) -> Callable[[str], float]:
    import polars as pl

    def body(measurement: str) -> float:
        points = make_points(n, measurement)
        frame = pl.DataFrame(
            {
                "host": [p["tags"]["host"] for p in points],
                "region": [p["tags"]["region"] for p in points],
                "dc": [p["tags"]["dc"] for p in points],
                "usage_user": [p["fields"]["usage_user"] for p in points],
                "usage_system": [p["fields"]["usage_system"] for p in points],
                "load": [p["fields"]["load"] for p in points],
                "up": [p["fields"]["up"] for p in points],
                "time": [p["time"] for p in points],
            }
        )
        with InfluxClient(settings_for(version), registry=CollectorRegistry()) as client:
            started = time.perf_counter()
            future = client.write(frame, measurement=measurement, tag_columns=["host", "region", "dc"])
            producer = time.perf_counter() - started
            future.result(timeout=600)
        return producer

    return body


def influxkit_single_points(version: int, n: int) -> Callable[[str], float]:
    """An application emitting one point per write() call (the fire-and-forget pattern)."""

    def body(measurement: str) -> float:
        points = make_points(n, measurement)
        with InfluxClient(settings_for(version), registry=CollectorRegistry()) as client:
            started = time.perf_counter()
            for point in points:
                client.write(point)
            producer = time.perf_counter() - started
            client.flush()
        return producer

    return body


def official_v2(n: int, mode: str) -> Callable[[str], float]:
    from influxdb_client import InfluxDBClient
    from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions

    server = SERVERS[2]

    def body(measurement: str) -> float:
        points = make_points(n, measurement)
        with InfluxDBClient(
            url=server["url"], token=_token(server["token_file"]), org=server["org"], enable_gzip=True
        ) as client:
            started = time.perf_counter()
            if mode == "batching":
                with client.write_api(
                    write_options=WriteOptions(batch_size=5000, flush_interval=1000)
                ) as api:
                    api.write(bucket=server["database"], record=points)
                    producer = time.perf_counter() - started
            else:
                api = client.write_api(write_options=SYNCHRONOUS)
                for start in range(0, n, 5000):
                    api.write(bucket=server["database"], record=points[start : start + 5000])
                producer = time.perf_counter() - started
        return producer

    return body


def official_v3(n: int, mode: str) -> Callable[[str], float]:
    from influxdb_client_3 import InfluxDBClient3, WriteOptions, write_client_options

    server = SERVERS[3]

    def body(measurement: str) -> float:
        points = make_points(n, measurement)
        if mode == "batching":
            options = write_client_options(write_options=WriteOptions(batch_size=5000, flush_interval=1000))
        else:
            options = None
        with InfluxDBClient3(
            host=server["url"],
            token=_token(server["token_file"]),
            database=server["database"],
            enable_gzip=True,
            write_client_options=options,
        ) as client:
            started = time.perf_counter()
            if mode == "batching":
                client.write(record=points)
            else:
                for start in range(0, n, 5000):
                    client.write(record=points[start : start + 5000])
            return time.perf_counter() - started

    return body


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--server", type=int, choices=[2, 3], default=3)
    parser.add_argument("--points", type=int, default=300_000)
    parser.add_argument("--only", choices=["influxkit", "official"], default=None)
    parser.add_argument("--tune", action="store_true", help="sweep batch size, concurrency and gzip")
    args = parser.parse_args()
    n = args.points
    version = args.server
    verifier = InfluxClient(settings_for(version), registry=CollectorRegistry())
    verifier.ping()
    print(f"InfluxDB {version} at {SERVERS[version]['url']}: {n:,} points (3 tags, 4 fields)\n")

    if args.tune:
        for gzip in (True, False):
            for batch_size in (5_000, 10_000, 25_000):
                for concurrency in (1, 2, 4, 8):
                    run(
                        f"influxkit batch={batch_size:,} concurrency={concurrency} gzip={gzip}",
                        n,
                        influxkit_records(
                            version,
                            n,
                            write={"batch_size": batch_size, "concurrency": concurrency, "gzip": gzip},
                        ),
                        verifier,
                    )
        return

    if args.only in (None, "influxkit"):
        run("influxkit: write(list of dicts)", n, influxkit_records(version, n), verifier)
        run("influxkit: write(polars DataFrame)", n, influxkit_frame(version, n), verifier)
        run("influxkit: one write() per point", n, influxkit_single_points(version, n), verifier)
    if args.only in (None, "official"):
        if version == 2:
            run("influxdb-client: batching mode (Rx), dicts", n, official_v2(n, "batching"), verifier)
            run("influxdb-client: SYNCHRONOUS, 5k-point chunks", n, official_v2(n, "sync"), verifier)
        else:
            run("influxdb3-python: batching mode, dicts", n, official_v3(n, "batching"), verifier)
            run("influxdb3-python: synchronous, 5k-point chunks", n, official_v3(n, "sync"), verifier)
    verifier.close()


if __name__ == "__main__":
    main()
