"""Serialization throughput: sluicebox vs the official InfluxDB clients.

Run: uv run python benchmarks/bench_serialize.py [--points N]
"""

from __future__ import annotations

import argparse
import gc
import time
from typing import TYPE_CHECKING, Any

from sluicebox._lineprotocol import Dialect
from sluicebox._serializer import Serializer
from sluicebox.config import TagsConfig, ValidationConfig
from sluicebox.point import Point
from sluicebox.tags import TagInjector

if TYPE_CHECKING:
    from collections.abc import Callable

BASE_NS = 1_700_000_000_000_000_000


def make_dicts(n: int) -> list[dict[str, Any]]:
    return [
        {
            "measurement": "cpu",
            "tags": {"host": f"host-{i % 200}", "region": "eu-west-1", "dc": f"dc{i % 3}"},
            "fields": {"usage_user": 12.5 + (i % 10), "usage_system": 3.25, "load": i % 7},
            "time": BASE_NS + i,
        }
        for i in range(n)
    ]


def bench(label: str, n: int, fn: Callable[[], Any], repeat: int = 5) -> float:
    best = float("inf")
    for _ in range(repeat):
        gc.collect()
        start = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - start)
    rate = n / best
    print(f"{label:58s} {best * 1e3:8.1f} ms  {rate / 1e6:6.2f} M points/s  {best / n * 1e9:7.0f} ns/point")
    return rate


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--points", type=int, default=200_000)
    args = parser.parse_args()
    n = args.points
    dicts = make_dicts(n)
    points = [Point(d["measurement"], d["tags"], d["fields"], d["time"]) for d in dicts]

    def serializer(version: int = 3, **validation: Any) -> Serializer:
        return Serializer(
            dialect=Dialect.for_version(version),
            validation=ValidationConfig(**validation),
            schemas={},
            injector=TagInjector(TagsConfig()),
            auto_timestamp=True,
        )

    print(f"{n:,} points, 3 tags + 3 fields each\n")
    ours = serializer()
    rate = bench(
        "sluicebox dicts (type lock on)", n, lambda: ours.serialize(dicts, database="db", precision="ns")
    )
    bench("sluicebox Points (type lock on)", n, lambda: ours.serialize(points, database="db", precision="ns"))
    no_lock = serializer(type_lock=False)
    bench(
        "sluicebox dicts (type lock off)", n, lambda: no_lock.serialize(dicts, database="db", precision="ns")
    )
    tagged = Serializer(
        dialect=Dialect.for_version(3),
        validation=ValidationConfig(),
        schemas={},
        injector=TagInjector(TagsConfig(static={"env": "prod", "app": "bench"})),
        auto_timestamp=True,
    )
    bench(
        "sluicebox dicts + 2 static tags", n, lambda: tagged.serialize(dicts, database="db", precision="ns")
    )

    try:
        from influxdb_client import Point as V2Point
    except ImportError:
        V2Point = None
    if V2Point is not None:

        def official_v2_dicts() -> None:
            for d in dicts:
                V2Point.from_dict(d).to_line_protocol()

        official = bench("influxdb-client Point.from_dict().to_line_protocol()", n, official_v2_dicts)
        print(f"{'':58s} -> sluicebox is {rate / official:.1f}x faster")

    try:
        from influxdb_client_3 import Point as V3Point
    except ImportError:
        V3Point = None
    if V3Point is not None:

        def official_v3_points() -> None:
            for d in dicts:
                p = V3Point(d["measurement"])
                for k, v in d["tags"].items():
                    p.tag(k, v)
                for k, v in d["fields"].items():
                    p.field(k, v)
                p.time(d["time"])
                p.to_line_protocol()

        official3 = bench("influxdb3-python Point builder + to_line_protocol()", n, official_v3_points)
        print(f"{'':58s} -> sluicebox is {rate / official3:.1f}x faster")


if __name__ == "__main__":
    main()
