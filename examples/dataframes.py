"""Write a polars (or pandas) DataFrame: serialization is vectorized, millions of rows per second.

uv run python examples/dataframes.py
"""

from datetime import UTC, datetime, timedelta

import polars as pl

from influxkit import InfluxClient

now = datetime.now(UTC)
frame = pl.DataFrame(
    {
        "time": [now - timedelta(seconds=i) for i in range(100_000)],
        "host": [f"host-{i % 100}" for i in range(100_000)],
        "cpu": [(i % 1000) / 10.0 for i in range(100_000)],
        "requests": [i % 500 for i in range(100_000)],
    }
)

with InfluxClient.from_config() as client:
    result = client.write(frame, measurement="frame_demo", tag_columns=["host"]).result()
    print(f"wrote {result.points:,} rows in {result.duration:.2f} s")
    if client.settings.connection.version == 3:
        print(
            client.query(
                "SELECT host, avg(cpu) AS cpu FROM frame_demo GROUP BY host ORDER BY host LIMIT 3"
            ).to_polars()
        )
