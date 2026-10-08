"""Write and read back a few points.

Setup: copy influxkit.example.toml to influxkit.toml (adjust [connection]) and put
INFLUXKIT_TOKEN=... in .env, then run:  uv run python examples/quickstart.py
"""

from datetime import UTC, datetime

from influxkit import InfluxClient, Point

with InfluxClient.from_config() as client:
    print("connected to", client.check())  # verifies URL, version, token and database

    # write() validates, serializes and buffers, then returns immediately ("asynchronous mode").
    client.write(Point("quickstart").tag("host", "web-1").field("load", 0.42).time(datetime.now(UTC)))
    client.write({"measurement": "quickstart", "tags": {"host": "web-2"}, "fields": {"load": 0.17}})

    # .result() waits for the server's acknowledgement ("synchronous mode") and raises on failure.
    result = client.write(
        f"quickstart,host=web-3 load=0.99 {datetime.now(UTC).timestamp():.0f}", precision="s"
    ).result()
    print(f"acknowledged {result.points} point(s) in {result.duration * 1000:.1f} ms")

    if client.settings.connection.version == 3:
        rows = client.query("SELECT host, load FROM quickstart ORDER BY time DESC LIMIT 5")
    else:
        rows = client.query(
            "from(bucket: params.bucket) |> range(start: -1h) "
            '|> filter(fn: (r) => r._measurement == "quickstart") '
            '|> keep(columns: ["_time", "host", "_value"])',
            params={"bucket": client.settings.connection.database},
        )
    for row in rows:
        print(row)
