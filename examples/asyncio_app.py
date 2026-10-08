"""asyncio usage: nothing here blocks the event loop.

uv run python examples/asyncio_app.py
"""

import asyncio
from datetime import UTC, datetime, timedelta

from influxkit import AsyncInfluxClient


async def main() -> None:
    async with AsyncInfluxClient.from_config() as client:
        # Buffer (returns quickly), then await the acknowledgement. Each point gets its own
        # timestamp: points of one series without timestamps would share the write() time.
        now = datetime.now(UTC)
        points = [
            {"measurement": "async_demo", "fields": {"value": float(i)}, "time": now - timedelta(seconds=i)}
            for i in range(1000)
        ]
        future = await client.write(points)
        result = await future
        print(f"{result.points} points acknowledged")

        if client.settings.connection.version == 3:
            query = "SELECT count(*) AS n FROM async_demo"
            print((await client.query(query)).to_dicts())
            async for chunk in client.query_stream("SELECT * FROM async_demo LIMIT 2500"):
                print("chunk with", len(chunk), "rows")


asyncio.run(main())
