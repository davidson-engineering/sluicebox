"""A long-running producer: fire-and-forget writes, dead-lettering, metrics and clean shutdown.

uv run python examples/ingest_service.py      (Ctrl+C to stop)
"""

import json
import logging
import random
import signal
import threading
import time
from pathlib import Path

from sluicebox import InfluxClient, WriteFailure, configure_logging, tag_context

DEAD_LETTERS = Path("dead-letters.lp")
stop = threading.Event()


def dead_letter(failure: WriteFailure) -> None:
    """Called on a sender thread for every batch that could not be written (after retries)."""
    if not failure.retryable:
        logging.getLogger("ingest").error("rejected by the server, not replayable: %s", failure.error)
        return
    # Replay later with client.write(lines, database=..., precision=failure.precision).
    with DEAD_LETTERS.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(failure.lines) + "\n")
    logging.getLogger("ingest").error(
        "dead-lettered %d points for %s: %s", failure.points, failure.database, failure.error
    )


def main() -> None:
    configure_logging()  # or set [logging] configure = true
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    with InfluxClient.from_config(on_error=dead_letter, tags={"service": "ingest-example"}) as client:
        client.check()  # fail fast on a wrong URL, version, token or bucket
        sensors = [f"lon-{i}" for i in range(50)]
        started = time.monotonic()
        while not stop.is_set() and time.monotonic() - started < 10:
            # Scoped tags with bounded values (a job id or timestamp would create a series each).
            with tag_context(site="london"):
                for sensor in sensors:
                    client.write(
                        {
                            "measurement": "sensor_climate",
                            "tags": {"device": sensor},
                            "fields": {
                                "temperature": random.gauss(22, 8),
                                "humidity": random.uniform(20, 80),
                            },
                        }
                    )
            time.sleep(0.1)
        client.flush()  # wait for everything written so far
        stats = client.stats()
        print(
            json.dumps({"written": stats.write.points_written, "failed": stats.write.points_failed}, indent=2)
        )
    # Leaving the block flushes again and closes; buffered data is never silently dropped.


if __name__ == "__main__":
    main()
