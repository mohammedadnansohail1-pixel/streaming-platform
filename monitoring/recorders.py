"""Metrics recorder - stateless functions to record metrics."""

import time
from contextlib import contextmanager
from typing import Generator

from monitoring.definitions import (
    EVENTS_PRODUCED,
    PRODUCE_LATENCY,
    SINK_WRITES,
    SINK_ROWS,
    SINK_LATENCY,
)


@contextmanager
def track_time() -> Generator[dict, None, None]:
    """
    Context manager to track execution time.

    Usage:
        with track_time() as t:
            do_work()
        print(t["duration"])  # seconds
    """
    result = {"duration": 0.0}
    start = time.perf_counter()
    try:
        yield result
    finally:
        result["duration"] = time.perf_counter() - start


class Metrics:
    """
    Stateless metrics recorder.

    Usage:
        from monitoring import Metrics, track_time

        with track_time() as t:
            producer.send(event)
        Metrics.produce_success("page_view", latency=t["duration"])
    """

    @staticmethod
    def produce_success(event_type: str, latency: float = None) -> None:
        """Record successful event production."""
        EVENTS_PRODUCED.labels(event_type=event_type, status="success").inc()
        if latency:
            PRODUCE_LATENCY.labels(event_type=event_type).observe(latency)

    @staticmethod
    def produce_error(event_type: str, latency: float = None) -> None:
        """Record failed event production."""
        EVENTS_PRODUCED.labels(event_type=event_type, status="error").inc()
        if latency:
            PRODUCE_LATENCY.labels(event_type=event_type).observe(latency)

    @staticmethod
    def sink_write(
        sink: str, table: str, rows: int, latency: float = None, success: bool = True
    ) -> None:
        """Record sink write operation."""
        status = "success" if success else "error"
        SINK_WRITES.labels(sink=sink, table=table, status=status).inc()
        if success:
            SINK_ROWS.labels(sink=sink, table=table).inc(rows)
        if latency:
            SINK_LATENCY.labels(sink=sink, table=table).observe(latency)
