"""Spark Streaming query listener for Prometheus metrics."""

from pyspark.sql.streaming import StreamingQueryListener

from core.utils.logging import get_logger
from monitoring.definitions import (
    SPARK_BATCHES,
    SPARK_BATCH_DURATION,
    SPARK_BATCH_ROWS,
    SPARK_INPUT_RATE,
    SPARK_PROCESSING_RATE,
)

logger = get_logger(__name__)


class PrometheusQueryListener(StreamingQueryListener):
    """
    Spark StreamingQueryListener that exports metrics to Prometheus.

    Usage:
        from monitoring import PrometheusQueryListener

        listener = PrometheusQueryListener()
        spark.streams.addListener(listener)
    """

    def onQueryStarted(self, event) -> None:
        """Called when a streaming query starts."""
        logger.info(f"Query started: {event.name or event.id}")

    def onQueryProgress(self, event) -> None:
        """Called on each micro-batch completion."""
        p = event.progress
        query = p.name or str(p.id)[:8]

        SPARK_BATCHES.labels(query=query).inc()

        if p.batchDuration:
            SPARK_BATCH_DURATION.labels(query=query).observe(p.batchDuration / 1000.0)

        if p.numInputRows:
            SPARK_BATCH_ROWS.labels(query=query).observe(p.numInputRows)

        if p.inputRowsPerSecond:
            SPARK_INPUT_RATE.labels(query=query).set(p.inputRowsPerSecond)

        if p.processedRowsPerSecond:
            SPARK_PROCESSING_RATE.labels(query=query).set(p.processedRowsPerSecond)

    def onQueryTerminated(self, event) -> None:
        """Called when a streaming query terminates."""
        if event.exception:
            logger.error(f"Query failed: {event.exception}")
        else:
            logger.info(f"Query terminated: {event.name or event.id}")
