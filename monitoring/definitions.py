"""Prometheus metric definitions."""

from prometheus_client import Counter, Histogram, Gauge

# ============================================================
# PRODUCER METRICS
# ============================================================

EVENTS_PRODUCED = Counter(
    "events_produced_total", "Events produced to Kafka", ["event_type", "status"]
)

PRODUCE_LATENCY = Histogram(
    "produce_latency_seconds",
    "Time to produce event",
    ["event_type"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

# ============================================================
# SPARK STREAMING METRICS
# ============================================================

SPARK_BATCHES = Counter(
    "spark_batches_total", "Spark micro-batches processed", ["query"]
)

SPARK_BATCH_DURATION = Histogram(
    "spark_batch_duration_seconds",
    "Batch processing time",
    ["query"],
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
)

SPARK_BATCH_ROWS = Histogram(
    "spark_batch_rows",
    "Rows per batch",
    ["query"],
    buckets=[10, 50, 100, 500, 1000, 5000, 10000],
)

SPARK_INPUT_RATE = Gauge("spark_input_rows_per_sec", "Input rate", ["query"])

SPARK_PROCESSING_RATE = Gauge(
    "spark_processing_rows_per_sec", "Processing rate", ["query"]
)

# ============================================================
# SINK METRICS
# ============================================================

SINK_WRITES = Counter(
    "sink_writes_total", "Writes to sink", ["sink", "table", "status"]
)

SINK_ROWS = Counter("sink_rows_total", "Rows written to sink", ["sink", "table"])

SINK_LATENCY = Histogram(
    "sink_latency_seconds",
    "Sink write latency",
    ["sink", "table"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)
