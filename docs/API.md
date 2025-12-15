# API Reference

Complete API documentation for the streaming platform.

## Table of Contents

1. [Configuration](#configuration)
2. [Schema Generation](#schema-generation)
3. [Data Generation](#data-generation)
4. [Kafka Producer](#kafka-producer)
5. [Spark Streaming](#spark-streaming)
6. [Sinks](#sinks)
7. [Monitoring](#monitoring)

---

## Configuration

### ConfigLoader

Load and merge configuration files with secret resolution.
```python
from core.config.loader import ConfigLoader

loader = ConfigLoader(config_dir="config")
```

#### Methods

##### `load(domain: str, environment: str = None) -> dict`

Load complete configuration for a domain.

**Parameters**:
- `domain`: Domain name (e.g., "ecommerce")
- `environment`: Optional environment override (e.g., "dev", "prod")

**Returns**: Merged configuration dictionary with secrets resolved

**Example**:
```python
config = loader.load(domain="ecommerce", environment="dev")

# Access config
kafka_servers = config["kafka"]["bootstrap_servers"]
event_types = config["event_types"]
```

**Merge Order**:
1. `platform.yaml` (base)
2. `environments/{environment}.yaml` (if specified)
3. `domains/{domain}.yaml` (domain-specific)

---

### SecretResolver

Resolve `${secret:KEY}` patterns in configuration.
```python
from core.secrets import SecretResolver

resolver = SecretResolver(backend="env")
```

#### Backends

| Backend | Config | Use Case |
|---------|--------|----------|
| `env` | None | Development (.env files) |
| `file` | `secrets_path` | File-based secrets |
| `vault` | `vault_url`, `vault_token` | Production (HashiCorp Vault) |

#### Methods

##### `resolve(config: dict) -> dict`

Recursively resolve all secrets in a configuration dictionary.

**Example**:
```python
config = {
    "database": {
        "password": "${secret:db_password}"
    }
}

resolved = resolver.resolve(config)
# {"database": {"password": "actual_password"}}
```

##### `health_check() -> bool`

Check if the secrets backend is accessible.

---

## Schema Generation

### SchemaGenerator

Generate Avro schemas from configuration.
```python
from core.schema.generator import SchemaGenerator

generator = SchemaGenerator()
```

#### Methods

##### `generate_event_schema(event_config: dict, entity_config: dict, namespace: str = "com.streaming.events") -> dict`

Generate Avro schema for a single event type.

**Parameters**:
- `event_config`: Event type configuration from domain config
- `entity_config`: Entity configuration (contains primary_key)
- `namespace`: Avro namespace

**Returns**: Avro schema dictionary

**Example**:
```python
event_config = {
    "name": "page_view",
    "attributes": ["page_url", "page_title"],
    "dimensions": ["device_type"]
}
entity_config = {"primary_key": "user_id"}

schema = generator.generate_event_schema(event_config, entity_config)
```

**Generated Schema**:
```json
{
  "type": "record",
  "namespace": "com.streaming.events",
  "name": "page_view",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "user_id", "type": "string"},
    {"name": "page_url", "type": ["null", "string"], "default": null},
    {"name": "page_title", "type": ["null", "string"], "default": null},
    {"name": "device_type", "type": "string"}
  ]
}
```

##### `generate_all_schemas(config: dict) -> dict[str, dict]`

Generate schemas for all event types in configuration.

**Returns**: Dictionary mapping event names to Avro schemas

**Example**:
```python
schemas = generator.generate_all_schemas(config)
# {"page_view": {...}, "purchase": {...}, ...}
```

##### `to_json(schema: dict) -> str`

Convert schema to JSON string for Schema Registry.

---

## Data Generation

### SyntheticDataGenerator

Generate realistic synthetic events for testing.
```python
from generators.synthetic import SyntheticDataGenerator

generator = SyntheticDataGenerator(config, schemas)
```

#### Constructor Parameters

- `config`: Platform configuration
- `schemas`: Generated Avro schemas
- `user_pool_size`: Number of simulated users (default: 100)

#### Methods

##### `generate_event(event_type: str, user_id: str = None, timestamp: int = None) -> dict`

Generate a single event.

**Parameters**:
- `event_type`: Event type name (e.g., "page_view")
- `user_id`: Optional specific user ID
- `timestamp`: Optional timestamp in milliseconds

**Returns**: Event dictionary matching Avro schema

**Example**:
```python
event = generator.generate_event("page_view")
# {
#   "event_id": "550e8400-e29b-41d4-a716-446655440000",
#   "timestamp": 1702567890123,
#   "user_id": "user_42",
#   "page_url": "https://example.com/products",
#   "page_title": "Products",
#   "device_type": "mobile"
# }
```

##### `generate_events(event_type: str, count: int) -> Generator[dict]`

Generate multiple events as a generator.

**Example**:
```python
for event in generator.generate_events("page_view", count=100):
    producer.send("page_view", event)
```

##### `generate_user_session(user_id: str = None, event_count: int = None) -> list[dict]`

Generate a realistic user session (sequence of related events).

**Session Pattern**:
1. `page_view` (landing page)
2. Multiple `page_view` events (browsing)
3. Optional `add_to_cart`
4. Optional `purchase`

**Example**:
```python
session = generator.generate_user_session()
# [
#   {"event_type": "page_view", ...},
#   {"event_type": "page_view", ...},
#   {"event_type": "add_to_cart", ...},
#   {"event_type": "purchase", ...}
# ]
```

---

## Kafka Producer

### KafkaEventProducer

Send events to Kafka with Avro serialization.
```python
from sources.kafka_producer import KafkaEventProducer

producer = KafkaEventProducer(config, schemas)
```

#### Methods

##### `send(event_type: str, event: dict, key: str = None) -> None`

Send a single event to Kafka.

**Parameters**:
- `event_type`: Event type name (determines topic)
- `event`: Event dictionary
- `key`: Optional partition key (default: user_id)

**Topic Naming**: `{prefix}.{event_type}` (e.g., `events.page_view`)

**Example**:
```python
producer.send("page_view", event)
producer.send("purchase", event, key=event["order_id"])
```

##### `send_batch(event_type: str, events: list[dict]) -> None`

Send multiple events efficiently.

**Example**:
```python
events = list(generator.generate_events("page_view", 1000))
producer.send_batch("page_view", events)
```

##### `flush(timeout: float = 10.0) -> None`

Wait for all pending messages to be delivered.

##### `get_stats() -> dict`

Get producer statistics.

**Returns**:
```python
{"sent": 1000, "errors": 0}
```

##### `health_check() -> bool`

Verify Kafka connectivity.

##### `close() -> None`

Close producer and release resources.

#### Full Example
```python
from core.config.loader import ConfigLoader
from core.schema.generator import SchemaGenerator
from generators.synthetic import SyntheticDataGenerator
from sources.kafka_producer import KafkaEventProducer

# Setup
loader = ConfigLoader()
config = loader.load(domain="ecommerce")
schemas = SchemaGenerator().generate_all_schemas(config)

# Create producer
producer = KafkaEventProducer(config, schemas)
producer.health_check()

# Generate and send
generator = SyntheticDataGenerator(config, schemas)
for event in generator.generate_events("page_view", count=1000):
    producer.send("page_view", event)

producer.flush()
print(producer.get_stats())  # {"sent": 1000, "errors": 0}
producer.close()
```

---

## Spark Streaming

### StreamingJob

Spark Structured Streaming job for event processing.
```python
from spark.streaming_job import StreamingJob

job = StreamingJob(config, schemas)
```

#### Methods

##### `read_stream(event_types: list[str]) -> DataFrame`

Create a streaming DataFrame from Kafka topics.

**Parameters**:
- `event_types`: List of event types to subscribe to

**Returns**: Raw Spark streaming DataFrame

##### `parse_events(df: DataFrame, event_type: str) -> DataFrame`

Parse Avro-encoded events into structured columns.

**Parameters**:
- `df`: Raw streaming DataFrame
- `event_type`: Event type (for schema lookup)

**Returns**: Parsed DataFrame with event fields as columns

##### `aggregate_events(df: DataFrame, agg_config: dict) -> DataFrame`

Apply windowed aggregation.

**Parameters**:
- `df`: Parsed streaming DataFrame
- `agg_config`: Aggregation configuration from domain config

**Aggregation Config**:
```yaml
aggregations:
  - name: events_per_minute
    type: count           # count, count_distinct, sum, avg
    field: null           # required for sum, avg, count_distinct
    window:
      type: tumbling      # tumbling or sliding
      duration: 1 minute
      slide: 30 seconds   # only for sliding
    group_by:
      - device_type
```

**Returns**: Aggregated streaming DataFrame

##### `write_console(df: DataFrame, query_name: str) -> None`

Write stream to console (for debugging).

##### `write_foreach_batch(df: DataFrame, batch_func: callable, query_name: str) -> None`

Write stream using custom batch function.

**Parameters**:
- `df`: Streaming DataFrame
- `batch_func`: Function `(batch_df, batch_id) -> None`
- `query_name`: Unique query name

**Example**:
```python
def write_to_clickhouse(batch_df, batch_id):
    sink.write_batch(batch_df, batch_id, "events_per_minute", column_mapping)

job.write_foreach_batch(agg_df, write_to_clickhouse, "ch_sink")
```

##### `await_termination(timeout: int = None) -> None`

Wait for streaming queries to terminate.

##### `stop() -> None`

Stop all streaming queries.

#### Full Example
```python
from spark.streaming_job import StreamingJob
from sinks import ClickHouseSink

job = StreamingJob(config, schemas)
sink = ClickHouseSink(config)

# Build pipeline
raw_df = job.read_stream(["page_view"])
parsed_df = job.parse_events(raw_df, "page_view")

agg_config = config["aggregations"][0]
agg_df = job.aggregate_events(parsed_df, agg_config)

# Write to sink
column_mapping = {
    "window.start": "window_start",
    "window.end": "window_end",
    "device_type": "device_type",
    "count": "count"
}

def write_batch(df, batch_id):
    sink.write_batch(df, batch_id, "events_per_minute", column_mapping)

job.write_foreach_batch(agg_df, write_batch, "clickhouse")

try:
    job.await_termination()
except KeyboardInterrupt:
    job.stop()
```

---

## Sinks

### BaseSink

Abstract base class for all sinks.
```python
from sinks.base import BaseSink
```

#### Abstract Methods

| Method | Description |
|--------|-------------|
| `_connect()` | Establish connection |
| `_write_rows(table, rows, columns)` | Write rows to table |
| `health_check()` | Verify connectivity |
| `create_table(table, schema)` | Create table from schema |
| `close()` | Close connection |

#### Concrete Methods

##### `write_batch(batch_df, batch_id: int, table: str, column_mapping: dict) -> int`

Generic batch writer with metrics tracking.

**Parameters**:
- `batch_df`: Spark DataFrame from foreachBatch
- `batch_id`: Batch identifier
- `table`: Target table name
- `column_mapping`: Maps DataFrame columns to table columns

**Column Mapping**:
```python
{
    "window.start": "window_start",  # Nested field
    "device_type": "device_type",    # Direct field
    "count": "event_count"           # Rename field
}
```

**Returns**: Number of rows written

---

### ClickHouseSink

ClickHouse sink implementation.
```python
from sinks import ClickHouseSink

sink = ClickHouseSink(config)
```

#### Methods

##### `health_check() -> bool`

Check connectivity and create database if needed.

##### `create_table(table: str, schema: dict) -> None`

Create table from schema definition.

**Schema Format**:
```python
schema = {
    "columns": [
        {"name": "window_start", "type": "datetime"},
        {"name": "device_type", "type": "string"},
        {"name": "count", "type": "long"}
    ],
    "order_by": ["window_start", "device_type"]
}
```

**Type Mapping**:
| Schema Type | ClickHouse Type |
|-------------|-----------------|
| string | String |
| int | Int64 |
| long | Int64 |
| float | Float64 |
| double | Float64 |
| boolean | UInt8 |
| timestamp | DateTime |
| datetime | DateTime |
| date | Date |

##### `query(sql: str) -> QueryResult`

Execute a query and return results.

**Example**:
```python
result = sink.query("SELECT * FROM events_per_minute LIMIT 10")
for row in result.result_rows:
    print(row)
```

---

### PostgreSQLSink

PostgreSQL sink implementation.
```python
from sinks import PostgreSQLSink

sink = PostgreSQLSink(config)
```

#### Methods

##### `health_check() -> bool`

Check connectivity and create database if needed.

##### `create_table(table: str, schema: dict) -> None`

Create table from schema definition.

**Schema Format**:
```python
schema = {
    "columns": [
        {"name": "id", "type": "string", "nullable": False},
        {"name": "event_type", "type": "string"},
        {"name": "count", "type": "long"}
    ],
    "primary_key": "id"
}
```

##### `query(sql: str, params: tuple = None) -> list`

Execute a query with optional parameters.

**Example**:
```python
rows = sink.query(
    "SELECT * FROM events WHERE event_type = %s",
    ("page_view",)
)
```

---

## Monitoring

### Metrics

Static class for recording Prometheus metrics.
```python
from monitoring import Metrics
```

#### Methods

##### `Metrics.produce_success(event_type: str, latency: float = None) -> None`

Record successful event production.

##### `Metrics.produce_error(event_type: str, latency: float = None) -> None`

Record failed event production.

##### `Metrics.sink_write(sink: str, table: str, rows: int, latency: float = None, success: bool = True) -> None`

Record sink write operation.

**Example**:
```python
from monitoring import Metrics, track_time

with track_time() as t:
    producer.send("page_view", event)
Metrics.produce_success("page_view", latency=t["duration"])
```

---

### track_time

Context manager for timing operations.
```python
from monitoring import track_time

with track_time() as t:
    do_work()

print(f"Took {t['duration']:.3f}s")
```

---

### start_metrics_server

Start Prometheus HTTP server.
```python
from monitoring import start_metrics_server

start_metrics_server(port=8000)
# Metrics available at http://localhost:8000/metrics
```

---

### PrometheusQueryListener

Spark StreamingQueryListener for automatic metrics.
```python
from monitoring import PrometheusQueryListener

listener = PrometheusQueryListener()
spark.streams.addListener(listener)
```

**Captured Metrics**:
- `spark_batches_total` - Batches processed
- `spark_batch_duration_seconds` - Batch duration
- `spark_batch_rows` - Rows per batch
- `spark_input_rows_per_sec` - Input rate
- `spark_processing_rows_per_sec` - Processing rate

---

## Utility Functions

### Decorators
```python
from core.utils.decorators import timed, retry, logged
```

##### `@timed`

Log function execution time.
```python
@timed
def process_data():
    ...
# Logs: "process_data completed in 1.234s"
```

##### `@retry(max_attempts=3, delay=1.0, exceptions=(Exception,))`

Retry function on failure.
```python
@retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError,))
def connect_to_db():
    ...
```

##### `@logged`

Log function entry and exit.
```python
@logged
def process_event(event):
    ...
# Logs: "Calling process_event"
# Logs: "process_event returned"
```

---

### Logging
```python
from core.utils.logging import get_logger, setup_logging

# Setup (call once at startup)
setup_logging(level="INFO")

# Get logger for module
logger = get_logger(__name__)
logger.info("Processing started")
```

**Log Format**:
```
2025-12-14 13:45:30 | INFO     | module_name | Processing started
```
