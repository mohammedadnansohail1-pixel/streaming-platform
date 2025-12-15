# Architecture & Design Decisions

## Overview

This document explains the architectural choices made in building this production-grade streaming platform, the reasoning behind each decision, and trade-offs considered.

## Table of Contents

1. [High-Level Architecture](#high-level-architecture)
2. [Core Design Principles](#core-design-principles)
3. [Technology Choices](#technology-choices)
4. [Component Deep Dives](#component-deep-dives)
5. [Data Flow](#data-flow)
6. [Trade-offs & Alternatives](#trade-offs--alternatives)

---

## High-Level Architecture
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CONFIGURATION LAYER                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ platform.yaml│  │ ecommerce.yaml│ │  dev.yaml   │  │  Secret Resolver   │ │
│  │  (global)    │  │  (domain)    │  │ (environment)│ │ (env/file/vault)   │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              INGESTION LAYER                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────┐  │
│  │ Schema Generator │───▶│ Synthetic Data  │───▶│   Kafka Producer       │  │
│  │ (Avro from YAML) │    │   Generator     │    │ (Avro + Schema Registry)│  │
│  └─────────────────┘    └─────────────────┘    └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              PROCESSING LAYER                                │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    Spark Structured Streaming                        │    │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐ │    │
│  │  │ Read     │─▶│ Parse    │─▶│ Aggregate│─▶│ Write (foreachBatch) │ │    │
│  │  │ Stream   │  │ Avro     │  │ Window   │  │                      │ │    │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────────────────┘ │    │
│  │                                                                      │    │
│  │  Features: Watermarks, Checkpointing, Exactly-Once Semantics         │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              STORAGE LAYER                                   │
│  ┌─────────────────┐              ┌─────────────────┐                       │
│  │   ClickHouse    │              │   PostgreSQL    │                       │
│  │  (Analytics)    │              │ (Transactional) │                       │
│  │  - Aggregations │              │  - Raw events   │                       │
│  │  - Time-series  │              │  - Lookup data  │                       │
│  └─────────────────┘              └─────────────────┘                       │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            OBSERVABILITY LAYER                               │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────┐  │
│  │   Prometheus    │───▶│    Grafana      │    │  Spark Query Listener   │  │
│  │   (Metrics)     │    │  (Dashboards)   │    │  (Batch Metrics)        │  │
│  └─────────────────┘    └─────────────────┘    └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Core Design Principles

### 1. Configuration Over Code

**Decision**: Domain logic defined in YAML, not Python code.

**Why**:
- New domains (IoT, fintech, gaming) without code changes
- Non-engineers can modify event schemas
- Easier testing - change config, not code
- Version control for business logic

**Example**:
```yaml
# Adding a new event type = no code changes
event_types:
  - name: player_action
    attributes:
      - action_type
      - game_level
    dimensions:
      - platform
      - region
```

**Trade-off**: Less flexibility for complex transformations. Mitigated by allowing custom transformation functions.

### 2. Registry Pattern for Extensibility

**Decision**: Use registry pattern for secrets backends and sinks.

**Why**:
- Add new backends without modifying existing code
- Open/Closed Principle - open for extension, closed for modification
- Easy to test with mock backends
- Self-documenting - registered backends are discoverable

**Implementation**:
```python
# Register a new backend
@register_backend("vault")
class VaultBackend(SecretsBackend):
    ...

# Use it via config
secrets:
  backend: vault
  vault:
    url: https://vault.example.com
```

### 3. Lazy Initialization

**Decision**: Don't connect to external services until first use.

**Why**:
- Faster application startup
- Only pay for what you use
- Easier testing - no connections needed for unit tests
- Graceful handling of unavailable services

**Example**:
```python
class ClickHouseSink:
    def __init__(self, config):
        self._client = None  # Not connected yet

    def _get_client(self):
        if self._client is None:
            self._client = connect(...)  # Connect on first use
        return self._client
```

### 4. Fail Fast, Recover Gracefully

**Decision**: Validate early, retry transient failures, dead-letter permanent failures.

**Why**:
- Catch config errors at startup, not runtime
- Don't lose data due to temporary network issues
- Preserve failed events for debugging/replay

---

## Technology Choices

### Apache Kafka

**Chosen Over**: RabbitMQ, AWS Kinesis, Pulsar

**Reasons**:
| Factor | Kafka | RabbitMQ | Kinesis |
|--------|-------|----------|---------|
| Throughput | 1M+ msg/sec | 100K msg/sec | 1K records/shard |
| Retention | Configurable (days/weeks) | Until consumed | 24h-7 days |
| Replay | Yes (offset reset) | No | Yes (limited) |
| Schema Registry | Native (Confluent) | Manual | Manual |
| Ecosystem | Rich (Connect, Streams) | Limited | AWS-only |

**Key Insight**: Kafka's log-based architecture allows replay of events for debugging, reprocessing after bug fixes, and building new consumers without re-ingesting data.

### Apache Avro

**Chosen Over**: JSON, Protobuf, Parquet

**Reasons**:
| Factor | Avro | JSON | Protobuf |
|--------|------|------|----------|
| Schema Evolution | Excellent | None | Good |
| Size | Compact (binary) | Large (text) | Compact |
| Schema in Message | Optional | No | No |
| Dynamic Schemas | Yes | Yes | No (needs compile) |
| Spark Support | Native | Native | Plugin |

**Key Insight**: Avro's schema evolution (add/remove fields with defaults) is critical for production systems where you can't redeploy all consumers simultaneously.

### Spark Structured Streaming

**Chosen Over**: Flink, Kafka Streams, custom consumers

**Reasons**:
| Factor | Spark SS | Flink | Kafka Streams |
|--------|----------|-------|---------------|
| Exactly-Once | Yes | Yes | Yes |
| Batch + Stream | Unified API | Separate | Stream only |
| State Management | Built-in | Built-in | Built-in |
| Learning Curve | Moderate | Steep | Low |
| Ecosystem | Huge (ML, SQL) | Growing | Limited |

**Key Insight**: Spark's unified batch/streaming API means the same code works for historical backfills and real-time processing.

### ClickHouse

**Chosen Over**: PostgreSQL (for analytics), Druid, TimescaleDB

**Reasons**:
| Factor | ClickHouse | PostgreSQL | Druid |
|--------|------------|------------|-------|
| OLAP Performance | Excellent | Poor | Excellent |
| Compression | 10-20x | 2-3x | 10-20x |
| SQL Support | Full | Full | Limited |
| Real-time Ingestion | Yes | Yes | Yes (complex) |
| Operational Complexity | Low | Low | High |

**Key Insight**: ClickHouse's columnar storage and vectorized execution make it 100x faster than PostgreSQL for analytical queries while maintaining familiar SQL interface.

### PostgreSQL (Transactional)

**Purpose**: Store raw events, lookup tables, transactional data

**Why alongside ClickHouse**:
- ACID transactions for critical data
- Complex joins and updates
- Smaller datasets with row-level access patterns
- Familiar tooling and ORM support

---

## Component Deep Dives

### Configuration System
```
config/
├── platform.yaml      # Global: Kafka, Spark, sinks
├── environments/
│   ├── dev.yaml       # Override for development
│   └── prod.yaml      # Override for production
└── domains/
    └── ecommerce.yaml # Domain: events, aggregations
```

**Merge Order**: `platform.yaml` ← `environment.yaml` ← `domain.yaml`

**Secret Resolution**:
```yaml
# In config
password: ${secret:db_password}

# Resolved from .env
db_password=actual_password
```

**Why This Structure**:
1. **Separation of Concerns**: Infrastructure (platform) vs. business logic (domain)
2. **Environment Parity**: Same domain config in dev/staging/prod
3. **Security**: Secrets never in YAML files

### Schema Generator

**Input** (YAML):
```yaml
event_types:
  - name: purchase
    attributes:
      - product_id
    metrics:
      - name: amount
        type: money
```

**Output** (Avro):
```json
{
  "type": "record",
  "name": "purchase",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "product_id", "type": ["null", "string"]},
    {"name": "amount_cents", "type": "long"}
  ]
}
```

**Key Decision - Money as Cents (Long)**:
- Floating point errors: `0.1 + 0.2 = 0.30000000000000004`
- Cents as integers: `10 + 20 = 30` (always exact)
- Industry standard (Stripe, Square use cents)

### Streaming Job

**Window Types**:
```
Tumbling (non-overlapping):
|----1min----|----1min----|----1min----|
   events       events       events

Sliding (overlapping):
|----5min window----|
     |----5min window----|
          |----5min window----|
```

**Watermark Strategy**:
```python
df.withWatermark("event_time", "10 minutes")
```

- Events up to 10 minutes late are included
- After watermark passes, late events dropped
- Trade-off: Higher delay = more complete data, higher latency

**Checkpoint Strategy**:
- Location: `/checkpoints/{query_name}`
- Enables exactly-once semantics
- Allows recovery from failures without reprocessing

### Sink Architecture

**Base Class Pattern**:
```python
class BaseSink(ABC):
    @abstractmethod
    def _write_rows(self, table, rows, columns): pass

    def write_batch(self, df, batch_id, table, column_mapping):
        # Generic: extract rows, track metrics, call _write_rows
        with track_time() as t:
            self._write_rows(table, rows, columns)
        Metrics.sink_write(self.sink_name, table, len(rows), t["duration"])
```

**Why This Design**:
- Common logic (metrics, logging) in base class
- Only implement database-specific `_write_rows`
- Adding new sink = ~50 lines of code

### Monitoring

**Metric Types**:
```python
# Counter - things that only increase
EVENTS_PRODUCED = Counter("events_produced_total", ...)

# Histogram - distribution of values
PRODUCE_LATENCY = Histogram("produce_latency_seconds", ...)

# Gauge - point-in-time value
ACTIVE_QUERIES = Gauge("spark_active_queries", ...)
```

**Labels for Dimensionality**:
```python
# Good: specific, bounded cardinality
EVENTS_PRODUCED.labels(event_type="page_view", status="success").inc()

# Bad: unbounded cardinality (user_id)
EVENTS_PRODUCED.labels(user_id="12345").inc()  # Don't do this!
```

---

## Data Flow

### Happy Path
```
1. Config loaded, secrets resolved
2. Avro schemas generated from config
3. Synthetic generator creates event
4. Producer serializes to Avro, sends to Kafka
5. Schema registered in Schema Registry
6. Spark consumes from Kafka
7. Avro deserialized using schema
8. Window aggregation computed
9. Results written to ClickHouse via foreachBatch
10. Metrics recorded to Prometheus
```

### Error Handling
```
Producer Error:
  → Retry 3 times with exponential backoff
  → If still failing, log error, increment error counter
  → Event NOT lost (can replay from source)

Spark Processing Error:
  → Checkpoint preserves state
  → Restart from last checkpoint
  → No duplicate processing (exactly-once)

Sink Error:
  → Retry batch write
  → If persistent, stop query (fail-fast)
  → Checkpoint not advanced (will retry same batch)
```

---

## Trade-offs & Alternatives

### What We Chose vs. Alternatives

| Decision | Chose | Alternative | Why Not Alternative |
|----------|-------|-------------|---------------------|
| Config format | YAML | JSON, TOML | YAML more readable, supports comments |
| Serialization | Avro | Protobuf | Avro has better schema evolution, dynamic schemas |
| Stream processor | Spark | Flink | Spark has better ecosystem, unified batch/stream |
| Analytics DB | ClickHouse | Druid | ClickHouse simpler to operate, full SQL |
| Metrics | Prometheus | StatsD, DataDog | Prometheus is free, pull-based (more reliable) |

### What We Would Change at Scale

| Current | At Scale | Why |
|---------|----------|-----|
| Single Kafka broker | 3+ broker cluster | Fault tolerance, throughput |
| Local checkpoints | S3/MinIO | Durable across restarts |
| Single Spark app | Separate apps per domain | Isolation, independent scaling |
| Monolithic sinks | Separate sink services | Independent failure domains |

### Known Limitations

1. **No Dead Letter Queue** (yet): Failed events are logged, not persisted
2. **Single domain per run**: Can't process multiple domains simultaneously
3. **No backpressure**: If sink is slow, Spark buffers in memory
4. **Limited late data handling**: Events after watermark are dropped

---

## Performance Characteristics

### Throughput Expectations

| Component | Expected | Bottleneck |
|-----------|----------|------------|
| Producer | 50K events/sec | Network, serialization |
| Kafka | 1M+ events/sec | Disk I/O, partitions |
| Spark | 100K events/sec | CPU, memory |
| ClickHouse | 500K rows/sec | Disk I/O |

### Latency Expectations

| Stage | Expected Latency |
|-------|------------------|
| Produce to Kafka | 5-10ms |
| Kafka to Spark | 100ms (trigger interval) |
| Aggregation | 10-50ms |
| Write to ClickHouse | 50-200ms |
| **End-to-end** | **200-500ms** |

---

## Future Enhancements

1. **Schema Registry UI**: Visual schema management
2. **Multi-tenancy**: Domain isolation with resource quotas
3. **Auto-scaling**: Scale Spark executors based on lag
4. **Data quality**: Automated anomaly detection
5. **Lineage tracking**: Data provenance with OpenLineage

---

## References

- [Kafka: The Definitive Guide](https://www.confluent.io/resources/kafka-the-definitive-guide/)
- [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [ClickHouse Documentation](https://clickhouse.com/docs)
- [Designing Data-Intensive Applications](https://dataintensive.net/)
