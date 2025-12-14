# Streaming Platform

A production-grade, config-driven streaming data platform for real-time event processing.

## Overview

Generic streaming platform that processes real-time events from any domain (e-commerce, IoT, fintech, APM) through a unified pipeline. Domain logic is defined in YAML configuration files, not code.

## Architecture
```
┌─────────────┐     ┌─────────┐     ┌─────────────────┐     ┌────────────┐
│  Synthetic  │────▶│  Kafka  │────▶│  Spark Streaming │────▶│ ClickHouse │
│  Generator  │     │ + Avro  │     │  (Aggregations)  │     │ (Analytics)│
└─────────────┘     └─────────┘     └─────────────────┘     └────────────┘
       │                 │                   │                     │
       └─────────────────┴───────────────────┴─────────────────────┘
                                    │
                         ┌──────────┴──────────┐
                         │   Config-Driven     │
                         │   (YAML + Secrets)  │
                         └─────────────────────┘
```

## Features

### Implemented ✅
- **Config-driven pipeline**: Domain logic defined in YAML, no code changes needed
- **Pluggable secrets management**: Registry pattern with env and file backends
- **Avro schema generation**: Auto-generate schemas from config with money field handling
- **Synthetic data generation**: Realistic event generation with user sessions
- **Kafka producer**: Avro serialization with Schema Registry integration
- **Spark Structured Streaming**: Windowed aggregations (tumbling/sliding)
- **ClickHouse sink**: Real-time analytics storage
- **Comprehensive testing**: 68 unit tests with mocked dependencies
- **Logging & decorators**: Centralized logging, retry, timing decorators

### Planned 🚧
- Dead Letter Queue for error handling
- Prometheus metrics
- Grafana dashboards
- Additional secret backends (Vault, AWS, Azure, GCP)

## Tech Stack

| Component | Technology |
|-----------|------------|
| Message Broker | Apache Kafka 7.5 |
| Schema Management | Confluent Schema Registry |
| Stream Processing | Spark Structured Streaming 3.5 |
| Serialization | Apache Avro |
| Analytics Store | ClickHouse |
| Language | Python 3.12 |

## Project Structure
```
streaming-platform/
├── config/
│   ├── platform.yaml           # Global settings (Kafka, Spark, sinks)
│   ├── environments/           # Environment overrides (dev, staging, prod)
│   └── domains/
│       └── ecommerce.yaml      # Domain-specific event definitions
├── core/
│   ├── config/                 # Config loader with YAML merging
│   ├── secrets/                # Pluggable secrets (registry pattern)
│   ├── schema/                 # Avro schema generator
│   └── utils/                  # Logging, decorators
├── generators/
│   └── synthetic.py            # Realistic event generator
├── sources/
│   └── kafka_producer.py       # Kafka producer with Avro
├── spark/
│   ├── streaming_job.py        # Spark Structured Streaming
│   └── sinks.py                # ClickHouse sink
├── scripts/
│   ├── test_config.py          # Full flow test
│   ├── test_kafka_producer.py  # Kafka integration test
│   └── test_clickhouse_sink.py # End-to-end test
└── tests/
    └── unit/                   # 68 unit tests
```

## Quick Start

### Prerequisites

- Python 3.12+
- Docker & Docker Compose
- Java 21 (for Spark)

### Setup
```bash
# Clone and setup
cd streaming-platform
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Set JAVA_HOME (add to ~/.bashrc for persistence)
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Start infrastructure
cd /path/to/kafka && docker compose up -d
cd /path/to/clickhouse && docker compose up -d
```

### Configure Secrets

Create `.env` file:
```bash
# Kafka
kafka_bootstrap_servers=localhost:9092
schema_registry_url=http://localhost:8081

# ClickHouse
clickhouse_host=localhost
clickhouse_user=default
clickhouse_password=your_password

# Checkpoints
checkpoint_path=./checkpoints
```

### Run Tests
```bash
# Unit tests
pytest tests/unit/ -v

# Full flow test (config → schema → events)
python scripts/test_config.py

# Kafka producer test
python scripts/test_kafka_producer.py

# End-to-end with ClickHouse
python scripts/test_clickhouse_sink.py
```

## Configuration

### Platform Config (`config/platform.yaml`)

Global settings for Kafka, Spark, and sinks:
```yaml
kafka:
  bootstrap_servers: ${secret:kafka_bootstrap_servers}
  schema_registry:
    url: ${secret:schema_registry_url}

spark:
  app_name: streaming-platform
  master: local[*]
  trigger_interval: 10 seconds
  watermark:
    delay: 10 minutes
```

### Domain Config (`config/domains/ecommerce.yaml`)

Domain-specific event definitions:
```yaml
domain: ecommerce
entity:
  primary_key: user_id

event_types:
  - name: page_view
    attributes:
      - page_url
      - page_title
    dimensions:
      - device_type
      - platform

aggregations:
  - name: events_per_minute
    type: count
    window:
      type: tumbling
      duration: 1 minute
    group_by:
      - device_type
```

### Adding a New Domain

1. Create `config/domains/your_domain.yaml`
2. Define entity, event_types, and aggregations
3. Run: `python scripts/test_config.py` with `domain="your_domain"`

No code changes required!

## Usage Examples

### Generate Synthetic Events
```python
from core.config.loader import ConfigLoader
from core.schema.generator import SchemaGenerator
from generators.synthetic import SyntheticDataGenerator

loader = ConfigLoader()
config = loader.load(domain="ecommerce")

generator = SchemaGenerator()
schemas = generator.generate_all_schemas(config)

data_gen = SyntheticDataGenerator(config, schemas)

# Single event
event = data_gen.generate_event("page_view")

# User session (realistic journey)
session = data_gen.generate_user_session()

# Batch generation
for event in data_gen.generate_events("page_view", count=100):
    print(event)
```

### Send to Kafka
```python
from sources.kafka_producer import KafkaEventProducer

producer = KafkaEventProducer(config, schemas)

for event in data_gen.generate_events("page_view", count=100):
    producer.send("page_view", event)

producer.flush()
print(producer.get_stats())  # {'sent': 100, 'errors': 0}
```

### Run Spark Streaming
```python
from spark.streaming_job import StreamingJob

job = StreamingJob(config, schemas)
job.run_aggregation_job("page_view", "events_per_minute")
job.await_termination()
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Config over code | Domain changes without deployments |
| Registry pattern for secrets | Easy to add new backends |
| Avro for serialization | Schema evolution, compact binary |
| Money as cents (long) | Avoid floating point errors |
| Watermarks in streaming | Handle late-arriving data |
| foreachBatch for sinks | Exactly-once semantics possible |

## Testing
```bash
# All unit tests
pytest tests/unit/ -v

# With coverage
pytest tests/unit/ --cov=core --cov=generators --cov=sources --cov=spark

# Specific module
pytest tests/unit/test_config_loader.py -v
```

## Author

**Adnan** - Data Engineer
Building production-grade streaming systems for real-time analytics.

## License

MIT
