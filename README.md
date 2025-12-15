# Streaming Platform

[![CI](https://github.com/mohammedadnansohail1-pixel/streaming-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/mohammedadnansohail1-pixel/streaming-platform/actions/workflows/ci.yml)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Production-grade, config-driven streaming data platform with CDC support.**

## ✨ Features

- **🔧 Config-Driven** - Define domains in YAML, no code changes needed
- **📊 Real-Time Analytics** - Sub-second aggregations with Spark Structured Streaming
- **🔄 CDC (Change Data Capture)** - Capture database changes with Debezium
- **📋 Schema Evolution** - Avro + Schema Registry for safe schema changes
- **📈 Full Observability** - Prometheus metrics + Grafana dashboards
- **🔌 Pluggable Sinks** - ClickHouse, PostgreSQL, extensible base class
- **🔐 Secure Secrets** - Registry pattern with env/file/vault backends
- **✅ Production Ready** - 92 unit tests, CI/CD, health checks

## 🏗️ Architecture
```
┌──────────────────────────────────────────────────────────────────────────────┐
│                            DATA SOURCES                                       │
├──────────────────────────────────┬───────────────────────────────────────────┤
│         PostgreSQL               │           Synthetic Generator              │
│         (CDC Source)             │           (Event Generator)                │
└────────────┬─────────────────────┴───────────────────┬───────────────────────┘
             │                                         │
             ▼                                         │
┌────────────────────────┐                            │
│       Debezium         │                            │
│   (Change Data Capture)│                            │
└────────────┬───────────┘                            │
             │                                         │
             ▼                                         ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                              KAFKA + SCHEMA REGISTRY                          │
│                        (Message Broker + Schema Management)                   │
└────────────────────────────────────┬─────────────────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                          SPARK STRUCTURED STREAMING                           │
│              (Windowed Aggregations, Watermarks, Checkpointing)               │
└────────────────────────────────────┬─────────────────────────────────────────┘
                                     │
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
          ┌─────────────────┐               ┌─────────────────┐
          │   ClickHouse    │               │   PostgreSQL    │
          │   (Analytics)   │               │ (Transactional) │
          └─────────────────┘               └─────────────────┘
                    │                                 │
                    └────────────────┬────────────────┘
                                     ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                      PROMETHEUS + GRAFANA (Monitoring)                        │
└──────────────────────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### One-Command Setup
```bash
# Clone
git clone https://github.com/mohammedadnansohail1-pixel/streaming-platform.git
cd streaming-platform

# Start all infrastructure
docker compose -f docker/docker-compose.yml up -d

# Setup Python
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

# Check all services
./streaming-cli health
```

### Expected Output
```
✓ Config loaded (ecommerce)
✓ Schemas generated (6 event types)
✓ Kafka connected (7 topics)
✓ Schema Registry connected (0 subjects)
✓ ClickHouse connected
✓ PostgreSQL connected
✓ Debezium connected (1 connectors)
```

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / streaming123 |
| Prometheus | http://localhost:9090 | - |
| Schema Registry | http://localhost:8081 | - |
| ClickHouse | http://localhost:8123 | default / streaming123 |
| Debezium | http://localhost:8083 | - |

## 📁 Project Structure
```
streaming-platform/
├── config/
│   ├── platform.yaml           # Kafka, Spark, sinks config
│   └── domains/
│       ├── ecommerce.yaml      # E-commerce events
│       ├── iot.yaml            # IoT sensor events
│       └── fintech.yaml        # Financial transactions
├── core/
│   ├── config/                 # Config loader + secret resolution
│   ├── schema/                 # Avro schema generator
│   └── secrets/                # Pluggable secrets backends
├── generators/                 # Synthetic data generation
├── sources/                    # Kafka producer
├── spark/                      # Streaming jobs
├── sinks/                      # ClickHouse, PostgreSQL
├── cdc/                        # Debezium CDC consumer
├── monitoring/                 # Prometheus + Grafana
├── cli/                        # Command-line interface
├── docker/                     # All-in-one Docker Compose
└── tests/                      # 92 unit tests
```

## 🖥️ CLI Commands
```bash
# List available domains
./streaming-cli domains

# Show configuration
./streaming-cli config --domain ecommerce

# Check all services health
./streaming-cli health

# Generate and send events to Kafka
./streaming-cli generate --domain ecommerce --event-type page_view --count 100

# Show Avro schema
./streaming-cli schema --event-type purchase

# Run Spark streaming job
./streaming-cli run --domain ecommerce --event-type page_view --aggregation events_per_minute
```

## 🔄 CDC (Change Data Capture)

Capture real-time database changes with Debezium:
```bash
# Test CDC - watch INSERT/UPDATE/DELETE events
python scripts/test_cdc.py
```

Output:
```
➕ INSERT   | customers    | {'id': 1, 'name': 'Jane Smith', ...}
📝 UPDATE   | customers    | {'id': 1, 'name': 'Jane Doe', ...}
➕ INSERT   | orders       | {'id': 1, 'amount_cents': 9999, ...}
📝 UPDATE   | orders       | {'id': 1, 'status': 'completed', ...}
❌ DELETE   | orders       | {'id': 1, ...}
```

## 🎯 Use Cases

| Domain | Event Types | Aggregations |
|--------|-------------|--------------|
| **E-Commerce** | page_view, add_to_cart, purchase | events_per_minute, revenue_per_hour |
| **IoT** | sensor_reading, alert, device_status | avg_temperature, alerts_by_severity |
| **Fintech** | transaction, login, fraud_alert | transaction_volume, failed_logins |

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| Message Broker | Apache Kafka 7.5 |
| Schema Registry | Confluent Schema Registry |
| Stream Processing | Spark Structured Streaming 3.5 |
| CDC | Debezium 2.5 |
| Serialization | Apache Avro |
| Analytics DB | ClickHouse |
| Transactional DB | PostgreSQL 16 |
| Monitoring | Prometheus + Grafana |
| Language | Python 3.12 |

## 📈 Performance

| Metric | Value |
|--------|-------|
| Producer Throughput | 50K events/sec |
| End-to-End Latency | < 500ms |
| CDC Latency | < 100ms |
| Spark Batch Processing | 100K events/sec |

## 🧪 Testing
```bash
# Run all tests (92 tests)
pytest tests/unit/ -v

# With coverage
pytest tests/unit/ --cov=core --cov=generators --cov=sources --cov=spark --cov=sinks --cov=monitoring --cov=cdc

# Lint
ruff check . && black --check .
```

## 📚 Documentation

- [Architecture & Design Decisions](docs/ARCHITECTURE.md)
- [Setup Guide](docs/SETUP.md)
- [API Reference](docs/API.md)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing`)
5. Open a Pull Request

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

## 👤 Author

**Adnan** - Data Engineer

Building production-grade streaming systems for real-time analytics.

---

<p align="center">
  <b>⭐ Star this repo if you find it useful!</b>
</p>
