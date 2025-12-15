# Streaming Platform

[![CI](https://github.com/mohammedadnansohail1-pixel/streaming-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/mohammedadnansohail1-pixel/streaming-platform/actions/workflows/ci.yml)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Production-grade, config-driven streaming data platform for real-time event processing.**

<p align="center">
  <img src="docs/images/architecture.png" alt="Architecture" width="800">
</p>

## ✨ Features

- **🔧 Config-Driven** - Define domains in YAML, no code changes needed
- **📊 Real-Time Analytics** - Sub-second aggregations with Spark Structured Streaming
- **🔄 Schema Evolution** - Avro + Schema Registry for safe schema changes
- **📈 Full Observability** - Prometheus metrics + Grafana dashboards
- **🔌 Pluggable Sinks** - ClickHouse, PostgreSQL, extensible base class
- **🔐 Secure Secrets** - Registry pattern with env/file/vault backends
- **✅ Production Ready** - 68 unit tests, CI/CD, health checks

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

# Run the demo
python scripts/test_clickhouse_sink.py
```

### What You'll See
```
✓ Events flowing: Kafka → Spark → ClickHouse
✓ Real-time aggregations by device type
✓ Data persisted to ClickHouse
```

**Service URLs:**
| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / streaming123 |
| Prometheus | http://localhost:9090 | - |
| Schema Registry | http://localhost:8081 | - |
| ClickHouse | http://localhost:8123 | default / streaming123 |

## 🏗️ Architecture
```
┌─────────────┐     ┌─────────┐     ┌─────────────────┐     ┌────────────┐
│  Synthetic  │────▶│  Kafka  │────▶│ Spark Streaming │────▶│ ClickHouse │
│  Generator  │     │ + Avro  │     │  (Aggregations) │     │ (Analytics)│
└─────────────┘     └─────────┘     └─────────────────┘     └────────────┘
       │                 │                   │                     │
       └─────────────────┴───────────────────┴─────────────────────┘
                                    │
                         ┌──────────┴──────────┐
                         │   Config-Driven     │
                         │   (YAML + Secrets)  │
                         └─────────────────────┘
```

## 📁 Project Structure
```
streaming-platform/
├── config/
│   ├── platform.yaml           # Kafka, Spark, sinks config
│   └── domains/
│       └── ecommerce.yaml      # Domain events & aggregations
├── core/
│   ├── config/                 # Config loader + secret resolution
│   ├── schema/                 # Avro schema generator
│   └── secrets/                # Pluggable secrets backends
├── generators/                 # Synthetic data generation
├── sources/                    # Kafka producer
├── spark/                      # Streaming jobs
├── sinks/                      # ClickHouse, PostgreSQL
├── monitoring/                 # Prometheus + Grafana
├── docker/                     # All-in-one Docker Compose
└── tests/                      # 68 unit tests
```

## 🎯 Use Cases

**E-Commerce**
```yaml
event_types:
  - page_view, add_to_cart, purchase
aggregations:
  - events_per_minute by device_type
  - revenue_per_hour by currency
```

**IoT**
```yaml
event_types:
  - sensor_reading, alert, device_status
aggregations:
  - avg_temperature per 5 minutes
  - anomaly_count by device_id
```

**Fintech**
```yaml
event_types:
  - transaction, login, fraud_alert
aggregations:
  - transaction_volume per minute
  - unique_users per hour
```

## 📊 Adding a New Domain

No code changes needed! Just create a YAML config:
```yaml
# config/domains/gaming.yaml
domain: gaming
entity:
  primary_key: player_id

event_types:
  - name: player_action
    attributes:
      - action_type
      - game_level
    dimensions:
      - platform
      - region

aggregations:
  - name: actions_per_minute
    type: count
    window:
      type: tumbling
      duration: 1 minute
    group_by:
      - platform
```

Then run:
```python
config = loader.load(domain="gaming")
```

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| Message Broker | Apache Kafka 7.5 |
| Schema Registry | Confluent Schema Registry |
| Stream Processing | Spark Structured Streaming 3.5 |
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
| Spark Batch Processing | 100K events/sec |
| ClickHouse Ingestion | 500K rows/sec |

## 🧪 Testing
```bash
# Run all tests
pytest tests/unit/ -v

# With coverage
pytest tests/unit/ --cov=core --cov=generators --cov=sources --cov=spark --cov=sinks

# Lint
ruff check .
black --check .
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

**Adnan** - Data Engineering

- Building production-grade streaming systems
- Available for freelance projects
- [LinkedIn]([https://linkedin.com/in/yourprofile](https://www.linkedin.com/in/adnan21/)) | [Email](mohammedadnansohai11@gmail.com)

---

<p align="center">
  <b>⭐ Star this repo if you find it useful!</b>
</p>
