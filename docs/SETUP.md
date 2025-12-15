# Setup Guide

Complete guide to setting up the streaming platform from scratch.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [System Requirements](#system-requirements)
3. [Infrastructure Setup](#infrastructure-setup)
4. [Project Setup](#project-setup)
5. [Configuration](#configuration)
6. [Verification](#verification)
7. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Software

| Software | Version | Purpose |
|----------|---------|---------|
| Python | 3.12+ | Core runtime |
| Java | 21 (OpenJDK) | Spark runtime |
| Docker | 24+ | Infrastructure containers |
| Docker Compose | 2.0+ | Multi-container orchestration |
| Git | 2.40+ | Version control |

### Verify Installation
```bash
python --version      # Python 3.12.x
java -version         # openjdk 21.x.x
docker --version      # Docker version 24.x.x
docker compose version # Docker Compose version v2.x.x
git --version         # git version 2.4x.x
```

---

## System Requirements

### Minimum (Development)

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 4 cores | 8+ cores |
| RAM | 8 GB | 16+ GB |
| Disk | 20 GB | 50+ GB SSD |

### Resource Allocation
```
Kafka + Zookeeper:  2 GB RAM
Schema Registry:    512 MB RAM
Spark:              4 GB RAM
ClickHouse:         2 GB RAM
PostgreSQL:         512 MB RAM
Prometheus/Grafana: 512 MB RAM
─────────────────────────────
Total:              ~10 GB RAM
```

### WSL2 Users (Windows)

Create `C:\Users\<username>\.wslconfig`:
```ini
[wsl2]
memory=8GB
processors=4
swap=4GB
```

Apply with `wsl --shutdown` and restart.

---

## Infrastructure Setup

### 1. Kafka Cluster

Create `docker-compose.yml`:
```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    depends_on:
      - kafka
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
```

Start:
```bash
docker compose up -d
```

Verify:
```bash
# Kafka
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Schema Registry
curl http://localhost:8081/subjects
```

### 2. ClickHouse

Create `docker-compose.yml`:
```yaml
version: '3.8'
services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    container_name: clickhouse
    ports:
      - "8123:8123"
      - "9000:9000"
    environment:
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD: your_password_here
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
    volumes:
      - clickhouse_data:/var/lib/clickhouse

volumes:
  clickhouse_data:
```

Start:
```bash
docker compose up -d
```

Verify:
```bash
curl "http://localhost:8123/?query=SELECT%201"
```

### 3. PostgreSQL

Create `docker-compose.yml`:
```yaml
version: '3.8'
services:
  postgres:
    image: postgres:16
    container_name: postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_PASSWORD: your_password_here
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

Start:
```bash
docker compose up -d
```

Verify:
```bash
PGPASSWORD='your_password' psql -h localhost -U postgres -c "SELECT 1"
```

**Note**: Stop any local PostgreSQL first:
```bash
sudo systemctl stop postgresql
```

### 4. Prometheus & Grafana

Create `docker-compose.yml`:
```yaml
version: '3.8'
services:
  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
```

Create `prometheus.yml`:
```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'streaming-platform'
    static_configs:
      - targets: ['host.docker.internal:8000']
```

Start:
```bash
docker compose up -d
```

Verify:
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)

---

## Project Setup

### 1. Clone Repository
```bash
git clone https://github.com/your-username/streaming-platform.git
cd streaming-platform
```

### 2. Create Virtual Environment
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# or
.venv\Scripts\activate     # Windows
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Set JAVA_HOME
```bash
# Find Java path
which java
# /usr/bin/java -> /usr/lib/jvm/java-21-openjdk-amd64/bin/java

# Add to ~/.bashrc
echo 'export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc

# Verify
echo $JAVA_HOME
```

### 5. Install Pre-commit Hooks
```bash
pre-commit install
```

---

## Configuration

### 1. Create Environment File
```bash
cp .env.example .env
```

Edit `.env`:
```bash
# Kafka
kafka_bootstrap_servers=localhost:9092
schema_registry_url=http://localhost:8081

# ClickHouse
clickhouse_host=localhost
clickhouse_user=default
clickhouse_password=your_clickhouse_password

# PostgreSQL
postgresql_host=localhost
postgresql_user=postgres
postgresql_password=your_postgres_password

# Checkpoints
checkpoint_path=./checkpoints
```

### 2. Verify Configuration
```bash
python scripts/test_config.py
```

Expected output:
```
✓ Loaded platform config
✓ Loaded domain config: ecommerce
✓ Secrets resolved
✓ Generated 6 schemas
```

### 3. Domain Configuration

Default domain is `ecommerce`. To add a new domain:

1. Create `config/domains/your_domain.yaml`:
```yaml
domain: your_domain
version: "1.0"

entity:
  primary_key: user_id

event_types:
  - name: your_event
    attributes:
      - field1
      - field2
    dimensions:
      - dimension1

aggregations:
  - name: events_per_minute
    type: count
    window:
      type: tumbling
      duration: 1 minute
    group_by:
      - dimension1
```

2. Load with:
```python
config = loader.load(domain="your_domain")
```

---

## Verification

### Step 1: Test Kafka Connection
```bash
python scripts/test_kafka_producer.py
```

Expected:
```
✓ Connected to Kafka
✓ Sent 15 events
✓ Schema registered
```

### Step 2: Test Spark Streaming
```bash
python scripts/test_spark_streaming.py
```

Expected:
```
Batch: 1
+------------------------------------------+-----------+-----+
|window                                    |device_type|count|
+------------------------------------------+-----------+-----+
|{2025-12-14 13:25:00, 2025-12-14 13:26:00}|desktop    |5    |
|{2025-12-14 13:25:00, 2025-12-14 13:26:00}|mobile     |3    |
+------------------------------------------+-----------+-----+
```

### Step 3: Test ClickHouse Sink
```bash
python scripts/test_clickhouse_sink.py
```

Verify data in ClickHouse:
```bash
docker exec clickhouse clickhouse-client \
  --password 'your_password' \
  -q "SELECT * FROM analytics.events_per_minute"
```

### Step 4: Test Monitoring
```bash
python -c "
from monitoring import start_metrics_server, Metrics
start_metrics_server(8000)
Metrics.produce_success('test', latency=0.01)
import time; time.sleep(60)
"
```

In another terminal:
```bash
curl localhost:8000/metrics | grep events_produced
```

### Step 5: Run All Unit Tests
```bash
pytest tests/unit/ -v
```

Expected: `68 passed`

---

## Troubleshooting

### Kafka Issues

**Error**: `NoBrokersAvailable`
```bash
# Check Kafka is running
docker ps | grep kafka

# Check logs
docker logs kafka

# Restart if needed
docker restart kafka
```

**Error**: `Cluster ID mismatch`
```bash
# Remove old data
docker compose down
docker volume rm kafka_kafka_data kafka_zookeeper_data
docker compose up -d
```

### Spark Issues

**Error**: `JavaPackage object is not callable`
```bash
# Check JAVA_HOME
echo $JAVA_HOME

# Should match Spark's expected Java
# Set if missing
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
```

**Error**: `PySpark version mismatch`
```bash
# Check versions
pip show pyspark | grep Version
ls /opt/spark*/

# Install matching version
pip install pyspark==3.5.1
```

### ClickHouse Issues

**Error**: `Authentication failed`
```bash
# Check password in .env matches docker-compose
grep clickhouse_password .env
docker inspect clickhouse | grep PASSWORD

# Reset password
docker exec clickhouse clickhouse-client \
  -q "ALTER USER default IDENTIFIED BY 'new_password';"
```

**Error**: `Database does not exist`
```bash
# Create database
docker exec clickhouse clickhouse-client \
  --password 'your_password' \
  -q "CREATE DATABASE IF NOT EXISTS analytics"
```

### PostgreSQL Issues

**Error**: `Connection refused`
```bash
# Check if local postgres is conflicting
sudo lsof -i :5432

# Stop local postgres
sudo systemctl stop postgresql

# Restart docker postgres
docker restart postgres
```

**Error**: `Password authentication failed`
```bash
# Reset password
docker exec postgres psql -U postgres \
  -c "ALTER USER postgres WITH PASSWORD 'new_password';"

# Update .env
sed -i 's/postgresql_password=.*/postgresql_password=new_password/' .env
```

### WSL/Docker Issues

**Error**: System crash when starting Docker Desktop

Create `C:\Users\<username>\.wslconfig`:
```ini
[wsl2]
memory=8GB
processors=4
swap=4GB
guiApplications=false
```

Apply:
```powershell
wsl --shutdown
```

### Memory Issues

**Error**: `Java heap space` or `OutOfMemoryError`
```bash
# Increase Spark memory in streaming_job.py
.config("spark.driver.memory", "4g")
.config("spark.executor.memory", "4g")
```

---

## Next Steps

After successful setup:

1. **Explore the codebase**: Start with `core/config/loader.py`
2. **Run the demo**: `python scripts/test_clickhouse_sink.py`
3. **Check Grafana**: http://localhost:3000, import dashboard from `monitoring/grafana/dashboards/`
4. **Add your domain**: Create config in `config/domains/`
5. **Read architecture docs**: `docs/ARCHITECTURE.md`
