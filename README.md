# Streaming Platform

A production-grade, config-driven streaming data platform.

## Overview

Generic streaming platform that processes real-time events from any domain (e-commerce, IoT, fintech, APM) through a unified pipeline.

## Architecture
```
Data Sources → Kafka → Spark Streaming → ClickHouse → Grafana
```

## Features

- **Config-driven**: Change domains without code changes
- **Pluggable secrets**: Support for env, Vault, AWS, Azure, GCP
- **Schema evolution**: Avro + Schema Registry
- **Error handling**: Dead Letter Queue for bad records
- **Observability**: Prometheus metrics, Grafana dashboards

## Tech Stack

- **Message Broker**: Apache Kafka
- **Stream Processing**: Spark Structured Streaming
- **Analytics Store**: ClickHouse
- **Schema Management**: Confluent Schema Registry
- **Monitoring**: Prometheus + Grafana
- **Infrastructure**: Docker, MinIO (S3-compatible)

## Status

🚧 Under Development

## Author

Adnan - Data Engineering