"""Integration tests for Kafka producer and consumer."""

import pytest
import time
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(project_root / ".env")


def is_kafka_available():
    """Check if Kafka is available."""
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": "localhost:9092"})
        admin.list_topics(timeout=5)
        return True
    except Exception:
        return False


# Skip all tests in this module if Kafka is not available
pytestmark = pytest.mark.skipif(not is_kafka_available(), reason="Kafka not available")


class TestKafkaProducer:
    """Integration tests for Kafka producer."""

    def test_producer_sends_events(self):
        """Should send events to Kafka."""
        from core.config.loader import ConfigLoader
        from core.schema.generator import SchemaGenerator
        from generators.synthetic import SyntheticDataGenerator
        from sources.kafka_producer import KafkaEventProducer

        loader = ConfigLoader()
        config = loader.load(domain="ecommerce")

        generator = SchemaGenerator()
        schemas = generator.generate_all_schemas(config)

        data_gen = SyntheticDataGenerator(config, schemas)
        producer = KafkaEventProducer(config, schemas)

        # Send 10 events
        for event in data_gen.generate_events("page_view", 10):
            producer.send("page_view", event)

        producer.flush()
        stats = producer.get_stats()
        producer.close()

        assert stats["sent"] == 10
        assert stats["errors"] == 0

    def test_producer_health_check(self):
        """Should pass health check when Kafka is available."""
        from core.config.loader import ConfigLoader
        from core.schema.generator import SchemaGenerator
        from sources.kafka_producer import KafkaEventProducer

        loader = ConfigLoader()
        config = loader.load(domain="ecommerce")

        generator = SchemaGenerator()
        schemas = generator.generate_all_schemas(config)

        producer = KafkaEventProducer(config, schemas)
        assert producer.health_check() is True
        producer.close()

    def test_producer_registers_schema(self):
        """Should register schema with Schema Registry."""
        import httpx
        from core.config.loader import ConfigLoader
        from core.schema.generator import SchemaGenerator
        from generators.synthetic import SyntheticDataGenerator
        from sources.kafka_producer import KafkaEventProducer

        loader = ConfigLoader()
        config = loader.load(domain="ecommerce")

        generator = SchemaGenerator()
        schemas = generator.generate_all_schemas(config)

        data_gen = SyntheticDataGenerator(config, schemas)
        producer = KafkaEventProducer(config, schemas)

        # Send one event to trigger schema registration
        event = next(data_gen.generate_events("page_view", 1))
        producer.send("page_view", event)
        producer.flush()
        producer.close()

        # Verify schema was registered
        resp = httpx.get(f"{config['kafka']['schema_registry']['url']}/subjects")
        subjects = resp.json()

        assert "events.page_view-value" in subjects


class TestKafkaConsumer:
    """Integration tests for Kafka consumer."""

    def test_consumer_receives_events(self):
        """Should receive events from Kafka."""
        from confluent_kafka import Consumer
        from core.config.loader import ConfigLoader
        from core.schema.generator import SchemaGenerator
        from generators.synthetic import SyntheticDataGenerator
        from sources.kafka_producer import KafkaEventProducer

        loader = ConfigLoader()
        config = loader.load(domain="ecommerce")

        generator = SchemaGenerator()
        schemas = generator.generate_all_schemas(config)

        # Send events
        data_gen = SyntheticDataGenerator(config, schemas)
        producer = KafkaEventProducer(config, schemas)

        for event in data_gen.generate_events("page_view", 5):
            producer.send("page_view", event)
        producer.flush()
        producer.close()

        # Consume events
        consumer = Consumer(
            {
                "bootstrap.servers": config["kafka"]["bootstrap_servers"],
                "group.id": "test-consumer-group",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe(["events.page_view"])

        received = 0
        timeout = time.time() + 10  # 10 second timeout

        while received < 5 and time.time() < timeout:
            msg = consumer.poll(1.0)
            if msg and not msg.error():
                received += 1

        consumer.close()
        assert received >= 5
