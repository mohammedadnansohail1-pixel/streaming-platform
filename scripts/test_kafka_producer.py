"""Test Kafka producer with live Kafka."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(project_root / ".env")

from core.config.loader import ConfigLoader  # noqa: E402
from core.schema.generator import SchemaGenerator  # noqa: E402
from core.utils.logging import setup_logging  # noqa: E402
from generators.synthetic import SyntheticDataGenerator  # noqa: E402
from sources.kafka_producer import KafkaEventProducer  # noqa: E402


def main():
    setup_logging(level="INFO")

    print("=" * 60)
    print("Loading config and generating schemas")
    print("=" * 60)

    loader = ConfigLoader()
    config = loader.load(domain="ecommerce")

    generator = SchemaGenerator()
    schemas = generator.generate_all_schemas(config)
    print(f"✓ Generated {len(schemas)} schemas")

    print("\n" + "=" * 60)
    print("Creating Kafka producer")
    print("=" * 60)

    producer = KafkaEventProducer(config, schemas)

    # Health check
    if not producer.health_check():
        print("✗ Kafka not reachable. Is it running?")
        return 1

    print("✓ Connected to Kafka")

    print("\n" + "=" * 60)
    print("Generating and sending events")
    print("=" * 60)

    data_gen = SyntheticDataGenerator(config, schemas)

    # Send 10 page_view events
    print("\nSending 10 page_view events...")
    for event in data_gen.generate_events("page_view", count=10):
        producer.send("page_view", event)

    # Send 5 purchase events
    print("Sending 5 purchase events...")
    for event in data_gen.generate_events("purchase", count=5):
        producer.send("purchase", event)

    # Flush and get stats
    producer.flush()
    stats = producer.get_stats()

    print("\n" + "=" * 60)
    print("Results")
    print("=" * 60)
    print(f"✓ Sent: {stats['sent']}")
    print(f"✗ Errors: {stats['errors']}")

    producer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
