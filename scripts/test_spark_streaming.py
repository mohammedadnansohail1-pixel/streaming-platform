"""Test Spark Structured Streaming with live Kafka."""

import sys
import time
import threading
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
from spark.streaming_job import StreamingJob  # noqa: E402


def send_test_events(config, schemas, count=20, delay=0.5):
    """Send test events in background."""
    print(f"\n[Producer] Sending {count} events with {delay}s delay...")

    producer = KafkaEventProducer(config, schemas)
    data_gen = SyntheticDataGenerator(config, schemas)

    for i in range(count):
        event = data_gen.generate_event("page_view")
        producer.send("page_view", event)
        print(f"[Producer] Sent event {i+1}/{count}")
        time.sleep(delay)

    producer.flush()
    producer.close()
    print("[Producer] Done sending events")


def main():
    setup_logging(level="WARN")  # Reduce Spark noise

    print("=" * 60)
    print("Spark Structured Streaming Test")
    print("=" * 60)

    # Load config and schemas
    loader = ConfigLoader()
    config = loader.load(domain="ecommerce")

    generator = SchemaGenerator()
    schemas = generator.generate_all_schemas(config)
    print(f"✓ Loaded config and {len(schemas)} schemas")

    # Start producer in background thread
    producer_thread = threading.Thread(
        target=send_test_events, args=(config, schemas, 20, 1.0)
    )

    # Create streaming job
    print("\n" + "=" * 60)
    print("Starting Spark Streaming Job")
    print("=" * 60)

    job = StreamingJob(config, schemas)

    # Start simple count aggregation
    print("\nRunning: events_per_minute aggregation")
    print("(Counting page_view events in 1-minute tumbling windows)")
    print("\nPress Ctrl+C to stop\n")

    try:
        # Run aggregation job
        job.run_aggregation_job("page_view", "events_per_minute")

        # Start sending events
        producer_thread.start()

        # Wait for streaming (or timeout after 60 seconds)
        job.await_termination(timeout=60)

    except KeyboardInterrupt:
        print("\n\nStopping...")
    finally:
        job.stop()
        if producer_thread.is_alive():
            producer_thread.join(timeout=5)

    print("\n✓ Test complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
