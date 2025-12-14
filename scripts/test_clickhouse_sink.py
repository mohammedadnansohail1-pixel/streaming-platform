"""Test Spark streaming with ClickHouse sink."""

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
from spark.sinks import ClickHouseSink  # noqa: E402


def send_test_events(config, schemas, count=30, delay=0.5):
    """Send test events in background."""
    print(f"\n[Producer] Sending {count} events...")

    producer = KafkaEventProducer(config, schemas)
    data_gen = SyntheticDataGenerator(config, schemas)

    for i in range(count):
        event = data_gen.generate_event("page_view")
        producer.send("page_view", event)
        if (i + 1) % 10 == 0:
            print(f"[Producer] Sent {i+1}/{count}")
        time.sleep(delay)

    producer.flush()
    producer.close()
    print("[Producer] Done")


def main():
    setup_logging(level="WARN")

    print("=" * 60)
    print("Spark Streaming → ClickHouse Test")
    print("=" * 60)

    # Load config and schemas
    loader = ConfigLoader()
    config = loader.load(domain="ecommerce")

    generator = SchemaGenerator()
    schemas = generator.generate_all_schemas(config)
    print(f"✓ Loaded config and {len(schemas)} schemas")

    # Setup ClickHouse
    print("\n" + "=" * 60)
    print("Setting up ClickHouse")
    print("=" * 60)

    sink = ClickHouseSink(config)
    if not sink.health_check():
        print("✗ ClickHouse not reachable")
        return 1

    sink.create_aggregation_tables()
    print("✓ Tables created")

    # Start producer thread
    producer_thread = threading.Thread(
        target=send_test_events, args=(config, schemas, 30, 0.5)
    )

    # Create streaming job
    print("\n" + "=" * 60)
    print("Starting Spark Streaming → ClickHouse")
    print("=" * 60)

    job = StreamingJob(config, schemas)

    try:
        # Build pipeline
        raw_df = job.read_stream(["page_view"])
        parsed_df = job.parse_events(raw_df, "page_view")

        agg_config = next(
            a for a in config["aggregations"] if a["name"] == "events_per_minute"
        )
        agg_df = job.aggregate_events(parsed_df, agg_config)

        # Write to both console and ClickHouse
        job.write_console(agg_df, "page_view_events_per_minute")
        job.write_clickhouse(agg_df, "page_view_events_per_minute", sink)

        # Start sending events
        producer_thread.start()

        # Wait
        job.await_termination(timeout=30)

    except KeyboardInterrupt:
        print("\n\nStopping...")
    finally:
        job.stop()
        if producer_thread.is_alive():
            producer_thread.join(timeout=5)

    # Query results
    print("\n" + "=" * 60)
    print("ClickHouse Results")
    print("=" * 60)

    result = sink.query(
        "SELECT * FROM events_per_minute ORDER BY window_start, device_type"
    )
    print(f"\nRows in events_per_minute: {result.row_count}")
    for row in result.result_rows:
        print(f"  {row}")

    sink.close()
    print("\n✓ Test complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
