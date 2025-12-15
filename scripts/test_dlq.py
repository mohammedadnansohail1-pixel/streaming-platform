"""Test Dead Letter Queue functionality."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(project_root / ".env")

from core.config.loader import ConfigLoader  # noqa: E402
from dlq import DeadLetterQueue, DLQEvent  # noqa: E402


def main():
    print("=" * 60)
    print("Dead Letter Queue Test")
    print("=" * 60)

    loader = ConfigLoader()
    config = loader.load(domain="ecommerce")

    # Test 1: File-based DLQ
    print("\n[Test 1] File-based DLQ")
    print("-" * 40)

    dlq_file = DeadLetterQueue(config, backend="file", file_path="./test_dlq")

    # Simulate failed event
    failed_event = {
        "event_id": "test-123",
        "timestamp": 1234567890,
        "user_id": "user_001",
        "page_url": "/products",
    }

    try:
        raise ValueError("Simulated processing error")
    except Exception as e:
        dlq_event = dlq_file.send(
            original_topic="events.page_view", original_event=failed_event, error=e
        )

    print(f"✓ Event sent to file DLQ: {dlq_event.event_id}")
    print(f"  Error: {dlq_event.error_type} - {dlq_event.error_message}")
    print(f"  Can retry: {dlq_event.can_retry()}")
    print(f"  Stats: {dlq_file.get_stats()}")

    dlq_file.close()

    # Test 2: Kafka-based DLQ
    print("\n[Test 2] Kafka-based DLQ")
    print("-" * 40)

    try:
        dlq_kafka = DeadLetterQueue(config, backend="kafka", dlq_topic="events.dlq")

        # Send multiple failed events
        for i in range(3):
            event = {
                "event_id": f"kafka-test-{i}",
                "timestamp": 1234567890 + i,
                "user_id": f"user_{i:03d}",
            }

            try:
                raise ConnectionError(f"Database connection failed (attempt {i})")
            except Exception as e:
                dlq_kafka.send(
                    original_topic="events.purchase",
                    original_event=event,
                    error=e,
                    retry_count=i,
                )

        print("✓ Sent 3 events to Kafka DLQ")
        print(f"  Stats: {dlq_kafka.get_stats()}")

        dlq_kafka.close()

    except Exception as e:
        print(f"✗ Kafka DLQ failed: {e}")
        print("  (Make sure Kafka is running)")

    # Test 3: DLQ Event operations
    print("\n[Test 3] DLQ Event Operations")
    print("-" * 40)

    event = DLQEvent(
        original_topic="test.topic",
        original_event={"id": 1},
        error_message="Test error",
        error_type="TestError",
        retry_count=0,
        max_retries=3,
    )

    print(f"Initial retry count: {event.retry_count}")
    print(f"Can retry: {event.can_retry()}")

    # Simulate retries
    for i in range(4):
        event = event.increment_retry()
        print(
            f"After retry {i+1}: count={event.retry_count}, can_retry={event.can_retry()}"
        )

    # Test serialization
    json_str = event.to_json()
    restored = DLQEvent.from_json(json_str)
    print(f"\n✓ Serialization works: {restored.event_id}")

    # Cleanup
    import shutil

    shutil.rmtree("./test_dlq", ignore_errors=True)

    print("\n" + "=" * 60)
    print("✓ DLQ tests complete!")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
