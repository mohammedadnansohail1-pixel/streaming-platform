"""Dead Letter Queue handler for failed events."""

import json
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional
from pathlib import Path

from confluent_kafka import Producer

from core.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class DLQEvent:
    """Represents a failed event in the Dead Letter Queue."""

    original_topic: str
    original_event: dict
    error_message: str
    error_type: str
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))
    retry_count: int = 0
    max_retries: int = 3
    event_id: Optional[str] = None

    def __post_init__(self):
        if self.event_id is None:
            self.event_id = self.original_event.get("event_id", f"dlq-{self.timestamp}")

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return asdict(self)

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), default=str)

    @classmethod
    def from_dict(cls, data: dict) -> "DLQEvent":
        """Create from dictionary."""
        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str) -> "DLQEvent":
        """Create from JSON string."""
        return cls.from_dict(json.loads(json_str))

    def can_retry(self) -> bool:
        """Check if event can be retried."""
        return self.retry_count < self.max_retries

    def increment_retry(self) -> "DLQEvent":
        """Return new event with incremented retry count."""
        return DLQEvent(
            original_topic=self.original_topic,
            original_event=self.original_event,
            error_message=self.error_message,
            error_type=self.error_type,
            timestamp=self.timestamp,
            retry_count=self.retry_count + 1,
            max_retries=self.max_retries,
            event_id=self.event_id,
        )


class DeadLetterQueue:
    """
    Dead Letter Queue for handling failed events.

    Supports multiple backends:
    - kafka: Write to a Kafka DLQ topic
    - file: Write to local JSON files

    Usage:
        dlq = DeadLetterQueue(config, backend="kafka")

        try:
            process_event(event)
        except Exception as e:
            dlq.send(
                original_topic="events.page_view",
                original_event=event,
                error=e
            )
    """

    def __init__(
        self,
        config: dict,
        backend: str = "kafka",
        dlq_topic: str = "events.dlq",
        file_path: str = "./dlq_events",
    ):
        self.config = config
        self.backend = backend
        self.dlq_topic = dlq_topic
        self.file_path = Path(file_path)

        self._producer: Optional[Producer] = None
        self._stats = {"sent": 0, "errors": 0, "retried": 0}

        if backend == "kafka":
            self._init_kafka()
        elif backend == "file":
            self._init_file()
        else:
            raise ValueError(f"Unknown DLQ backend: {backend}")

    def _init_kafka(self) -> None:
        """Initialize Kafka producer for DLQ."""
        kafka_config = self.config.get("kafka", {})
        self._producer = Producer(
            {
                "bootstrap.servers": kafka_config.get(
                    "bootstrap_servers", "localhost:9092"
                ),
                "client.id": "dlq-producer",
            }
        )
        logger.info(f"DLQ initialized with Kafka backend, topic: {self.dlq_topic}")

    def _init_file(self) -> None:
        """Initialize file-based DLQ."""
        self.file_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"DLQ initialized with file backend, path: {self.file_path}")

    def send(
        self,
        original_topic: str,
        original_event: dict,
        error: Exception,
        retry_count: int = 0,
    ) -> DLQEvent:
        """
        Send a failed event to the DLQ.

        Args:
            original_topic: The topic the event was originally destined for
            original_event: The original event data
            error: The exception that caused the failure
            retry_count: Number of times this event has been retried

        Returns:
            DLQEvent object
        """
        dlq_event = DLQEvent(
            original_topic=original_topic,
            original_event=original_event,
            error_message=str(error),
            error_type=type(error).__name__,
            retry_count=retry_count,
        )

        try:
            if self.backend == "kafka":
                self._send_kafka(dlq_event)
            else:
                self._send_file(dlq_event)

            self._stats["sent"] += 1
            logger.warning(
                f"Event sent to DLQ: {dlq_event.event_id} "
                f"(topic={original_topic}, error={type(error).__name__})"
            )

        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"Failed to send to DLQ: {e}")
            raise

        return dlq_event

    def _send_kafka(self, dlq_event: DLQEvent) -> None:
        """Send event to Kafka DLQ topic."""
        self._producer.produce(
            topic=self.dlq_topic,
            key=dlq_event.event_id.encode("utf-8"),
            value=dlq_event.to_json().encode("utf-8"),
        )
        self._producer.flush()

    def _send_file(self, dlq_event: DLQEvent) -> None:
        """Write event to file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{dlq_event.event_id}_{timestamp}.json"
        filepath = self.file_path / filename

        with open(filepath, "w") as f:
            json.dump(dlq_event.to_dict(), f, indent=2, default=str)

    def get_stats(self) -> dict:
        """Get DLQ statistics."""
        return self._stats.copy()

    def flush(self) -> None:
        """Flush any pending messages."""
        if self._producer:
            self._producer.flush()

    def close(self) -> None:
        """Close the DLQ handler."""
        self.flush()
        logger.info(f"DLQ closed. Stats: {self._stats}")


class DLQConsumer:
    """
    Consumer for processing DLQ events.

    Usage:
        consumer = DLQConsumer(config)

        for event in consumer.consume():
            if event.can_retry():
                # Retry processing
                reprocess(event)
            else:
                # Move to permanent failure storage
                archive(event)
    """

    def __init__(
        self,
        config: dict,
        dlq_topic: str = "events.dlq",
        group_id: str = "dlq-processor",
    ):
        from confluent_kafka import Consumer

        self.config = config
        self.dlq_topic = dlq_topic

        kafka_config = config.get("kafka", {})
        self._consumer = Consumer(
            {
                "bootstrap.servers": kafka_config.get(
                    "bootstrap_servers", "localhost:9092"
                ),
                "group.id": group_id,
                "auto.offset.reset": "earliest",
            }
        )
        self._consumer.subscribe([dlq_topic])
        self._running = False

    def consume(self, timeout: float = 1.0, max_messages: Optional[int] = None):
        """
        Consume DLQ events.

        Args:
            timeout: Poll timeout in seconds
            max_messages: Maximum messages to consume (None for infinite)

        Yields:
            DLQEvent objects
        """
        from confluent_kafka import KafkaError

        self._running = True
        count = 0

        while self._running:
            if max_messages and count >= max_messages:
                break

            msg = self._consumer.poll(timeout)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                event = DLQEvent.from_json(msg.value().decode("utf-8"))
                yield event
                count += 1
            except Exception as e:
                logger.error(f"Failed to parse DLQ event: {e}")

    def stop(self) -> None:
        """Stop consuming."""
        self._running = False

    def close(self) -> None:
        """Close the consumer."""
        self._consumer.close()
