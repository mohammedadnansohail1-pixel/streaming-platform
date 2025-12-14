"""Kafka producer with Avro serialization."""

import json
from typing import Optional

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from core.utils.logging import get_logger
from core.utils.decorators import retry

logger = get_logger(__name__)


class KafkaEventProducer:
    """
    Produces events to Kafka with Avro serialization.

    Usage:
        producer = KafkaEventProducer(config, schemas)
        producer.send("page_view", event)
        producer.flush()
    """

    def __init__(self, config: dict, schemas: dict):
        """
        Args:
            config: Platform config with kafka settings
            schemas: Dict of event_name → Avro schema
        """
        self.config = config
        self.schemas = schemas

        kafka_config = config.get("kafka", {})
        self.bootstrap_servers = kafka_config.get("bootstrap_servers", "localhost:9092")
        self.topic_prefix = kafka_config.get("topics", {}).get("prefix", "events")

        # Schema Registry
        schema_registry_config = kafka_config.get("schema_registry", {})
        self.schema_registry_url = schema_registry_config.get(
            "url", "http://localhost:8081"
        )

        # Producer settings
        producer_config = kafka_config.get("producer", {})
        self.acks = producer_config.get("acks", "all")
        self.retries = producer_config.get("retries", 3)
        self.compression = producer_config.get("compression", "lz4")

        # Initialize clients
        self._producer: Optional[Producer] = None
        self._schema_registry: Optional[SchemaRegistryClient] = None
        self._serializers: dict[str, AvroSerializer] = {}
        self._string_serializer = StringSerializer("utf-8")

        # Stats
        self._sent_count = 0
        self._error_count = 0

    def _get_producer(self) -> Producer:
        """Lazy initialization of Kafka producer."""
        if self._producer is None:
            logger.info(f"Connecting to Kafka at {self.bootstrap_servers}")
            self._producer = Producer(
                {
                    "bootstrap.servers": self.bootstrap_servers,
                    "acks": self.acks,
                    "retries": self.retries,
                    "compression.type": self.compression,
                    "linger.ms": 5,  # Batch for 5ms
                    "batch.size": 16384,  # 16KB batches
                }
            )
        return self._producer

    def _get_schema_registry(self) -> SchemaRegistryClient:
        """Lazy initialization of Schema Registry client."""
        if self._schema_registry is None:
            logger.info(f"Connecting to Schema Registry at {self.schema_registry_url}")
            self._schema_registry = SchemaRegistryClient(
                {"url": self.schema_registry_url}
            )
        return self._schema_registry

    def _get_serializer(self, event_type: str) -> AvroSerializer:
        """Get or create Avro serializer for event type."""
        if event_type not in self._serializers:
            if event_type not in self.schemas:
                raise ValueError(f"Unknown event type: {event_type}")

            schema_str = json.dumps(self.schemas[event_type])
            self._serializers[event_type] = AvroSerializer(
                self._get_schema_registry(), schema_str
            )
            logger.debug(f"Created serializer for {event_type}")

        return self._serializers[event_type]

    def _delivery_callback(self, err, msg):
        """Callback for delivery reports."""
        if err is not None:
            logger.error(f"Delivery failed: {err}")
            self._error_count += 1
        else:
            logger.debug(f"Delivered to {msg.topic()}[{msg.partition()}]")
            self._sent_count += 1

    def _get_topic(self, event_type: str) -> str:
        """Get topic name for event type."""
        return f"{self.topic_prefix}.{event_type}"

    @retry(max_attempts=3, delay=1.0, exceptions=(Exception,))
    def send(self, event_type: str, event: dict, key: str = None) -> None:
        """
        Send an event to Kafka.

        Args:
            event_type: Type of event (e.g., "page_view")
            event: Event data dict
            key: Optional partition key (defaults to user_id)
        """
        producer = self._get_producer()
        serializer = self._get_serializer(event_type)
        topic = self._get_topic(event_type)

        # Use user_id as default key for partitioning
        if key is None:
            key = event.get("user_id", "")

        # Serialize and send
        producer.produce(
            topic=topic,
            key=self._string_serializer(key),
            value=serializer(event, SerializationContext(topic, MessageField.VALUE)),
            on_delivery=self._delivery_callback,
        )

        # Trigger delivery callbacks
        producer.poll(0)

    def send_batch(self, event_type: str, events: list[dict]) -> int:
        """
        Send multiple events efficiently.

        Args:
            event_type: Type of events
            events: List of event dicts

        Returns:
            Number of events queued
        """
        logger.info(f"Sending batch of {len(events)} {event_type} events")

        for event in events:
            self.send(event_type, event)

        return len(events)

    def flush(self, timeout: float = 10.0) -> int:
        """
        Wait for all messages to be delivered.

        Args:
            timeout: Max seconds to wait

        Returns:
            Number of messages still in queue (0 = all delivered)
        """
        if self._producer is None:
            return 0

        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning(f"{remaining} messages still in queue after flush")
        else:
            logger.info(
                f"All messages delivered. Sent: {self._sent_count}, Errors: {self._error_count}"
            )

        return remaining

    def health_check(self) -> bool:
        """Check if Kafka is reachable."""
        try:
            producer = self._get_producer()
            # List topics to verify connection
            metadata = producer.list_topics(timeout=5)
            logger.info(f"Connected to Kafka. Brokers: {len(metadata.brokers)}")
            return True
        except Exception as e:
            logger.error(f"Kafka health check failed: {e}")
            return False

    def get_stats(self) -> dict:
        """Get producer statistics."""
        return {
            "sent": self._sent_count,
            "errors": self._error_count,
            "connected": self._producer is not None,
        }

    def close(self) -> None:
        """Close producer connection."""
        if self._producer is not None:
            self.flush()
            logger.info("Kafka producer closed")
            self._producer = None
