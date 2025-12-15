"""CDC Consumer for processing Debezium change events."""

import json
from typing import Callable, Optional

from confluent_kafka import Consumer, KafkaError

from core.utils.logging import get_logger

logger = get_logger(__name__)


class CDCConsumer:
    """
    Consume CDC events from Debezium via Kafka.

    Usage:
        consumer = CDCConsumer(config)
        consumer.subscribe(["cdc.public.customers", "cdc.public.orders"])

        for event in consumer.consume():
            print(f"{event['op']}: {event['table']} - {event['data']}")
    """

    # Operation types
    OP_CREATE = "c"
    OP_UPDATE = "u"
    OP_DELETE = "d"
    OP_READ = "r"  # Snapshot read

    def __init__(self, config: dict, group_id: str = "cdc-consumer"):
        """
        Args:
            config: Platform config with kafka settings
            group_id: Consumer group ID
        """
        kafka_config = config.get("kafka", {})
        self.bootstrap_servers = kafka_config.get("bootstrap_servers", "localhost:9092")

        self._consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )

        self._running = False
        self._topics = []

    def subscribe(self, topics: list[str]) -> None:
        """Subscribe to CDC topics."""
        self._topics = topics
        self._consumer.subscribe(topics)
        logger.info(f"Subscribed to CDC topics: {topics}")

    def consume(
        self,
        timeout: float = 1.0,
        max_messages: Optional[int] = None,
        callback: Optional[Callable[[dict], None]] = None,
    ):
        """
        Consume CDC events.

        Args:
            timeout: Poll timeout in seconds
            max_messages: Max messages to consume (None for infinite)
            callback: Optional callback function for each event

        Yields:
            Parsed CDC event dictionaries
        """
        self._running = True
        count = 0

        logger.info("Starting CDC consumer...")

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
                event = self._parse_event(msg)

                if callback:
                    callback(event)

                yield event
                count += 1

            except Exception as e:
                logger.error(f"Error parsing CDC event: {e}")
                continue

        logger.info(f"CDC consumer stopped. Processed {count} events.")

    def _parse_event(self, msg) -> dict:
        """Parse Debezium CDC message into structured event."""
        value_bytes = msg.value()

        # Handle tombstone messages (null value for deleted keys)
        if value_bytes is None:
            topic = msg.topic()
            parts = topic.split(".")
            table = parts[2] if len(parts) > 2 else topic
            return {
                "op": "d",
                "op_name": "TOMBSTONE",
                "schema": parts[1] if len(parts) > 1 else "public",
                "table": table,
                "data": {},
                "before": None,
                "after": None,
                "ts_ms": None,
                "source": {},
                "raw": None,
            }

        value = json.loads(value_bytes.decode("utf-8"))

        # Extract topic info
        topic = msg.topic()
        parts = topic.split(".")
        schema = parts[1] if len(parts) > 1 else "public"
        table = parts[2] if len(parts) > 2 else topic

        # Get operation type
        op = value.get("op", "unknown")
        op_name = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "SNAPSHOT"}.get(
            op, "UNKNOWN"
        )

        # Get data (before/after depending on operation)
        if op == "d":
            data = value.get("before", {})
        else:
            data = value.get("after", {})

        # Source metadata
        source = value.get("source", {})

        return {
            "op": op,
            "op_name": op_name,
            "schema": schema,
            "table": table,
            "data": data,
            "before": value.get("before"),
            "after": value.get("after"),
            "ts_ms": value.get("ts_ms"),
            "source": {
                "db": source.get("db"),
                "table": source.get("table"),
                "txId": source.get("txId"),
                "lsn": source.get("lsn"),
            },
            "raw": value,
        }

    def stop(self) -> None:
        """Stop consuming."""
        self._running = False

    def close(self) -> None:
        """Close consumer."""
        self._consumer.close()
        logger.info("CDC consumer closed")


class CDCProcessor:
    """
    Process CDC events and sync to sinks.

    Usage:
        processor = CDCProcessor(config)
        processor.add_handler("customers", handle_customer_change)
        processor.run()
    """

    def __init__(self, config: dict):
        self.config = config
        self._handlers: dict[str, Callable] = {}
        self._consumer = CDCConsumer(config)

    def add_handler(self, table: str, handler: Callable[[dict], None]) -> None:
        """Register a handler for a specific table."""
        self._handlers[table] = handler
        logger.info(f"Registered handler for table: {table}")

    def run(self, topics: list[str], max_messages: Optional[int] = None) -> None:
        """
        Run the processor.

        Args:
            topics: CDC topics to consume
            max_messages: Max messages to process (None for infinite)
        """
        self._consumer.subscribe(topics)

        try:
            for event in self._consumer.consume(max_messages=max_messages):
                table = event["table"]

                if table in self._handlers:
                    self._handlers[table](event)
                else:
                    logger.debug(f"No handler for table: {table}")

        except KeyboardInterrupt:
            logger.info("Interrupted")
        finally:
            self._consumer.close()
