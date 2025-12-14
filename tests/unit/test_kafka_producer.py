"""Tests for Kafka producer."""

import pytest
from unittest.mock import Mock, patch, MagicMock

from sources.kafka_producer import KafkaEventProducer


@pytest.fixture
def sample_config():
    """Sample platform config."""
    return {
        "kafka": {
            "bootstrap_servers": "localhost:9092",
            "topics": {"prefix": "events"},
            "schema_registry": {"url": "http://localhost:8081"},
            "producer": {"acks": "all", "retries": 3, "compression": "lz4"},
        }
    }


@pytest.fixture
def sample_schemas():
    """Sample Avro schemas."""
    return {
        "page_view": {
            "type": "record",
            "name": "page_view",
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "timestamp", "type": "long"},
                {"name": "user_id", "type": "string"},
                {"name": "page_url", "type": ["null", "string"]},
            ],
        }
    }


class TestKafkaEventProducerInit:
    """Tests for producer initialization."""

    def test_init_loads_config(self, sample_config, sample_schemas):
        """Should load config values."""
        producer = KafkaEventProducer(sample_config, sample_schemas)

        assert producer.bootstrap_servers == "localhost:9092"
        assert producer.topic_prefix == "events"
        assert producer.schema_registry_url == "http://localhost:8081"

    def test_init_default_values(self, sample_schemas):
        """Should use defaults for missing config."""
        producer = KafkaEventProducer({}, sample_schemas)

        assert producer.bootstrap_servers == "localhost:9092"
        assert producer.topic_prefix == "events"

    def test_lazy_initialization(self, sample_config, sample_schemas):
        """Producer should not connect until first use."""
        producer = KafkaEventProducer(sample_config, sample_schemas)

        assert producer._producer is None
        assert producer._schema_registry is None


class TestKafkaEventProducerTopics:
    """Tests for topic naming."""

    def test_get_topic(self, sample_config, sample_schemas):
        """Should create correct topic name."""
        producer = KafkaEventProducer(sample_config, sample_schemas)

        topic = producer._get_topic("page_view")

        assert topic == "events.page_view"

    def test_get_topic_custom_prefix(self, sample_schemas):
        """Should use custom topic prefix."""
        config = {"kafka": {"topics": {"prefix": "myapp"}}}
        producer = KafkaEventProducer(config, sample_schemas)

        topic = producer._get_topic("page_view")

        assert topic == "myapp.page_view"


class TestKafkaEventProducerSerialization:
    """Tests for serialization."""

    def test_unknown_event_type_raises_error(self, sample_config, sample_schemas):
        """Should raise error for unknown event type."""
        producer = KafkaEventProducer(sample_config, sample_schemas)

        with pytest.raises(ValueError) as exc_info:
            producer._get_serializer("unknown_event")

        assert "Unknown event type" in str(exc_info.value)


class TestKafkaEventProducerStats:
    """Tests for statistics."""

    def test_initial_stats(self, sample_config, sample_schemas):
        """Should start with zero counts."""
        producer = KafkaEventProducer(sample_config, sample_schemas)

        stats = producer.get_stats()

        assert stats["sent"] == 0
        assert stats["errors"] == 0
        assert stats["connected"] is False

    def test_delivery_callback_success(self, sample_config, sample_schemas):
        """Should increment sent count on success."""
        producer = KafkaEventProducer(sample_config, sample_schemas)
        mock_msg = Mock()
        mock_msg.topic.return_value = "events.page_view"
        mock_msg.partition.return_value = 0

        producer._delivery_callback(None, mock_msg)

        assert producer._sent_count == 1
        assert producer._error_count == 0

    def test_delivery_callback_error(self, sample_config, sample_schemas):
        """Should increment error count on failure."""
        producer = KafkaEventProducer(sample_config, sample_schemas)
        mock_error = Mock()

        producer._delivery_callback(mock_error, None)

        assert producer._sent_count == 0
        assert producer._error_count == 1


class TestKafkaEventProducerSend:
    """Tests for send functionality (mocked)."""

    @patch("sources.kafka_producer.Producer")
    @patch("sources.kafka_producer.SchemaRegistryClient")
    @patch("sources.kafka_producer.AvroSerializer")
    def test_send_creates_producer(
        self, mock_avro, mock_sr, mock_producer, sample_config, sample_schemas
    ):
        """Should create producer on first send."""
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_sr_instance = MagicMock()
        mock_sr.return_value = mock_sr_instance

        producer = KafkaEventProducer(sample_config, sample_schemas)
        event = {
            "event_id": "123",
            "timestamp": 1000,
            "user_id": "user_1",
            "page_url": "/home",
        }

        producer.send("page_view", event)

        mock_producer.assert_called_once()
        mock_producer_instance.produce.assert_called_once()

    @patch("sources.kafka_producer.Producer")
    @patch("sources.kafka_producer.SchemaRegistryClient")
    @patch("sources.kafka_producer.AvroSerializer")
    def test_send_uses_user_id_as_key(
        self, mock_avro, mock_sr, mock_producer, sample_config, sample_schemas
    ):
        """Should use user_id as partition key by default."""
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance

        producer = KafkaEventProducer(sample_config, sample_schemas)
        event = {
            "event_id": "123",
            "timestamp": 1000,
            "user_id": "user_42",
            "page_url": "/home",
        }

        producer.send("page_view", event)

        call_kwargs = mock_producer_instance.produce.call_args[1]
        # Key should be serialized user_id
        assert call_kwargs["key"] == b"user_42"

    @patch("sources.kafka_producer.Producer")
    @patch("sources.kafka_producer.SchemaRegistryClient")
    @patch("sources.kafka_producer.AvroSerializer")
    def test_send_batch(
        self, mock_avro, mock_sr, mock_producer, sample_config, sample_schemas
    ):
        """Should send multiple events."""
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance

        producer = KafkaEventProducer(sample_config, sample_schemas)
        events = [
            {"event_id": "1", "timestamp": 1000, "user_id": "user_1", "page_url": "/"},
            {
                "event_id": "2",
                "timestamp": 1001,
                "user_id": "user_2",
                "page_url": "/products",
            },
        ]

        count = producer.send_batch("page_view", events)

        assert count == 2
        assert mock_producer_instance.produce.call_count == 2


class TestKafkaEventProducerFlush:
    """Tests for flush functionality."""

    @patch("sources.kafka_producer.Producer")
    def test_flush_returns_remaining(
        self, mock_producer, sample_config, sample_schemas
    ):
        """Should return remaining message count."""
        mock_producer_instance = MagicMock()
        mock_producer_instance.flush.return_value = 0
        mock_producer.return_value = mock_producer_instance

        producer = KafkaEventProducer(sample_config, sample_schemas)
        producer._producer = mock_producer_instance

        remaining = producer.flush()

        assert remaining == 0
        mock_producer_instance.flush.assert_called_once_with(10.0)
