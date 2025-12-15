"""Unit tests for Dead Letter Queue module."""

import json
import pytest
from unittest.mock import MagicMock, patch


class TestDLQEvent:
    """Tests for DLQEvent class."""

    def test_create_event(self):
        """Should create DLQ event with required fields."""
        from dlq import DLQEvent

        event = DLQEvent(
            original_topic="test.topic",
            original_event={"id": 1, "data": "test"},
            error_message="Test error",
            error_type="ValueError",
        )

        assert event.original_topic == "test.topic"
        assert event.original_event == {"id": 1, "data": "test"}
        assert event.error_message == "Test error"
        assert event.error_type == "ValueError"
        assert event.retry_count == 0
        assert event.max_retries == 3

    def test_event_id_from_original(self):
        """Should use event_id from original event if present."""
        from dlq import DLQEvent

        event = DLQEvent(
            original_topic="test.topic",
            original_event={"event_id": "original-123", "data": "test"},
            error_message="Error",
            error_type="Error",
        )

        assert event.event_id == "original-123"

    def test_can_retry_true(self):
        """Should return True when retries remaining."""
        from dlq import DLQEvent

        event = DLQEvent(
            original_topic="test.topic",
            original_event={},
            error_message="Error",
            error_type="Error",
            retry_count=2,
            max_retries=3,
        )

        assert event.can_retry() is True

    def test_can_retry_false(self):
        """Should return False when max retries reached."""
        from dlq import DLQEvent

        event = DLQEvent(
            original_topic="test.topic",
            original_event={},
            error_message="Error",
            error_type="Error",
            retry_count=3,
            max_retries=3,
        )

        assert event.can_retry() is False

    def test_increment_retry(self):
        """Should increment retry count."""
        from dlq import DLQEvent

        event = DLQEvent(
            original_topic="test.topic",
            original_event={},
            error_message="Error",
            error_type="Error",
            retry_count=1,
        )

        new_event = event.increment_retry()

        assert new_event.retry_count == 2
        assert event.retry_count == 1  # Original unchanged

    def test_to_dict(self):
        """Should convert to dictionary."""
        from dlq import DLQEvent

        event = DLQEvent(
            original_topic="test.topic",
            original_event={"id": 1},
            error_message="Error",
            error_type="Error",
        )

        data = event.to_dict()

        assert data["original_topic"] == "test.topic"
        assert data["original_event"] == {"id": 1}
        assert "timestamp" in data

    def test_to_json_and_back(self):
        """Should serialize and deserialize correctly."""
        from dlq import DLQEvent

        event = DLQEvent(
            original_topic="test.topic",
            original_event={"id": 1, "name": "test"},
            error_message="Connection failed",
            error_type="ConnectionError",
            retry_count=2,
        )

        json_str = event.to_json()
        restored = DLQEvent.from_json(json_str)

        assert restored.original_topic == event.original_topic
        assert restored.original_event == event.original_event
        assert restored.error_message == event.error_message
        assert restored.retry_count == event.retry_count


class TestDeadLetterQueue:
    """Tests for DeadLetterQueue class."""

    def test_init_kafka_backend(self):
        """Should initialize with Kafka backend."""
        from dlq import DeadLetterQueue

        config = {"kafka": {"bootstrap_servers": "localhost:9092"}}

        with patch("dlq.handler.Producer"):
            dlq = DeadLetterQueue(config, backend="kafka")
            assert dlq.backend == "kafka"
            assert dlq.dlq_topic == "events.dlq"

    def test_init_file_backend(self, tmp_path):
        """Should initialize with file backend."""
        from dlq import DeadLetterQueue

        config = {}
        dlq = DeadLetterQueue(config, backend="file", file_path=str(tmp_path / "dlq"))

        assert dlq.backend == "file"
        assert dlq.file_path.exists()

    def test_init_invalid_backend(self):
        """Should raise error for invalid backend."""
        from dlq import DeadLetterQueue

        config = {}

        with pytest.raises(ValueError, match="Unknown DLQ backend"):
            DeadLetterQueue(config, backend="invalid")

    def test_send_to_file(self, tmp_path):
        """Should write event to file."""
        from dlq import DeadLetterQueue

        config = {}
        dlq_path = tmp_path / "dlq"
        dlq = DeadLetterQueue(config, backend="file", file_path=str(dlq_path))

        event = {"event_id": "test-1", "data": "test"}
        error = ValueError("Test error")

        dlq.send(original_topic="test.topic", original_event=event, error=error)

        # Check file was created
        files = list(dlq_path.glob("*.json"))
        assert len(files) == 1

        # Check content
        with open(files[0]) as f:
            content = json.load(f)
            assert content["original_topic"] == "test.topic"
            assert content["error_type"] == "ValueError"

        assert dlq.get_stats()["sent"] == 1

    def test_send_to_kafka(self):
        """Should send event to Kafka topic."""
        from dlq import DeadLetterQueue

        config = {"kafka": {"bootstrap_servers": "localhost:9092"}}

        with patch("dlq.handler.Producer") as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer

            dlq = DeadLetterQueue(config, backend="kafka", dlq_topic="test.dlq")

            event = {"event_id": "test-1"}
            error = Exception("Test error")

            dlq.send(original_topic="test.topic", original_event=event, error=error)

            mock_producer.produce.assert_called_once()
            call_args = mock_producer.produce.call_args
            assert call_args.kwargs["topic"] == "test.dlq"

    def test_get_stats(self, tmp_path):
        """Should return statistics."""
        from dlq import DeadLetterQueue

        config = {}
        dlq = DeadLetterQueue(config, backend="file", file_path=str(tmp_path / "dlq"))

        # Send some events
        for i in range(3):
            dlq.send(
                original_topic="test.topic",
                original_event={"id": i},
                error=Exception(f"Error {i}"),
            )

        stats = dlq.get_stats()

        assert stats["sent"] == 3
        assert stats["errors"] == 0


class TestDLQConsumer:
    """Tests for DLQConsumer class."""

    def test_init_consumer(self):
        """Should initialize consumer."""
        from dlq.handler import DLQConsumer

        config = {"kafka": {"bootstrap_servers": "localhost:9092"}}

        with patch("confluent_kafka.Consumer") as mock_consumer_class:
            mock_consumer = MagicMock()
            mock_consumer_class.return_value = mock_consumer

            DLQConsumer(config, dlq_topic="test.dlq", group_id="test-group")

            mock_consumer.subscribe.assert_called_once_with(["test.dlq"])

    def test_stop_consumer(self):
        """Should stop consuming."""
        from dlq.handler import DLQConsumer

        config = {"kafka": {"bootstrap_servers": "localhost:9092"}}

        with patch("confluent_kafka.Consumer"):
            consumer = DLQConsumer(config)
            consumer._running = True

            consumer.stop()

            assert consumer._running is False
