"""Unit tests for CDC module."""

import json
from unittest.mock import MagicMock, patch


class TestCDCConsumer:
    """Tests for CDCConsumer class."""

    def test_consumer_initialization(self):
        """Should initialize with config."""
        from cdc.consumer import CDCConsumer

        config = {"kafka": {"bootstrap_servers": "localhost:9092"}}

        with patch("cdc.consumer.Consumer"):
            consumer = CDCConsumer(config, group_id="test-group")
            assert consumer.bootstrap_servers == "localhost:9092"

    def test_subscribe_to_topics(self):
        """Should subscribe to CDC topics."""
        from cdc.consumer import CDCConsumer

        config = {"kafka": {"bootstrap_servers": "localhost:9092"}}

        with patch("cdc.consumer.Consumer") as mock_consumer_class:
            mock_consumer = MagicMock()
            mock_consumer_class.return_value = mock_consumer

            consumer = CDCConsumer(config)
            consumer.subscribe(["cdc.public.customers", "cdc.public.orders"])

            mock_consumer.subscribe.assert_called_once_with(
                ["cdc.public.customers", "cdc.public.orders"]
            )

    def test_parse_insert_event(self):
        """Should parse INSERT (create) event."""
        from cdc.consumer import CDCConsumer

        config = {"kafka": {"bootstrap_servers": "localhost:9092"}}

        with patch("cdc.consumer.Consumer"):
            consumer = CDCConsumer(config)

        # Mock message
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "cdc.public.customers"
        mock_msg.value.return_value = json.dumps(
            {
                "op": "c",
                "before": None,
                "after": {"id": 1, "name": "John", "email": "john@example.com"},
                "source": {
                    "db": "streaming",
                    "table": "customers",
                    "txId": 123,
                    "lsn": 456,
                },
                "ts_ms": 1234567890,
            }
        ).encode("utf-8")

        event = consumer._parse_event(mock_msg)

        assert event["op"] == "c"
        assert event["op_name"] == "INSERT"
        assert event["table"] == "customers"
        assert event["data"]["name"] == "John"
        assert event["after"]["id"] == 1
        assert event["before"] is None

    def test_parse_update_event(self):
        """Should parse UPDATE event."""
        from cdc.consumer import CDCConsumer

        config = {"kafka": {"bootstrap_servers": "localhost:9092"}}

        with patch("cdc.consumer.Consumer"):
            consumer = CDCConsumer(config)

        mock_msg = MagicMock()
        mock_msg.topic.return_value = "cdc.public.customers"
        mock_msg.value.return_value = json.dumps(
            {
                "op": "u",
                "before": {"id": 1, "name": "John", "email": "john@example.com"},
                "after": {"id": 1, "name": "Jane", "email": "jane@example.com"},
                "source": {
                    "db": "streaming",
                    "table": "customers",
                    "txId": 124,
                    "lsn": 457,
                },
                "ts_ms": 1234567891,
            }
        ).encode("utf-8")

        event = consumer._parse_event(mock_msg)

        assert event["op"] == "u"
        assert event["op_name"] == "UPDATE"
        assert event["data"]["name"] == "Jane"
        assert event["before"]["name"] == "John"
        assert event["after"]["name"] == "Jane"

    def test_parse_delete_event(self):
        """Should parse DELETE event."""
        from cdc.consumer import CDCConsumer

        config = {"kafka": {"bootstrap_servers": "localhost:9092"}}

        with patch("cdc.consumer.Consumer"):
            consumer = CDCConsumer(config)

        mock_msg = MagicMock()
        mock_msg.topic.return_value = "cdc.public.customers"
        mock_msg.value.return_value = json.dumps(
            {
                "op": "d",
                "before": {"id": 1, "name": "Jane", "email": "jane@example.com"},
                "after": None,
                "source": {
                    "db": "streaming",
                    "table": "customers",
                    "txId": 125,
                    "lsn": 458,
                },
                "ts_ms": 1234567892,
            }
        ).encode("utf-8")

        event = consumer._parse_event(mock_msg)

        assert event["op"] == "d"
        assert event["op_name"] == "DELETE"
        assert event["data"]["name"] == "Jane"  # Data comes from 'before' on delete
        assert event["before"]["id"] == 1
        assert event["after"] is None

    def test_parse_tombstone_event(self):
        """Should handle tombstone (null value) messages."""
        from cdc.consumer import CDCConsumer

        config = {"kafka": {"bootstrap_servers": "localhost:9092"}}

        with patch("cdc.consumer.Consumer"):
            consumer = CDCConsumer(config)

        mock_msg = MagicMock()
        mock_msg.topic.return_value = "cdc.public.customers"
        mock_msg.value.return_value = None  # Tombstone

        event = consumer._parse_event(mock_msg)

        assert event["op"] == "d"
        assert event["op_name"] == "TOMBSTONE"
        assert event["data"] == {}

    def test_stop_consumer(self):
        """Should stop consuming."""
        from cdc.consumer import CDCConsumer

        config = {"kafka": {"bootstrap_servers": "localhost:9092"}}

        with patch("cdc.consumer.Consumer"):
            consumer = CDCConsumer(config)
            consumer._running = True
            consumer.stop()
            assert consumer._running is False

    def test_close_consumer(self):
        """Should close Kafka consumer."""
        from cdc.consumer import CDCConsumer

        config = {"kafka": {"bootstrap_servers": "localhost:9092"}}

        with patch("cdc.consumer.Consumer") as mock_consumer_class:
            mock_consumer = MagicMock()
            mock_consumer_class.return_value = mock_consumer

            consumer = CDCConsumer(config)
            consumer.close()

            mock_consumer.close.assert_called_once()


class TestCDCProcessor:
    """Tests for CDCProcessor class."""

    def test_add_handler(self):
        """Should register table handlers."""
        from cdc.consumer import CDCProcessor

        config = {"kafka": {"bootstrap_servers": "localhost:9092"}}

        with patch("cdc.consumer.Consumer"):
            processor = CDCProcessor(config)

            handler = MagicMock()
            processor.add_handler("customers", handler)

            assert "customers" in processor._handlers
            assert processor._handlers["customers"] == handler

    def test_multiple_handlers(self):
        """Should support multiple table handlers."""
        from cdc.consumer import CDCProcessor

        config = {"kafka": {"bootstrap_servers": "localhost:9092"}}

        with patch("cdc.consumer.Consumer"):
            processor = CDCProcessor(config)

            customers_handler = MagicMock()
            orders_handler = MagicMock()

            processor.add_handler("customers", customers_handler)
            processor.add_handler("orders", orders_handler)

            assert len(processor._handlers) == 2


class TestCDCOperationTypes:
    """Tests for CDC operation type constants."""

    def test_operation_constants(self):
        """Should have correct operation type constants."""
        from cdc.consumer import CDCConsumer

        assert CDCConsumer.OP_CREATE == "c"
        assert CDCConsumer.OP_UPDATE == "u"
        assert CDCConsumer.OP_DELETE == "d"
        assert CDCConsumer.OP_READ == "r"
