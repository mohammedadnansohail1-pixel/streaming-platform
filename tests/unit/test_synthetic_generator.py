"""Tests for synthetic data generator."""

import pytest

from generators.synthetic import SyntheticDataGenerator, SAMPLE_DATA


@pytest.fixture
def sample_config():
    """Sample domain config."""
    return {
        "entity": {"primary_key": "user_id"},
        "synthetic": {
            "user_pool_size": 1000,
            "patterns": {
                "cart_abandonment_rate": 0.70,
                "purchase_conversion_rate": 0.03,
            },
        },
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
                {
                    "name": "timestamp",
                    "type": {"type": "long", "logicalType": "timestamp-millis"},
                },
                {"name": "user_id", "type": "string"},
                {"name": "page_url", "type": ["null", "string"], "default": None},
                {"name": "device_type", "type": "string"},
            ],
        },
        "product_view": {
            "type": "record",
            "name": "product_view",
            "fields": [
                {"name": "event_id", "type": "string"},
                {
                    "name": "timestamp",
                    "type": {"type": "long", "logicalType": "timestamp-millis"},
                },
                {"name": "user_id", "type": "string"},
                {"name": "product_id", "type": ["null", "string"], "default": None},
            ],
        },
        "add_to_cart": {
            "type": "record",
            "name": "add_to_cart",
            "fields": [
                {"name": "event_id", "type": "string"},
                {
                    "name": "timestamp",
                    "type": {"type": "long", "logicalType": "timestamp-millis"},
                },
                {"name": "user_id", "type": "string"},
                {"name": "product_id", "type": ["null", "string"], "default": None},
            ],
        },
        "purchase": {
            "type": "record",
            "name": "purchase",
            "fields": [
                {"name": "event_id", "type": "string"},
                {
                    "name": "timestamp",
                    "type": {"type": "long", "logicalType": "timestamp-millis"},
                },
                {"name": "user_id", "type": "string"},
                {"name": "total_amount", "type": ["null", "long"], "default": None},
            ],
        },
        "search": {
            "type": "record",
            "name": "search",
            "fields": [
                {"name": "event_id", "type": "string"},
                {
                    "name": "timestamp",
                    "type": {"type": "long", "logicalType": "timestamp-millis"},
                },
                {"name": "user_id", "type": "string"},
                {"name": "search_query", "type": ["null", "string"], "default": None},
            ],
        },
    }


class TestSyntheticDataGenerator:
    """Tests for SyntheticDataGenerator."""

    def test_generate_event_has_required_fields(self, sample_config, sample_schemas):
        """Generated event should have all schema fields."""
        # Arrange
        generator = SyntheticDataGenerator(sample_config, sample_schemas)

        # Act
        event = generator.generate_event("page_view")

        # Assert
        assert "event_id" in event
        assert "timestamp" in event
        assert "user_id" in event
        assert "page_url" in event
        assert "device_type" in event

    def test_generate_event_id_is_uuid(self, sample_config, sample_schemas):
        """Event ID should be a valid UUID format."""
        # Arrange
        generator = SyntheticDataGenerator(sample_config, sample_schemas)

        # Act
        event = generator.generate_event("page_view")

        # Assert
        assert len(event["event_id"]) == 36  # UUID format
        assert event["event_id"].count("-") == 4

    def test_generate_event_timestamp_is_milliseconds(
        self, sample_config, sample_schemas
    ):
        """Timestamp should be in milliseconds."""
        # Arrange
        generator = SyntheticDataGenerator(sample_config, sample_schemas)

        # Act
        event = generator.generate_event("page_view")

        # Assert
        # Milliseconds since epoch should be > 1 trillion
        assert event["timestamp"] > 1_000_000_000_000

    def test_generate_event_user_id_from_pool(self, sample_config, sample_schemas):
        """User ID should be from configured pool size."""
        # Arrange
        sample_config["synthetic"]["user_pool_size"] = 100
        generator = SyntheticDataGenerator(sample_config, sample_schemas)

        # Act
        event = generator.generate_event("page_view")

        # Assert
        assert event["user_id"].startswith("user_")
        user_num = int(event["user_id"].split("_")[1])
        assert 1 <= user_num <= 100

    def test_generate_event_uses_sample_data(self, sample_config, sample_schemas):
        """Should use sample data for known fields."""
        # Arrange
        generator = SyntheticDataGenerator(sample_config, sample_schemas)

        # Act
        event = generator.generate_event("page_view")

        # Assert
        assert event["device_type"] in SAMPLE_DATA["device_type"]

    def test_generate_event_with_custom_user_id(self, sample_config, sample_schemas):
        """Should use provided user ID."""
        # Arrange
        generator = SyntheticDataGenerator(sample_config, sample_schemas)

        # Act
        event = generator.generate_event("page_view", user_id="custom_user_123")

        # Assert
        assert event["user_id"] == "custom_user_123"

    def test_generate_event_with_custom_timestamp(self, sample_config, sample_schemas):
        """Should use provided timestamp."""
        # Arrange
        generator = SyntheticDataGenerator(sample_config, sample_schemas)
        custom_ts = 1699999999000

        # Act
        event = generator.generate_event("page_view", timestamp=custom_ts)

        # Assert
        assert event["timestamp"] == custom_ts

    def test_generate_event_unknown_type_raises_error(
        self, sample_config, sample_schemas
    ):
        """Should raise error for unknown event type."""
        # Arrange
        generator = SyntheticDataGenerator(sample_config, sample_schemas)

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            generator.generate_event("unknown_event")

        assert "Unknown event type" in str(exc_info.value)

    def test_generate_events_returns_correct_count(self, sample_config, sample_schemas):
        """Should generate requested number of events."""
        # Arrange
        generator = SyntheticDataGenerator(sample_config, sample_schemas)

        # Act
        events = list(generator.generate_events("page_view", count=10))

        # Assert
        assert len(events) == 10

    def test_generate_events_all_unique_ids(self, sample_config, sample_schemas):
        """Each event should have unique event_id."""
        # Arrange
        generator = SyntheticDataGenerator(sample_config, sample_schemas)

        # Act
        events = list(generator.generate_events("page_view", count=100))
        event_ids = [e["event_id"] for e in events]

        # Assert
        assert len(set(event_ids)) == 100  # All unique

    def test_generate_user_session_starts_with_page_view(
        self, sample_config, sample_schemas
    ):
        """User session should always start with page_view."""
        # Arrange
        generator = SyntheticDataGenerator(sample_config, sample_schemas)

        # Act
        session = generator.generate_user_session()

        # Assert
        assert len(session) >= 1
        # First event should be page_view (check by fields)
        assert "page_url" in session[0] or "device_type" in session[0]

    def test_generate_user_session_same_user_id(self, sample_config, sample_schemas):
        """All events in session should have same user_id."""
        # Arrange
        generator = SyntheticDataGenerator(sample_config, sample_schemas)

        # Act
        session = generator.generate_user_session()

        # Assert
        user_ids = [e["user_id"] for e in session]
        assert len(set(user_ids)) == 1  # All same user

    def test_generate_user_session_timestamps_increase(
        self, sample_config, sample_schemas
    ):
        """Timestamps should increase through session."""
        # Arrange
        generator = SyntheticDataGenerator(sample_config, sample_schemas)

        # Act
        session = generator.generate_user_session()

        # Assert
        timestamps = [e["timestamp"] for e in session]
        assert timestamps == sorted(timestamps)  # In order
