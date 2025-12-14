"""Tests for Avro schema generator."""

from core.schema.generator import SchemaGenerator
from core.schema.types import MONEY_FIELDS


class TestSchemaGenerator:
    """Tests for SchemaGenerator."""

    def test_generate_event_schema_basic(self):
        """Should generate schema with base fields."""
        # Arrange
        generator = SchemaGenerator()
        event_config = {"name": "test_event"}

        # Act
        schema = generator.generate_event_schema(event_config, entity_key="user_id")

        # Assert
        assert schema["type"] == "record"
        assert schema["name"] == "test_event"
        assert schema["namespace"] == "com.streaming.events"

        field_names = [f["name"] for f in schema["fields"]]
        assert "event_id" in field_names
        assert "timestamp" in field_names
        assert "user_id" in field_names

    def test_generate_event_schema_with_attributes(self):
        """Should add nullable string fields for attributes."""
        # Arrange
        generator = SchemaGenerator()
        event_config = {"name": "page_view", "attributes": ["page_url", "referrer"]}

        # Act
        schema = generator.generate_event_schema(event_config, entity_key="user_id")

        # Assert
        fields = {f["name"]: f for f in schema["fields"]}

        assert "page_url" in fields
        assert fields["page_url"]["type"] == ["null", "string"]
        assert fields["page_url"]["default"] is None

        assert "referrer" in fields
        assert fields["referrer"]["type"] == ["null", "string"]

    def test_generate_event_schema_with_metrics(self):
        """Should add nullable double fields for metrics."""
        # Arrange
        generator = SchemaGenerator()
        event_config = {
            "name": "product_view",
            "metrics": ["view_duration", "scroll_depth"],
        }

        # Act
        schema = generator.generate_event_schema(event_config, entity_key="user_id")

        # Assert
        fields = {f["name"]: f for f in schema["fields"]}

        assert "view_duration" in fields
        assert fields["view_duration"]["type"] == ["null", "double"]

    def test_generate_event_schema_money_fields_are_long(self):
        """Money fields should be long (cents) not double."""
        # Arrange
        generator = SchemaGenerator()
        event_config = {
            "name": "purchase",
            "metrics": ["total_amount", "price", "quantity"],
        }

        # Act
        schema = generator.generate_event_schema(event_config, entity_key="user_id")

        # Assert
        fields = {f["name"]: f for f in schema["fields"]}

        # Money fields → long
        assert fields["total_amount"]["type"] == ["null", "long"]
        assert fields["price"]["type"] == ["null", "long"]

        # Non-money metric → double
        assert fields["quantity"]["type"] == ["null", "double"]

    def test_generate_event_schema_with_dimensions(self):
        """Should add required string fields for dimensions."""
        # Arrange
        generator = SchemaGenerator()
        event_config = {
            "name": "page_view",
            "dimensions": ["device_type", "geo_country"],
        }

        # Act
        schema = generator.generate_event_schema(event_config, entity_key="user_id")

        # Assert
        fields = {f["name"]: f for f in schema["fields"]}

        assert "device_type" in fields
        assert fields["device_type"]["type"] == "string"
        assert "default" not in fields["device_type"]  # Required, no default

    def test_generate_event_schema_custom_namespace(self):
        """Should use custom namespace."""
        # Arrange
        generator = SchemaGenerator(namespace="com.netflix.events")
        event_config = {"name": "test_event"}

        # Act
        schema = generator.generate_event_schema(event_config, entity_key="user_id")

        # Assert
        assert schema["namespace"] == "com.netflix.events"

    def test_generate_all_schemas(self):
        """Should generate schemas for all event types."""
        # Arrange
        generator = SchemaGenerator()
        config = {
            "entity": {"primary_key": "user_id"},
            "event_types": [
                {"name": "page_view", "attributes": ["page_url"]},
                {"name": "purchase", "metrics": ["total_amount"]},
            ],
        }

        # Act
        schemas = generator.generate_all_schemas(config)

        # Assert
        assert "page_view" in schemas
        assert "purchase" in schemas
        assert schemas["page_view"]["name"] == "page_view"
        assert schemas["purchase"]["name"] == "purchase"

    def test_to_json(self):
        """Should convert schema to JSON string."""
        # Arrange
        generator = SchemaGenerator()
        schema = {"type": "record", "name": "test"}

        # Act
        json_str = generator.to_json(schema)

        # Assert
        assert '"type": "record"' in json_str
        assert '"name": "test"' in json_str


class TestMoneyFields:
    """Tests for money field detection."""

    def test_money_fields_defined(self):
        """Should have common money field names."""
        assert "price" in MONEY_FIELDS
        assert "total_amount" in MONEY_FIELDS
        assert "revenue" in MONEY_FIELDS
        assert "cost" in MONEY_FIELDS
