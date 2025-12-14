"""Avro schema generator from domain configuration."""

import json

from core.schema.types import BASE_FIELDS, get_field_type


class SchemaGenerator:
    """
    Generates Avro schemas from domain event configuration.

    Usage:
        generator = SchemaGenerator(namespace="com.mycompany.events")
        schema = generator.generate_event_schema(event_config, entity_key="user_id")
    """

    def __init__(self, namespace: str = "com.streaming.events"):
        self.namespace = namespace

    def generate_event_schema(self, event_config: dict, entity_key: str) -> dict:
        """
        Generate Avro schema for a single event type.

        Args:
            event_config: Event definition from domain config
            entity_key: Primary key field (e.g., "user_id")

        Returns:
            Avro schema dict
        """
        event_name = event_config["name"]

        # Start with base fields
        fields = list(BASE_FIELDS)

        # Add entity key (e.g., user_id)
        fields.append({"name": entity_key, "type": "string"})

        # Add attributes (nullable strings)
        for attr in event_config.get("attributes", []):
            fields.append(
                {
                    "name": attr,
                    "type": get_field_type(attr, "attributes"),
                    "default": None,
                }
            )

        # Add metrics (nullable doubles or longs for money)
        for metric in event_config.get("metrics", []):
            fields.append(
                {
                    "name": metric,
                    "type": get_field_type(metric, "metrics"),
                    "default": None,
                }
            )

        # Add dimensions (required strings)
        for dim in event_config.get("dimensions", []):
            fields.append({"name": dim, "type": get_field_type(dim, "dimensions")})

        return {
            "type": "record",
            "namespace": self.namespace,
            "name": event_name,
            "fields": fields,
        }

    def generate_all_schemas(self, config: dict) -> dict[str, dict]:
        """
        Generate schemas for all event types in config.

        Args:
            config: Full domain config with event_types and entity

        Returns:
            Dict mapping event name → Avro schema
        """
        entity_key = config["entity"]["primary_key"]
        schemas = {}

        for event_config in config.get("event_types", []):
            event_name = event_config["name"]
            schemas[event_name] = self.generate_event_schema(event_config, entity_key)

        return schemas

    def to_json(self, schema: dict, indent: int = 2) -> str:
        """Convert schema dict to JSON string."""
        return json.dumps(schema, indent=indent)
