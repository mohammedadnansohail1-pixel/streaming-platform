"""Type mappings for Avro schema generation."""

# Base fields every event gets
BASE_FIELDS = [
    {"name": "event_id", "type": "string"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
]

# Field category → Avro type
CATEGORY_TYPES = {
    "attributes": ["null", "string"],  # Nullable strings
    "dimensions": "string",  # Required categorical
    "metrics": ["null", "double"],  # Nullable doubles
}

# Specific fields that should be long (money stored in cents)
MONEY_FIELDS = {
    "price",
    "total_amount",
    "unit_price",
    "revenue",
    "cost",
    "amount",
}


def get_field_type(field_name: str, category: str):
    """
    Get Avro type for a field.

    Args:
        field_name: Name of the field (e.g., "price", "device_type")
        category: Field category (attributes, metrics, dimensions)

    Returns:
        Avro type definition
    """
    # Money fields → nullable long (cents)
    if field_name in MONEY_FIELDS:
        return ["null", "long"]

    # Everything else → category default
    return CATEGORY_TYPES.get(category, "string")


def make_nullable(avro_type):
    """Wrap type in nullable union if not already."""
    if isinstance(avro_type, list) and "null" in avro_type:
        return avro_type
    return ["null", avro_type]
