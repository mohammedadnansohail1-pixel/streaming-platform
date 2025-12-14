"""Synthetic data generator for testing and development."""

import random
import time
import uuid
from typing import Any, Generator

from core.utils.logging import get_logger
from core.utils.decorators import log_time

logger = get_logger(__name__)

# Sample data for realistic values
SAMPLE_DATA = {
    "page_url": [
        "/",
        "/products",
        "/cart",
        "/checkout",
        "/search",
        "/account",
        "/help",
    ],
    "page_title": [
        "Home",
        "Products",
        "Shopping Cart",
        "Checkout",
        "Search",
        "My Account",
        "Help",
    ],
    "referrer": ["google.com", "facebook.com", "twitter.com", "direct", "email", ""],
    "device_type": ["mobile", "desktop", "tablet"],
    "platform": ["ios", "android", "web", "macos", "windows"],
    "geo_country": ["US", "UK", "CA", "DE", "FR", "JP", "AU", "BR", "IN", "MX"],
    "product_id": [f"PROD_{i:04d}" for i in range(1, 101)],
    "product_name": [
        "Laptop",
        "Phone",
        "Headphones",
        "Tablet",
        "Watch",
        "Camera",
        "Speaker",
        "Monitor",
    ],
    "category": ["electronics", "clothing", "home", "sports", "books", "toys"],
    "currency": ["USD", "EUR", "GBP", "JPY", "CAD"],
    "search_query": [
        "laptop",
        "phone case",
        "headphones",
        "gift ideas",
        "sale",
        "new arrivals",
    ],
}


class SyntheticDataGenerator:
    """
    Generates realistic synthetic events based on config and schemas.

    Usage:
        generator = SyntheticDataGenerator(config, schemas)
        for event in generator.generate_events("page_view", count=100):
            print(event)
    """

    def __init__(self, config: dict, schemas: dict):
        """
        Args:
            config: Domain config with entity and synthetic patterns
            schemas: Dict of event_name → Avro schema
        """
        self.config = config
        self.schemas = schemas
        self.entity_key = config["entity"]["primary_key"]

        # Load synthetic patterns
        synthetic = config.get("synthetic", {})
        self.user_pool_size = synthetic.get("user_pool_size", 10000)

        patterns = synthetic.get("patterns", {})
        self.cart_abandonment_rate = patterns.get("cart_abandonment_rate", 0.70)
        self.purchase_conversion_rate = patterns.get("purchase_conversion_rate", 0.03)
        self.peak_hours = patterns.get("peak_hours", [10, 11, 12, 19, 20, 21])
        self.weekend_multiplier = patterns.get("weekend_multiplier", 1.3)

    def _generate_user_id(self) -> str:
        """Generate a random user ID from the pool."""
        user_num = random.randint(1, self.user_pool_size)
        return f"user_{user_num:06d}"

    def _generate_field_value(self, field_name: str, field_type: Any) -> Any:
        """Generate a value for a specific field."""
        # Handle nullable types ["null", "actual_type"]
        actual_type = field_type
        if isinstance(field_type, list):
            # Nullable - 10% chance of null
            if random.random() < 0.1:
                return None
            actual_type = [t for t in field_type if t != "null"][0]

        # Handle logical types (timestamp)
        if isinstance(actual_type, dict):
            if actual_type.get("logicalType") == "timestamp-millis":
                return int(time.time() * 1000)
            actual_type = actual_type.get("type", "string")

        # Check if we have sample data for this field
        if field_name in SAMPLE_DATA:
            return random.choice(SAMPLE_DATA[field_name])

        # Generate based on type
        if actual_type == "string":
            if field_name == self.entity_key:
                return self._generate_user_id()
            elif field_name == "event_id":
                return str(uuid.uuid4())
            elif "id" in field_name.lower():
                return f"{field_name}_{uuid.uuid4().hex[:8]}"
            else:
                return f"sample_{field_name}"

        elif actual_type == "long":
            # Money fields stored in cents
            return random.randint(100, 100000)  # $1 to $1000

        elif actual_type == "double":
            return round(random.uniform(0, 100), 2)

        elif actual_type == "int":
            return random.randint(1, 100)

        elif actual_type == "boolean":
            return random.choice([True, False])

        return None

    def generate_event(
        self, event_type: str, user_id: str = None, timestamp: int = None
    ) -> dict:
        """
        Generate a single event.

        Args:
            event_type: Name of event (e.g., "page_view")
            user_id: Optional user ID (generates random if not provided)
            timestamp: Optional timestamp in ms (uses current time if not provided)

        Returns:
            Event dict matching schema
        """
        if event_type not in self.schemas:
            raise ValueError(
                f"Unknown event type: {event_type}. Available: {list(self.schemas.keys())}"
            )

        schema = self.schemas[event_type]
        event = {}

        for field in schema["fields"]:
            field_name = field["name"]
            field_type = field["type"]

            # Use provided values for key fields
            if field_name == self.entity_key and user_id:
                event[field_name] = user_id
            elif field_name == "timestamp" and timestamp:
                event[field_name] = timestamp
            else:
                event[field_name] = self._generate_field_value(field_name, field_type)

        return event

    @log_time
    def generate_events(
        self, event_type: str, count: int
    ) -> Generator[dict, None, None]:
        """
        Generate multiple events of the same type.

        Args:
            event_type: Name of event
            count: Number of events to generate

        Yields:
            Event dicts
        """
        logger.info(f"Generating {count} '{event_type}' events")
        for _ in range(count):
            yield self.generate_event(event_type)

    def generate_user_session(self, user_id: str = None) -> list[dict]:
        """
        Generate a realistic user session (browse → maybe cart → maybe purchase).

        Returns:
            List of events representing a user journey
        """
        user_id = user_id or self._generate_user_id()
        logger.debug(f"Generating session for {user_id}")
        base_time = int(time.time() * 1000)
        events = []

        # Always start with page view
        events.append(self.generate_event("page_view", user_id, base_time))

        # 60% chance to view a product
        if random.random() < 0.6:
            base_time += random.randint(5000, 30000)  # 5-30 seconds later
            events.append(self.generate_event("product_view", user_id, base_time))

            # 40% chance to add to cart (of those who viewed)
            if random.random() < 0.4:
                base_time += random.randint(10000, 60000)
                events.append(self.generate_event("add_to_cart", user_id, base_time))

                # Use configured purchase conversion rate
                if random.random() < self.purchase_conversion_rate:
                    base_time += random.randint(30000, 120000)
                    events.append(self.generate_event("purchase", user_id, base_time))

        # 20% chance of search
        if random.random() < 0.2:
            base_time += random.randint(1000, 10000)
            events.append(self.generate_event("search", user_id, base_time))

        logger.debug(f"Session complete: {len(events)} events")
        return events
