"""Test full flow: config → schema → synthetic events."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load .env file
from dotenv import load_dotenv  # noqa: E402

load_dotenv(project_root / ".env")

from core.config.loader import ConfigLoader  # noqa: E402
from core.schema.generator import SchemaGenerator  # noqa: E402
from core.utils.logging import setup_logging  # noqa: E402
from generators.synthetic import SyntheticDataGenerator  # noqa: E402


def main():
    # Setup logging
    setup_logging(level="INFO")

    print("=" * 60)
    print("STEP 1: Load Configuration")
    print("=" * 60)

    loader = ConfigLoader()
    config = loader.load(domain="ecommerce")

    print("✓ Loaded config for domain: ecommerce")
    print(f"  Entity key: {config['entity']['primary_key']}")
    print(f"  Event types: {[e['name'] for e in config['event_types']]}")

    print("\n" + "=" * 60)
    print("STEP 2: Generate Avro Schemas")
    print("=" * 60)

    generator = SchemaGenerator()
    schemas = generator.generate_all_schemas(config)

    print(f"✓ Generated {len(schemas)} schemas")
    for name, schema in schemas.items():
        fields = [f["name"] for f in schema["fields"]]
        print(f"  - {name}: {len(fields)} fields")

    print("\n" + "=" * 60)
    print("STEP 3: Generate Synthetic Events")
    print("=" * 60)

    data_gen = SyntheticDataGenerator(config, schemas)

    # Generate a few events of each type
    print("\nSample events:")
    for event_type in ["page_view", "purchase"]:
        event = data_gen.generate_event(event_type)
        print(f"\n{event_type}:")
        for key, value in event.items():
            print(f"  {key}: {value}")

    print("\n" + "=" * 60)
    print("STEP 4: Generate User Session")
    print("=" * 60)

    session = data_gen.generate_user_session()
    print(f"\n✓ Generated session with {len(session)} events:")
    for event in session:
        event_type = next(
            (
                name
                for name, schema in schemas.items()
                if set(f["name"] for f in schema["fields"]) == set(event.keys())
            ),
            "unknown",
        )
        print(f"  - {event_type} at {event['timestamp']}")

    print("\n" + "=" * 60)
    print("STEP 5: Batch Generation")
    print("=" * 60)

    events = list(data_gen.generate_events("page_view", count=100))
    print(f"\n✓ Generated {len(events)} page_view events")

    # Show unique users
    unique_users = set(e["user_id"] for e in events)
    print(f"  Unique users: {len(unique_users)}")

    print("\n" + "=" * 60)
    print("✓ Full flow complete!")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
