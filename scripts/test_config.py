"""Quick test of config loading with actual config files."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
from dotenv import load_dotenv  # noqa: E402

load_dotenv(project_root / ".env")

from core.config.loader import ConfigLoader  # noqa: E402
from core.schema.generator import SchemaGenerator  # noqa: E402


def main():
    loader = ConfigLoader()

    print("=" * 50)
    print("Loading ecommerce config (no environment)")
    print("=" * 50)

    try:
        config = loader.load(domain="ecommerce")

        print("\n✓ Loaded successfully!")
        print(f"\nTop-level keys: {list(config.keys())}")

        # Show some config values
        if "kafka" in config:
            print("\nKafka config:")
            print(
                f"  bootstrap_servers: {config['kafka'].get('bootstrap_servers', 'N/A')}"
            )

        if "entity" in config:
            print(f"\nEntity: {config['entity']}")

        if "event_types" in config:
            event_names = [e.get("name", "unknown") for e in config["event_types"]]
            print(f"\nEvent types: {event_names}")
        # Test schema generation
        print("\n" + "=" * 50)
        print("Generating Avro schemas")
        print("=" * 50)

        generator = SchemaGenerator()
        schemas = generator.generate_all_schemas(config)

        print(f"\nGenerated {len(schemas)} schemas:")
        for name, schema in schemas.items():
            field_count = len(schema["fields"])
            print(f"  - {name}: {field_count} fields")

        # Show one full schema as example
        print("\nExample schema (page_view):")
        print(generator.to_json(schemas["page_view"]))

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback

        traceback.print_exc()
        return 1

    print("\n" + "=" * 50)
    print("Health check")
    print("=" * 50)
    print(f"Config loader healthy: {loader.health_check()}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
