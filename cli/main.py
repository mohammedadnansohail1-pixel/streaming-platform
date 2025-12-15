"""CLI for streaming platform."""

import sys
from pathlib import Path

import click

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(project_root / ".env")


@click.group()
@click.version_option(version="1.0.0", prog_name="streaming-cli")
def cli():
    """Streaming Platform CLI - Real-time event processing."""
    pass


@cli.command()
@click.option("--domain", "-d", default="ecommerce", help="Domain to use")
@click.option("--environment", "-e", default=None, help="Environment (dev/prod)")
def config(domain: str, environment: str):
    """Show loaded configuration."""
    from core.config.loader import ConfigLoader

    loader = ConfigLoader()
    cfg = loader.load(domain=domain, environment=environment)

    click.echo(f"\n{'='*60}")
    click.echo(f"Domain: {cfg.get('domain', domain)}")
    click.echo(f"{'='*60}")

    click.echo(f"\nKafka: {cfg['kafka']['bootstrap_servers']}")
    click.echo(f"Schema Registry: {cfg['kafka']['schema_registry']['url']}")

    click.echo(f"\nEvent Types ({len(cfg['event_types'])}):")
    for et in cfg["event_types"]:
        click.echo(f"  - {et['name']}")

    click.echo(f"\nAggregations ({len(cfg['aggregations'])}):")
    for agg in cfg["aggregations"]:
        click.echo(f"  - {agg['name']} ({agg['type']})")


@cli.command()
def domains():
    """List available domains."""
    config_dir = project_root / "config" / "domains"

    click.echo(f"\n{'='*60}")
    click.echo("Available Domains")
    click.echo(f"{'='*60}\n")

    for f in sorted(config_dir.glob("*.yaml")):
        domain_name = f.stem
        click.echo(f"  • {domain_name}")

    click.echo("\nUsage: streaming-cli config --domain <name>")


@cli.command()
@click.option("--domain", "-d", default="ecommerce", help="Domain to use")
@click.option("--event-type", "-t", default="page_view", help="Event type")
@click.option("--count", "-n", default=10, help="Number of events")
@click.option("--delay", default=0.1, help="Delay between events (seconds)")
@click.option("--dry-run", is_flag=True, help="Print events without sending")
def generate(domain: str, event_type: str, count: int, delay: float, dry_run: bool):
    """Generate and send synthetic events to Kafka."""
    import time
    import json

    from core.config.loader import ConfigLoader
    from core.schema.generator import SchemaGenerator
    from generators.synthetic import SyntheticDataGenerator

    click.echo(f"\n{'='*60}")
    click.echo(f"Generating {count} '{event_type}' events")
    click.echo(f"{'='*60}\n")

    loader = ConfigLoader()
    cfg = loader.load(domain=domain)

    generator = SchemaGenerator()
    schemas = generator.generate_all_schemas(cfg)

    if event_type not in schemas:
        click.echo(f"Error: Unknown event type '{event_type}'", err=True)
        click.echo(f"Available: {', '.join(schemas.keys())}", err=True)
        raise SystemExit(1)

    data_gen = SyntheticDataGenerator(cfg, schemas)

    if dry_run:
        click.echo("[Dry Run - Not sending to Kafka]\n")
        for i, event in enumerate(data_gen.generate_events(event_type, count)):
            if i < 3:  # Show first 3
                click.echo(json.dumps(event, indent=2, default=str))
            elif i == 3:
                click.echo(f"... and {count - 3} more events")
                break
        return

    from sources.kafka_producer import KafkaEventProducer

    producer = KafkaEventProducer(cfg, schemas)

    with click.progressbar(
        data_gen.generate_events(event_type, count), length=count, label="Sending"
    ) as events:
        for event in events:
            producer.send(event_type, event)
            time.sleep(delay)

    producer.flush()
    stats = producer.get_stats()
    producer.close()

    click.echo(f"\n✓ Sent: {stats['sent']}, Errors: {stats['errors']}")


@cli.command()
@click.option("--domain", "-d", default="ecommerce", help="Domain to use")
@click.option("--event-type", "-t", default="page_view", help="Event type to process")
@click.option(
    "--aggregation", "-a", default="events_per_minute", help="Aggregation to run"
)
@click.option("--timeout", default=60, help="Timeout in seconds (0 for infinite)")
def run(domain: str, event_type: str, aggregation: str, timeout: int):
    """Run Spark streaming aggregation job."""
    from core.config.loader import ConfigLoader
    from core.schema.generator import SchemaGenerator
    from spark.streaming_job import StreamingJob

    click.echo(f"\n{'='*60}")
    click.echo("Starting Spark Streaming Job")
    click.echo(f"{'='*60}")
    click.echo(f"Domain: {domain}")
    click.echo(f"Event: {event_type}")
    click.echo(f"Aggregation: {aggregation}")
    click.echo(f"{'='*60}\n")

    loader = ConfigLoader()
    cfg = loader.load(domain=domain)

    generator = SchemaGenerator()
    schemas = generator.generate_all_schemas(cfg)

    # Find aggregation config
    agg_config = None
    for agg in cfg["aggregations"]:
        if agg["name"] == aggregation:
            agg_config = agg
            break

    if not agg_config:
        click.echo(f"Error: Unknown aggregation '{aggregation}'", err=True)
        click.echo(
            f"Available: {', '.join(a['name'] for a in cfg['aggregations'])}", err=True
        )
        raise SystemExit(1)

    job = StreamingJob(cfg, schemas)

    try:
        raw_df = job.read_stream([event_type])
        parsed_df = job.parse_events(raw_df, event_type)
        agg_df = job.aggregate_events(parsed_df, agg_config)

        job.write_console(agg_df, f"{event_type}_{aggregation}")

        click.echo("Streaming started. Press Ctrl+C to stop.\n")
        job.await_termination(timeout=timeout if timeout > 0 else None)

    except KeyboardInterrupt:
        click.echo("\n\nStopping...")
    finally:
        job.stop()
        click.echo("✓ Stopped")


@cli.command()
@click.option("--domain", "-d", default="ecommerce", help="Domain to use")
def health(domain: str):
    """Check health of all services."""
    from core.config.loader import ConfigLoader
    from core.schema.generator import SchemaGenerator

    click.echo(f"\n{'='*60}")
    click.echo("Health Check")
    click.echo(f"{'='*60}\n")

    loader = ConfigLoader()
    cfg = loader.load(domain=domain)

    # Config
    click.echo(f"✓ Config loaded ({domain})")

    # Schemas
    generator = SchemaGenerator()
    schemas = generator.generate_all_schemas(cfg)
    click.echo(f"✓ Schemas generated ({len(schemas)} event types)")

    # Kafka
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": cfg["kafka"]["bootstrap_servers"]})
        metadata = admin.list_topics(timeout=5)
        click.echo(f"✓ Kafka connected ({len(metadata.topics)} topics)")
    except Exception as e:
        click.echo(f"✗ Kafka failed: {e}")

    # Schema Registry
    try:
        import httpx

        resp = httpx.get(
            f"{cfg['kafka']['schema_registry']['url']}/subjects", timeout=5
        )
        subjects = resp.json()
        click.echo(f"✓ Schema Registry connected ({len(subjects)} subjects)")
    except Exception as e:
        click.echo(f"✗ Schema Registry failed: {e}")

    # ClickHouse
    try:
        from sinks import ClickHouseSink

        sink = ClickHouseSink(cfg)
        if sink.health_check():
            click.echo("✓ ClickHouse connected")
        sink.close()
    except Exception as e:
        click.echo(f"✗ ClickHouse failed: {e}")

    # PostgreSQL
    try:
        from sinks import PostgreSQLSink

        sink = PostgreSQLSink(cfg)
        if sink.health_check():
            click.echo("✓ PostgreSQL connected")
        sink.close()
    except Exception as e:
        click.echo(f"✗ PostgreSQL failed: {e}")

    click.echo("")


@cli.command()
@click.option("--domain", "-d", default="ecommerce", help="Domain to use")
@click.option("--event-type", "-t", default="page_view", help="Event type")
def schema(domain: str, event_type: str):
    """Show Avro schema for an event type."""
    import json

    from core.config.loader import ConfigLoader
    from core.schema.generator import SchemaGenerator

    loader = ConfigLoader()
    cfg = loader.load(domain=domain)

    generator = SchemaGenerator()
    schemas = generator.generate_all_schemas(cfg)

    if event_type not in schemas:
        click.echo(f"Error: Unknown event type '{event_type}'", err=True)
        click.echo(f"Available: {', '.join(schemas.keys())}", err=True)
        raise SystemExit(1)

    click.echo(json.dumps(schemas[event_type], indent=2))


def main():
    """Entry point."""
    cli()


if __name__ == "__main__":
    main()
