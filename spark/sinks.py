"""Sink writers for streaming data."""

from typing import Optional

import clickhouse_connect

from core.utils.logging import get_logger

logger = get_logger(__name__)


class ClickHouseSink:
    """
    Write streaming data to ClickHouse.

    Usage:
        sink = ClickHouseSink(config)
        sink.create_table("events_per_minute", schema)
        sink.write_batch(df, "events_per_minute")
    """

    def __init__(self, config: dict):
        """
        Args:
            config: Platform config with clickhouse settings
        """
        ch_config = config.get("sinks", {}).get("primary", {}).get("clickhouse", {})
        self.host = ch_config.get("host", "localhost")
        self.port = ch_config.get("port", 8123)
        self.database = ch_config.get("database", "default")
        self.username = ch_config.get("username", "default")
        self.password = ch_config.get("password", "")

        self._client: Optional[clickhouse_connect.driver.Client] = None

    def _get_client(self):
        """Lazy initialization of ClickHouse client."""
        if self._client is None:
            logger.info(f"Connecting to ClickHouse at {self.host}:{self.port}")
            self._client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password,
            )
        return self._client

    def health_check(self) -> bool:
        """Check if ClickHouse is reachable and create database if needed."""
        try:
            # Connect without database first
            client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
            )
            client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            client.close()

            # Now connect with database
            self._client = None  # Reset
            client = self._get_client()
            client.query("SELECT 1")
            logger.info("ClickHouse connection OK")
            return True
        except Exception as e:
            logger.error(f"ClickHouse health check failed: {e}")
            return False

    def create_aggregation_tables(self) -> None:
        """Create tables for common aggregations."""
        client = self._get_client()

        # Events per minute by device type
        client.command(
            """
            CREATE TABLE IF NOT EXISTS events_per_minute (
                window_start DateTime,
                window_end DateTime,
                device_type String,
                count UInt64,
                inserted_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (window_start, device_type)
        """
        )
        logger.info("Created table: events_per_minute")

        # Active users (unique count)
        client.command(
            """
            CREATE TABLE IF NOT EXISTS active_users (
                window_start DateTime,
                window_end DateTime,
                unique_count UInt64,
                inserted_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (window_start)
        """
        )
        logger.info("Created table: active_users")

        # Revenue per hour
        client.command(
            """
            CREATE TABLE IF NOT EXISTS revenue_per_hour (
                window_start DateTime,
                window_end DateTime,
                currency String,
                total_cents Int64,
                inserted_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (window_start, currency)
        """
        )
        logger.info("Created table: revenue_per_hour")

    def write_events_per_minute(self, batch_df, batch_id: int) -> None:
        """
        Write events_per_minute aggregation to ClickHouse.
        Called from foreachBatch.
        """
        if batch_df.isEmpty():
            return

        client = self._get_client()

        # Convert to list of tuples
        rows = []
        for row in batch_df.collect():
            window = row["window"]
            rows.append(
                (window["start"], window["end"], row["device_type"], row["count"])
            )

        if rows:
            client.insert(
                "events_per_minute",
                rows,
                column_names=["window_start", "window_end", "device_type", "count"],
            )
            logger.info(
                f"Batch {batch_id}: Wrote {len(rows)} rows to events_per_minute"
            )

    def write_active_users(self, batch_df, batch_id: int) -> None:
        """Write active_users aggregation to ClickHouse."""
        if batch_df.isEmpty():
            return

        client = self._get_client()

        rows = []
        for row in batch_df.collect():
            window = row["window"]
            rows.append((window["start"], window["end"], row["unique_count"]))

        if rows:
            client.insert(
                "active_users",
                rows,
                column_names=["window_start", "window_end", "unique_count"],
            )
            logger.info(f"Batch {batch_id}: Wrote {len(rows)} rows to active_users")

    def query(self, sql: str):
        """Execute a query and return results."""
        client = self._get_client()
        return client.query(sql)

    def close(self) -> None:
        """Close connection."""
        if self._client:
            self._client.close()
            self._client = None
            logger.info("ClickHouse connection closed")
