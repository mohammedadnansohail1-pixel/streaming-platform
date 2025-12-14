"""ClickHouse sink implementation."""

from typing import Optional

import clickhouse_connect

from core.utils.logging import get_logger
from sinks.base import BaseSink

logger = get_logger(__name__)

# Type mapping from config schema to ClickHouse types
TYPE_MAP = {
    "string": "String",
    "int": "Int64",
    "long": "Int64",
    "float": "Float64",
    "double": "Float64",
    "boolean": "UInt8",
    "timestamp": "DateTime",
    "datetime": "DateTime",
    "date": "Date",
}


class ClickHouseSink(BaseSink):
    """
    ClickHouse sink implementation.

    Usage:
        sink = ClickHouseSink(config)
        sink.health_check()
        sink.create_table("events_per_minute", schema)

        # In foreachBatch:
        sink.write_batch(df, batch_id, "events_per_minute", {
            "window.start": "window_start",
            "window.end": "window_end",
            "device_type": "device_type",
            "count": "count"
        })
    """

    def __init__(self, config: dict):
        super().__init__(config, "clickhouse")

        ch_config = config.get("sinks", {}).get("primary", {}).get("clickhouse", {})
        self.host = ch_config.get("host", "localhost")
        self.port = ch_config.get("port", 8123)
        self.database = ch_config.get("database", "default")
        self.username = ch_config.get("user", ch_config.get("username", "default"))
        self.password = ch_config.get("password", "")

        self._client: Optional[clickhouse_connect.driver.Client] = None

    def _connect(self) -> None:
        """Establish connection to ClickHouse."""
        if self._client is None:
            logger.info(f"Connecting to ClickHouse at {self.host}:{self.port}")
            self._client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password,
            )
            self._connected = True

    def _get_client(self):
        """Get client, connecting if needed."""
        if self._client is None:
            self._connect()
        return self._client

    def _write_rows(self, table: str, rows: list[tuple], columns: list[str]) -> None:
        """Write rows to ClickHouse table."""
        client = self._get_client()
        client.insert(table, rows, column_names=columns)

    def health_check(self) -> bool:
        """Check connection and create database if needed."""
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

            # Connect with database
            self._client = None
            self._get_client()
            self._client.query("SELECT 1")
            logger.info("ClickHouse connection OK")
            return True
        except Exception as e:
            logger.error(f"ClickHouse health check failed: {e}")
            return False

    def create_table(self, table: str, schema: dict) -> None:
        """
        Create table from schema definition.

        Args:
            table: Table name
            schema: Dict with "columns" list and optional "order_by"
                   {"columns": [{"name": "x", "type": "string"}], "order_by": ["x"]}
        """
        client = self._get_client()

        # Build column definitions
        col_defs = []
        for col in schema["columns"]:
            ch_type = TYPE_MAP.get(col["type"], "String")
            nullable = col.get("nullable", False)
            if nullable:
                ch_type = f"Nullable({ch_type})"
            col_defs.append(f"{col['name']} {ch_type}")

        # Add inserted_at timestamp
        col_defs.append("inserted_at DateTime DEFAULT now()")

        # Build ORDER BY
        order_by = schema.get("order_by", [schema["columns"][0]["name"]])
        order_clause = ", ".join(order_by)

        sql = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                {', '.join(col_defs)}
            ) ENGINE = MergeTree()
            ORDER BY ({order_clause})
        """

        client.command(sql)
        logger.info(f"Created table: {table}")

    def query(self, sql: str):
        """Execute a query and return results."""
        client = self._get_client()
        return client.query(sql)

    def close(self) -> None:
        """Close connection."""
        if self._client:
            self._client.close()
            self._client = None
            self._connected = False
            logger.info("ClickHouse connection closed")
