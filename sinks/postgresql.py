"""PostgreSQL sink implementation."""

from typing import Optional

import psycopg2
from psycopg2.extras import execute_values

from core.utils.logging import get_logger
from sinks.base import BaseSink

logger = get_logger(__name__)

# Type mapping from config schema to PostgreSQL types
TYPE_MAP = {
    "string": "TEXT",
    "int": "INTEGER",
    "long": "BIGINT",
    "float": "REAL",
    "double": "DOUBLE PRECISION",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "datetime": "TIMESTAMP",
    "date": "DATE",
}


class PostgreSQLSink(BaseSink):
    """
    PostgreSQL sink implementation.

    Usage:
        sink = PostgreSQLSink(config)
        sink.health_check()
        sink.create_table("events", schema)

        # In foreachBatch:
        sink.write_batch(df, batch_id, "events", {
            "event_id": "event_id",
            "timestamp": "event_time",
        })
    """

    def __init__(self, config: dict):
        super().__init__(config, "postgresql")

        pg_config = config.get("sinks", {}).get("primary", {}).get("postgresql", {})
        self.host = pg_config.get("host", "localhost")
        self.port = pg_config.get("port", 5432)
        self.database = pg_config.get("database", "streaming")
        self.username = pg_config.get("user", pg_config.get("username", "postgres"))
        self.password = pg_config.get("password", "")

        self._conn: Optional[psycopg2.extensions.connection] = None

    def _connect(self) -> None:
        """Establish connection to PostgreSQL."""
        if self._conn is None or self._conn.closed:
            logger.info(f"Connecting to PostgreSQL at {self.host}:{self.port}")
            self._conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.username,
                password=self.password,
            )
            self._conn.autocommit = True
            self._connected = True

    def _get_conn(self):
        """Get connection, connecting if needed."""
        if self._conn is None or self._conn.closed:
            self._connect()
        return self._conn

    def _write_rows(self, table: str, rows: list[tuple], columns: list[str]) -> None:
        """Write rows to PostgreSQL table using batch insert."""
        conn = self._get_conn()
        col_list = ", ".join(columns)

        with conn.cursor() as cur:
            execute_values(
                cur,
                f"INSERT INTO {table} ({col_list}) VALUES %s",
                rows,
            )

    def health_check(self) -> bool:
        """Check connection and create database if needed."""
        try:
            # First try connecting to default database
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname="postgres",
                user=self.username,
                password=self.password,
            )
            conn.autocommit = True

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s", (self.database,)
                )
                if not cur.fetchone():
                    cur.execute(f"CREATE DATABASE {self.database}")
                    logger.info(f"Created database: {self.database}")
            conn.close()

            # Connect to target database
            self._connect()
            with self._conn.cursor() as cur:
                cur.execute("SELECT 1")

            logger.info("PostgreSQL connection OK")
            return True
        except Exception as e:
            logger.error(f"PostgreSQL health check failed: {e}")
            return False

    def create_table(self, table: str, schema: dict) -> None:
        """
        Create table from schema definition.

        Args:
            table: Table name
            schema: Dict with "columns" list and optional "primary_key"
                   {"columns": [{"name": "x", "type": "string"}], "primary_key": "id"}
        """
        conn = self._get_conn()

        # Build column definitions
        col_defs = []
        for col in schema["columns"]:
            pg_type = TYPE_MAP.get(col["type"], "TEXT")
            nullable = col.get("nullable", True)
            null_clause = "" if nullable else " NOT NULL"
            col_defs.append(f"{col['name']} {pg_type}{null_clause}")

        # Add timestamp
        col_defs.append("inserted_at TIMESTAMP DEFAULT NOW()")

        # Primary key
        pk = schema.get("primary_key")
        if pk:
            col_defs.append(f"PRIMARY KEY ({pk})")

        sql = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                {', '.join(col_defs)}
            )
        """

        with conn.cursor() as cur:
            cur.execute(sql)

        logger.info(f"Created table: {table}")

    def query(self, sql: str, params: tuple = None) -> list:
        """Execute a query and return results."""
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def close(self) -> None:
        """Close connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None
            self._connected = False
            logger.info("PostgreSQL connection closed")
