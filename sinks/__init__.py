"""Sink implementations for streaming data."""

from sinks.base import BaseSink
from sinks.clickhouse import ClickHouseSink
from sinks.postgresql import PostgreSQLSink

__all__ = [
    "BaseSink",
    "ClickHouseSink",
    "PostgreSQLSink",
]
