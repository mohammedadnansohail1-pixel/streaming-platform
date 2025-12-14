"""Sink implementations for streaming data."""

from sinks.base import BaseSink
from sinks.clickhouse import ClickHouseSink

__all__ = [
    "BaseSink",
    "ClickHouseSink",
]
