"""Abstract base sink for streaming data."""

from abc import ABC, abstractmethod

from core.utils.logging import get_logger

logger = get_logger(__name__)


class BaseSink(ABC):
    """
    Abstract base class for all sinks.

    Subclasses must implement:
        - _connect()
        - _write_rows()
        - health_check()
        - close()
    """

    def __init__(self, config: dict, sink_name: str):
        self.config = config
        self.sink_name = sink_name
        self._connected = False

    @abstractmethod
    def _connect(self) -> None:
        """Establish connection to sink."""
        pass

    @abstractmethod
    def _write_rows(self, table: str, rows: list[tuple], columns: list[str]) -> None:
        """Write rows to table."""
        pass

    @abstractmethod
    def health_check(self) -> bool:
        """Check if sink is reachable."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close connection."""
        pass

    def write_batch(
        self, batch_df, batch_id: int, table: str, column_mapping: dict
    ) -> int:
        """
        Generic batch writer for Spark foreachBatch.

        Args:
            batch_df: Spark DataFrame from foreachBatch
            batch_id: Batch identifier
            table: Target table name
            column_mapping: Maps DataFrame columns to table columns
                           {"df_col": "table_col"} or {"window.start": "window_start"}

        Returns:
            Number of rows written
        """
        from monitoring import Metrics, track_time

        if batch_df.isEmpty():
            return 0

        # Extract rows from DataFrame
        rows = []
        df_columns = list(column_mapping.keys())
        table_columns = list(column_mapping.values())

        for row in batch_df.collect():
            values = []
            for df_col in df_columns:
                # Handle nested fields like "window.start"
                if "." in df_col:
                    parts = df_col.split(".")
                    val = row[parts[0]]
                    for part in parts[1:]:
                        val = val[part]
                    values.append(val)
                else:
                    values.append(row[df_col])
            rows.append(tuple(values))

        if not rows:
            return 0

        # Write with metrics
        with track_time() as t:
            self._write_rows(table, rows, table_columns)

        Metrics.sink_write(self.sink_name, table, len(rows), t["duration"])
        logger.info(f"Batch {batch_id}: Wrote {len(rows)} rows to {table}")

        return len(rows)

    @abstractmethod
    def create_table(self, table: str, schema: dict) -> None:
        """Create table from schema definition."""
        pass
