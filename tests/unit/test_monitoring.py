"""Unit tests for monitoring module."""

import pytest
from unittest.mock import patch, MagicMock


class TestTrackTime:
    """Tests for track_time context manager."""

    def test_track_time_records_duration(self):
        """Should record elapsed time."""
        from monitoring import track_time
        import time

        with track_time() as t:
            time.sleep(0.01)

        assert t["duration"] >= 0.01
        assert t["duration"] < 0.1  # Shouldn't take too long

    def test_track_time_handles_exception(self):
        """Should still record duration even if exception raised."""
        from monitoring import track_time

        t_result = None
        with pytest.raises(ValueError):
            with track_time() as t:
                t_result = t
                raise ValueError("test error")

        assert t_result is not None
        assert t_result["duration"] >= 0


class TestMetrics:
    """Tests for Metrics recorder."""

    def test_produce_success_increments_counter(self):
        """Should increment events_produced counter."""
        from monitoring.recorders import Metrics
        from monitoring.definitions import EVENTS_PRODUCED

        initial = EVENTS_PRODUCED.labels(
            event_type="test_event", status="success"
        )._value.get()
        Metrics.produce_success("test_event", latency=0.05)
        after = EVENTS_PRODUCED.labels(
            event_type="test_event", status="success"
        )._value.get()

        assert after == initial + 1

    def test_produce_error_increments_counter(self):
        """Should increment error counter."""
        from monitoring.recorders import Metrics
        from monitoring.definitions import EVENTS_PRODUCED

        initial = EVENTS_PRODUCED.labels(
            event_type="error_event", status="error"
        )._value.get()
        Metrics.produce_error("error_event", latency=0.1)
        after = EVENTS_PRODUCED.labels(
            event_type="error_event", status="error"
        )._value.get()

        assert after == initial + 1

    def test_produce_success_records_latency(self):
        """Should record latency in histogram."""
        from monitoring.recorders import Metrics
        from monitoring.definitions import PRODUCE_LATENCY

        initial_count = PRODUCE_LATENCY.labels(event_type="latency_test")._sum.get()
        Metrics.produce_success("latency_test", latency=0.25)
        after_count = PRODUCE_LATENCY.labels(event_type="latency_test")._sum.get()

        assert after_count >= initial_count + 0.25

    def test_sink_write_success(self):
        """Should record successful sink write."""
        from monitoring.recorders import Metrics
        from monitoring.definitions import SINK_WRITES, SINK_ROWS

        initial_writes = SINK_WRITES.labels(
            sink="test_sink", table="test_table", status="success"
        )._value.get()
        initial_rows = SINK_ROWS.labels(
            sink="test_sink", table="test_table"
        )._value.get()

        Metrics.sink_write(
            "test_sink", "test_table", rows=100, latency=0.5, success=True
        )

        after_writes = SINK_WRITES.labels(
            sink="test_sink", table="test_table", status="success"
        )._value.get()
        after_rows = SINK_ROWS.labels(sink="test_sink", table="test_table")._value.get()

        assert after_writes == initial_writes + 1
        assert after_rows == initial_rows + 100

    def test_sink_write_failure(self):
        """Should record failed sink write without incrementing rows."""
        from monitoring.recorders import Metrics
        from monitoring.definitions import SINK_WRITES, SINK_ROWS

        initial_writes = SINK_WRITES.labels(
            sink="fail_sink", table="fail_table", status="error"
        )._value.get()
        initial_rows = SINK_ROWS.labels(
            sink="fail_sink", table="fail_table"
        )._value.get()

        Metrics.sink_write("fail_sink", "fail_table", rows=50, success=False)

        after_writes = SINK_WRITES.labels(
            sink="fail_sink", table="fail_table", status="error"
        )._value.get()
        after_rows = SINK_ROWS.labels(sink="fail_sink", table="fail_table")._value.get()

        assert after_writes == initial_writes + 1
        assert after_rows == initial_rows  # Rows not incremented on failure

    def test_sink_write_without_latency(self):
        """Should work without latency parameter."""
        from monitoring.recorders import Metrics

        # Should not raise
        Metrics.sink_write("no_latency_sink", "table", rows=10)


class TestMetricsServer:
    """Tests for metrics server."""

    def test_start_metrics_server_idempotent(self):
        """Should only start server once."""
        from monitoring import server

        # Reset state
        server._started = False

        with patch.object(server, "start_http_server") as mock_start:
            server.start_metrics_server(port=9999)
            server.start_metrics_server(port=9999)
            server.start_metrics_server(port=9999)

            # Should only be called once
            assert mock_start.call_count == 1


class TestPrometheusQueryListener:
    """Tests for Spark query listener."""

    def test_on_query_started_logs_info(self):
        """Should log when query starts."""
        from monitoring.spark_listener import PrometheusQueryListener

        listener = PrometheusQueryListener()

        # Create mock event
        mock_event = MagicMock()
        mock_event.name = "test_query"
        mock_event.id = "123"

        # Should not raise
        listener.onQueryStarted(mock_event)

    def test_on_query_terminated_logs_info(self):
        """Should log when query terminates."""
        from monitoring.spark_listener import PrometheusQueryListener

        listener = PrometheusQueryListener()

        # Create mock event without exception
        mock_event = MagicMock()
        mock_event.name = "test_query"
        mock_event.id = "123"
        mock_event.exception = None

        # Should not raise
        listener.onQueryTerminated(mock_event)

    def test_on_query_terminated_with_error(self):
        """Should log error when query fails."""
        from monitoring.spark_listener import PrometheusQueryListener

        listener = PrometheusQueryListener()

        # Create mock event with exception
        mock_event = MagicMock()
        mock_event.name = "failed_query"
        mock_event.exception = "Connection lost"

        # Should not raise
        listener.onQueryTerminated(mock_event)

    def test_on_query_progress_updates_metrics(self):
        """Should update Spark metrics on progress."""
        from monitoring.spark_listener import PrometheusQueryListener
        from monitoring.definitions import SPARK_BATCHES

        listener = PrometheusQueryListener()

        # Create mock progress event
        mock_progress = MagicMock()
        mock_progress.name = "progress_test"
        mock_progress.id = "456"
        mock_progress.batchDuration = 1500  # 1.5 seconds in ms
        mock_progress.numInputRows = 1000
        mock_progress.inputRowsPerSecond = 500.0
        mock_progress.processedRowsPerSecond = 450.0

        mock_event = MagicMock()
        mock_event.progress = mock_progress

        initial = SPARK_BATCHES.labels(query="progress_test")._value.get()
        listener.onQueryProgress(mock_event)
        after = SPARK_BATCHES.labels(query="progress_test")._value.get()

        assert after == initial + 1
