"""Prometheus HTTP server."""

from prometheus_client import start_http_server

from core.utils.logging import get_logger

logger = get_logger(__name__)

_started = False


def start_metrics_server(port: int = 8000) -> None:
    """
    Start Prometheus HTTP server (idempotent).

    Usage:
        from monitoring import start_metrics_server

        start_metrics_server(port=8000)
        # Metrics available at http://localhost:8000/metrics
    """
    global _started
    if not _started:
        start_http_server(port)
        _started = True
        logger.info(f"Metrics server started on :{port}/metrics")
