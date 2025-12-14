"""Centralized logging configuration."""

import logging
import sys
from typing import Optional


def setup_logging(
    level: str = "INFO", format_style: str = "standard", log_file: Optional[str] = None
) -> logging.Logger:
    """
    Configure logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        format_style: 'standard' for dev, 'json' for production
        log_file: Optional file path to write logs

    Returns:
        Root logger
    """
    # Format options
    if format_style == "json":
        # Production - structured logging
        fmt = '{"time": "%(asctime)s", "level": "%(levelname)s", "module": "%(name)s", "message": "%(message)s"}'
    else:
        # Development - human readable
        fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"

    # Configure root logger
    handlers = [logging.StreamHandler(sys.stdout)]

    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
        force=True,  # Override any existing config
    )

    return logging.getLogger()


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger for a module.

    Usage:
        from core.utils.logging import get_logger
        logger = get_logger(__name__)
        logger.info("Something happened")
    """
    return logging.getLogger(name)
