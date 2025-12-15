"""Dead Letter Queue module for failed event handling."""

from dlq.handler import DeadLetterQueue, DLQEvent

__all__ = ["DeadLetterQueue", "DLQEvent"]
