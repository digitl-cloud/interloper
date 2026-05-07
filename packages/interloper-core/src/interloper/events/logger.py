"""Event-based logger emitting LOG events on the event bus."""

from __future__ import annotations

import logging
from typing import Any

from interloper.events.bus import EventBus
from interloper.events.types import EventType


class EventLogger:
    """Logger that emits messages as ``LOG`` events on the event bus.

    Provides a familiar logging interface (debug/info/warning/error) where
    each call emits an ``EventType.LOG`` event visible to all event handlers.
    Log levels use the standard :mod:`logging` module constants.

    Usage::

        context.logger.info("Fetched 142 records")
        context.logger.warning("Rate limited, retrying...")
    """

    def __init__(self, asset_key: str, metadata: dict[str, Any]) -> None:
        """Initialize the logger.

        Args:
            asset_key: Qualified key of the asset that owns this logger.
            metadata: Run metadata included in every emitted ``LOG`` event.
        """
        self._asset_key = asset_key
        self._metadata = metadata

    def _emit(self, level: int, message: str) -> None:
        """Emit a ``LOG`` event with the given level and message."""
        EventBus.emit(EventType.LOG, metadata={
            **self._metadata,
            "asset_key": self._asset_key,
            "message": message,
            "level": logging.getLevelName(level),
        })

    def debug(self, message: str) -> None:
        """Emit a debug-level log event."""
        self._emit(logging.DEBUG, message)

    def info(self, message: str) -> None:
        """Emit an info-level log event."""
        self._emit(logging.INFO, message)

    def warning(self, message: str) -> None:
        """Emit a warning-level log event."""
        self._emit(logging.WARNING, message)

    def error(self, message: str) -> None:
        """Emit an error-level log event."""
        self._emit(logging.ERROR, message)
