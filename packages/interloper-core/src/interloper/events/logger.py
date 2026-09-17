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

    def __init__(
        self,
        component_key: str,
        metadata: dict[str, Any],
        component_id: str | None = None,
        component_kind: str = "asset",
        parent_id: str | None = None,
    ) -> None:
        """Initialize the logger.

        Args:
            component_key: Qualified key of the component that owns this logger.
            metadata: Run metadata included in every emitted ``LOG`` event.
            component_id: Id of the component that owns this logger. Carried on
                every emitted ``LOG`` event so it can be attributed to the
                component (e.g. filtered alongside its lifecycle events).
            component_kind: Kind of the owning component.
            parent_id: Id of the component that owns this one, if any.
        """
        self._component_key = component_key
        self._metadata = metadata
        self._component_id = component_id
        self._component_kind = component_kind
        self._parent_id = parent_id

    def _emit(self, level: int, message: str) -> None:
        """Emit a ``LOG`` event with the given level and message.

        Args:
            level: Standard :mod:`logging` level constant, carried on the event
                as its level name.
            message: The log message.
        """
        metadata: dict[str, Any] = {
            **self._metadata,
            "component_key": self._component_key,
            "message": message,
            "level": logging.getLevelName(level),
        }
        if self._component_id is not None:
            metadata["component_id"] = self._component_id
            metadata["component_kind"] = self._component_kind
        if self._parent_id is not None:
            metadata["parent_id"] = self._parent_id
        EventBus.emit(EventType.LOG, metadata=metadata)

    def debug(self, message: str) -> None:
        """Emit a debug-level log event.

        Args:
            message: The log message.
        """
        self._emit(logging.DEBUG, message)

    def info(self, message: str) -> None:
        """Emit an info-level log event.

        Args:
            message: The log message.
        """
        self._emit(logging.INFO, message)

    def warning(self, message: str) -> None:
        """Emit a warning-level log event.

        Args:
            message: The log message.
        """
        self._emit(logging.WARNING, message)

    def error(self, message: str) -> None:
        """Emit an error-level log event.

        Args:
            message: The log message.
        """
        self._emit(logging.ERROR, message)
