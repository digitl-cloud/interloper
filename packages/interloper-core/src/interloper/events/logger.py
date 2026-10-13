"""Event-based logger emitting LOG events on the event bus."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from interloper.events.bus import EventBus
from interloper.events.types import EventType

if TYPE_CHECKING:
    from interloper.component.base import Component
    from interloper.partitioning.base import PartitionWindow


class EventLogger:
    """Logger that emits messages as ``LOG`` events attributed to one component.

    Provides the familiar ``debug`` / ``info`` / ``warning`` / ``error`` interface; each call
    emits an ``EventType.LOG`` event carrying the component's identity, the run metadata and the
    window, built through `Component.event_metadata`, plus ``level`` and ``message``.

    ```py
    context.logger.info("Fetched 142 records")
    ```
    """

    def __init__(
        self,
        component: Component,
        metadata: Mapping[str, Any],
        window: PartitionWindow | None = None,
    ) -> None:
        """Bind the logger to a component and a run.

        Args:
            component: The component the events are attributed to.
            metadata: Run metadata carried on every event.
            window: The window the run covers, carried on every event; ``None`` when unscoped.
        """
        self._component = component
        self._metadata = metadata
        self._window = window

    def _emit(self, level: int, message: str) -> None:
        """Emit a ``LOG`` event at a level.

        Args:
            level: Standard `logging` level constant, carried on the event as its level name.
            message: The log message.
        """
        metadata = self._component.event_metadata(
            self._metadata, self._window, level=logging.getLevelName(level), message=message
        )
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
