"""This module contains the Event and EventType classes."""
from dataclasses import dataclass
from enum import Enum

from interloper.execution.state import ExecutionStatus


class EventType(Enum):
    """The type of an event."""

    ASSET_EXECUTION = "ASSET_EXECUTION"
    ASSET_NORMALIZATION = "ASSET_NORMALIZATION"
    ASSET_WRITING = "ASSET_WRITING"
    ASSET_MATERIALIZATION = "ASSET_MATERIALIZATION"
    RUN = "RUN"
    EXECUTION = "EXECUTION"

    def __str__(self) -> str:
        """Return the string representation of the event type."""
        return self.value


@dataclass(frozen=True)
class Event:
    """An event that can be published to an event bus.

    Attributes:
        type: The type of the event.
        status: The status of the event.
        error: The error associated with the event, if any.
    """

    type: EventType
    status: ExecutionStatus
    error: Exception | None = None
