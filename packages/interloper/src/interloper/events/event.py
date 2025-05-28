from dataclasses import dataclass
from enum import Enum

from interloper.execution.state import ExecutionStatus


class EventType(Enum):
    ASSET_EXECUTION = "ASSET_EXECUTION"
    ASSET_NORMALIZATION = "ASSET_NORMALIZATION"
    ASSET_WRITING = "ASSET_WRITING"
    ASSET_MATERIALIZATION = "ASSET_MATERIALIZATION"
    RUN = "RUN"
    EXECUTION = "EXECUTION"

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class Event:
    type: EventType
    status: ExecutionStatus
    error: Exception | None = None
