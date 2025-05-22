from dataclasses import dataclass
from enum import Enum, auto


class ExecutionStatus(Enum):
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    BLOCKED = auto()


@dataclass
class ExecutionState:
    status: ExecutionStatus = ExecutionStatus.PENDING
    error: Exception | None = None
