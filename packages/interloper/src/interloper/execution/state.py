from dataclasses import dataclass
from enum import Enum


class ExecutionStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESSFUL = "SUCCESSFUL"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"


@dataclass
class ExecutionState:
    status: ExecutionStatus = ExecutionStatus.PENDING
    error: Exception | None = None

    def __repr__(self) -> str:
        return f"{self.status.value}{f'(error={self.error})' if self.error else ''}"
