"""This module contains the execution state classes."""
from dataclasses import dataclass
from enum import Enum


class ExecutionStatus(Enum):
    """The status of an execution."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESSFUL = "SUCCESSFUL"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"


@dataclass
class ExecutionState:
    """The state of an execution.

    Attributes:
        status: The status of the execution.
        error: The error of the execution.
    """

    status: ExecutionStatus = ExecutionStatus.PENDING
    error: Exception | None = None

    def __repr__(self) -> str:
        """Return a string representation of the execution state."""
        return f"{self.status.value}{f'(error={self.error})' if self.error else ''}"
