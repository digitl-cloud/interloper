"""Result types for operation and DAG execution."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from interloper.partitioning.base import Partition, PartitionWindow

if TYPE_CHECKING:
    from interloper.operation.base import OperationResult


class ExecutionStatus(str, Enum):
    """Execution status for operations and runs."""

    QUEUED = "queued"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELED = "canceled"


_TERMINAL_STATUSES = frozenset({
    ExecutionStatus.COMPLETED,
    ExecutionStatus.FAILED,
    ExecutionStatus.CANCELED,
    ExecutionStatus.SKIPPED,
})


@dataclass
class ExecutionInfo:
    """Execution information for a single operation.

    ``effects`` carries the executed operation's
    :class:`~interloper.operation.base.OperationResult` — the config and
    state fields the platform envelope persists after the run. ``None``
    until the node reaches a terminal state, and for skipped or canceled
    nodes, which never executed.
    """

    component_id: str
    component_key: str
    status: ExecutionStatus
    start_time: dt.datetime | None = None
    end_time: dt.datetime | None = None
    error: str | None = None
    traceback: str | None = None
    effects: OperationResult | None = None

    @property
    def execution_time(self) -> float | None:
        """Computed execution time in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    def mark_running(self) -> None:
        """Transition to RUNNING and record start time."""
        self.status = ExecutionStatus.RUNNING
        self.start_time = dt.datetime.now(dt.timezone.utc)

    def mark_completed(self) -> None:
        """Transition to COMPLETED and record end time."""
        self.status = ExecutionStatus.COMPLETED
        self.end_time = dt.datetime.now(dt.timezone.utc)

    def mark_failed(self, error: str, tb: str | None = None) -> None:
        """Transition to FAILED with an error message and optional traceback.

        Args:
            error: Error message describing the failure.
            tb: Formatted traceback string, or ``None`` when unavailable.
        """
        self.status = ExecutionStatus.FAILED
        self.end_time = dt.datetime.now(dt.timezone.utc)
        self.error = error
        self.traceback = tb

    def mark_canceled(self) -> None:
        """Transition to CANCELED and record end time."""
        self.status = ExecutionStatus.CANCELED
        self.end_time = dt.datetime.now(dt.timezone.utc)

    @property
    def is_terminal(self) -> bool:
        """Whether this asset has reached a final state.

        Returns:
            True if completed, failed, canceled, or skipped.
        """
        return self.status in _TERMINAL_STATUSES

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dict.

        Returns:
            A dict representation of this execution info.
        """
        return {
            "component_id": self.component_id,
            "component_key": self.component_key,
            "status": self.status.value,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "execution_time": self.execution_time,
            "error": self.error,
            "traceback": self.traceback,
        }


@dataclass
class RunResult:
    """Result of a single DAG execution (one partition, window, or unpartitioned)."""

    partition_or_window: Partition | PartitionWindow | None = None
    status: ExecutionStatus = ExecutionStatus.COMPLETED
    executions: dict[str, ExecutionInfo] = field(default_factory=dict)
    execution_time: float = 0.0

    @property
    def completed_ids(self) -> list[str]:
        """Ids of the operations that completed successfully."""
        return [k for k, v in self.executions.items() if v.status == ExecutionStatus.COMPLETED]

    @property
    def failed_ids(self) -> list[str]:
        """Ids of the operations that failed."""
        return [k for k, v in self.executions.items() if v.status == ExecutionStatus.FAILED]

    @property
    def canceled_ids(self) -> list[str]:
        """Ids of the operations that were canceled (downstream of a failure)."""
        return [k for k, v in self.executions.items() if v.status == ExecutionStatus.CANCELED]

    def __str__(self) -> str:
        """Human-friendly summary string.

        Returns:
            A formatted summary of this run result.
        """
        identifier: str
        if self.partition_or_window is None:
            identifier = "partition=None"
        elif isinstance(self.partition_or_window, PartitionWindow):
            identifier = f"window={self.partition_or_window}"
        else:
            identifier = f"partition={self.partition_or_window}"

        completed_count = len(self.completed_ids)
        failed_count = len(self.failed_ids)
        canceled_count = len(self.canceled_ids)

        parts: list[str] = [
            f"status={self.status.value}",
            identifier,
            f"completed={completed_count}",
            f"failed={failed_count}",
            f"canceled={canceled_count}",
            f"time={self.execution_time:.2f}s",
        ]

        if failed_count > 0:
            failed_preview = ", ".join(self.failed_ids[:5])
            if failed_count > 5:
                failed_preview += f" +{failed_count - 5} more"
            parts.append(f"failed=[{failed_preview}]")

        if canceled_count > 0:
            canceled_preview = ", ".join(self.canceled_ids[:5])
            if canceled_count > 5:
                canceled_preview += f" +{canceled_count - 5} more"
            parts.append(f"canceled=[{canceled_preview}]")

        return "RunResult(" + ", ".join(parts) + ")"
