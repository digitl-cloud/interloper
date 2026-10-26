"""Result types for operation and DAG execution."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from interloper.operation.base import Effect
from interloper.partitioning.base import PartitionWindow

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


_TERMINAL_STATUSES = frozenset(
    {
        ExecutionStatus.COMPLETED,
        ExecutionStatus.FAILED,
        ExecutionStatus.CANCELED,
        ExecutionStatus.SKIPPED,
    }
)


@dataclass
class ExecutionInfo:
    """One operation's record in a run: its status, its attempt, when it ran, and its result.

    ``result`` is set once the operation finished, either way, and carries
    what the platform persists (its effects) and what failed (its error,
    traceback and, in memory only, its exception). It stays ``None`` for an
    operation skipped, canceled or still running.
    """

    component_id: str
    component_key: str
    status: ExecutionStatus
    attempt: int = 1
    start_time: dt.datetime | None = None
    end_time: dt.datetime | None = None
    result: OperationResult | None = None

    @property
    def error(self) -> str | None:
        """The failure's message, ``None`` unless the operation failed."""
        return self.result.error if self.result is not None else None

    @property
    def traceback(self) -> str | None:
        """The failure's traceback, when one was captured."""
        return self.result.traceback if self.result is not None else None

    @property
    def execution_time(self) -> float | None:
        """Seconds between start and end, ``None`` until both are known."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    @property
    def is_terminal(self) -> bool:
        """Whether the operation reached a final status: completed, failed, canceled or skipped."""
        return self.status in _TERMINAL_STATUSES

    def start(self) -> None:
        """Mark the operation running, from now."""
        self.status = ExecutionStatus.RUNNING
        self.start_time = dt.datetime.now(dt.timezone.utc)

    def finish(self, result: OperationResult) -> None:
        """Record what the operation came to: completed when it succeeded, failed otherwise.

        Args:
            result: The operation's result.
        """
        self.status = ExecutionStatus.COMPLETED if result.succeeded else ExecutionStatus.FAILED
        self.end_time = dt.datetime.now(dt.timezone.utc)
        self.result = result

    def cancel(self) -> None:
        """Mark the operation canceled, from now."""
        self.status = ExecutionStatus.CANCELED
        self.end_time = dt.datetime.now(dt.timezone.utc)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the record to a plain dict; the result's effects and exception stay in the process.

        Returns:
            The record as JSON-able values.
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

    window: PartitionWindow | None = None
    status: ExecutionStatus = ExecutionStatus.COMPLETED
    executions: dict[str, ExecutionInfo] = field(default_factory=dict)
    execution_time: float = 0.0

    @property
    def effects(self) -> dict[str, Effect]:
        """The effects the platform persists, by component id: every finished execution's non-empty ones."""
        return {
            component_id: Effect(config=dict(info.result.config), state=dict(info.result.state))
            for component_id, info in self.executions.items()
            if info.result is not None and (info.result.config or info.result.state)
        }

    @property
    def completed_ids(self) -> list[str]:
        """Ids of the operations that completed successfully."""
        return [
            operation_id
            for operation_id, execution in self.executions.items()
            if execution.status == ExecutionStatus.COMPLETED
        ]

    @property
    def failed_ids(self) -> list[str]:
        """Ids of the operations that failed."""
        return [
            operation_id
            for operation_id, execution in self.executions.items()
            if execution.status == ExecutionStatus.FAILED
        ]

    @property
    def canceled_ids(self) -> list[str]:
        """Ids of the operations that were canceled (downstream of a failure)."""
        return [
            operation_id
            for operation_id, execution in self.executions.items()
            if execution.status == ExecutionStatus.CANCELED
        ]

    def __str__(self) -> str:
        """Human-friendly summary string.

        Returns:
            A formatted summary of this run result.
        """
        completed_count = len(self.completed_ids)
        failed_count = len(self.failed_ids)
        canceled_count = len(self.canceled_ids)

        parts: list[str] = [
            f"status={self.status.value}",
            f"window={self.window}",
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
