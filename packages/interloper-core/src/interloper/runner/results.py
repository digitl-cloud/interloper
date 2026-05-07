"""Result types for asset and DAG execution."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from interloper.partitioning.base import Partition, PartitionWindow


class ExecutionStatus(str, Enum):
    """Execution status for assets and runs."""

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
class AssetExecutionInfo:
    """Execution information for a single asset."""

    asset_id: str
    asset_key: str
    status: ExecutionStatus
    start_time: dt.datetime | None = None
    end_time: dt.datetime | None = None
    error: str | None = None
    traceback: str | None = None

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
        """Transition to FAILED with an error message and optional traceback."""
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
            "asset_id": self.asset_id,
            "asset_key": self.asset_key,
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
    asset_executions: dict[str, AssetExecutionInfo] = field(default_factory=dict)
    execution_time: float = 0.0

    @property
    def completed_assets(self) -> list[str]:
        """List of asset keys that completed successfully."""
        return [k for k, v in self.asset_executions.items() if v.status == ExecutionStatus.COMPLETED]

    @property
    def failed_assets(self) -> list[str]:
        """List of asset keys that failed."""
        return [k for k, v in self.asset_executions.items() if v.status == ExecutionStatus.FAILED]

    @property
    def canceled_assets(self) -> list[str]:
        """List of asset keys that were canceled (downstream of a failure)."""
        return [k for k, v in self.asset_executions.items() if v.status == ExecutionStatus.CANCELED]

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

        completed_count = len(self.completed_assets)
        failed_count = len(self.failed_assets)
        canceled_count = len(self.canceled_assets)

        parts: list[str] = [
            f"status={self.status.value}",
            identifier,
            f"completed={completed_count}",
            f"failed={failed_count}",
            f"canceled={canceled_count}",
            f"time={self.execution_time:.2f}s",
        ]

        if failed_count > 0:
            failed_preview = ", ".join(self.failed_assets[:5])
            if failed_count > 5:
                failed_preview += f" +{failed_count - 5} more"
            parts.append(f"failed_assets=[{failed_preview}]")

        if canceled_count > 0:
            canceled_preview = ", ".join(self.canceled_assets[:5])
            if canceled_count > 5:
                canceled_preview += f" +{canceled_count - 5} more"
            parts.append(f"canceled_assets=[{canceled_preview}]")

        return "RunResult(" + ", ".join(parts) + ")"
