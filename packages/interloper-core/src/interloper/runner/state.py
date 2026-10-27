"""RunState: the state machine of a run's operations."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from interloper.events import Event, EventBus, EventType
from interloper.operation.base import OperationContext
from interloper.runner.results import ExecutionInfo, ExecutionStatus

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.operation.base import Operation, OperationResult
    from interloper.partitioning.base import PartitionWindow


class RunState:
    """The operations of one run: their statuses, attempts and results, and the events their transitions emit.

    Every transition is one verb, and emits its operation-lifecycle event with
    a deterministic id. The run that walks the DAG is the only author of its
    operations' lifecycle, wherever an attempt executes: a container's child
    reports only what happens inside its operation.

    Event ids derive from ``(run_id, component_id, event_type, attempt)``, so
    a verdict the store authors for an operation its run left open collapses
    with the same verdict arriving late from a stopping pod.

    Every transition happens on the run's event loop thread, so no locking is
    required.
    """

    _OPERATION_EVENT_NAMESPACE = uuid.UUID("a3f1c2d4-5b6e-4a7c-9d8f-0e1a2b3c4d5e")

    def __init__(
        self,
        dag: DAG,
        window: PartitionWindow | None,
        metadata: dict[str, Any],
    ) -> None:
        """Track a DAG's operations for one run, roots ready.

        Args:
            dag: The DAG the run walks.
            window: The window the run covers, ``None`` when unscoped.
            metadata: Run-level metadata, carrying the ``run_id``.
        """
        self.dag = dag
        self.window = window
        self.metadata = metadata
        self.executions: dict[str, ExecutionInfo] = {
            operation.id: ExecutionInfo(
                component_id=operation.id,
                component_key=operation.key,
                status=ExecutionStatus.QUEUED if operation.enabled else ExecutionStatus.SKIPPED,
            )
            for operation in dag.operations
        }
        for operation in dag.operations:
            self._promote(operation.id)

    # -- Views -----------------------------------------------------------------

    @property
    def run_id(self) -> str:
        """The run's id, from its metadata."""
        return self.metadata["run_id"]

    @property
    def ready(self) -> list[Operation]:
        """The operations whose dependencies are met and that have not started, in DAG order."""
        return self._with_status(ExecutionStatus.READY)

    @property
    def completed(self) -> list[Operation]:
        """The operations that completed, in DAG order."""
        return self._with_status(ExecutionStatus.COMPLETED)

    @property
    def failed(self) -> list[Operation]:
        """The operations that failed, in DAG order."""
        return self._with_status(ExecutionStatus.FAILED)

    @property
    def done(self) -> bool:
        """Whether every operation reached a final status."""
        return all(info.is_terminal for info in self.executions.values())

    def context(self, operation: Operation) -> OperationContext:
        """The facts an operation executes with: its own window, the DAG and the run's metadata.

        Args:
            operation: The operation about to execute.

        Returns:
            Its context.
        """
        return OperationContext(window=operation.effective_window(self.window), dag=self.dag, metadata=self.metadata)

    # -- Transitions -------------------------------------------------------------

    def queue(self) -> None:
        """Announce every operation that will execute (``OPERATION_QUEUED``)."""
        for operation in self.dag.operations:
            if self.executions[operation.id].status in (ExecutionStatus.QUEUED, ExecutionStatus.READY):
                self._emit(EventType.OPERATION_QUEUED, operation, f"Operation '{operation.key}' queued")

    def start(self, operation: Operation) -> None:
        """Mark an operation's attempt running (``OPERATION_STARTED``).

        Args:
            operation: The operation starting an attempt.
        """
        self.executions[operation.id].start()
        self._emit(EventType.OPERATION_STARTED, operation, f"Operation '{operation.key}' started")

    def retry(self, operation: Operation, error: str) -> None:
        """Record a failed attempt that will be retried (``OPERATION_RETRIED``), and count the next one.

        A retried attempt is not a verdict: the operation keeps its status,
        nothing downstream is canceled, and ``OPERATION_FAILED`` stays reserved
        for an exhausted budget. Counting the next attempt is what makes its
        events distinct from this one's.

        Args:
            operation: The operation whose attempt failed.
            error: The attempt's error.
        """
        info = self.executions[operation.id]
        message = f"Operation '{operation.key}' failed on attempt {info.attempt}, retrying: {error}"
        self._emit(EventType.OPERATION_RETRIED, operation, message, error=error)
        info.attempt += 1

    def finish(self, operation: Operation, result: OperationResult) -> None:
        """Record what an operation came to.

        Completed: its dependents whose every predecessor is done become ready
        (``OPERATION_COMPLETED``). Failed: its dependents are canceled
        (``OPERATION_FAILED``, then ``OPERATION_CANCELED`` for each).

        Args:
            operation: The operation that finished.
            result: What it came to.
        """
        self.executions[operation.id].finish(result)
        if result.succeeded:
            for successor_id in self.dag.successors.get(operation.id, []):
                self._promote(successor_id)
            self._emit(EventType.OPERATION_COMPLETED, operation, f"Operation '{operation.key}' completed")
            return
        canceled = self._cancel_dependents(operation.id)
        extra = {"traceback": result.traceback} if result.traceback else {}
        message = f"Operation '{operation.key}' failed: {result.error}"
        self._emit(EventType.OPERATION_FAILED, operation, message, error=result.error, **extra)
        for dependent in canceled:
            message = f"Operation '{dependent.key}' canceled (upstream failure)"
            self._emit(EventType.OPERATION_CANCELED, dependent, message)

    def cancel_pending(self) -> None:
        """Cancel every operation that never reached a final status (``OPERATION_CANCELED``).

        Called when the walk ends before the DAG is exhausted (a fail-fast stop
        or a fault of the runner): without it, operations never started would
        stay non-terminal forever.
        """
        for operation in self.dag.operations:
            info = self.executions[operation.id]
            if not info.is_terminal:
                info.cancel()
                self._emit(EventType.OPERATION_CANCELED, operation, f"Operation '{operation.key}' canceled")

    # -- Event ids ---------------------------------------------------------------

    @staticmethod
    def operation_event_id(run_id: str, component_id: str, event_type: EventType, attempt: int = 1) -> str:
        """Derive a deterministic event id from a run/component/type triple.

        The store derives the same ids when it closes the operations a run
        left open, so a late event from a stopping pod dedups.

        Args:
            run_id: Id of the run the event belongs to.
            component_id: Id of the operation the event is about.
            event_type: The operation-lifecycle event type.
            attempt: The attempt the event belongs to, so a retried node's
                events are their own rows rather than a dedup of the first
                attempt's. Left out of the key when it is 1, which keeps every
                id written before retries existed unchanged.

        Returns:
            A stable UUID5 string for the event.
        """
        key = f"{run_id}:{component_id}:{event_type.value}"
        if attempt > 1:
            key = f"{key}:{attempt}"
        return str(uuid.uuid5(RunState._OPERATION_EVENT_NAMESPACE, key))

    # -- Internals -----------------------------------------------------------------

    def _with_status(self, status: ExecutionStatus) -> list[Operation]:
        """The operations with a status, in DAG order.

        Args:
            status: The status to select.

        Returns:
            The matching operations.
        """
        return [operation for operation in self.dag.operations if self.executions[operation.id].status is status]

    def _promote(self, operation_id: str) -> None:
        """Make a queued operation ready once every predecessor completed or was skipped.

        Args:
            operation_id: The operation to consider.
        """
        info = self.executions[operation_id]
        if info.status is not ExecutionStatus.QUEUED:
            return
        predecessors = self.dag.predecessors.get(operation_id, [])
        done = (ExecutionStatus.COMPLETED, ExecutionStatus.SKIPPED)
        if all(self.executions[predecessor].status in done for predecessor in predecessors):
            info.status = ExecutionStatus.READY

    def _cancel_dependents(self, operation_id: str) -> list[Operation]:
        """Cancel everything transitively downstream of a failed operation that has not finished.

        Args:
            operation_id: The failed operation.

        Returns:
            The canceled operations, depth first.
        """
        canceled: list[Operation] = []
        for successor_id in self.dag.successors.get(operation_id, []):
            info = self.executions[successor_id]
            if info.is_terminal:
                continue
            info.cancel()
            canceled.append(self.dag.operation_map[successor_id])
            canceled.extend(self._cancel_dependents(successor_id))
        return canceled

    def _emit(self, event_type: EventType, operation: Operation, message: str, **extra: Any) -> None:
        """Emit an operation-lifecycle event with its deterministic id.

        Args:
            event_type: The lifecycle event.
            operation: The operation it is about.
            message: The event's message.
            **extra: More metadata (``error``, ``traceback``).
        """
        attempt = self.executions[operation.id].attempt
        metadata = {
            **operation.event_metadata(self.metadata, self.window, attempt=attempt, **extra),
            "message": message,
        }
        event_id = self.operation_event_id(self.run_id, operation.id, event_type, attempt=attempt)
        EventBus.emit_event(Event(type=event_type, metadata=metadata, id=event_id))
