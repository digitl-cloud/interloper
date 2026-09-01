"""Run state tracking and dynamic operation scheduling."""

from __future__ import annotations

import datetime as dt
import uuid
from typing import TYPE_CHECKING, Any

from interloper.events import Event, EventBus, EventType
from interloper.runner.results import ExecutionInfo, ExecutionStatus

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.operation.base import Operation, OperationResult
    from interloper.partitioning.base import Partition, PartitionWindow


# Fixed namespace for deterministic operation-event ids. A given lifecycle
# event is uniquely identified by ``(run_id, component_id, event_type)``;
# deriving the event id from that triple makes the *same* logical event
# collapse to a single row when it is produced more than once — e.g. a child
# container emits its own ``operation_failed`` and the host also authors one
# as a fallback, or the host bulk-emits ``operation_queued`` and the child
# re-emits it. Combined with the idempotent (``ON CONFLICT (id) DO NOTHING``)
# event save, this dedups without any cross-process coordination.
class RunState:
    """Tracks operation execution state and decides which operations are ready.

    Used by runners to dynamically schedule operations based on dependency
    completion rather than static DAG levels.

    All state mutations occur on the asyncio event loop's single thread,
    so no locking is required.
    """

    _OPERATION_EVENT_NS = uuid.UUID("a3f1c2d4-5b6e-4a7c-9d8f-0e1a2b3c4d5e")

    def __init__(
        self,
        dag: DAG,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the run state.

        Args:
            dag: The DAG to track.
            metadata: Arbitrary metadata (e.g. run_id, backfill_id).
                A ``run_id`` is generated automatically if not provided.
        """
        self.dag = dag
        self.metadata: dict[str, Any] = metadata or {}
        if "run_id" not in self.metadata:
            self.metadata["run_id"] = str(uuid.uuid4())

        self.executions: dict[str, ExecutionInfo] = {}
        self.partition_or_window: Partition | PartitionWindow | None = None
        self.start_time: dt.datetime | None = None
        self.end_time: dt.datetime | None = None

        self._initialize_operations()

    # -- Properties ------------------------------------------------------------

    @property
    def run_id(self) -> str:
        """The run ID."""
        return self.metadata["run_id"]

    @property
    def backfill_id(self) -> str | None:
        """The backfill ID, if set."""
        return self.metadata.get("backfill_id")

    @property
    def elapsed_time(self) -> float | None:
        """Elapsed wall-clock time of the run in seconds, or None if not finished."""
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    @property
    def queued_operations(self) -> list[Operation]:
        """List of operations waiting to be scheduled."""
        return self._operations_with_status(ExecutionStatus.QUEUED)

    @property
    def ready_operations(self) -> list[Operation]:
        """List of operations whose dependencies are met and can be executed."""
        return self._operations_with_status(ExecutionStatus.READY)

    @property
    def running_operations(self) -> list[Operation]:
        """List of operations currently being executed."""
        return self._operations_with_status(ExecutionStatus.RUNNING)

    @property
    def completed_operations(self) -> list[Operation]:
        """List of operations that completed successfully."""
        return self._operations_with_status(ExecutionStatus.COMPLETED)

    @property
    def failed_operations(self) -> list[Operation]:
        """List of operations that failed."""
        return self._operations_with_status(ExecutionStatus.FAILED)

    # -- Run lifecycle ---------------------------------------------------------

    def start_run(self, partition_or_window: Partition | PartitionWindow | None) -> None:
        """Record the run start time and emit RUN_STARTED + OPERATION_QUEUED events.

        Args:
            partition_or_window: Partition or window the run is scoped to, or
                ``None`` for an unpartitioned run. Stamped on every event this
                state emits.
        """
        self.partition_or_window = partition_or_window
        self.start_time = dt.datetime.now(dt.timezone.utc)
        self.end_time = None

        EventBus.emit(
            EventType.RUN_STARTED,
            metadata={
                **self.metadata,
                "partition_or_window": str(self.partition_or_window) if self.partition_or_window else None,
                "message": f"Run started ({len(self.dag.operations)} operations)",
            },
        )

        for operation in self.dag.operations:
            info = self.executions[operation.id]
            if info.status in (ExecutionStatus.QUEUED, ExecutionStatus.READY):
                self._emit_operation_event(
                    EventType.OPERATION_QUEUED,
                    {
                        **self._operation_event_metadata(operation),
                        "message": f"Operation '{operation.key}' queued",
                    },
                )

    def end_run(
        self,
        status: ExecutionStatus,
        error: str | None = None,
    ) -> dict[str, ExecutionInfo]:
        """Record the run end time, emit a terminal event, and return the executions.

        Args:
            status: Terminal status of the run; anything other than
                ``COMPLETED`` emits ``RUN_FAILED``.
            error: Run-level error message included in the terminal event, or
                ``None`` when there is no specific error to report.

        Returns:
            A copy of the execution info dictionary.
        """
        self.end_time = dt.datetime.now(dt.timezone.utc)

        event_type = EventType.RUN_COMPLETED if status == ExecutionStatus.COMPLETED else EventType.RUN_FAILED
        if status == ExecutionStatus.COMPLETED:
            message = f"Run completed ({len(self.completed_operations)}/{len(self.dag.operations)} succeeded)"
        else:
            failed_count = len(self.failed_operations)
            message = f"Run failed: {error}" if error else f"Run failed ({failed_count} operation(s) failed)"

        EventBus.emit(
            event_type,
            metadata={
                **self.metadata,
                "partition_or_window": str(self.partition_or_window) if self.partition_or_window else None,
                "error": error,
                "message": message,
            },
        )

        return self.executions.copy()

    def is_run_complete(self) -> bool:
        """Check whether every operation has reached a terminal state.

        Returns:
            True if all operations are completed, failed, canceled, or skipped.
        """
        return all(info.is_terminal for info in self.executions.values())

    # -- Operation state transitions ---------------------------------------------

    def mark_running(self, operation: Operation, *, emit: bool = True) -> None:
        """Transition an operation to RUNNING.

        Args:
            operation: The operation that started.
            emit: Emit ``OPERATION_STARTED`` on the EventBus.  Set to
                ``False`` for cross-process runners where the child
                process emits the event itself.
        """
        self.executions[operation.id].mark_running()

        if emit:
            self._emit_operation_event(
                EventType.OPERATION_STARTED,
                {
                    **self._operation_event_metadata(operation),
                    "message": f"Operation '{operation.key}' started",
                },
            )

    def mark_completed(
        self, operation: Operation, *, emit: bool = True, effects: OperationResult | None = None
    ) -> None:
        """Transition an operation to COMPLETED and promote ready dependents.

        Args:
            operation: The operation that completed.
            emit: Emit ``OPERATION_COMPLETED`` on the EventBus.  Set to
                ``False`` for cross-process runners where the child
                process emits the event itself.
            effects: The operation's returned effects, recorded on the
                execution info for the platform to persist.
        """
        self.executions[operation.id].mark_completed()
        self.executions[operation.id].effects = effects
        self._promote_dependents(operation.id)

        if emit:
            self._emit_operation_event(
                EventType.OPERATION_COMPLETED,
                {
                    **self._operation_event_metadata(operation),
                    "message": f"Operation '{operation.key}' completed",
                },
            )

    def mark_canceled(self, operation: Operation, *, emit: bool = True) -> None:
        """Transition an operation to CANCELED.

        Args:
            operation: The operation that was canceled.
            emit: Emit ``OPERATION_CANCELED`` on the EventBus.  Set to
                ``False`` for cross-process runners where the child
                process emits the event itself.
        """
        self.executions[operation.id].mark_canceled()

        if emit:
            self._emit_operation_event(
                EventType.OPERATION_CANCELED,
                {
                    **self._operation_event_metadata(operation),
                    "message": f"Operation '{operation.key}' canceled",
                },
            )

    def mark_failed(
        self,
        operation: Operation,
        error: str,
        tb: str | None = None,
        *,
        emit: bool = True,
        effects: OperationResult | None = None,
        exception: Exception | None = None,
    ) -> None:
        """Transition an operation to FAILED and cancel downstream dependents.

        Args:
            operation: The operation that failed.
            error: Error message describing the failure.
            tb: Optional formatted traceback string.
            emit: Emit ``OPERATION_FAILED`` and ``OPERATION_CANCELED`` events
                on the EventBus.  Set to ``False`` for cross-process runners
                where the child process emits the events itself.
            effects: The operation's failure effects, recorded on the
                execution info for the platform to persist.
            exception: The original exception, kept in memory only so
                ``reraise`` can re-raise it faithfully; ``None`` when the
                failure happened in another process.
        """
        self.executions[operation.id].mark_failed(error, tb=tb, exception=exception)
        self.executions[operation.id].effects = effects
        canceled = self._propagate_failure(operation.id)

        if emit:
            metadata: dict[str, Any] = {
                **self._operation_event_metadata(operation),
                "error": error,
                "message": f"Operation '{operation.key}' failed: {error}",
            }
            if tb:
                metadata["traceback"] = tb
            self._emit_operation_event(EventType.OPERATION_FAILED, metadata)

            for key in canceled:
                canceled_operation = self.dag.operation_map[key]
                self._emit_operation_event(
                    EventType.OPERATION_CANCELED,
                    {
                        **self._operation_event_metadata(canceled_operation),
                        "message": (
                            f"Operation '{type(self.dag.operation_map[key]).key}' canceled (upstream failure)"
                        ),
                    },
                )

    # -- Internals -------------------------------------------------------------

    def _initialize_operations(self) -> None:
        """Initialize all operations as QUEUED, then promote root operations to READY."""
        for operation in self.dag.operations:
            status = ExecutionStatus.SKIPPED if not operation.materializable else ExecutionStatus.QUEUED
            self.executions[operation.id] = ExecutionInfo(
                component_id=operation.id,
                component_key=operation.key,
                status=status,
            )

        # Promote operations whose predecessors are all skipped (or empty)
        for operation in self.dag.operations:
            info = self.executions[operation.id]
            if info.status != ExecutionStatus.QUEUED:
                continue
            preds = self.dag.predecessors.get(operation.id, [])
            if all(self.executions[p].status == ExecutionStatus.SKIPPED for p in preds):
                info.status = ExecutionStatus.READY

    def _operations_with_status(self, status: ExecutionStatus) -> list[Operation]:
        """Return all operations matching the given execution status.

        Args:
            status: The execution status to filter on.

        Returns:
            Operations whose current status equals ``status``, in DAG order.
        """
        return [operation for operation in self.dag.operations if self.executions[operation.id].status == status]

    def _operation_event_metadata(self, operation: Operation) -> dict[str, Any]:
        """Build event metadata for an operation state transition.

        Args:
            operation: The operation the event is about.

        Returns:
            Dictionary of metadata keys for the event.
        """
        meta: dict[str, Any] = {
            **self.metadata,
            "component_id": operation.id,
            "component_kind": operation.kind,
            "component_key": operation.key,
            "partition_or_window": str(self.partition_or_window) if self.partition_or_window else None,
        }
        if operation.source is not None:
            meta["source_id"] = operation.source.id
        return meta

    @staticmethod
    def _operation_event_id(run_id: str, component_id: str, event_type: EventType) -> str:
        """Derive a deterministic event id from a run/component/type triple.

        Both the host and the in-container child run this same code with the
        same ``run_id`` (passed via ``--run-id``) and ``component_id`` (carried
        in the mini-DAG spec), so they compute identical ids and their events
        dedup.

        Args:
            run_id: Id of the run the event belongs to.
            component_id: Id of the operation the event is about.
            event_type: The operation-lifecycle event type.

        Returns:
            A stable UUID5 string for the event.
        """
        return str(uuid.uuid5(RunState._OPERATION_EVENT_NS, f"{run_id}:{component_id}:{event_type.value}"))

    def _emit_operation_event(self, event_type: EventType, metadata: dict[str, Any]) -> None:
        """Emit an operation-lifecycle event with a deterministic id.

        The id is derived from ``(run_id, component_id, event_type)`` so the
        same logical event dedups across producers (host fallback vs child, or
        the duplicate ``operation_queued``).  ``metadata`` must carry
        ``component_id``.

        Args:
            event_type: The operation-lifecycle event type to emit.
            metadata: Event metadata, as built by ``_operation_event_metadata``;
                must carry a ``component_id`` key.
        """
        event = Event(
            type=event_type,
            metadata=metadata,
            id=self._operation_event_id(self.run_id, str(metadata["component_id"]), event_type),
        )
        EventBus.emit_event(event)

    def _promote_dependents(self, completed_key: str) -> None:
        """Promote queued successors to READY if all their predecessors are done.

        Args:
            completed_key: Id of the operation that just completed, whose
                successors are candidates for promotion.
        """
        completed_keys = {
            k
            for k, info in self.executions.items()
            if info.status in (ExecutionStatus.COMPLETED, ExecutionStatus.SKIPPED)
        }
        for successor_key in self.dag.successors.get(completed_key, []):
            info = self.executions[successor_key]
            if info.status != ExecutionStatus.QUEUED:
                continue
            preds = self.dag.predecessors.get(successor_key, [])
            if all(p in completed_keys for p in preds):
                info.status = ExecutionStatus.READY

    def _propagate_failure(self, failed_key: str) -> list[str]:
        """Recursively mark all downstream dependents as CANCELED.

        Args:
            failed_key: Id of the operation that failed, whose transitive
                successors are canceled.

        Returns:
            List of operation ids that were canceled.
        """
        canceled: list[str] = []
        for successor_key in self.dag.successors.get(failed_key, []):
            info = self.executions[successor_key]
            if info.is_terminal:
                continue
            info.mark_canceled()
            canceled.append(successor_key)
            canceled.extend(self._propagate_failure(successor_key))
        return canceled
