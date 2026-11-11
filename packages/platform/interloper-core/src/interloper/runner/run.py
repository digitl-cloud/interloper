"""Run: one run of a runner, its operations' records, the walk, and where each operation executes."""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import uuid
from abc import ABC, abstractmethod
from contextlib import AsyncExitStack
from functools import partial
from types import TracebackType
from typing import TYPE_CHECKING, Any

from opentelemetry import trace
from opentelemetry.trace import StatusCode
from typing_extensions import Self

from interloper.errors import RunnerError, format_exception
from interloper.events import Event, EventBus, EventType
from interloper.operation.base import OperationContext, OperationResult
from interloper.runner.results import ExecutionInfo, ExecutionStatus, RunResult
from interloper.runner.subscription import RunSubscription
from interloper.telemetry import attributes
from interloper.telemetry.propagation import context_from_env, extract_metadata, inject_metadata
from interloper.telemetry.tracer import tracer
from interloper.utils.text import to_snake_case

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.operation.base import Operation
    from interloper.partitioning.base import PartitionWindow
    from interloper.runner.base import Runner

logger = logging.getLogger(__name__)


class Run(ABC):
    """One run of a runner: its operations' records, the walk, and where each operation executes.

    A runner is configuration; a run is made from it for one DAG and one
    window, walks the DAG once and is closed. Each kind of run says where an
    operation executes (`execute`) and what it holds while the run lasts
    (acquired by `open`, released by `close`); the walk, the retries, every
    lifecycle event and the result are the same for every kind.

    The run is the only author of its operations' lifecycle, wherever an
    execution happens: a container's child reports only what happens inside
    its operation. Every transition is one verb (`start`, `retry`, `finish`,
    `cancel_pending`) and emits its operation-lifecycle event with a
    deterministic id (`operation_event_id`), so a verdict the store authors
    for an operation its run left open collapses with the same verdict
    arriving late from a stopping pod. Every transition happens on the run's
    event loop thread, so no locking is required.

    Entering the run delivers its events to the runner's ``on_event`` and
    makes its span current; leaving it releases what it holds, ends the span
    and delivers what is still queued.
    """

    _OPERATION_EVENT_NAMESPACE = uuid.UUID("a3f1c2d4-5b6e-4a7c-9d8f-0e1a2b3c4d5e")

    def __init__(self, runner: Runner, dag: DAG, window: PartitionWindow | None, metadata: dict[str, Any]) -> None:
        """Make a run of a runner, checking the window before anything is emitted, its roots ready.

        Args:
            runner: The runner this is a run of.
            dag: The DAG to walk.
            window: The window the run covers, ``None`` when unscoped.
            metadata: Run-level metadata, carried by every event; a
                ``run_id`` is generated when absent.
        """
        dag.check_window(window)
        self.runner = runner
        self.dag = dag
        self.window = window
        self.metadata = {"run_id": str(uuid.uuid4()), **metadata}
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
        self.fault: BaseException | None = None
        self.started_at = dt.datetime.now(dt.timezone.utc)
        self._held = AsyncExitStack()

    async def __aenter__(self) -> Self:
        """Enter the run: its events reach the runner's ``on_event`` and its span is current until it is left.

        The span continues the trace the metadata or the environment carries,
        and the metadata carries the span on, onto every event.

        Returns:
            This run.
        """
        if self.runner.on_event is not None:
            await self._held.enter_async_context(RunSubscription(self.runner.on_event, self.run_id))
        parent = extract_metadata(self.metadata) or context_from_env()
        self._held.enter_context(
            tracer().start_as_current_span("interloper.runner.run", context=parent, attributes=self._span_attributes())
        )
        inject_metadata(self.metadata)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Leave the run; a failure to release is logged, never masking the result.

        Args:
            exc_type: The type of the exception leaving the block, if any.
            exc: The exception leaving the block, if any; recorded on the span, not suppressed.
            traceback: Its traceback, if any.
        """
        try:
            await self.close()
        except Exception:
            logger.warning("Closing run %s failed", self.run_id, exc_info=True)
        await self._held.__aexit__(exc_type, exc, traceback)

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
        return OperationContext(
            window=operation.effective_window(self.window, self.metadata), dag=self.dag, metadata=self.metadata
        )

    # -- The walk ----------------------------------------------------------------

    async def walk(self) -> RunResult:
        """Execute every operation in dependency order, at most ``capacity`` at a time.

        Operations whose dependencies are met start as slots free up; each
        result is recorded as it lands. Under ``fail_fast`` nothing new starts
        once an operation failed; what is in flight always finishes. An
        ``execute`` that raises, rather than returning a failed result, is a
        fault of the run itself and becomes the run's error, as is a failure to
        `open` or one raised while draining; either way the run still ends, its
        pending operations canceled.

        Returns:
            The run's result.
        """
        self._begin()
        inflight: dict[asyncio.Task[OperationResult], Operation] = {}
        try:
            await self.open()
            await self._schedule(inflight)
        except Exception as fault:  # noqa: BLE001 - a fault of the run itself becomes the run's error
            self.fault = fault
        try:
            await self._drain(inflight)
        except Exception as fault:  # noqa: BLE001 - so is one while draining
            self.fault = self.fault or fault
        self.cancel_pending()
        return self._end(format_exception(self.fault) if self.fault is not None else None)

    async def _schedule(self, inflight: dict[asyncio.Task[OperationResult], Operation]) -> None:
        """Start ready operations and record results until the run is done, faults or stops early.

        Args:
            inflight: The tasks in flight, by operation; updated in place.

        Raises:
            RunnerError: If nothing is ready, nothing is in flight and the run is not done.
        """
        while not self.done and self.fault is None:
            for operation in self.ready[: self.runner.capacity - len(inflight)]:
                self.start(operation)
                inflight[asyncio.create_task(self._execute_with_retries(operation))] = operation
            if not inflight:
                raise RunnerError(
                    "No operations ready but execution not complete. "
                    "This indicates a circular dependency or invalid DAG state."
                )
            finished, _ = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
            self._record(finished, inflight)
            if self.runner.fail_fast and self.failed:
                return

    async def _drain(self, inflight: dict[asyncio.Task[OperationResult], Operation]) -> None:
        """Let every task in flight finish, recording what each came to.

        Args:
            inflight: The tasks in flight, by operation; emptied.
        """
        if inflight:
            finished, _ = await asyncio.wait(inflight)
            self._record(finished, inflight)

    def _record(
        self,
        finished: set[asyncio.Task[OperationResult]],
        inflight: dict[asyncio.Task[OperationResult], Operation],
    ) -> None:
        """Record what each finished task's operation came to; a task that raised is the run's fault.

        A task canceled from within (an operation letting a ``CancelledError``
        escape) fails its operation: left running, it would stall the walk.

        Args:
            finished: The tasks that finished.
            inflight: The tasks in flight, by operation; the finished ones are removed.
        """
        for task in finished:
            operation = inflight.pop(task)
            if task.cancelled():
                self.finish(operation, OperationResult(error=f"Operation '{operation.key}' was canceled"))
                continue
            fault = task.exception()
            if fault is None:
                self.finish(operation, task.result())
            elif self.fault is None:
                self.fault = fault

    async def _execute_with_retries(self, operation: Operation) -> OperationResult:
        """Execute an operation under its retry policy, reporting each execution as it happens.

        Args:
            operation: The operation to execute.

        Returns:
            What its last execution came to.
        """
        return await OperationResult.from_retries(
            operation,
            partial(self.execute, operation),
            on_retry=partial(self.retry, operation),
            on_restart=partial(self.start, operation),
        )

    def reraise(self) -> None:
        """Raise what failed the run, for a caller that asked for exceptions.

        That is the run's own fault when it had one; else the first failed
        operation's original exception, when it executed in this process.

        Raises:
            RunnerError: When the failure's exception stayed in another process.
        """
        if self.fault is not None:
            raise self.fault
        if not self.failed:
            return
        operation = self.failed[0]
        info = self.executions[operation.id]
        if info.result is not None and info.result.exception is not None:
            raise info.result.exception
        raise RunnerError(f"Operation '{operation.key}' failed: {info.error}")

    # -- Transitions -------------------------------------------------------------

    def start(self, operation: Operation) -> None:
        """Mark an operation's execution running (``OPERATION_STARTED``).

        Args:
            operation: The operation starting an execution.
        """
        self.executions[operation.id].start()
        self._emit(EventType.OPERATION_STARTED, operation, f"Operation '{operation.key}' started")

    def retry(self, operation: Operation, error: str) -> None:
        """Record a failed execution that will be retried (``OPERATION_RETRIED``), and count the next one.

        A retried execution is not a verdict: the operation keeps its status,
        nothing downstream is canceled, and ``OPERATION_FAILED`` stays reserved
        for an exhausted budget. Counting the next execution is what makes its
        events distinct from this one's.

        Args:
            operation: The operation whose execution failed.
            error: The execution's error.
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
        or a fault of the run): without it, operations never started would
        stay non-terminal forever.
        """
        for operation in self.dag.operations:
            info = self.executions[operation.id]
            if not info.is_terminal:
                info.cancel()
                self._emit(EventType.OPERATION_CANCELED, operation, f"Operation '{operation.key}' canceled")

    @staticmethod
    def operation_event_id(run_id: str, component_id: str, event_type: EventType, attempt: int = 1) -> str:
        """Derive a deterministic event id from a run/component/type triple.

        The store derives the same ids when it closes the operations a run
        left open, so a late event from a stopping pod dedups.

        Args:
            run_id: Id of the run the event belongs to.
            component_id: Id of the operation the event is about.
            event_type: The operation-lifecycle event type.
            attempt: The execution the event belongs to, so a retried node's
                events are their own rows rather than a dedup of the first
                execution's. Left out of the key when it is 1, which keeps every
                id written before retries existed unchanged.

        Returns:
            A stable UUID5 string for the event.
        """
        key = f"{run_id}:{component_id}:{event_type.value}"
        if attempt > 1:
            key = f"{key}:{attempt}"
        return str(uuid.uuid5(Run._OPERATION_EVENT_NAMESPACE, key))

    # -- The kind of run ---------------------------------------------------------

    @abstractmethod
    async def execute(self, operation: Operation) -> OperationResult:
        """Execute an operation once and return what it came to; never raise for its own failure.

        Args:
            operation: The operation to execute.

        Returns:
            The execution's result, its ``retryable`` set on a failure.
        """

    async def open(self) -> None:  # noqa: B027 - a run holding nothing acquires nothing
        """Acquire what the run holds once its lifecycle has started; nothing by default.

        Called inside the walk, so a failure to acquire (a daemon or a cluster
        out of reach) fails the run with its events instead of raising before
        any is emitted.
        """

    async def close(self) -> None:  # noqa: B027 - a run holding nothing releases nothing
        """Release what the run holds; nothing by default."""

    # -- Internals -----------------------------------------------------------------

    def _begin(self) -> None:
        """Start the run: ``RUN_STARTED``, then ``OPERATION_QUEUED`` for every operation that will execute."""
        EventBus.emit(
            EventType.RUN_STARTED,
            metadata={
                **self.metadata,
                "window": str(self.window) if self.window else None,
                "message": f"Run started ({len(self.dag.operations)} operations)",
            },
        )
        for operation in self.dag.operations:
            if self.executions[operation.id].status in (ExecutionStatus.QUEUED, ExecutionStatus.READY):
                self._emit(EventType.OPERATION_QUEUED, operation, f"Operation '{operation.key}' queued")

    def _end(self, error: str | None) -> RunResult:
        """End the run: ``RUN_COMPLETED`` or ``RUN_FAILED``, its span's status, and its result.

        Args:
            error: The run's own fault, ``None`` when there was none; failed
                operations carry their own detail on their records and events.

        Returns:
            The run's result.
        """
        failed = bool(self.failed) or error is not None
        if not failed:
            event_type = EventType.RUN_COMPLETED
            message = f"Run completed ({len(self.completed)}/{len(self.dag.operations)} succeeded)"
        else:
            event_type = EventType.RUN_FAILED
            message = f"Run failed: {error}" if error else f"Run failed ({len(self.failed)} operation(s) failed)"
            trace.get_current_span().set_status(StatusCode.ERROR, f"{len(self.failed)} operation(s) failed")
        EventBus.emit(
            event_type,
            metadata={
                **self.metadata,
                "window": str(self.window) if self.window else None,
                "error": error,
                "message": message,
            },
        )
        return RunResult(
            window=self.window,
            status=ExecutionStatus.FAILED if failed else ExecutionStatus.COMPLETED,
            executions=dict(self.executions),
            execution_time=(dt.datetime.now(dt.timezone.utc) - self.started_at).total_seconds(),
        )

    def _span_attributes(self) -> dict[str, str]:
        """The run span's attributes: the metadata's, the runner's snake_cased class name and the window.

        Returns:
            The attributes.
        """
        span_attributes = attributes.from_metadata(self.metadata)
        span_attributes[attributes.RUNNER_TYPE] = to_snake_case(type(self.runner).__name__)
        if self.window is not None:
            span_attributes[attributes.PARTITION] = str(self.window)
        return span_attributes

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

        The event is scoped to the window the operation consumes, and names its partition when
        that window holds one: the platform files the execution under it, which for a snapshot
        is the period observed rather than the run's partition.

        Args:
            event_type: The lifecycle event.
            operation: The operation it is about.
            message: The event's message.
            **extra: More metadata (``error``, ``traceback``).
        """
        attempt = self.executions[operation.id].attempt
        window = operation.effective_window(self.window, self.metadata)
        if window is not None and len(window) == 1:
            extra["partition_key"] = window.first.key
        metadata = {
            **operation.event_metadata(self.metadata, window, attempt=attempt, **extra),
            "message": message,
        }
        event_id = self.operation_event_id(self.run_id, operation.id, event_type, attempt=attempt)
        EventBus.emit_event(Event(type=event_type, metadata=metadata, id=event_id))
