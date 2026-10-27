"""Run: one run of a runner, walking its DAG and executing each operation where the runner says."""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from abc import ABC, abstractmethod
from functools import partial
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from interloper.errors import RunnerError, format_exception
from interloper.events import EventBus, EventType
from interloper.operation.base import OperationResult
from interloper.runner.results import ExecutionStatus, RunResult
from interloper.runner.state import RunState

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.operation.base import Operation
    from interloper.partitioning.base import PartitionWindow
    from interloper.runner.base import Runner

logger = logging.getLogger(__name__)


class Run(ABC):
    """One run of a runner: where its operations execute, and what it holds while the run lasts.

    A runner is configuration; a run is made from it for one DAG and one
    window, walks the DAG once and is closed. Each kind of run says where an
    attempt at an operation executes (`attempt`) and what it holds while the
    run lasts (acquired by `open`, released by `close`); the walk, the
    retries, every lifecycle event and the result are the same for every kind.
    """

    def __init__(self, runner: Runner, dag: DAG, window: PartitionWindow | None, metadata: dict[str, Any]) -> None:
        """Make a run of a runner, checking the window before anything is emitted.

        Args:
            runner: The runner this is a run of.
            dag: The DAG to walk.
            window: The window the run covers, ``None`` when unscoped.
            metadata: Run-level metadata, carrying the ``run_id``.
        """
        dag.check_window(window)
        self.runner = runner
        self.state = RunState(dag, window, metadata)
        self.fault: BaseException | None = None
        self.started_at = dt.datetime.now(dt.timezone.utc)

    async def __aenter__(self) -> Self:
        """Enter the run.

        Returns:
            This run.
        """
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Close the run; a failure to release is logged, never masking the result.

        Args:
            *exc_info: The exception leaving the block, if any; not suppressed.
        """
        try:
            await self.close()
        except Exception:
            logger.warning("Closing run %s failed", self.state.run_id, exc_info=True)

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
        self.start()
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
        self.state.cancel_pending()
        return self.finish(format_exception(self.fault) if self.fault is not None else None)

    async def _schedule(self, inflight: dict[asyncio.Task[OperationResult], Operation]) -> None:
        """Start ready operations and record results until the run is done, faults or stops early.

        Args:
            inflight: The tasks in flight, by operation; updated in place.

        Raises:
            RunnerError: If nothing is ready, nothing is in flight and the run is not done.
        """
        while not self.state.done and self.fault is None:
            for operation in self.state.ready[: self.runner.capacity - len(inflight)]:
                self.state.start(operation)
                inflight[asyncio.create_task(self.execute(operation))] = operation
            if not inflight:
                raise RunnerError(
                    "No operations ready but execution not complete. "
                    "This indicates a circular dependency or invalid DAG state."
                )
            finished, _ = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
            self._record(finished, inflight)
            if self.runner.fail_fast and self.state.failed:
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
                self.state.finish(operation, OperationResult(error=f"Operation '{operation.key}' was canceled"))
                continue
            fault = task.exception()
            if fault is None:
                self.state.finish(operation, task.result())
            elif self.fault is None:
                self.fault = fault

    # -- The run's lifecycle -------------------------------------------------------

    def start(self) -> None:
        """Start the run: ``RUN_STARTED``, then ``OPERATION_QUEUED`` for every operation that will execute."""
        EventBus.emit(
            EventType.RUN_STARTED,
            metadata={
                **self.state.metadata,
                "window": str(self.state.window) if self.state.window else None,
                "message": f"Run started ({len(self.state.dag.operations)} operations)",
            },
        )
        self.state.queue()

    def finish(self, error: str | None = None) -> RunResult:
        """Finish the run: ``RUN_COMPLETED`` or ``RUN_FAILED``, and its result.

        Args:
            error: The run's own fault, ``None`` when there was none; failed
                operations carry their own detail on their records and events.

        Returns:
            The run's result.
        """
        failed = bool(self.state.failed) or error is not None
        if not failed:
            event_type = EventType.RUN_COMPLETED
            message = f"Run completed ({len(self.state.completed)}/{len(self.state.dag.operations)} succeeded)"
        else:
            event_type = EventType.RUN_FAILED
            message = f"Run failed: {error}" if error else f"Run failed ({len(self.state.failed)} operation(s) failed)"
        EventBus.emit(
            event_type,
            metadata={
                **self.state.metadata,
                "window": str(self.state.window) if self.state.window else None,
                "error": error,
                "message": message,
            },
        )
        return RunResult(
            window=self.state.window,
            status=ExecutionStatus.FAILED if failed else ExecutionStatus.COMPLETED,
            executions=dict(self.state.executions),
            execution_time=(dt.datetime.now(dt.timezone.utc) - self.started_at).total_seconds(),
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
        if not self.state.failed:
            return
        operation = self.state.failed[0]
        info = self.state.executions[operation.id]
        if info.result is not None and info.result.exception is not None:
            raise info.result.exception
        raise RunnerError(f"Operation '{operation.key}' failed: {info.error}")

    async def execute(self, operation: Operation) -> OperationResult:
        """Execute an operation under its retry policy, reporting each attempt as it happens.

        Args:
            operation: The operation to execute.

        Returns:
            What its last attempt came to.
        """
        return await OperationResult.from_execution(
            operation,
            partial(self.attempt, operation),
            on_retry=partial(self.state.retry, operation),
            on_restart=partial(self.state.start, operation),
        )

    # -- The kind of run ---------------------------------------------------------

    @abstractmethod
    async def attempt(self, operation: Operation) -> OperationResult:
        """Make one attempt at an operation and return what it came to; never raise for its own failure.

        Args:
            operation: The operation to attempt.

        Returns:
            The attempt's result, its ``retryable`` set on a failure.
        """

    async def open(self) -> None:  # noqa: B027 - a run holding nothing acquires nothing
        """Acquire what the run holds once its lifecycle has started; nothing by default.

        Called inside the walk, so a failure to acquire (a daemon or a cluster
        out of reach) fails the run with its events instead of raising before
        any is emitted.
        """

    async def close(self) -> None:  # noqa: B027 - a run holding nothing releases nothing
        """Release what the run holds; nothing by default."""
