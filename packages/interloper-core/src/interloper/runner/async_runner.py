"""Async-native runner using asyncio tasks for bounded concurrency."""

from __future__ import annotations

import asyncio
import traceback
from typing import TYPE_CHECKING, Any

from pydantic import PrivateAttr

from interloper.errors import RunnerError, format_exception
from interloper.operation.base import Operation, OperationContext
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runner.base import Runner
from interloper.runner.results import RunResult
from interloper.telemetry import attributes
from interloper.telemetry.tracer import tracer

if TYPE_CHECKING:
    from interloper.dag.base import DAG


class AsyncRunner(Runner):
    """Async-native, in-process runner — the single DAG-walking engine.

    Schedules ready operations as ``asyncio`` tasks bounded by an
    ``asyncio.Semaphore``. It subsumes both serial and thread-pool execution:

    - ``AsyncRunner(max_workers=1)`` — serial execution (deterministic ordering).
    - ``AsyncRunner(max_workers=4)`` — concurrent execution (default).

    Sync ``data()`` functions are automatically offloaded to threads via
    ``asyncio.to_thread``, while async ``data()`` functions run natively
    on the event loop. Either way, exactly one event loop is created per
    run (not per asset)::

        # async
        result = await AsyncRunner(max_workers=2, on_event=log_event).run(dag)

        # sync edge (scripts, REPL, notebooks)
        result = il.run(AsyncRunner(on_event=log_event).run(dag))
    """

    max_workers: int = 4
    fail_fast: bool = True
    reraise: bool = False

    _semaphore: asyncio.Semaphore | None = PrivateAttr(default=None)

    async def _run(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RunResult:
        """Execute the DAG by dynamically scheduling ready operations.

        Args:
            dag: The DAG to execute.
            partition_or_window: Partition or window to scope the run.
            metadata: Arbitrary metadata (e.g. run_id, backfill_id).

        Returns:
            A RunResult summarizing the execution outcome.

        Operation failures are absorbed into state (each node's own error,
        traceback, and effects); with ``reraise`` set, the first failed
        operation's original exception is re-raised after the run is
        finalized.

        Raises:
            RunnerError: If a deadlock or invalid DAG state is detected.
        """
        self._init_run(dag, partition_or_window, metadata)
        inflight: dict[asyncio.Task[Any], Operation] = {}

        try:
            self._on_start()
            self._semaphore = asyncio.Semaphore(self.max_workers)

            while not self.state.is_run_complete():
                submitted_ids = {operation.id for operation in inflight.values()}

                for operation in self.state.ready_operations:
                    if len(inflight) >= self._capacity:
                        break
                    if operation.id in submitted_ids:
                        continue
                    handle = self._submit_operation(operation, partition_or_window)
                    inflight[handle] = operation

                if not inflight:
                    raise RunnerError(
                        "No operations ready but execution not complete. "
                        "This indicates a circular dependency or invalid DAG state."
                    )

                done, _ = await asyncio.wait(inflight.keys(), return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    inflight.pop(task)
                    exception = task.exception()
                    if exception is not None:
                        raise exception

                if self.fail_fast and self.state.failed_operations:
                    break

            await self._flush(inflight)
            result = self._finalize_run()

        except Exception as e:
            # Operation failures are absorbed into state, so an exception
            # here means the walk machinery itself broke.
            await self._flush(inflight)
            result = self._finalize_run(error=format_exception(e))
            if self.reraise:
                raise
        finally:
            try:
                self._on_end()
            except Exception:  # noqa: BLE001, S110
                pass

        if self.reraise and self.state.failed_operations:
            self._reraise_first_failure()
        return result

    # -- Scheduling primitives -------------------------------------------------

    @property
    def _capacity(self) -> int:
        """Maximum number of concurrent operations this runner can execute.

        Returns:
            ``max_workers``, the size of the semaphore bounding execution.
        """
        return self.max_workers

    def _submit_operation(
        self,
        operation: Operation,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> asyncio.Task[Any]:
        """Schedule an operation as an asyncio task guarded by the semaphore.

        Args:
            operation: The operation to execute.
            partition_or_window: Partition or window to scope the run.

        Returns:
            The created task, which acquires a concurrency slot before
            executing the operation.
        """
        assert self._semaphore is not None

        sem = self._semaphore

        async def _guarded() -> Any:
            async with sem:
                return await self._execute_operation(operation, partition_or_window)

        return asyncio.create_task(_guarded())

    # -- Operation execution -----------------------------------------------------

    async def _execute_operation(
        self,
        operation: Operation,
        partition_or_window: Partition | PartitionWindow | None = None,
    ) -> Any:
        """Execute a single operation with state tracking.

        On failure the operation's own :meth:`~Operation.failure` hook
        curates the recorded message and effects, and the traceback is
        attached only when the operation's class allows it — operations
        whose raw errors embed secrets opt out.

        Args:
            operation: The operation to execute.
            partition_or_window: Partition or window to scope the run; narrowed
                to the operation's own effective partition before executing.

        Returns:
            The execution's effects, or ``None`` if the operation failed.
        """
        self.state.mark_running(operation)

        effective_partition = operation.effective_partition(partition_or_window)
        span_attrs = attributes.from_metadata(
            operation._event_metadata(self.state.metadata, effective_partition)
        )
        context = OperationContext(
            partition_or_window=effective_partition,
            dag=self.state.dag,
            metadata=self.state.metadata,
        )
        try:
            with tracer().start_as_current_span("interloper.operation.execute", attributes=span_attrs):
                result = await operation.execute(context)
            self.state.mark_completed(operation, effects=result)
        except Exception as e:  # noqa: BLE001 — every failure becomes the node's record
            failed = operation.failure(e)
            tb = traceback.format_exc() if type(operation).capture_traceback else None
            self.state.mark_failed(operation, failed.error or format_exception(e), tb=tb, effects=failed, exception=e)
            return None
        return result

    async def _flush(self, inflight: dict[asyncio.Task[Any], Operation]) -> None:
        """Wait for all in-flight tasks and emit terminal events.

        Called when the walk ends — a fail-fast break, a machinery abort,
        or natural completion (where it is effectively a no-op) — so that
        every in-flight operation gets a proper terminal event rather than
        being silently abandoned as 'running'.

        In the async runner, ``_execute_operation`` already calls
        ``mark_failed`` / ``mark_completed``, so completed tasks are
        already handled.  We only need to cancel tasks that are still
        pending (e.g. waiting for the semaphore).

        Args:
            inflight: Tasks still in flight, mapped to the operation each was
                created for. An empty mapping is a no-op.
        """
        if not inflight:
            return

        if self.fail_fast:
            for task, operation in inflight.items():
                task.cancel()
                info = self.state.executions.get(operation.id)
                if info and not info.is_terminal:
                    self.state.mark_canceled(operation)
            return

        # Let running tasks finish naturally and record their outcomes.
        # (_execute_operation already calls mark_failed/mark_completed.)
        done, _ = await asyncio.wait(inflight.keys())

        for task in done:
            if not task.cancelled():
                task.exception()  # Consume to avoid "exception never retrieved"
