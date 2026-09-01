"""Base class for synchronous runners using concurrent.futures."""

from __future__ import annotations

import asyncio
import traceback
from abc import abstractmethod
from concurrent.futures import FIRST_COMPLETED, Future, wait
from typing import TYPE_CHECKING, Any

from interloper.errors import RunnerError, format_exception
from interloper.operation.base import Operation, OperationResult
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runner.base import Runner
from interloper.runner.results import RunResult

if TYPE_CHECKING:
    from interloper.dag.base import DAG


class SyncRunner(Runner):
    """Base class for ``concurrent.futures``-backed, out-of-process runners.

    Backs the runners whose unit of execution is a separate process or
    container — :class:`MultiProcessRunner`, ``DockerRunner``,
    ``KubernetesRunner`` — where the event loop lives at the process/pod
    boundary, not in this scheduler. In-process DAG walking is handled by
    the async-native :class:`~interloper.runner.async_runner.AsyncRunner`.

    These runners are inherently blocking (they poll futures / Jobs), so the
    async-native :meth:`~interloper.runner.base.Runner.run` contract is
    satisfied by offloading the blocking DAG walk to a worker thread. Subclasses
    implement ``_submit_operation`` to submit work to their executor and
    ``_handle_completed`` to interpret what the future returned.
    """

    async def _run(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any] | None,
    ) -> RunResult:
        """Offload the blocking DAG walk to a worker thread.

        Args:
            dag: The DAG to execute.
            partition_or_window: Partition or window to scope the run.
            metadata: Arbitrary metadata (e.g. run_id, backfill_id).

        Returns:
            A RunResult summarizing the execution outcome.
        """
        return await asyncio.to_thread(self._run_blocking, dag, partition_or_window, metadata)

    def _run_blocking(
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
        operation's exception is re-raised after the run is finalized.

        Raises:
            RunnerError: If a deadlock or invalid DAG state is detected.
        """
        self._init_run(dag, partition_or_window, metadata)
        inflight: dict[Future[Any], Operation] = {}

        try:
            self._on_start()

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

                done, _ = wait(inflight.keys(), return_when=FIRST_COMPLETED)
                for future in done:
                    operation = inflight.pop(future)
                    self._handle_completed(future, operation)

                if self.fail_fast and self.state.failed_operations:
                    break

            self._flush(inflight)
            result = self._finalize_run()

        except Exception as e:
            # Operation failures are absorbed into state, so an exception
            # here means the walk machinery itself broke.
            self._flush(inflight)
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

    # -- Abstract interface ----------------------------------------------------

    @property
    @abstractmethod
    def _capacity(self) -> int:
        """Maximum number of concurrent operations this runner can execute.

        Returns:
            The concurrency ceiling used to throttle submissions.
        """

    @abstractmethod
    def _submit_operation(
        self,
        operation: Operation,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> Future[Any]:
        """Submit an operation for execution and return a Future.

        Args:
            operation: The operation to execute.
            partition_or_window: Partition or window to scope the run.

        Returns:
            A Future resolving when the operation's execution finishes.
        """

    # -- Shared execution helpers ----------------------------------------------

    def _handle_completed(self, future: Future[Any], operation: Operation) -> None:
        """Process a completed future and update state.

        Args:
            future: The finished future returned by ``_submit_operation``.
            operation: The operation the future was submitted for.
        """
        try:
            result = future.result()
        except Exception as e:  # noqa: BLE001 — every failure becomes the node's record
            self.state.mark_failed(operation, format_exception(e), tb=traceback.format_exc(), exception=e)
            return

        self.state.mark_completed(operation, effects=result if isinstance(result, OperationResult) else None)

    def _flush(self, inflight: dict[Future[Any], Operation]) -> None:
        """Wait for all in-flight futures and emit terminal events.

        Called when the walk ends — a fail-fast break, a machinery abort,
        or natural completion (where it is effectively a no-op) — so that
        every in-flight operation gets a proper FAILED or CANCELED event
        rather than being silently abandoned as 'running'.

        Args:
            inflight: Futures still in flight, mapped to the operation each
                was submitted for. An empty mapping is a no-op.
        """
        if not inflight:
            return

        if self.fail_fast:
            for future, operation in inflight.items():
                future.cancel()
                info = self.state.executions.get(operation.id)
                if info and not info.is_terminal:
                    self.state.mark_canceled(operation)
            return

        # Let running tasks finish naturally and record their outcomes.
        done, _ = wait(inflight.keys())

        for future in done:
            operation = inflight[future]
            info = self.state.executions.get(operation.id)
            if info and info.is_terminal:
                continue
            self._handle_flushed(future, operation)

    def _handle_flushed(self, future: Future[Any], operation: Operation) -> None:
        """Process a completed future during flush.

        Subclasses that return structured results from their futures
        (e.g. ``(success, error, tb)`` tuples) should override this to
        interpret the result correctly — just like they override
        ``_handle_completed``.

        The default assumes ``future.result()`` raises on failure
        (the same contract as the base ``_handle_completed``).

        Args:
            future: The finished future to interpret.
            operation: The operation the future was submitted for.
        """
        try:
            result = future.result()
            self.state.mark_completed(operation, effects=result if isinstance(result, OperationResult) else None)
        except Exception as e:  # noqa: BLE001
            self.state.mark_failed(operation, format_exception(e), tb=traceback.format_exc())
