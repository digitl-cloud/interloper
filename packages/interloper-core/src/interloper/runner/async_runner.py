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

    Schedules ready assets as ``asyncio`` tasks bounded by an
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
        """Materialize the DAG by dynamically scheduling ready assets.

        Args:
            dag: The DAG to execute.
            partition_or_window: Partition or window to scope the run.
            metadata: Arbitrary metadata (e.g. run_id, backfill_id).

        Returns:
            A RunResult summarizing the execution outcome.

        Raises:
            RunnerError: If a deadlock or invalid DAG state is detected.
        """
        self._init_run(dag, partition_or_window, metadata)
        inflight: dict[asyncio.Task[Any], Operation] = {}

        try:
            self._on_start()
            self._semaphore = asyncio.Semaphore(self.max_workers)

            while not self.state.is_run_complete():
                submitted_ids = {asset.id for asset in inflight.values()}
                ready_assets = self.state.ready_assets

                for asset in ready_assets:
                    if len(inflight) >= self._capacity:
                        break
                    if asset.id in submitted_ids:
                        continue
                    handle = self._submit_asset(asset, partition_or_window)
                    inflight[handle] = asset

                if not inflight:
                    raise RunnerError(
                        "No assets ready but execution not complete. "
                        "This indicates a circular dependency or invalid DAG state."
                    )

                done, _ = await asyncio.wait(inflight.keys(), return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    inflight.pop(task)
                    exception = task.exception()
                    if exception is not None and (self.fail_fast or self.reraise):
                        raise exception

            return self._finalize_run()

        except Exception as e:
            await self._flush(inflight)
            result = self._finalize_run(error=format_exception(e))
            if self.reraise:
                raise
            return result
        finally:
            try:
                self._on_end()
            except Exception:  # noqa: BLE001, S110
                pass

    # -- Scheduling primitives -------------------------------------------------

    @property
    def _capacity(self) -> int:
        """Maximum number of concurrent assets this runner can execute.

        Returns:
            ``max_workers``, the size of the semaphore bounding execution.
        """
        return self.max_workers

    def _submit_asset(
        self,
        asset: Operation,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> asyncio.Task[Any]:
        """Schedule an asset as an asyncio task guarded by the semaphore.

        Args:
            asset: The asset to execute.
            partition_or_window: Partition or window to scope the run.

        Returns:
            The created task, which acquires a concurrency slot before
            materializing the asset.
        """
        assert self._semaphore is not None

        sem = self._semaphore

        async def _guarded() -> Any:
            async with sem:
                return await self._execute_asset(asset, partition_or_window)

        return asyncio.create_task(_guarded())

    # -- Asset execution -------------------------------------------------------

    async def _execute_asset(
        self,
        asset: Operation,
        partition_or_window: Partition | PartitionWindow | None = None,
    ) -> Any:
        """Execute a single operation with state tracking.

        On failure the operation's own :meth:`~Operation.failure` hook
        curates the recorded message and effects, and the traceback is
        attached only when the operation's class allows it — operations
        whose raw errors embed secrets opt out.

        Args:
            asset: The operation to execute.
            partition_or_window: Partition or window to scope the run; narrowed
                to the operation's own effective partition before executing.

        Returns:
            The execution's effects, or ``None`` if the operation failed and
            ``reraise`` is False.
        """
        self.state.mark_asset_running(asset)

        effective_partition = asset.effective_partition(partition_or_window)
        span_attrs = attributes.from_metadata(
            asset._event_metadata(self.state.metadata, effective_partition)
        )
        context = OperationContext(
            partition_or_window=effective_partition,
            dag=self.state.dag,
            metadata=self.state.metadata,
        )
        try:
            with tracer().start_as_current_span("interloper.asset.materialize", attributes=span_attrs):
                result = await asset.execute(context)
            self.state.mark_asset_completed(asset, effects=result)
        except Exception as e:
            failed = asset.failure(e)
            tb = traceback.format_exc() if type(asset).capture_traceback else None
            self.state.mark_asset_failed(asset, failed.error or format_exception(e), tb=tb, effects=failed)
            if self.reraise or self.fail_fast:
                raise
            return None
        return result

    async def _flush(self, inflight: dict[asyncio.Task[Any], Operation]) -> None:
        """Wait for all in-flight tasks and emit terminal events.

        Called after an exception aborts the main loop so that every
        in-flight asset gets a proper terminal event rather than being
        silently abandoned as 'running'.

        In the async runner, ``_execute_asset`` already calls
        ``mark_asset_failed`` / ``mark_asset_completed``, so completed
        tasks are already handled.  We only need to cancel tasks that
        are still pending (e.g. waiting for the semaphore).

        Args:
            inflight: Tasks still in flight, mapped to the asset each was
                created for. An empty mapping is a no-op.
        """
        if not inflight:
            return

        if self.fail_fast:
            for task, asset in inflight.items():
                task.cancel()
                info = self.state.asset_executions.get(asset.id)
                if info and not info.is_terminal:
                    self.state.mark_asset_canceled(asset)
            return

        # Let running tasks finish naturally and record their outcomes.
        # (_execute_asset already calls mark_asset_failed/completed.)
        done, _ = await asyncio.wait(inflight.keys())

        for task in done:
            if not task.cancelled():
                task.exception()  # Consume to avoid "exception never retrieved"
