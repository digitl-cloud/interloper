"""Base class for synchronous runners using concurrent.futures."""

from __future__ import annotations

import asyncio
import traceback
from abc import abstractmethod
from concurrent.futures import FIRST_COMPLETED, Future, wait
from typing import TYPE_CHECKING, Any

from interloper.asset.base import Asset
from interloper.errors import RunnerError
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
    implement ``_submit_asset`` to submit work to their executor and
    ``_handle_completed`` to interpret what the future returned.
    """

    async def _run(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any] | None,
    ) -> RunResult:
        """Offload the blocking DAG walk to a worker thread.

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
        inflight: dict[Future[Any], Asset] = {}

        try:
            self._on_start()

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

                done, _ = wait(inflight.keys(), return_when=FIRST_COMPLETED)
                for future in done:
                    asset = inflight.pop(future)
                    self._handle_completed(future, asset)

            return self._finalize_run()

        except Exception as e:
            self._flush(inflight)
            result = self._finalize_run(error=str(e))
            if self.reraise:
                raise
            return result
        finally:
            try:
                self._on_end()
            except Exception:  # noqa: BLE001, S110
                pass

    # -- Abstract interface ----------------------------------------------------

    @property
    @abstractmethod
    def _capacity(self) -> int:
        """Maximum number of concurrent assets this runner can execute."""

    @abstractmethod
    def _submit_asset(
        self,
        asset: Asset,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> Future[Any]:
        """Submit an asset for execution and return a Future."""

    # -- Shared execution helpers ----------------------------------------------

    def _handle_completed(self, future: Future[Any], asset: Asset) -> None:
        """Process a completed future and update state."""
        try:
            future.result()
        except Exception as e:
            self.state.mark_asset_failed(asset, str(e), tb=traceback.format_exc())
            if self.fail_fast or self.reraise:
                raise
            return

        self.state.mark_asset_completed(asset)

    def _flush(self, inflight: dict[Future[Any], Asset]) -> None:
        """Wait for all in-flight futures and emit terminal events.

        Called after an exception aborts the main loop so that every
        in-flight asset gets a proper FAILED or CANCELED event rather
        than being silently abandoned as 'running'.
        """
        if not inflight:
            return

        if self.fail_fast:
            for future, asset in inflight.items():
                future.cancel()
                info = self.state.asset_executions.get(asset.id)
                if info and not info.is_terminal:
                    self.state.mark_asset_canceled(asset)
            return

        # Let running tasks finish naturally and record their outcomes.
        done, _ = wait(inflight.keys())

        for future in done:
            asset = inflight[future]
            info = self.state.asset_executions.get(asset.id)
            if info and info.is_terminal:
                continue
            self._handle_flushed(future, asset)

    def _handle_flushed(self, future: Future[Any], asset: Asset) -> None:
        """Process a completed future during flush.

        Subclasses that return structured results from their futures
        (e.g. ``(success, error, tb)`` tuples) should override this to
        interpret the result correctly — just like they override
        ``_handle_completed``.

        The default assumes ``future.result()`` raises on failure
        (the same contract as the base ``_handle_completed``).
        """
        try:
            future.result()
            self.state.mark_asset_completed(asset)
        except Exception as e:  # noqa: BLE001
            self.state.mark_asset_failed(asset, str(e), tb=traceback.format_exc())
