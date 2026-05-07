"""Base class for synchronous runners using concurrent.futures."""

from __future__ import annotations

import asyncio
import traceback
from abc import abstractmethod
from concurrent.futures import FIRST_COMPLETED, Future, wait
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from interloper.asset.base import Asset
from interloper.errors import RunnerError
from interloper.events import EventBus
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runner.base import Runner
from interloper.runner.results import RunResult

if TYPE_CHECKING:
    from interloper.dag.base import DAG


class SyncRunner(Runner):
    """Base class for synchronous runners backed by ``concurrent.futures``.

    Subclasses implement ``_submit_asset`` to submit work to their executor
    and ``_handle_future_result`` to interpret what the future returned.
    The DAG walk loop, context manager, and error handling are shared.

    Usage::

        with MyRunner(on_event=log_event) as runner:
            result = runner.run(dag)
    """

    # ------------------------------------------------------------------
    # Sync context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> Self:
        """Subscribe the ``on_event`` handler if provided.

        Returns:
            The runner instance.
        """
        if self.on_event is not None:
            EventBus.subscribe(self.on_event)
        return self

    def __exit__(self, *_: object) -> None:
        """Unsubscribe the handler and flush pending events."""
        if self.on_event is not None:
            EventBus.flush(timeout=5.0)
            EventBus.unsubscribe(self.on_event)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
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

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Shared execution helpers
    # ------------------------------------------------------------------

    def _execute_asset(
        self,
        asset: Asset,
        partition_or_window: Partition | PartitionWindow | None = None,
    ) -> Any:
        """Execute a single asset synchronously via ``asyncio.run``.

        Returns:
            The materialization result.
        """
        effective_partition = partition_or_window if asset.partitioning is not None else None
        return asyncio.run(
            asset.materialize(
                partition_or_window=effective_partition,
                dag=self.state.dag,
                metadata=self.state.metadata,
            )
        )

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
