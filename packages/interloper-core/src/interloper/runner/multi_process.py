"""Multi-process runner using concurrent.futures."""

from __future__ import annotations

import asyncio
import traceback
from concurrent.futures import Future, ProcessPoolExecutor
from typing import Any

from pydantic import PrivateAttr

from interloper.asset.base import Asset
from interloper.errors import RunnerError
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runner.sync_runner import SyncRunner


def _worker(
    asset_id: str,
    dag_spec: dict[str, Any],
    partition_or_window: Partition | PartitionWindow | None,
    metadata: dict[str, Any],
) -> tuple[str, bool, str | None, str | None]:
    """Execute a single asset in a worker process.

    Reconstructs the DAG from its serialized spec, looks up the target
    asset by ID, and materializes it.

    Returns:
        Tuple of ``(id, success, error_message, formatted_traceback)``.
    """
    from interloper.dag.spec import DAGSpec

    try:
        dag = DAGSpec(**dag_spec).reconstruct()
        asset = dag.asset_map[asset_id]

        if asset.partitioning is None:
            partition_or_window = None

        asyncio.run(
            asset.materialize(
                partition_or_window=partition_or_window,
                dag=dag,
                metadata=metadata,
            )
        )
    except Exception as e:  # noqa: BLE001
        return (asset_id, False, str(e), traceback.format_exc())
    return (asset_id, True, None, None)


class MultiProcessRunner(SyncRunner):
    """Process-based parallel runner.

    Executes independent assets concurrently using a process pool.
    Each asset is serialized via :class:`~interloper.dag.spec.DAGSpec`,
    reconstructed in a child process, and materialized there.  State
    tracking remains in the main process.

    Use this runner when assets perform CPU-bound work or when
    you need true parallelism (bypassing the GIL)::

        with MultiProcessRunner(max_workers=2, on_event=log_event) as runner:
            result = runner.run(dag)
    """

    max_workers: int = 4
    fail_fast: bool = True
    reraise: bool = False

    _pool: ProcessPoolExecutor | None = PrivateAttr(default=None)
    _futures: dict[Future[Any], Asset] = PrivateAttr(default_factory=dict)
    _dag_spec: dict[str, Any] | None = PrivateAttr(default=None)

    @property
    def _capacity(self) -> int:
        return self.max_workers

    def _on_start(self) -> None:
        self._pool = ProcessPoolExecutor(max_workers=self.max_workers)
        self._dag_spec = self.state.dag.to_spec().model_dump(mode="json")

    def _on_end(self) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=True, cancel_futures=False)
            self._pool = None
        self._futures.clear()
        self._dag_spec = None

    def _submit_asset(
        self,
        asset: Asset,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> Future[Any]:
        """Submit asset for execution in a child process.

        Returns:
            A Future representing the worker result.

        Raises:
            RunnerError: If the process pool is not initialized.
        """
        if self._pool is None or self._dag_spec is None:
            raise RunnerError("Process pool not initialized")

        self.state.mark_asset_running(asset)

        future = self._pool.submit(
            _worker,
            asset.id,
            self._dag_spec,
            partition_or_window,
            self.state.metadata,
        )
        self._futures[future] = asset
        return future

    def _handle_completed(self, future: Future[Any], asset: Asset) -> None:
        """Process a completed future from a worker process.

        Unlike the base ``_handle_completed``, this interprets the
        ``(id, success, error_msg, tb)`` tuple returned by ``_worker``.

        Raises:
            RunnerError: If the asset failed and ``fail_fast`` or ``reraise`` is set.
        """
        self._futures.pop(future, None)

        try:
            _key, success, error_msg, tb = future.result()
        except Exception as e:
            self.state.mark_asset_failed(asset, str(e), tb=traceback.format_exc())
            if self.fail_fast or self.reraise:
                raise
            return

        if success:
            self.state.mark_asset_completed(asset)
        else:
            self.state.mark_asset_failed(asset, error_msg or "Unknown error", tb=tb)
            if self.fail_fast or self.reraise:
                raise RunnerError(f"Asset '{type(asset).key}' failed: {error_msg}")

    def _handle_flushed(self, future: Future[Any], asset: Asset) -> None:
        """Interpret the worker ``(id, success, error_msg, tb)`` tuple during flush."""
        try:
            _key, success, error_msg, tb = future.result()
        except Exception as e:  # noqa: BLE001
            self.state.mark_asset_failed(asset, str(e), tb=traceback.format_exc())
            return

        if success:
            self.state.mark_asset_completed(asset)
        else:
            self.state.mark_asset_failed(asset, error_msg or "Unknown error", tb=tb)
