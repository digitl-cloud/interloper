"""Multi-thread runner — executes assets concurrently using a thread pool."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from pydantic import PrivateAttr

from interloper.asset.base import Asset
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runner.sync_runner import SyncRunner


class MultiThreadRunner(SyncRunner):
    """Execute assets concurrently using a thread pool.

    Good for IO-bound workloads where assets spend most of their time
    waiting on network or disk::

        with MultiThreadRunner(max_workers=4, on_event=log_event) as runner:
            result = runner.run(dag)
    """

    max_workers: int = 4
    fail_fast: bool = True
    reraise: bool = False

    _pool: ThreadPoolExecutor | None = PrivateAttr(default=None)

    @property
    def _capacity(self) -> int:
        return self.max_workers

    def _on_start(self) -> None:
        self._pool = ThreadPoolExecutor(max_workers=self.max_workers)

    def _on_end(self) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=True, cancel_futures=False)
            self._pool = None

    def _submit_asset(
        self,
        asset: Asset,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> Future[Any]:
        assert self._pool is not None
        self.state.mark_asset_running(asset)
        return self._pool.submit(self._execute_asset, asset, partition_or_window)
