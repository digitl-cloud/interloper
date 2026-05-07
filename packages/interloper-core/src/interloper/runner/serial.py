"""Serial runner — executes assets one at a time in dependency order."""

from __future__ import annotations

from concurrent.futures import Future
from typing import Any

from interloper.asset.base import Asset
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runner.sync_runner import SyncRunner


class SerialRunner(SyncRunner):
    """Execute assets one at a time in dependency order.

    The simplest runner — deterministic, easy to debug::

        with SerialRunner(on_event=log_event) as runner:
            result = runner.run(dag)
    """

    fail_fast: bool = True
    reraise: bool = False

    @property
    def _capacity(self) -> int:
        return 1

    def _submit_asset(
        self,
        asset: Asset,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> Future[Any]:
        """Execute the asset inline and return an already-completed Future.

        Returns:
            A resolved Future containing the result.
        """
        self.state.mark_asset_running(asset)

        future: Future[Any] = Future()
        try:
            result = self._execute_asset(asset, partition_or_window)
            future.set_result(result)
        except Exception as e:  # noqa: BLE001
            future.set_exception(e)
        return future
