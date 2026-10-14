"""Runtime context for asset execution."""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any

from interloper.events import EventLogger
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.partitioning.time import TimeGranularity, TimePartition, TimePartitionConfig

if TYPE_CHECKING:
    from interloper.asset.base import Asset


class ExecutionContext:
    """What an asset's ``data()`` knows about the run it executes in.

    Built by the asset for each run from itself, the run's window and the run metadata, and
    handed to ``data()`` as its ``context`` parameter. Each partition accessor raises an
    ``AttributeError`` naming why it is unavailable: the asset is not partitioned, no window was
    given, the window holds several partitions, or the granularity is not daily.
    """

    def __init__(
        self,
        asset: Asset,
        window: PartitionWindow | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Bind the context to one run of one asset.

        Args:
            asset: The asset executing.
            window: The window the run covers, ``None`` for an unpartitioned asset.
            metadata: Run metadata (run id, backfill id), carried onto every event the asset
                emits; ``None`` is an empty dict.
        """
        self._asset = asset
        self._window = window
        self._metadata = metadata or {}
        self._logger: EventLogger | None = None

    @property
    def asset(self) -> Asset:
        """The asset executing: its key, table, dataset, source and partitioning."""
        return self._asset

    @property
    def metadata(self) -> dict[str, Any]:
        """The run metadata (run id, backfill id, anything the caller passed)."""
        return self._metadata

    @property
    def logger(self) -> EventLogger:
        """Logger whose messages become ``LOG`` events attributed to the asset."""
        if self._logger is None:
            self._logger = EventLogger(self._asset, self._metadata, self._window)
        return self._logger

    @property
    def window(self) -> PartitionWindow:
        """The window this run covers, of one partition unless the asset allows windows."""
        return self._require_window("window")

    @property
    def partition(self) -> Partition:
        """The partition this run covers: the window's only one.

        The partition answers everything about its own scope: ``id``, ``bounds`` and
        ``granularity`` for a time partition, ``value`` for a list one.

        Raises:
            AttributeError: If the asset is not partitioned, no window was given, or the window
                covers several partitions.
        """
        window = self._require_window("partition")
        if len(window) > 1:
            raise AttributeError(f"`context.partition` is not available, the run covers {len(window)} partitions.")
        return window.first

    @property
    def partition_date(self) -> dt.date:
        """The partition's day, for daily time partitioning only.

        The typed reading of ``context.partition.value``: the one accessor that hands an asset a
        real ``date`` without narrowing a type first, and it asserts the daily granularity rather
        than assuming it.

        Raises:
            AttributeError: If the asset is not time-partitioned at the daily granularity, no
                window was given, or the window covers several partitions.
        """
        partitioning = self._asset.partitioning
        if partitioning is None:
            raise AttributeError("`context.partition_date` is not available, asset is not partitioned.")
        if not isinstance(partitioning, TimePartitionConfig):
            raise AttributeError(  # noqa: TRY004 - the accessor is unavailable, not mistyped
                "`context.partition_date` is not available, asset is not time-partitioned."
            )
        if partitioning.granularity is not TimeGranularity.DAY:
            raise AttributeError(
                f"`context.partition_date` is not available, asset is partitioned by "
                f"{partitioning.granularity.value}. Use `context.partition` instead."
            )
        window = self._require_window("partition_date")
        if len(window) > 1:
            raise AttributeError(f"`context.partition_date` is not available, the run covers {len(window)} partitions.")
        partition = window.first
        assert isinstance(partition, TimePartition)
        return partition.value

    def _require_window(self, accessor: str) -> PartitionWindow:
        """Return the run's window, or explain why there is none.

        Args:
            accessor: Name of the calling accessor, quoted back in the error message.

        Returns:
            The window.

        Raises:
            AttributeError: If the asset is not partitioned, or no window was given.
        """
        if self._asset.partitioning is None:
            raise AttributeError(f"`context.{accessor}` is not available, asset is not partitioned.")
        if self._window is None:
            raise AttributeError(f"`context.{accessor}` is not available, no window provided.")
        return self._window
