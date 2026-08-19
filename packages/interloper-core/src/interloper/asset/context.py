"""Runtime context for asset execution."""

from __future__ import annotations

import datetime as dt
from typing import Any

from interloper.events import EventLogger
from interloper.partitioning.base import Partition, PartitionConfig, PartitionWindow
from interloper.partitioning.time import TimeGranularity, TimePartitionConfig, TimePartitionWindow


class ExecutionContext:
    """Runtime context providing access to partitions and metadata.

    Created fresh on each ``asset.run()`` call and passed to the asset
    function if the signature includes a ``context`` parameter.
    """

    def __init__(
        self,
        asset_key: str,
        partitioning: PartitionConfig | None = None,
        partition_or_window: Partition | PartitionWindow | None = None,
        metadata: dict[str, Any] | None = None,
        asset_id: str | None = None,
        source_id: str | None = None,
    ) -> None:
        """Initialize the execution context.

        Args:
            asset_key: The qualified key of the current asset.
            partitioning: The partitioning configuration for the asset.
            partition_or_window: The partition or partition window for the asset.
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).
            asset_id: Id of the current asset, propagated onto ``LOG`` events.
            source_id: Id of the source the asset belongs to, if any.
        """
        self._asset_key = asset_key
        self._partitioning = partitioning
        self._partition_or_window = partition_or_window
        self._metadata = metadata or {}
        self._asset_id = asset_id
        self._source_id = source_id
        self._logger: EventLogger | None = None

    @property
    def asset_key(self) -> str:
        """The qualified key of the current asset."""
        return self._asset_key

    @property
    def metadata(self) -> dict[str, Any]:
        """Arbitrary metadata dict (e.g. run_id, backfill_id)."""
        return self._metadata

    @property
    def logger(self) -> EventLogger:
        """Logger that emits messages as events on the event bus."""
        if self._logger is None:
            self._logger = EventLogger(
                self._asset_key,
                self._metadata,
                asset_id=self._asset_id,
                source_id=self._source_id,
            )
        return self._logger

    @property
    def partition(self) -> Partition:
        """The partition this run covers.

        The partition answers everything about its own scope: ``value`` (the
        period's start), ``id`` (its canonical key), ``granularity`` and
        ``bounds`` (its half-open extent) for a time partition. Nothing here
        re-derives per granularity what the partition already knows.

        Raises:
            AttributeError: If the asset is not partitioned, no partition is
                provided, or the context holds a window.
        """
        if self._partitioning is None:
            raise AttributeError("`context.partition` is not available, asset is not partitioned.")

        return self._require_single_partition("partition")

    @property
    def window(self) -> PartitionWindow:
        """The window this run covers, for an asset that declares ``allow_window``.

        A single partition normalizes to a one-partition window, because a
        windowed asset is still run per partition by the platform: reading
        ``window`` must not depend on how the run was scoped.

        Raises:
            AttributeError: If the asset is not time-partitioned, no partition
                is provided, or windows are not allowed.
        """
        partitioning = self._time_partitioning("window")

        if self._partition_or_window is None:
            raise AttributeError("`context.window` is not available, no partition provided.")

        if not partitioning.allow_window:
            raise AttributeError(
                "`context.window` is not available, asset does not allow windows. "
                "Set `allow_window=True` in `TimePartitionConfig` to enable windowed partitions."
            )

        if isinstance(self._partition_or_window, PartitionWindow):
            return self._partition_or_window

        value = self._partition_or_window.value
        return TimePartitionWindow(value, value, partitioning.granularity)

    @property
    def partition_date(self) -> dt.date:
        """The partition value as a datetime.date object.

        The typed reading of ``context.partition.value``, which is annotated
        ``Any`` on the base class: this is the only accessor that hands an
        asset a real ``date`` without narrowing a type first, and it asserts
        the daily granularity rather than assuming it. Any other granularity
        reads :attr:`partition` (or :attr:`window`) and asks the partition
        itself.

        Raises:
            AttributeError: If the asset is not time-partitioned, is
                partitioned at another granularity, or no partition is provided.
        """
        partitioning = self._time_partitioning("partition_date")
        partition = self._require_single_partition("partition_date")

        if partitioning.granularity is not TimeGranularity.DAY:
            raise AttributeError(
                f"`context.partition_date` is not available, asset is partitioned by "
                f"{partitioning.granularity.value}. Use `context.partition` instead."
            )

        return partition.value

    # -- Internals -------------------------------------------------------------

    def _time_partitioning(self, accessor: str) -> TimePartitionConfig:
        """Return the asset's time partition config, or explain why there is none.

        Returns:
            The asset's ``TimePartitionConfig``.

        Raises:
            AttributeError: If the asset is not time-partitioned.
        """
        if self._partitioning is None:
            raise AttributeError(f"`context.{accessor}` is not available, asset is not partitioned.")

        if not isinstance(self._partitioning, TimePartitionConfig):
            raise AttributeError(  # noqa: TRY004
                f"`context.{accessor}` is not available, asset is not time-partitioned. "
                "Use `TimePartitionConfig` in the asset decorator."
            )

        return self._partitioning

    def _require_single_partition(self, accessor: str) -> Partition:
        """Return the single partition in scope, or explain why there is none.

        Returns:
            The context's ``Partition``.

        Raises:
            AttributeError: If no partition is provided, or the context holds
                a window rather than a single partition.
        """
        if self._partition_or_window is None:
            raise AttributeError(f"`context.{accessor}` is not available, no partition provided.")

        if isinstance(self._partition_or_window, PartitionWindow):
            raise AttributeError(  # noqa: TRY004
                f"`context.{accessor}` is not available. "
                "Context currently holds a partition window, not a partition."
            )

        return self._partition_or_window
