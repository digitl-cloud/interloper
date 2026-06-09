"""Runtime context for asset execution."""

from __future__ import annotations

import datetime as dt
from typing import Any

from interloper.events import EventLogger
from interloper.partitioning.base import Partition, PartitionConfig, PartitionWindow
from interloper.partitioning.time import TimePartitionConfig


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
    def partition_date(self) -> dt.date:
        """The partition value as a datetime.date object.

        Only available for time-based partitioning with a single partition.

        Raises:
            AttributeError: If the asset is not time-partitioned or no partition is provided.
        """
        if self._partitioning is None:
            raise AttributeError("`context.partition_date` is not available, asset is not partitioned.")

        if self._partition_or_window is None:
            raise AttributeError("`context.partition_date` is not available, no partition provided.")

        if not isinstance(self._partitioning, TimePartitionConfig):
            raise AttributeError(  # noqa: TRY004
                "`context.partition_date` is not available, asset is not time-partitioned. "
                "Use `TimePartitionConfig` in the asset decorator."
            )

        if isinstance(self._partition_or_window, PartitionWindow):
            raise AttributeError(  # noqa: TRY004
                "`context.partition_date` is not available. "
                "Context currently holds a partition window, not a partition."
            )

        return self._partition_or_window.value

    @property
    def partition_date_window(self) -> tuple[dt.date, dt.date]:
        """A tuple of (start_date, end_date) representing a date range.

        Only available for TimePartitionConfig with allow_window=True.

        Raises:
            AttributeError: If the asset is not time-partitioned or windows are not allowed.
        """
        if self._partitioning is None:
            raise AttributeError("`context.partition_date_window` is not available, asset is not partitioned.")

        if self._partition_or_window is None:
            raise AttributeError("`context.partition_date_window` is not available, no partition provided.")

        if not isinstance(self._partitioning, TimePartitionConfig):
            raise AttributeError(  # noqa: TRY004
                "`context.partition_date_window` is not available, asset is not time-partitioned. "
                "Use `TimePartitionConfig` in the asset decorator."
            )

        if not self._partitioning.allow_window:
            raise AttributeError(
                "`context.partition_date_window` is not available, asset does not allow windows. "
                "Set `allow_window=True` in `TimePartitionConfig` to enable windowed partitions."
            )

        if isinstance(self._partition_or_window, Partition):
            return (self._partition_or_window.value, self._partition_or_window.value)

        assert isinstance(self._partition_or_window, PartitionWindow)
        return (self._partition_or_window.start, self._partition_or_window.end)
