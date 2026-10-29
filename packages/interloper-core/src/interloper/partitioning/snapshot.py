"""Snapshot partitioning: the period an asset observed, whatever window its run was given.

An asset that reads a vendor's *current* state (an entity: its ads, its campaigns) cannot read
the past, so the partition it writes is the period in which it looked, never the window of its
run. A `SnapshotPartitionConfig` accepts any window and ignores it, as an unpartitioned asset
does; the one partition it writes is ``granularity.truncate(observed_at)``, the period of the
run's observation instant. The run states that instant in its metadata, under ``observed_at``,
as an ISO-8601 instant with its offset; a run without one observes now, in UTC. A rerun of a
past window therefore writes the current period, and a second observation within one period
replaces the first: a coarser period holds the latest observation taken within it.

A snapshot is a time partitioning, with the same keys, filters and slices, so destinations
store it as any other; only the period a run writes differs. Its ``start`` is the first period
it is owed: coverage counts every period since as owed, and one that ended without an
observation as missed. The instant is aware, and its zone is the clock: a day, month or year is
the zone's, while an hour is UTC's, as hourly windows are (see `TimeGranularity`).
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, ClassVar

from interloper.errors import PartitionError
from interloper.partitioning.base import PartitionConfig, PartitionWindow
from interloper.partitioning.time import TimePartition, TimePartitionConfig, TimePartitionWindow


@dataclass(frozen=True, kw_only=True)
class SnapshotPartitionConfig(TimePartitionConfig):
    """Partitioning by the period an asset observed, at one granularity.

    Attributes:
        start: Optional first period the asset is owed. It bounds no window, since a snapshot
            ignores its run's; coverage owes every period since, and a period that ended
            without an observation is missed.
    """

    kind: ClassVar[str] = "snapshot"
    windowed: ClassVar[bool] = False

    def __str__(self) -> str:
        """Describe the partitioning, as an error message names it.

        Returns:
            ``snapshot-partitioned by <granularity>``, e.g. ``snapshot-partitioned by day``.
        """
        return f"snapshot-partitioned by {self.granularity.value}"

    def check(self, window: PartitionWindow | None) -> None:
        """Accept any window, or none: the run's window is not what a snapshot writes.

        Args:
            window: The run's window, ignored.
        """

    def _check(self, window: PartitionWindow) -> None:
        """Accept any window (see `check`).

        Args:
            window: The run's window, ignored.
        """

    def effective_window(self, window: PartitionWindow | None, metadata: Mapping[str, Any]) -> TimePartitionWindow:
        """The one period the run observes in, whatever its window.

        Args:
            window: The run's window, ignored.
            metadata: The run's metadata, whose ``observed_at`` is the instant observed: an
                ISO-8601 instant with its offset, whose zone decides a day, month or year (an
                hour is UTC's). Without one, the run observes now, in UTC.

        Returns:
            The one-partition window of the period holding the instant.

        Raises:
            PartitionError: If ``observed_at`` carries no offset.
        """
        value = metadata.get("observed_at")
        observed_at = dt.datetime.fromisoformat(value) if value else dt.datetime.now(dt.timezone.utc)
        if observed_at.tzinfo is None:
            raise PartitionError(f"The run's observed_at needs its offset (e.g. +01:00 or Z): {value!r}")
        return TimePartition(observed_at, self.granularity).window

    def compatible(self, other: PartitionConfig) -> bool:
        """Whether *other* is a snapshot partitioning of the same granularity.

        Two snapshots of one granularity observe the same period in a run, so the reader finds
        its upstream's observation; anything partitioned on the run's window holds another one.

        Args:
            other: The partitioning at the other end of a DAG edge.

        Returns:
            True when both are snapshots of the same granularity.
        """
        return isinstance(other, SnapshotPartitionConfig) and other.granularity is self.granularity
