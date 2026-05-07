"""Time-based (daily) partitioning."""

from __future__ import annotations

import datetime as dt
from collections.abc import Generator, Iterator
from dataclasses import dataclass

from interloper.partitioning.base import Partition, PartitionConfig, PartitionWindow


def date_range(start_date: dt.date, end_date: dt.date, reversed: bool = False) -> Generator[dt.date, None, None]:
    """Yield each date from *start_date* to *end_date* inclusive.

    Args:
        start_date: Start of the range.
        end_date: End of the range.
        reversed: When True, yield dates from end to start.
    """
    if reversed:
        while end_date >= start_date:
            yield end_date
            end_date -= dt.timedelta(days=1)
    else:
        while start_date <= end_date:
            yield start_date
            start_date += dt.timedelta(days=1)


@dataclass(frozen=True)
class TimePartitionConfig(PartitionConfig):
    """Partition config with an optional start date bound."""

    start_date: dt.date | None = None


@dataclass(frozen=True)
class TimePartition(Partition):
    """A single date-based partition."""

    value: dt.date

    def __repr__(self) -> str:
        """Return ISO-formatted date string."""
        return self.value.isoformat()

    @property
    def id(self) -> str:
        """ISO-formatted date string."""
        return self.value.isoformat()


@dataclass(frozen=True)
class TimePartitionWindow(PartitionWindow):
    """A date-range window that yields daily ``TimePartition`` instances."""

    start: dt.date
    end: dt.date

    def __iter__(self) -> Iterator[TimePartition]:
        """Iterate over partitions from most recent to oldest.

        Yields:
            Each ``TimePartition`` in the window.
        """
        yield from self.iter_partitions()

    def __str__(self) -> str:
        """Return ``start:end`` in ISO format."""
        return f"{self.start.isoformat()}:{self.end.isoformat()}"

    def __repr__(self) -> str:
        """Return ``start to end`` in ISO format."""
        return f"{self.start.isoformat()} to {self.end.isoformat()}"

    def iter_partitions(self) -> Generator[TimePartition, None, None]:
        """Yield partitions from end to start (most recent first)."""
        for date in date_range(self.start, self.end, reversed=True):
            yield TimePartition(date)

    def partition_count(self) -> int:
        """Return the number of days in the window (inclusive)."""
        return (self.end - self.start).days + 1
