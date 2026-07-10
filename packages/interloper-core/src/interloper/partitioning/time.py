"""Time-based (daily) partitioning."""

from __future__ import annotations

import datetime as dt
from collections.abc import Generator, Iterator
from dataclasses import dataclass

from interloper.partitioning.base import Partition, PartitionConfig, PartitionWindow

# -- Date helpers --------------------------------------------------------------


def coerce_to_date(value: object) -> dt.date:
    """Coerce a partition value to a ``datetime.date``.

    Accepts a ``date``, a ``datetime`` (its date part is used), or an ISO-8601
    date string. Anything else raises ``TypeError``.

    Args:
        value: The partition value to coerce.

    Returns:
        The value as a ``datetime.date``.

    Raises:
        TypeError: If the value cannot be interpreted as a date.
    """
    # `datetime` is a subclass of `date`, so check it first.
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if isinstance(value, str):
        try:
            return dt.date.fromisoformat(value)
        except ValueError as e:
            raise TypeError(
                f"Could not parse partition value {value!r} as a date: expected an ISO-8601 date string (YYYY-MM-DD)."
            ) from e
    raise TypeError(
        f"Time partition value must be a `datetime.date` or an ISO-8601 date string, got {type(value).__name__}: "
        f"{value!r}."
    )


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


# -- Time partitions -----------------------------------------------------------


@dataclass(frozen=True)
class TimePartitionConfig(PartitionConfig):
    """Partition config with an optional start date bound."""

    start_date: dt.date | None = None


@dataclass(frozen=True)
class TimePartition(Partition):
    """A single date-based partition."""

    value: dt.date

    def __post_init__(self) -> None:
        """Normalize the value to a ``datetime.date``, coercing strings/datetimes."""
        coerced = coerce_to_date(self.value)
        if coerced is not self.value:
            object.__setattr__(self, "value", coerced)

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
