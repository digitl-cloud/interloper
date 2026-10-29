"""Time partitioning: periods identified by their start, at a declared granularity.

A time partition is a **period identified by its start**: the value is the period's first
instant, and the `TimeGranularity` says how long the period lasts. The granularities an asset may
declare are the ones BigQuery time partitioning offers: hourly, daily, monthly, yearly. Every
piece of time arithmetic goes through the granularity, so nothing outside this module hardcodes a
period length.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Generator
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar

from interloper.errors import PartitionError
from interloper.partitioning.base import Partition, PartitionConfig, PartitionFilter, PartitionWindow
from interloper.utils import add_months, coerce_to_date, coerce_to_datetime

# -- Granularity ---------------------------------------------------------------


class TimeGranularity(str, Enum):
    """The length of the period one time partition covers.

    Partition keys are ISO-8601 prefixes (``2026``, ``2026-08``, ``2026-08-21``, ``2026-08-21T13``):
    they sort chronologically as strings, embed in paths unchanged, and each shape names its
    granularity, which is what `TimePartition.from_key` relies on.
    """

    HOUR = "hour"
    DAY = "day"
    MONTH = "month"
    YEAR = "year"

    @property
    def key_format(self) -> str:
        """The ``strptime`` shape of this granularity's partition key."""
        return {
            TimeGranularity.HOUR: "%Y-%m-%dT%H",
            TimeGranularity.DAY: "%Y-%m-%d",
            TimeGranularity.MONTH: "%Y-%m",
            TimeGranularity.YEAR: "%Y",
        }[self]

    def coerce(self, value: object) -> dt.date:
        """Coerce a raw value to this granularity's value type.

        Args:
            value: A raw period label: a ``date``, a ``datetime``, or an ISO-8601 string.

        Returns:
            A ``datetime`` for the hourly granularity, a ``date`` otherwise.
        """
        if self is TimeGranularity.HOUR:
            return coerce_to_datetime(value)
        return coerce_to_date(value)

    def truncate(self, value: object) -> dt.date:
        """Reduce a value to the start of the period containing it.

        Args:
            value: Any value this granularity can coerce, at any offset inside the period.

        Returns:
            The period's first instant.
        """
        coerced = self.coerce(value)
        if self is TimeGranularity.HOUR:
            assert isinstance(coerced, dt.datetime)
            return coerced.replace(minute=0, second=0, microsecond=0)
        if self is TimeGranularity.DAY:
            return coerced
        if self is TimeGranularity.MONTH:
            return coerced.replace(day=1)
        return coerced.replace(month=1, day=1)

    def advance(self, value: object, periods: int) -> dt.date:
        """Shift a value by *periods* whole periods (negative goes back).

        Args:
            value: The starting value; truncated to its period before shifting.
            periods: How many whole periods to shift by, negative to go back.

        Returns:
            The start of the resulting period.
        """
        start = self.truncate(value)
        if self is TimeGranularity.HOUR:
            return start + dt.timedelta(hours=periods)
        if self is TimeGranularity.DAY:
            return start + dt.timedelta(days=periods)
        if self is TimeGranularity.MONTH:
            return add_months(start, periods)
        return add_months(start, 12 * periods)

    def bounds(self, value: object) -> tuple[dt.date, dt.date]:
        """Return the period's ``(start, end_exclusive)`` bounds.

        Half-open bounds are what range-scoped reads and deletes need: a partition covering more
        (or less) than one row of a ``DATE`` column cannot be selected by equality.

        Args:
            value: Any value inside the period whose bounds to return.

        Returns:
            The period's start and its exclusive end.
        """
        start = self.truncate(value)
        return start, self.advance(start, 1)

    def periods_between(self, start: object, end: object) -> int:
        """Return how many periods separate two values (``end`` minus ``start``).

        Args:
            start: The value the count starts from; truncated to its period.
            end: The value the count runs to; truncated to its period.

        Returns:
            The number of steps from *start*'s period to *end*'s period, negative when *end*
            precedes *start*.
        """
        first = self.truncate(start)
        last = self.truncate(end)
        if self is TimeGranularity.HOUR:
            assert isinstance(first, dt.datetime) and isinstance(last, dt.datetime)
            return int((last - first).total_seconds() // 3600)
        if self is TimeGranularity.DAY:
            return (last - first).days
        months = (last.year - first.year) * 12 + (last.month - first.month)
        if self is TimeGranularity.MONTH:
            return months
        return last.year - first.year

    def format(self, value: object) -> str:
        """Render a value as a partition key.

        Args:
            value: Any value inside the period the key names.

        Returns:
            The canonical key of the period containing *value*.
        """
        return self.truncate(value).strftime(self.key_format)

    def parse(self, key: str) -> dt.date:
        """Parse a partition key back into the period's start.

        Args:
            key: A partition key in this granularity's own format (see `key_format`).

        Returns:
            The period's first instant.

        Raises:
            ValueError: If *key* is not this granularity's format.
        """
        try:
            parsed = dt.datetime.strptime(key, self.key_format)
        except ValueError as error:
            raise ValueError(
                f"Partition key {key!r} is not a {self.value} key (expected the shape {self.key_format!r})."
            ) from error
        return parsed if self is TimeGranularity.HOUR else parsed.date()

    def period_range(self, start: dt.date, end: dt.date) -> Generator[dt.date, None, None]:
        """Yield the start of each period from *start* to *end* inclusive.

        Args:
            start: Start of the range (truncated to this granularity).
            end: End of the range (truncated to this granularity).

        Yields:
            Each period's first instant.
        """
        first = self.truncate(start)
        last = self.truncate(end)
        while first <= last:
            yield first
            first = self.advance(first, 1)


# -- Time partitions -----------------------------------------------------------


@dataclass(frozen=True)
class TimePartition(Partition):
    """A single time partition, identified by the start of its period.

    Attributes:
        value: The period's start, normalised on construction.
        granularity: The period one partition covers.
    """

    value: dt.date
    granularity: TimeGranularity = TimeGranularity.DAY

    @classmethod
    def from_key(cls, key: str) -> TimePartition:
        """Parse a partition key, inferring its granularity from the shape.

        The key shapes are mutually unambiguous, so the key alone carries the granularity: storage
        and transport need one string, not a pair.

        Args:
            key: A partition key in any granularity's format.

        Returns:
            The partition the key names.

        Raises:
            PartitionError: If *key* matches no known key shape.
        """
        for granularity in TimeGranularity:
            try:
                return cls(granularity.parse(key), granularity)
            except ValueError:
                continue
        shapes = ", ".join(repr(granularity.key_format) for granularity in TimeGranularity)
        raise PartitionError(f"Partition key {key!r} matches no granularity (known shapes: {shapes}).")

    def __post_init__(self) -> None:
        """Normalise the value to the start of its period."""
        object.__setattr__(self, "value", self.granularity.truncate(self.value))

    @property
    def key(self) -> str:
        """The partition's canonical key (see `TimeGranularity.format`)."""
        return self.granularity.format(self.value)

    @property
    def bounds(self) -> tuple[dt.date, dt.date]:
        """The partition's ``(start, end_exclusive)`` bounds."""
        return self.granularity.bounds(self.value)

    @property
    def window(self) -> TimePartitionWindow:
        """The one-partition window covering only this period."""
        return TimePartitionWindow(self.value, self.value, self.granularity)

    def filter(self, column: str) -> PartitionFilter:
        """The rows inside this period.

        Rows may carry values anywhere inside the period (a monthly partition whose rows hold
        daily dates), so the filter is the half-open bounds, never the key.

        Args:
            column: The partition column the rows carry.

        Returns:
            A bounds filter on the period.
        """
        return PartitionFilter(column, bounds=self.bounds)

    def slice(self, data: Any, column: str) -> Any:
        """Return this partition's slice of *data*, selecting by its half-open bounds.

        Args:
            data: The table to slice, in any registered representation's type.
            column: Name of the partition column to select on.

        Returns:
            The partition's slice of the data.
        """
        from interloper.representation import TableData

        return TableData.from_data(data).filter_range(column, *self.bounds)


@dataclass(frozen=True)
class TimePartitionWindow(PartitionWindow):
    """A contiguous range of time partitions, inclusive of both bounds.

    Attributes:
        start: The first period's start.
        end: The last period's start.
        granularity: The period one partition covers.
    """

    start: dt.date
    end: dt.date
    granularity: TimeGranularity = TimeGranularity.DAY

    def __post_init__(self) -> None:
        """Normalise both bounds to period starts.

        Raises:
            ValueError: If the window ends before it starts.
        """
        object.__setattr__(self, "start", self.granularity.truncate(self.start))
        object.__setattr__(self, "end", self.granularity.truncate(self.end))
        if self.start > self.end:
            raise ValueError(f"Time partition window ends before it starts: {self.start} to {self.end}.")

    @property
    def first(self) -> TimePartition:
        """The window's first partition."""
        return TimePartition(self.start, self.granularity)

    @property
    def last(self) -> TimePartition:
        """The window's last partition."""
        return TimePartition(self.end, self.granularity)

    @property
    def partitions(self) -> list[Partition]:
        """Every period from ``start`` to ``end`` inclusive, oldest first."""
        return [TimePartition(value, self.granularity) for value in self.granularity.period_range(self.start, self.end)]

    @property
    def bounds(self) -> tuple[dt.date, dt.date]:
        """The window's ``(start, end_exclusive)`` bounds: the first period's start to the last's end."""
        return self.start, self.granularity.advance(self.end, 1)

    def __len__(self) -> int:
        """Count the window's partitions.

        Returns:
            The number of periods from ``start`` to ``end`` inclusive, at least 1.
        """
        return self.granularity.periods_between(self.start, self.end) + 1

    def filter(self, column: str) -> PartitionFilter:
        """The rows inside the whole window.

        Args:
            column: The partition column the rows carry.

        Returns:
            A bounds filter spanning every period of the window.
        """
        return PartitionFilter(column, bounds=self.bounds)

    @classmethod
    def lookback(
        cls,
        now: dt.date,
        lookback: int,
        offset: int = 1,
        granularity: TimeGranularity = TimeGranularity.DAY,
        start: dt.date | None = None,
    ) -> TimePartitionWindow | None:
        """Build the trailing window a scheduled workload should cover.

        The window is counted in partitions, never in days: *offset* is how many partitions back
        from the current one the window ends, and *lookback* is how many it spans. At daily
        granularity, ``offset=1, lookback=1`` is "yesterday only" and ``offset=0`` includes the
        current (still incomplete) partition.

        The caller's timezone reaches the window through *now*: passing an aware datetime
        localized to a zone makes DAY, MONTH and YEAR windows follow that zone's calendar. HOUR
        windows are the deliberate exception: an aware *now* is normalized back to UTC (see
        `coerce_to_datetime`), because hour partition keys are naive-UTC labels and zones on a
        fractional offset do not align to UTC hour boundaries at all.

        Args:
            now: The reference instant, in whatever timezone the caller labels partitions with.
            lookback: How many partitions the window spans (at least 1).
            offset: How many partitions back from the current one the window ends.
            granularity: The period one partition covers.
            start: Optional lower bound (an asset's earliest partition) to clamp the window to.

        Returns:
            The window, or ``None`` if clamping to *start* leaves it empty.

        Raises:
            ValueError: If *lookback* is below 1, or *offset* is negative.
        """
        if lookback < 1:
            raise ValueError(f"lookback must cover at least one partition, got {lookback}.")
        if offset < 0:
            raise ValueError(f"offset cannot be negative, got {offset}.")

        end = granularity.advance(now, -offset)
        first = granularity.advance(end, -(lookback - 1))

        if start is not None:
            bound = granularity.truncate(start)
            if end < bound:
                return None
            first = max(first, bound)

        return cls(first, end, granularity)


@dataclass(frozen=True, kw_only=True)
class PeriodPartitionConfig(PartitionConfig):
    """Partitioning by time periods of one granularity: the space the time and snapshot kinds share.

    The kinds differ in which period a run writes: the run's window for `TimePartitionConfig`,
    the period it observed in for `interloper.partitioning.snapshot.SnapshotPartitionConfig`.
    Their keys, partitions and windows are the same.

    Attributes:
        granularity: The period one partition covers.
    """

    granularity: TimeGranularity = TimeGranularity.DAY

    def __post_init__(self) -> None:
        """Coerce what a definition round trip loosens: the granularity name."""
        object.__setattr__(self, "granularity", TimeGranularity(self.granularity))

    def partition(self, key: str) -> TimePartition:
        """Parse a key at this granularity.

        Args:
            key: The partition key.

        Returns:
            The partition.

        Raises:
            PartitionError: If the key is not of this granularity's shape.
        """
        try:
            return TimePartition(self.granularity.parse(key), self.granularity)
        except ValueError as error:
            raise PartitionError(str(error)) from None

    def _window(self, first: Partition, last: Partition) -> TimePartitionWindow:
        """Build the window between two periods.

        Args:
            first: The first partition.
            last: The last partition.

        Returns:
            The window.

        Raises:
            PartitionError: If *last* precedes *first*.
        """
        assert isinstance(first, TimePartition) and isinstance(last, TimePartition)
        if last.value < first.value:
            raise PartitionError(f"window ends before it starts: {first.key!r} to {last.key!r}")
        return TimePartitionWindow(first.value, last.value, self.granularity)


@dataclass(frozen=True, kw_only=True)
class TimePartitionConfig(PeriodPartitionConfig):
    """Partitioning by time periods of one granularity, the run's window naming the ones it writes.

    Attributes:
        start: Optional lower bound. Windows reaching before it are rejected, and trailing
            windows built against this asset are clamped to it.
    """

    kind: ClassVar[str] = "time"

    start: dt.date | None = None

    def __post_init__(self) -> None:
        """Coerce what a definition round trip loosens: the granularity name and the start."""
        super().__post_init__()
        if self.start is not None:
            object.__setattr__(self, "start", self.granularity.truncate(self.start))

    def __str__(self) -> str:
        """Describe the partitioning, as an error message names it.

        Returns:
            ``partitioned by <granularity>``, e.g. ``partitioned by day``.
        """
        return f"partitioned by {self.granularity.value}"

    def _check(self, window: PartitionWindow) -> None:
        """Require a time window of this granularity that does not reach before ``start``.

        Args:
            window: The run's window.

        Raises:
            PartitionError: If the window is not a time window, has another granularity, or
                starts before ``start``.
        """
        if not isinstance(window, TimePartitionWindow):
            raise PartitionError(f"expects a time window, but the run was given a {type(window).__name__}")
        if window.granularity is not self.granularity:
            raise PartitionError(
                f"is partitioned by {self.granularity.value}, but the run was given a {window.granularity.value} window"
            )
        if self.start is not None and window.start < self.start:
            raise PartitionError(
                f"has no data before {self.start.isoformat()}, but the run reaches back to {window.start.isoformat()}"
            )

    def compatible(self, other: PartitionConfig) -> bool:
        """Whether *other* is a time partitioning of the same granularity.

        Args:
            other: The partitioning at the other end of a DAG edge.

        Returns:
            True when both share the granularity.
        """
        return isinstance(other, TimePartitionConfig) and other.granularity is self.granularity
