"""Time-based partitioning.

A time partition is a **period identified by its start**: the value is the
period's first instant, and the :class:`TimeGranularity` says how long the
period lasts. The granularities an asset may declare are the ones BigQuery
time partitioning offers — hourly, daily, monthly, yearly (see
:data:`SUPPORTED_GRANULARITIES`) — and every piece of time arithmetic goes
through the granularity, so nothing outside this module hardcodes a period
length.
"""

from __future__ import annotations

import calendar
import datetime as dt
from collections.abc import Generator, Iterator
from dataclasses import dataclass
from enum import Enum

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


def coerce_to_datetime(value: object) -> dt.datetime:
    """Coerce a partition value to a ``datetime.datetime``.

    Accepts a ``datetime``, a ``date`` (midnight is assumed), or an ISO-8601
    datetime string. Anything else raises ``TypeError``.

    Partition values are period *labels* in UTC, not instants: an aware
    datetime is converted to UTC and stripped of its tzinfo, so ids, bounds
    and comparisons never mix aware and naive values. This matches BigQuery,
    whose time partitions are UTC-based.

    Args:
        value: The partition value to coerce.

    Returns:
        The value as a ``datetime.datetime``.

    Raises:
        TypeError: If the value cannot be interpreted as a datetime.
    """
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            return value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, dt.date):
        return dt.datetime(value.year, value.month, value.day)  # noqa: DTZ001 — a label, not an instant
    if isinstance(value, str):
        try:
            return coerce_to_datetime(dt.datetime.fromisoformat(value))
        except ValueError as e:
            raise TypeError(
                f"Could not parse partition value {value!r} as a datetime: expected an ISO-8601 datetime string."
            ) from e
    raise TypeError(
        f"Sub-daily partition value must be a `datetime.datetime` or an ISO-8601 datetime string, got "
        f"{type(value).__name__}: {value!r}."
    )


def _add_months(value: dt.date, months: int) -> dt.date:
    """Shift a date by *months*, clamping the day to the target month's length.

    Returns:
        The shifted date.
    """
    total = (value.year * 12 + value.month - 1) + months
    year, month = divmod(total, 12)
    month += 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


# -- Granularity ---------------------------------------------------------------


class TimeGranularity(str, Enum):
    """The length of the period one time partition covers.

    The arithmetic (:meth:`truncate`, :meth:`advance`, :meth:`bounds`,
    :meth:`periods_between`) is implemented for every member: it is pure and
    fully testable, and it is what makes the granularity a real seam rather
    than a placeholder.

    Partition *identity* (:meth:`format` / :meth:`parse`) exists for the
    declarable granularities. An id is a storage contract — it lands in hive
    paths, object prefixes and ``DELETE`` predicates — so ``WEEK`` and
    ``QUARTER``, which nothing may declare, deliberately have none.

    Only the granularities in :data:`SUPPORTED_GRANULARITIES` may be declared
    on a :class:`TimePartitionConfig`.
    """

    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"

    @property
    def key_format(self) -> str | None:
        """The ``strptime`` shape of this granularity's partition id.

        ``None`` for the granularities that have no id (``WEEK``,
        ``QUARTER``): an id is a storage contract, and nothing may declare
        them.
        """
        return {
            TimeGranularity.HOUR: "%Y-%m-%dT%H",
            TimeGranularity.DAY: "%Y-%m-%d",
            TimeGranularity.MONTH: "%Y-%m",
            TimeGranularity.YEAR: "%Y",
        }.get(self)

    def coerce(self, value: object) -> dt.date:
        """Coerce a raw value to this granularity's value type.

        Returns:
            A ``datetime`` for sub-daily granularities, a ``date`` otherwise.
        """
        if self is TimeGranularity.HOUR:
            return coerce_to_datetime(value)
        return coerce_to_date(value)

    def truncate(self, value: object) -> dt.date:
        """Reduce a value to the start of the period containing it.

        Returns:
            The period's first instant.
        """
        coerced = self.coerce(value)
        if self is TimeGranularity.HOUR:
            assert isinstance(coerced, dt.datetime)
            return coerced.replace(minute=0, second=0, microsecond=0)
        if self is TimeGranularity.DAY:
            return coerced
        if self is TimeGranularity.WEEK:
            return coerced - dt.timedelta(days=coerced.weekday())
        if self is TimeGranularity.MONTH:
            return coerced.replace(day=1)
        if self is TimeGranularity.QUARTER:
            return coerced.replace(month=3 * ((coerced.month - 1) // 3) + 1, day=1)
        return coerced.replace(month=1, day=1)

    def advance(self, value: object, periods: int) -> dt.date:
        """Shift a value by *periods* whole periods (negative goes back).

        Returns:
            The start of the resulting period.
        """
        start = self.truncate(value)
        if self is TimeGranularity.HOUR:
            return start + dt.timedelta(hours=periods)
        if self is TimeGranularity.DAY:
            return start + dt.timedelta(days=periods)
        if self is TimeGranularity.WEEK:
            return start + dt.timedelta(weeks=periods)
        if self is TimeGranularity.MONTH:
            return _add_months(start, periods)
        if self is TimeGranularity.QUARTER:
            return _add_months(start, 3 * periods)
        return _add_months(start, 12 * periods)

    def bounds(self, value: object) -> tuple[dt.date, dt.date]:
        """Return the period's ``(start, end_exclusive)`` bounds.

        Half-open bounds are what range-scoped reads and deletes need: a
        partition covering more (or less) than one row of a ``DATE`` column
        cannot be selected by equality.

        Returns:
            The period's start and its exclusive end.
        """
        start = self.truncate(value)
        return start, self.advance(start, 1)

    def periods_between(self, start: object, end: object) -> int:
        """Return how many periods separate two values (``end`` minus ``start``).

        Returns:
            The number of steps from *start*'s period to *end*'s period.
            Negative when *end* precedes *start*.
        """
        first = self.truncate(start)
        last = self.truncate(end)
        if self is TimeGranularity.HOUR:
            assert isinstance(first, dt.datetime) and isinstance(last, dt.datetime)
            return int((last - first).total_seconds() // 3600)
        if self is TimeGranularity.DAY:
            return (last - first).days
        if self is TimeGranularity.WEEK:
            return (last - first).days // 7
        months = (last.year - first.year) * 12 + (last.month - first.month)
        if self is TimeGranularity.MONTH:
            return months
        if self is TimeGranularity.QUARTER:
            return months // 3
        return last.year - first.year

    def format(self, value: object) -> str:
        """Render a value as a partition id.

        Ids are ISO-8601 prefixes — ``2026``, ``2026-08``, ``2026-08-21``,
        ``2026-08-21T13`` — so they sort chronologically as strings, embed in
        hive paths and object prefixes unchanged, and each shape names its
        granularity unambiguously (see :meth:`TimePartition.from_key`).

        Returns:
            The canonical id of the period containing *value*.

        Raises:
            NotImplementedError: For granularities whose id format is not
                yet defined (``WEEK``/``QUARTER``: an id is a storage
                contract, and BigQuery parity does not need them).
        """
        if self.key_format is None:
            raise NotImplementedError(self._no_id_format_message())
        return self.truncate(value).strftime(self.key_format)

    def parse(self, key: str) -> dt.date:
        """Parse a partition id back into the period's start.

        Returns:
            The period's first instant.

        Raises:
            NotImplementedError: For granularities whose id format is not
                yet defined.
            ValueError: If *key* is not this granularity's format.
        """
        if self.key_format is None:
            raise NotImplementedError(self._no_id_format_message())
        try:
            parsed = dt.datetime.strptime(key, self.key_format)  # noqa: DTZ007 — a label, not an instant
        except ValueError as e:
            raise ValueError(
                f"Partition key {key!r} is not a {self.value} key (expected the shape {self.key_format!r})."
            ) from e
        return parsed if self is TimeGranularity.HOUR else parsed.date()

    def _no_id_format_message(self) -> str:
        supported = ", ".join(sorted(g.value for g in TimeGranularity if g.key_format is not None))
        return f"No partition id format is defined for {self.value!r} granularity. Supported: {supported}."


SUPPORTED_GRANULARITIES = frozenset(
    {TimeGranularity.HOUR, TimeGranularity.DAY, TimeGranularity.MONTH, TimeGranularity.YEAR}
)
"""Granularities an asset may declare: the set BigQuery time partitioning offers.

``WEEK`` and ``QUARTER`` keep their arithmetic (it is pure and tested) but
have no id format and cannot be declared — a partition id is a storage
contract, and no destination needs those two.
"""


def period_range(
    start: dt.date,
    end: dt.date,
    granularity: TimeGranularity = TimeGranularity.DAY,
    reversed: bool = False,
) -> Generator[dt.date, None, None]:
    """Yield the start of each period from *start* to *end* inclusive.

    Args:
        start: Start of the range (truncated to *granularity*).
        end: End of the range (truncated to *granularity*).
        granularity: The period length to step by.
        reversed: When True, yield periods from end to start.

    Yields:
        Each period's first instant.
    """
    first = granularity.truncate(start)
    last = granularity.truncate(end)
    if reversed:
        while last >= first:
            yield last
            last = granularity.advance(last, -1)
    else:
        while first <= last:
            yield first
            first = granularity.advance(first, 1)


# -- Time partitions -----------------------------------------------------------


@dataclass(frozen=True)
class TimePartitionConfig(PartitionConfig):
    """Partition config for time-partitioned assets.

    Attributes:
        granularity: The period one partition covers. Must be one of
            :data:`SUPPORTED_GRANULARITIES`.
        start: Optional lower bound. Partitions before it are rejected, and
            windows built against this asset are clamped to it.
    """

    granularity: TimeGranularity = TimeGranularity.DAY
    start: dt.date | None = None

    def __post_init__(self) -> None:
        """Validate the granularity and normalize ``start`` to a period start.

        Raises:
            ValueError: If the granularity is not supported yet.
        """
        if self.granularity not in SUPPORTED_GRANULARITIES:
            supported = ", ".join(sorted(g.value for g in SUPPORTED_GRANULARITIES))
            raise ValueError(
                f"Time partitioning at {self.granularity.value!r} granularity is not supported yet "
                f"(supported: {supported})."
            )
        if self.start is not None:
            object.__setattr__(self, "start", self.granularity.truncate(self.start))


@dataclass(frozen=True)
class TimePartition(Partition):
    """A single time partition, identified by the start of its period."""

    value: dt.date
    granularity: TimeGranularity = TimeGranularity.DAY

    @classmethod
    def from_key(cls, key: str) -> TimePartition:
        """Parse a partition id, inferring its granularity from the shape.

        The id shapes are mutually unambiguous (``2026`` / ``2026-08`` /
        ``2026-08-21`` / ``2026-08-21T13``), so the key alone carries the
        granularity: storage and transport need one string, not a pair.

        Returns:
            The partition the key names.

        Raises:
            ValueError: If *key* matches no known id shape.
        """
        for granularity in TimeGranularity:
            if granularity.key_format is None:
                continue
            try:
                return cls(granularity.parse(key), granularity)
            except ValueError:
                continue
        shapes = ", ".join(
            repr(g.key_format) for g in TimeGranularity if g.key_format is not None
        )
        raise ValueError(f"Partition key {key!r} matches no granularity (known shapes: {shapes}).")

    def __post_init__(self) -> None:
        """Normalize the value to the start of its period."""
        object.__setattr__(self, "value", self.granularity.truncate(self.value))

    def __repr__(self) -> str:
        """Return the ISO-formatted period start."""
        return self.value.isoformat()

    @property
    def id(self) -> str:
        """The partition's canonical id (see :meth:`TimeGranularity.format`)."""
        return self.granularity.format(self.value)

    @property
    def bounds(self) -> tuple[dt.date, dt.date]:
        """The partition's ``(start, end_exclusive)`` bounds."""
        return self.granularity.bounds(self.value)


@dataclass(frozen=True)
class TimePartitionWindow(PartitionWindow):
    """A contiguous range of time partitions, inclusive of both bounds."""

    start: dt.date
    end: dt.date
    granularity: TimeGranularity = TimeGranularity.DAY

    def __post_init__(self) -> None:
        """Normalize both bounds to period starts.

        Raises:
            ValueError: If the window ends before it starts.
        """
        object.__setattr__(self, "start", self.granularity.truncate(self.start))
        object.__setattr__(self, "end", self.granularity.truncate(self.end))
        if self.start > self.end:
            raise ValueError(f"Time partition window ends before it starts: {self.start} to {self.end}.")

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
        for value in period_range(self.start, self.end, self.granularity, reversed=True):
            yield TimePartition(value, self.granularity)

    def partition_count(self) -> int:
        """Return the number of partitions in the window (inclusive)."""
        return self.granularity.periods_between(self.start, self.end) + 1

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

        The window is counted in partitions, never in days: *offset* is how
        many partitions back from the current one the window ends, and
        *lookback* is how many it spans. At daily granularity, ``offset=1,
        lookback=1`` is "yesterday only" and ``offset=0`` includes the
        current (still incomplete) partition.

        The caller's timezone reaches the window through *now*: passing an
        aware datetime localized to a zone makes DAY/MONTH/YEAR windows
        follow that zone's calendar (its "yesterday"). HOUR windows are the
        deliberate exception — an aware *now* is normalized back to UTC (see
        :func:`coerce_to_datetime`), because hour partition ids are naive-UTC
        labels and zones on a fractional offset (e.g. UTC+05:45) don't align
        to UTC hour boundaries at all.

        Args:
            now: The reference instant, in whatever timezone the caller
                labels partitions with.
            lookback: How many partitions the window spans (at least 1).
            offset: How many partitions back from the current one the window
                ends.
            granularity: The period one partition covers.
            start: Optional lower bound (an asset's earliest partition) to
                clamp the window to.

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

