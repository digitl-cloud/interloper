"""Coverage: what each partition of each asset came to, and which are missing.

Coverage is a property of an asset's data, not of what triggered it: runs of
every target count (a job, a source, the asset itself, a backfill, a deleted
target). An asset-partition is *successful* once any execution of it
succeeded, and *failed* when one failed and none succeeded. An asset's
partitions count from its history start (its own ``start``, else its
source's; else its first attempt) to its last attempt, and on to the last
closed period while it is scheduled; one of them that is neither successful
nor failed (never attempted, in flight, canceled) is *missing*.

A snapshot asset counts only what it can still deliver: it observes the
vendor's current state, so a period counts only once a run observed in it,
and only while it is current. One that ended without a success is *missed*:
it does not count, since nothing can backfill it. Once a history start is
set, a snapshot also misses every period since that ended with no observation
at all.

Two views read each asset's partitions (`AssetPartitions`), each over periods
of its own: the calendar by day (`GroupCoverage`, `DayCoverage`), one job's
range by partition key (`JobCoverage`, `KeyCoverage`).
"""

from __future__ import annotations

import datetime as dt
from bisect import bisect_right
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from functools import cached_property
from typing import NamedTuple
from uuid import UUID

from interloper.partitioning import TimeGranularity, TimePartition, TimePartitionConfig
from interloper.utils import coerce_to_datetime

from interloper_db.models import Component


class PartitionResult(NamedTuple):
    """What one asset's executions came to for one time partition, from runs of any target.

    Attributes:
        asset_id: The asset.
        partition_key: The partition, in its own granularity's key format.
        succeeded: Whether any execution of the asset for that partition succeeded.
        failed: Whether any execution of the asset for that partition failed,
            whether or not another one succeeded; an asset attempted but
            neither succeeded nor failed (in flight, canceled) is neither.
        failed_run_id: The greatest id among the runs whose execution of the
            asset failed, or ``None``.
    """

    asset_id: UUID
    partition_key: str
    succeeded: bool
    failed: bool
    failed_run_id: UUID | None


class Period(NamedTuple):
    """A partition's period, or a day's, as naive-UTC labels, its end excluded, comparable across granularities.

    Attributes:
        start: The period's first instant.
        end: The next period's first instant.
    """

    start: dt.datetime
    end: dt.datetime

    @classmethod
    def from_key(cls, key: str) -> Period:
        """Parse a time partition key of any granularity into its period.

        Args:
            key: The partition key.

        Returns:
            The period.
        """
        start, end = TimePartition.from_key(key).bounds
        return cls(coerce_to_datetime(start), coerce_to_datetime(end))

    @classmethod
    def from_day(cls, day: dt.date) -> Period:
        """The period of one calendar day.

        Args:
            day: The day.

        Returns:
            Its period, midnight to midnight.
        """
        return cls(coerce_to_datetime(day), coerce_to_datetime(day + dt.timedelta(days=1)))

    @classmethod
    def enclosing(cls, periods: list[Period]) -> Period:
        """The period from the earliest start to the latest end of several periods.

        Args:
            periods: The periods, at least one; they may overlap or leave
                gaps, which the result covers.

        Returns:
            The enclosing period.
        """
        return cls(min(period.start for period in periods), max(period.end for period in periods))

    @property
    def first_day(self) -> dt.date:
        """The day the period starts on."""
        return self.start.date()

    @property
    def last_day(self) -> dt.date:
        """The last day the period reaches, inclusive: an hourly period's own day."""
        return (self.end - dt.timedelta(microseconds=1)).date()

    def closed(self, now: dt.datetime) -> bool:
        """Whether the period has ended by *now*.

        Args:
            now: The reference instant, aware UTC.

        Returns:
            True once the period's end is at or before *now*.
        """
        return self.end <= coerce_to_datetime(now)


class Verdict(StrEnum):
    """What an asset's partitions came to over one period.

    ``successful`` once one succeeded, ``failed`` when one failed and none
    succeeded, ``missing`` when the period counts and neither happened yet.
    A snapshot's period is ``missed`` once every period overlapping it ended
    without a success, or, since a history start, without an observation: it
    does not count, and nothing can recover it.
    """

    SUCCESSFUL = "successful"
    FAILED = "failed"
    MISSING = "missing"
    MISSED = "missed"


class AssetPartitions:
    """One asset's attempted partitions, and what any period of them came to.

    The partitions of one asset never overlap, since it declares one
    granularity, so those overlapping a period are found by bisecting their
    starts, ordered the first time a period is asked about. An asset without
    a partitioning (one whose catalog entry drifted) is read like a dated one.
    """

    def __init__(
        self,
        asset: Component,
        partitioning: TimePartitionConfig | None,
        results: list[tuple[Period, PartitionResult]],
    ) -> None:
        """Bind an asset to its results.

        Args:
            asset: The asset row, its parent loaded.
            partitioning: The asset's partitioning, time or snapshot, from its
                catalog definition; ``None`` when the catalog has none for it.
            results: The asset's results, each with its key's period, in any order.
        """
        self.asset = asset
        self.partitioning = partitioning
        self.results = results
        self._start = coerce_to_datetime(asset.start) if asset.start is not None else None

    @classmethod
    def from_results(
        cls,
        assets: list[Component],
        partitionings: Mapping[UUID, TimePartitionConfig],
        results: list[PartitionResult],
    ) -> list[AssetPartitions]:
        """Give each asset its own results, each distinct partition key parsed once however many assets executed it.

        Args:
            assets: The asset rows, their parents loaded.
            partitionings: Each partitioned asset's partitioning by id.
            results: The results read, of these assets and any other.

        Returns:
            One entry per asset, in the order of *assets*.
        """
        by_asset: dict[UUID, list[tuple[Period, PartitionResult]]] = {asset.id: [] for asset in assets}
        periods: dict[str, Period] = {}
        for result in results:
            owned = by_asset.get(result.asset_id)
            if owned is None:
                continue
            period = periods.get(result.partition_key)
            if period is None:
                period = periods[result.partition_key] = Period.from_key(result.partition_key)
            owned.append((period, result))
        return [cls(asset, partitionings.get(asset.id), by_asset[asset.id]) for asset in assets]

    @cached_property
    def _ordered(self) -> list[tuple[Period, PartitionResult]]:
        """The results, oldest period first."""
        return sorted(self.results, key=lambda pair: pair[0].start)

    @cached_property
    def _starts(self) -> list[dt.datetime]:
        """The start of each result's period, oldest first, for bisecting."""
        return [period.start for period, _ in self._ordered]

    @property
    def windowed(self) -> bool:
        """Whether the asset is partitioned on its run's window, rather than by the period it observed in."""
        return self.partitioning is None or self.partitioning.windowed

    def verdict(self, period: Period, now: dt.datetime) -> tuple[Verdict, UUID | None] | None:
        """What the asset's partitions overlapping a period came to.

        A partition ending by the history start does not count. Past it, a
        dated asset's period is ``successful``, ``failed`` or ``missing``.
        A snapshot's counts only while one of the periods it observed in is
        current, and is missed otherwise (see `Verdict`). A day of a monthly
        snapshot takes its month's verdict; a day of an hourly snapshot is
        successful once any of its hours is.

        Args:
            period: The period, of any granularity.
            now: The reference instant, aware UTC: it decides which periods ended.

        Returns:
            The verdict, with the greatest failed run id when it is
            ``failed``, among the current periods for a snapshot; ``None``
            when the period does not count.
        """
        if not self.windowed:
            return self._observed(period, now)
        if self._start is not None and period.end <= self._start:
            return None
        overlapping = [result for _, result in self._overlapping(period)]
        if any(result.succeeded for result in overlapping):
            return Verdict.SUCCESSFUL, None
        failed_runs = [result.failed_run_id for result in overlapping if result.failed]
        if failed_runs:
            return Verdict.FAILED, max((run for run in failed_runs if run is not None), default=None)
        return Verdict.MISSING, None

    def _observed(self, period: Period, now: dt.datetime) -> tuple[Verdict, UUID | None] | None:
        """What a snapshot's observations overlapping a period came to.

        Args:
            period: The period, of any granularity.
            now: The reference instant, aware UTC.

        Returns:
            The verdict, with the greatest failed run id among the current
            periods when it is ``failed``; ``None`` when nothing overlapping
            it was observed and no period since the history start ended in it.
        """
        overlapping = self._overlapping(period)
        if any(result.succeeded for _, result in overlapping):
            return Verdict.SUCCESSFUL, None
        current = [result for observed, result in overlapping if not observed.closed(now)]
        if any(result.failed for result in current):
            failed_runs = [result.failed_run_id for result in current if result.failed_run_id is not None]
            return Verdict.FAILED, max(failed_runs, default=None)
        if current:
            return Verdict.MISSING, None
        if overlapping or self._ended_since_start(period, now):
            return Verdict.MISSED, None
        return None

    def _overlapping(self, period: Period) -> list[tuple[Period, PartitionResult]]:
        """The asset's results whose periods overlap a period, oldest first.

        Args:
            period: The period, of any granularity.

        Returns:
            The overlapping results, each with its period.
        """
        index = max(bisect_right(self._starts, period.start) - 1, 0)
        overlapping: list[tuple[Period, PartitionResult]] = []
        while index < len(self._ordered) and self._ordered[index][0].start < period.end:
            if self._ordered[index][0].end > period.start:
                overlapping.append(self._ordered[index])
            index += 1
        return overlapping

    def _ended_since_start(self, period: Period, now: dt.datetime) -> bool:
        """Whether a period reaches past the history start and the snapshot's last period in it has ended.

        Args:
            period: The period, of any granularity.
            now: The reference instant, aware UTC.

        Returns:
            False when no history start is set, or the period ends by it.
        """
        if self._start is None or self.partitioning is None or period.end <= self._start:
            return False
        first, end = self.partitioning.granularity.bounds(period.end - dt.timedelta(microseconds=1))
        return Period(coerce_to_datetime(first), coerce_to_datetime(end)).closed(now)


@dataclass
class DayCoverage:
    """Asset-partitions per day over a run of consecutive days, as parallel arrays.

    Index ``i`` of every array, and key ``i`` of :attr:`failed_run_ids`, is
    the day ``start + i``. A day's partitions are its successful, failed and
    missing ones; its missed snapshot periods are not among them.

    Attributes:
        start: The first day of the run.
        successful: The successful partitions each day.
        failed: The failed partitions each day.
        missing: The missing partitions each day.
        missed: The snapshot periods missed each day.
        failed_run_ids: The greatest failed run id per day offset, for the
            days with a failed run.
    """

    start: dt.date
    successful: list[int]
    failed: list[int]
    missing: list[int]
    missed: list[int]
    failed_run_ids: dict[int, UUID]

    @classmethod
    def empty(cls, start: dt.date, length: int) -> DayCoverage:
        """A run of days with nothing on any of them.

        Args:
            start: The first day of the run.
            length: How many consecutive days it holds.

        Returns:
            The run, every count zero.
        """
        return cls(start, [0] * length, [0] * length, [0] * length, [0] * length, {})

    @classmethod
    def from_asset(cls, asset: AssetPartitions, first: dt.date, last: dt.date, now: dt.datetime) -> DayCoverage:
        """Roll one asset's partitions onto a run of days.

        A dated asset counts on each day the partitions it holds, at least
        the day's :meth:`_slots`: a monthly or yearly key counts toward every
        day it spans, an hourly one toward its day. A snapshot's day takes the
        verdict of the periods overlapping it (see `AssetPartitions.verdict`).
        The failed run kept for a day is the greatest id among its failed
        runs, so the pick does not depend on row order.

        Args:
            asset: The asset with its partitions.
            first: The first day of the run.
            last: The last day of the run, inclusive.
            now: The reference instant, aware UTC.

        Returns:
            The asset's days.
        """
        length = (last - first).days + 1
        days = cls.empty(first, length)
        if not asset.windowed:
            for i in range(length):
                verdict = asset.verdict(Period.from_day(first + dt.timedelta(days=i)), now)
                if verdict is not None:
                    days._count(i, *verdict)
            return days
        attempted = [0] * length
        for period, result in asset.results:
            first_day, last_day = period.first_day, period.last_day
            if last_day < first or first_day > last:
                continue
            for i in range((max(first_day, first) - first).days, (min(last_day, last) - first).days + 1):
                attempted[i] += 1
                if result.succeeded:
                    days.successful[i] += 1
                elif result.failed:
                    days.failed[i] += 1
                    if result.failed_run_id is not None:
                        days._keep_failed_run(i, result.failed_run_id)
        for i, (slots, count) in enumerate(zip(cls._slots(asset, first, length, now), attempted)):
            days.missing[i] = max(slots, count) - days.successful[i] - days.failed[i]
        return days

    @classmethod
    def from_parts(cls, parts: list[DayCoverage]) -> DayCoverage:
        """Sum runs of days onto one run spanning them all, keeping the greatest failed run id per day.

        Args:
            parts: The runs to sum, at least one; they may start on different
                days and leave gaps between them, which read as zeros.

        Returns:
            The summed run, from the earliest start to the latest end.
        """
        start = min(part.start for part in parts)
        merged = cls.empty(start, max((part.start - start).days + len(part.successful) for part in parts))
        for part in parts:
            offset = (part.start - start).days
            for i, (successful, failed, missing, missed) in enumerate(
                zip(part.successful, part.failed, part.missing, part.missed)
            ):
                merged.successful[offset + i] += successful
                merged.failed[offset + i] += failed
                merged.missing[offset + i] += missing
                merged.missed[offset + i] += missed
            for i, run_id in part.failed_run_ids.items():
                merged._keep_failed_run(offset + i, run_id)
        return merged

    @staticmethod
    def _slots(asset: AssetPartitions, first: dt.date, length: int, now: dt.datetime) -> list[int]:
        """How many partitions a dated asset holds on each day of a run of days.

        An hourly asset holds the hours of a day up to *now*'s hour when the
        day is today (the hours elapsed since midnight UTC), else all 24, and
        at least one. Any other asset holds one partition a day.

        Args:
            asset: The asset.
            first: The first day of the run.
            length: How many consecutive days the run holds.
            now: The reference instant, aware UTC.

        Returns:
            The partitions per day, index ``i`` being day ``first + i``,
            before raising to the partitions attempted that day.
        """
        if asset.partitioning is None or asset.partitioning.granularity is not TimeGranularity.HOUR:
            return [1] * length
        slots = [24] * length
        today = (now.date() - first).days
        if 0 <= today < length:
            slots[today] = max(1, now.hour)
        return slots

    def _count(self, index: int, verdict: Verdict, failed_run_id: UUID | None) -> None:
        """Count one verdict on one day, in the array named after it.

        Args:
            index: The day's offset in the run.
            verdict: What came to it.
            failed_run_id: The failed run, for a ``failed`` verdict.
        """
        getattr(self, verdict.value)[index] += 1
        if failed_run_id is not None:
            self._keep_failed_run(index, failed_run_id)

    def _keep_failed_run(self, index: int, run_id: UUID) -> None:
        """Keep the greatest failed run id seen for one day.

        Args:
            index: The day's offset in the run.
            run_id: A failed run of that day.
        """
        self.failed_run_ids[index] = max(self.failed_run_ids.get(index, run_id), run_id)


@dataclass(frozen=True)
class GroupCoverage:
    """One calendar group, a source with its assets or a standalone asset, with its days.

    Attributes:
        component: The source row, or the standalone asset row.
        days: The group's days, summed over its assets.
    """

    component: Component
    days: DayCoverage

    @classmethod
    def from_components(
        cls,
        components: list[Component],
        partitionings: Mapping[UUID, TimePartitionConfig],
        results: list[PartitionResult],
        attempted: Mapping[UUID, Period],
        *,
        since: dt.date,
        until: dt.date,
        now: dt.datetime,
    ) -> list[GroupCoverage]:
        """Roll each partitioned asset's days up by group, keeping only the groups with a day in the window.

        Args:
            components: The organisation's source, asset and job rows, with
                their outgoing relations loaded.
            partitionings: Each partitioned asset's partitioning by id; an
                asset absent from it (unpartitioned, drifted) is left out.
            results: The results read for the window, from runs of any target.
            attempted: The period each asset's attempted partitions span,
                all-time; an asset absent from it was never attempted.
            since: First day of the window.
            until: Last day of the window, inclusive.
            now: The reference instant, aware UTC.

        Returns:
            The groups, by name then id, each with its days summed over its assets.
        """
        by_id = {component.id: component for component in components}
        targeted = {
            relation.dst_id
            for job in components
            if job.kind == "job" and job.enabled
            for relation in job.out_relations
            if relation.name == "targets"
        }
        assets = [asset for asset in components if asset.kind == "asset" and asset.id in partitionings]
        groups: dict[UUID, Component] = {}
        parts: dict[UUID, list[DayCoverage]] = defaultdict(list)
        for asset in AssetPartitions.from_results(assets, partitionings, results):
            group = by_id[asset.asset.parent_id] if asset.asset.parent_id else asset.asset
            scheduled = asset.asset.id in targeted or asset.asset.parent_id in targeted
            span = cls._span(asset, group, scheduled, attempted.get(asset.asset.id), now)
            if span is None:
                continue
            first, last = max(span[0], since), min(span[1], until, now.date())
            if first <= last:
                groups[group.id] = group
                parts[group.id].append(DayCoverage.from_asset(asset, first, last, now))
        coverages = [cls(groups[group_id], DayCoverage.from_parts(days)) for group_id, days in parts.items()]
        return sorted(
            coverages,
            key=lambda coverage: (coverage.component.name or coverage.component.key, str(coverage.component.id)),
        )

    @staticmethod
    def _span(
        asset: AssetPartitions,
        group: Component,
        scheduled: bool,
        attempted: Period | None,
        now: dt.datetime,
    ) -> tuple[dt.date, dt.date] | None:
        """The days an asset's partitions count over, before clipping to a window.

        The span starts at the asset's history start (see `Component.start`)
        when one is set, else on the first day of its earliest attempted
        partition. It ends on the last day of its latest attempted partition;
        a scheduled asset that is enabled, under an enabled group, also counts
        every period closed before *now*: up to yesterday for a daily asset,
        the end of last month or year for a monthly or yearly one, and the
        hours elapsed today for an hourly one.

        Args:
            asset: The asset, partitioned.
            group: The row it counts toward on the calendar.
            scheduled: Whether an enabled job targets the asset or its parent source.
            attempted: The period its attempted partitions span, all-time;
                ``None`` when it was never attempted.
            now: The reference instant, aware UTC.

        Returns:
            The first and last day, inclusive, or ``None`` when nothing
            counts: no attempt and no history start, or a history start with
            no attempt and no schedule.
        """
        assert asset.partitioning is not None, "the calendar reads partitioned assets only"
        granularity = asset.partitioning.granularity
        first = asset.asset.start or (attempted.first_day if attempted else None)
        last = attempted.last_day if attempted else None
        if scheduled and asset.asset.enabled and group.enabled:
            if granularity is TimeGranularity.HOUR:
                closed = (now - dt.timedelta(hours=1)).date()
            else:
                closed = granularity.truncate(now.date()) - dt.timedelta(days=1)
            last = closed if last is None else max(last, closed)
        if first is None or last is None:
            return None
        return first, last


@dataclass(frozen=True)
class KeyCoverage:
    """One asset's coverage over a range of partition keys.

    Attributes:
        asset_id: The asset.
        asset_key: The asset's key.
        successful: The keys with a successful execution.
        failed: The keys with a failed execution and no successful one.
        missing: The keys that count and have neither yet.
        missed: The keys whose snapshot observations ended without a success,
            or, since the history start, without an observation: they do not
            count, and are never worth a backfill.
    """

    asset_id: UUID
    asset_key: str
    successful: frozenset[str]
    failed: frozenset[str]
    missing: frozenset[str]
    missed: frozenset[str]

    @classmethod
    def from_asset(cls, asset: AssetPartitions, keys: list[tuple[str, Period]], now: dt.datetime) -> KeyCoverage:
        """Give each key of a range the verdict of the asset's partitions overlapping it.

        Args:
            asset: The asset with its partitions.
            keys: Every key of the range, of any granularity, with its period.
            now: The reference instant, aware UTC.

        Returns:
            The asset's coverage; a key that does not count is in no set.
        """
        verdicts: dict[Verdict, set[str]] = defaultdict(set)
        for key, period in keys:
            verdict = asset.verdict(period, now)
            if verdict is not None:
                verdicts[verdict[0]].add(key)
        return cls(
            asset.asset.id,
            asset.asset.key,
            successful=frozenset(verdicts[Verdict.SUCCESSFUL]),
            failed=frozenset(verdicts[Verdict.FAILED]),
            missing=frozenset(verdicts[Verdict.MISSING]),
            missed=frozenset(verdicts[Verdict.MISSED]),
        )


@dataclass(frozen=True)
class JobCoverage:
    """The coverage of a job's target assets over a range of partition keys.

    Attributes:
        job_id: The job.
        keys: Every key of the range, oldest first.
        assets: One entry per asset the job targets, directly or through its source.
    """

    job_id: UUID
    keys: list[str]
    assets: list[KeyCoverage]

    @classmethod
    def from_results(
        cls,
        job_id: UUID,
        keys: list[str],
        assets: list[Component],
        partitionings: Mapping[UUID, TimePartitionConfig],
        results: list[PartitionResult],
        *,
        now: dt.datetime,
    ) -> JobCoverage:
        """Read each target asset's coverage off the results overlapping the range.

        Args:
            job_id: The job.
            keys: Every key of the range, oldest first.
            assets: The job's target asset rows, their parents loaded.
            partitionings: The partitioning of the target assets that have one, by id.
            results: The results of those assets overlapping the range, of any granularity.
            now: The reference instant, aware UTC.

        Returns:
            The coverage, assets in the order of *assets*.
        """
        periods = [(key, Period.from_key(key)) for key in keys]
        partitions = AssetPartitions.from_results(assets, partitionings, results)
        return cls(job_id, keys, [KeyCoverage.from_asset(asset, periods, now) for asset in partitions])
