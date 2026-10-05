"""Coverage: which partitions of each asset hold data, and which it still owes.

Coverage is a property of an asset's data, not of what triggered it: runs of
every target count (a job, a source, the asset itself, a backfill, a deleted
target). An asset-partition is *covered* once any execution of it succeeded,
and *failed* when one failed and none succeeded; one attempted but still in
flight, or canceled, is neither. What an asset *owes* runs from its declared
start (else its first attempt) to its last attempt, and on to the last closed
period while it is scheduled.

Two views read the same evidence: by day, for the calendar, and by partition
key, for one job's range.
"""

from __future__ import annotations

import datetime as dt
from collections import defaultdict
from dataclasses import dataclass
from typing import NamedTuple
from uuid import UUID

from interloper.partitioning.time import TimeGranularity, TimePartition, TimePartitionConfig

from interloper_db.models import Component


class CoverageRow(NamedTuple):
    """Whether one asset ever succeeded or failed for one time partition, from runs of any target.

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


class PartitionSpan(NamedTuple):
    """The days one partition key covers, of any granularity.

    Attributes:
        first: The first day of the partition.
        last: The last day of the partition, inclusive.
    """

    first: dt.date
    last: dt.date

    @classmethod
    def from_key(cls, key: str) -> PartitionSpan:
        """Parse a time partition key into the days it covers.

        Args:
            key: A time partition key of any granularity.

        Returns:
            The days it covers; an hourly key covers its one day.
        """
        start, end = TimePartition.from_key(key).bounds
        # An hourly partition's bounds are datetimes (a datetime is also a date), and it never crosses midnight.
        if isinstance(start, dt.datetime):
            return cls(first=start.date(), last=start.date())
        return cls(first=start, last=end - dt.timedelta(days=1))

    @classmethod
    def from_spans(cls, spans: list[PartitionSpan]) -> PartitionSpan:
        """Span the days of several partitions, from the earliest first day to the latest last day.

        Args:
            spans: The partitions' spans, at least one; they may overlap or
                leave gaps, which the result covers.

        Returns:
            The enclosing span.
        """
        return cls(first=min(span.first for span in spans), last=max(span.last for span in spans))


@dataclass(frozen=True)
class AssetEvidence:
    """One partitioned asset's evidence, and what the calendar expects of it.

    Attributes:
        asset: The asset row.
        group: The row the asset counts toward on the calendar: its parent
            source, or the asset itself when standalone.
        partitioning: The asset's partitioning, from its catalog definition.
        scheduled: Whether an enabled job targets the asset or its parent source.
        evidence: Its coverage rows, all-time, each with the days its key covers.
        bounds: The days its attempted partitions span, all-time, or ``None``
            when it was never attempted.
    """

    asset: Component
    group: Component
    partitioning: TimePartitionConfig
    scheduled: bool
    evidence: list[tuple[PartitionSpan, CoverageRow]]
    bounds: PartitionSpan | None

    @classmethod
    def from_components(
        cls,
        components: list[Component],
        partitionings: dict[UUID, TimePartitionConfig],
        rows: list[CoverageRow],
    ) -> list[AssetEvidence]:
        """Pair each partitioned asset row with its group, partitioning and evidence.

        Each distinct partition key is parsed once, however many assets
        executed it.

        Args:
            components: The organisation's source, asset and job rows, with
                their outgoing relations loaded.
            partitionings: Each partitioned asset's partitioning by id; an
                asset absent from it (unpartitioned, drifted) is left out.
            rows: The store's all-time coverage rows, from runs of any target.

        Returns:
            One entry per partitioned asset, in the order of *components*.
        """
        by_id = {component.id: component for component in components}
        targeted = {
            relation.dst_id
            for job in components
            if job.kind == "job" and job.enabled
            for relation in job.out_relations
            if relation.name == "targets"
        }
        rows_by_asset: dict[UUID, list[CoverageRow]] = defaultdict(list)
        for row in rows:
            rows_by_asset[row.asset_id].append(row)
        spans: dict[str, PartitionSpan] = {}
        coverages = []
        for asset in components:
            if asset.kind != "asset" or asset.id not in partitionings:
                continue
            group = by_id[asset.parent_id] if asset.parent_id else asset
            evidence = []
            for row in rows_by_asset[asset.id]:
                span = spans.get(row.partition_key)
                if span is None:
                    span = spans[row.partition_key] = PartitionSpan.from_key(row.partition_key)
                evidence.append((span, row))
            bounds = PartitionSpan.from_spans([span for span, _ in evidence]) if evidence else None
            coverages.append(
                cls(
                    asset=asset,
                    group=group,
                    partitioning=partitionings[asset.id],
                    scheduled=asset.id in targeted or asset.parent_id in targeted,
                    evidence=evidence,
                    bounds=bounds,
                )
            )
        return coverages

    def expected_span(self, now: dt.datetime) -> tuple[dt.date, dt.date] | None:
        """The days the asset owes partitions for, before clipping to a window.

        The span starts at the declared ``start`` when there is one, else on
        the first day of the earliest attempted partition. It ends on the last
        day of the latest attempted partition; a scheduled asset that is
        enabled, under an enabled group, also owes every period closed before
        *now*: up to yesterday for a daily asset, the end of last month or
        year for a monthly or yearly one, and the hours elapsed today for an
        hourly one.

        Args:
            now: The reference instant, aware UTC.

        Returns:
            The first and last day owed, inclusive, or ``None`` when nothing
            is owed: no evidence and no declared start, or a declared start
            with no evidence and no schedule.
        """
        granularity = self.partitioning.granularity
        start = self.partitioning.start
        if start is not None:
            first = start.date() if isinstance(start, dt.datetime) else start
        else:
            first = self.bounds.first if self.bounds else None
        last = self.bounds.last if self.bounds else None
        if self.scheduled and self.asset.enabled and self.group.enabled:
            if granularity is TimeGranularity.HOUR:
                closed = (now - dt.timedelta(hours=1)).date()
            else:
                closed = granularity.truncate(now.date()) - dt.timedelta(days=1)
            last = closed if last is None else max(last, closed)
        if first is None or last is None:
            return None
        return first, last

    def slots(self, first: dt.date, length: int, now: dt.datetime) -> list[int]:
        """How many partitions the asset owes on each day of a run of days.

        An hourly asset owes the hours of a day from its declared start, when
        the start falls on that day, to *now*'s hour, when the day is today
        (the hours elapsed since midnight UTC), else to midnight: 24 on a
        full day, at least one. Any other asset owes one slot a day.

        Args:
            first: The first day of the run, inside the asset's expected span.
            length: How many consecutive days the run holds.
            now: The reference instant, aware UTC.

        Returns:
            The slots owed per day, index ``i`` being day ``first + i``,
            before raising to the partitions attempted that day.
        """
        if self.partitioning.granularity is not TimeGranularity.HOUR:
            return [1] * length
        start = self.partitioning.start
        partial_days = {now.date(), start.date()} if isinstance(start, dt.datetime) else {now.date()}
        slots = [24] * length
        for day in partial_days:
            index = (day - first).days
            if 0 <= index < length:
                first_hour = start.hour if isinstance(start, dt.datetime) and start.date() == day else 0
                end_hour = now.hour if day == now.date() else 24
                slots[index] = max(1, end_hour - first_hour)
        return slots


@dataclass
class DayCounts:
    """Asset-partitions per day over a run of consecutive days, as parallel arrays.

    Index ``i`` of every array, and key ``i`` of :attr:`failed_run_ids`, is
    the day ``start + i``.

    Attributes:
        start: The first day of the run.
        expected: The asset-partitions owed each day.
        covered: Those covered each day.
        failed: Those failed each day.
        failed_run_ids: The greatest failed run id per day offset, for the
            days with a failed run.
    """

    start: dt.date
    expected: list[int]
    covered: list[int]
    failed: list[int]
    failed_run_ids: dict[int, UUID]

    @classmethod
    def from_asset(cls, asset: AssetEvidence, since: dt.date, until: dt.date, now: dt.datetime) -> DayCounts | None:
        """Roll one asset's partitions onto the days of its expected span, clipped to the window and today.

        Each day owes the asset's :meth:`AssetEvidence.slots`, raised to the
        partitions attempted on it when more. A monthly or yearly key counts
        toward every day it spans. A partition is covered once any execution
        succeeded, failed when one failed and none succeeded; one attempted
        but still in flight, or canceled, is neither, so it reads as missing,
        as does a day with nothing attempted. The failed run kept for a day
        is the greatest id among its failed runs, so the pick does not depend
        on row order.

        Args:
            asset: The asset with its evidence.
            since: First day of the window.
            until: Last day of the window, inclusive.
            now: The reference instant, aware UTC.

        Returns:
            The asset's days, or ``None`` when it owes none in the window.
        """
        span = asset.expected_span(now)
        if span is None:
            return None
        first, last = max(span[0], since), min(span[1], until, now.date())
        if first > last:
            return None
        length = (last - first).days + 1
        attempted, covered, failed = [0] * length, [0] * length, [0] * length
        failed_run_ids: dict[int, UUID] = {}
        for key_span, row in asset.evidence:
            if key_span.last < first or key_span.first > last:
                continue
            for i in range((max(key_span.first, first) - first).days, (min(key_span.last, last) - first).days + 1):
                attempted[i] += 1
                if row.succeeded:
                    covered[i] += 1
                elif row.failed:
                    failed[i] += 1
                    if row.failed_run_id is not None:
                        failed_run_ids[i] = max(failed_run_ids.get(i, row.failed_run_id), row.failed_run_id)
        expected = [max(owed, count) for owed, count in zip(asset.slots(first, length, now), attempted)]
        return cls(start=first, expected=expected, covered=covered, failed=failed, failed_run_ids=failed_run_ids)

    @classmethod
    def from_parts(cls, parts: list[DayCounts]) -> DayCounts:
        """Sum runs of days onto one run spanning them all, keeping the greatest failed run id per day.

        Args:
            parts: The runs to sum, at least one; they may start on different
                days and leave gaps between them, which read as zeros.

        Returns:
            The summed run, from the earliest start to the latest end.
        """
        start = min(part.start for part in parts)
        length = max((part.start - start).days + len(part.expected) for part in parts)
        merged = cls(start=start, expected=[0] * length, covered=[0] * length, failed=[0] * length, failed_run_ids={})
        for part in parts:
            offset = (part.start - start).days
            for i, (expected, covered, failed) in enumerate(zip(part.expected, part.covered, part.failed)):
                merged.expected[offset + i] += expected
                merged.covered[offset + i] += covered
                merged.failed[offset + i] += failed
            for i, run_id in part.failed_run_ids.items():
                merged.failed_run_ids[offset + i] = max(merged.failed_run_ids.get(offset + i, run_id), run_id)
        return merged


@dataclass(frozen=True)
class CoverageGroup:
    """One calendar group, a source with its assets or a standalone asset, with its days.

    Attributes:
        component: The source row, or the standalone asset row.
        days: The group's days, summed over its assets.
    """

    component: Component
    days: DayCounts

    @classmethod
    def from_assets(
        cls, assets: list[AssetEvidence], since: dt.date, until: dt.date, now: dt.datetime
    ) -> list[CoverageGroup]:
        """Group the assets' days by source, keeping only the groups with a day expected in the window.

        Args:
            assets: The partitioned assets with their evidence.
            since: First day of the window.
            until: Last day of the window, inclusive.
            now: The reference instant, aware UTC.

        Returns:
            The groups, by name then id, each with its days summed over its assets.
        """
        components: dict[UUID, Component] = {}
        parts: dict[UUID, list[DayCounts]] = defaultdict(list)
        for asset in assets:
            days = DayCounts.from_asset(asset, since, until, now)
            if days is not None:
                components[asset.group.id] = asset.group
                parts[asset.group.id].append(days)
        groups = [cls(components[group_id], DayCounts.from_parts(days)) for group_id, days in parts.items()]
        return sorted(groups, key=lambda group: (group.component.name or group.component.key, str(group.component.id)))


@dataclass(frozen=True)
class AssetKeyCoverage:
    """One asset's coverage over a range of partition keys.

    Attributes:
        asset_id: The asset.
        asset_key: The asset's key.
        covered: The keys with a successful execution.
        failed: The keys with a failed execution and no successful one.
    """

    asset_id: UUID
    asset_key: str
    covered: frozenset[str]
    failed: frozenset[str]


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
    assets: list[AssetKeyCoverage]

    @classmethod
    def from_rows(cls, job_id: UUID, keys: list[str], assets: dict[UUID, str], rows: list[CoverageRow]) -> JobCoverage:
        """Read each target asset's covered and failed keys off the evidence rows.

        Args:
            job_id: The job.
            keys: Every key of the range, oldest first.
            assets: The job's target assets, their keys by id.
            rows: The evidence rows of those assets over the range.

        Returns:
            The coverage, assets in the order of *assets*.
        """
        covered: dict[UUID, set[str]] = defaultdict(set)
        failed: dict[UUID, set[str]] = defaultdict(set)
        for row in rows:
            if row.succeeded:
                covered[row.asset_id].add(row.partition_key)
            elif row.failed:
                failed[row.asset_id].add(row.partition_key)
        return cls(
            job_id=job_id,
            keys=keys,
            assets=[
                AssetKeyCoverage(asset_id, key, frozenset(covered[asset_id]), frozenset(failed[asset_id]))
                for asset_id, key in assets.items()
            ],
        )
