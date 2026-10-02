"""Overview API: the landing page's health, attention, schedule and inventory in one read.

The store answers what the data says; this module answers what it means
for the reader. Each section of the response is built from store rows by its
own model's ``from_*`` constructor, so every tile and list on the page shares
one vocabulary (see ``docs/superpowers/specs/2026-09-30-overview-page-design.md``).

Run and event timestamps pass through ``assume_utc`` before any arithmetic:
Postgres hands ``TIMESTAMPTZ`` columns back aware, but SQLite has no timezone
type and hands them back naive, and comparing those with an aware ``now``
would raise.
"""

from __future__ import annotations

import datetime as dt
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from interloper.partitioning.time import TimeGranularity, TimePartitionConfig, TimePartitionWindow
from interloper.utils.time import assume_utc
from interloper_db import Backfill, Component, ComponentStatus
from interloper_db.models import Run
from interloper_db.store.events import CoverageRow, ErrorGroup, PartitionBounds
from pydantic import BaseModel

from interloper_api.dependencies import OrgIdDep, StoreDep, ViewerDep
from interloper_api.routes.runs import RunResponse
from interloper_api.utils import format_duration, job_zone

router = APIRouter(prefix="/overview", tags=["overview"])

OVERDUE_AFTER = dt.timedelta(minutes=15)
# Verdicts only: a step-level failure (dest_write_failed and the like) repeats its operation's own verdict.
FAILURE_EVENT_TYPES = ("operation_failed", "run_failed")
INVENTORY_KINDS = ("source", "asset", "destination", "connection", "job", "hook")
HOOK_EVENTS_LOOKBACK = dt.timedelta(days=30)
RECENT_LIMIT = 5
MAX_COVERAGE_SPAN_DAYS = 400
TERMINAL_STATUSES = frozenset({"success", "failed", "canceled"})
DRIFT_TITLES = {
    ComponentStatus.MISSING: "{name} is missing from the catalog",
    ComponentStatus.DISABLED: "{name} is disabled in this deployment",
    ComponentStatus.UNREADABLE: "{name} has an unreadable config",
}


# -- Response models -----------------------------------------------------------


class HourBucket(BaseModel):
    """Attempts that finished inside one hour, by verdict."""

    hour: dt.datetime
    succeeded: int
    failed: int


class RunsSummary(BaseModel):
    """Attempts finished over the last 24 clock hours, with their hourly profile.

    The window starts at the top of the hour 23 hours before *now*'s and ends
    at *now*, so the totals always equal the sum of the bars.
    """

    total: int
    succeeded: int
    failed: int
    hourly: list[HourBucket]

    @classmethod
    def from_runs(cls, runs: list[Run], now: dt.datetime) -> RunsSummary:
        """Bucket the attempts that finished in the window by the clock hour they completed in.

        Args:
            runs: Attempts completed around the window, whether or not they
                started; those that did not complete inside it, or ended other
                than in success or failure, are left out.
            now: The window's end, aware UTC.

        Returns:
            The verdict totals and 24 hourly buckets, oldest first.
        """
        start = now.replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=23)
        counts: dict[dt.datetime, Counter[str]] = defaultdict(Counter)
        for run in runs:
            if run.completed_at is None or run.status not in ("success", "failed"):
                continue
            completed = assume_utc(run.completed_at).astimezone(dt.timezone.utc)
            if start <= completed <= now:
                counts[completed.replace(minute=0, second=0, microsecond=0)][run.status] += 1
        hourly = [
            HourBucket(hour=hour, succeeded=counts[hour]["success"], failed=counts[hour]["failed"])
            for hour in (start + dt.timedelta(hours=i) for i in range(24))
        ]
        succeeded = sum(bucket.succeeded for bucket in hourly)
        failed = sum(bucket.failed for bucket in hourly)
        return cls(total=succeeded + failed, succeeded=succeeded, failed=failed, hourly=hourly)


class ActivitySummary(BaseModel):
    """What is executing right now."""

    running: int
    queued: int
    longest_running_seconds: float | None = None

    @classmethod
    def from_runs(cls, running: list[Run], queued: int, now: dt.datetime) -> ActivitySummary:
        """Summarise the attempts in flight.

        Args:
            running: The attempts currently running.
            queued: How many attempts wait in the queue.
            now: The reference instant the running time is measured to.

        Returns:
            The counts, and the longest running time in seconds (``None``
            when nothing that started is running).
        """
        longest = max(
            ((now - assume_utc(run.started_at)).total_seconds() for run in running if run.started_at), default=None
        )
        return cls(running=len(running), queued=queued, longest_running_seconds=longest)


class BackfillsSummary(BaseModel):
    """Backfills still queued or running, their partitions rolled up."""

    active: int
    partitions_done: int
    partitions_total: int

    @classmethod
    def from_backfills(cls, backfills: list[Backfill], counts: dict[UUID, dict[str, int]]) -> BackfillsSummary:
        """Roll the active backfills' partitions up into one progress figure.

        Args:
            backfills: The active backfills.
            counts: Each backfill's runs per status, by backfill id; a
                backfill absent from it has no runs counted as done.

        Returns:
            The active count, the partitions in a terminal status and the
            partitions in total.
        """
        done = sum(
            n
            for backfill in backfills
            for status, n in counts.get(backfill.id, {}).items()
            if status in TERMINAL_STATUSES
        )
        return cls(
            active=len(backfills),
            partitions_done=done,
            partitions_total=sum(backfill.partitions for backfill in backfills),
        )


class JobsSummary(BaseModel):
    """Enabled jobs, and how many of them last ended in failure."""

    enabled: int
    failing: int

    @classmethod
    def from_jobs(cls, enabled_jobs: list[Component], failing_ids: set[UUID]) -> JobsSummary:
        """Count the enabled jobs and those whose latest attempt failed.

        Args:
            enabled_jobs: The organisation's enabled job rows.
            failing_ids: Ids of the jobs whose latest attempt failed.

        Returns:
            The two counts.
        """
        return cls(enabled=len(enabled_jobs), failing=sum(1 for job in enabled_jobs if job.id in failing_ids))


class AttentionItem(BaseModel):
    """One thing that needs a person, with what to open to act on it."""

    kind: Literal["error_group", "connection", "run_stack", "drift", "overdue"]
    severity: Literal["error", "warning"]
    title: str
    target: str | None = None
    since: dt.datetime | None = None
    component_id: UUID | None = None
    component_kind: str | None = None
    run_id: UUID | None = None

    @classmethod
    def from_error_groups(cls, groups: list[ErrorGroup], names: dict[UUID, str]) -> list[AttentionItem]:
        """Merge error groups by job and the error's first line.

        Args:
            groups: The store's error groups (one per job, run, component, type and text).
            names: Job names by id; a group whose run targets anything else
                (an ad-hoc asset run, a deleted target) names no job.

        Returns:
            One error per job and first line, the most runs first, then by title.
        """
        merged: dict[tuple[UUID | None, str], dict[str, Any]] = {}
        for group in groups:
            line = group.error.splitlines()[0].strip() if group.error else ""
            entry = merged.setdefault(
                (group.job_id, line), {"runs": set(), "last_seen": group.last_seen, "sample": group.run_id}
            )
            entry["runs"].add(group.run_id)
            if group.last_seen > entry["last_seen"]:
                entry["last_seen"] = group.last_seen
                entry["sample"] = group.run_id
        pairs = []
        for (job_id, line), entry in merged.items():
            count = len(entry["runs"])
            name = names.get(job_id) if job_id is not None else None
            item = cls(
                kind="error_group",
                severity="error",
                title=f"{count} run{'s' if count != 1 else ''} failed: {line}",
                target=name,
                since=assume_utc(entry["last_seen"]),
                component_id=job_id,
                component_kind="job" if name is not None else None,
                run_id=entry["sample"],
            )
            pairs.append((count, item))
        return [item for _, item in sorted(pairs, key=lambda pair: (-pair[0], pair[1].title))]

    @classmethod
    def from_connections(cls, connections: list[Component]) -> list[AttentionItem]:
        """Flag the connections whose last renewal failed.

        Args:
            connections: The organisation's connection rows.

        Returns:
            One error per connection carrying ``state.last_renewal_error``.
        """
        items = []
        for connection in connections:
            error = (connection.state or {}).get("last_renewal_error")
            if not error:
                continue
            items.append(
                cls(
                    kind="connection",
                    severity="error",
                    title=f"{connection.name or connection.key} connection needs re-authorisation",
                    target=str(error).splitlines()[0],
                    since=connection.state_datetime("last_renewed_at"),
                    component_id=connection.id,
                    component_kind="connection",
                )
            )
        return items

    @classmethod
    def from_run_stacks(
        cls, failed: list[Run], names: dict[UUID, str], since: dt.datetime, now: dt.datetime
    ) -> list[AttentionItem]:
        """Flag the stacks whose latest attempt is a retry that still failed.

        Every stack counts, not only each job's newest one, so a partition
        still failing after retries stays listed once a later run starts.

        Args:
            failed: The latest attempt of each job stack whose latest attempt
                failed, whether or not that attempt ever started.
            names: Job names by id.
            since: Only stacks that finished at or after this instant count.
            now: Only stacks that finished by this instant count.

        Returns:
            One error per stack.
        """
        items = []
        for run in failed:
            if run.status != "failed" or run.attempt <= 1 or run.completed_at is None:
                continue
            completed = assume_utc(run.completed_at)
            if not since <= completed <= now:
                continue
            name = names.get(run.component_id) if run.component_id else None
            items.append(
                cls(
                    kind="run_stack",
                    severity="error",
                    title=f"Still failing after {run.attempt} attempts",
                    target=f"{name} · {run.partition_key}" if run.partition_key else name,
                    since=completed,
                    component_id=run.component_id,
                    component_kind="job",
                    run_id=run.id,
                )
            )
        return items

    @classmethod
    def from_drift(cls, statuses: list[tuple[Component, ComponentStatus]]) -> list[AttentionItem]:
        """Flag the components whose catalog status is not ``ok``.

        An owned asset whose source is itself drifted is left out: it drifts
        because its source does, and the source's one warning covers it.

        Args:
            statuses: Every component row with its read status.

        Returns:
            One warning per drifted component whose parent, if any, is not drifted.
        """
        drifted = {component.id for component, status in statuses if status is not ComponentStatus.OK}
        return [
            cls(
                kind="drift",
                severity="warning",
                title=DRIFT_TITLES[status].format(name=component.name or component.key),
                target=component.key,
                component_id=component.id,
                component_kind=component.kind,
            )
            for component, status in statuses
            if status is not ComponentStatus.OK and component.parent_id not in drifted
        ]

    @classmethod
    def from_overdue(cls, jobs: list[Component], now: dt.datetime) -> list[AttentionItem]:
        """Flag the enabled jobs whose next slot passed more than :data:`OVERDUE_AFTER` ago.

        Args:
            jobs: The organisation's job rows.
            now: The reference instant.

        Returns:
            One warning per overdue job.
        """
        items = []
        for job in jobs:
            due = job.state_datetime("next_run_at")
            if not job.enabled or due is None or now - due <= OVERDUE_AFTER:
                continue
            items.append(
                cls(
                    kind="overdue",
                    severity="warning",
                    title=f"Scheduled run is {format_duration(now - due)} overdue",
                    target=job.name or job.key,
                    since=due,
                    component_id=job.id,
                    component_kind="job",
                )
            )
        return items


class UpcomingRun(BaseModel):
    """A job's next firing and the partition window it will cover."""

    job_id: UUID
    job_name: str
    next_run_at: dt.datetime
    start_key: str | None = None
    end_key: str | None = None

    @classmethod
    def from_jobs(cls, jobs: list[Component], granularities: dict[UUID, TimeGranularity | None]) -> list[UpcomingRun]:
        """List the enabled jobs by their next firing, each with the window that firing covers.

        The window is resolved the way the scheduler resolves it: the job's
        ``lookback`` and ``offset`` over its targets' granularity, on the job
        timezone's clock at the firing instant.

        Args:
            jobs: The organisation's job rows.
            granularities: Each job's target granularity by id; a job absent
                from it, or mapped to ``None``, is unpartitioned.

        Returns:
            Upcoming firings, soonest first; jobs without a stored next slot are absent.
        """
        upcoming = []
        for job in jobs:
            next_run = job.state_datetime("next_run_at")
            if not job.enabled or next_run is None:
                continue
            window = cls._window(job, granularities.get(job.id), next_run)
            upcoming.append(
                cls(
                    job_id=job.id,
                    job_name=job.name or job.key,
                    next_run_at=next_run,
                    start_key=window.granularity.format(window.start) if window else None,
                    end_key=window.granularity.format(window.end) if window else None,
                )
            )
        return sorted(upcoming, key=lambda run: run.next_run_at)

    @classmethod
    def _window(
        cls, job: Component, granularity: TimeGranularity | None, fires_at: dt.datetime
    ) -> TimePartitionWindow | None:
        """Resolve the partition window a job's firing at *fires_at* will cover.

        Args:
            job: The job row, whose config carries ``lookback``, ``offset`` and ``timezone``.
            granularity: The granularity its targets share, ``None`` when unpartitioned.
            fires_at: The firing instant, aware.

        Returns:
            The window, or ``None`` for an unpartitioned job, one whose lookback
            is explicitly null, or one whose lookback or offset is out of range.
        """
        config = job.config or {}
        lookback = config.get("lookback", 1)
        if not lookback or granularity is None:
            return None
        try:
            return TimePartitionWindow.lookback(
                fires_at.astimezone(job_zone(config.get("timezone"))),
                lookback=lookback,
                offset=config.get("offset", 1),
                granularity=granularity,
            )
        except ValueError:
            return None


class KindInventory(BaseModel):
    """How many components of one kind sit in each state."""

    kind: str
    total: int
    healthy: int
    failing: int
    attention: int
    disabled: int

    @classmethod
    def from_statuses(
        cls,
        statuses: list[tuple[Component, ComponentStatus]],
        failing_ids: set[UUID],
        attention_ids: set[UUID],
    ) -> list[KindInventory]:
        """Count each kind's components by state, first match winning: disabled, failing, attention, healthy.

        Args:
            statuses: Every component row with its read status.
            failing_ids: Components whose latest execution, attempt or hook event failed.
            attention_ids: Components needing attention for a reason other than drift.

        Returns:
            One row per kind in :data:`INVENTORY_KINDS`, in that order.
        """
        counts: dict[str, Counter[str]] = {kind: Counter() for kind in INVENTORY_KINDS}
        for component, status in statuses:
            if component.kind not in counts:
                continue
            if not component.enabled:
                state = "disabled"
            elif component.id in failing_ids:
                state = "failing"
            elif status is not ComponentStatus.OK or component.id in attention_ids:
                state = "attention"
            else:
                state = "healthy"
            counts[component.kind][state] += 1
        return [
            cls(
                kind=kind,
                total=sum(tally.values()),
                healthy=tally["healthy"],
                failing=tally["failing"],
                attention=tally["attention"],
                disabled=tally["disabled"],
            )
            for kind, tally in counts.items()
        ]


class OverviewResponse(BaseModel):
    """Everything the overview page draws, except the coverage calendar."""

    generated_at: dt.datetime
    runs: RunsSummary
    activity: ActivitySummary
    backfills: BackfillsSummary
    jobs: JobsSummary
    attention: list[AttentionItem]
    upcoming: list[UpcomingRun]
    recent: list[RunResponse]
    components: list[KindInventory]


@dataclass(frozen=True)
class AssetCoverage:
    """One partitioned asset's evidence, and what the calendar expects of it.

    Attributes:
        asset: The asset row.
        group: The row the asset counts toward on the calendar: its parent
            source, or the asset itself when standalone.
        partitioning: The asset's partitioning, from its catalog definition.
        bounds: The days its attempted partitions span, all-time, or ``None``
            when it was never attempted.
        scheduled: Whether an enabled job targets the asset or its parent source.
        rows: Its coverage rows for the window.
    """

    asset: Component
    group: Component
    partitioning: TimePartitionConfig
    bounds: PartitionBounds | None
    scheduled: bool
    rows: list[CoverageRow]

    @classmethod
    def from_components(
        cls,
        components: list[Component],
        partitionings: dict[UUID, TimePartitionConfig],
        bounds: dict[UUID, PartitionBounds],
        rows: list[CoverageRow],
    ) -> list[AssetCoverage]:
        """Pair each partitioned asset row with its group, partitioning, bounds and evidence.

        Args:
            components: The organisation's source, asset and job rows, with
                their outgoing relations loaded.
            partitionings: Each partitioned asset's partitioning by id; an
                asset absent from it (unpartitioned, drifted) is left out.
            bounds: Each attempted asset's all-time bounds by id.
            rows: The store's coverage rows for the window, from runs of any target.

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
        coverages = []
        for asset in components:
            if asset.kind != "asset" or asset.id not in partitionings:
                continue
            group = by_id.get(asset.parent_id) if asset.parent_id else asset
            if group is None:
                continue
            coverages.append(
                cls(
                    asset=asset,
                    group=group,
                    partitioning=partitionings[asset.id],
                    bounds=bounds.get(asset.id),
                    scheduled=asset.id in targeted or asset.parent_id in targeted,
                    rows=rows_by_asset[asset.id],
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

    def slots(self, day: dt.date, now: dt.datetime) -> int:
        """How many partitions the asset owes on one day of its span.

        An hourly asset owes the hours of the day from its declared start, when
        the start falls on that day, to *now*'s hour, when the day is today
        (the hours elapsed since midnight UTC), else to midnight: 24 on a
        full day, at least one. Any other asset owes one slot a day.

        Args:
            day: A day of the asset's expected span.
            now: The reference instant, aware UTC.

        Returns:
            The slots owed, before raising to the partitions attempted that day.
        """
        if self.partitioning.granularity is not TimeGranularity.HOUR:
            return 1
        start = self.partitioning.start
        first_hour = start.hour if isinstance(start, dt.datetime) and start.date() == day else 0
        end_hour = now.hour if day == now.date() else 24
        return max(1, end_hour - first_hour)


class CoverageDay(BaseModel):
    """One group's asset-partitions on one day: expected, covered, failed."""

    date: dt.date
    source_id: UUID
    expected: int
    covered: int
    failed: int
    failed_run_id: UUID | None = None

    @classmethod
    def from_assets(
        cls, assets: list[AssetCoverage], since: dt.date, until: dt.date, now: dt.datetime
    ) -> list[CoverageDay]:
        """Roll every asset's days onto its group, summing per group and day.

        Args:
            assets: The partitioned assets with their evidence.
            since: First day of the window.
            until: Last day of the window, inclusive.
            now: The reference instant, aware UTC.

        Returns:
            One entry per group and day with anything expected, by group id then day.
        """
        merged: dict[tuple[UUID, dt.date], CoverageDay] = {}
        for asset in assets:
            for day in cls.from_asset(asset, since, until, now):
                slot = (day.source_id, day.date)
                merged[slot] = merged[slot]._plus(day) if slot in merged else day
        return [merged[slot] for slot in sorted(merged)]

    @classmethod
    def from_asset(cls, asset: AssetCoverage, since: dt.date, until: dt.date, now: dt.datetime) -> list[CoverageDay]:
        """Roll one asset's partitions onto the days of its expected span, clipped to the window and today.

        Each day owes the asset's :meth:`AssetCoverage.slots`, raised to the
        partitions attempted on it when more. A
        monthly or yearly key counts toward every day it spans. A partition
        is covered once any execution succeeded, failed when one failed and
        none succeeded; one attempted but still in flight, or canceled, is
        neither, so it reads as missing, as does a day with nothing attempted.
        The failed run kept for a day is the greatest id among its failed
        runs, so the pick does not depend on row order.

        Args:
            asset: The asset with its evidence.
            since: First day of the window.
            until: Last day of the window, inclusive.
            now: The reference instant, aware UTC.

        Returns:
            One entry per day owed, oldest first, each carrying the asset's group id.
        """
        today = now.date()
        span = asset.expected_span(now)
        if span is None:
            return []
        first, last = max(span[0], since), min(span[1], until, today)
        if first > last:
            return []

        attempted: dict[dt.date, set[str]] = defaultdict(set)
        covered: dict[dt.date, set[str]] = defaultdict(set)
        failed: dict[dt.date, set[str]] = defaultdict(set)
        failed_run: dict[dt.date, UUID] = {}
        for row in asset.rows:
            for day in cls._days_of(row.partition_key, first, last):
                attempted[day].add(row.partition_key)
                if row.succeeded:
                    covered[day].add(row.partition_key)
                elif row.failed:
                    failed[day].add(row.partition_key)
                    if row.failed_run_id is not None:
                        failed_run[day] = max(failed_run.get(day, row.failed_run_id), row.failed_run_id)

        return [
            cls(
                date=day,
                source_id=asset.group.id,
                expected=max(asset.slots(day, now), len(attempted[day])),
                covered=len(covered[day]),
                failed=len(failed[day]),
                failed_run_id=failed_run.get(day),
            )
            for day in (first + dt.timedelta(days=i) for i in range((last - first).days + 1))
        ]

    @classmethod
    def _days_of(cls, key: str, since: dt.date, until: dt.date) -> list[dt.date]:
        """List the days a partition key spans, clipped to a range of days.

        Args:
            key: A partition key of any granularity.
            since: First day of the range.
            until: Last day of the range, inclusive.

        Returns:
            The spanned days inside the range, oldest first; empty when the
            partition lies outside it.
        """
        span = PartitionBounds.from_keys([key])
        first, last = max(span.first, since), min(span.last, until)
        return [first + dt.timedelta(days=i) for i in range((last - first).days + 1)]

    def _plus(self, other: CoverageDay) -> CoverageDay:
        """Sum this day with another of the same group and date.

        Args:
            other: The other entry.

        Returns:
            The summed entry, keeping the greater failed run id.
        """
        failed_runs = [run_id for run_id in (self.failed_run_id, other.failed_run_id) if run_id is not None]
        return self.model_copy(
            update={
                "expected": self.expected + other.expected,
                "covered": self.covered + other.covered,
                "failed": self.failed + other.failed,
                "failed_run_id": max(failed_runs, default=None),
            }
        )


class CoverageSource(BaseModel):
    """A calendar group: a source with its assets, or a standalone asset."""

    id: UUID
    name: str
    kind: Literal["source", "asset"]

    @classmethod
    def from_component(cls, component: Component) -> CoverageSource:
        """Name a group after its row.

        Args:
            component: The source row, or the standalone asset row.

        Returns:
            The group, named by the row's name or else its key.
        """
        return cls(
            id=component.id,
            name=component.name or component.key,
            kind="source" if component.kind == "source" else "asset",
        )


class CoverageResponse(BaseModel):
    """The coverage calendar's data over a window of days."""

    since: dt.date
    until: dt.date
    sources: list[CoverageSource]
    days: list[CoverageDay]

    @classmethod
    def from_assets(
        cls, assets: list[AssetCoverage], since: dt.date, until: dt.date, now: dt.datetime
    ) -> CoverageResponse:
        """Build the calendar's data, listing only the groups with a day in the window.

        Args:
            assets: The partitioned assets with their evidence.
            since: First day of the window.
            until: Last day of the window, inclusive.
            now: The reference instant, aware UTC.

        Returns:
            The window, its groups by name, and their days by group (in that
            order) then date.
        """
        days = CoverageDay.from_assets(assets, since, until, now)
        groups = {asset.group.id: asset.group for asset in assets}
        sources = sorted(
            (CoverageSource.from_component(groups[group_id]) for group_id in {day.source_id for day in days}),
            key=lambda source: (source.name, str(source.id)),
        )
        rank = {source.id: i for i, source in enumerate(sources)}
        days.sort(key=lambda day: (rank[day.source_id], day.date))
        return cls(since=since, until=until, sources=sources, days=days)


# -- Endpoints -----------------------------------------------------------------


@router.get("")
def get_overview(
    user: ViewerDep,
    org_id: OrgIdDep,
    store: StoreDep,
    now: Annotated[
        dt.datetime | None,
        Query(description="Reference instant, a test seam for reproducible reads; defaults to the current time"),
    ] = None,
) -> OverviewResponse:
    """Compose the overview page's data for the current organisation.

    Args:
        user: The authenticated user, required to hold at least the ``viewer`` role.
        org_id: The active organisation's UUID.
        store: The Store instance.
        now: Reference instant. A test seam: tests pin it so every window and
            "overdue" reading is deterministic; clients omit it.

    Returns:
        The overview.
    """
    now = assume_utc(now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    day_ago = now - dt.timedelta(hours=24)

    # gather rows
    components = store.components.list_all(org_id)
    keys = {component.id: component.key for component in components}
    statuses = [
        (component, store.components.read(component, parent_key=keys.get(component.parent_id)).status)
        for component in components
    ]
    jobs = [component for component in components if component.kind == "job"]
    enabled_jobs = [job for job in jobs if job.enabled]
    names = {job.id: job.name or job.key for job in jobs}
    running = store.runs.list_all(org_id, status="running", limit=10_000)
    backfills = store.runs.list_backfills(org_id, active_only=True)
    latest_by_job = {run.component_id: run for run in store.runs.latest_by_target(org_id, component_kind="job")}
    failed_stacks = store.runs.list_all(
        org_id, status="failed", component_kind="job", completed_after=day_ago, completed_before=now, limit=10_000
    )
    granularities = store.components.job_partition_granularities([job.id for job in enabled_jobs])
    groups, _ = store.events.error_groups(org_id, event_types=FAILURE_EVENT_TYPES, since=day_ago, until=now)
    hook_events = store.events.latest_by_component(
        org_id, event_types=("hook_fired", "hook_failed"), since=now - HOOK_EVENTS_LOOKBACK
    )

    # derive failing ids
    failing_jobs = {job.id for job in enabled_jobs if (run := latest_by_job.get(job.id)) and run.status == "failed"}
    failing_assets = {
        execution.component_id
        for execution in store.events.latest_executions(org_id)
        if execution.status == "failed" and execution.component_id
    }
    failing_sources = {
        component.parent_id
        for component in components
        if component.kind == "asset" and component.id in failing_assets and component.parent_id
    }
    failing_hooks = {event.component_id for event in hook_events if event.event_type == "hook_failed"}
    failing_ids = {i for i in (*failing_jobs, *failing_assets, *failing_sources, *failing_hooks) if i is not None}

    # build sections
    attention = [
        *AttentionItem.from_error_groups(groups, names),
        *AttentionItem.from_connections([component for component in components if component.kind == "connection"]),
        *AttentionItem.from_run_stacks(failed_stacks, names, day_ago, now),
        *AttentionItem.from_drift(statuses),
        *AttentionItem.from_overdue(jobs, now),
    ]
    attention.sort(key=lambda item: (item.severity != "error", -(item.since.timestamp() if item.since else 0)))
    attention_ids = {item.component_id for item in attention if item.kind == "connection" and item.component_id}

    return OverviewResponse(
        generated_at=now,
        runs=RunsSummary.from_runs(
            store.runs.list_all(
                org_id, completed_after=day_ago, completed_before=now, all_attempts=True, limit=100_000
            ),
            now,
        ),
        activity=ActivitySummary.from_runs(running, store.runs.count(org_id, status="queued"), now),
        backfills=BackfillsSummary.from_backfills(
            backfills, store.runs.count_backfill_runs([backfill.id for backfill in backfills])
        ),
        jobs=JobsSummary.from_jobs(enabled_jobs, failing_jobs),
        attention=attention,
        upcoming=UpcomingRun.from_jobs(jobs, granularities),
        recent=[
            RunResponse.from_run(run)
            for run in store.runs.list_all(org_id, completed_before=now, sort="-completed_at", limit=RECENT_LIMIT)
        ],
        components=KindInventory.from_statuses(statuses, failing_ids, attention_ids),
    )


@router.get("/coverage")
def get_coverage(
    user: ViewerDep,
    org_id: OrgIdDep,
    store: StoreDep,
    since: dt.date,
    until: dt.date,
    now: Annotated[
        dt.datetime | None,
        Query(description="Reference instant, a test seam for reproducible reads; defaults to the current time"),
    ] = None,
) -> CoverageResponse:
    """Read per source and day coverage over a window of at most :data:`MAX_COVERAGE_SPAN_DAYS` days.

    Args:
        user: The authenticated user, required to hold at least the ``viewer`` role.
        org_id: The active organisation's UUID.
        store: The Store instance.
        since: First day of the window.
        until: Last day of the window, inclusive.
        now: Reference instant, which decides which periods have closed and
            how much of today is owed. A test seam: tests pin it; clients omit it.

    Returns:
        The coverage calendar's data.

    Raises:
        HTTPException: 422 when the window is empty or spans more than :data:`MAX_COVERAGE_SPAN_DAYS` days.
    """
    if until < since or (until - since).days + 1 > MAX_COVERAGE_SPAN_DAYS:
        raise HTTPException(status_code=422, detail=f"The window must span 1 to {MAX_COVERAGE_SPAN_DAYS} days")
    now = assume_utc(now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    assets = AssetCoverage.from_components(
        store.components.list_all(org_id, kinds=["source", "asset", "job"]),
        store.components.asset_partitionings(org_id),
        store.events.partition_bounds(org_id),
        store.events.coverage_rows(org_id, since, until),
    )
    return CoverageResponse.from_assets(assets, since, until, now)
