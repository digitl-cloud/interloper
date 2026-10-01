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
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from interloper.partitioning.time import TimeGranularity, TimePartition, TimePartitionWindow
from interloper.utils.time import assume_utc
from interloper_db import Backfill, Component, ComponentStatus
from interloper_db.models import Run
from interloper_db.store.events import CoverageRow, ErrorGroup
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


class CoverageDay(BaseModel):
    """One job's asset-partitions on one day: expected, covered, failed."""

    date: dt.date
    job_id: UUID
    expected: int
    covered: int
    failed: int
    failed_run_id: UUID | None = None

    @classmethod
    def from_rows(
        cls, rows: list[CoverageRow], jobs: list[Component], since: dt.date, until: dt.date, now: dt.datetime
    ) -> list[CoverageDay]:
        """Roll partition rows onto days, per job, applying the calendar's day rules.

        Only rows of the listed jobs count: runs targeting another kind of
        component (an ad-hoc asset run) have no place on the calendar.

        Args:
            rows: The store's coverage rows for the window.
            jobs: The organisation's job rows, in the order the result follows.
            since: First day of the window.
            until: Last day of the window, inclusive.
            now: The reference instant, aware UTC; its date decides which periods
                have closed, and its hour how much of today an hourly job owes.

        Returns:
            One entry per job and day with anything expected, by job then day.
        """
        rows_by_job: dict[UUID, list[CoverageRow]] = defaultdict(list)
        for row in rows:
            rows_by_job[row.job_id].append(row)
        return [
            day
            for job in jobs
            if job.id in rows_by_job
            for day in cls._from_job_rows(job, rows_by_job[job.id], since, until, now)
        ]

    @classmethod
    def _from_job_rows(
        cls, job: Component, rows: list[CoverageRow], since: dt.date, until: dt.date, now: dt.datetime
    ) -> list[CoverageDay]:
        """Roll one job's partition rows onto days.

        An hourly key counts toward its day (24 slots per asset; today, the
        hours elapsed since midnight UTC, at least one, or the hours attempted
        when more), a monthly or yearly key toward every day it spans up to
        today. The days run from the first attempted day in the window to the
        last day of the last period that closed before today (yesterday, for a
        daily or hourly job), or to the last attempted day when that is later,
        so the open period counts once attempted. A disabled job stops at its
        last attempted day. Days in that range with nothing attempted are
        expected and uncovered. A day's failed count holds the uncovered
        asset-partitions with a failed execution; one attempted but still in
        flight, or canceled, is neither covered nor failed, so it reads as
        missing. The failed run kept for a day is the greatest id among its
        failed runs, so the pick does not depend on row order.

        Args:
            job: The job row, whose ``enabled`` decides where its days stop.
            rows: The job's coverage rows; those whose partition falls after
                today, or outside the window, are left out.
            since: First day of the window.
            until: Last day of the window, inclusive.
            now: The reference instant, aware UTC.

        Returns:
            One entry per day with anything expected, oldest first.
        """
        today = now.date()
        horizon = min(until, today)
        spans = {row.partition_key: cls._days_of(row.partition_key, since, horizon) for row in rows}
        rows = [row for row in rows if spans[row.partition_key]]
        if not rows:
            return []

        attempted: dict[dt.date, set[tuple[UUID, str]]] = defaultdict(set)
        covered: dict[dt.date, set[tuple[UUID, str]]] = defaultdict(set)
        failed: dict[dt.date, set[tuple[UUID, str]]] = defaultdict(set)
        failed_run: dict[dt.date, UUID] = {}
        for row in rows:
            for day in spans[row.partition_key]:
                attempted[day].add((row.asset_id, row.partition_key))
                if row.succeeded:
                    covered[day].add((row.asset_id, row.partition_key))
                elif row.failed:
                    failed[day].add((row.asset_id, row.partition_key))
                    if row.failed_run_id is not None:
                        failed_run[day] = max(failed_run.get(day, row.failed_run_id), row.failed_run_id)

        # A job whose targets changed granularity mid-window is read at its most recent key's.
        latest = max(rows, key=lambda row: (spans[row.partition_key][-1], row.partition_key))
        granularity = TimePartition.from_key(latest.partition_key).granularity
        slots = 24 if granularity is TimeGranularity.HOUR else 1
        slots_today = max(1, now.hour) if granularity is TimeGranularity.HOUR else 1
        first, last = min(attempted), max(attempted)
        end = last
        if job.enabled:
            period = TimeGranularity.DAY if granularity is TimeGranularity.HOUR else granularity
            end = max(last, min(period.truncate(today) - dt.timedelta(days=1), until))

        assets = len({row.asset_id for row in rows})
        return [
            cls(
                date=day,
                job_id=job.id,
                expected=max(assets * slots_today, len(attempted[day])) if day == today else assets * slots,
                covered=len(covered[day]),
                failed=len(failed[day]),
                failed_run_id=failed_run.get(day),
            )
            for day in (first + dt.timedelta(days=i) for i in range((end - first).days + 1))
        ]

    @classmethod
    def _days_of(cls, key: str, since: dt.date, until: dt.date) -> list[dt.date]:
        """List the days a partition key spans, clipped to the window.

        Args:
            key: A partition key of any granularity.
            since: First day of the window.
            until: Last day of the window, inclusive.

        Returns:
            The spanned days inside the window, oldest first; empty when the
            partition lies outside it.
        """
        start, end = TimePartition.from_key(key).bounds
        # An hourly partition's bounds are datetimes (a datetime is also a date), and it never crosses midnight.
        if isinstance(start, dt.datetime):
            first = last = start.date()
        else:
            first, last = start, end - dt.timedelta(days=1)
        first, last = max(first, since), min(last, until)
        return [first + dt.timedelta(days=i) for i in range((last - first).days + 1)]


class CoverageJob(BaseModel):
    """A job that has coverage in the window."""

    id: UUID
    name: str


class CoverageResponse(BaseModel):
    """The coverage calendar's data over a window of days."""

    since: dt.date
    until: dt.date
    jobs: list[CoverageJob]
    days: list[CoverageDay]

    @classmethod
    def from_rows(
        cls, rows: list[CoverageRow], jobs: list[Component], since: dt.date, until: dt.date, now: dt.datetime
    ) -> CoverageResponse:
        """Build the calendar's data, listing only the jobs with a day in the window.

        Args:
            rows: The store's coverage rows for the window.
            jobs: The organisation's job rows, in the order both lists follow.
            since: First day of the window.
            until: Last day of the window, inclusive.
            now: The reference instant, aware UTC.

        Returns:
            The window, its jobs and their days.
        """
        days = CoverageDay.from_rows(rows, jobs, since, until, now)
        seen = {day.job_id for day in days}
        return cls(
            since=since,
            until=until,
            jobs=[CoverageJob(id=job.id, name=job.name or job.key) for job in jobs if job.id in seen],
            days=days,
        )


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
    """Read per job and day coverage over a window of at most :data:`MAX_COVERAGE_SPAN_DAYS` days.

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
    jobs = store.components.list_all(org_id, kinds=["job"])
    rows = store.events.coverage_rows(org_id, since, until)
    return CoverageResponse.from_rows(rows, jobs, since, until, now)
