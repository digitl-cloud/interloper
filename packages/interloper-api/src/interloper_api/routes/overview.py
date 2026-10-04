"""Overview API: the landing page's health, attention, schedule and inventory in one read.

``store.insights`` decides what the data means (failing, overdue, covered);
this module words it for the page. Each section of the response is built from
an insight fact by its own model's ``from_*`` constructor, so every tile and
list on the page shares one vocabulary (see
``docs/superpowers/specs/2026-09-30-overview-page-design.md``).
"""

from __future__ import annotations

import datetime as dt
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from interloper.utils.time import assume_utc
from interloper_db import ComponentStatus, RunQuery
from interloper_db.store import insights
from pydantic import BaseModel

from interloper_api.dependencies import OrgIdDep, StoreDep, ViewerDep
from interloper_api.routes.runs import RunResponse
from interloper_api.utils import format_duration

router = APIRouter(prefix="/overview", tags=["overview"])

RECENT_LIMIT = 5
MAX_COVERAGE_SPAN_DAYS = 400
DRIFT_TITLES = {
    ComponentStatus.MISSING: "{name} is missing from the catalog",
    ComponentStatus.DISABLED: "{name} is disabled in this deployment",
    ComponentStatus.UNREADABLE: "{name} has an unreadable config",
}


# -- Response models -----------------------------------------------------------


class JobsSummary(BaseModel):
    """Enabled jobs, and how many of them last ended in failure."""

    enabled: int
    failing: int

    @classmethod
    def from_jobs(cls, jobs: list[insights.JobHealth]) -> JobsSummary:
        """Count the enabled jobs and the failing ones.

        Args:
            jobs: Every job's health.

        Returns:
            The two counts.
        """
        return cls(enabled=sum(job.job.enabled for job in jobs), failing=sum(job.failing for job in jobs))


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
    def from_attention(cls, item: insights.Attention, now: dt.datetime) -> AttentionItem:
        """Word an attention fact for the page.

        Args:
            item: The insight attention fact.
            now: The reference instant an overdue job's lateness is measured to.

        Returns:
            The response model.
        """
        component = item.component
        name = (component.name or component.key) if component else None
        identity = {
            "since": item.since,
            "component_id": component.id if component else None,
            "component_kind": component.kind if component else None,
            "run_id": item.run_id,
        }
        if item.kind == "error_group" and item.error_group is not None:
            group = item.error_group
            count = len(group.runs)
            summary = group.cause.summary if group.cause else group.sample
            title = f"{count} run{'s' if count != 1 else ''} failed: {summary}"
            return cls(kind="error_group", severity="error", title=title, target=name, **identity)
        if item.kind == "renewal_error":
            title = f"{name} connection needs re-authorisation"
            return cls(kind="connection", severity="error", title=title, target=item.detail, **identity)
        if item.kind == "still_failing":
            target = f"{name} · {item.partition_key}" if item.partition_key else name
            title = f"Still failing after {item.attempt} attempts"
            return cls(kind="run_stack", severity="error", title=title, target=target, **identity)
        if item.kind == "drift" and item.status is not None and component is not None:
            title = DRIFT_TITLES[item.status].format(name=name)
            return cls(kind="drift", severity="warning", title=title, target=component.key, **identity)
        late = format_duration(now - item.since) if item.since else "some time"
        title = f"Scheduled run is {late} overdue"
        return cls(kind="overdue", severity="warning", title=title, target=name, **identity)


class UpcomingRun(BaseModel):
    """A job's next firing and the partition window it will cover."""

    job_id: UUID
    job_name: str
    next_run_at: dt.datetime
    start_key: str | None = None
    end_key: str | None = None

    @classmethod
    def from_jobs(cls, jobs: list[insights.JobHealth]) -> list[UpcomingRun]:
        """List the enabled jobs by their next firing, each with the window that firing covers.

        Args:
            jobs: Every job's health.

        Returns:
            Upcoming firings, soonest first; jobs without a stored next slot are absent.
        """
        upcoming = [
            cls(
                job_id=job.job.id,
                job_name=job.job.name or job.job.key,
                next_run_at=job.next_run_at,
                start_key=job.window.granularity.format(job.window.start) if job.window else None,
                end_key=job.window.granularity.format(job.window.end) if job.window else None,
            )
            for job in jobs
            if job.job.enabled and job.next_run_at is not None
        ]
        return sorted(upcoming, key=lambda run: run.next_run_at)


class OverviewResponse(BaseModel):
    """Everything the overview page draws, except the coverage calendar."""

    generated_at: dt.datetime
    runs: insights.FinishedRuns
    activity: insights.InFlight
    backfills: insights.BackfillProgress
    jobs: JobsSummary
    attention: list[AttentionItem]
    upcoming: list[UpcomingRun]
    recent: list[RunResponse]
    components: list[insights.KindInventory]


class CoverageSource(BaseModel):
    """A calendar group, a source with its assets or a standalone asset, with its days.

    The arrays run from :attr:`start` to the group's last expected day in the
    window: index ``i`` is the day ``start + i``, and a day inside that run
    with nothing expected is a zero in every array.
    """

    id: UUID
    name: str
    kind: Literal["source", "asset"]
    start: dt.date
    expected: list[int]
    covered: list[int]
    failed: list[int]
    failed_run_ids: dict[int, UUID]

    @classmethod
    def from_group(cls, group: insights.CoverageGroup) -> CoverageSource:
        """Name a calendar group after its row and attach its days.

        Args:
            group: The insight coverage group.

        Returns:
            The group, named by the row's name or else its key.
        """
        component, days = group.component, group.days
        return cls(
            id=component.id,
            name=component.name or component.key,
            kind="source" if component.kind == "source" else "asset",
            start=days.start,
            expected=days.expected,
            covered=days.covered,
            failed=days.failed,
            failed_run_ids=days.failed_run_ids,
        )


class CoverageResponse(BaseModel):
    """The coverage calendar's data over a window of days."""

    since: dt.date
    until: dt.date
    sources: list[CoverageSource]


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
    health = store.insights.health(org_id, now=now)
    activity = store.insights.activity(org_id, now=now)
    attention = [AttentionItem.from_attention(item, now) for item in health.attention]
    attention.sort(key=lambda item: (item.severity != "error", -(item.since.timestamp() if item.since else 0)))
    recent = store.runs.list(org_id, RunQuery(completed_before=now, sort="-completed_at", limit=RECENT_LIMIT))
    return OverviewResponse(
        generated_at=now,
        runs=activity.runs,
        activity=activity.in_flight,
        backfills=activity.backfills,
        jobs=JobsSummary.from_jobs(health.jobs),
        attention=attention,
        upcoming=UpcomingRun.from_jobs(health.jobs),
        recent=[RunResponse.from_run(run) for run in recent.items],
        components=health.inventory,
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
    groups = store.insights.coverage_by_day(org_id, since=since, until=until, now=now)
    return CoverageResponse(since=since, until=until, sources=[CoverageSource.from_group(group) for group in groups])
