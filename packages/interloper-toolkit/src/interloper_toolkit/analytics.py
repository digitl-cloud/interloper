"""Analytics tools: job health, run statistics and partition coverage, read off ``store.insights``."""

from __future__ import annotations

import datetime
from uuid import UUID

from interloper_toolkit.context import ToolkitContext
from interloper_toolkit.models import (
    AssetCoverage,
    JobHealthReport,
    JobHealthRow,
    PipelineOverview,
    RunStats,
    ToolError,
)
from interloper_toolkit.stats import window


def pipeline_overview(ctx: ToolkitContext, upcoming: int = 5) -> PipelineOverview | ToolError:
    """The organisation at a glance: start here for health, status and "what failed?" questions.

    One call returns the last 24 hours of runs (succeeded, failed), what is
    running and queued now, active backfills, the failing and overdue jobs,
    the next scheduled firings, everything that needs a person (failures
    grouped by cause, connections needing re-authorisation, runs still
    failing after retries, catalog drift, overdue schedules) and each
    component kind's count by state. Its ids lead to the detailed tools,
    for when the user asks for more.

    Args:
        upcoming: How many of the next scheduled firings to include (default 5).
    """
    try:
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        health = ctx.store.insights.health(ctx.org_id, now=now)
        activity = ctx.store.insights.activity(ctx.org_id, now=now)
        return PipelineOverview.from_insights(health, activity, upcoming)
    except Exception as e:
        return ToolError(error=str(e))


def job_health(ctx: ToolkitContext, limit: int = 50, offset: int = 0) -> JobHealthReport | ToolError:
    """Every job's health: failing, overdue, last success and next firing.

    A job is failing when it is enabled and its latest attempt failed, and
    overdue when its scheduled slot passed more than 15 minutes ago without
    the scheduler firing it.

    Args:
        limit: Maximum number of jobs to return (default 50).
        offset: Number of jobs to skip, for paging past the first page.

    Returns the page of jobs, failing then overdue ones first, each with its
    latest run, last success, next firing and the partitions that firing
    covers, plus the org-wide failing and overdue counts.
    """
    try:
        health = ctx.store.insights.health(ctx.org_id, now=datetime.datetime.now(tz=datetime.timezone.utc))
        rows = sorted(
            (JobHealthRow.from_health(job) for job in health.jobs),
            key=lambda row: (not row.failing, not row.overdue, row.job_name or ""),
        )
        page = rows[offset : offset + limit]
        return JobHealthReport(
            failing=sum(row.failing for row in rows),
            overdue=sum(row.overdue for row in rows),
            count=len(page),
            total=len(rows),
            jobs=page,
        )
    except Exception as e:
        return ToolError(error=str(e))


def run_stats(
    ctx: ToolkitContext,
    since: str | None = None,
    until: str | None = None,
    component_id: str | None = None,
    limit: int = 25,
    offset: int = 0,
) -> RunStats | ToolError:
    """Per-job run statistics over a window: verdicts, durations and retries.

    Args:
        since: ISO date or datetime the window opens at (default: 7 days ago).
        until: ISO date or datetime the window closes before (default: open).
        component_id: Restrict to this job UUID.
        limit: Maximum number of jobs to return (default 25).
        offset: Number of jobs to skip, for paging past the first page.

    Returns one row per job that ran in the window, most failures first: run
    stacks by final status, attempts, duration p50/p90/max in seconds, how
    many stacks were retried and of those how many healed or are still
    failing, and the last success.
    """
    try:
        start, end = window(since, until, default_days=7)
        job_id = UUID(component_id) if component_id else None
        outcomes = ctx.store.insights.outcomes(ctx.org_id, since=start, until=end, job_id=job_id)
        page = outcomes[offset : offset + limit]
        return RunStats(since=start, until=end, count=len(page), total=len(outcomes), jobs=page)
    except Exception as e:
        return ToolError(error=str(e))


def asset_coverage(
    ctx: ToolkitContext,
    component_id: str,
    start_key: str,
    end_key: str,
    limit: int = 50,
    offset: int = 0,
) -> AssetCoverage | ToolError:
    """Per-asset partition coverage of a job over a range of partition keys.

    An asset counts as covered for a partition once any run's execution of it
    succeeded, whatever the run targeted, so a run where most assets
    succeeded shows what is actually missing. Every asset the job targets
    appears, directly or through a source it targets.

    Args:
        component_id: UUID of the job.
        start_key: First partition key, in the job's granularity (2026-07-01,
            2026-07, 2026, or 2026-07-01T13).
        end_key: Last partition key, inclusive; must share the start key's
            granularity.
        limit: Maximum number of assets to return (default 50).
        offset: Number of assets to skip, for paging past the first page.

    Returns the page of assets, least covered first, each with its covered,
    failed and never-run partition counts and its missing partitions as
    ranges ready for a backfill, plus a rollup of the range's partitions by
    whether all, some or none of the assets are covered.
    """
    try:
        coverage = ctx.store.insights.coverage_by_key(
            ctx.org_id, UUID(component_id), start_key=start_key, end_key=end_key
        )
        return AssetCoverage.from_coverage(coverage, limit=limit, offset=offset)
    except Exception as e:
        return ToolError(error=str(e))
