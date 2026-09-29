"""Scheduling tools — jobs, runs, and backfills: read-only monitoring.

Mutating operations (toggling jobs, triggering runs and backfills) live with
the agent — this module is shared with surfaces that must stay read-only.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from interloper_toolkit.context import ToolkitContext, clip
from interloper_toolkit.models import (
    BackfillList,
    EventDetail,
    EventList,
    EventRecord,
    FailureList,
    JobHealth,
    JobHealthStats,
    JobList,
    RunDetail,
    RunErrorEvent,
    RunFailure,
    RunList,
    ToolError,
)

FAILURE_EVENT_TYPES = ("operation_failed", "run_failed")
"""The event types that record a failure's verdict once.

A failed attempt also writes its error on ``asset_data_failed`` /
``dest_write_failed``, so reading every event carrying an error counts each
failure twice; these two are the verdicts.
"""

_ERROR_TEXT_LIMIT = 1_000
_EVENT_TEXT_LIMIT = 10_000
_ERRORS_PER_FAILURE = 50

# --- Jobs ---


def list_jobs(ctx: ToolkitContext, limit: int = 50, offset: int = 0) -> JobList | ToolError:
    """List the scheduled jobs in the organisation, oldest first.

    Args:
        limit: Maximum number of jobs to return (default 50).
        offset: Number of jobs to skip, for paging past the first page.

    Returns each job with its name, cron expression, enabled status,
    last_run_at, and next_run_at, plus the total number of jobs.
    """
    try:
        jobs = ctx.store.components.list_all(ctx.org_id, kinds=["job"], limit=limit, offset=offset)
        total = ctx.store.components.count(ctx.org_id, kinds=["job"])
        return JobList(count=len(jobs), total=total, jobs=jobs)
    except Exception as e:
        return ToolError(error=str(e))


def get_job_health(ctx: ToolkitContext, component_id: str) -> JobHealth | ToolError:
    """Get health summary for a job: metadata, recent success/failure rate.

    Args:
        component_id: UUID of the job to inspect.

    Returns job metadata plus success rate computed from the last 20 runs.
    """
    try:
        jid = UUID(component_id)
        job = ctx.store.components.get(jid, kind="job", org_id=ctx.org_id)
        runs = ctx.store.runs.list_all(ctx.org_id, component_id=jid, limit=20)

        total = len(runs)
        success = sum(1 for r in runs if r.status == "success")
        failed = sum(1 for r in runs if r.status == "failed")

        # Compute average duration for completed runs
        durations = []
        for r in runs:
            if r.started_at and r.completed_at:
                delta = r.completed_at - r.started_at
                durations.append(delta.total_seconds())
        avg_duration_seconds = sum(durations) / len(durations) if durations else None

        return JobHealth(
            job=job,
            health=JobHealthStats(
                total_recent_runs=total,
                success_count=success,
                failed_count=failed,
                success_rate=round(success / total, 2) if total > 0 else None,
                avg_duration_seconds=round(avg_duration_seconds, 1) if avg_duration_seconds else None,
            ),
        )
    except Exception as e:
        return ToolError(error=str(e))


# --- Runs ---


def list_recent_runs(
    ctx: ToolkitContext,
    component_id: str | None = None,
    status: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> RunList | ToolError:
    """List recent runs, newest first, with optional filters.

    Args:
        component_id: Filter by job UUID (optional).
        status: Filter by status: 'queued', 'running', 'success', 'failed', 'canceled' (optional).
        limit: Maximum number of runs to return (default 20).
        offset: Number of runs to skip, for paging past the first page.

    Returns the page of runs and the total number matching the filters.
    """
    try:
        jid = UUID(component_id) if component_id else None
        runs = ctx.store.runs.list_all(ctx.org_id, component_id=jid, status=status, limit=limit, offset=offset)
        total = ctx.store.runs.count(ctx.org_id, component_id=jid, status=status)
        return RunList(count=len(runs), total=total, runs=runs)
    except Exception as e:
        return ToolError(error=str(e))


def get_run_detail(ctx: ToolkitContext, run_id: str) -> RunDetail | ToolError:
    """Get a single run with its per-operation execution status.

    Args:
        run_id: UUID of the run.

    Returns the run metadata and one execution summary per operation (status,
    attempts, timing). The run's event timeline is list_run_events.
    """
    try:
        rid = UUID(run_id)
        run = ctx.store.runs.get(rid, org_id=ctx.org_id)
        executions = ctx.store.events.list_executions(rid)

        return RunDetail(run=run, executions=executions)
    except Exception as e:
        return ToolError(error=str(e))


def list_run_events(
    ctx: ToolkitContext,
    run_id: str,
    component_id: str | None = None,
    event_types: list[str] | None = None,
    errors_only: bool = False,
    limit: int = 50,
    offset: int = 0,
) -> EventList | ToolError:
    """List a run's events, oldest first, with optional filters.

    Args:
        run_id: UUID of the run.
        component_id: Keep only events of this component (an asset's UUID).
        event_types: Keep only events of these types, e.g. ['operation_failed',
            'operation_retried'].
        errors_only: Keep only events carrying an error.
        limit: Maximum number of events to return (default 50).
        offset: Number of events to skip, for paging past the first page.

    Returns the page of events without their tracebacks (get_event has them)
    and the total number of events matching the filters.
    """
    try:
        rid = UUID(run_id)
        ctx.store.runs.get(rid, org_id=ctx.org_id)
        filters: dict[str, Any] = {
            "run_id": rid,
            "component_ids": [UUID(component_id)] if component_id else None,
            "event_types": event_types,
            "has_error": errors_only,
        }
        events = ctx.store.events.list_all(**filters, limit=limit, offset=offset)
        total = ctx.store.events.count(**filters)
        records = [EventRecord(**e.model_dump(exclude={"org_id", "traceback"})) for e in events]
        return EventList(run_id=run_id, count=len(records), total=total, events=records)
    except Exception as e:
        return ToolError(error=str(e))


def get_event(ctx: ToolkitContext, event_id: str) -> EventDetail | ToolError:
    """Get one event in full, including its traceback.

    Args:
        event_id: UUID of the event, from list_run_events or list_failures.

    Returns the event row. A very long error keeps its first 10k characters
    and a very long traceback its last 10k (the raising frame is at the end);
    a clipped field ends or starts with an ``…[+N chars]`` marker.
    """
    try:
        event = ctx.store.events.get(UUID(event_id), org_id=ctx.org_id)
        clipped = event.model_copy(
            update={
                "error": clip(event.error, _EVENT_TEXT_LIMIT),
                "traceback": clip(event.traceback, _EVENT_TEXT_LIMIT, tail=True),
            }
        )
        return EventDetail(event=clipped)
    except Exception as e:
        return ToolError(error=str(e))


def list_failures(ctx: ToolkitContext, limit: int = 20, offset: int = 0) -> FailureList | ToolError:
    """List recent failed runs, newest first, with their error events.

    Args:
        limit: Maximum number of failed runs to return (default 20).
        offset: Number of failed runs to skip, for paging past the first page.

    Returns the page of failed runs, each with its error count and the first
    50 of its errors (operation and run failures, text clipped to 1k
    characters; get_event has the full text), plus the total number of
    failed runs.
    """
    try:
        failed_runs = ctx.store.runs.list_all(ctx.org_id, status="failed", limit=limit, offset=offset)
        total = ctx.store.runs.count(ctx.org_id, status="failed")

        results = []
        for run in failed_runs:
            filters: dict[str, Any] = {"run_id": run.id, "event_types": FAILURE_EVENT_TYPES, "has_error": True}
            events = ctx.store.events.list_all(**filters, limit=_ERRORS_PER_FAILURE)
            errors = [
                RunErrorEvent(
                    event_id=e.id,
                    component_key=e.component_key,
                    error=clip(e.error, _ERROR_TEXT_LIMIT) or "",
                    timestamp=e.timestamp,
                )
                for e in events
            ]
            error_count = ctx.store.events.count(**filters)
            results.append(RunFailure(run=run, error_count=error_count, errors=errors))

        return FailureList(count=len(results), total=total, failures=results)
    except Exception as e:
        return ToolError(error=str(e))


# --- Backfills ---


def list_backfills(
    ctx: ToolkitContext, active_only: bool = True, limit: int = 20, offset: int = 0
) -> BackfillList | ToolError:
    """List backfills, newest first, optionally filtered to active ones only.

    Args:
        active_only: If true, only return running/queued backfills (default true).
        limit: Maximum number of backfills to return (default 20).
        offset: Number of backfills to skip, for paging past the first page.

    Returns the page of backfills with their status, date range, and
    partition progress, plus the total number matching the filter.
    """
    try:
        backfills = ctx.store.runs.list_backfills(ctx.org_id, active_only=active_only, limit=limit, offset=offset)
        total = ctx.store.runs.count_backfills(ctx.org_id, active_only=active_only)
        return BackfillList(count=len(backfills), total=total, backfills=backfills)
    except Exception as e:
        return ToolError(error=str(e))
