"""Scheduling tools — jobs, runs, and backfills: read-only monitoring.

Mutating operations (toggling jobs, triggering runs and backfills) live with
the agent — this module is shared with surfaces that must stay read-only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

from interloper_toolkit.context import ToolkitContext, clip
from interloper_toolkit.errors import classify
from interloper_toolkit.models import (
    AttemptTiming,
    BackfillList,
    BackfillTimeline,
    ErrorBreakdown,
    ErrorCause,
    ErrorGroupRow,
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
    Scan,
    ToolError,
)
from interloper_toolkit.stats import max_concurrent, percentile, window

FAILURE_EVENT_TYPES = ("operation_failed", "run_failed")
"""The event types that record a failure's verdict once.

A failed attempt also writes its error on ``asset_data_failed`` /
``dest_write_failed``, so reading every event carrying an error counts each
failure twice; these two are the verdicts, and ``operation_retried`` the
attempts that were not final.
"""

_ATTEMPT_EVENT_TYPES = ("operation_retried", *FAILURE_EVENT_TYPES)
_GROUP_KEYS = ("job", "asset", "cause")
_ERROR_TEXT_LIMIT = 1_000
_EVENT_TEXT_LIMIT = 10_000
_ERRORS_PER_FAILURE = 50
_SAMPLE_LIMIT = 200


@dataclass
class _Group:
    """An error group under construction, before it becomes an :class:`ErrorGroupRow`."""

    job_id: UUID | None
    asset_key: str | None
    cause: ErrorCause | None
    first_seen: datetime
    last_seen: datetime
    sample_run_id: UUID
    sample: str
    failed_attempts: int = 0
    terminal_failures: int = 0
    runs: set[UUID] = field(default_factory=set)

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


def error_breakdown(
    ctx: ToolkitContext,
    since: str | None = None,
    until: str | None = None,
    component_id: str | None = None,
    backfill_id: str | None = None,
    run_id: str | None = None,
    group_by: list[str] | None = None,
    limit: int = 25,
    offset: int = 0,
) -> ErrorBreakdown | ToolError:
    """Group the failures over a window by job, asset and cause, loudest first.

    Counts every failed attempt once (retried attempts and final failures,
    plus run-level failures), never the duplicate error an asset event
    carries. The cause is parsed from the error text: exception type, HTTP
    status, endpoint host and path with ids masked, and the vendor's error
    code where the text has one.

    Args:
        since: ISO date or datetime the window opens at (default: 24 hours
            ago, or unbounded when run_id or backfill_id is given).
        until: ISO date or datetime the window closes before (default: open).
        component_id: Keep failures of runs targeting this job UUID.
        backfill_id: Keep failures of this backfill's runs.
        run_id: Keep failures of this run.
        group_by: Any of 'job', 'asset', 'cause' (default all three); a key
            left out is omitted from the rows.
        limit: Maximum number of groups to return (default 25).
        offset: Number of groups to skip, for paging past the first page.

    Returns the page of groups with attempt and run counts, first and last
    occurrence, a sample run to drill into with list_run_events, and the
    total number of groups; ``scan.truncated`` says whether the read hit its
    cap.
    """
    try:
        keys = tuple(group_by or _GROUP_KEYS)
        if unknown := set(keys) - set(_GROUP_KEYS):
            return ToolError(error=f"Unknown group_by key(s): {sorted(unknown)}; expected any of {list(_GROUP_KEYS)}")
        scoped = run_id is not None or backfill_id is not None
        start, end = window(since, until, default_days=None if scoped else 1)
        rows, truncated = ctx.store.events.error_groups(
            ctx.org_id,
            event_types=_ATTEMPT_EVENT_TYPES,
            since=start,
            until=end,
            job_id=UUID(component_id) if component_id else None,
            backfill_id=UUID(backfill_id) if backfill_id else None,
            run_id=UUID(run_id) if run_id else None,
        )

        causes: dict[str, ErrorCause] = {}
        merged: dict[tuple[Any, ...], _Group] = {}
        for row in rows:
            cause = causes.get(row.error) or causes.setdefault(row.error, classify(row.error))
            key = (
                row.job_id if "job" in keys else None,
                row.component_key if "asset" in keys else None,
                cause.fingerprint if "cause" in keys else None,
            )
            if key not in merged:
                merged[key] = _Group(
                    job_id=key[0],
                    asset_key=key[1],
                    cause=cause if "cause" in keys else None,
                    first_seen=row.first_seen,
                    last_seen=row.last_seen,
                    sample_run_id=row.run_id,
                    sample=clip(row.error.splitlines()[0] if row.error else "", _SAMPLE_LIMIT) or "",
                )
            group = merged[key]
            group.failed_attempts += row.count
            if row.event_type != "operation_retried":
                group.terminal_failures += row.count
            group.runs.add(row.run_id)
            group.first_seen = min(group.first_seen, row.first_seen)
            group.last_seen = max(group.last_seen, row.last_seen)

        job_names = {j.id: j.name for j in ctx.store.components.list_all(ctx.org_id, kinds=["job"])}
        groups = sorted(merged.values(), key=lambda g: (-g.failed_attempts, g.last_seen))
        page = [
            ErrorGroupRow(
                job_id=g.job_id,
                job_name=job_names.get(g.job_id) if g.job_id else None,
                asset_key=g.asset_key,
                cause=g.cause,
                failed_attempts=g.failed_attempts,
                terminal_failures=g.terminal_failures,
                runs_affected=len(g.runs),
                first_seen=g.first_seen,
                last_seen=g.last_seen,
                sample_run_id=g.sample_run_id,
                sample=g.sample,
            )
            for g in groups[offset : offset + limit]
        ]
        return ErrorBreakdown(
            since=start,
            until=end,
            group_by=list(keys),
            count=len(page),
            total=len(groups),
            scan=Scan(rows=len(rows), truncated=truncated),
            groups=page,
        )
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


def backfill_timeline(
    ctx: ToolkitContext, backfill_id: str, limit: int = 50, offset: int = 0
) -> BackfillTimeline | ToolError:
    """Show what a backfill's runs actually did next to what it declared.

    Args:
        backfill_id: UUID of the backfill.
        limit: Maximum number of run attempts to return (default 50).
        offset: Number of attempts to skip, for paging past the first page.

    Returns the backfill row, the peak number of attempts running at once
    (to compare with its declared concurrency), the lag from creation to
    the first start, run duration percentiles, attempts by status, and a
    page of attempts in start order with their timing.
    """
    try:
        bid = UUID(backfill_id)
        backfill = ctx.store.runs.get_backfill(bid, org_id=ctx.org_id)
        total = ctx.store.runs.count(ctx.org_id, backfill_id=bid, all_attempts=True)
        runs = ctx.store.runs.list_all(ctx.org_id, backfill_id=bid, all_attempts=True, limit=total)
        runs.sort(key=lambda r: (r.started_at is None, r.started_at or r.created_at or datetime.min))

        started = [r for r in runs if r.started_at is not None]
        durations = [_seconds(r.started_at, r.completed_at) for r in started if r.completed_at is not None]
        by_status: dict[str, int] = {}
        for r in runs:
            by_status[r.status] = by_status.get(r.status, 0) + 1
        first_start = min((r.started_at for r in started), default=None)  # ty: ignore[invalid-argument-type]

        page = [
            AttemptTiming(
                run_id=r.id,
                partition_key=r.partition_key,
                status=r.status,
                attempt=r.attempt,
                started_at=r.started_at,
                completed_at=r.completed_at,
                duration_s=_seconds(r.started_at, r.completed_at),
                queue_wait_s=_seconds(r.created_at, r.started_at),
            )
            for r in runs[offset : offset + limit]
        ]
        return BackfillTimeline(
            backfill=backfill,
            max_concurrent_runs=max_concurrent((r.started_at, r.completed_at) for r in started),  # ty: ignore[invalid-argument-type]
            first_start_lag_s=_seconds(backfill.created_at, first_start),
            duration_p50_s=percentile([d for d in durations if d is not None], 50),
            duration_p90_s=percentile([d for d in durations if d is not None], 90),
            runs_by_status=by_status,
            count=len(page),
            total=total,
            attempts=page,
        )
    except Exception as e:
        return ToolError(error=str(e))


def _seconds(start: datetime | None, end: datetime | None) -> float | None:
    """Seconds from *start* to *end*, to a tenth.

    Args:
        start: The opening instant, or ``None`` when it never happened.
        end: The closing instant, or ``None`` when it never happened.

    Returns:
        The elapsed seconds, or ``None`` when either instant is missing.
    """
    if start is None or end is None:
        return None
    return round((end - start).total_seconds(), 1)
