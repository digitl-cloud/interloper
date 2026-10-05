"""Scheduling tools — jobs, runs, and backfills: monitoring and control."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from interloper.utils import percentile
from interloper_db import (
    ACTIVE_BACKFILL_STATUSES,
    BackfillQuery,
    ComponentQuery,
    EventQuery,
    ExecutionQuery,
    PageQuery,
    RunQuery,
    RunStatus,
)
from interloper_db.store.insights import GROUP_KEYS

from interloper_toolkit.authz import requires_role
from interloper_toolkit.context import ToolkitContext
from interloper_toolkit.models import (
    AttemptTiming,
    BackfillCanceled,
    BackfillList,
    BackfillQueued,
    BackfillTimeline,
    ComponentRef,
    ComponentToggled,
    ErrorBreakdown,
    ErrorGroupRow,
    EventDetail,
    EventList,
    EventRecord,
    FailureList,
    JobList,
    RunDetail,
    RunErrorEvent,
    RunFailure,
    RunList,
    RunQueued,
    RunRetried,
    Scan,
    ToolError,
)
from interloper_toolkit.stats import max_concurrent, window
from interloper_toolkit.utils import clip

FAILURE_EVENT_TYPES = ("operation_failed", "run_failed")
"""The event types that record a failure's verdict once.

A failed attempt also writes its error on ``asset_data_failed`` /
``dest_write_failed``, so reading every event carrying an error counts each
failure twice; these two are the verdicts, and ``operation_retried`` the
attempts that were not final.
"""

_ERROR_TEXT_LIMIT = 1_000
_EVENT_TEXT_LIMIT = 10_000
_ERRORS_PER_FAILURE = 50
_SAMPLE_LIMIT = 200


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
        jobs = ctx.store.components.list(
            ctx.org_id, ComponentQuery(kind=["job"], roots_only=False, limit=limit, offset=offset)
        )
        return JobList(count=len(jobs.items), total=jobs.total, jobs=jobs.items)
    except Exception as e:
        return ToolError(error=str(e))


@requires_role("editor")
def toggle_job(ctx: ToolkitContext, component_id: str, enabled: bool) -> ComponentToggled | ToolError:
    """Enable or disable a scheduled job.

    Args:
        component_id: UUID of the job.
        enabled: True to enable, false to disable.
    """
    try:
        job = ctx.store.components.get(UUID(component_id), kind="job", org_id=ctx.org_id)
        updated = ctx.store.components.update(job.id, config={**(job.config or {}), "enabled": enabled})
        return ComponentToggled(
            message=f"Job '{job.name}' {'enabled' if enabled else 'disabled'}",
            component=ComponentRef(id=updated.id, kind=updated.kind, key=updated.key, name=updated.name),
            enabled=enabled,
        )
    except Exception as e:
        return ToolError(error=str(e))


@requires_role("editor")
def toggle_asset(ctx: ToolkitContext, asset_id: str, enabled: bool) -> ComponentToggled | ToolError:
    """Enable or disable materialization for an asset.

    Args:
        asset_id: UUID of the asset.
        enabled: True to enable materialization, false to disable.
    """
    try:
        asset = ctx.store.components.get(UUID(asset_id), kind="asset", org_id=ctx.org_id)
        updated = ctx.store.components.update(asset.id, config={**(asset.config or {}), "enabled": enabled})
        return ComponentToggled(
            message=f"Asset '{updated.key}' materialization {'enabled' if enabled else 'disabled'}",
            component=ComponentRef(id=updated.id, kind=updated.kind, key=updated.key, name=updated.name),
            enabled=enabled,
        )
    except Exception as e:
        return ToolError(error=str(e))


# --- Runs ---


def list_recent_runs(
    ctx: ToolkitContext,
    component_id: str | None = None,
    status: RunStatus | None = None,
    limit: int = 20,
    offset: int = 0,
) -> RunList | ToolError:
    """List recent runs, newest first, with optional filters.

    Args:
        component_id: Filter by job UUID (optional).
        status: Filter by status (optional).
        limit: Maximum number of runs to return (default 20).
        offset: Number of runs to skip, for paging past the first page.

    Returns the page of runs and the total number matching the filters.
    """
    try:
        jid = UUID(component_id) if component_id else None
        query = RunQuery(component_id=jid, status=[status] if status else None, limit=limit, offset=offset)
        runs = ctx.store.runs.list(ctx.org_id, query)
        return RunList(count=len(runs.items), total=runs.total, runs=runs.items)
    except Exception as e:
        return ToolError(error=str(e))


@requires_role("editor")
def trigger_run(ctx: ToolkitContext, component_id: str, partition_key: str | None = None) -> RunQueued | ToolError:
    """Queue a single run for a job.

    Args:
        component_id: UUID of the job to run.
        partition_key: Optional partition key. The shape carries the
            granularity: 2026-04-09 (day), 2026-04 (month), 2026 (year),
            2026-04-09T13 (hour).
    """
    try:
        target = ctx.store.components.get(UUID(component_id), org_id=ctx.org_id)
        run = ctx.store.runs.create(ctx.org_id, component_id=target.id, partition_key=partition_key)
        return RunQueued(message="Run queued successfully", run=run)
    except Exception as e:
        return ToolError(error=str(e))


@requires_role("editor")
def retry_run(ctx: ToolkitContext, run_id: str, scope: str = "all") -> RunRetried | ToolError:
    """Queue a retry of a failed run as a new attempt of the same stack.

    Args:
        run_id: UUID of the failed run.
        scope: 'all' re-runs the whole DAG (default); 'failed' re-runs only
            the operations that failed or were canceled.

    Returns the queued attempt; its ``attempt`` number and ``root_run_id``
    tie it to the run it retries. The retry continues from the stack's latest
    attempt, so retrying an earlier attempt retries the stack. A run that is
    not failed, or a stack whose latest attempt is pending or succeeded,
    cannot be retried.
    """
    try:
        ctx.store.runs.get(UUID(run_id), org_id=ctx.org_id)
        run = ctx.store.runs.retry(UUID(run_id), scope=scope)
        return RunRetried(message=f"Retry queued as attempt {run.attempt}", run=run)
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
        executions = ctx.store.executions.list(ctx.org_id, ExecutionQuery(limit=None), run_id=rid).items

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
        query = EventQuery(
            component_id=[UUID(component_id)] if component_id else None,
            event_type=event_types,
            has_error=errors_only,
            limit=limit,
            offset=offset,
        )
        events = ctx.store.events.list(ctx.org_id, query, run_id=rid)
        records = [EventRecord(**e.model_dump(exclude={"org_id", "traceback"})) for e in events.items]
        return EventList(run_id=run_id, count=len(records), total=events.total, events=records)
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
        failed_runs = ctx.store.runs.failures(ctx.org_id, PageQuery(limit=limit, offset=offset))

        query = EventQuery(event_type=list(FAILURE_EVENT_TYPES), has_error=True, limit=_ERRORS_PER_FAILURE)
        results = []
        for run in failed_runs.items:
            events = ctx.store.events.list(ctx.org_id, query, run_id=run.id)
            errors = [
                RunErrorEvent(
                    event_id=e.id,
                    component_key=e.component_key,
                    error=clip(e.error, _ERROR_TEXT_LIMIT) or "",
                    timestamp=e.timestamp,
                )
                for e in events.items
            ]
            results.append(RunFailure(run=run, error_count=events.total, errors=errors))

        return FailureList(count=len(results), total=failed_runs.total, failures=results)
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
        keys = tuple(group_by or GROUP_KEYS)
        scoped = run_id is not None or backfill_id is not None
        start, end = window(since, until, default_days=None if scoped else 1)
        errors = ctx.store.insights.error_groups(
            ctx.org_id,
            since=start,
            until=end,
            job_id=UUID(component_id) if component_id else None,
            backfill_id=UUID(backfill_id) if backfill_id else None,
            run_id=UUID(run_id) if run_id else None,
            group_by=keys,
        )
        jobs = ctx.store.components.list(ctx.org_id, ComponentQuery(kind=["job"], roots_only=False, limit=None))
        job_names = {job.id: job.name for job in jobs.items}
        page = [
            ErrorGroupRow(
                job_id=group.job_id,
                job_name=job_names.get(group.job_id) if group.job_id else None,
                asset_key=group.asset_key,
                cause=group.cause,
                failed_attempts=group.failed_attempts,
                terminal_failures=group.terminal_failures,
                runs_affected=len(group.runs),
                first_seen=group.first_seen,
                last_seen=group.last_seen,
                sample_run_id=group.sample_run_id,
                sample=clip(group.sample, _SAMPLE_LIMIT) or "",
            )
            for group in errors.groups[offset : offset + limit]
        ]
        return ErrorBreakdown(
            since=start,
            until=end,
            group_by=list(keys),
            count=len(page),
            total=len(errors.groups),
            scan=Scan(rows=errors.rows, truncated=errors.truncated),
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
        query = BackfillQuery(status=[*ACTIVE_BACKFILL_STATUSES] if active_only else None, limit=limit, offset=offset)
        backfills = ctx.store.backfills.list(ctx.org_id, query)
        return BackfillList(count=len(backfills.items), total=backfills.total, backfills=backfills.items)
    except Exception as e:
        return ToolError(error=str(e))


@requires_role("editor")
def trigger_backfill(
    ctx: ToolkitContext,
    component_id: str,
    start_key: str,
    end_key: str,
    concurrency: int = 1,
    fail_fast: bool = False,
) -> BackfillQueued | ToolError:
    """Start a backfill for a job over a partition range.

    Args:
        component_id: UUID of the job.
        start_key: First partition's key (e.g. 2026-04-09, or 2026-04 for a
            monthly job).
        end_key: Last partition's key, inclusive. Must share the start key's
            granularity.
        concurrency: Max number of runs in-flight at once (default 1).
        fail_fast: If true, cancel remaining runs on first failure (default false).
    """
    try:
        target = ctx.store.components.get(UUID(component_id), org_id=ctx.org_id)
        backfill = ctx.store.backfills.create(
            ctx.org_id,
            component_id=target.id,
            start_key=start_key,
            end_key=end_key,
            concurrency=concurrency,
            fail_fast=fail_fast,
        )
        return BackfillQueued(message="Backfill created successfully", backfill=backfill)
    except Exception as e:
        return ToolError(error=str(e))


@requires_role("editor")
def cancel_backfill(ctx: ToolkitContext, backfill_id: str) -> BackfillCanceled | ToolError:
    """Cancel a backfill: its runs not yet dispatched will never execute.

    Args:
        backfill_id: UUID of the backfill, from list_backfills.

    Returns the backfill in its terminal state and how many runs were
    canceled. Runs already dispatched or running drain to their own verdict.
    A backfill that already finished cannot be canceled.
    """
    try:
        bid = UUID(backfill_id)
        ctx.store.backfills.get(bid, org_id=ctx.org_id)
        backfill = ctx.store.backfills.cancel(bid)
        canceled = ctx.store.backfills.run_counts([bid]).get(bid, {}).get(RunStatus.CANCELED, 0)
        return BackfillCanceled(
            message=f"Backfill canceled, {canceled} run(s) will not execute", backfill=backfill, runs_canceled=canceled
        )
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
        backfill = ctx.store.backfills.get(bid, org_id=ctx.org_id)
        runs = ctx.store.backfills.attempts(bid)
        total = len(runs)

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
