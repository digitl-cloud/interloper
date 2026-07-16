"""Scheduling tools — jobs, runs, and backfills: read-only monitoring.

Mutating operations (toggling jobs, triggering runs and backfills) live with
the agent — this module is shared with surfaces that must stay read-only.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from interloper_db.toolkit.context import ToolkitContext, serialize

# --- Jobs ---


def list_jobs(ctx: ToolkitContext) -> dict[str, Any]:
    """List all scheduled jobs in the organisation.

    Returns each job with its name, cron expression, enabled status,
    last_run_at, and next_run_at.
    """
    try:
        jobs = ctx.store.list_components(ctx.org_id, kinds=["job"])
        return {"status": "success", "count": len(jobs), "jobs": [serialize(j) for j in jobs]}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def get_job_health(ctx: ToolkitContext, component_id: str) -> dict[str, Any]:
    """Get health summary for a job: metadata, recent success/failure rate.

    Args:
        component_id: UUID of the job to inspect.

    Returns job metadata plus success rate computed from the last 20 runs.
    """
    try:
        jid = UUID(component_id)
        job = ctx.store.get_component(jid, kind="job")
        runs = ctx.store.list_runs(ctx.org_id, component_id=jid, limit=20)

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

        return {
            "status": "success",
            "job": serialize(job),
            "health": {
                "total_recent_runs": total,
                "success_count": success,
                "failed_count": failed,
                "success_rate": round(success / total, 2) if total > 0 else None,
                "avg_duration_seconds": round(avg_duration_seconds, 1) if avg_duration_seconds else None,
            },
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


# --- Runs ---


def list_recent_runs(
    ctx: ToolkitContext,
    component_id: str | None = None,
    status: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """List recent runs with optional filters.

    Args:
        component_id: Filter by job UUID (optional).
        status: Filter by status: 'queued', 'running', 'success', 'failed', 'canceled' (optional).
        limit: Maximum number of runs to return (default 20).
    """
    try:
        runs = ctx.store.list_runs(
            ctx.org_id,
            component_id=UUID(component_id) if component_id else None,
            status=status,
            limit=limit,
        )
        return {"status": "success", "count": len(runs), "runs": [serialize(r) for r in runs]}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def get_run_detail(ctx: ToolkitContext, run_id: str) -> dict[str, Any]:
    """Get full detail for a single run including events and per-asset execution status.

    Args:
        run_id: UUID of the run.

    Returns the run metadata, event timeline, and per-asset execution summary.
    """
    try:
        rid = UUID(run_id)
        run = ctx.store.get_run(rid)
        events = ctx.store.list_events(run_id=rid)
        asset_execs = ctx.store.list_asset_executions(rid)

        return {
            "status": "success",
            "run": serialize(run),
            "events": [serialize(e) for e in events],
            "asset_executions": asset_execs,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def list_failures(ctx: ToolkitContext, limit: int = 20) -> dict[str, Any]:
    """List recent failed runs with their error events.

    Args:
        limit: Maximum number of failed runs to return (default 20).

    Returns failed runs along with the error messages from their events.
    """
    try:
        failed_runs = ctx.store.list_runs(ctx.org_id, status="failed", limit=limit)

        results = []
        for run in failed_runs:
            run_id = run.id
            events = ctx.store.list_events(run_id=run_id)
            errors = [
                {"asset_key": e.asset_key, "error": e.error, "timestamp": serialize(e.timestamp)}
                for e in events
                if e.error
            ]
            results.append({
                "run": serialize(run),
                "errors": errors,
            })

        return {"status": "success", "count": len(results), "failures": results}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# --- Backfills ---


def list_backfills(ctx: ToolkitContext, active_only: bool = True) -> dict[str, Any]:
    """List backfills, optionally filtered to active ones only.

    Args:
        active_only: If true, only return running/queued backfills (default true).

    Returns backfills with their status, date range, and partition progress.
    """
    try:
        if active_only:
            backfills = ctx.store.list_active_backfills(ctx.org_id)
        else:
            backfills = ctx.store.list_backfills(ctx.org_id)
        return {"status": "success", "count": len(backfills), "backfills": [serialize(b) for b in backfills]}
    except Exception as e:
        return {"status": "error", "error": str(e)}
