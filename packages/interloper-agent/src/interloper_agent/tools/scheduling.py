"""Scheduling tools — jobs, runs, and backfills: monitoring and control."""

from __future__ import annotations

import datetime
from typing import Any
from uuid import UUID

from google.adk.tools.tool_context import ToolContext

from interloper_agent.context import get_org_id, get_store, serialize

# --- Jobs ---


def list_jobs(tool_context: ToolContext) -> dict[str, Any]:
    """List all scheduled jobs in the organisation.

    Returns each job with its name, cron expression, enabled status,
    last_run_at, and next_run_at.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        jobs = store.list_components(org_id, kinds=["job"])
        return {"status": "success", "count": len(jobs), "jobs": [serialize(j) for j in jobs]}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def get_job_health(component_id: str, tool_context: ToolContext) -> dict[str, Any]:
    """Get health summary for a job: metadata, recent success/failure rate.

    Args:
        component_id: UUID of the job to inspect.

    Returns job metadata plus success rate computed from the last 20 runs.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        jid = UUID(component_id)
        job = store.get_component(jid, kind="job")
        runs = store.list_runs(org_id, component_id=jid, limit=20)

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


def toggle_job(
    component_id: str,
    enabled: bool,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """Enable or disable a scheduled job.

    Args:
        component_id: UUID of the job.
        enabled: True to enable, false to disable.
    """
    try:
        store = get_store()
        jid = UUID(component_id)
        job = store.get_component(jid, kind="job")
        updated = store.update_component(jid, config={**(job.config or {}), "enabled": enabled})
        action = "enabled" if enabled else "disabled"
        return {
            "status": "success",
            "message": f"Job '{job.name}' {action}",
            "job": serialize(updated),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


# --- Runs ---


def list_recent_runs(
    component_id: str | None = None,
    status: str | None = None,
    limit: int = 20,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """List recent runs with optional filters.

    Args:
        component_id: Filter by job UUID (optional).
        status: Filter by status: 'queued', 'running', 'success', 'failed', 'canceled' (optional).
        limit: Maximum number of runs to return (default 20).
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        runs = store.list_runs(
            org_id,
            component_id=UUID(component_id) if component_id else None,
            status=status,
            limit=limit,
        )
        return {"status": "success", "count": len(runs), "runs": [serialize(r) for r in runs]}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def get_run_detail(run_id: str, tool_context: ToolContext) -> dict[str, Any]:
    """Get full detail for a single run including events and per-asset execution status.

    Args:
        run_id: UUID of the run.

    Returns the run metadata, event timeline, and per-asset execution summary.
    """
    try:
        store = get_store()
        rid = UUID(run_id)
        run = store.get_run(rid)
        events = store.list_events(run_id=rid)
        asset_execs = store.list_asset_executions(rid)

        return {
            "status": "success",
            "run": serialize(run),
            "events": [serialize(e) for e in events],
            "asset_executions": asset_execs,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def list_failures(limit: int = 20, tool_context: ToolContext | None = None) -> dict[str, Any]:
    """List recent failed runs with their error events.

    Args:
        limit: Maximum number of failed runs to return (default 20).

    Returns failed runs along with the error messages from their events.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        failed_runs = store.list_runs(org_id, status="failed", limit=limit)

        results = []
        for run in failed_runs:
            run_id = run.id
            events = store.list_events(run_id=run_id)
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


def trigger_run(
    component_id: str,
    partition_date: str | None = None,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """Queue a single run for a job.

    Args:
        component_id: UUID of the job to run.
        partition_date: Optional partition date in ISO format (YYYY-MM-DD).
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        pd = datetime.date.fromisoformat(partition_date) if partition_date else None
        run = store.create_run(org_id, component_id=UUID(component_id), partition_date=pd)
        return {
            "status": "success",
            "message": "Run queued successfully",
            "run": serialize(run),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


# --- Backfills ---


def list_backfills(active_only: bool = True, tool_context: ToolContext | None = None) -> dict[str, Any]:
    """List backfills, optionally filtered to active ones only.

    Args:
        active_only: If true, only return running/queued backfills (default true).

    Returns backfills with their status, date range, and partition progress.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        if active_only:
            backfills = store.list_active_backfills(org_id)
        else:
            backfills = store.list_backfills(org_id)
        return {"status": "success", "count": len(backfills), "backfills": [serialize(b) for b in backfills]}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def trigger_backfill(
    component_id: str,
    start_date: str,
    end_date: str,
    concurrency: int = 1,
    fail_fast: bool = False,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """Start a backfill for a job over a date range.

    Args:
        component_id: UUID of the job.
        start_date: Start date in ISO format (YYYY-MM-DD).
        end_date: End date in ISO format (YYYY-MM-DD), inclusive.
        concurrency: Max number of runs in-flight at once (default 1).
        fail_fast: If true, cancel remaining runs on first failure (default false).
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        backfill = store.create_backfill(
            org_id,
            component_id=UUID(component_id),
            start_date=datetime.date.fromisoformat(start_date),
            end_date=datetime.date.fromisoformat(end_date),
            concurrency=concurrency,
            fail_fast=fail_fast,
        )
        return {
            "status": "success",
            "message": "Backfill created successfully",
            "backfill": serialize(backfill),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


# --- Assets ---


def toggle_asset(
    asset_id: str,
    materializable: bool,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """Enable or disable materialization for an asset.

    Args:
        asset_id: UUID of the asset.
        materializable: True to enable materialization, false to disable.
    """
    try:
        store = get_store()
        aid = UUID(asset_id)
        asset = store.get_component(aid, kind="asset")
        updated = store.update_component(aid, config={**(asset.config or {}), "materializable": materializable})
        action = "enabled" if materializable else "disabled"
        return {
            "status": "success",
            "message": f"Asset '{updated.key}' materialization {action}",
            "asset": serialize(updated),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
