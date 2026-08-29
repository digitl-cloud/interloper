"""Scheduling tools — jobs, runs, and backfills: monitoring and control.

Read-only monitoring is implemented in ``interloper_toolkit.scheduling``
(shared with the MCP server; docstrings adopted below); the mutating
operations — toggling, triggering — stay here, agent-only.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from google.adk.tools.tool_context import ToolContext
from interloper_toolkit import scheduling as toolkit_scheduling

from interloper_agent.context import get_org_id, get_store, serialize, toolkit_ctx

# --- Jobs ---


def list_jobs(tool_context: ToolContext) -> dict[str, Any]:
    return toolkit_scheduling.list_jobs(toolkit_ctx(tool_context)).model_dump(mode="json")


def get_job_health(component_id: str, tool_context: ToolContext) -> dict[str, Any]:
    return toolkit_scheduling.get_job_health(toolkit_ctx(tool_context), component_id).model_dump(mode="json")


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
        job = store.components.get(jid, kind="job")
        updated = store.components.update(jid, config={**(job.config or {}), "enabled": enabled})
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
    result = toolkit_scheduling.list_recent_runs(toolkit_ctx(tool_context), component_id, status, limit)
    return result.model_dump(mode="json")


def get_run_detail(run_id: str, tool_context: ToolContext) -> dict[str, Any]:
    return toolkit_scheduling.get_run_detail(toolkit_ctx(tool_context), run_id).model_dump(mode="json")


def list_failures(limit: int = 20, tool_context: ToolContext | None = None) -> dict[str, Any]:
    return toolkit_scheduling.list_failures(toolkit_ctx(tool_context), limit).model_dump(mode="json")


def trigger_run(
    component_id: str,
    partition_key: str | None = None,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """Queue a single run for a job.

    Args:
        component_id: UUID of the job to run.
        partition_key: Optional partition key. The shape carries the
            granularity: 2026-04-09 (day), 2026-04 (month), 2026 (year),
            2026-04-09T13 (hour).
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        run = store.runs.create(org_id, component_id=UUID(component_id), partition_key=partition_key)
        return {
            "status": "success",
            "message": "Run queued successfully",
            "run": serialize(run),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


# --- Backfills ---


def list_backfills(active_only: bool = True, tool_context: ToolContext | None = None) -> dict[str, Any]:
    return toolkit_scheduling.list_backfills(toolkit_ctx(tool_context), active_only).model_dump(mode="json")


def trigger_backfill(
    component_id: str,
    start_key: str,
    end_key: str,
    concurrency: int = 1,
    fail_fast: bool = False,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
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
        org_id = get_org_id(tool_context)
        store = get_store()
        backfill = store.runs.create_backfill(
            org_id,
            component_id=UUID(component_id),
            start_key=start_key,
            end_key=end_key,
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
        asset = store.components.get(aid, kind="asset")
        updated = store.components.update(aid, config={**(asset.config or {}), "materializable": materializable})
        action = "enabled" if materializable else "disabled"
        return {
            "status": "success",
            "message": f"Asset '{updated.key}' materialization {action}",
            "asset": serialize(updated),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


list_jobs.__doc__ = toolkit_scheduling.list_jobs.__doc__
get_job_health.__doc__ = toolkit_scheduling.get_job_health.__doc__
list_recent_runs.__doc__ = toolkit_scheduling.list_recent_runs.__doc__
get_run_detail.__doc__ = toolkit_scheduling.get_run_detail.__doc__
list_failures.__doc__ = toolkit_scheduling.list_failures.__doc__
list_backfills.__doc__ = toolkit_scheduling.list_backfills.__doc__
