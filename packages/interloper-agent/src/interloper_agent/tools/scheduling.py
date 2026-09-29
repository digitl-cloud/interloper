"""Scheduling tools — thin ADK wrappers over the shared toolkit.

The implementations (and the LLM-facing docstrings, adopted below) live in
``interloper_toolkit.scheduling`` so the MCP server exposes the same logic.
"""

from __future__ import annotations

from typing import Any

from google.adk.tools.tool_context import ToolContext
from interloper_toolkit import scheduling as toolkit_scheduling

from interloper_agent.context import toolkit_ctx

# --- Jobs ---


def list_jobs(limit: int = 50, offset: int = 0, tool_context: ToolContext | None = None) -> dict[str, Any]:
    return toolkit_scheduling.list_jobs(toolkit_ctx(tool_context), limit, offset).model_dump(mode="json")


def get_job_health(component_id: str, tool_context: ToolContext) -> dict[str, Any]:
    return toolkit_scheduling.get_job_health(toolkit_ctx(tool_context), component_id).model_dump(mode="json")


def toggle_job(component_id: str, enabled: bool, tool_context: ToolContext | None = None) -> dict[str, Any]:
    return toolkit_scheduling.toggle_job(toolkit_ctx(tool_context), component_id, enabled).model_dump(mode="json")


# --- Runs ---


def list_recent_runs(
    component_id: str | None = None,
    status: str | None = None,
    limit: int = 20,
    offset: int = 0,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    result = toolkit_scheduling.list_recent_runs(toolkit_ctx(tool_context), component_id, status, limit, offset)
    return result.model_dump(mode="json")


def get_run_detail(run_id: str, tool_context: ToolContext) -> dict[str, Any]:
    return toolkit_scheduling.get_run_detail(toolkit_ctx(tool_context), run_id).model_dump(mode="json")


def list_run_events(
    run_id: str,
    component_id: str | None = None,
    event_types: list[str] | None = None,
    errors_only: bool = False,
    limit: int = 50,
    offset: int = 0,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    result = toolkit_scheduling.list_run_events(
        toolkit_ctx(tool_context), run_id, component_id, event_types, errors_only, limit, offset
    )
    return result.model_dump(mode="json")


def get_event(event_id: str, tool_context: ToolContext) -> dict[str, Any]:
    return toolkit_scheduling.get_event(toolkit_ctx(tool_context), event_id).model_dump(mode="json")


def list_failures(limit: int = 20, offset: int = 0, tool_context: ToolContext | None = None) -> dict[str, Any]:
    return toolkit_scheduling.list_failures(toolkit_ctx(tool_context), limit, offset).model_dump(mode="json")


def error_breakdown(
    since: str | None = None,
    until: str | None = None,
    component_id: str | None = None,
    backfill_id: str | None = None,
    run_id: str | None = None,
    group_by: list[str] | None = None,
    limit: int = 25,
    offset: int = 0,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    result = toolkit_scheduling.error_breakdown(
        toolkit_ctx(tool_context), since, until, component_id, backfill_id, run_id, group_by, limit, offset
    )
    return result.model_dump(mode="json")


def retry_run(run_id: str, scope: str = "all", tool_context: ToolContext | None = None) -> dict[str, Any]:
    return toolkit_scheduling.retry_run(toolkit_ctx(tool_context), run_id, scope).model_dump(mode="json")


def trigger_run(
    component_id: str, partition_key: str | None = None, tool_context: ToolContext | None = None
) -> dict[str, Any]:
    result = toolkit_scheduling.trigger_run(toolkit_ctx(tool_context), component_id, partition_key)
    return result.model_dump(mode="json")


# --- Backfills ---


def list_backfills(
    active_only: bool = True, limit: int = 20, offset: int = 0, tool_context: ToolContext | None = None
) -> dict[str, Any]:
    result = toolkit_scheduling.list_backfills(toolkit_ctx(tool_context), active_only, limit, offset)
    return result.model_dump(mode="json")


def backfill_timeline(
    backfill_id: str, limit: int = 50, offset: int = 0, tool_context: ToolContext | None = None
) -> dict[str, Any]:
    result = toolkit_scheduling.backfill_timeline(toolkit_ctx(tool_context), backfill_id, limit, offset)
    return result.model_dump(mode="json")


def cancel_backfill(backfill_id: str, tool_context: ToolContext | None = None) -> dict[str, Any]:
    return toolkit_scheduling.cancel_backfill(toolkit_ctx(tool_context), backfill_id).model_dump(mode="json")


def trigger_backfill(
    component_id: str,
    start_key: str,
    end_key: str,
    concurrency: int = 1,
    fail_fast: bool = False,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    result = toolkit_scheduling.trigger_backfill(
        toolkit_ctx(tool_context), component_id, start_key, end_key, concurrency, fail_fast
    )
    return result.model_dump(mode="json")


# --- Assets ---


def toggle_asset(asset_id: str, enabled: bool, tool_context: ToolContext | None = None) -> dict[str, Any]:
    return toolkit_scheduling.toggle_asset(toolkit_ctx(tool_context), asset_id, enabled).model_dump(mode="json")


for _wrapper in (
    list_jobs,
    get_job_health,
    toggle_job,
    list_recent_runs,
    get_run_detail,
    list_run_events,
    get_event,
    list_failures,
    error_breakdown,
    retry_run,
    trigger_run,
    list_backfills,
    backfill_timeline,
    cancel_backfill,
    trigger_backfill,
    toggle_asset,
):
    _wrapper.__doc__ = getattr(toolkit_scheduling, _wrapper.__name__).__doc__
