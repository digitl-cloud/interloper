"""Action tools — trigger runs, backfills, and toggle state."""

from __future__ import annotations

import datetime
from typing import Any
from uuid import UUID

from google.adk.tools.tool_context import ToolContext

from interloper_agent.context import get_org_id, get_store, serialize


def trigger_run(
    job_id: str,
    partition_date: str | None = None,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """Queue a single run for a job.

    Args:
        job_id: UUID of the job to run.
        partition_date: Optional partition date in ISO format (YYYY-MM-DD).
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        pd = datetime.date.fromisoformat(partition_date) if partition_date else None
        run = store.create_run(org_id, job_id=UUID(job_id), partition_date=pd)
        return {
            "status": "success",
            "message": "Run queued successfully",
            "run": serialize(run),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def trigger_backfill(
    job_id: str,
    start_date: str,
    end_date: str,
    concurrency: int = 1,
    fail_fast: bool = False,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """Start a backfill for a job over a date range.

    Args:
        job_id: UUID of the job.
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
            job_id=UUID(job_id),
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


def toggle_job(
    job_id: str,
    enabled: bool,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """Enable or disable a scheduled job.

    Args:
        job_id: UUID of the job.
        enabled: True to enable, false to disable.
    """
    try:
        store = get_store()
        jid = UUID(job_id)
        job = store.get_job(jid)
        updated = store.update_job(
            jid,
            name=job.name,
            cron=job.cron,
            source_ids=job.source_ids or None,
            asset_ids=job.asset_ids or None,
            tags=job.tags,
            enabled=enabled,
            partitioned=job.partitioned,
            backfill_days=job.backfill_days,
        )
        action = "enabled" if enabled else "disabled"
        return {
            "status": "success",
            "message": f"Job '{job.name}' {action}",
            "job": serialize(updated),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


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
        updated = store.update_asset(UUID(asset_id), materializable=materializable)
        action = "enabled" if materializable else "disabled"
        return {
            "status": "success",
            "message": f"Asset '{updated.key}' materialization {action}",
            "asset": serialize(updated),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
