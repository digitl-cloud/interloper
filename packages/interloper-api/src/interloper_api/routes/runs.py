"""Runs API: read endpoints for runs and their events."""

from __future__ import annotations

import datetime as dt
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response
from interloper.errors import NotFoundError
from interloper_db import Profile, Store
from interloper_db.models import Event, Run
from pydantic import BaseModel

from interloper_api.dependencies import get_org_id, get_store, require_editor, require_viewer

router = APIRouter()

#: Hard cap on the number of events returned in a single page, regardless of
#: the requested ``limit``. Keeps a pathological ``?limit=1000000`` from loading
#: an entire run's history into memory at once.
MAX_EVENTS_PAGE_SIZE = 1000


class RunResponse(BaseModel):
    """Response body for a run."""

    id: UUID
    org_id: UUID
    job_id: UUID | None
    backfill_id: UUID | None
    partition_date: dt.date | None
    status: str
    retry_of: UUID | None = None
    attempt: int = 1
    retry_scope: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    created_at: str | None = None


class RetryRequest(BaseModel):
    """Request body for retrying a failed run."""

    scope: Literal["all", "failed"] = "all"


class AssetExecutionResponse(BaseModel):
    """Response body for an asset execution (from the asset_executions view)."""

    run_id: UUID
    org_id: UUID
    asset_id: UUID | None = None
    asset_key: str
    status: str
    started_at: str | None = None
    completed_at: str | None = None
    created_at: str | None = None


class EventResponse(BaseModel):
    """Response body for an event."""

    id: UUID
    org_id: UUID
    run_id: UUID | None
    event_type: str
    asset_id: UUID | None = None
    asset_key: str | None
    partition_or_window: str | None
    error: str | None
    traceback: str | None
    message: str | None
    timestamp: str


def _run_to_response(run: Run) -> RunResponse:
    """Convert a DB Run to a RunResponse.

    Args:
        run: The DB Run row.

    Returns:
        The response model.
    """
    return RunResponse(
        id=run.id,
        org_id=run.org_id,
        job_id=run.job_id,
        backfill_id=run.backfill_id,
        partition_date=run.partition_date,
        status=run.status,
        retry_of=run.retry_of,
        attempt=run.attempt,
        retry_scope=run.retry_scope,
        started_at=str(run.started_at) if run.started_at else None,
        completed_at=str(run.completed_at) if run.completed_at else None,
        created_at=str(run.created_at) if run.created_at else None,
    )


def _event_to_response(event: Event) -> EventResponse:
    """Convert a DB Event to an EventResponse.

    Args:
        event: The DB Event row.

    Returns:
        The response model.
    """
    return EventResponse(
        id=event.id,
        org_id=event.org_id,
        run_id=event.run_id,
        event_type=event.event_type,
        asset_id=event.asset_id,
        asset_key=event.asset_key,
        partition_or_window=event.partition_or_window,
        error=event.error,
        traceback=event.traceback,
        message=event.message,
        timestamp=str(event.timestamp),
    )


@router.get("/")
def list_runs(
    response: Response,
    job_id: UUID | None = None,
    backfill_id: UUID | None = None,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[RunResponse]:
    """List runs with optional filters.

    The total number of matching runs (ignoring ``limit``/``offset``) is
    returned in the ``X-Total-Count`` response header so clients can paginate.
    """
    total = store.count_runs(org_id, job_id=job_id, backfill_id=backfill_id, status=status)
    response.headers["X-Total-Count"] = str(total)
    runs = store.list_runs(
        org_id,
        job_id=job_id,
        backfill_id=backfill_id,
        status=status,
        limit=limit,
        offset=offset,
    )
    return [_run_to_response(r) for r in runs]


@router.get("/{run_id}")
def get_run(
    run_id: UUID,
    user: Profile = Depends(require_viewer),
    store: Store = Depends(get_store),
) -> RunResponse:
    """Get a single run by ID."""
    try:
        run = store.get_run(run_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return _run_to_response(run)


@router.get("/{run_id}/asset-executions")
def list_asset_executions(
    run_id: UUID,
    user: Profile = Depends(require_viewer),
    store: Store = Depends(get_store),
) -> list[AssetExecutionResponse]:
    """List asset executions for a run."""
    rows = store.list_asset_executions(run_id)
    return [
        AssetExecutionResponse(
            run_id=row["run_id"],
            org_id=row["org_id"],
            asset_id=row.get("asset_id"),
            asset_key=row["asset_key"],
            status=row["status"],
            started_at=str(row["started_at"]) if row.get("started_at") else None,
            completed_at=str(row["completed_at"]) if row.get("completed_at") else None,
            created_at=str(row["created_at"]) if row.get("created_at") else None,
        )
        for row in rows
    ]


@router.post("/{run_id}/retry")
def retry_run(
    run_id: UUID,
    body: RetryRequest | None = None,
    user: Profile = Depends(require_editor),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Queue a retry of a failed run.

    Creates a new run linked to the original via ``retry_of``. With
    ``scope="all"`` the whole DAG re-runs; with ``scope="failed"`` only the
    previously failed/cancelled assets re-run.
    """
    scope = body.scope if body else "all"
    try:
        run = store.retry_run(run_id, scope=scope)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return {"status": "queued", "run_id": str(run.id)}


@router.get("/{run_id}/events")
def list_run_events(
    run_id: UUID,
    response: Response,
    limit: int = 100,
    offset: int = 0,
    asset_id: UUID | None = None,
    user: Profile = Depends(require_viewer),
    store: Store = Depends(get_store),
) -> list[EventResponse]:
    """List events for a run, oldest first.

    Events are ordered ``timestamp ASC`` and paged with ``limit``/``offset``.
    ``asset_id`` narrows the listing to one asset's events. The total number
    of matching events (ignoring ``limit``/``offset``, honouring ``asset_id``)
    is returned in the ``X-Total-Count`` response header so clients can page
    through every event — including the terminal/outcome events
    (``asset_completed``, ``asset_failed``, ``run_failed``, …) that sort last.
    """
    limit = max(1, min(limit, MAX_EVENTS_PAGE_SIZE))
    offset = max(0, offset)
    total = store.count_events(run_id=run_id, asset_id=asset_id)
    response.headers["X-Total-Count"] = str(total)
    events = store.list_events(run_id=run_id, asset_id=asset_id, limit=limit, offset=offset)
    return [_event_to_response(e) for e in events]
