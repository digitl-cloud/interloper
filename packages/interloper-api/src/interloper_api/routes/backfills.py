"""Backfills API: read endpoints for backfill state."""

from __future__ import annotations

import datetime as dt
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from interloper.errors import NotFoundError
from interloper_db import Profile, Store
from interloper_db.models import Backfill
from pydantic import BaseModel

from interloper_api.dependencies import authorize_org_member, get_current_user, get_org_id, get_store, require_viewer

router = APIRouter()


class BackfillResponse(BaseModel):
    """Response body for a backfill."""

    id: UUID
    org_id: UUID
    job_id: UUID | None
    status: str
    start_date: dt.date
    end_date: dt.date
    concurrency: int
    fail_fast: bool
    partitions: int
    started_at: str | None = None
    completed_at: str | None = None
    created_at: str | None = None


def _backfill_to_response(backfill: Backfill) -> BackfillResponse:
    """Convert a DB Backfill to a BackfillResponse.

    Args:
        backfill: The DB Backfill row.

    Returns:
        The response model.
    """
    return BackfillResponse(
        id=backfill.id,
        org_id=backfill.org_id,
        job_id=backfill.job_id,
        status=backfill.status,
        start_date=backfill.start_date,
        end_date=backfill.end_date,
        concurrency=backfill.concurrency,
        fail_fast=backfill.fail_fast,
        partitions=backfill.partitions,
        started_at=str(backfill.started_at) if backfill.started_at else None,
        completed_at=str(backfill.completed_at) if backfill.completed_at else None,
        created_at=str(backfill.created_at) if backfill.created_at else None,
    )


@router.get("/")
def list_backfills(
    active_only: bool = False,
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[BackfillResponse]:
    """List backfills for the current organisation."""
    if active_only:
        backfills = store.list_active_backfills(org_id)
    else:
        backfills = store.list_backfills(org_id)
    return [_backfill_to_response(b) for b in backfills]


@router.get("/{backfill_id}")
def get_backfill(
    backfill_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> BackfillResponse:
    """Get a single backfill by ID. Authorized by membership in the backfill's org."""
    try:
        backfill = store.get_backfill(backfill_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"Backfill {backfill_id} not found")
    authorize_org_member(user, backfill.org_id, store, detail=f"Backfill {backfill_id} not found")
    return _backfill_to_response(backfill)
