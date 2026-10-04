"""Backfills API: queue, inspect, and cancel backfills."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query
from interloper_db import BackfillQuery, Page
from interloper_db.models import Backfill
from pydantic import BaseModel, Field

from interloper_api.dependencies import (
    CurrentUserDep,
    OrgIdDep,
    StoreDep,
    ViewerDep,
    load_authorized,
)

router = APIRouter(prefix="/backfills", tags=["backfills"])


# -- Request / response models -------------------------------------------------


class BackfillCreateRequest(BaseModel):
    """Request body for queuing a backfill."""

    component_id: UUID
    start_key: str
    end_key: str
    concurrency: int = 1
    fail_fast: bool = False


class BackfillResponse(BaseModel):
    """Response body for a backfill.

    The target component's identity is resolved server-side, same contract
    as ``RunResponse``: the ``component_*`` identity fields are ``None``
    exactly when the target was deleted.

    ``run_counts`` is the backfill's partitions per status, each read as its
    stack's latest attempt, so a listing can draw the backfill's progress
    without fetching its runs.
    """

    id: UUID
    org_id: UUID
    component_id: UUID | None
    component_kind: str | None = None
    component_key: str | None = None
    component_name: str | None = None
    status: str
    start_key: str
    end_key: str
    concurrency: int
    fail_fast: bool
    partitions: int
    started_at: datetime | None = None
    completed_at: datetime | None = None
    created_at: datetime | None = None
    run_counts: dict[str, int] = Field(default_factory=dict)

    @classmethod
    def from_backfill(cls, backfill: Backfill, run_counts: dict[str, int] | None = None) -> BackfillResponse:
        """Convert a DB Backfill to a BackfillResponse.

        Args:
            backfill: The DB Backfill row, with its ``target`` relationship loaded.
            run_counts: The backfill's partition count per status; None reads
                as no runs yet.

        Returns:
            The response model.
        """
        return cls(
            id=backfill.id,
            org_id=backfill.org_id,
            component_id=backfill.component_id,
            component_kind=backfill.target.kind if backfill.target else None,
            component_key=backfill.target.key if backfill.target else None,
            component_name=backfill.target.name if backfill.target else None,
            status=backfill.status,
            start_key=backfill.start_key,
            end_key=backfill.end_key,
            concurrency=backfill.concurrency,
            fail_fast=backfill.fail_fast,
            partitions=backfill.partitions,
            started_at=backfill.started_at,
            completed_at=backfill.completed_at,
            created_at=backfill.created_at,
            run_counts=run_counts or {},
        )


# -- Endpoints -----------------------------------------------------------------


@router.get("")
def list_backfills(
    user: ViewerDep,
    org_id: OrgIdDep,
    store: StoreDep,
    query: Annotated[BackfillQuery, Query()],
) -> Page[BackfillResponse]:
    """List the organisation's backfills, newest first.

    Args:
        user: The authenticated user, required to hold at least the ``viewer`` role.
        org_id: The active organisation's UUID.
        store: The Store instance.
        query: The statuses to keep (``status=queued&status=running`` for the
            active ones) and the window to read.

    Returns:
        The page of backfills, each with its partitions per status.
    """
    backfills = store.backfills.list(org_id, query)
    counts = store.backfills.run_counts([backfill.id for backfill in backfills.items])
    return backfills.map(lambda backfill: BackfillResponse.from_backfill(backfill, counts.get(backfill.id)))


@router.post("", status_code=201)
def create_backfill(
    body: BackfillCreateRequest,
    user: CurrentUserDep,
    store: StoreDep,
) -> BackfillResponse:
    """Queue a backfill for a job over a partition-key range.

    Args:
        body: The target job, the inclusive partition-key range, and the
            concurrency and fail-fast settings.
        user: The authenticated user.
        store: The Store instance.

    Returns:
        The queued backfill, as a response model.
    """
    job = load_authorized(
        lambda i: store.components.get(i, kind="job"), body.component_id, user, store, label="Job", minimum="editor"
    )
    backfill = store.backfills.create(
        job.org_id,
        component_id=body.component_id,
        start_key=body.start_key,
        end_key=body.end_key,
        concurrency=body.concurrency,
        fail_fast=body.fail_fast,
    )
    return BackfillResponse.from_backfill(backfill, store.backfills.run_counts([backfill.id]).get(backfill.id))


@router.get("/{backfill_id}")
def get_backfill(
    backfill_id: UUID,
    user: CurrentUserDep,
    store: StoreDep,
) -> BackfillResponse:
    """Get a single backfill by ID. Authorized by membership in the backfill's org.

    Args:
        backfill_id: The backfill UUID.
        user: The authenticated user.
        store: The Store instance.

    Returns:
        The backfill, as a response model.
    """
    backfill = load_authorized(store.backfills.get, backfill_id, user, store, label="Backfill")
    return BackfillResponse.from_backfill(backfill, store.backfills.run_counts([backfill_id]).get(backfill_id))


@router.post("/{backfill_id}/cancel")
def cancel_backfill(
    backfill_id: UUID,
    user: CurrentUserDep,
    store: StoreDep,
) -> BackfillResponse:
    """Cancel a backfill: pending and queued runs are canceled, in-flight runs drain.

    A backfill already terminal answers 409.

    Args:
        backfill_id: The backfill UUID.
        user: The authenticated user.
        store: The Store instance.

    Returns:
        The cancelled backfill, as a response model, with its partitions per status.
    """
    load_authorized(store.backfills.get, backfill_id, user, store, label="Backfill", minimum="editor")
    backfill = store.backfills.cancel(backfill_id)
    return BackfillResponse.from_backfill(backfill, store.backfills.run_counts([backfill_id]).get(backfill_id))
