"""Runs API: read endpoints for runs and their events."""

from __future__ import annotations

import datetime as dt
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from interloper.errors import NotFoundError
from interloper_db import Profile, Store
from interloper_db.models import Event, Run
from pydantic import BaseModel

from interloper_api.dependencies import (
    authorize_org_member,
    get_current_user,
    get_org_id,
    get_store,
    load_authorized,
    require_viewer,
)

router = APIRouter(prefix="/runs", tags=["runs"])

#: Hard cap on the number of events returned in a single page, regardless of
#: the requested ``limit``. Keeps a pathological ``?limit=1000000`` from loading
#: an entire run's history into memory at once.
MAX_EVENTS_PAGE_SIZE = 1000


# -- Request / response models -------------------------------------------------


class RunResponse(BaseModel):
    """Response body for a run."""

    id: UUID
    org_id: UUID
    component_id: UUID | None
    backfill_id: UUID | None
    partition_key: str | None
    status: str
    retry_of: UUID | None = None
    attempt: int = 1
    retry_scope: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    created_at: str | None = None

    @classmethod
    def from_run(cls, run: Run) -> RunResponse:
        """Convert a DB Run to a RunResponse.

        Args:
            run: The DB Run row.

        Returns:
            The response model.
        """
        return cls(
            id=run.id,
            org_id=run.org_id,
            component_id=run.component_id,
            backfill_id=run.backfill_id,
            partition_key=run.partition_key,
            status=run.status,
            retry_of=run.retry_of,
            attempt=run.attempt,
            retry_scope=run.retry_scope,
            started_at=str(run.started_at) if run.started_at else None,
            completed_at=str(run.completed_at) if run.completed_at else None,
            created_at=str(run.created_at) if run.created_at else None,
        )


class RunCreateRequest(BaseModel):
    """Request body for queuing a run targeting a runnable component."""

    component_id: UUID
    partition_key: str | None = None


class RetryRequest(BaseModel):
    """Request body for retrying a failed run."""

    scope: Literal["all", "failed"] = "all"


class ExecutionResponse(BaseModel):
    """Response body for an operation execution (from the ``executions`` view)."""

    run_id: UUID
    org_id: UUID
    component_id: UUID | None = None
    component_key: str
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
    component_id: UUID | None = None
    component_kind: str | None
    component_key: str | None
    error: str | None
    traceback: str | None
    message: str | None
    level: str | None
    data: dict[str, object] | None
    timestamp: str

    @classmethod
    def from_event(cls, event: Event) -> EventResponse:
        """Convert a DB Event to an EventResponse.

        Args:
            event: The DB Event row.

        Returns:
            The response model.
        """
        return cls(
            id=event.id,
            org_id=event.org_id,
            run_id=event.run_id,
            event_type=event.event_type,
            component_id=event.component_id,
            component_kind=event.component_kind,
            component_key=event.component_key,
            error=event.error,
            traceback=event.traceback,
            message=event.message,
            level=event.level,
            data=event.data,
            timestamp=str(event.timestamp),
        )


# -- Helpers -------------------------------------------------------------------


def _load_authorized_run(run_id: UUID, user: Profile, store: Store, *, minimum: str = "viewer") -> Run:
    """Load a run and authorize the user by membership in its org.

    Args:
        run_id: The run UUID.
        user: The authenticated user.
        store: The Store instance.
        minimum: Minimum role required in the run's organisation.

    Returns:
        The Run row.

    Raises:
        HTTPException: 404 if missing or the user is not a member of the
            owning org, 403 if the role is insufficient.
    """
    try:
        run = store.runs.get(run_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    authorize_org_member(user, run.org_id, store, minimum=minimum, detail=f"Run {run_id} not found")
    return run


# -- Endpoints -----------------------------------------------------------------


@router.get("/")
def list_runs(
    response: Response,
    component_id: UUID | None = None,
    backfill_id: UUID | None = None,
    status: str | None = None,
    after: dt.datetime | None = None,
    before: dt.datetime | None = None,
    limit: int = 50,
    offset: int = 0,
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[RunResponse]:
    """List runs with optional filters.

    ``after``/``before`` bound the runs to those whose execution overlaps that
    window — a run occupies ``started_at`` → ``completed_at`` (open-ended while
    running), so runs that never started match no window. This is what a
    timeline view over a time range asks for.

    The total number of matching runs (ignoring ``limit``/``offset``) is
    returned in the ``X-Total-Count`` response header so clients can paginate.

    Args:
        response: The outgoing response, used to set ``X-Total-Count``.
        component_id: Keep only runs of this component; None applies no filter.
        backfill_id: Keep only runs belonging to this backfill; None applies no filter.
        status: Keep only runs in this status; None applies no filter.
        after: Start of the overlap window; None leaves it open-ended.
        before: End of the overlap window; None leaves it open-ended.
        limit: Maximum number of runs on the page.
        offset: Number of matching runs to skip before the page starts.
        user: The authenticated user, required to hold at least the ``viewer`` role.
        org_id: The active organisation's UUID.
        store: The Store instance.

    Returns:
        The matching page of runs, as response models.
    """
    total = store.runs.count(
        org_id, component_id=component_id, backfill_id=backfill_id, status=status, after=after, before=before
    )
    response.headers["X-Total-Count"] = str(total)
    runs = store.runs.list_all(
        org_id,
        component_id=component_id,
        backfill_id=backfill_id,
        status=status,
        after=after,
        before=before,
        limit=limit,
        offset=offset,
    )
    return [RunResponse.from_run(r) for r in runs]


@router.post("/", status_code=201)
def create_run(
    body: RunCreateRequest,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> RunResponse:
    """Queue a single run targeting a component whose kind declares an operation.

    Args:
        body: The component to run and the partition key to run it for.
        user: The authenticated user.
        store: The Store instance.

    Returns:
        The queued run, as a response model.

    Raises:
        HTTPException: 400 if the store rejects the run (a kind with no
            operation, or an invalid partition key).
    """
    target = load_authorized(store.components.get, body.component_id, user, store, label="Component", minimum="editor")
    try:
        run = store.runs.create(target.org_id, component_id=body.component_id, partition_key=body.partition_key)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return RunResponse.from_run(run)


@router.get("/{run_id}")
def get_run(
    run_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> RunResponse:
    """Get a single run by ID. Authorized by membership in the run's org.

    Args:
        run_id: The run UUID.
        user: The authenticated user.
        store: The Store instance.

    Returns:
        The run, as a response model.
    """
    run = _load_authorized_run(run_id, user, store)
    return RunResponse.from_run(run)


@router.get("/{run_id}/executions")
def list_executions(
    run_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> list[ExecutionResponse]:
    """List operation executions for a run.

    Args:
        run_id: The run UUID.
        user: The authenticated user.
        store: The Store instance.

    Returns:
        The run's operation executions, as response models.
    """
    _load_authorized_run(run_id, user, store)
    rows = store.events.list_executions(run_id)
    return [
        ExecutionResponse(
            run_id=row.run_id,
            org_id=row.org_id,
            component_id=row.component_id,
            component_key=row.component_key or "",
            status=row.status,
            started_at=str(row.started_at) if row.started_at else None,
            completed_at=str(row.completed_at) if row.completed_at else None,
            created_at=str(row.created_at) if row.created_at else None,
        )
        for row in rows
    ]


@router.post("/{run_id}/retry")
def retry_run(
    run_id: UUID,
    body: RetryRequest | None = None,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Queue a retry of a failed run.

    Creates a new run linked to the original via ``retry_of``. With
    ``scope="all"`` the whole DAG re-runs; with ``scope="failed"`` only the
    previously failed/cancelled assets re-run.

    Args:
        run_id: The UUID of the run to retry.
        body: The retry scope; None retries the whole DAG, as ``scope="all"`` does.
        user: The authenticated user.
        store: The Store instance.

    Returns:
        The queued status and the new run's ID.

    Raises:
        HTTPException: 404 if the run no longer exists, 409 if it is not in a
            retryable state.
    """
    _load_authorized_run(run_id, user, store, minimum="editor")
    scope = body.scope if body else "all"
    try:
        run = store.runs.retry(run_id, scope=scope)
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
    component_id: Annotated[list[UUID] | None, Query()] = None,
    event_type: Annotated[list[str] | None, Query()] = None,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> list[EventResponse]:
    """List events for a run, oldest first.

    Events are ordered ``timestamp ASC`` and paged with ``limit``/``offset``.
    ``component_id`` and ``event_type`` may each be repeated to narrow the
    listing to one or more components (e.g. every asset sharing a status)
    and/or event types (e.g. a "Lifecycle"/"Errors"/"Logs" tab); the two
    compose. The total number of matching events (ignoring
    ``limit``/``offset``, honouring the filters) is returned in the
    ``X-Total-Count`` response header so clients can page through every event
    — including the terminal/outcome events (``asset_completed``,
    ``asset_failed``, ``run_failed``, …) that sort last.

    Args:
        run_id: The run UUID.
        response: The outgoing response, used to set ``X-Total-Count``.
        limit: Maximum number of events on the page, clamped to at least 1 and
            at most ``MAX_EVENTS_PAGE_SIZE``.
        offset: Number of matching events to skip, clamped to at least 0.
        component_id: Keep only events emitted by these components; None applies
            no filter.
        event_type: Keep only events of these types; None applies no filter.
        user: The authenticated user.
        store: The Store instance.

    Returns:
        The matching page of events, oldest first, as response models.
    """
    _load_authorized_run(run_id, user, store)
    limit = max(1, min(limit, MAX_EVENTS_PAGE_SIZE))
    offset = max(0, offset)
    total = store.events.count(run_id=run_id, component_ids=component_id, event_types=event_type)
    response.headers["X-Total-Count"] = str(total)
    events = store.events.list_all(
        run_id=run_id, component_ids=component_id, event_types=event_type, limit=limit, offset=offset
    )
    return [EventResponse.from_event(e) for e in events]
