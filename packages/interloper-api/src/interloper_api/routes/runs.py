"""Runs API: queue, inspect, retry and cancel runs, read their executions and events, take in a run's reports.

Two routers: ``org_router`` is mounted under the organisation's path and serves
its members; ``router`` is mounted at the API root and serves a run's pod, which
authenticates as its run (:func:`~interloper_api.dependencies.require_run`) and
names no organisation.
"""

from __future__ import annotations

import datetime as dt
from typing import Annotated, Any, Literal
from uuid import UUID

import interloper as il
from fastapi import APIRouter, Depends, HTTPException, Query
from interloper.errors import EventError
from interloper_db import EventQuery, ExecutionQuery, Page, PageQuery, RunQuery
from interloper_db.models import Event, Run
from pydantic import BaseModel, Field

from interloper_api.dependencies import (
    OrgDep,
    StoreDep,
    ViewerDep,
    load_authorized,
    require_run,
)
from interloper_api.routes.executions import ExecutionResponse

org_router = APIRouter(prefix="/runs", tags=["runs"])
router = APIRouter(prefix="/runs", tags=["runs"])

# -- Request / response models -------------------------------------------------


class RunResponse(BaseModel):
    """Response body for a run.

    The target component's identity is resolved server-side (the row is
    eagerly joined), so clients never need a second lookup — including for
    target kinds they do not know about. All three ``component_*`` identity
    fields are ``None`` exactly when the target was deleted
    (``component_id`` nulls on deletion).

    A response is one attempt. In a stack-native listing it is the stack's
    latest, so ``attempt`` is also how many attempts the stack took; fetching
    an older attempt by id gives that attempt's own number. ``root_run_id``
    is what groups them, and lists them through ``?root_run_id=``.

    ``execution_counts`` is the attempt's own operation executions per status
    (a failed-scope retry only re-executes what had not succeeded), so a
    listing can draw each attempt's progress without fetching its executions.
    """

    id: UUID
    org_id: UUID
    component_id: UUID | None
    component_kind: str | None = None
    component_key: str | None = None
    component_name: str | None = None
    backfill_id: UUID | None
    partition_key: str | None
    status: str
    retry_of: UUID | None = None
    root_run_id: UUID | None = None
    attempt: int = 1
    retry_scope: str | None = None
    scheduled_for: dt.datetime | None = None
    error: str | None = None
    started_at: dt.datetime | None = None
    completed_at: dt.datetime | None = None
    created_at: dt.datetime | None = None
    execution_counts: dict[str, int] = Field(default_factory=dict)

    @classmethod
    def from_run(cls, run: Run, execution_counts: dict[str, int] | None = None) -> RunResponse:
        """Convert a DB Run to a RunResponse.

        Args:
            run: The DB Run row, with its ``target`` relationship loaded.
            execution_counts: The run's execution count per status; None (a
                run just queued) reads as no executions yet.

        Returns:
            The response model.
        """
        return cls(
            id=run.id,
            org_id=run.org_id,
            component_id=run.component_id,
            component_kind=run.target.kind if run.target else None,
            component_key=run.target.key if run.target else None,
            component_name=run.target.name if run.target else None,
            backfill_id=run.backfill_id,
            partition_key=run.partition_key,
            status=run.status,
            retry_of=run.retry_of,
            root_run_id=run.root_run_id,
            attempt=run.attempt,
            retry_scope=run.retry_scope,
            scheduled_for=run.scheduled_for,
            error=run.error,
            started_at=run.started_at,
            completed_at=run.completed_at,
            created_at=run.created_at,
            execution_counts=execution_counts or {},
        )


class RunCreateRequest(BaseModel):
    """Request body for queuing a run targeting a runnable component."""

    component_id: UUID
    partition_key: str | None = None


class RetryRequest(BaseModel):
    """Request body for retrying a failed run."""

    scope: Literal["all", "failed"] = "all"


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
    timestamp: dt.datetime

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
            timestamp=event.timestamp,
        )


# -- Endpoints -----------------------------------------------------------------


@org_router.get("")
def list_runs(
    scope: ViewerDep,
    store: StoreDep,
    query: Annotated[RunQuery, Query()],
) -> Page[RunResponse]:
    """List one row per run stack, one stack's attempts, or every attempt.

    A stack is one piece of work, so a listing carries its latest attempt and
    every filter reads that attempt: a stack whose first attempt failed and
    whose second succeeded is a success. ``root_run_id`` asks for one stack's
    attempts instead, newest first. ``after``/``before`` bound the runs to
    those whose execution overlaps that window, which is what a timeline over
    a time range asks for.

    Args:
        scope: The organisation the path names, the caller at least a viewer there.
        store: The Store instance.
        query: The filters, the order and the window to read; see
            :class:`~interloper_db.RunQuery`.

    Returns:
        The page of runs, each with its executions counted per status.
    """
    runs = store.runs.list(scope.organisation.id, query)
    counts = store.executions.counts([run.id for run in runs.items])
    return runs.map(lambda run: RunResponse.from_run(run, counts.get(run.id)))


@org_router.post("", status_code=201)
def create_run(
    body: RunCreateRequest,
    scope: OrgDep,
    store: StoreDep,
) -> RunResponse:
    """Queue a single run targeting a component whose kind declares an operation.

    Args:
        body: The component to run and the partition key to run it for.
        scope: The organisation the path names and the caller's role there.
        store: The Store instance.

    Returns:
        The queued run.
    """
    target = load_authorized(store.components.get, body.component_id, scope, label="Component", minimum="editor")
    return RunResponse.from_run(
        store.runs.create(target.org_id, component_id=body.component_id, partition_key=body.partition_key)
    )


@org_router.get("/{run_id}")
def get_run(
    run_id: UUID,
    scope: OrgDep,
    store: StoreDep,
) -> RunResponse:
    """Get a single run by ID. It must belong to the path's organisation.

    Args:
        run_id: The run UUID.
        scope: The organisation the path names and the caller's role there.
        store: The Store instance.

    Returns:
        The run, with its executions counted per status.
    """
    run = load_authorized(store.runs.get, run_id, scope, label="Run")
    return RunResponse.from_run(run, store.executions.counts([run_id]).get(run_id))


@org_router.post("/{run_id}/retry", status_code=201)
def retry_run(
    run_id: UUID,
    scope: OrgDep,
    store: StoreDep,
    body: RetryRequest | None = None,
) -> RunResponse:
    """Queue the next attempt of a failed run's stack.

    The new run continues the stack (``retry_of``, ``root_run_id``). With
    ``scope="all"`` the whole DAG re-runs; with ``scope="failed"`` only the
    previously failed or cancelled assets re-run. A run not in a retryable
    state answers 409.

    Args:
        run_id: The UUID of the run to retry, any attempt of its stack.
        scope: The organisation the path names and the caller's role there.
        store: The Store instance.
        body: The retry scope; None retries the whole DAG, as ``scope="all"`` does.

    Returns:
        The queued attempt.
    """
    load_authorized(store.runs.get, run_id, scope, label="Run", minimum="editor")
    return RunResponse.from_run(store.runs.retry(run_id, scope=body.scope if body else "all"))


@org_router.post("/{run_id}/cancel")
def cancel_run(
    run_id: UUID,
    scope: OrgDep,
    store: StoreDep,
) -> RunResponse:
    """Cancel a run that has not ended, wherever it stands.

    A run not yet executing never will; an executing one stops on its next
    heartbeat. A canceled run is not retried. A run already ended answers 409.

    Args:
        run_id: The run UUID.
        scope: The organisation the path names and the caller's role there.
        store: The Store instance.

    Returns:
        The canceled run.
    """
    load_authorized(store.runs.get, run_id, scope, label="Run", minimum="editor")
    store.runs.cancel(run_id)
    return RunResponse.from_run(store.runs.get(run_id))


@org_router.get("/{run_id}/executions")
def list_run_executions(
    run_id: UUID,
    scope: OrgDep,
    store: StoreDep,
    query: Annotated[PageQuery, Query()],
) -> Page[ExecutionResponse]:
    """List a run's operation executions.

    Args:
        run_id: The run UUID.
        scope: The organisation the path names and the caller's role there.
        store: The Store instance.
        query: The window to read.

    Returns:
        The page of the run's executions.
    """
    run = load_authorized(store.runs.get, run_id, scope, label="Run")
    executions = store.executions.list(run.org_id, ExecutionQuery(**query.model_dump()), run_id=run_id)
    return executions.map(ExecutionResponse.from_row)


@org_router.get("/{run_id}/events")
def list_run_events(
    run_id: UUID,
    scope: OrgDep,
    store: StoreDep,
    query: Annotated[EventQuery, Query()],
) -> Page[EventResponse]:
    """List a run's events, oldest first.

    ``component_id`` and ``event_type`` may each be repeated to narrow the
    listing to one or more components (every asset sharing a status) and
    event types (a "Lifecycle"/"Errors"/"Logs" tab); the two compose, and the
    page's total lets a client reach the terminal events that sort last.

    Args:
        run_id: The run UUID.
        scope: The organisation the path names and the caller's role there.
        store: The Store instance.
        query: The components, event types, error filter and window to read.

    Returns:
        The page of events, oldest first.
    """
    run = load_authorized(store.runs.get, run_id, scope, label="Run")
    return store.events.list(run.org_id, query, run_id=run_id).map(EventResponse.from_event)


class IngestResponse(BaseModel):
    """Whether the run was live when the batch arrived."""

    live: bool


@router.post("/{run_id}/events", dependencies=[Depends(require_run)])
def ingest_events(run_id: UUID, body: dict[str, Any], store: StoreDep) -> IngestResponse:
    """Take in a batch of the run's events; everything a run's pod writes comes through here.

    Only the run's own token is accepted (`require_run`).

    Args:
        run_id: The run, the one its token stands for.
        body: The batch, in `EventBatch`'s JSON shape.
        store: The Store instance.

    Returns:
        Whether the run was live when the batch arrived.

    Raises:
        HTTPException: 422 when the batch does not parse.
    """
    try:
        batch = il.EventBatch.from_dict(body)
    except EventError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    return IngestResponse(live=store.runs.ingest(run_id, batch))
