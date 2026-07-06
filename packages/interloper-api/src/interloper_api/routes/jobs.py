"""Jobs API: CRUD for scheduled materialization jobs."""

from __future__ import annotations

import datetime as dt
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from interloper.errors import NotFoundError
from interloper_db import JobRecord, Profile, Store
from pydantic import BaseModel

from interloper_api.components import timestamp
from interloper_api.dependencies import (
    get_current_user,
    get_org_id,
    get_store,
    load_authorized,
    require_editor,
    require_viewer,
)

router = APIRouter()


class JobCreateRequest(BaseModel):
    """Request body for creating or updating a job."""

    name: str
    cron: str
    source_ids: list[UUID] | None = None
    asset_ids: list[UUID] | None = None
    tags: list[str] | None = None
    enabled: bool = True
    partitioned: bool = False
    backfill_days: int | None = None


class JobResponse(BaseModel):
    """Response body for a job."""

    id: UUID
    org_id: UUID
    name: str
    cron: str
    tags: list[str]
    enabled: bool
    partitioned: bool
    backfill_days: int | None
    source_ids: list[UUID]
    asset_ids: list[UUID]
    last_run_at: str | None = None
    next_run_at: str | None = None
    created_at: str | None = None


class RunRequest(BaseModel):
    """Request body for queuing a single run."""

    partition_date: dt.date | None = None


class BackfillRequest(BaseModel):
    """Request body for queuing a backfill."""

    start_date: dt.date
    end_date: dt.date
    concurrency: int = 1
    fail_fast: bool = False


def _job_to_response(job: JobRecord) -> JobResponse:
    """Convert a JobRecord to a JobResponse."""
    return JobResponse(
        id=job.id,
        org_id=job.org_id,
        name=job.name,
        cron=job.cron,
        tags=job.tags,
        enabled=job.enabled,
        partitioned=job.partitioned,
        backfill_days=job.backfill_days,
        source_ids=job.source_ids,
        asset_ids=job.asset_ids,
        last_run_at=timestamp(job.last_run_at),
        next_run_at=timestamp(job.next_run_at),
        created_at=timestamp(job.created_at),
    )


@router.get("/")
def list_jobs(
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[JobResponse]:
    """List all jobs for the current organisation."""
    jobs = store.list_jobs(org_id)
    return [_job_to_response(j) for j in jobs]


@router.get("/{job_id}")
def get_job(
    job_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> JobResponse:
    """Get a single job by ID. Authorized by membership in the job's org."""
    job = load_authorized(store.get_job, job_id, user, store, label="Job")
    return _job_to_response(job)


@router.post("/")
def create_job(
    body: JobCreateRequest,
    user: Profile = Depends(require_editor),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> JobResponse:
    """Create a new job."""
    job = store.create_job(
        org_id,
        name=body.name,
        cron=body.cron,
        source_ids=body.source_ids,
        asset_ids=body.asset_ids,
        tags=body.tags,
        enabled=body.enabled,
        partitioned=body.partitioned,
        backfill_days=body.backfill_days,
    )
    return _job_to_response(job)


@router.put("/{job_id}")
def update_job(
    job_id: UUID,
    body: JobCreateRequest,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> JobResponse:
    """Update an existing job."""
    load_authorized(store.get_job, job_id, user, store, label="Job", minimum="editor")
    try:
        job = store.update_job(
            job_id,
            name=body.name,
            cron=body.cron,
            source_ids=body.source_ids,
            asset_ids=body.asset_ids,
            tags=body.tags,
            enabled=body.enabled,
            partitioned=body.partitioned,
            backfill_days=body.backfill_days,
        )
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return _job_to_response(job)


@router.delete("/{job_id}")
def delete_job(
    job_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Delete a job."""
    load_authorized(store.get_job, job_id, user, store, label="Job", minimum="editor")
    store.delete_job(job_id)
    return {"status": "deleted"}


@router.post("/{job_id}/run")
def queue_run(
    job_id: UUID,
    body: RunRequest | None = None,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Queue a single run for a job."""
    job = load_authorized(store.get_job, job_id, user, store, label="Job", minimum="editor")

    run = store.create_run(
        job.org_id,
        job_id=job_id,
        partition_date=body.partition_date if body else None,
    )
    return {"status": "queued", "run_id": str(run.id)}


@router.post("/{job_id}/backfill")
def queue_backfill(
    job_id: UUID,
    body: BackfillRequest,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Queue a backfill for a job."""
    job = load_authorized(store.get_job, job_id, user, store, label="Job", minimum="editor")

    backfill = store.create_backfill(
        job.org_id,
        job_id=job_id,
        start_date=body.start_date,
        end_date=body.end_date,
        concurrency=body.concurrency,
        fail_fast=body.fail_fast,
    )
    return {"status": "queued", "backfill_id": str(backfill.id)}
