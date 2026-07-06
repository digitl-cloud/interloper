"""Job persistence: CRUD and hydration for scheduled materialization jobs.

A job is a component row (``kind='job'``): its trigger and policy live in
``config`` (the spec) and its targets are ``target`` relations to sources and
standalone assets. The ``state`` column is machine-owned — the scheduler
writes ``next_run_at``/``last_run_at`` as ISO-8601 strings via targeted
updates; CRUD here never touches it.

:class:`JobRecord` is the flat read model handed to API consumers, merging
config, target ids, and schedule state from the single row.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast
from uuid import UUID

import interloper as il
from interloper.errors import ComponentDriftError, HydrationError, NotFoundError
from sqlmodel import Session, select

from interloper_db.drift import ComponentStatus, asset_status, source_status
from interloper_db.engine import get_engine
from interloper_db.hydration import Hydrator
from interloper_db.models import Component, ComponentRelation
from interloper_db.store.components import sync_relations


@dataclass
class JobRecord:
    """Flat, API-friendly view of a persisted job."""

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
    last_run_at: datetime | None
    next_run_at: datetime | None
    created_at: datetime | None


def _job_record(db_job: Component, targets: list[ComponentRelation]) -> JobRecord:
    """Assemble a JobRecord from a job row and its target relations."""
    config = db_job.config or {}
    state = db_job.state or {}
    return JobRecord(
        id=db_job.id,
        org_id=db_job.org_id,
        name=db_job.name or "",
        cron=config.get("cron") or "",
        tags=config.get("tags") or [],
        enabled=config.get("enabled", True),
        partitioned=config.get("partitioned", False),
        backfill_days=config.get("backfill_days"),
        source_ids=[rel.dst_id for rel in targets if rel.dst_kind == "source"],
        asset_ids=[rel.dst_id for rel in targets if rel.dst_kind == "asset"],
        last_run_at=_state_datetime(state, "last_run_at"),
        next_run_at=_state_datetime(state, "next_run_at"),
        created_at=db_job.created_at,
    )


def _state_datetime(state: dict[str, Any], key: str) -> datetime | None:
    """Parse an ISO-8601 timestamp from a state dict."""
    value = state.get(key)
    return datetime.fromisoformat(value) if value else None


def _target_relations(session: Session, job_id: UUID | None) -> list[ComponentRelation]:
    """Fetch a job's target relations."""
    if job_id is None:
        return []
    statement = select(ComponentRelation).where(ComponentRelation.src_id == job_id, ComponentRelation.type == "target")
    return list(session.exec(statement).all())


class JobMixin:
    """Store methods for job management."""

    _hydrator: Hydrator
    _catalog: il.Catalog

    def create_job(
        self,
        org_id: UUID,
        *,
        name: str,
        cron: str,
        source_ids: list[UUID] | None = None,
        asset_ids: list[UUID] | None = None,
        tags: list[str] | None = None,
        enabled: bool = True,
        partitioned: bool = False,
        backfill_days: int | None = None,
    ) -> JobRecord:
        """Create a new job.

        Args:
            org_id: Organisation UUID.
            name: Job name.
            cron: Cron expression.
            source_ids: Source UUIDs to target.
            asset_ids: Standalone asset UUIDs to target.
            tags: Optional tags for grouping.
            enabled: Whether the job is active.
            partitioned: Whether the job creates partitioned runs.
            backfill_days: Days to backfill on first run.

        Returns:
            The saved job record.
        """
        with Session(get_engine()) as session:
            db_job = Component(
                org_id=org_id,
                kind="job",
                key="job",
                name=name,
                config=_job_config(cron, tags, enabled, partitioned, backfill_days),
            )
            session.add(db_job)
            session.flush()
            sync_relations(session, db_job, "target", [*(source_ids or []), *(asset_ids or [])])
            session.commit()
            return _job_record(db_job, _target_relations(session, db_job.id))

    def update_job(
        self,
        job_id: UUID,
        *,
        name: str,
        cron: str,
        source_ids: list[UUID] | None = None,
        asset_ids: list[UUID] | None = None,
        tags: list[str] | None = None,
        enabled: bool = True,
        partitioned: bool = False,
        backfill_days: int | None = None,
    ) -> JobRecord:
        """Update an existing job's spec and targets (state is untouched).

        Args:
            job_id: The job UUID.
            name: Job name.
            cron: Cron expression.
            source_ids: Source UUIDs to target.
            asset_ids: Standalone asset UUIDs to target.
            tags: Optional tags for grouping.
            enabled: Whether the job is active.
            partitioned: Whether the job creates partitioned runs.
            backfill_days: Days to backfill on first run.

        Returns:
            The updated job record.

        Raises:
            NotFoundError: If the job is not found.
        """
        with Session(get_engine()) as session:
            db_job = session.get(Component, job_id)
            if not db_job or db_job.kind != "job":
                raise NotFoundError(f"Job {job_id} not found")
            db_job.name = name
            db_job.config = _job_config(cron, tags, enabled, partitioned, backfill_days)
            sync_relations(session, db_job, "target", [*(source_ids or []), *(asset_ids or [])])
            session.commit()
            return _job_record(db_job, _target_relations(session, db_job.id))

    def get_job(self, job_id: UUID) -> JobRecord:
        """Load a job by ID.

        Args:
            job_id: The job UUID.

        Returns:
            The job record.

        Raises:
            NotFoundError: If the job is not found.
        """
        with Session(get_engine()) as session:
            db_job = session.get(Component, job_id)
            if not db_job or db_job.kind != "job":
                raise NotFoundError(f"Job {job_id} not found")
            return _job_record(db_job, _target_relations(session, db_job.id))

    def list_jobs(self, org_id: UUID) -> list[JobRecord]:
        """List all jobs for an organisation.

        Args:
            org_id: Organisation UUID.

        Returns:
            List of job records.
        """
        with Session(get_engine()) as session:
            db_jobs = session.exec(
                select(Component)
                .where(Component.org_id == org_id, Component.kind == "job")
                .order_by(Component.created_at)  # ty: ignore[invalid-argument-type]
            ).all()
            return [_job_record(db_job, _target_relations(session, db_job.id)) for db_job in db_jobs]

    def delete_job(self, job_id: UUID) -> None:
        """Delete a job. Target relations cascade via FK.

        Args:
            job_id: The job UUID.
        """
        with Session(get_engine()) as session:
            db_job = session.get(Component, job_id)
            if db_job and db_job.kind == "job":
                session.delete(db_job)
                session.commit()

    def load_job(self, job_id: UUID) -> il.Job:
        """Hydrate a framework Job with live target instances.

        Every target is drift-checked first, so a job whose source or asset
        has drifted out of the catalog fails closed instead of silently
        materializing a partial workload.

        Args:
            job_id: The job UUID.

        Returns:
            The hydrated framework Job, targets included.

        Raises:
            NotFoundError: If the job is not found.
            ComponentDriftError: If any target's catalog key does not resolve.
            HydrationError: If reconstruction fails.
        """
        with Session(get_engine()) as session:
            db_job = session.get(Component, job_id)
            if not db_job or db_job.kind != "job":
                raise NotFoundError(f"Job {job_id} not found")

            for rel in _target_relations(session, db_job.id):
                target = session.get(Component, rel.dst_id)
                if target is None:
                    continue  # defensive: FKs make this unreachable
                if target.kind == "source":
                    status = source_status(self._catalog, target.key)
                else:
                    status = asset_status(self._catalog, target.key)
                if status is not ComponentStatus.OK:
                    raise ComponentDriftError(
                        f"Job '{db_job.name}' ({db_job.id}) cannot be hydrated: target "
                        f"{target.kind} '{target.key}' ({target.id}) is {status.value}."
                    )

            spec = self._hydrator.build_component_spec(session, db_job)

        try:
            return cast(il.Job, spec.reconstruct())
        except Exception as e:
            raise HydrationError(f"Failed to hydrate job '{db_job.name}' ({db_job.id}): {e}") from e


def _job_config(
    cron: str,
    tags: list[str] | None,
    enabled: bool,
    partitioned: bool,
    backfill_days: int | None,
) -> dict[str, Any]:
    """Build the config payload for a job row."""
    return {
        "cron": cron,
        "tags": tags or [],
        "enabled": enabled,
        "partitioned": partitioned,
        "backfill_days": backfill_days,
    }
