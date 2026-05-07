"""Job persistence: CRUD for scheduled materialization jobs."""

from __future__ import annotations

from uuid import UUID

from interloper.errors import JobNotFoundError
from sqlalchemy.orm import selectinload
from sqlmodel import Session, select

from interloper_db.engine import get_engine
from interloper_db.models import Job, JobAsset, JobSource

_JOB_LOAD_OPTIONS = [
    selectinload(Job.sources),  # type: ignore[arg-type]
    selectinload(Job.assets),  # type: ignore[arg-type]
]


class JobMixin:
    """Store methods for job management."""

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
    ) -> Job:
        """Create a new job.

        Args:
            org_id: Organisation UUID.
            name: Job name.
            cron: Cron expression.
            source_ids: Source UUIDs to link.
            asset_ids: Standalone asset UUIDs to link.
            tags: Optional tags for grouping.
            enabled: Whether the job is active.
            partitioned: Whether the job creates partitioned runs.
            backfill_days: Days to backfill on first run.

        Returns:
            The saved Job row.
        """
        with Session(get_engine()) as session:
            db_job = Job(
                org_id=org_id,
                name=name,
                cron=cron,
                tags=tags or [],
                enabled=enabled,
                partitioned=partitioned,
                backfill_days=backfill_days,
            )
            session.add(db_job)
            session.flush()
            assert db_job.id is not None
            for source_id in source_ids or []:
                session.add(JobSource(job_id=db_job.id, source_id=source_id))
            for asset_id in asset_ids or []:
                session.add(JobAsset(job_id=db_job.id, asset_id=asset_id))  # type: ignore[arg-type]
            session.commit()
            session.refresh(db_job)
            return db_job

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
    ) -> Job:
        """Update an existing job.

        Args:
            job_id: The job UUID.
            name: Job name.
            cron: Cron expression.
            source_ids: Source UUIDs to link.
            asset_ids: Standalone asset UUIDs to link.
            tags: Optional tags for grouping.
            enabled: Whether the job is active.
            partitioned: Whether the job creates partitioned runs.
            backfill_days: Days to backfill on first run.

        Returns:
            The updated Job row.

        Raises:
            JobNotFoundError: If the job is not found.
        """
        with Session(get_engine()) as session:
            db_job = session.get(Job, job_id)
            if not db_job:
                raise JobNotFoundError(f"Job {job_id} not found")
            db_job.name = name
            db_job.cron = cron
            db_job.tags = tags or []
            db_job.enabled = enabled
            db_job.partitioned = partitioned
            db_job.backfill_days = backfill_days

            # Rebuild source bindings
            existing_source_links = session.exec(select(JobSource).where(JobSource.job_id == db_job.id)).all()
            for link in existing_source_links:
                session.delete(link)
            for source_id in source_ids or []:
                session.add(JobSource(job_id=db_job.id, source_id=source_id))  # type: ignore[arg-type]

            # Rebuild asset bindings
            existing_asset_links = session.exec(select(JobAsset).where(JobAsset.job_id == db_job.id)).all()
            for link in existing_asset_links:
                session.delete(link)
            for asset_id in asset_ids or []:
                session.add(JobAsset(job_id=db_job.id, asset_id=asset_id))  # type: ignore[arg-type]

            session.commit()
            session.refresh(db_job)
            return db_job

    def get_job(self, job_id: UUID) -> Job:
        """Load a job by ID with sources and standalone assets.

        Args:
            job_id: The job UUID.

        Returns:
            The Job row.

        Raises:
            JobNotFoundError: If the job is not found.
        """
        with Session(get_engine()) as session:
            db_job = session.get(Job, job_id, options=_JOB_LOAD_OPTIONS)
            if not db_job:
                raise JobNotFoundError(f"Job {job_id} not found")
            return db_job

    def list_jobs(self, org_id: UUID) -> list[Job]:
        """List all jobs for an organisation with sources and standalone assets loaded.

        Args:
            org_id: Organisation UUID.

        Returns:
            List of Job rows.
        """
        with Session(get_engine()) as session:
            statement = (
                select(Job)
                .where(Job.org_id == org_id)
                .options(*_JOB_LOAD_OPTIONS)
            )
            return list(session.exec(statement).all())

    def delete_job(self, job_id: UUID) -> None:
        """Delete a job. Source and asset bindings cascade via FK.

        Args:
            job_id: The job UUID.
        """
        with Session(get_engine()) as session:
            db_job = session.get(Job, job_id)
            if db_job:
                session.delete(db_job)
                session.commit()
