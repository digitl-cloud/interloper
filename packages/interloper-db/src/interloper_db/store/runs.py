"""Run, event, and backfill persistence."""

from __future__ import annotations

import datetime as dt
import logging
from datetime import datetime, timedelta, timezone
from uuid import UUID

import interloper as il
from interloper.errors import NotFoundError, RunNotFoundError
from sqlalchemy import text
from sqlmodel import Session, col, select

from interloper_db.engine import get_engine
from interloper_db.models import Backfill, Event, Run

logger = logging.getLogger(__name__)


class RunMixin:
    """Store methods for runs, events, and backfills."""

    def save_event(self, event: il.Event, org_id: UUID, run_id: UUID | None = None) -> Event:
        """Persist a framework event to the database.

        Args:
            event: The framework Event.
            org_id: Organisation UUID.
            run_id: Optional run UUID.

        Returns:
            The saved Event row.
        """

        with Session(get_engine()) as session:
            db_event = Event(
                org_id=org_id,
                run_id=run_id,
                backfill_id=UUID(event.metadata["backfill_id"]) if event.metadata.get("backfill_id") else None,
                event_type=event.type.value,
                asset_id=UUID(event.metadata["asset_id"]) if event.metadata.get("asset_id") else None,
                asset_key=event.metadata.get("asset_key"),
                partition_or_window=event.metadata.get("partition_or_window"),
                error=event.metadata.get("error"),
                traceback=event.metadata.get("traceback"),
                message=event.metadata.get("message"),
                timestamp=event.timestamp,
            )
            session.add(db_event)
            session.commit()
            session.refresh(db_event)
            return db_event

    def list_events(
        self,
        *,
        run_id: UUID | None = None,
        org_id: UUID | None = None,
        limit: int = 100,
    ) -> list[Event]:
        """List events, optionally filtered by run.

        Args:
            run_id: Optional run filter.
            org_id: Optional org filter.
            limit: Max results (default 100).

        Returns:
            List of Event rows.
        """
        with Session(get_engine()) as session:
            statement = select(Event)
            if run_id:
                statement = statement.where(Event.run_id == run_id)
            if org_id:
                statement = statement.where(Event.org_id == org_id)
            statement = statement.order_by(col(Event.timestamp).asc()).limit(limit)
            return list(session.exec(statement).all())

    def list_asset_executions(self, run_id: UUID) -> list[dict]:
        """List asset executions for a run from the ``asset_executions`` view.

        Args:
            run_id: The run UUID.

        Returns:
            List of dicts with run_id, org_id, asset_key, status,
            started_at, completed_at, created_at.
        """
        with Session(get_engine()) as session:
            result = session.execute(  # type: ignore[deprecated]
                text("SELECT * FROM asset_executions WHERE run_id = :run_id"),
                {"run_id": str(run_id)},
            )
            return [dict(row._mapping) for row in result.fetchall()]

    # -- Runs -----------------------------------------------------------------

    def create_run(
        self,
        org_id: UUID,
        *,
        job_id: UUID | None = None,
        partition_date: dt.date | None = None,
    ) -> Run:
        """Create a single queued run.

        Args:
            org_id: Organisation UUID.
            job_id: Optional job UUID.
            partition_date: Optional partition date.

        Returns:
            The created Run row.
        """
        with Session(get_engine()) as session:
            db_run = Run(
                org_id=org_id,
                job_id=job_id,
                partition_date=partition_date,
                status="queued",
            )
            session.add(db_run)
            session.commit()
            session.refresh(db_run)
            return db_run

    def get_run(self, run_id: UUID) -> Run:
        """Load a run by ID.

        Args:
            run_id: The run UUID.

        Returns:
            The Run row.

        Raises:
            ValueError: If the run is not found.
        """
        with Session(get_engine()) as session:
            db_run = session.get(Run, run_id)
            if not db_run:
                raise RunNotFoundError(f"Run {run_id} not found")
            return db_run

    def list_runs(
        self,
        org_id: UUID,
        *,
        job_id: UUID | None = None,
        backfill_id: UUID | None = None,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Run]:
        """List runs with optional filters.

        Args:
            org_id: Organisation UUID.
            job_id: Optional job filter.
            backfill_id: Optional backfill filter.
            status: Optional status filter.
            limit: Max results (default 50).
            offset: Pagination offset.

        Returns:
            List of Run rows.
        """
        with Session(get_engine()) as session:
            statement = select(Run).where(Run.org_id == org_id)
            if job_id:
                statement = statement.where(Run.job_id == job_id)
            if backfill_id:
                statement = statement.where(Run.backfill_id == backfill_id)
            if status:
                statement = statement.where(Run.status == status)
            statement = statement.order_by(col(Run.created_at).desc()).offset(offset).limit(limit)
            return list(session.exec(statement).all())

    def complete_run(self, run_id: UUID, *, success: bool) -> Run:
        """Mark a run as completed and advance its backfill if applicable.

        Args:
            run_id: The run UUID.
            success: Whether the run succeeded.

        Returns:
            The updated Run row.

        Raises:
            ValueError: If the run is not found.
        """
        with Session(get_engine()) as session:
            db_run = session.get(Run, run_id)
            if not db_run:
                raise RunNotFoundError(f"Run {run_id} not found")

            db_run.status = "success" if success else "failed"
            db_run.completed_at = datetime.now(timezone.utc)
            session.add(db_run)

            if db_run.backfill_id:
                _advance_backfill(session, db_run.backfill_id, failed=not success)

            session.commit()
            session.refresh(db_run)
            return db_run

    # -- Backfills ------------------------------------------------------------

    def create_backfill(
        self,
        org_id: UUID,
        *,
        job_id: UUID | None = None,
        start_date: dt.date,
        end_date: dt.date,
        concurrency: int = 1,
        fail_fast: bool = False,
    ) -> Backfill:
        """Create a backfill with one run per day from start to end (inclusive).

        The first ``concurrency`` runs are queued immediately; the rest
        are set to ``"pending"`` until earlier runs complete.

        Args:
            org_id: Organisation UUID.
            job_id: Optional job UUID.
            start_date: First partition date.
            end_date: Last partition date (inclusive).
            concurrency: Max runs in-flight at once.
            fail_fast: Cancel remaining runs on first failure.

        Returns:
            The created Backfill row with runs.
        """
        with Session(get_engine()) as session:
            db_backfill = Backfill(
                org_id=org_id,
                job_id=job_id,
                start_date=start_date,
                end_date=end_date,
                concurrency=concurrency,
                fail_fast=fail_fast,
                status="running",
                started_at=datetime.now(timezone.utc),
            )
            session.add(db_backfill)
            session.flush()

            current_date = start_date
            partition_count = 0
            launched = 0
            while current_date <= end_date:
                run_status = "queued" if launched < concurrency else "pending"
                db_run = Run(
                    org_id=org_id,
                    job_id=job_id,
                    backfill_id=db_backfill.id,
                    partition_date=current_date,
                    status=run_status,
                )
                session.add(db_run)
                if run_status == "queued":
                    launched += 1
                partition_count += 1
                current_date += timedelta(days=1)

            db_backfill.partitions = partition_count
            session.add(db_backfill)
            session.commit()
            session.refresh(db_backfill)
            return db_backfill

    def get_backfill(self, backfill_id: UUID) -> Backfill:
        """Load a backfill by ID.

        Args:
            backfill_id: The backfill UUID.

        Returns:
            The Backfill row.

        Raises:
            ValueError: If the backfill is not found.
        """
        with Session(get_engine()) as session:
            db_backfill = session.get(Backfill, backfill_id)
            if not db_backfill:
                raise NotFoundError(f"Backfill {backfill_id} not found")
            return db_backfill

    def list_backfills(self, org_id: UUID) -> list[Backfill]:
        """List all backfills for an organisation.

        Args:
            org_id: Organisation UUID.

        Returns:
            List of Backfill rows.
        """
        with Session(get_engine()) as session:
            statement = select(Backfill).where(Backfill.org_id == org_id).order_by(col(Backfill.created_at).desc())
            return list(session.exec(statement).all())

    def list_active_backfills(self, org_id: UUID) -> list[Backfill]:
        """List in-progress backfills for an organisation.

        Args:
            org_id: Organisation UUID.

        Returns:
            List of Backfill rows with status ``"running"`` or ``"queued"``.
        """
        with Session(get_engine()) as session:
            statement = select(Backfill).where(
                Backfill.org_id == org_id,
                col(Backfill.status).in_(["running", "queued"]),
            )
            return list(session.exec(statement).all())


def _advance_backfill(session: Session, backfill_id: UUID, *, failed: bool) -> None:
    """Advance a backfill after a run completes.

    1. **Fail-fast**: if enabled and the run failed, cancel pending runs.
    2. **Finalize**: if nothing in-flight or pending, mark complete.
    3. **Advance**: promote next pending runs up to concurrency limit.

    Args:
        session: Active database session (caller commits).
        backfill_id: The backfill UUID.
        failed: Whether the completing run failed.
    """
    db_backfill = session.get(Backfill, backfill_id)
    if not db_backfill or db_backfill.status not in ("running", "queued"):
        return

    if db_backfill.fail_fast and failed:
        pending_runs = session.exec(
            select(Run).where(Run.backfill_id == backfill_id, Run.status == "pending")
        ).all()
        for pending_run in pending_runs:
            pending_run.status = "canceled"
            session.add(pending_run)

        db_backfill.status = "failed"
        db_backfill.completed_at = datetime.now(timezone.utc)
        session.add(db_backfill)
        return

    in_flight_count = len(
        session.exec(
            select(Run).where(
                Run.backfill_id == backfill_id,
                col(Run.status).in_(["queued", "running"]),
            )
        ).all()
    )
    pending_runs = session.exec(
        select(Run).where(Run.backfill_id == backfill_id, Run.status == "pending").order_by(Run.partition_date)  # type: ignore[arg-type]
    ).all()

    if in_flight_count == 0 and len(pending_runs) == 0:
        any_failed = session.exec(
            select(Run).where(Run.backfill_id == backfill_id, Run.status == "failed")
        ).first()
        db_backfill.status = "failed" if any_failed else "success"
        db_backfill.completed_at = datetime.now(timezone.utc)
        session.add(db_backfill)
        return

    available_slots = max(0, db_backfill.concurrency - in_flight_count)
    for pending_run in pending_runs[:available_slots]:
        pending_run.status = "queued"
        session.add(pending_run)
