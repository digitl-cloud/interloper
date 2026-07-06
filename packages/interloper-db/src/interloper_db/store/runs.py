"""Run, event, and backfill persistence."""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import interloper as il
from interloper.errors import NotFoundError
from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlmodel import Session, col, select

from interloper_db.engine import get_engine
from interloper_db.models import Backfill, Component, Event, Run

logger = logging.getLogger(__name__)

_MAX_EVENT_TEXT = 60_000
"""Defensive cap for free-text event fields (well under Postgres limits)."""


def _sanitize_text(value: str | None, *, max_len: int = _MAX_EVENT_TEXT) -> str | None:
    """Make a free-text event field safe to persist.

    Postgres ``text`` columns cannot store NUL bytes (``0x00``) — a single
    one makes the whole INSERT raise, which (because event persistence is
    best-effort) would silently drop the event.  Strip NULs and cap the
    length so an oversized traceback can't fail the write either.

    Returns:
        The cleaned string, or ``None`` if *value* is ``None``.
    """
    if value is None:
        return None
    cleaned = value.replace("\x00", "")
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len] + "…[truncated]"
    return cleaned


class RunMixin:
    """Store methods for runs, events, and backfills."""

    def save_event(self, event: il.Event, org_id: UUID, run_id: UUID | None = None) -> Event:
        """Persist a framework event to the database, idempotently.

        The event's producer-assigned ``id`` becomes the row primary key
        and the insert is an upsert (``ON CONFLICT DO NOTHING``), so the
        same event delivered more than once — e.g. re-emitted from a child
        container's log stream and also written directly — yields a single
        row rather than a duplicate or an error.  Free-text fields are
        sanitized so a stray NUL byte or oversized traceback can't fail the
        write and silently drop the event.

        Args:
            event: The framework Event.
            org_id: Organisation UUID.
            run_id: Optional run UUID.

        Returns:
            The saved Event row.
        """
        meta = event.metadata
        try:
            event_id = UUID(event.id)
        except (ValueError, TypeError):
            event_id = uuid4()

        values: dict[str, object | None] = {
            "id": event_id,
            "org_id": org_id,
            "run_id": run_id,
            "backfill_id": UUID(meta["backfill_id"]) if meta.get("backfill_id") else None,
            "event_type": event.type.value,
            "asset_id": UUID(meta["asset_id"]) if meta.get("asset_id") else None,
            "asset_key": _sanitize_text(meta.get("asset_key")),
            "partition_or_window": _sanitize_text(meta.get("partition_or_window")),
            "error": _sanitize_text(meta.get("error")),
            "traceback": _sanitize_text(meta.get("traceback")),
            "message": _sanitize_text(meta.get("message")),
            "level": _sanitize_text(meta.get("level")),
            "timestamp": event.timestamp,
        }

        with Session(get_engine()) as session:
            stmt = pg_insert(Event).values(**values).on_conflict_do_nothing(index_elements=["id"])
            session.execute(stmt)  # ty: ignore[deprecated]
            session.commit()
            saved = session.get(Event, event_id)
            if saved is None:  # pragma: no cover - only if the row was concurrently deleted
                raise RuntimeError(f"Event {event_id} missing immediately after upsert")
            return saved

    def list_events(
        self,
        *,
        run_id: UUID | None = None,
        org_id: UUID | None = None,
        asset_ids: Sequence[UUID] | None = None,
        event_types: Sequence[str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Event]:
        """List events, optionally filtered by run, asset(s) and/or type(s).

        Ordering is ``timestamp ASC, id ASC`` — stable and deterministic so
        ``offset``/``limit`` paging never skips or repeats a row when several
        events share a timestamp.

        Args:
            run_id: Optional run filter.
            org_id: Optional org filter.
            asset_ids: Optional filter to events of any of these assets.
            event_types: Optional filter to events of any of these types.
            limit: Max results (default 100).
            offset: Pagination offset.

        Returns:
            List of Event rows.
        """
        with Session(get_engine()) as session:
            statement = select(Event)
            if run_id:
                statement = statement.where(Event.run_id == run_id)
            if org_id:
                statement = statement.where(Event.org_id == org_id)
            if asset_ids:
                statement = statement.where(col(Event.asset_id).in_(asset_ids))
            if event_types:
                statement = statement.where(col(Event.event_type).in_(event_types))
            statement = (
                statement.order_by(col(Event.timestamp).asc(), col(Event.id).asc()).offset(offset).limit(limit)
            )
            return list(session.exec(statement).all())

    def count_events(
        self,
        *,
        run_id: UUID | None = None,
        org_id: UUID | None = None,
        asset_ids: Sequence[UUID] | None = None,
        event_types: Sequence[str] | None = None,
    ) -> int:
        """Count events matching the same filters as :meth:`list_events`.

        Args:
            run_id: Optional run filter.
            org_id: Optional org filter.
            asset_ids: Optional filter to events of any of these assets.
            event_types: Optional filter to events of any of these types.

        Returns:
            Total number of matching events (ignoring limit/offset).
        """
        with Session(get_engine()) as session:
            statement = select(func.count()).select_from(Event)
            if run_id:
                statement = statement.where(Event.run_id == run_id)
            if org_id:
                statement = statement.where(Event.org_id == org_id)
            if asset_ids:
                statement = statement.where(col(Event.asset_id).in_(asset_ids))
            if event_types:
                statement = statement.where(col(Event.event_type).in_(event_types))
            return session.exec(statement).one()

    def list_asset_executions(self, run_id: UUID) -> list[dict]:
        """List asset executions for a run from the ``asset_executions`` view.

        Args:
            run_id: The run UUID.

        Returns:
            List of dicts with run_id, org_id, asset_key, status,
            started_at, completed_at, created_at.
        """
        with Session(get_engine()) as session:
            result = session.execute(  # ty: ignore[deprecated]
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
                raise NotFoundError(f"Run {run_id} not found")
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

    def count_runs(
        self,
        org_id: UUID,
        *,
        job_id: UUID | None = None,
        backfill_id: UUID | None = None,
        status: str | None = None,
    ) -> int:
        """Count runs matching the same filters as :meth:`list_runs`.

        Args:
            org_id: Organisation UUID.
            job_id: Optional job filter.
            backfill_id: Optional backfill filter.
            status: Optional status filter.

        Returns:
            Total number of matching runs (ignoring limit/offset).
        """
        with Session(get_engine()) as session:
            statement = select(func.count()).select_from(Run).where(Run.org_id == org_id)
            if job_id:
                statement = statement.where(Run.job_id == job_id)
            if backfill_id:
                statement = statement.where(Run.backfill_id == backfill_id)
            if status:
                statement = statement.where(Run.status == status)
            return session.exec(statement).one()

    def complete_run(self, run_id: UUID, *, success: bool) -> Run:
        """Mark a run as completed and advance its backfill if applicable.

        Also stamps ``last_run_at`` on the job's machine-owned state — this is
        the single terminal path every run takes (scheduled, manual, retried),
        so the job's "last run" reflects all of them.

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
                raise NotFoundError(f"Run {run_id} not found")

            db_run.status = "success" if success else "failed"
            db_run.completed_at = datetime.now(timezone.utc)
            session.add(db_run)

            if db_run.job_id:
                db_job = session.get(Component, db_run.job_id)
                if db_job:
                    db_job.state = {**(db_job.state or {}), "last_run_at": db_run.completed_at.isoformat()}
                    session.add(db_job)

            if db_run.backfill_id:
                _advance_backfill(session, db_run.backfill_id, failed=not success)

            session.commit()
            session.refresh(db_run)
            return db_run

    def retry_run(self, run_id: UUID, *, scope: str = "all") -> Run:
        """Queue a new run that retries a failed one.

        Each retry is a fresh ``Run`` row linked to its predecessor via
        ``retry_of`` with an incremented ``attempt``. The new run is created
        outside any backfill so backfill accounting is unaffected.

        Args:
            run_id: The failed run to retry.
            scope: ``"all"`` to re-run the whole DAG, or ``"failed"`` to
                re-run only the previously failed/cancelled assets.

        Returns:
            The newly created, queued Run row.

        Raises:
            NotFoundError: If the run is not found.
            ValueError: If the run is not in a failed state or ``scope`` is invalid.
        """
        if scope not in ("all", "failed"):
            raise ValueError(f"Invalid retry scope: {scope!r} (expected 'all' or 'failed')")

        with Session(get_engine()) as session:
            src = session.get(Run, run_id)
            if not src:
                raise NotFoundError(f"Run {run_id} not found")
            if src.status != "failed":
                raise ValueError(f"Run {run_id} is not failed (status={src.status!r}); only failed runs can be retried")

            db_run = Run(
                org_id=src.org_id,
                job_id=src.job_id,
                partition_date=src.partition_date,
                status="queued",
                retry_of=run_id,
                attempt=src.attempt + 1,
                retry_scope=scope,
            )
            session.add(db_run)
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
        select(Run).where(Run.backfill_id == backfill_id, Run.status == "pending").order_by(Run.partition_date)  # ty: ignore[invalid-argument-type]
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
