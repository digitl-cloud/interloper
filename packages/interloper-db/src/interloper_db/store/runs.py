"""Run, event, and backfill persistence."""

from __future__ import annotations

import datetime as dt
import json
import logging
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import interloper as il
from interloper.errors import NotFoundError
from interloper.partitioning.time import TimeGranularity, TimePartitionWindow, period_range
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlmodel import Session, col, select

from interloper_db.models import AssetExecution, Backfill, Component, Event, Run
from interloper_db.store.base import StoreBase
from interloper_db.store.components import stamp_component_state
from interloper_db.store.quotas import (
    QUOTA_MAX_BACKFILL_PARTITIONS,
    QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH,
    settle_run_usage,
)

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


def _sanitize_data(meta: dict[str, Any]) -> dict[str, Any] | None:
    """Make a metadata dict safe to persist as JSONB, best-effort.

    Non-JSON values are coerced through ``str``; a dict that still can't be
    encoded (circular refs, NaN) is dropped rather than failing the event
    write. Postgres ``jsonb`` rejects NUL escapes the same way ``text``
    rejects NUL bytes, so they are stripped from the encoded form; an
    oversized payload is replaced by a marker so the write can't fail on
    size either.

    Returns:
        The cleaned dict, or ``None`` when there is nothing worth storing.
    """
    if not meta:
        return None
    try:
        encoded = json.dumps(meta, default=str, allow_nan=False)
    except (TypeError, ValueError):
        return None
    if len(encoded) > _MAX_EVENT_TEXT:
        return {"truncated": True}
    if "\\u0000" in encoded:
        encoded = encoded.replace("\\u0000", "")
    return json.loads(encoded) or None


#: Metadata keys whose content is already represented by a structured
#: ``events`` column — everything else spills into the ``data`` JSONB
#: column. ``asset_id``/``asset_key`` are the compat aliases core emitters
#: use for the component columns; ``run_id`` is redundant with the
#: ``save_event`` argument that fills the column.
_PROMOTED_METADATA_KEYS = frozenset(
    {
        "run_id",
        # Also arrives via run metadata; the column is authoritative.
        "org_id",
        "asset_id",
        "asset_key",
        "component_id",
        "component_kind",
        "component_key",
        "error",
        "traceback",
        "message",
        "level",
    }
)


def _event_values(event: il.Event, org_id: UUID, run_id: UUID | None) -> dict[str, Any]:
    """Map a framework event onto ``events`` column values.

    The component reference comes from ``component_id``/``component_kind``/
    ``component_key`` metadata; the ``asset_id``/``asset_key`` keys the asset
    runners emit map onto the same columns (with kind ``"asset"``), so core
    emitters need no knowledge of the persistence schema. Metadata not
    covered by a structured column lands losslessly in ``data``.

    Returns:
        Column values for an ``events`` insert.
    """
    meta = event.metadata
    try:
        event_id = UUID(event.id)
    except (ValueError, TypeError):
        event_id = uuid4()

    component_id = meta.get("component_id") or meta.get("asset_id")
    component_kind = meta.get("component_kind") or ("asset" if meta.get("asset_id") else None)
    component_key = meta.get("component_key") or meta.get("asset_key")

    return {
        "id": event_id,
        "org_id": org_id,
        "run_id": run_id,
        "event_type": event.type.value,
        "component_id": UUID(str(component_id)) if component_id else None,
        "component_kind": _sanitize_text(component_kind),
        "component_key": _sanitize_text(component_key),
        "error": _sanitize_text(meta.get("error")),
        "traceback": _sanitize_text(meta.get("traceback")),
        "message": _sanitize_text(meta.get("message")),
        "level": _sanitize_text(meta.get("level")),
        # None values are the absence of a key, not payload — producers emit
        # them unconditionally (backfill_id on non-backfill runs, …).
        "data": _sanitize_data({k: v for k, v in meta.items() if k not in _PROMOTED_METADATA_KEYS and v is not None}),
        "timestamp": event.timestamp,
    }


def _event_filters(
    run_id: UUID | None,
    org_id: UUID | None,
    component_ids: Sequence[UUID] | None,
    event_types: Sequence[str] | None,
) -> list[Any]:
    """The shared where-clauses of :meth:`RunMixin.list_events` / ``count_events``.

    One builder for both so listing and counting can never disagree.

    Returns:
        Filter expressions for the given (optional) criteria.
    """
    filters: list[Any] = []
    if run_id:
        filters.append(Event.run_id == run_id)
    if org_id:
        filters.append(Event.org_id == org_id)
    if component_ids:
        filters.append(col(Event.component_id).in_(component_ids))
    if event_types:
        filters.append(col(Event.event_type).in_(event_types))
    return filters


def _run_filters(
    org_id: UUID,
    component_id: UUID | None,
    backfill_id: UUID | None,
    status: str | None,
    after: datetime | None = None,
    before: datetime | None = None,
) -> list[Any]:
    """The shared where-clauses of :meth:`RunMixin.list_runs` / ``count_runs``.

    ``after``/``before`` select the runs whose execution *overlaps* the window
    — a run occupies ``[started_at, completed_at)``, left open-ended while it
    is still running. Runs that never started occupy no time and so fall
    outside every window.

    Returns:
        Filter expressions for the given criteria.
    """
    filters: list[Any] = [Run.org_id == org_id]
    if component_id:
        filters.append(Run.component_id == component_id)
    if backfill_id:
        filters.append(Run.backfill_id == backfill_id)
    if status:
        filters.append(Run.status == status)
    if after is not None:
        filters.append(col(Run.completed_at).is_(None) | (col(Run.completed_at) >= after))
    if before is not None:
        filters.append(col(Run.started_at) <= before)
    if after is not None and before is None:
        # An `after` bound alone still means "ran at some point", so a
        # never-started run must not slip through on the NULL completed_at.
        filters.append(col(Run.started_at).is_not(None))
    return filters


class RunMixin(StoreBase):
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
        values = _event_values(event, org_id, run_id)

        with self._session() as session:
            stmt = pg_insert(Event).values(**values).on_conflict_do_nothing(index_elements=["id"])
            session.execute(stmt)  # ty: ignore[deprecated]
            session.commit()
            saved = session.get(Event, values["id"])
            if saved is None:  # pragma: no cover - only if the row was concurrently deleted
                raise RuntimeError(f"Event {values['id']} missing immediately after upsert")
            return saved

    def list_events(
        self,
        *,
        run_id: UUID | None = None,
        org_id: UUID | None = None,
        component_ids: Sequence[UUID] | None = None,
        event_types: Sequence[str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Event]:
        """List events, optionally filtered by run, component(s) and/or type(s).

        Ordering is ``timestamp ASC, id ASC`` — stable and deterministic so
        ``offset``/``limit`` paging never skips or repeats a row when several
        events share a timestamp.

        Args:
            run_id: Optional run filter.
            org_id: Optional org filter.
            component_ids: Optional filter to events of any of these components.
            event_types: Optional filter to events of any of these types.
            limit: Max results (default 100).
            offset: Pagination offset.

        Returns:
            List of Event rows.
        """
        with self._session() as session:
            statement = (
                select(Event)
                .where(*_event_filters(run_id, org_id, component_ids, event_types))
                .order_by(col(Event.timestamp).asc(), col(Event.id).asc())
                .offset(offset)
                .limit(limit)
            )
            return list(session.exec(statement).all())

    def count_events(
        self,
        *,
        run_id: UUID | None = None,
        org_id: UUID | None = None,
        component_ids: Sequence[UUID] | None = None,
        event_types: Sequence[str] | None = None,
    ) -> int:
        """Count events matching the same filters as :meth:`list_events`.

        Args:
            run_id: Optional run filter.
            org_id: Optional org filter.
            component_ids: Optional filter to events of any of these components.
            event_types: Optional filter to events of any of these types.

        Returns:
            Total number of matching events (ignoring limit/offset).
        """
        with self._session() as session:
            statement = (
                select(func.count())
                .select_from(Event)
                .where(*_event_filters(run_id, org_id, component_ids, event_types))
            )
            return session.exec(statement).one()

    def list_asset_executions(self, run_id: UUID) -> list[AssetExecution]:
        """List a run's asset executions from the ``asset_executions`` view.

        Args:
            run_id: The run UUID.

        Returns:
            One read-model row per asset touched by the run.
        """
        with self._session() as session:
            statement = select(AssetExecution).where(AssetExecution.run_id == run_id)
            return list(session.exec(statement).all())

    # -- Runs -----------------------------------------------------------------

    def create_run(
        self,
        org_id: UUID,
        *,
        component_id: UUID | None = None,
        partition_date: dt.date | None = None,
    ) -> Run:
        """Create a single queued run.

        Args:
            org_id: Organisation UUID.
            component_id: Optional target component UUID (any runnable kind).
            partition_date: Optional partition date.

        Returns:
            The created Run row.
        """
        with self._session() as session:
            self.quotas.check(session, org_id, QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH)
            db_run = Run(
                org_id=org_id,
                component_id=component_id,
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
            NotFoundError: If the run is not found.
        """
        with self._session() as session:
            db_run = session.get(Run, run_id)
            if not db_run:
                raise NotFoundError(f"Run {run_id} not found")
            return db_run

    def list_runs(
        self,
        org_id: UUID,
        *,
        component_id: UUID | None = None,
        backfill_id: UUID | None = None,
        status: str | None = None,
        after: datetime | None = None,
        before: datetime | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Run]:
        """List runs with optional filters.

        Args:
            org_id: Organisation UUID.
            component_id: Optional target component filter.
            backfill_id: Optional backfill filter.
            status: Optional status filter.
            after: Keep runs still executing at or after this instant.
            before: Keep runs that had started by this instant.
            limit: Max results (default 50).
            offset: Pagination offset.

        Returns:
            List of Run rows.
        """
        with self._session() as session:
            statement = (
                select(Run)
                .where(*_run_filters(org_id, component_id, backfill_id, status, after, before))
                .order_by(col(Run.created_at).desc())
                .offset(offset)
                .limit(limit)
            )
            return list(session.exec(statement).all())

    def count_runs(
        self,
        org_id: UUID,
        *,
        component_id: UUID | None = None,
        backfill_id: UUID | None = None,
        status: str | None = None,
        after: datetime | None = None,
        before: datetime | None = None,
    ) -> int:
        """Count runs matching the same filters as :meth:`list_runs`.

        Args:
            org_id: Organisation UUID.
            component_id: Optional target component filter.
            backfill_id: Optional backfill filter.
            status: Optional status filter.
            after: Keep runs still executing at or after this instant.
            before: Keep runs that had started by this instant.

        Returns:
            Total number of matching runs (ignoring limit/offset).
        """
        with self._session() as session:
            statement = (
                select(func.count())
                .select_from(Run)
                .where(*_run_filters(org_id, component_id, backfill_id, status, after, before))
            )
            return session.exec(statement).one()

    def complete_run(self, run_id: UUID, *, success: bool) -> Run:
        """Mark a run as completed and advance its backfill if applicable.

        Also stamps ``last_run_at`` on the target component's machine-owned
        state — this is the single terminal path every run takes (scheduled,
        manual, retried), so the component's "last run" reflects all of them.

        Args:
            run_id: The run UUID.
            success: Whether the run succeeded.

        Returns:
            The updated Run row.

        Raises:
            NotFoundError: If the run is not found.
        """
        with self._session() as session:
            db_run = session.get(Run, run_id)
            if not db_run:
                raise NotFoundError(f"Run {run_id} not found")

            db_run.status = "success" if success else "failed"
            db_run.completed_at = datetime.now(timezone.utc)
            session.add(db_run)

            settle_run_usage(session, db_run, success=success)

            if db_run.component_id:
                db_component = session.get(Component, db_run.component_id)
                if db_component:
                    stamp_component_state(db_component, last_run_at=db_run.completed_at)
                    session.add(db_component)

            if db_run.backfill_id:
                _advance_backfill(session, db_run.backfill_id, failed=not success)

            session.commit()
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

        with self._session() as session:
            src = session.get(Run, run_id)
            if not src:
                raise NotFoundError(f"Run {run_id} not found")
            if src.status != "failed":
                raise ValueError(f"Run {run_id} is not failed (status={src.status!r}); only failed runs can be retried")

            self.quotas.check(session, src.org_id, QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH, subject="retry")
            db_run = Run(
                org_id=src.org_id,
                component_id=src.component_id,
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
        component_id: UUID | None = None,
        start_date: dt.date,
        end_date: dt.date,
        concurrency: int = 1,
        fail_fast: bool = False,
    ) -> Backfill:
        """Create a backfill with one run per partition from start to end (inclusive).

        Runs are dispatched **newest partition first**: the latest
        ``concurrency`` of them are queued immediately and the rest are
        ``"pending"`` until earlier runs complete. The freshest data lands
        first, and an interrupted backfill keeps the recent window rather than
        the ancient tail.

        Args:
            org_id: Organisation UUID.
            component_id: Optional target component UUID.
            start_date: First partition date.
            end_date: Last partition date (inclusive).
            concurrency: Max runs in-flight at once.
            fail_fast: Cancel remaining runs on first failure.

        Returns:
            The created Backfill row with runs.

        Raises:
            ValueError: If the range is inverted.
        """
        window = TimePartitionWindow(start_date, end_date, TimeGranularity.DAY)
        span = window.partition_count()

        with self._session() as session:
            # Cron top-ups (a job's `lookback` window) are deliberately not
            # bounded here — they never pass through this method.
            self.quotas.check(session, org_id, QUOTA_MAX_BACKFILL_PARTITIONS, used=span)
            self.quotas.check(session, org_id, QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH, subject="backfill")
            db_backfill = Backfill(
                org_id=org_id,
                component_id=component_id,
                start_date=start_date,
                end_date=end_date,
                concurrency=concurrency,
                fail_fast=fail_fast,
                status="running",
                started_at=datetime.now(timezone.utc),
            )
            session.add(db_backfill)
            session.flush()

            # Rows are created oldest-first but the *newest* `concurrency` of
            # them are the ones queued, so the freshest partitions run first
            # (`_advance_backfill` promotes in the same order). Creation order
            # is deliberately left alone: `list_runs` orders by `created_at`
            # desc, so reversing it would flip the runs list to oldest-first.
            first_queued = max(0, span - concurrency)
            for index, partition_date in enumerate(period_range(window.start, window.end, window.granularity)):
                db_run = Run(
                    org_id=org_id,
                    component_id=component_id,
                    backfill_id=db_backfill.id,
                    partition_date=partition_date,
                    status="queued" if index >= first_queued else "pending",
                )
                session.add(db_run)

            db_backfill.partitions = span
            session.add(db_backfill)
            session.commit()
            session.refresh(db_backfill)
            return db_backfill

    def cancel_backfill(self, backfill_id: UUID) -> Backfill:
        """Cancel a backfill: runs not yet dispatched will never execute.

        Pending and queued runs flip to ``"canceled"``; runs already
        dispatched or running drain to their own terminal state (their late
        completions are no-ops on the now-terminal backfill).

        Args:
            backfill_id: The backfill UUID.

        Returns:
            The updated Backfill row.

        Raises:
            NotFoundError: If the backfill is not found.
            ValueError: If the backfill is already terminal.
        """
        with self._session() as session:
            db_backfill = session.get(Backfill, backfill_id)
            if not db_backfill:
                raise NotFoundError(f"Backfill {backfill_id} not found")
            if db_backfill.status not in ("running", "queued"):
                raise ValueError(f"Backfill {backfill_id} is already {db_backfill.status}")

            cancel_backfill_runs(session, db_backfill)
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
            NotFoundError: If the backfill is not found.
        """
        with self._session() as session:
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
        with self._session() as session:
            statement = select(Backfill).where(Backfill.org_id == org_id).order_by(col(Backfill.created_at).desc())
            return list(session.exec(statement).all())

    def list_active_backfills(self, org_id: UUID) -> list[Backfill]:
        """List in-progress backfills for an organisation.

        Args:
            org_id: Organisation UUID.

        Returns:
            List of Backfill rows with status ``"running"`` or ``"queued"``.
        """
        with self._session() as session:
            statement = select(Backfill).where(
                Backfill.org_id == org_id,
                col(Backfill.status).in_(["running", "queued"]),
            )
            return list(session.exec(statement).all())


def cancel_backfill_runs(session: Session, db_backfill: Backfill) -> None:
    """Cancel a backfill's not-yet-dispatched runs and terminalize it.

    Part of the caller's transaction (the caller commits). ``skip_locked``
    leaves runs the worker is claiming right now to the worker — they are
    effectively dispatched and drain like any other in-flight run.
    """
    cancellable = session.exec(
        select(Run)
        .where(Run.backfill_id == db_backfill.id, col(Run.status).in_(["pending", "queued"]))
        .with_for_update(skip_locked=True)
    ).all()
    for db_run in cancellable:
        db_run.status = "canceled"
        session.add(db_run)

    db_backfill.status = "canceled"
    db_backfill.completed_at = datetime.now(timezone.utc)
    session.add(db_backfill)


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
    # Newest partition first, matching create_backfill's initial dispatch.
    pending_runs = session.exec(
        select(Run)
        .where(Run.backfill_id == backfill_id, Run.status == "pending")
        .order_by(col(Run.partition_date).desc())
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
