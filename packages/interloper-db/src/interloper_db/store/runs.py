"""Run, event, and backfill persistence."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import interloper as il
from interloper.errors import NotFoundError
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from sqlalchemy import Engine, func
from sqlmodel import Session, col, select

from interloper_db.models import Backfill, Component, Run
from interloper_db.session import commit, session_scope
from interloper_db.store.quotas import (
    QUOTA_MAX_BACKFILL_PARTITIONS,
    QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH,
    QuotaStore,
    UsageLedger,
)

logger = logging.getLogger(__name__)


class RunStore:
    """Store methods for runs and the backfills that batch them."""

    def __init__(self, engine: Engine, quotas: QuotaStore) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
            quotas: Quota gates it enforces through.
        """
        self._engine = engine
        self._quotas = quotas

    # -- Runs ------------------------------------------------------------------

    def create(
        self,
        org_id: UUID,
        *,
        component_id: UUID | None = None,
        partition_key: str | None = None,
    ) -> Run:
        """Create a single queued run.

        The target's kind must declare a workload (its anchor subclasses
        ``Workload``); the run records the workload's billability, and a
        non-billable run skips the run quota entirely.

        Args:
            org_id: Organisation UUID.
            component_id: Optional target component UUID (any kind whose
                anchor declares a workload).
            partition_key: Optional partition key (its shape carries the
                granularity, e.g. ``2026-08-21`` or ``2026-08``). A key matching
                no known shape is rejected by :meth:`TimePartition.from_key`.

        Returns:
            The created Run row.
        """
        if partition_key is not None:
            TimePartition.from_key(partition_key)
        with session_scope(self._engine) as session:
            billable = True
            if component_id is not None:
                _, anchor = self._target_anchor(session, component_id)
                billable = anchor.billable
            if billable:
                self._quotas.check(org_id, QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH)
            db_run = Run(
                org_id=org_id,
                component_id=component_id,
                partition_key=partition_key,
                status="queued",
                billable=billable,
            )
            session.add(db_run)
            commit(session)
            session.refresh(db_run)
            return db_run

    @staticmethod
    def _target_anchor(session: Session, component_id: UUID) -> tuple[str, type[il.Workload]]:
        """Resolve a run target's kind anchor, requiring it to declare a workload.

        Args:
            session: Open session the component row is read through.
            component_id: The target component UUID.

        Returns:
            The target's kind and its anchor class, narrowed to the
            workload contract.

        Raises:
            NotFoundError: If the component does not exist.
            ValueError: If the kind's anchor declares no workload.
        """
        db_component = session.get(Component, component_id)
        if not db_component:
            raise NotFoundError(f"Component {component_id} not found")
        anchor = il.KINDS[db_component.kind]
        if not issubclass(anchor, il.Workload):
            # A caller mistake, not a type bug: routes map ValueError to 400.
            raise ValueError(f"Components of kind '{db_component.kind}' cannot be run")  # noqa: TRY004
        return db_component.kind, anchor

    def get(self, run_id: UUID) -> Run:
        """Load a run by ID.

        Args:
            run_id: The run UUID.

        Returns:
            The Run row.

        Raises:
            NotFoundError: If the run is not found.
        """
        with session_scope(self._engine) as session:
            db_run = session.get(Run, run_id)
            if not db_run:
                raise NotFoundError(f"Run {run_id} not found")
            return db_run

    def list_all(
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
        with session_scope(self._engine) as session:
            statement = (
                select(Run)
                .where(*self._run_filters(org_id, component_id, backfill_id, status, after, before))
                .order_by(col(Run.created_at).desc())
                .offset(offset)
                .limit(limit)
            )
            return list(session.exec(statement).all())

    def count(
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
        with session_scope(self._engine) as session:
            statement = (
                select(func.count())
                .select_from(Run)
                .where(*self._run_filters(org_id, component_id, backfill_id, status, after, before))
            )
            return session.exec(statement).one()

    def complete(self, run_id: UUID, *, success: bool) -> Run:
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
        with session_scope(self._engine) as session:
            db_run = session.get(Run, run_id)
            if not db_run:
                raise NotFoundError(f"Run {run_id} not found")

            db_run.status = "success" if success else "failed"
            db_run.completed_at = datetime.now(timezone.utc)
            session.add(db_run)

            UsageLedger(session).settle_run(db_run, success=success)

            if db_run.component_id:
                db_component = session.get(Component, db_run.component_id)
                if db_component:
                    db_component.stamp_state(last_run_at=db_run.completed_at)
                    session.add(db_component)

            if db_run.backfill_id:
                self._advance_backfill(session, db_run.backfill_id, failed=not success)

            commit(session)
            return db_run

    def retry(self, run_id: UUID, *, scope: str = "all") -> Run:
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

        with session_scope(self._engine) as session:
            src = session.get(Run, run_id)
            if not src:
                raise NotFoundError(f"Run {run_id} not found")
            if src.status != "failed":
                raise ValueError(f"Run {run_id} is not failed (status={src.status!r}); only failed runs can be retried")

            if src.billable:
                self._quotas.check(src.org_id, QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH, subject="retry")
            db_run = Run(
                org_id=src.org_id,
                component_id=src.component_id,
                partition_key=src.partition_key,
                status="queued",
                retry_of=run_id,
                attempt=src.attempt + 1,
                retry_scope=scope,
                billable=src.billable,
            )
            session.add(db_run)
            commit(session)
            session.refresh(db_run)
            return db_run

    # -- Backfills -------------------------------------------------------------

    def create_backfill(
        self,
        org_id: UUID,
        *,
        component_id: UUID | None = None,
        start_key: str,
        end_key: str,
        concurrency: int = 1,
        fail_fast: bool = False,
    ) -> Backfill:
        """Create a backfill with one run per partition from start to end (inclusive).

        The bounds are partition keys whose shape carries the granularity
        (``2026-08-21``, ``2026-08``, ``2026``, ``2026-08-21T13``), so a
        monthly backfill is just two month keys. Runs are dispatched
        **newest partition first**: the latest ``concurrency`` of them are
        queued immediately and the rest are ``"pending"`` until earlier runs
        complete. The freshest data lands first, and an interrupted backfill
        keeps the recent window rather than the ancient tail.

        Args:
            org_id: Organisation UUID.
            component_id: Optional target component UUID.
            start_key: First partition's key.
            end_key: Last partition's key (inclusive).
            concurrency: Max runs in-flight at once.
            fail_fast: Cancel remaining runs on first failure.

        Returns:
            The created Backfill row with runs.

        Raises:
            ValueError: If a key matches no known shape, the two keys differ
                in granularity, or the range is inverted.
        """
        start = TimePartition.from_key(start_key)
        end = TimePartition.from_key(end_key)
        if start.granularity is not end.granularity:
            raise ValueError(
                f"Backfill bounds must share one granularity: {start_key!r} is a "
                f"{start.granularity.value} key but {end_key!r} is a {end.granularity.value} key"
            )
        window = TimePartitionWindow(start.value, end.value, start.granularity)
        span = window.partition_count()

        with session_scope(self._engine) as session:
            if component_id is not None:
                self._target_anchor(session, component_id)
            # Cron top-ups (a job's `lookback` window) are deliberately not
            # bounded here — they never pass through this method.
            self._quotas.check(org_id, QUOTA_MAX_BACKFILL_PARTITIONS, used=span)
            self._quotas.check(org_id, QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH, subject="backfill")
            db_backfill = Backfill(
                org_id=org_id,
                component_id=component_id,
                start_key=start_key,
                end_key=end_key,
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
            for index, value in enumerate(window.granularity.period_range(window.start, window.end)):
                db_run = Run(
                    org_id=org_id,
                    component_id=component_id,
                    backfill_id=db_backfill.id,
                    partition_key=window.granularity.format(value),
                    status="queued" if index >= first_queued else "pending",
                )
                session.add(db_run)

            db_backfill.partitions = span
            session.add(db_backfill)
            commit(session)
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
        with session_scope(self._engine) as session:
            db_backfill = session.get(Backfill, backfill_id)
            if not db_backfill:
                raise NotFoundError(f"Backfill {backfill_id} not found")
            if db_backfill.status not in ("running", "queued"):
                raise ValueError(f"Backfill {backfill_id} is already {db_backfill.status}")

            cancel_backfill_runs(session, db_backfill)
            commit(session)
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
        with session_scope(self._engine) as session:
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
        with session_scope(self._engine) as session:
            statement = select(Backfill).where(Backfill.org_id == org_id).order_by(col(Backfill.created_at).desc())
            return list(session.exec(statement).all())

    def list_active_backfills(self, org_id: UUID) -> list[Backfill]:
        """List in-progress backfills for an organisation.

        Args:
            org_id: Organisation UUID.

        Returns:
            List of Backfill rows with status ``"running"`` or ``"queued"``.
        """
        with session_scope(self._engine) as session:
            statement = select(Backfill).where(
                Backfill.org_id == org_id,
                col(Backfill.status).in_(["running", "queued"]),
            )
            return list(session.exec(statement).all())

    # -- Internals -------------------------------------------------------------

    @staticmethod
    def _run_filters(
        org_id: UUID,
        component_id: UUID | None,
        backfill_id: UUID | None,
        status: str | None,
        after: datetime | None = None,
        before: datetime | None = None,
    ) -> list[Any]:
        """The shared where-clauses of :meth:`RunStore.list_runs` / ``count_runs``.

        ``after``/``before`` select the runs whose execution *overlaps* the window
        — a run occupies ``[started_at, completed_at)``, left open-ended while it
        is still running. Runs that never started occupy no time and so fall
        outside every window.

        Args:
            org_id: Organisation whose runs are listed; always applied.
            component_id: Keep runs targeting this component; ``None`` applies
                no component filter.
            backfill_id: Keep runs belonging to this backfill; ``None`` applies
                no backfill filter.
            status: Keep runs in this status; ``None`` applies no status filter.
            after: Window start — keep runs still executing at or after this
                instant. ``None`` leaves the window open-ended in the past.
            before: Window end — keep runs that had started by this instant.
                ``None`` leaves the window open-ended in the future.

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

    @staticmethod
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
        # Newest partition first, matching create_backfill's initial dispatch. A
        # backfill is single-granularity, so the string order is the time order.
        pending_runs = session.exec(
            select(Run)
            .where(Run.backfill_id == backfill_id, Run.status == "pending")
            .order_by(col(Run.partition_key).desc())
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


def cancel_backfill_runs(session: Session, db_backfill: Backfill) -> None:
    """Cancel a backfill's not-yet-dispatched runs and terminalize it.

    Part of the caller's transaction (the caller commits). ``skip_locked``
    leaves runs the worker is claiming right now to the worker — they are
    effectively dispatched and drain like any other in-flight run.

    Args:
        session: Active database session (the caller commits).
        db_backfill: The backfill row to cancel, mutated in place along with
            its pending and queued runs.
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


