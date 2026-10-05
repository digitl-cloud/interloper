"""Backfill persistence: batches of runs over a range of partitions.

A backfill fans out one run per partition and admits them ``concurrency`` at
a time. Each partition reads as its run stack's latest attempt, so a partition
a retry healed counts as healed everywhere: in the progress counts, in the
failed list, and in the batch's own verdict.
"""

from __future__ import annotations

import builtins
from collections.abc import Sequence
from datetime import datetime, timezone
from uuid import UUID

from interloper.errors import ConfigError, ConflictError, NotFoundError
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from sqlalchemy import Engine
from sqlalchemy.orm import joinedload
from sqlmodel import Session, col, func, select

from interloper_db.models import (
    ACTIVE_BACKFILL_STATUSES,
    OPEN_RUN_STATUSES,
    Backfill,
    BackfillStatus,
    Component,
    Event,
    Run,
    RunStatus,
)
from interloper_db.session import commit, save, session_scope
from interloper_db.store.page import Page, PageQuery
from interloper_db.store.quotas import QUOTA_MAX_BACKFILL_PARTITIONS, QuotaStore
from interloper_db.store.runs import latest_attempt

# Reader queries eagerly join the target so its identity survives the session;
# not mapped on the relationship, since FOR UPDATE rejects outer joins.
BACKFILL_LOAD_OPTIONS = (joinedload(Backfill.target),)  # ty: ignore[invalid-argument-type]


class BackfillQuery(PageQuery):
    """Which of an organisation's backfills a listing reads.

    Attributes:
        status: Keep backfills in any of these statuses; ``None`` keeps every
            status.
    """

    status: list[BackfillStatus] | None = None


class BackfillStore:
    """Store methods for backfills and the batch accounting of their runs."""

    def __init__(self, engine: Engine, quotas: QuotaStore) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
            quotas: Quota gates it enforces through.
        """
        self._engine = engine
        self._quotas = quotas

    def get(self, backfill_id: UUID, *, org_id: UUID | None = None) -> Backfill:
        """Load a backfill by ID.

        Args:
            backfill_id: The backfill UUID.
            org_id: Organisation the row must belong to; a mismatch raises
                ``NotFoundError`` like an absent row, so a caller cannot learn
                that an id exists in another tenant. ``None`` accepts any
                organisation, for a caller that authorizes by the row's own
                ``org_id`` afterwards (the API) or serves every organisation
                (the scheduler).

        Returns:
            The Backfill row, its target loaded.

        Raises:
            NotFoundError: If the backfill is not found, or belongs to another
                organisation.
        """
        with session_scope(self._engine) as session:
            db_backfill = session.get(Backfill, backfill_id, options=BACKFILL_LOAD_OPTIONS)
            if not db_backfill or (org_id is not None and db_backfill.org_id != org_id):
                raise NotFoundError(f"Backfill {backfill_id} not found")
            return db_backfill

    def list(self, org_id: UUID, query: BackfillQuery) -> Page[Backfill]:
        """List an organisation's backfills, newest first.

        Args:
            org_id: Organisation UUID.
            query: Which statuses, and the window to read.

        Returns:
            The page of backfills, their targets loaded.
        """
        statement = (
            select(Backfill)
            .where(Backfill.org_id == org_id)
            .order_by(col(Backfill.created_at).desc(), col(Backfill.id))
            .options(*BACKFILL_LOAD_OPTIONS)
        )
        if query.status:
            statement = statement.where(col(Backfill.status).in_(query.status))
        with session_scope(self._engine) as session:
            return Page.read(session, statement, query)

    def hooks_pending(self) -> builtins.list[Backfill]:
        """Every organisation's finished backfills whose hooks have not been evaluated, oldest completion first.

        For the hook sweep; a canceled backfill is stamped at cancelation.

        Returns:
            The backfills, their targets loaded.
        """
        statement = (
            select(Backfill)
            .where(col(Backfill.status).not_in(ACTIVE_BACKFILL_STATUSES), col(Backfill.hooks_evaluated_at).is_(None))
            .order_by(col(Backfill.completed_at), col(Backfill.id))
            .options(*BACKFILL_LOAD_OPTIONS)
        )
        with session_scope(self._engine) as session:
            return [*session.exec(statement).all()]

    def attempts(self, backfill_id: UUID) -> builtins.list[Run]:
        """Every attempt of a backfill's runs, in the order they started; those that never started last.

        Args:
            backfill_id: The backfill UUID.

        Returns:
            The attempts.
        """
        statement = (
            select(Run)
            .where(Run.backfill_id == backfill_id)
            .order_by(col(Run.started_at).asc().nulls_last(), col(Run.created_at), col(Run.id))
        )
        with session_scope(self._engine) as session:
            return [*session.exec(statement).all()]

    def create(
        self,
        org_id: UUID,
        *,
        component_id: UUID,
        start_key: str,
        end_key: str,
        concurrency: int = 1,
        fail_fast: bool = False,
    ) -> Backfill:
        """Create a backfill with one run per partition from start to end (inclusive).

        The bounds are partition keys whose shape carries the granularity
        (``2026-08-21``, ``2026-08``, ``2026``, ``2026-08-21T13``), so a
        monthly backfill is just two month keys. The runs are fanned out
        newest partition first, ``concurrency`` of them queued at once. Like
        a single run, they record the target workload's billability, and a
        non-billable backfill skips the run quota.

        Args:
            org_id: Organisation UUID.
            component_id: The target component UUID.
            start_key: First partition's key.
            end_key: Last partition's key (inclusive).
            concurrency: Max runs in-flight at once.
            fail_fast: Cancel remaining runs on first failure.

        Returns:
            The created Backfill row, its target loaded.

        Raises:
            ConfigError: If the two keys differ in granularity or the range
                is inverted.
            NotFoundError: If the target component does not exist.
        """
        start = TimePartition.from_key(start_key)
        end = TimePartition.from_key(end_key)
        if start.granularity is not end.granularity:
            raise ConfigError(
                f"Backfill bounds must share one granularity: {start_key!r} is a "
                f"{start.granularity.value} key but {end_key!r} is a {end.granularity.value} key"
            )
        if end.value < start.value:
            raise ConfigError(f"Backfill range ends before it starts: {start_key!r} to {end_key!r}")
        window = TimePartitionWindow(start.value, end.value, start.granularity)
        span = window.partition_count()

        with session_scope(self._engine) as session:
            target = session.get(Component, component_id)
            if not target:
                raise NotFoundError(f"Component {component_id} not found")
            billable = target.run_billable()
            self._quotas.check(org_id, QUOTA_MAX_BACKFILL_PARTITIONS, used=span)
            self._quotas.admit_run(org_id, billable=billable, subject="backfill")
            db_backfill = Backfill(
                org_id=org_id,
                component_id=component_id,
                start_key=start_key,
                end_key=end_key,
                concurrency=concurrency,
                fail_fast=fail_fast,
                status=BackfillStatus.RUNNING,
                started_at=datetime.now(timezone.utc),
            )
            session.add(db_backfill)
            session.flush()
            self._create_runs(db_backfill, window, billable=billable)
            save(session, db_backfill, "target")
            return db_backfill

    def cancel(self, backfill_id: UUID) -> Backfill:
        """Cancel a backfill: runs not yet dispatched will never execute.

        Pending and queued runs flip to ``canceled``; runs already
        dispatched or running drain to their own terminal state (their late
        completions are no-ops on the now-terminal backfill).

        Args:
            backfill_id: The backfill UUID.

        Returns:
            The updated Backfill row, its target loaded.

        Raises:
            ConflictError: If the backfill is already terminal.
        """
        with session_scope(self._engine) as session:
            db_backfill = self._lock(backfill_id)
            if db_backfill.status not in ACTIVE_BACKFILL_STATUSES:
                raise ConflictError(f"Backfill {backfill_id} is already {db_backfill.status}")
            self._cancel(db_backfill)
            save(session, db_backfill, "target")
            return db_backfill

    def mark_hooks_evaluated(self, backfill_id: UUID) -> None:
        """Stamp a backfill's hooks as evaluated, so the hook sweep moves past it.

        Args:
            backfill_id: The backfill UUID.
        """
        with session_scope(self._engine) as session:
            db_backfill = self._lock(backfill_id)
            db_backfill.hooks_evaluated_at = datetime.now(timezone.utc)
            save(session, db_backfill)

    def advance(self, backfill_id: UUID, *, failed: bool) -> None:
        """Advance a backfill after one of its runs completes, in the completion's transaction.

        1. **Fail-fast**: if enabled and the run failed, cancel pending runs.
        2. **Finalize**: if nothing in-flight or pending, mark complete. In
           flight is queued, dispatched or running: a claimed run occupies its
           slot before its pod first writes. The verdict reads each stack's
           latest attempt, so an attempt a later one healed no longer condemns
           the batch. A queued successor still counts as in flight, which is
           what keeps the batch open while a retry waits out its backoff.
        3. **Advance**: promote next pending runs up to concurrency limit.

        Args:
            backfill_id: The backfill UUID.
            failed: Whether the completing run failed.
        """
        with session_scope(self._engine) as session:
            db_backfill = self._lock(backfill_id)
            if db_backfill.status in ACTIVE_BACKFILL_STATUSES:
                self._advance(db_backfill, failed=failed)
            commit(session)

    # -- Internals -------------------------------------------------------------

    def _lock(self, backfill_id: UUID) -> Backfill:
        """Load a backfill for a write, holding its row for the rest of the transaction.

        Args:
            backfill_id: The backfill UUID.

        Returns:
            The backfill row.

        Raises:
            NotFoundError: If the backfill is not found.
        """
        with session_scope(self._engine) as session:
            db_backfill = session.exec(select(Backfill).where(Backfill.id == backfill_id).with_for_update()).first()
            if db_backfill is None:
                raise NotFoundError(f"Backfill {backfill_id} not found")
            return db_backfill

    def _advance(self, db_backfill: Backfill, *, failed: bool) -> None:
        """Advance an active backfill after one of its runs completes, in :meth:`advance`'s transaction.

        Args:
            db_backfill: The backfill row, locked.
            failed: Whether the completing run failed.
        """
        with session_scope(self._engine) as session:
            self._advance_in(session, db_backfill, failed=failed)

    def _advance_in(self, session: Session, db_backfill: Backfill, *, failed: bool) -> None:
        """The three steps of :meth:`_advance`: fail fast, finalize, promote.

        Args:
            session: The open session.
            db_backfill: The backfill row, locked.
            failed: Whether the completing run failed.
        """
        backfill_id = db_backfill.id
        if db_backfill.fail_fast and failed:
            for pending_run in session.exec(
                select(Run).where(Run.backfill_id == backfill_id, Run.status == RunStatus.PENDING)
            ).all():
                pending_run.cancel()
                session.add(pending_run)
            db_backfill.status = BackfillStatus.FAILED
            db_backfill.completed_at = datetime.now(timezone.utc)
            session.add(db_backfill)
            return

        in_flight_count = session.exec(
            select(func.count())
            .select_from(Run)
            .where(Run.backfill_id == backfill_id, col(Run.status).in_(OPEN_RUN_STATUSES))
        ).one()
        # Newest partition first, matching the initial fan-out. A backfill is
        # single-granularity, so the string order is the time order.
        pending_runs = session.exec(
            select(Run)
            .where(Run.backfill_id == backfill_id, Run.status == RunStatus.PENDING)
            .order_by(col(Run.partition_key).desc())
        ).all()

        if in_flight_count == 0 and not pending_runs:
            any_failed = session.exec(
                select(Run.id).where(Run.backfill_id == backfill_id, Run.status == RunStatus.FAILED, latest_attempt())
            ).first()
            db_backfill.status = BackfillStatus.FAILED if any_failed else BackfillStatus.SUCCESS
            db_backfill.completed_at = datetime.now(timezone.utc)
            session.add(db_backfill)
            return

        for pending_run in pending_runs[: max(0, db_backfill.concurrency - in_flight_count)]:
            pending_run.status = RunStatus.QUEUED
            session.add(pending_run)

    def run_counts(self, backfill_ids: Sequence[UUID]) -> dict[UUID, dict[str, int]]:
        """Count each backfill's partitions by their latest attempt's status, in one query.

        A partition reads as its stack's latest attempt, so one a retry healed
        counts as a success and one waiting out a retry's backoff as queued.

        Args:
            backfill_ids: The backfills to count, typically one listing.

        Returns:
            Per backfill, its partition count per status; a backfill with no
            runs is absent.
        """
        if not backfill_ids:
            return {}
        statement = (
            select(col(Run.backfill_id), col(Run.status), func.count())
            .where(col(Run.backfill_id).in_(backfill_ids), latest_attempt())
            .group_by(col(Run.backfill_id), col(Run.status))
        )
        counts: dict[UUID, dict[str, int]] = {}
        with session_scope(self._engine) as session:
            for backfill_id, status, count in session.exec(statement).all():
                assert backfill_id is not None
                counts.setdefault(backfill_id, {})[status] = count
        return counts

    def failed_partitions(self, backfill_id: UUID) -> builtins.list[tuple[str, str | None]]:
        """A backfill's failed partitions, newest first, each with its recorded error.

        A partition reads as its stack's latest attempt, so one a retry healed
        is absent. The error is what that attempt's ``run_failed`` event
        recorded; ``None`` when nothing was.

        Args:
            backfill_id: The backfill UUID.

        Returns:
            ``(partition_key, error)`` pairs, newest partition first.
        """
        statement = (
            select(Run)
            .where(Run.backfill_id == backfill_id, Run.status == RunStatus.FAILED, latest_attempt())
            .order_by(col(Run.partition_key).desc())
        )
        with session_scope(self._engine) as session:
            failed = session.exec(statement).all()
            return [(run.partition_key or "", self._recorded_error(run.id)) for run in failed]

    def _recorded_error(self, run_id: UUID) -> str | None:
        """The error a run's newest ``run_failed`` event recorded.

        Args:
            run_id: The run whose failure is read.

        Returns:
            The error text, or ``None`` when no failure event carries one.
        """
        statement = (
            select(Event.error)
            .where(Event.run_id == run_id, Event.event_type == "run_failed", col(Event.error).is_not(None))
            .order_by(col(Event.timestamp).desc())
        )
        with session_scope(self._engine) as session:
            return session.exec(statement).first()

    def _cancel(self, db_backfill: Backfill) -> None:
        """Cancel a backfill's not-yet-dispatched runs and terminalize it.

        ``skip_locked`` leaves runs the worker is claiming right now to the
        worker: they are effectively dispatched and drain like any other
        in-flight run. A canceled backfill fires no hooks, so it is stamped
        evaluated with its runs.

        Args:
            db_backfill: The backfill row to cancel, mutated in place along with
                its pending and queued runs.
        """
        statement = (
            select(Run)
            .where(Run.backfill_id == db_backfill.id, col(Run.status).in_([RunStatus.PENDING, RunStatus.QUEUED]))
            .with_for_update(skip_locked=True)
        )
        with session_scope(self._engine) as session:
            for db_run in session.exec(statement).all():
                db_run.cancel()
                session.add(db_run)
            db_backfill.status = BackfillStatus.CANCELED
            db_backfill.completed_at = db_backfill.hooks_evaluated_at = datetime.now(timezone.utc)
            session.add(db_backfill)

    def _create_runs(self, db_backfill: Backfill, window: TimePartitionWindow, *, billable: bool) -> None:
        """Create a backfill's runs: one per partition, the newest ``concurrency`` of them queued.

        Part of the caller's transaction, on a backfill row already flushed so
        the runs can reference it. Rows are created oldest first, so a runs
        list ordered by ``created_at`` desc keeps the newest partition on top,
        while the *newest* ``concurrency`` of them are ``queued`` and the rest
        wait ``pending``; :meth:`_advance` promotes in the same newest-first
        order, so the freshest data lands first and an interrupted backfill
        keeps the recent window rather than the ancient tail.

        Args:
            db_backfill: The flushed backfill row the runs belong to; its
                ``partitions`` count is stamped here.
            window: The partitions the backfill covers.
            billable: Whether the runs count against the run quota, as the
                target's workload declares.
        """
        span = window.partition_count()
        first_queued = max(0, span - db_backfill.concurrency)
        with session_scope(self._engine) as session:
            for index, value in enumerate(window.granularity.period_range(window.start, window.end)):
                session.add(
                    Run(
                        org_id=db_backfill.org_id,
                        component_id=db_backfill.component_id,
                        backfill_id=db_backfill.id,
                        partition_key=window.granularity.format(value),
                        status=RunStatus.QUEUED if index >= first_queued else RunStatus.PENDING,
                        billable=billable,
                    )
                )
            db_backfill.partitions = span
            session.add(db_backfill)
