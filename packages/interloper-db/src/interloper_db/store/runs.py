"""Run, event, and backfill persistence."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import interloper as il
from interloper.errors import NotFoundError
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from sqlalchemy import Engine, exists, func
from sqlalchemy.orm import aliased, joinedload
from sqlmodel import Session, col, select

from interloper_db.models import Backfill, Component, Event, Run
from interloper_db.session import commit, session_scope
from interloper_db.store.quotas import (
    QUOTA_MAX_BACKFILL_PARTITIONS,
    QuotaStore,
    UsageLedger,
)

logger = logging.getLogger(__name__)

# Reader queries eagerly join the target so its identity survives the session.
# Deliberately not mapped on the relationship itself: locking queries (the
# queue claim, backfill cancelation) select these tables with FOR UPDATE,
# which rejects outer joins.
RUN_LOAD_OPTIONS = (joinedload(Run.target),)  # ty: ignore[invalid-argument-type]
BACKFILL_LOAD_OPTIONS = (joinedload(Backfill.target),)  # ty: ignore[invalid-argument-type]

TERMINAL_RUN_STATUSES = frozenset({"success", "failed", "canceled"})

_ACTIVE_BACKFILL_STATUSES = ("running", "queued")

RUN_SORT_FIELDS = frozenset({"id", "partition_key", "status", "created_at", "started_at", "completed_at"})


def partition_key_range(start_key: str, end_key: str) -> list[Any]:
    """Filter runs to the partition keys from *start_key* to *end_key*, inclusive.

    Keys of one granularity sort as strings, but keys of another can fall
    between them (``2026-08-21T13`` sorts between two day keys), so the range
    also requires the bounds' key length.

    Args:
        start_key: First partition key.
        end_key: Last partition key; the caller ensures it shares the start
            key's granularity.

    Returns:
        Filter expressions over ``runs.partition_key``.
    """
    key = col(Run.partition_key)
    return [key >= start_key, key <= end_key, func.length(key) == len(start_key)]


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
            billable = self._target_billable(session, component_id)
            self._quotas.admit_run(org_id, billable=billable)
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
            _ = db_run.target  # load before the session closes; readers reach it detached
            return db_run

    @staticmethod
    def _target_billable(session: Session, component_id: UUID | None) -> bool:
        """Resolve whether runs of a target count against the run quota.

        The target's kind must declare a workload, whose ``billable`` decides;
        an untargeted run is billable.

        Args:
            session: Open session the component row is read through.
            component_id: The target component UUID, or None for an
                untargeted run.

        Returns:
            The billability the target's workload declares.

        Raises:
            NotFoundError: If the component does not exist.
            ValueError: If the kind's anchor declares no workload.
        """
        if component_id is None:
            return True
        db_component = session.get(Component, component_id)
        if not db_component:
            raise NotFoundError(f"Component {component_id} not found")
        anchor = il.KINDS[db_component.kind]
        if not issubclass(anchor, il.Workload):
            # A caller mistake, not a type bug: routes map ValueError to 400.
            raise ValueError(f"Components of kind '{db_component.kind}' cannot be run")  # noqa: TRY004
        return anchor.billable

    def get(self, run_id: UUID, *, org_id: UUID | None = None) -> Run:
        """Load a run by ID.

        Args:
            run_id: The run UUID.
            org_id: Organisation the run must belong to (``None`` accepts
                any); a mismatch raises ``NotFoundError`` like an absent row,
                so a caller cannot learn that an id exists in another tenant.

        Returns:
            The Run row.

        Raises:
            NotFoundError: If the run is not found, or belongs to another
                organisation.
        """
        with session_scope(self._engine) as session:
            db_run = session.get(Run, run_id, options=RUN_LOAD_OPTIONS)
            if not db_run or (org_id is not None and db_run.org_id != org_id):
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
        completed_after: datetime | None = None,
        completed_before: datetime | None = None,
        q: str | None = None,
        component_kind: str | None = None,
        component_key: str | None = None,
        root_run_id: UUID | None = None,
        partition_from: str | None = None,
        partition_to: str | None = None,
        all_attempts: bool = False,
        sort: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Run]:
        """List one row per stack, one stack's attempts, or every attempt.

        A stack is one piece of work, so a listing shows its **latest
        attempt** and every filter reads that attempt: a stack whose first
        attempt failed and whose second succeeded is a success, which is what
        a reader means by "failed runs". Passing *root_run_id* asks for one
        stack instead, and returns its attempts newest first; *all_attempts*
        keeps every attempt of every stack, which is what statistics over
        durations and retries read.

        Args:
            org_id: Organisation UUID.
            component_id: Optional target component filter.
            backfill_id: Optional backfill filter.
            status: Optional status filter.
            after: Keep runs still executing at or after this instant.
            before: Keep runs that had started by this instant.
            completed_after: Keep runs that completed at or after this instant.
            completed_before: Keep runs that completed at or before this instant.
            q: Keep runs whose target's name or key contains this, case-insensitively.
            component_kind: Keep runs whose target is of this kind.
            component_key: Keep runs whose target is of this type (catalog key).
            root_run_id: List this stack's attempts rather than one row per stack.
            partition_from: With *partition_to*, keep runs whose partition key
                lies in that inclusive range (see :func:`partition_key_range`).
            partition_to: Last partition key of that range.
            all_attempts: Keep every attempt rather than each stack's latest.
            sort: A field of :data:`RUN_SORT_FIELDS` to order by, ``-``-prefixed
                for descending (any other field raises ``ValueError``); None
                keeps the default (newest first, or a stack's latest attempt
                first).
            limit: Max results (default 50).
            offset: Pagination offset.

        Returns:
            List of Run rows.
        """
        with session_scope(self._engine) as session:
            filters = self._run_filters(
                org_id,
                component_id,
                backfill_id,
                status,
                after,
                before,
                completed_after=completed_after,
                completed_before=completed_before,
                q=q,
                component_kind=component_kind,
                component_key=component_key,
                root_run_id=root_run_id,
                partition_from=partition_from,
                partition_to=partition_to,
            )
            if root_run_id is None and not all_attempts:
                filters.append(self._latest_attempt_only())
            statement = (
                select(Run)
                .where(*filters)
                .order_by(*_run_order(sort, root_run_id))
                .offset(offset)
                .limit(limit)
                .options(*RUN_LOAD_OPTIONS)
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
        completed_after: datetime | None = None,
        completed_before: datetime | None = None,
        q: str | None = None,
        component_kind: str | None = None,
        component_key: str | None = None,
        root_run_id: UUID | None = None,
        partition_from: str | None = None,
        partition_to: str | None = None,
        all_attempts: bool = False,
    ) -> int:
        """Count runs matching the same filters as :meth:`list_all`.

        Args:
            org_id: Organisation UUID.
            component_id: Optional target component filter.
            backfill_id: Optional backfill filter.
            status: Optional status filter.
            after: Keep runs still executing at or after this instant.
            before: Keep runs that had started by this instant.
            completed_after: Keep runs that completed at or after this instant.
            completed_before: Keep runs that completed at or before this instant.
            q: Keep runs whose target's name or key contains this, case-insensitively.
            component_kind: Keep runs whose target is of this kind.
            component_key: Keep runs whose target is of this type (catalog key).
            root_run_id: Count this stack's attempts rather than one per stack.
            partition_from: With *partition_to*, count runs whose partition key
                lies in that inclusive range.
            partition_to: Last partition key of that range.
            all_attempts: Count every attempt rather than each stack's latest.

        Returns:
            Total number of matching runs (ignoring limit/offset).
        """
        with session_scope(self._engine) as session:
            filters = self._run_filters(
                org_id,
                component_id,
                backfill_id,
                status,
                after,
                before,
                completed_after=completed_after,
                completed_before=completed_before,
                q=q,
                component_kind=component_kind,
                component_key=component_key,
                root_run_id=root_run_id,
                partition_from=partition_from,
                partition_to=partition_to,
            )
            if root_run_id is None and not all_attempts:
                filters.append(self._latest_attempt_only())
            return session.exec(select(func.count()).select_from(Run).where(*filters)).one()

    def complete(self, run_id: UUID, *, success: bool) -> Run:
        """Mark a run as completed and advance its backfill if applicable.

        Also stamps ``last_run_at`` and ``last_run_status`` on the target
        component's machine-owned state — this is the single terminal path
        every run takes (scheduled, manual, retried), so the component's
        "last run" reflects all of them.

        A failure also queues its own next attempt when the target's policy
        allows one, before the backfill advances so the batch's in-flight
        count sees the successor and does not finalize early.

        Args:
            run_id: The run UUID.
            success: Whether the run succeeded.

        Returns:
            The updated Run row.

        Raises:
            NotFoundError: If the run is not found.
            ValueError: If the run is already terminal, so a late completion
                (the reaper's, after a pod finally started) cannot overwrite
                the verdict or queue a retry of work that succeeded.
        """
        with session_scope(self._engine) as session:
            db_run = session.get(Run, run_id)
            if not db_run:
                raise NotFoundError(f"Run {run_id} not found")
            if db_run.status in TERMINAL_RUN_STATUSES:
                raise ValueError(f"Run {run_id} is already {db_run.status}")

            db_run.status = "success" if success else "failed"
            db_run.completed_at = datetime.now(timezone.utc)
            session.add(db_run)

            UsageLedger(session).settle_run(db_run, success=success)

            if db_run.component_id:
                db_component = session.get(Component, db_run.component_id)
                if db_component:
                    db_component.stamp_state(last_run_at=db_run.completed_at, last_run_status=db_run.status)
                    session.add(db_component)

            if not success:
                self._plan_retry(session, db_run)

            if db_run.backfill_id:
                self._advance_backfill(session, db_run.backfill_id, failed=not success)

            commit(session)
            return db_run

    def retry(self, run_id: UUID, *, scope: str = "all") -> Run:
        """Queue the next attempt of a failed run's stack.

        A stack has one head, its latest attempt, and every retry continues
        from it: retrying an earlier attempt retries the stack, so attempt
        numbers stay unique and the hook evaluator's "failed with a
        successor" gate keeps meaning what it says. The head must have
        failed: a stack whose head succeeded is healed, and one whose head
        is still queued or running already has its next attempt. The new
        run is created outside any backfill so backfill accounting is
        unaffected.

        Args:
            run_id: The failed run to retry, any attempt of its stack.
            scope: ``"all"`` to re-run the whole DAG, or ``"failed"`` to
                re-run only the previously failed/cancelled assets.

        Returns:
            The newly created, queued Run row.

        Raises:
            NotFoundError: If the run is not found.
            ValueError: If ``scope`` is invalid, the run has not failed, or
                the stack's latest attempt is not a failure.
        """
        if scope not in ("all", "failed"):
            raise ValueError(f"Invalid retry scope: {scope!r} (expected 'all' or 'failed')")

        with session_scope(self._engine) as session:
            src = session.get(Run, run_id)
            if not src:
                raise NotFoundError(f"Run {run_id} not found")
            if src.status != "failed":
                raise ValueError(f"Run {run_id} is not failed (status={src.status!r}); only failed runs can be retried")
            head = session.exec(
                select(Run).where(Run.root_run_id == src.root_run_id).order_by(col(Run.attempt).desc())
            ).first()
            assert head is not None
            if head.status != "failed":
                raise ValueError(
                    f"Run {run_id} is attempt {src.attempt} of a stack whose latest attempt {head.attempt} "
                    f"is {head.status!r}; only a stack whose latest attempt failed can be retried"
                )

            self._quotas.admit_run(head.org_id, billable=head.billable, subject="retry")
            db_run = Run(
                root_run_id=head.root_run_id,
                org_id=head.org_id,
                component_id=head.component_id,
                partition_key=head.partition_key,
                status="queued",
                retry_of=head.id,
                attempt=head.attempt + 1,
                retry_scope=scope,
                billable=head.billable,
            )
            session.add(db_run)
            commit(session)
            session.refresh(db_run)
            _ = db_run.target  # load before the session closes; readers reach it detached
            return db_run

    @staticmethod
    def _retry_policy(session: Session, db_run: Run) -> il.RetryPolicy | None:
        """The run-level policy in force for a run.

        A job's declared policy governs its runs, and nothing else does: a
        source's or an asset's own ``retry`` is an operation budget, and
        reading it here would spend an operation's attempts on whole runs.
        There is no instance-wide default either, so a run whose target
        declares nothing is attempted once. ``config`` is a plain JSON
        column, so this is one row read and no hydration.

        Args:
            session: Open session the target row is read through.
            db_run: The run whose policy is resolved.

        Returns:
            The policy, or ``None`` when the target declares none.
        """
        if db_run.component_id is None:
            return None
        db_component = session.get(Component, db_run.component_id)
        if db_component is None or db_component.kind != "job":
            return None
        declared = (db_component.config or {}).get("retry")
        return il.RetryPolicy.model_validate(declared) if declared else None

    def _plan_retry(self, session: Session, db_run: Run) -> Run | None:
        """Queue the next attempt of a failed run, when its budget allows one.

        Called from the single terminal path, in the transaction that marks
        the run failed, so a doomed attempt never looks final to anything
        reading the table — which is what lets the hook evaluator gate on the
        successor's existence without knowing anything about budgets. The
        successor stays in its predecessor's backfill and stack, and re-runs
        only what failed.

        The quota is deliberately not checked here: dispatch is the
        authoritative gate and cancels an over-quota run at claim time, like
        any other run.

        Args:
            session: Open session the successor is written through.
            db_run: The run that just failed.

        Returns:
            The queued successor, or ``None`` when nothing is retried.
        """
        policy = self._retry_policy(session, db_run)
        if policy is None or not policy.allows(db_run.attempt + 1):
            return None

        successor = Run(
            org_id=db_run.org_id,
            component_id=db_run.component_id,
            backfill_id=db_run.backfill_id,
            partition_key=db_run.partition_key,
            status="queued",
            scheduled_for=datetime.now(timezone.utc) + timedelta(seconds=policy.delay_before(db_run.attempt + 1)),
            retry_of=db_run.id,
            root_run_id=db_run.root_run_id,
            attempt=db_run.attempt + 1,
            retry_scope="failed",
            billable=db_run.billable,
        )
        session.add(successor)
        session.flush()
        logger.info("Queued attempt %d of run stack %s", successor.attempt, successor.root_run_id)
        return successor

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
        monthly backfill is just two month keys. The runs are fanned out by
        :func:`create_backfill_runs`: newest partition first, ``concurrency``
        of them queued at once. Like :meth:`create`, the runs record the
        target workload's billability, and a non-billable backfill skips the
        run quota.

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
            billable = self._target_billable(session, component_id)
            self._quotas.check(org_id, QUOTA_MAX_BACKFILL_PARTITIONS, used=span)
            self._quotas.admit_run(org_id, billable=billable, subject="backfill")
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
            create_backfill_runs(session, db_backfill, window, billable=billable)
            commit(session)
            session.refresh(db_backfill)
            _ = db_backfill.target  # load before the session closes; readers reach it detached
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
            if db_backfill.status not in _ACTIVE_BACKFILL_STATUSES:
                raise ValueError(f"Backfill {backfill_id} is already {db_backfill.status}")

            cancel_backfill_runs(session, db_backfill)
            commit(session)
            session.refresh(db_backfill)
            _ = db_backfill.target  # load before the session closes; readers reach it detached
            return db_backfill

    def get_backfill(self, backfill_id: UUID, *, org_id: UUID | None = None) -> Backfill:
        """Load a backfill by ID.

        Args:
            backfill_id: The backfill UUID.
            org_id: Organisation the backfill must belong to (``None`` accepts
                any); a mismatch raises ``NotFoundError`` like an absent row.

        Returns:
            The Backfill row.

        Raises:
            NotFoundError: If the backfill is not found, or belongs to another
                organisation.
        """
        with session_scope(self._engine) as session:
            db_backfill = session.get(Backfill, backfill_id, options=BACKFILL_LOAD_OPTIONS)
            if not db_backfill or (org_id is not None and db_backfill.org_id != org_id):
                raise NotFoundError(f"Backfill {backfill_id} not found")
            return db_backfill

    def list_backfills(
        self, org_id: UUID, *, active_only: bool = False, limit: int | None = None, offset: int = 0
    ) -> list[Backfill]:
        """List an organisation's backfills, newest first.

        Args:
            org_id: Organisation UUID.
            active_only: Keep only backfills still ``"queued"`` or ``"running"``.
            limit: Max results; ``None`` lists them all.
            offset: Pagination offset.

        Returns:
            List of Backfill rows.
        """
        with session_scope(self._engine) as session:
            statement = (
                select(Backfill)
                .where(*self._backfill_filters(org_id, active_only))
                .order_by(col(Backfill.created_at).desc())
                .offset(offset)
                .limit(limit)
                .options(*BACKFILL_LOAD_OPTIONS)
            )
            return list(session.exec(statement).all())

    def count_backfills(self, org_id: UUID, *, active_only: bool = False) -> int:
        """Count backfills matching the same filters as :meth:`list_backfills`.

        Args:
            org_id: Organisation UUID.
            active_only: Count only backfills still ``"queued"`` or ``"running"``.

        Returns:
            Total number of matching backfills (ignoring limit/offset).
        """
        with session_scope(self._engine) as session:
            statement = select(func.count()).select_from(Backfill).where(*self._backfill_filters(org_id, active_only))
            return session.exec(statement).one()

    def failed_partitions(self, backfill_id: UUID) -> list[tuple[str, str | None]]:
        """A backfill's failed partitions, newest first, each with its recorded error.

        A partition reads as its stack's latest attempt, so one a retry healed
        is absent. The error is what that attempt's ``run_failed`` event
        recorded; ``None`` when nothing was.

        Args:
            backfill_id: The backfill UUID.

        Returns:
            ``(partition_key, error)`` pairs, newest partition first.
        """
        latest = (
            select(col(Run.root_run_id), func.max(col(Run.attempt)).label("attempt"))
            .where(Run.backfill_id == backfill_id)
            .group_by(col(Run.root_run_id))
            .subquery()
        )
        statement = (
            select(Run)
            .join(
                latest,
                onclause=(col(Run.root_run_id) == latest.c.root_run_id) & (col(Run.attempt) == latest.c.attempt),
            )
            .where(Run.backfill_id == backfill_id, Run.status == "failed")
            .order_by(col(Run.partition_key).desc())
        )
        with session_scope(self._engine) as session:
            failed = session.exec(statement).all()
            return [(run.partition_key or "", self._recorded_error(session, run.id)) for run in failed]

    @staticmethod
    def _recorded_error(session: Session, run_id: UUID) -> str | None:
        """The error a run's newest ``run_failed`` event recorded.

        Args:
            session: Open session the event is read through.
            run_id: The run whose failure is read.

        Returns:
            The error text, or ``None`` when no failure event carries one.
        """
        return session.exec(
            select(Event.error)
            .where(Event.run_id == run_id, Event.event_type == "run_failed", col(Event.error).is_not(None))
            .order_by(col(Event.timestamp).desc())
        ).first()

    def count_backfill_runs(self, backfill_ids: Sequence[UUID]) -> dict[UUID, dict[str, int]]:
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
        latest = (
            select(col(Run.root_run_id), func.max(col(Run.attempt)).label("attempt"))
            .where(col(Run.backfill_id).in_(backfill_ids))
            .group_by(col(Run.root_run_id))
            .subquery()
        )
        statement = (
            select(col(Run.backfill_id), col(Run.status), func.count())
            .join(
                latest,
                onclause=(col(Run.root_run_id) == latest.c.root_run_id) & (col(Run.attempt) == latest.c.attempt),
            )
            .where(col(Run.backfill_id).in_(backfill_ids))
            .group_by(col(Run.backfill_id), col(Run.status))
        )
        counts: dict[UUID, dict[str, int]] = {}
        with session_scope(self._engine) as session:
            for backfill_id, status, count in session.exec(statement).all():
                assert backfill_id is not None
                counts.setdefault(backfill_id, {})[status] = count
        return counts

    def latest_by_target(self, org_id: UUID, *, component_kind: str | None = None) -> list[Run]:
        """The most recent attempt of every target.

        What a reader means by "the last time this job ran": the most recently
        created attempt targeting the component, whatever stack it belongs to.
        That attempt is necessarily the latest of its own stack, so no
        per-stack reduction is needed. The runs of one backfill share their
        creation instant, so a tie goes to the later partition key, then to
        the greater id. Runs whose target was deleted have no target to
        report on and are left out.

        Args:
            org_id: Organisation UUID.
            component_kind: Keep targets of this kind; ``None`` keeps every kind.

        Returns:
            One run per target, newest first, with the target loaded.
        """
        rank = (
            func.row_number()
            .over(
                partition_by=col(Run.component_id),
                order_by=(
                    col(Run.created_at).desc(),
                    col(Run.partition_key).desc().nulls_last(),
                    col(Run.id).desc(),
                ),
            )
            .label("rank")
        )
        ranked = (
            select(col(Run.id), rank)
            .where(Run.org_id == org_id, col(Run.component_id).is_not(None))
            .subquery()
        )
        filters: list[Any] = [col(Run.id).in_(select(ranked.c.id).where(ranked.c.rank == 1))]
        if component_kind:
            filters.append(col(Run.target).has(col(Component.kind) == component_kind))
        with session_scope(self._engine) as session:
            statement = select(Run).where(*filters).order_by(col(Run.created_at).desc()).options(*RUN_LOAD_OPTIONS)
            return list(session.exec(statement).all())

    # -- Internals -------------------------------------------------------------

    @staticmethod
    def _backfill_filters(org_id: UUID, active_only: bool) -> list[Any]:
        """The shared where-clauses of :meth:`list_backfills` / :meth:`count_backfills`.

        Args:
            org_id: Organisation whose backfills are listed; always applied.
            active_only: Keep only backfills still ``"queued"`` or ``"running"``.

        Returns:
            Filter expressions for the given criteria.
        """
        filters: list[Any] = [Backfill.org_id == org_id]
        if active_only:
            filters.append(col(Backfill.status).in_(_ACTIVE_BACKFILL_STATUSES))
        return filters

    @staticmethod
    def _latest_attempt_only() -> Any:
        """Keep only each stack's latest attempt.

        An attempt is its stack's latest when no attempt of the same stack
        carries a higher number: a per-row probe of ``ix_runs_root_run_id``
        for a narrow listing, one anti-join over the organisation's runs for a
        wide one, and never an aggregate the planner has to estimate. Keyed on
        the attempt number rather than on ``retry_of``, so a stack two
        concurrent retries branched still reads as its highest attempts.

        The probe matches the organisation, which keeps it inside the tenant,
        but none of the caller's other filters on purpose: every attempt of a
        stack shares its org and its target, and narrowing the probe would
        answer "the latest *failed* attempt" rather than "the stacks whose
        latest attempt failed".

        Returns:
            A filter expression selecting the latest attempt of each stack.
        """
        later = aliased(Run)
        return ~exists().where(
            col(later.org_id) == col(Run.org_id),
            col(later.root_run_id) == col(Run.root_run_id),
            col(later.attempt) > col(Run.attempt),
        )

    @staticmethod
    def _run_filters(
        org_id: UUID,
        component_id: UUID | None,
        backfill_id: UUID | None,
        status: str | None,
        after: datetime | None = None,
        before: datetime | None = None,
        *,
        completed_after: datetime | None = None,
        completed_before: datetime | None = None,
        q: str | None = None,
        component_kind: str | None = None,
        component_key: str | None = None,
        root_run_id: UUID | None = None,
        partition_from: str | None = None,
        partition_to: str | None = None,
    ) -> list[Any]:
        """The shared where-clauses of :meth:`RunStore.list_all` / :meth:`RunStore.count`.

        ``after``/``before`` select the runs whose execution *overlaps* the window
        — a run occupies ``[started_at, completed_at)``, left open-ended while it
        is still running. Runs that never started occupy no time and so fall
        outside every window. ``completed_after``/``completed_before`` read
        the completion instant alone, so they keep a run that ended without
        ever starting (failed or canceled in the queue) and drop every run
        not yet completed.

        The target filters read the target component through the relationship,
        so a run whose target was deleted matches none of them.

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
            completed_after: Keep runs that completed at or after this
                instant; ``None`` applies no lower completion bound.
            completed_before: Keep runs that completed at or before this
                instant; ``None`` applies no upper completion bound.
            q: Keep runs whose target's name or key contains this text,
                case-insensitively; ``None`` applies no search.
            component_kind: Keep runs whose target is of this kind; ``None``
                applies no kind filter.
            component_key: Keep runs whose target is of this type (catalog
                key); ``None`` applies no type filter.
            root_run_id: Keep the attempts of this stack; ``None`` applies no
                stack filter.
            partition_from: With *partition_to*, keep runs whose partition key
                lies in that inclusive range; either alone applies no filter.
            partition_to: Last partition key of that range.

        Returns:
            Filter expressions for the given criteria.
        """
        filters: list[Any] = [Run.org_id == org_id]
        target = col(Run.target)
        if q:
            filters.append(
                target.has(
                    col(Component.name).icontains(q, autoescape=True)
                    | col(Component.key).icontains(q, autoescape=True)
                )
            )
        if component_kind:
            filters.append(target.has(col(Component.kind) == component_kind))
        if component_key:
            filters.append(target.has(col(Component.key) == component_key))
        if component_id:
            filters.append(Run.component_id == component_id)
        if backfill_id:
            filters.append(Run.backfill_id == backfill_id)
        if root_run_id:
            filters.append(Run.root_run_id == root_run_id)
        if partition_from is not None and partition_to is not None:
            filters.extend(partition_key_range(partition_from, partition_to))
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
        if completed_after is not None:
            filters.append(col(Run.completed_at) >= completed_after)
        if completed_before is not None:
            filters.append(col(Run.completed_at) <= completed_before)
        return filters

    @staticmethod
    def _advance_backfill(session: Session, backfill_id: UUID, *, failed: bool) -> None:
        """Advance a backfill after a run completes.

        1. **Fail-fast**: if enabled and the run failed, cancel pending runs.
        2. **Finalize**: if nothing in-flight or pending, mark complete. In
           flight is queued, dispatched or running: a claimed run occupies its
           slot before its pod first writes. The verdict reads each stack's
           latest attempt, so an attempt a later one healed no longer condemns
           the batch. A queued successor still counts as in flight, which is
           what keeps the batch open while a retry waits out its backoff.
        3. **Advance**: promote next pending runs up to concurrency limit.

        Args:
            session: Active database session (caller commits).
            backfill_id: The backfill UUID.
            failed: Whether the completing run failed.
        """
        db_backfill = session.get(Backfill, backfill_id)
        if not db_backfill or db_backfill.status not in _ACTIVE_BACKFILL_STATUSES:
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
                    col(Run.status).in_(["queued", "dispatched", "running"]),
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
            latest = (
                select(col(Run.root_run_id), func.max(col(Run.attempt)).label("attempt"))
                .where(Run.backfill_id == backfill_id)
                .group_by(col(Run.root_run_id))
                .subquery()
            )
            any_failed = session.exec(
                select(Run)
                .join(
                    latest,
                    onclause=(col(Run.root_run_id) == latest.c.root_run_id)
                    & (col(Run.attempt) == latest.c.attempt),
                )
                .where(Run.backfill_id == backfill_id, Run.status == "failed")
            ).first()
            db_backfill.status = "failed" if any_failed else "success"
            db_backfill.completed_at = datetime.now(timezone.utc)
            session.add(db_backfill)
            return

        available_slots = max(0, db_backfill.concurrency - in_flight_count)
        for pending_run in pending_runs[:available_slots]:
            pending_run.status = "queued"
            session.add(pending_run)


def _run_order(sort: str | None, root_run_id: UUID | None) -> tuple[Any, ...]:
    """The ORDER BY of a runs listing, ending on the id so pages never overlap.

    A backfill's runs share one ``created_at`` (one transaction creates them
    all), so without the tiebreaker offset paging could repeat or skip rows.

    Args:
        sort: A field of :data:`RUN_SORT_FIELDS`, ``-``-prefixed for
            descending; None picks the listing's default.
        root_run_id: Set when listing one stack, whose default is its
            attempts newest first.

    Returns:
        The ordering clauses.

    Raises:
        ValueError: If *sort* names a field outside :data:`RUN_SORT_FIELDS`.
    """
    if sort is None:
        primary = col(Run.created_at).desc() if root_run_id is None else col(Run.attempt).desc()
    else:
        field = sort.removeprefix("-")
        if field not in RUN_SORT_FIELDS:
            raise ValueError(f"Cannot sort runs by '{field}'. Known: {', '.join(sorted(RUN_SORT_FIELDS))}")
        column = col(getattr(Run, field))
        primary = (column.desc() if sort.startswith("-") else column.asc()).nulls_last()
    return (primary, col(Run.id).asc())


def create_backfill_runs(
    session: Session, db_backfill: Backfill, window: TimePartitionWindow, *, billable: bool = True
) -> None:
    """Create a backfill's runs: one per partition, the newest ``concurrency`` of them queued.

    Part of the caller's transaction (the caller commits), on a backfill row
    already flushed so the runs can reference it. Rows are created oldest
    first, so a runs list ordered by ``created_at`` desc keeps the newest
    partition on top, while the *newest* ``concurrency`` of them are ``queued``
    and the rest wait ``pending``; ``_advance_backfill`` promotes in the same
    newest-first order, so the freshest data lands first and an interrupted
    backfill keeps the recent window rather than the ancient tail.

    Args:
        session: Active database session (the caller commits).
        db_backfill: The flushed backfill row the runs belong to; its
            ``partitions`` count is stamped here.
        window: The partitions the backfill covers.
        billable: Whether the runs count against the run quota, as the
            target's workload declares.
    """
    span = window.partition_count()
    first_queued = max(0, span - db_backfill.concurrency)
    for index, value in enumerate(window.granularity.period_range(window.start, window.end)):
        session.add(
            Run(
                org_id=db_backfill.org_id,
                component_id=db_backfill.component_id,
                backfill_id=db_backfill.id,
                partition_key=window.granularity.format(value),
                status="queued" if index >= first_queued else "pending",
                billable=billable,
            )
        )
    db_backfill.partitions = span
    session.add(db_backfill)


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


