"""Run persistence: every state transition a run takes, and its listings.

A run is one *attempt*; the attempts of one unit of work form a stack rooted
at ``root_run_id``. Listings read each stack's latest attempt unless told
otherwise, so a retried failure reads as whatever its last attempt became.
Backfill batches are :mod:`~interloper_db.store.backfills`, which a run's
completion advances in the same transaction.

The lifecycle is ``pending`` (a backfill's runs beyond its concurrency) →
``queued`` → ``dispatched`` (:meth:`RunStore.claim_next`) → ``running``
(:meth:`RunStore.start`) → ``success``/``failed`` (:meth:`RunStore.complete`,
:meth:`RunStore.fail`), or ``canceled`` from any open state. A run whose
verdict will never reach hooks, canceled or failed and retried, is stamped
``hooks_evaluated_at`` at that transition, so ``hooks_pending`` lists
exactly the verdicts hooks still owe a reaction (:meth:`RunStore.hooks_pending`).
"""

from __future__ import annotations

import builtins
import logging
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID, uuid4

import interloper as il
from interloper.errors import ConfigError, ConflictError, NotFoundError
from interloper.partitioning.time import TimePartition
from sqlalchemy import Engine, exists, func
from sqlalchemy.orm import aliased, joinedload
from sqlmodel import col, select

from interloper_db.models import OPEN_RUN_STATUSES, TERMINAL_RUN_STATUSES, Component, Event, Run, RunStatus
from interloper_db.session import commit, save, session_scope
from interloper_db.store.page import Page, PageQuery
from interloper_db.store.quotas import QuotaStore, UsageLedger

if TYPE_CHECKING:
    from interloper_db.store.backfills import BackfillStore
    from interloper_db.store.events import EventStore

logger = logging.getLogger(__name__)

# Reader queries eagerly join the target so its identity survives the session.
# Deliberately not mapped on the relationship itself: locking queries (the
# queue claim, backfill cancelation) select these tables with FOR UPDATE,
# which rejects outer joins.
RUN_LOAD_OPTIONS = (joinedload(Run.target),)  # ty: ignore[invalid-argument-type]

RunSort = Literal[
    "id",
    "-id",
    "partition_key",
    "-partition_key",
    "status",
    "-status",
    "created_at",
    "-created_at",
    "started_at",
    "-started_at",
    "completed_at",
    "-completed_at",
]
"""A runs listing's sort: a column, ``-``-prefixed for descending."""


# -- Expressions ---------------------------------------------------------------


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


class RunQuery(PageQuery):
    """Which of an organisation's runs a listing reads, and in which order.

    ``after``/``before`` select the runs whose execution *overlaps* the window
    — a run occupies ``[started_at, completed_at)``, left open-ended while it
    is still running, so runs that never started fall outside every window.
    ``completed_after``/``completed_before`` read the completion instant
    alone, so they keep a run that ended without ever starting and drop every
    run not yet completed. The target filters read the target through the
    relationship, so a run whose target was deleted matches none of them.

    Attributes:
        component_id: Keep runs targeting this component.
        backfill_id: Keep runs of this backfill.
        root_run_id: List this stack's attempts rather than one row per stack.
        status: Keep runs (each stack's latest attempt) in any of these statuses.
        after: Window start: keep runs still executing at or after this instant.
        before: Window end: keep runs that had started by this instant.
        completed_after: Keep runs that completed at or after this instant.
        completed_before: Keep runs that completed at or before this instant.
        q: Keep runs whose target's name or key contains this, case-insensitively.
        component_kind: Keep runs whose target is of this kind.
        component_key: Keep runs whose target is of this type (catalog key).
        all_attempts: Keep every attempt rather than each stack's latest.
        sort: The order; ``None`` lists newest first (one stack: its latest
            attempt first).
    """

    component_id: UUID | None = None
    backfill_id: UUID | None = None
    root_run_id: UUID | None = None
    status: builtins.list[RunStatus] | None = None
    after: datetime | None = None
    before: datetime | None = None
    completed_after: datetime | None = None
    completed_before: datetime | None = None
    q: str | None = None
    component_kind: str | None = None
    component_key: str | None = None
    all_attempts: bool = False
    sort: RunSort | None = None


def latest_attempt() -> Any:
    """Keep only each stack's latest attempt.

    An attempt is its stack's latest when no attempt of the same stack
    carries a higher number: a per-row probe of the stack's unique
    ``(root_run_id, attempt)`` index, never an aggregate the planner has to
    estimate. The probe takes none of the caller's filters on purpose:
    narrowing it would answer "the latest *failed* attempt" rather than "the
    stacks whose latest attempt failed".

    Returns:
        A filter expression selecting the latest attempt of each stack.
    """
    later = aliased(Run)
    return ~exists().where(col(later.root_run_id) == col(Run.root_run_id), col(later.attempt) > col(Run.attempt))


class RunStore:
    """Store methods for runs."""

    def __init__(self, engine: Engine, quotas: QuotaStore, backfills: BackfillStore, events: EventStore) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
            quotas: Quota gates it enforces through.
            backfills: Backfill facet a completing run advances its batch through.
            events: Event facet a failure's reason is recorded through.
        """
        self._engine = engine
        self._quotas = quotas
        self._backfills = backfills
        self._events = events

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
                granularity, e.g. ``2026-08-21`` or ``2026-08``).

        Returns:
            The created Run row.

        Raises:
            NotFoundError: If the target component does not exist.
        """
        if partition_key is not None:
            TimePartition.from_key(partition_key)
        with session_scope(self._engine) as session:
            billable = True
            if component_id is not None:
                target = session.get(Component, component_id)
                if target is None:
                    raise NotFoundError(f"Component {component_id} not found")
                billable = target.run_billable()
            self._quotas.admit_run(org_id, billable=billable)
            db_run = Run(
                org_id=org_id,
                component_id=component_id,
                partition_key=partition_key,
                status=RunStatus.QUEUED,
                billable=billable,
            )
            save(session, db_run, "target")
            return db_run

    def get(self, run_id: UUID, *, org_id: UUID | None = None) -> Run:
        """Load a run by ID.

        Args:
            run_id: The run UUID.
            org_id: Organisation the row must belong to; a mismatch raises
                ``NotFoundError`` like an absent row, so a caller cannot learn
                that an id exists in another tenant. ``None`` accepts any
                organisation, for a caller that authorizes by the row's own
                ``org_id`` afterwards (the API) or serves every organisation
                (the scheduler).

        Returns:
            The Run row, its target loaded.

        Raises:
            NotFoundError: If the run is not found, or belongs to another
                organisation.
        """
        with session_scope(self._engine) as session:
            db_run = session.get(Run, run_id, options=RUN_LOAD_OPTIONS)
            if not db_run or (org_id is not None and db_run.org_id != org_id):
                raise NotFoundError(f"Run {run_id} not found")
            return db_run

    def list(self, org_id: UUID, query: RunQuery) -> Page[Run]:
        """List one row per stack, one stack's attempts, or every attempt.

        A stack is one piece of work, so a listing shows its **latest
        attempt** and every filter reads that attempt: a stack whose first
        attempt failed and whose second succeeded is a success, which is what
        a reader means by "failed runs". A ``root_run_id`` asks for one stack
        instead, and lists its attempts newest first; ``all_attempts`` keeps
        every attempt of every stack, which is what statistics over durations
        and retries read.

        Args:
            org_id: Organisation UUID.
            query: The filters, the order, and the window to read.

        Returns:
            The page of runs, their targets loaded.
        """
        statement = (
            select(Run).where(*self._filters(org_id, query)).order_by(*self._order(query)).options(*RUN_LOAD_OPTIONS)
        )
        with session_scope(self._engine) as session:
            return Page.read(session, statement, query)

    def recent(self, org_id: UUID, *, until: datetime, limit: int) -> builtins.list[Run]:
        """The stacks that completed most recently, newest completion first.

        Args:
            org_id: Organisation UUID.
            until: Keep stacks completed by this instant.
            limit: How many to read.

        Returns:
            Each stack's latest attempt, its target loaded.
        """
        return self.list(org_id, RunQuery(completed_before=until, sort="-completed_at", limit=limit)).items

    def failures(self, org_id: UUID, query: PageQuery) -> Page[Run]:
        """The stacks whose latest attempt failed, newest first.

        Args:
            org_id: Organisation UUID.
            query: The window to read.

        Returns:
            The page of failed stacks' latest attempts, their targets loaded.
        """
        return self.list(org_id, RunQuery(status=[RunStatus.FAILED], limit=query.limit, offset=query.offset))

    def attempts(self, root_run_id: UUID) -> builtins.list[Run]:
        """A stack's attempts, the first one first.

        Args:
            root_run_id: The stack's root run.

        Returns:
            The attempts, their targets loaded.
        """
        statement = (
            select(Run).where(Run.root_run_id == root_run_id).order_by(col(Run.attempt)).options(*RUN_LOAD_OPTIONS)
        )
        with session_scope(self._engine) as session:
            return [*session.exec(statement).all()]

    def has_open(self, component_id: UUID) -> bool:
        """Whether a run of a component is queued, dispatched or running.

        Args:
            component_id: The component UUID.

        Returns:
            True when one is.
        """
        statement = select(Run.id).where(Run.component_id == component_id, col(Run.status).in_(OPEN_RUN_STATUSES))
        with session_scope(self._engine) as session:
            return session.exec(statement.limit(1)).first() is not None

    def dispatched(self) -> builtins.list[Run]:
        """Every organisation's runs claimed by the queue that have not yet started.

        For the reaper, which reconciles them with what the launcher reports.

        Returns:
            The dispatched runs, their targets loaded.
        """
        statement = select(Run).where(Run.status == RunStatus.DISPATCHED).options(*RUN_LOAD_OPTIONS)
        with session_scope(self._engine) as session:
            return [*session.exec(statement).all()]

    def hooks_pending(self) -> builtins.list[Run]:
        """Every organisation's verdicts whose hooks have not been evaluated, oldest completion first.

        For the hook sweep: a run that will never fire hooks (canceled, or a
        failure a retry superseded) is stamped at that transition, so what
        is left is exactly what the sweep owes.

        Returns:
            The runs, their targets loaded.
        """
        statement = (
            select(Run)
            .where(col(Run.status).in_(TERMINAL_RUN_STATUSES), col(Run.hooks_evaluated_at).is_(None))
            .order_by(col(Run.completed_at), col(Run.id))
            .options(*RUN_LOAD_OPTIONS)
        )
        with session_scope(self._engine) as session:
            return [*session.exec(statement).all()]

    def latest_by_target(self, org_id: UUID, *, kind: str | None = None) -> builtins.list[Run]:
        """The most recent attempt of every target.

        What a reader means by "the last time this job ran": the most recently
        created attempt targeting the component, whatever stack it belongs to.
        The runs of one backfill share their creation instant, so a tie goes
        to the later partition key, then to the greater id. Runs whose target
        was deleted are left out.

        Args:
            org_id: Organisation UUID.
            kind: Keep targets of this kind; ``None`` keeps every kind.

        Returns:
            One run per target, newest first, with the target loaded.
        """
        rank = (
            func.row_number()
            .over(
                partition_by=col(Run.component_id),
                order_by=(col(Run.created_at).desc(), col(Run.partition_key).desc().nulls_last(), col(Run.id).desc()),
            )
            .label("rank")
        )
        ranked = select(col(Run.id), rank).where(Run.org_id == org_id, col(Run.component_id).is_not(None)).subquery()
        statement = (
            select(Run)
            .where(col(Run.id).in_(select(ranked.c.id).where(ranked.c.rank == 1)))
            .order_by(col(Run.created_at).desc())
            .options(*RUN_LOAD_OPTIONS)
        )
        if kind:
            statement = statement.where(col(Run.target).has(col(Component.kind) == kind))
        with session_scope(self._engine) as session:
            return [*session.exec(statement).all()]

    def last_successes(self, org_id: UUID) -> dict[UUID, datetime]:
        """When each target last completed a successful run.

        Args:
            org_id: Organisation UUID.

        Returns:
            The latest successful completion by target id; a target that never
            succeeded is absent.
        """
        statement = (
            select(col(Run.component_id), func.max(col(Run.completed_at)))
            .where(Run.org_id == org_id, Run.status == RunStatus.SUCCESS, col(Run.component_id).is_not(None))
            .group_by(col(Run.component_id))
        )
        with session_scope(self._engine) as session:
            return {
                component_id: completed
                for component_id, completed in session.exec(statement).all()
                if component_id is not None and completed is not None
            }

    def claim_next(self) -> Run | None:
        """Claim the oldest claimable queued run and dispatch it, reserving its quota.

        A run carrying a schedule is not claimable until it has passed, which
        is how a retry serves its backoff without a status of its own; it is
        skipped rather than waited on, so a run backing off never holds the
        head of the queue. Rows are locked with ``SKIP LOCKED``, so concurrent
        workers claim different runs.

        This is the authoritative run-quota gate: dispatch requires an atomic
        reservation, so an exhausted organisation never executes past its
        limit. A denied run is canceled, its whole backfill with it, and the
        next one is tried, so an exhausted organisation cannot block the
        queue for the others.

        Returns:
            The dispatched run, or ``None`` when nothing is claimable.
        """
        while True:
            with session_scope(self._engine) as session:
                statement = (
                    select(Run)
                    .where(Run.status == RunStatus.QUEUED)
                    .where(col(Run.scheduled_for).is_(None) | (col(Run.scheduled_for) <= func.now()))
                    .order_by(col(Run.created_at).asc())
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                db_run = session.exec(statement).first()
                if db_run is None:
                    return None
                if self._quotas.try_reserve_run(db_run):
                    db_run.status = RunStatus.DISPATCHED
                    session.add(db_run)
                    commit(session)
                    logger.info("Dispatched run %s", db_run.id)
                    return db_run
                self._cancel_over_quota(db_run)
                commit(session)
                logger.warning(
                    "Canceled run %s: monthly successful-run quota exhausted for org %s", db_run.id, db_run.org_id
                )

    def start(self, run_id: UUID) -> Run:
        """Mark a dispatched run as running and stamp its start.

        Args:
            run_id: The run UUID.

        Returns:
            The running Run row, its target loaded.

        Raises:
            ConflictError: If the run is already terminal, as when the reaper
                failed it while its pod was still being scheduled.
        """
        with session_scope(self._engine) as session:
            db_run = self._lock(run_id)
            if db_run.status in TERMINAL_RUN_STATUSES:
                raise ConflictError(f"Run {run_id} is already {db_run.status}")
            db_run.status = RunStatus.RUNNING
            db_run.started_at = datetime.now(timezone.utc)
            save(session, db_run, "target")
            return db_run

    def fail(self, run_id: UUID, error: str, *, metadata: dict[str, Any] | None = None) -> Run:
        """Fail a run with a reason: record a ``run_failed`` event carrying it, then complete the run as failed.

        Both writes share one transaction, so a run is never failed without
        its reason, nor its reason recorded on a run another writer completed:
        a run already terminal raises the completion's ``ConflictError`` and
        records nothing.

        Args:
            run_id: The run UUID.
            error: Why the run failed, as a reader should see it.
            metadata: The run's event metadata, when the caller carries more
                than the row does (an executor's trace context); ``None``
                builds it from the row and its target.

        Returns:
            The failed Run row.
        """
        with session_scope(self._engine) as session:
            db_run = self._lock(run_id)
            if metadata is None:
                metadata = db_run.event_metadata(db_run.target)
            event = il.Event(type=il.EventType.RUN_FAILED, metadata={**metadata, "error": error})
            self._events.save(event, org_id=db_run.org_id, run_id=db_run.id)
            failed = self.complete(run_id, success=False)
            commit(session)
            return failed

    def mark_hooks_evaluated(self, run_id: UUID) -> None:
        """Stamp a run's hooks as evaluated, so the hook sweep moves past it.

        Args:
            run_id: The run UUID.
        """
        with session_scope(self._engine) as session:
            db_run = self._lock(run_id)
            db_run.hooks_evaluated_at = datetime.now(timezone.utc)
            save(session, db_run)

    def complete(self, run_id: UUID, *, success: bool) -> Run:
        """Mark a run as completed and advance its backfill if applicable.

        Also stamps ``last_run_at`` and ``last_run_status`` on the target
        component's machine-owned state — this is the single terminal path
        every run takes (scheduled, manual, retried), so the component's
        "last run" reflects all of them.

        A failure also queues its own next attempt when the target's policy
        allows one, before the backfill advances so the batch's in-flight
        count sees the successor and does not finalize early. The run's row is
        locked, so a concurrent completion waits and then finds it terminal
        rather than queueing a second successor.

        Args:
            run_id: The run UUID.
            success: Whether the run succeeded.

        Returns:
            The updated Run row.

        Raises:
            ConflictError: If the run is already terminal, so a late completion
                (the reaper's, after a pod finally started) cannot overwrite
                the verdict or queue a retry of work that succeeded.
        """
        with session_scope(self._engine) as session:
            db_run = self._lock(run_id)
            if db_run.status in TERMINAL_RUN_STATUSES:
                raise ConflictError(f"Run {run_id} is already {db_run.status}")

            db_run.status = RunStatus.SUCCESS if success else RunStatus.FAILED
            db_run.completed_at = datetime.now(timezone.utc)
            session.add(db_run)

            UsageLedger(session).settle_run(db_run, success=success)

            if db_run.target is not None:
                db_run.target.stamp_state(last_run_at=db_run.completed_at, last_run_status=db_run.status)
                session.add(db_run.target)

            if not success:
                self._plan_retry(db_run)

            if db_run.backfill_id:
                self._backfills.advance(db_run.backfill_id, failed=not success)

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

        The head's row is locked for the rest of the transaction, so of two
        concurrent retries the second waits for the first to commit, then
        finds the head is no longer the latest attempt and is refused.

        Args:
            run_id: The failed run to retry, any attempt of its stack.
            scope: ``"all"`` to re-run the whole DAG, or ``"failed"`` to
                re-run only the previously failed/cancelled assets.

        Returns:
            The newly created, queued Run row.

        Raises:
            ConfigError: If ``scope`` is invalid.
            ConflictError: If the run has not failed, the stack's latest
                attempt is not a failure, or another retry of the stack
                committed while this one waited for its head.
        """
        if scope not in ("all", "failed"):
            raise ConfigError(f"Invalid retry scope: {scope!r} (expected 'all' or 'failed')")

        with session_scope(self._engine) as session:
            src = self.get(run_id)
            if src.status != RunStatus.FAILED:
                raise ConflictError(
                    f"Run {run_id} is not failed (status={src.status!r}); only failed runs can be retried"
                )
            latest = select(Run).where(Run.root_run_id == src.root_run_id).order_by(col(Run.attempt).desc()).limit(1)
            head = session.exec(latest.with_for_update().execution_options(populate_existing=True)).one()
            # A fresh statement, so it sees an attempt committed while the lock was awaited.
            if session.exec(latest).one().id != head.id:
                raise ConflictError(
                    f"Run {run_id}'s stack was retried concurrently; retry it again from its latest attempt"
                )
            if head.status != RunStatus.FAILED:
                raise ConflictError(
                    f"Run {run_id} is attempt {src.attempt} of a stack whose latest attempt {head.attempt} "
                    f"is {head.status!r}; only a stack whose latest attempt failed can be retried"
                )

            self._quotas.admit_run(head.org_id, billable=head.billable, subject="retry")
            db_run = Run(
                root_run_id=head.root_run_id,
                org_id=head.org_id,
                component_id=head.component_id,
                partition_key=head.partition_key,
                status=RunStatus.QUEUED,
                retry_of=head.id,
                attempt=head.attempt + 1,
                retry_scope=scope,
                billable=head.billable,
            )
            head.supersede()
            session.add(head)
            save(session, db_run, "target")
            return db_run

    # -- Internals -------------------------------------------------------------

    def _lock(self, run_id: UUID) -> Run:
        """Load a run for a write, holding its row for the rest of the transaction.

        Args:
            run_id: The run UUID.

        Returns:
            The run row, its state read fresh. Its target is not loaded: a
            locking select rejects the outer join, so a caller that needs it
            reaches it while the session is open.

        Raises:
            NotFoundError: If the run is not found.
        """
        statement = select(Run).where(Run.id == run_id).with_for_update().execution_options(populate_existing=True)
        with session_scope(self._engine) as session:
            db_run = session.exec(statement).first()
            if db_run is None:
                raise NotFoundError(f"Run {run_id} not found")
            return db_run

    def _plan_retry(self, db_run: Run) -> Run | None:
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
            db_run: The run that just failed, its target loaded.

        Returns:
            The queued successor, or ``None`` when nothing is retried.
        """
        policy = db_run.target.retry_policy if db_run.target is not None else None
        if policy is None or not policy.allows(db_run.attempt + 1):
            return None

        successor = Run(
            org_id=db_run.org_id,
            component_id=db_run.component_id,
            backfill_id=db_run.backfill_id,
            partition_key=db_run.partition_key,
            status=RunStatus.QUEUED,
            scheduled_for=datetime.now(timezone.utc) + timedelta(seconds=policy.delay_before(db_run.attempt + 1)),
            retry_of=db_run.id,
            root_run_id=db_run.root_run_id,
            attempt=db_run.attempt + 1,
            retry_scope="failed",
            billable=db_run.billable,
        )
        db_run.supersede()
        with session_scope(self._engine) as session:
            session.add_all((successor, db_run))
            session.flush()
        logger.info("Queued attempt %d of run stack %s", successor.attempt, successor.root_run_id)
        return successor

    def _cancel_over_quota(self, db_run: Run) -> None:
        """Cancel a quota-denied run, its backfill with it, and record why.

        A canceled run is never claimed again, so the event cannot double-write.

        Args:
            db_run: The run the quota denied.
        """
        db_run.cancel()
        if db_run.backfill_id:
            with suppress(ConflictError):
                self._backfills.cancel(db_run.backfill_id)
        reason = Event(
            id=uuid4(),
            org_id=db_run.org_id,
            run_id=db_run.id,
            component_id=db_run.component_id,
            event_type="log",
            level="warning",
            message="Run canceled: the organisation's monthly successful-run quota is exhausted",
            timestamp=datetime.now(timezone.utc),
        )
        with session_scope(self._engine) as session:
            session.add_all((db_run, reason))

    def _filters(self, org_id: UUID, query: RunQuery) -> builtins.list[Any]:
        """The where-clauses of a runs listing.

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
            query: The filters to translate; see :class:`RunQuery`.

        Returns:
            Filter expressions for the given criteria.
        """
        filters: builtins.list[Any] = [Run.org_id == org_id]
        target = col(Run.target)
        if query.q:
            filters.append(
                target.has(
                    col(Component.name).icontains(query.q, autoescape=True)
                    | col(Component.key).icontains(query.q, autoescape=True)
                )
            )
        if query.component_kind:
            filters.append(target.has(col(Component.kind) == query.component_kind))
        if query.component_key:
            filters.append(target.has(col(Component.key) == query.component_key))
        if query.component_id:
            filters.append(Run.component_id == query.component_id)
        if query.backfill_id:
            filters.append(Run.backfill_id == query.backfill_id)
        if query.root_run_id:
            filters.append(Run.root_run_id == query.root_run_id)
        elif not query.all_attempts:
            filters.append(latest_attempt())
        if query.status:
            filters.append(col(Run.status).in_(query.status))
        if query.after is not None:
            filters.append(col(Run.completed_at).is_(None) | (col(Run.completed_at) >= query.after))
        if query.before is not None:
            filters.append(col(Run.started_at) <= query.before)
        if query.after is not None and query.before is None:
            # An `after` bound alone still means "ran at some point", so a
            # never-started run must not slip through on the NULL completed_at.
            filters.append(col(Run.started_at).is_not(None))
        if query.completed_after is not None:
            filters.append(col(Run.completed_at) >= query.completed_after)
        if query.completed_before is not None:
            filters.append(col(Run.completed_at) <= query.completed_before)
        return filters

    @staticmethod
    def _order(query: RunQuery) -> tuple[Any, ...]:
        """The ORDER BY of a runs listing, ending on the id so pages never overlap.

        A backfill's runs share one ``created_at`` (one transaction creates
        them all), so without the tiebreaker offset paging could repeat or
        skip rows.

        Args:
            query: The listing's query; its ``sort`` wins, else one stack lists
                its attempts newest first and anything else lists newest first.

        Returns:
            The ordering clauses.
        """
        if query.sort is None:
            primary = col(Run.created_at).desc() if query.root_run_id is None else col(Run.attempt).desc()
        else:
            column = col(getattr(Run, query.sort.removeprefix("-")))
            primary = (column.desc() if query.sort.startswith("-") else column.asc()).nulls_last()
        return (primary, col(Run.id).asc())
