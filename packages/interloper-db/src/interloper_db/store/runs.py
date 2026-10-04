"""Run persistence: queue, complete and retry runs, and list them.

A run is one *attempt*; the attempts of one unit of work form a stack rooted
at ``root_run_id``. Listings read each stack's latest attempt unless told
otherwise, so a retried failure reads as whatever its last attempt became.
Backfill batches are :mod:`~interloper_db.store.backfills`, which a run's
completion advances in the same transaction.
"""

from __future__ import annotations

import builtins
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

import interloper as il
from interloper.errors import ConfigError, ConflictError, NotFoundError
from interloper.partitioning.time import TimePartition
from sqlalchemy import Engine, exists, func
from sqlalchemy.orm import aliased, joinedload
from sqlmodel import Session, col, select

from interloper_db.models import Component, Run
from interloper_db.session import commit, session_scope
from interloper_db.store.page import Page, PageQuery
from interloper_db.store.quotas import QuotaStore, UsageLedger

if TYPE_CHECKING:
    from interloper_db.store.backfills import BackfillStore

logger = logging.getLogger(__name__)

# Reader queries eagerly join the target so its identity survives the session.
# Deliberately not mapped on the relationship itself: locking queries (the
# queue claim, backfill cancelation) select these tables with FOR UPDATE,
# which rejects outer joins.
RUN_LOAD_OPTIONS = (joinedload(Run.target),)  # ty: ignore[invalid-argument-type]

TERMINAL_RUN_STATUSES = frozenset({"success", "failed", "canceled"})

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
        status: Keep runs (each stack's latest attempt) in this status.
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
    status: str | None = None
    after: datetime | None = None
    before: datetime | None = None
    completed_after: datetime | None = None
    completed_before: datetime | None = None
    q: str | None = None
    component_kind: str | None = None
    component_key: str | None = None
    all_attempts: bool = False
    sort: RunSort | None = None


class RunStore:
    """Store methods for runs."""

    def __init__(self, engine: Engine, quotas: QuotaStore, backfills: BackfillStore) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
            quotas: Quota gates it enforces through.
            backfills: Backfill facet a completing run advances its batch through.
        """
        self._engine = engine
        self._quotas = quotas
        self._backfills = backfills

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
        """
        if partition_key is not None:
            self.parse_partition(partition_key)
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
            select(Run)
            .where(*self._filters(org_id, query))
            .order_by(*self._order(query))
            .options(*RUN_LOAD_OPTIONS)
        )
        with session_scope(self._engine) as session:
            return Page.read(session, statement, query)

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
            NotFoundError: If the run is not found.
            ConflictError: If the run is already terminal, so a late completion
                (the reaper's, after a pod finally started) cannot overwrite
                the verdict or queue a retry of work that succeeded.
        """
        with session_scope(self._engine) as session:
            db_run = session.get(Run, run_id, with_for_update=True, populate_existing=True)
            if not db_run:
                raise NotFoundError(f"Run {run_id} not found")
            if db_run.status in TERMINAL_RUN_STATUSES:
                raise ConflictError(f"Run {run_id} is already {db_run.status}")

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
                self._backfills._advance(session, db_run.backfill_id, failed=not success)

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
            NotFoundError: If the run is not found.
            ConfigError: If ``scope`` is invalid.
            ConflictError: If the run has not failed, the stack's latest
                attempt is not a failure, or another retry of the stack
                committed while this one waited for its head.
        """
        if scope not in ("all", "failed"):
            raise ConfigError(f"Invalid retry scope: {scope!r} (expected 'all' or 'failed')")

        with session_scope(self._engine) as session:
            src = session.get(Run, run_id)
            if not src:
                raise NotFoundError(f"Run {run_id} not found")
            if src.status != "failed":
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
            if head.status != "failed":
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
    def parse_partition(key: str) -> TimePartition:
        """Parse a caller-supplied partition key, rejecting one of no known shape.

        Args:
            key: The key, whose shape carries its granularity (``2026-08-21``,
                ``2026-08``, ``2026``, ``2026-08-21T13``).

        Returns:
            The partition the key names.

        Raises:
            ConfigError: If the key matches no known shape.
        """
        try:
            return TimePartition.from_key(key)
        except ValueError as error:
            raise ConfigError(str(error)) from error

    # -- Internals -------------------------------------------------------------

    @staticmethod
    def _target_billable(session: Session, component_id: UUID | None) -> bool:
        """Resolve whether runs of a target count against the run quota.

        An untargeted run is billable; a targeted one follows what its kind's
        workload declares (:meth:`Component.run_billable`).

        Args:
            session: Open session the component row is read through.
            component_id: The target component UUID, or None for an
                untargeted run.

        Returns:
            The billability.

        Raises:
            NotFoundError: If the component does not exist.
        """
        if component_id is None:
            return True
        db_component = session.get(Component, component_id)
        if not db_component:
            raise NotFoundError(f"Component {component_id} not found")
        return db_component.run_billable()

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

    @staticmethod
    def _latest_attempt_only() -> Any:
        """Keep only each stack's latest attempt.

        An attempt is its stack's latest when no attempt of the same stack
        carries a higher number: a per-row probe of ``ix_runs_root_run_id``
        for a narrow listing, one anti-join over the organisation's runs for a
        wide one, and never an aggregate the planner has to estimate.

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
            filters.append(self._latest_attempt_only())
        if query.status:
            filters.append(Run.status == query.status)
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
