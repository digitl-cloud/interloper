"""Run, event, and backfill persistence."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import interloper as il
from interloper.errors import NotFoundError
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from sqlalchemy import Engine, func
from sqlalchemy.orm import joinedload
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

# Reader queries eagerly join the target so its identity survives the session.
# Deliberately not mapped on the relationship itself: locking queries (the
# queue claim, backfill cancelation) select these tables with FOR UPDATE,
# which rejects outer joins.
RUN_LOAD_OPTIONS = (joinedload(Run.target),)  # ty: ignore[invalid-argument-type]
BACKFILL_LOAD_OPTIONS = (joinedload(Backfill.target),)  # ty: ignore[invalid-argument-type]


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
            _ = db_run.target  # load before the session closes; readers reach it detached
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
            db_run = session.get(Run, run_id, options=RUN_LOAD_OPTIONS)
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
        q: str | None = None,
        component_kind: str | None = None,
        component_key: str | None = None,
        root_run_id: UUID | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Run]:
        """List one row per stack, or one stack's attempts.

        A stack is one piece of work, so a listing shows its **latest
        attempt** and every filter reads that attempt: a stack whose first
        attempt failed and whose second succeeded is a success, which is what
        a reader means by "failed runs". Passing *root_run_id* asks for one
        stack instead, and returns its attempts newest first.

        Args:
            org_id: Organisation UUID.
            component_id: Optional target component filter.
            backfill_id: Optional backfill filter.
            status: Optional status filter.
            after: Keep runs still executing at or after this instant.
            before: Keep runs that had started by this instant.
            q: Keep runs whose target's name or key contains this, case-insensitively.
            component_kind: Keep runs whose target is of this kind.
            component_key: Keep runs whose target is of this type (catalog key).
            root_run_id: List this stack's attempts rather than one row per stack.
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
                q=q,
                component_kind=component_kind,
                component_key=component_key,
                root_run_id=root_run_id,
            )
            if root_run_id is None:
                filters.append(self._latest_attempt_only(org_id))
            order = col(Run.created_at).desc() if root_run_id is None else col(Run.attempt).desc()
            statement = (
                select(Run)
                .where(*filters)
                .order_by(order)
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
        q: str | None = None,
        component_kind: str | None = None,
        component_key: str | None = None,
        root_run_id: UUID | None = None,
    ) -> int:
        """Count runs matching the same filters as :meth:`list_all`.

        Args:
            org_id: Organisation UUID.
            component_id: Optional target component filter.
            backfill_id: Optional backfill filter.
            status: Optional status filter.
            after: Keep runs still executing at or after this instant.
            before: Keep runs that had started by this instant.
            q: Keep runs whose target's name or key contains this, case-insensitively.
            component_kind: Keep runs whose target is of this kind.
            component_key: Keep runs whose target is of this type (catalog key).
            root_run_id: Count this stack's attempts rather than one per stack.

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
                q=q,
                component_kind=component_kind,
                component_key=component_key,
                root_run_id=root_run_id,
            )
            if root_run_id is None:
                filters.append(self._latest_attempt_only(org_id))
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
                    db_component.stamp_state(last_run_at=db_run.completed_at, last_run_status=db_run.status)
                    session.add(db_component)

            if not success:
                self._plan_retry(session, db_run)

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
                root_run_id=src.root_run_id,
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
            if db_backfill.status not in ("running", "queued"):
                raise ValueError(f"Backfill {backfill_id} is already {db_backfill.status}")

            cancel_backfill_runs(session, db_backfill)
            commit(session)
            session.refresh(db_backfill)
            _ = db_backfill.target  # load before the session closes; readers reach it detached
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
            db_backfill = session.get(Backfill, backfill_id, options=BACKFILL_LOAD_OPTIONS)
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
            statement = (
                select(Backfill)
                .where(Backfill.org_id == org_id)
                .order_by(col(Backfill.created_at).desc())
                .options(*BACKFILL_LOAD_OPTIONS)
            )
            return list(session.exec(statement).all())

    def list_active_backfills(self, org_id: UUID) -> list[Backfill]:
        """List in-progress backfills for an organisation.

        Args:
            org_id: Organisation UUID.

        Returns:
            List of Backfill rows with status ``"running"`` or ``"queued"``.
        """
        with session_scope(self._engine) as session:
            statement = (
                select(Backfill)
                .where(
                    Backfill.org_id == org_id,
                    col(Backfill.status).in_(["running", "queued"]),
                )
                .options(*BACKFILL_LOAD_OPTIONS)
            )
            return list(session.exec(statement).all())

    # -- Internals -------------------------------------------------------------

    @staticmethod
    def _latest_attempt_only(org_id: UUID) -> Any:
        """Keep only each stack's latest attempt.

        Scoped to the organisation alone on purpose: every attempt of a stack
        shares its target, its backfill and its org, so no other filter can
        change which attempt is the latest. Narrowing by the caller's filters
        instead would answer a different question, such as "the latest *failed*
        attempt" rather than "the stacks whose latest attempt failed".

        Expressed as a grouped join rather than ``DISTINCT ON`` so it runs on
        SQLite as well as Postgres.

        Args:
            org_id: Organisation whose stacks are reduced.

        Returns:
            A filter expression selecting the latest attempt of each stack.
        """
        latest = (
            select(col(Run.root_run_id), func.max(col(Run.attempt)).label("attempt"))
            .where(Run.org_id == org_id)
            .group_by(col(Run.root_run_id))
            .subquery()
        )
        return col(Run.id).in_(
            select(col(Run.id)).join(
                latest,
                onclause=(col(Run.root_run_id) == latest.c.root_run_id) & (col(Run.attempt) == latest.c.attempt),
            )
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
        q: str | None = None,
        component_kind: str | None = None,
        component_key: str | None = None,
        root_run_id: UUID | None = None,
    ) -> list[Any]:
        """The shared where-clauses of :meth:`RunStore.list_all` / :meth:`RunStore.count`.

        ``after``/``before`` select the runs whose execution *overlaps* the window
        — a run occupies ``[started_at, completed_at)``, left open-ended while it
        is still running. Runs that never started occupy no time and so fall
        outside every window.

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
            q: Keep runs whose target's name or key contains this text,
                case-insensitively; ``None`` applies no search.
            component_kind: Keep runs whose target is of this kind; ``None``
                applies no kind filter.
            component_key: Keep runs whose target is of this type (catalog
                key); ``None`` applies no type filter.
            root_run_id: Keep the attempts of this stack; ``None`` applies no
                stack filter.

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
        2. **Finalize**: if nothing in-flight or pending, mark complete. The
           verdict reads each stack's latest attempt, so an attempt a later one
           healed no longer condemns the batch. A queued successor still counts
           as in flight, which is what keeps the batch open while a retry waits
           out its backoff.
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


