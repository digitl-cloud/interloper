"""Tests for the backfill store (``interloper_db.store.backfills``).

These run against an in-memory SQLite database (only the runs/backfills
tables) so a batch's status transitions are exercised against real SQL.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from typing import Any, ClassVar
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.errors import ConfigError, ConflictError, NotFoundError
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, col, select

from interloper_db import BackfillStatus, RunStatus
from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Component, Event, Quota, Run, Usage
from interloper_db.store import BackfillQuery, Store
from interloper_db.store.backfills import BackfillStore

_ORG_ID = uuid4()


class FakeBackfillPlumbing(il.Operation):
    """Test-only kind whose operation is platform plumbing (non-billable)."""

    kind: ClassVar[str] = "fake_backfill_plumbing"
    billable: ClassVar[bool] = False

    async def execute(self, context: il.OperationContext) -> il.OperationResult:
        """Do nothing.

        Args:
            context: The platform-provided execution context, unused.

        Returns:
            An effectless success.
        """
        return il.OperationResult()


il.KINDS.register(FakeBackfillPlumbing.kind, FakeBackfillPlumbing.anchor())


@pytest.fixture
def store() -> Iterator[Store]:
    """A store wired to a fresh in-memory SQLite database.

    Yields:
        The store bound to that database, disposed once the test finishes.
    """
    engine = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _sqlite_uuid(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    for model in (Backfill, Component, Run, Event, Quota, Usage):
        model.__table__.create(engine)  # ty: ignore[unresolved-attribute]
    try:
        yield Store(catalog=il.Catalog(components={}), engine=engine)
    finally:
        engine.dispose()
        engine_module._engine = None


def _component(store: Store, kind: str, key: str | None = None) -> UUID:
    """Insert a component the backfills can target.

    Args:
        store: The store under test.
        kind: The component's kind.
        key: The component's catalog key; defaults to the kind.

    Returns:
        The component id.
    """
    with Session(store.engine) as session:
        row = Component(id=uuid4(), org_id=_ORG_ID, kind=kind, key=key or kind, name=key or kind)
        session.add(row)
        session.commit()
        assert row.id is not None
        return row.id


def _job_with_retry(store: Store, **policy: Any) -> UUID:
    """A job component whose config declares a retry policy.

    Args:
        store: The store under test.
        **policy: The retry policy's fields.

    Returns:
        The component id.
    """
    with Session(store.engine) as session:
        row = Component(
            id=uuid4(),
            org_id=_ORG_ID,
            kind="job",
            key="job",
            name="job",
            config={"retry": policy} if policy else {},
        )
        session.add(row)
        session.commit()
        assert row.id is not None
        return row.id


def _backfill(store: Store, *, days: int = 4, concurrency: int = 2) -> Backfill:
    """Start a daily backfill over a fresh job, from 2026-01-01 on.

    Args:
        store: The store under test.
        days: How many daily partitions it spans.
        concurrency: How many of its runs are queued at once.

    Returns:
        The created backfill.
    """
    return store.backfills.create(
        _ORG_ID,
        component_id=_component(store, kind="job"),
        start_key="2026-01-01",
        end_key=f"2026-01-{days:02d}",
        concurrency=concurrency,
    )


def _mark_dispatched(store: Store, backfill_id: UUID) -> UUID:
    """Flip one queued run to dispatched, simulating a worker claim.

    Returns:
        The id of the run that was flipped.
    """
    with Session(store.engine) as session:
        run = session.exec(select(Run).where(Run.backfill_id == backfill_id, Run.status == "queued")).first()
        assert run is not None and run.id is not None
        run.status = RunStatus.DISPATCHED
        session.add(run)
        session.commit()
        return run.id


def _run_statuses(store: Store, backfill_id: UUID) -> dict[UUID, str]:
    with Session(store.engine) as session:
        runs = session.exec(select(Run).where(Run.backfill_id == backfill_id)).all()
        return {run.id: run.status for run in runs if run.id}


def _runs_of(store: Store, backfill_id: UUID) -> list[Run]:
    with Session(store.engine) as session:
        return list(session.exec(select(Run).where(Run.backfill_id == backfill_id)).all())


def _record_run_failure(store: Store, run_id: UUID, error: str) -> None:
    with Session(store.engine) as session:
        session.add(
            Event(
                id=uuid4(),
                org_id=_ORG_ID,
                run_id=run_id,
                event_type="run_failed",
                error=error,
                timestamp=dt.datetime.now(dt.timezone.utc),
            )
        )
        session.commit()


def _partition_statuses(store: Store, backfill_id: UUID) -> dict[str, str]:
    with Session(store.engine) as session:
        runs = session.exec(select(Run).where(Run.backfill_id == backfill_id)).all()
        return {run.partition_key: run.status for run in runs if run.partition_key}


class TestTargetOperations:
    """A backfill validates its target and records the target's billability on every run."""

    def test_backfill_runs_record_billable_from_the_operation(self, store: Store):
        target = _component(store, kind="fake_backfill_plumbing")
        backfill = store.backfills.create(_ORG_ID, component_id=target, start_key="2026-01-01", end_key="2026-01-03")
        with Session(store.engine) as session:
            runs = session.exec(select(Run).where(Run.backfill_id == backfill.id)).all()
        assert len(runs) == 3
        assert all(run.billable is False for run in runs)

    def test_non_billable_backfill_skips_the_run_quota(self, store: Store):
        from types import SimpleNamespace

        from interloper_db.store.quotas import METRIC_SUCCESSFUL_RUNS, UsageLedger

        store._quota_defaults = SimpleNamespace(max_successful_runs_per_month=1)
        with Session(store.engine) as session:
            ledger = UsageLedger(session)
            ledger.increment(_ORG_ID, METRIC_SUCCESSFUL_RUNS, ledger.current_period(), used=1)
            session.commit()
        target = _component(store, kind="fake_backfill_plumbing")
        store.backfills.create(_ORG_ID, component_id=target, start_key="2026-01-01", end_key="2026-01-02")

    def test_backfill_rejects_a_kind_with_no_workload(self, store: Store):
        target = _component(store, kind="destination")
        with pytest.raises(ConfigError, match="cannot be run"):
            store.backfills.create(_ORG_ID, component_id=target, start_key="2026-01-01", end_key="2026-01-02")

    def test_missing_component_is_rejected(self, store: Store):
        with pytest.raises(NotFoundError, match=r"Component .* not found"):
            store.backfills.create(_ORG_ID, component_id=uuid4(), start_key="2026-01-01", end_key="2026-01-02")

    def test_target_is_loaded_with_the_backfill(self, store: Store):
        target = _component(store, kind="job")
        created = store.backfills.create(_ORG_ID, component_id=target, start_key="2026-01-01", end_key="2026-01-02")

        assert created.target is not None and created.target.key == "job"
        listed = store.backfills.list(_ORG_ID, BackfillQuery(limit=None)).items
        assert [b.target.key for b in listed if b.target] == ["job"]
        assert store.backfills.get(created.id).target is not None
        canceled = store.backfills.cancel(created.id)
        assert canceled.target is not None

    def test_backfill_runs_are_each_their_own_root(self, store: Store) -> None:
        backfill = _backfill(store)

        with Session(store.engine) as session:
            runs = session.exec(select(Run).where(Run.backfill_id == backfill.id)).all()
        assert {run.root_run_id for run in runs} == {run.id for run in runs}


class TestCreate:
    """Dispatch order: newest partition first (ITLPR-120)."""

    def test_the_newest_partitions_are_queued_first(self, store: Store):
        backfill = _backfill(store, days=4, concurrency=2)

        assert _partition_statuses(store, backfill.id) == {
            "2026-01-01": "pending",
            "2026-01-02": "pending",
            "2026-01-03": "queued",
            "2026-01-04": "queued",
        }

    def test_promotion_walks_backwards(self, store: Store):
        backfill = _backfill(store, days=4, concurrency=1)
        assert _partition_statuses(store, backfill.id)["2026-01-04"] == "queued"

        dispatched = _mark_dispatched(store, backfill.id)
        store.runs.complete(dispatched, success=True)

        statuses = _partition_statuses(store, backfill.id)
        assert statuses["2026-01-04"] == "success"
        assert statuses["2026-01-03"] == "queued"
        assert statuses["2026-01-02"] == "pending"

    def test_concurrency_beyond_the_span_queues_everything(self, store: Store):
        backfill = _backfill(store, days=2, concurrency=5)
        assert set(_partition_statuses(store, backfill.id).values()) == {"queued"}

    def test_rows_are_still_created_oldest_first(self, store: Store):
        # A runs listing orders by created_at desc, so creation order decides
        # how it reads: newest partition on top.
        backfill = _backfill(store, days=3, concurrency=1)
        with Session(store.engine) as session:
            runs = session.exec(select(Run).where(Run.backfill_id == backfill.id).order_by(col(Run.created_at))).all()
        assert [run.partition_key for run in runs] == [
            "2026-01-01",
            "2026-01-02",
            "2026-01-03",
        ]

    def test_inverted_range_is_rejected(self, store: Store):
        with pytest.raises(ConfigError, match="ends before it starts"):
            store.backfills.create(
                _ORG_ID, component_id=_component(store, kind="job"), start_key="2026-01-05", end_key="2026-01-01"
            )


class TestCreateRuns:
    """The fan-out every backfill shares: newest `concurrency` queued, the rest pending."""

    def test_queues_the_newest_partitions_and_leaves_the_rest_pending(self, store: Store):
        window = il.TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 4))
        with Session(store.engine) as session:
            db_backfill = Backfill(
                org_id=_ORG_ID, start_key="2026-01-01", end_key="2026-01-04", concurrency=2, status="running"
            )
            session.add(db_backfill)
            session.flush()

            BackfillStore._create_runs(session, db_backfill, window, billable=True)
            session.commit()
            backfill_id = db_backfill.id

        assert store.backfills.get(backfill_id).partitions == 4
        assert _partition_statuses(store, backfill_id) == {
            "2026-01-01": "pending",
            "2026-01-02": "pending",
            "2026-01-03": "queued",
            "2026-01-04": "queued",
        }

    def test_creates_rows_oldest_first(self, store: Store):
        window = il.TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 3))
        with Session(store.engine) as session:
            db_backfill = Backfill(
                org_id=_ORG_ID, start_key="2026-01-01", end_key="2026-01-03", concurrency=1, status="running"
            )
            session.add(db_backfill)
            session.flush()
            BackfillStore._create_runs(session, db_backfill, window, billable=True)
            session.commit()
            runs = session.exec(
                select(Run).where(Run.backfill_id == db_backfill.id).order_by(col(Run.created_at))
            ).all()
        assert [run.partition_key for run in runs] == ["2026-01-01", "2026-01-02", "2026-01-03"]

    def test_stamps_the_billability_it_is_handed(self, store: Store):
        window = il.TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 2))
        with Session(store.engine) as session:
            db_backfill = Backfill(
                org_id=_ORG_ID, start_key="2026-01-01", end_key="2026-01-02", concurrency=1, status="running"
            )
            session.add(db_backfill)
            session.flush()
            BackfillStore._create_runs(session, db_backfill, window, billable=False)
            session.commit()
            backfill_id = db_backfill.id

        assert {run.billable for run in _runs_of(store, backfill_id)} == {False}


class TestGranularity:
    """A backfill spans one granularity; mixed bounds fail closed."""

    def test_mixed_granularity_bounds_are_rejected(self, store: Store):
        with pytest.raises(ConfigError, match="must share one granularity"):
            store.backfills.create(
                _ORG_ID, component_id=_component(store, kind="job"), start_key="2026-01", end_key="2026-01-05"
            )

    def test_a_monthly_span_is_accepted(self, store: Store):
        backfill = store.backfills.create(
            _ORG_ID, component_id=_component(store, kind="job"), start_key="2026-01", end_key="2026-03"
        )

        assert backfill.partitions == 3


class TestGet:
    """Id-addressed reads, scoped to an organisation on request."""

    def test_get_backfill_returns_it(self, store: Store):
        backfill = _backfill(store)

        assert store.backfills.get(backfill.id).id == backfill.id

    def test_get_missing_backfill_raises(self, store: Store):
        missing = uuid4()

        with pytest.raises(NotFoundError, match=f"Backfill {missing} not found"):
            store.backfills.get(missing)

    def test_marking_a_missing_backfills_hooks_raises(self, store: Store):
        with pytest.raises(NotFoundError):
            store.backfills.mark_hooks_evaluated(uuid4())

    def test_another_orgs_backfill_reads_as_missing(self, store: Store):
        backfill = _backfill(store, days=1)

        assert store.backfills.get(backfill.id, org_id=_ORG_ID).id == backfill.id
        with pytest.raises(NotFoundError, match=f"Backfill {backfill.id} not found"):
            store.backfills.get(backfill.id, org_id=uuid4())


class TestCancel:
    def test_cancels_pending_and_queued_runs_only(self, store: Store):
        backfill = _backfill(store)  # 2 queued + 2 pending
        dispatched_id = _mark_dispatched(store, backfill.id)

        canceled = store.backfills.cancel(backfill.id)

        assert canceled.status == "canceled"
        assert canceled.completed_at is not None
        statuses = _run_statuses(store, backfill.id)
        assert statuses.pop(dispatched_id) == "dispatched"
        assert set(statuses.values()) == {"canceled"}

    def test_late_completion_does_not_resurrect_canceled_backfill(self, store: Store):
        backfill = _backfill(store)
        dispatched_id = _mark_dispatched(store, backfill.id)
        store.backfills.cancel(backfill.id)

        completed = store.runs.complete(dispatched_id, success=True)

        assert completed.status == "success"
        assert store.backfills.get(backfill.id).status == "canceled"
        # The completion must not promote canceled runs back to queued.
        statuses = _run_statuses(store, backfill.id)
        statuses.pop(dispatched_id)
        assert set(statuses.values()) == {"canceled"}

    def test_cancel_terminal_backfill_raises(self, store: Store):
        backfill = _backfill(store)
        store.backfills.cancel(backfill.id)
        with pytest.raises(ConflictError, match="already canceled"):
            store.backfills.cancel(backfill.id)

    def test_cancel_missing_backfill_raises(self, store: Store):
        with pytest.raises(NotFoundError):
            store.backfills.cancel(uuid4())


class TestList:
    """The listing pages newest first and counts what it filters."""

    def test_pages_newest_first_and_counts_the_whole_listing(self, store: Store):
        created = [_backfill(store, days=2).id for _ in range(3)]

        page = store.backfills.list(_ORG_ID, BackfillQuery(limit=2, offset=1))

        assert len(page.items) == 2
        assert {row.id for row in page.items} <= set(created)
        assert page.total == 3
        assert store.backfills.list(uuid4(), BackfillQuery()).total == 0


class TestListActive:
    """The active listing covers the two non-terminal statuses, and counts what it lists."""

    def test_running_and_queued_are_listed(self, store: Store):
        backfill = _backfill(store)

        active = store.backfills.list(_ORG_ID, BackfillQuery(status=["queued", "running"], limit=None))

        assert [row.id for row in active.items] == [backfill.id]
        assert active.total == 1

    def test_a_terminal_backfill_is_excluded(self, store: Store):
        backfill = _backfill(store)
        with Session(store.engine) as session:
            row = session.get(Backfill, backfill.id)
            assert row is not None
            row.status = BackfillStatus.SUCCESS
            session.add(row)
            session.commit()

        assert store.backfills.list(_ORG_ID, BackfillQuery(status=["queued", "running"], limit=None)).items == []

    def test_another_orgs_backfills_are_excluded(self, store: Store):
        _backfill(store)

        assert store.backfills.list(uuid4(), BackfillQuery(status=["queued", "running"], limit=None)).items == []


class TestRunCounts:
    """One partition count per status, per backfill, read off each stack's latest attempt."""

    def test_counts_each_requested_backfill_by_status(self, store: Store):
        first = _backfill(store, days=3, concurrency=2)
        second = _backfill(store, days=1, concurrency=1)
        unrequested = _backfill(store, days=1, concurrency=1)
        queued = [run_id for run_id, status in _run_statuses(store, first.id).items() if status == "queued"]
        store.runs.complete(queued[0], success=True)
        store.runs.complete(queued[1], success=False)

        counts = store.backfills.run_counts([first.id, second.id])

        assert counts == {
            first.id: {"success": 1, "failed": 1, "queued": 1},
            second.id: {"queued": 1},
        }
        assert unrequested.id not in counts
        assert store.backfills.run_counts([]) == {}

    def test_a_retried_partition_counts_once_as_its_latest_attempt(self, store: Store):
        backfill = _backfill(store, days=1, concurrency=1)
        with Session(store.engine) as session:
            first = session.exec(select(Run).where(Run.backfill_id == backfill.id)).one()
            first.status = RunStatus.FAILED
            session.add(first)
            session.add(
                Run(
                    org_id=_ORG_ID,
                    backfill_id=backfill.id,
                    partition_key=first.partition_key,
                    status="success",
                    retry_of=first.id,
                    root_run_id=first.root_run_id,
                    attempt=2,
                )
            )
            session.commit()

        assert store.backfills.run_counts([backfill.id]) == {backfill.id: {"success": 1}}


class TestFailedPartitions:
    """A backfill's failed partitions, read off each stack's latest attempt, with their errors."""

    def test_reads_the_latest_attempt_and_its_error(self, store: Store):
        backfill = _backfill(store, days=3, concurrency=3)
        by_partition = {run.partition_key: run for run in _runs_of(store, backfill.id)}
        store.runs.complete(by_partition["2026-01-01"].id, success=True)
        failed = by_partition["2026-01-02"]
        store.runs.complete(failed.id, success=False)
        _record_run_failure(store, failed.id, "boom")
        healed = by_partition["2026-01-03"]
        store.runs.complete(healed.id, success=False)
        with Session(store.engine) as session:
            session.add(
                Run(
                    org_id=_ORG_ID,
                    backfill_id=backfill.id,
                    partition_key=healed.partition_key,
                    status="success",
                    retry_of=healed.id,
                    root_run_id=healed.root_run_id,
                    attempt=2,
                )
            )
            session.commit()

        assert store.backfills.failed_partitions(backfill.id) == [("2026-01-02", "boom")]

    def test_lists_newest_first_and_tolerates_a_missing_error(self, store: Store):
        backfill = _backfill(store, days=2, concurrency=2)
        for run in _runs_of(store, backfill.id):
            store.runs.complete(run.id, success=False)

        assert store.backfills.failed_partitions(backfill.id) == [("2026-01-02", None), ("2026-01-01", None)]


class TestProgression:
    """Completing a run advances the backfill, or terminates it."""

    def test_the_backfill_succeeds_once_every_run_has(self, store: Store):
        backfill = store.backfills.create(
            _ORG_ID,
            component_id=_component(store, kind="job"),
            start_key="2026-01-01",
            end_key="2026-01-02",
            concurrency=2,
        )
        for run_id in _run_statuses(store, backfill.id):
            store.runs.complete(run_id, success=True)

        assert store.backfills.get(backfill.id).status == "success"

    def test_one_failure_without_fail_fast_still_finishes_as_failed(self, store: Store):
        backfill = store.backfills.create(
            _ORG_ID,
            component_id=_component(store, kind="job"),
            start_key="2026-01-01",
            end_key="2026-01-02",
            concurrency=2,
            fail_fast=False,
        )
        run_ids = list(_run_statuses(store, backfill.id))
        store.runs.complete(run_ids[0], success=False)
        store.runs.complete(run_ids[1], success=True)

        finished = store.backfills.get(backfill.id)
        assert finished.status == "failed"
        assert finished.completed_at is not None

    def test_fail_fast_cancels_the_pending_runs(self, store: Store):
        # The remaining partitions are not worth spending once one failed.
        backfill = store.backfills.create(
            _ORG_ID,
            component_id=_component(store, kind="job"),
            start_key="2026-01-01",
            end_key="2026-01-04",
            concurrency=1,
            fail_fast=True,
        )
        first = next(run_id for run_id, status in _run_statuses(store, backfill.id).items() if status == "queued")

        store.runs.complete(first, success=False)

        assert store.backfills.get(backfill.id).status == "failed"
        assert "pending" not in _run_statuses(store, backfill.id).values()

    def test_a_completion_promotes_the_next_partition(self, store: Store):
        backfill = store.backfills.create(
            _ORG_ID,
            component_id=_component(store, kind="job"),
            start_key="2026-01-01",
            end_key="2026-01-04",
            concurrency=1,
        )
        first = next(run_id for run_id, status in _run_statuses(store, backfill.id).items() if status == "queued")

        store.runs.complete(first, success=True)

        statuses = _partition_statuses(store, backfill.id)
        assert statuses["2026-01-04"] == "success"
        # Newest-first, matching the initial dispatch order.
        assert statuses["2026-01-03"] == "queued"

    def test_a_dispatched_run_holds_its_slot(self, store: Store):
        # Between the queue's claim and the pod's first write a run is
        # `dispatched`: still occupying its slot, not yet `running`.
        backfill = store.backfills.create(
            _ORG_ID,
            component_id=_component(store, kind="job"),
            start_key="2026-01-01",
            end_key="2026-01-04",
            concurrency=2,
        )
        _mark_dispatched(store, backfill.id)
        second = _mark_dispatched(store, backfill.id)

        store.runs.complete(second, success=True)

        statuses = _partition_statuses(store, backfill.id)
        assert list(statuses.values()).count("queued") == 1
        assert statuses["2026-01-01"] == "pending"

    def test_a_dispatched_run_keeps_the_backfill_open(self, store: Store):
        backfill = store.backfills.create(
            _ORG_ID,
            component_id=_component(store, kind="job"),
            start_key="2026-01-01",
            end_key="2026-01-02",
            concurrency=2,
        )
        _mark_dispatched(store, backfill.id)
        second = _mark_dispatched(store, backfill.id)

        store.runs.complete(second, success=True)

        assert store.backfills.get(backfill.id).status == "running"

    def test_a_completion_outside_any_backfill_is_a_no_op(self, store: Store):
        run = store.runs.create(_ORG_ID)

        store.runs.complete(run.id, success=True)

        assert store.runs.get(run.id).status == "success"


class TestStacks:
    """A batch's verdict reads each stack's latest attempt, not every attempt."""

    def _single_partition_backfill(self, store: Store, target: UUID) -> tuple[UUID, Run]:
        backfill = store.backfills.create(_ORG_ID, component_id=target, start_key="2026-01-01", end_key="2026-01-01")
        with Session(store.engine) as session:
            run = session.exec(select(Run).where(Run.backfill_id == backfill.id)).one()
        assert backfill.id is not None
        return backfill.id, run

    def _successor(self, store: Store, run_id: UUID) -> Run:
        with Session(store.engine) as session:
            return session.exec(select(Run).where(Run.retry_of == run_id)).one()

    def test_a_backfill_healed_by_a_retry_succeeds(self, store: Store) -> None:
        target = _job_with_retry(store, max_attempts=2, delay=0)
        backfill_id, first = self._single_partition_backfill(store, target)

        store.runs.complete(first.id, success=False)
        store.runs.complete(self._successor(store, first.id).id, success=True)

        assert store.backfills.get(backfill_id).status == "success"

    def test_a_backfill_whose_stack_exhausts_its_budget_fails(self, store: Store) -> None:
        target = _job_with_retry(store, max_attempts=2, delay=0)
        backfill_id, first = self._single_partition_backfill(store, target)

        store.runs.complete(first.id, success=False)
        store.runs.complete(self._successor(store, first.id).id, success=False)

        assert store.backfills.get(backfill_id).status == "failed"

    def test_a_pending_retry_keeps_the_backfill_open(self, store: Store) -> None:
        target = _job_with_retry(store, max_attempts=2, delay=0)
        backfill_id, first = self._single_partition_backfill(store, target)

        store.runs.complete(first.id, success=False)

        # The successor is queued, so the batch still has work in flight.
        assert store.backfills.get(backfill_id).status == "running"
