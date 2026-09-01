"""Tests for the run/backfill lifecycle methods in ``RunStore`` (``store/runs.py``).

These run against an in-memory SQLite database (only the runs/backfills
tables) so status transitions are exercised against real SQL.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from typing import Any, ClassVar
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.errors import NotFoundError
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, col, select

from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Component, Quota, Run, Usage
from interloper_db.store import Store

_ORG_ID = uuid4()


class FakePlumbing(il.Component, il.Operation):
    """Test-only kind whose operation is platform plumbing (non-billable)."""

    billable: ClassVar[bool] = False

    async def execute(self, context: il.OperationContext) -> il.OperationResult:
        """Do nothing.

        Args:
            context: The platform-provided execution context, unused.

        Returns:
            An effectless success.
        """
        return il.OperationResult()


il.KINDS.register(FakePlumbing.kind, FakePlumbing.anchor())


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

    for model in (Backfill, Component, Run, Quota, Usage):
        model.__table__.create(engine)  # ty: ignore[unresolved-attribute]
    try:
        yield Store(catalog=il.Catalog(components={}), engine=engine)
    finally:
        engine.dispose()
        engine_module._engine = None


def _backfill(store: Store, *, days: int = 4, concurrency: int = 2) -> Backfill:
    return store.runs.create_backfill(
        _ORG_ID,
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
        run.status = "dispatched"
        session.add(run)
        session.commit()
        return run.id


def _run_statuses(store: Store, backfill_id: UUID) -> dict[UUID, str]:
    with Session(store.engine) as session:
        runs = session.exec(select(Run).where(Run.backfill_id == backfill_id)).all()
        return {run.id: run.status for run in runs if run.id}


def _partition_statuses(store: Store, backfill_id: UUID) -> dict[str, str]:
    with Session(store.engine) as session:
        runs = session.exec(select(Run).where(Run.backfill_id == backfill_id)).all()
        return {run.partition_key: run.status for run in runs if run.partition_key}


def _component(store: Store, kind: str) -> UUID:
    with Session(store.engine) as session:
        row = Component(id=uuid4(), org_id=_ORG_ID, kind=kind, key=kind, name=kind)
        session.add(row)
        session.commit()
        assert row.id is not None
        return row.id


class TestRunTargetOperations:
    """Run creation validates the target's operation and records billability."""

    def test_kind_without_operation_is_rejected(self, store: Store):
        target = _component(store, kind="destination")
        with pytest.raises(ValueError, match="cannot be run"):
            store.runs.create(_ORG_ID, component_id=target)

    def test_missing_component_is_rejected(self, store: Store):
        with pytest.raises(NotFoundError):
            store.runs.create(_ORG_ID, component_id=uuid4())

    def test_billable_recorded_from_the_operation(self, store: Store):
        target = _component(store, kind="fake_plumbing")
        run = store.runs.create(_ORG_ID, component_id=target)
        assert run.billable is False


class TestTargetResolution:
    """Runs carry their target component, eagerly joined and deletion-aware."""

    def test_target_is_loaded_with_the_run(self, store: Store):
        target = _component(store, kind="job")
        created = store.runs.create(_ORG_ID, component_id=target)

        # Accessed after the store call returns (its session is closed), so
        # this only works if the join is eager.
        run = store.runs.get(created.id)
        assert run.target is not None
        assert (run.target.kind, run.target.key, run.target.name) == ("job", "job", "job")

        listed = store.runs.list_all(_ORG_ID)
        assert [r.target.key for r in listed if r.target] == ["job"]

    def test_deleted_target_resolves_to_none(self, store: Store):
        target = _component(store, kind="job")
        created = store.runs.create(_ORG_ID, component_id=target)

        # Mirror the FK's ON DELETE SET NULL by hand — SQLite does not
        # enforce it without the foreign_keys pragma.
        with Session(store.engine) as session:
            run_row = session.get(Run, created.id)
            component = session.get(Component, target)
            assert run_row is not None and component is not None
            run_row.component_id = None
            session.add(run_row)
            session.delete(component)
            session.commit()

        run = store.runs.get(created.id)
        assert run.component_id is None
        assert run.target is None

    def test_retry_copies_the_record(self, store: Store):
        target = _component(store, kind="fake_plumbing")
        run = store.runs.create(_ORG_ID, component_id=target)
        store.runs.complete(run.id, success=False)

        retry = store.runs.retry(run.id)

        assert retry.billable is False

    def test_backfill_rejects_a_kind_with_no_workload(self, store: Store):
        target = _component(store, kind="destination")
        with pytest.raises(ValueError, match="cannot be run"):
            store.runs.create_backfill(_ORG_ID, component_id=target, start_key="2026-01-01", end_key="2026-01-02")


class TestCreateBackfill:
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
        # `list_runs` orders by created_at desc, so creation order decides how
        # the runs list reads: newest partition on top.
        backfill = _backfill(store, days=3, concurrency=1)
        with Session(store.engine) as session:
            runs = session.exec(
                select(Run).where(Run.backfill_id == backfill.id).order_by(col(Run.created_at))
            ).all()
        assert [run.partition_key for run in runs] == [
            "2026-01-01",
            "2026-01-02",
            "2026-01-03",
        ]

    def test_inverted_range_is_rejected(self, store: Store):
        with pytest.raises(ValueError, match="ends before it starts"):
            store.runs.create_backfill(_ORG_ID, start_key="2026-01-05", end_key="2026-01-01")


class TestCancelBackfill:
    def test_cancels_pending_and_queued_runs_only(self, store: Store):
        backfill = _backfill(store)  # 2 queued + 2 pending
        dispatched_id = _mark_dispatched(store, backfill.id)

        canceled = store.runs.cancel_backfill(backfill.id)

        assert canceled.status == "canceled"
        assert canceled.completed_at is not None
        statuses = _run_statuses(store, backfill.id)
        assert statuses.pop(dispatched_id) == "dispatched"
        assert set(statuses.values()) == {"canceled"}

    def test_late_completion_does_not_resurrect_canceled_backfill(self, store: Store):
        backfill = _backfill(store)
        dispatched_id = _mark_dispatched(store, backfill.id)
        store.runs.cancel_backfill(backfill.id)

        completed = store.runs.complete(dispatched_id, success=True)

        assert completed.status == "success"
        assert store.runs.get_backfill(backfill.id).status == "canceled"
        # The completion must not promote canceled runs back to queued.
        statuses = _run_statuses(store, backfill.id)
        statuses.pop(dispatched_id)
        assert set(statuses.values()) == {"canceled"}

    def test_cancel_terminal_backfill_raises(self, store: Store):
        backfill = _backfill(store)
        store.runs.cancel_backfill(backfill.id)
        with pytest.raises(ValueError, match="already canceled"):
            store.runs.cancel_backfill(backfill.id)

    def test_cancel_missing_backfill_raises(self, store: Store):
        with pytest.raises(NotFoundError):
            store.runs.cancel_backfill(uuid4())


def _H(hours: int) -> dt.timedelta:
    return dt.timedelta(hours=hours)


def _timed_run(
    store: Store,
    *,
    started_at: dt.datetime | None,
    completed_at: dt.datetime | None,
    org_id: UUID = _ORG_ID,
) -> UUID:
    """Insert a run occupying a known interval.

    Returns:
        The id of the inserted run.
    """
    with Session(store.engine) as session:
        run = Run(
            id=uuid4(),
            org_id=org_id,
            status="success" if completed_at else "running",
            started_at=started_at,
            completed_at=completed_at,
        )
        session.add(run)
        session.commit()
        return run.id


class TestListRunsWindow:
    """`after`/`before` select runs whose execution overlaps the window."""

    def test_overlapping_runs_only(self, store: Store):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        before_window = _timed_run(store, started_at=base - _H(3), completed_at=base - _H(2))
        straddling_start = _timed_run(store, started_at=base - _H(2), completed_at=base + _H(1))
        inside = _timed_run(store, started_at=base + _H(2), completed_at=base + _H(3))
        after_window = _timed_run(store, started_at=base + _H(6), completed_at=base + _H(7))

        found = store.runs.list_all(_ORG_ID, after=base, before=base + _H(4), limit=100)

        assert {r.id for r in found} == {straddling_start, inside}
        assert before_window not in {r.id for r in found}
        assert after_window not in {r.id for r in found}

    def test_running_run_is_open_ended(self, store: Store):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        running = _timed_run(store, started_at=base - _H(5), completed_at=None)

        found = store.runs.list_all(_ORG_ID, after=base, before=base + _H(1), limit=100)

        assert [r.id for r in found] == [running]

    def test_never_started_runs_are_excluded(self, store: Store):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        _timed_run(store, started_at=None, completed_at=None)

        assert store.runs.list_all(_ORG_ID, after=base, before=base + _H(1), limit=100) == []
        assert store.runs.list_all(_ORG_ID, after=base, limit=100) == []
        assert store.runs.count(_ORG_ID, after=base) == 0

    def test_count_matches_the_same_window(self, store: Store):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        _timed_run(store, started_at=base + _H(1), completed_at=base + _H(2))
        _timed_run(store, started_at=base + _H(9), completed_at=base + _H(10))

        assert store.runs.count(_ORG_ID, after=base, before=base + _H(4)) == 1

    def test_unbounded_listing_keeps_every_run(self, store: Store):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        _timed_run(store, started_at=base, completed_at=base + _H(1))
        _timed_run(store, started_at=None, completed_at=None)

        assert len(store.runs.list_all(_ORG_ID, limit=100)) == 2
