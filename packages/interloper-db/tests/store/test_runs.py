"""Tests for the run/backfill lifecycle methods in ``RunMixin`` (``store/runs.py``).

These run against an in-memory SQLite database (only the runs/backfills
tables) so status transitions are exercised against real SQL.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from typing import Any
from uuid import UUID, uuid4

import pytest
from interloper.errors import NotFoundError
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, col, select

from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Quota, Run, Usage
from interloper_db.store.runs import RunMixin

_ORG_ID = uuid4()


@pytest.fixture
def store() -> Iterator[RunMixin]:
    """A RunMixin wired to a fresh in-memory SQLite database."""
    eng = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(eng, "connect")
    def _sqlite_uuid(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    for model in (Backfill, Run, Quota, Usage):
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        mixin = RunMixin()
        mixin._engine = eng
        yield mixin
    finally:
        eng.dispose()
        engine_module._engine = None


def _backfill(store: RunMixin, *, days: int = 4, concurrency: int = 2) -> Backfill:
    return store.create_backfill(
        _ORG_ID,
        start_key="2026-01-01",
        end_key=f"2026-01-{days:02d}",
        concurrency=concurrency,
    )


def _mark_dispatched(store: RunMixin, backfill_id: UUID) -> UUID:
    """Flip one queued run to dispatched, simulating a worker claim."""
    with Session(store._engine) as session:
        run = session.exec(select(Run).where(Run.backfill_id == backfill_id, Run.status == "queued")).first()
        assert run is not None and run.id is not None
        run.status = "dispatched"
        session.add(run)
        session.commit()
        return run.id


def _run_statuses(store: RunMixin, backfill_id: UUID) -> dict[UUID, str]:
    with Session(store._engine) as session:
        runs = session.exec(select(Run).where(Run.backfill_id == backfill_id)).all()
        return {run.id: run.status for run in runs if run.id}


def _partition_statuses(store: RunMixin, backfill_id: UUID) -> dict[str, str]:
    with Session(store._engine) as session:
        runs = session.exec(select(Run).where(Run.backfill_id == backfill_id)).all()
        return {run.partition_key: run.status for run in runs if run.partition_key}


class TestCreateBackfill:
    """Dispatch order: newest partition first (ITLPR-120)."""

    def test_the_newest_partitions_are_queued_first(self, store: RunMixin):
        backfill = _backfill(store, days=4, concurrency=2)

        assert _partition_statuses(store, backfill.id) == {
            "2026-01-01": "pending",
            "2026-01-02": "pending",
            "2026-01-03": "queued",
            "2026-01-04": "queued",
        }

    def test_promotion_walks_backwards(self, store: RunMixin):
        backfill = _backfill(store, days=4, concurrency=1)
        assert _partition_statuses(store, backfill.id)["2026-01-04"] == "queued"

        dispatched = _mark_dispatched(store, backfill.id)
        store.complete_run(dispatched, success=True)

        statuses = _partition_statuses(store, backfill.id)
        assert statuses["2026-01-04"] == "success"
        assert statuses["2026-01-03"] == "queued"
        assert statuses["2026-01-02"] == "pending"

    def test_concurrency_beyond_the_span_queues_everything(self, store: RunMixin):
        backfill = _backfill(store, days=2, concurrency=5)
        assert set(_partition_statuses(store, backfill.id).values()) == {"queued"}

    def test_rows_are_still_created_oldest_first(self, store: RunMixin):
        # `list_runs` orders by created_at desc, so creation order decides how
        # the runs list reads: newest partition on top.
        backfill = _backfill(store, days=3, concurrency=1)
        with Session(store._engine) as session:
            runs = session.exec(
                select(Run).where(Run.backfill_id == backfill.id).order_by(col(Run.created_at))
            ).all()
        assert [run.partition_key for run in runs] == [
            "2026-01-01",
            "2026-01-02",
            "2026-01-03",
        ]

    def test_inverted_range_is_rejected(self, store: RunMixin):
        with pytest.raises(ValueError, match="ends before it starts"):
            store.create_backfill(_ORG_ID, start_key="2026-01-05", end_key="2026-01-01")


class TestCancelBackfill:
    def test_cancels_pending_and_queued_runs_only(self, store: RunMixin):
        backfill = _backfill(store)  # 2 queued + 2 pending
        dispatched_id = _mark_dispatched(store, backfill.id)

        canceled = store.cancel_backfill(backfill.id)

        assert canceled.status == "canceled"
        assert canceled.completed_at is not None
        statuses = _run_statuses(store, backfill.id)
        assert statuses.pop(dispatched_id) == "dispatched"
        assert set(statuses.values()) == {"canceled"}

    def test_late_completion_does_not_resurrect_canceled_backfill(self, store: RunMixin):
        backfill = _backfill(store)
        dispatched_id = _mark_dispatched(store, backfill.id)
        store.cancel_backfill(backfill.id)

        completed = store.complete_run(dispatched_id, success=True)

        assert completed.status == "success"
        assert store.get_backfill(backfill.id).status == "canceled"
        # The completion must not promote canceled runs back to queued.
        statuses = _run_statuses(store, backfill.id)
        statuses.pop(dispatched_id)
        assert set(statuses.values()) == {"canceled"}

    def test_cancel_terminal_backfill_raises(self, store: RunMixin):
        backfill = _backfill(store)
        store.cancel_backfill(backfill.id)
        with pytest.raises(ValueError, match="already canceled"):
            store.cancel_backfill(backfill.id)

    def test_cancel_missing_backfill_raises(self, store: RunMixin):
        with pytest.raises(NotFoundError):
            store.cancel_backfill(uuid4())


def _H(hours: int) -> dt.timedelta:
    return dt.timedelta(hours=hours)


def _timed_run(
    store: RunMixin,
    *,
    started_at: dt.datetime | None,
    completed_at: dt.datetime | None,
    org_id: UUID = _ORG_ID,
) -> UUID:
    """Insert a run occupying a known interval."""
    with Session(store._engine) as session:
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

    def test_overlapping_runs_only(self, store: RunMixin):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        before_window = _timed_run(store, started_at=base - _H(3), completed_at=base - _H(2))
        straddling_start = _timed_run(store, started_at=base - _H(2), completed_at=base + _H(1))
        inside = _timed_run(store, started_at=base + _H(2), completed_at=base + _H(3))
        after_window = _timed_run(store, started_at=base + _H(6), completed_at=base + _H(7))

        found = store.list_runs(_ORG_ID, after=base, before=base + _H(4), limit=100)

        assert {r.id for r in found} == {straddling_start, inside}
        assert before_window not in {r.id for r in found}
        assert after_window not in {r.id for r in found}

    def test_running_run_is_open_ended(self, store: RunMixin):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        running = _timed_run(store, started_at=base - _H(5), completed_at=None)

        found = store.list_runs(_ORG_ID, after=base, before=base + _H(1), limit=100)

        assert [r.id for r in found] == [running]

    def test_never_started_runs_are_excluded(self, store: RunMixin):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        _timed_run(store, started_at=None, completed_at=None)

        assert store.list_runs(_ORG_ID, after=base, before=base + _H(1), limit=100) == []
        assert store.list_runs(_ORG_ID, after=base, limit=100) == []
        assert store.count_runs(_ORG_ID, after=base) == 0

    def test_count_matches_the_same_window(self, store: RunMixin):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        _timed_run(store, started_at=base + _H(1), completed_at=base + _H(2))
        _timed_run(store, started_at=base + _H(9), completed_at=base + _H(10))

        assert store.count_runs(_ORG_ID, after=base, before=base + _H(4)) == 1

    def test_unbounded_listing_keeps_every_run(self, store: RunMixin):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        _timed_run(store, started_at=base, completed_at=base + _H(1))
        _timed_run(store, started_at=None, completed_at=None)

        assert len(store.list_runs(_ORG_ID, limit=100)) == 2
