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
from sqlmodel import Session, select

from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Run, Usage
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

    Backfill.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    Run.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    Usage.__table__.create(eng)  # ty: ignore[unresolved-attribute]  (complete_run settles usage)
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
        start_date=dt.date(2026, 1, 1),
        end_date=dt.date(2026, 1, days),
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
