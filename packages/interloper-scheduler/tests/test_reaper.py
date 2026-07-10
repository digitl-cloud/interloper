"""Tests for the reaper (``interloper_scheduler.reaper``)."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper_db import Store
from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Component, ComponentRelation, Run
from interloper_db.models import Event as EventRow
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session

from interloper_scheduler.launcher import Launcher, RunState, RunStatus
from interloper_scheduler.reaper import Reaper

_ORG = uuid4()


class _FakeLauncher(Launcher):
    """Answers ``describe_run`` with one canned state."""

    def __init__(self, state: RunState | None) -> None:
        self._state = state

    def launch(self, run_id: UUID) -> None:  # pragma: no cover - unused
        raise NotImplementedError

    def describe_run(self, run_id: UUID) -> RunState | None:
        return self._state


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> Iterator[Store]:
    """A store over an in-memory database, with a SQLite-friendly save_event."""
    eng = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(eng, "connect")
    def _sqlite_uuid(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    for model in (Component, ComponentRelation, Run, Backfill, EventRow):
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]

    store = Store(catalog=il.Catalog(components={}))

    def sqlite_save_event(event: il.Event, org_id: UUID, run_id: UUID | None = None) -> EventRow:
        row = EventRow(
            id=UUID(event.id),
            org_id=org_id,
            run_id=run_id,
            event_type=event.type.value,
            error=event.metadata.get("error"),
            timestamp=event.timestamp,
        )
        with Session(eng) as session:
            session.add(row)
            session.commit()
        return row

    monkeypatch.setattr(store, "save_event", sqlite_save_event)
    try:
        yield store
    finally:
        eng.dispose()
        engine_module._engine = None


def _dispatched_run(store: Store, *, age_seconds: int = 0) -> UUID:
    run = store.create_run(_ORG)
    assert run.id is not None
    with Session(store.engine) as session:
        db_run = session.get(Run, run.id)
        assert db_run is not None
        db_run.status = "dispatched"
        db_run.created_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=age_seconds)
        session.add(db_run)
        session.commit()
    return run.id


def _status(store: Store, run_id: UUID) -> str:
    return store.get_run(run_id).status


class TestLauncherTruth:
    def test_running_is_left_alone(self, store: Store) -> None:
        run_id = _dispatched_run(store)
        reaper = Reaper(store=store, launcher=_FakeLauncher(RunState(status=RunStatus.RUNNING)))
        assert reaper._reap() == 0
        assert _status(store, run_id) == "dispatched"

    def test_failed_is_reaped_with_the_launcher_error(self, store: Store) -> None:
        run_id = _dispatched_run(store)
        state = RunState(status=RunStatus.FAILED, error="OOMKilled")
        assert Reaper(store=store, launcher=_FakeLauncher(state))._reap() == 1
        assert _status(store, run_id) == "failed"

    def test_succeeded_without_db_update_is_reaped(self, store: Store) -> None:
        run_id = _dispatched_run(store)
        state = RunState(status=RunStatus.SUCCEEDED)
        assert Reaper(store=store, launcher=_FakeLauncher(state))._reap() == 1
        assert _status(store, run_id) == "failed"

    def test_not_found_waits_for_the_timeout(self, store: Store) -> None:
        fresh = _dispatched_run(store)
        stale = _dispatched_run(store, age_seconds=1200)
        reaper = Reaper(store=store, launcher=_FakeLauncher(RunState(status=RunStatus.NOT_FOUND)), timeout=600)
        assert reaper._reap() == 1
        assert _status(store, fresh) == "dispatched"
        assert _status(store, stale) == "failed"


class TestTimeoutFallback:
    def test_blind_launcher_reaps_on_timeout_only(self, store: Store) -> None:
        fresh = _dispatched_run(store)
        stale = _dispatched_run(store, age_seconds=1200)
        reaper = Reaper(store=store, launcher=None, timeout=600)
        assert reaper._reap() == 1
        assert _status(store, fresh) == "dispatched"
        assert _status(store, stale) == "failed"
