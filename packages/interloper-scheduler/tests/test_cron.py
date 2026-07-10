"""Tests for the cron controller (``interloper_scheduler.cron``).

SQLite stands in for Postgres (``with_for_update`` is ignored there, which
is fine — the locking semantics are Postgres-only machinery, the scheduling
logic is what these tests pin).
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.errors import ConfigError
from interloper_db import Store
from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Component, ComponentRelation, Run
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, select

from interloper_scheduler.cron import CronController

_ORG = uuid4()


@pytest.fixture
def store() -> Iterator[Store]:
    """A store over an in-memory database with the scheduling tables."""
    eng = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(eng, "connect")
    def _sqlite_uuid(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    for model in (Component, ComponentRelation, Run, Backfill):
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        yield Store(catalog=il.Catalog(components={}))
    finally:
        eng.dispose()
        engine_module._engine = None


def _job(store: Store, *, config: dict[str, Any], state: dict[str, Any] | None = None) -> UUID:
    row = store.create_component(_ORG, kind="job", key="cron_job", name="J", config=config)
    if state is not None:
        with Session(store.engine) as session:
            db_job = session.get(Component, row.id)
            assert db_job is not None
            db_job.state = state
            session.add(db_job)
            session.commit()
    assert row.id is not None
    return row.id


def _runs(store: Store) -> list[Run]:
    with Session(store.engine) as session:
        return list(session.exec(select(Run)).all())


def _state(store: Store, job_id: UUID) -> dict[str, Any]:
    with Session(store.engine) as session:
        db_job = session.get(Component, job_id)
        assert db_job is not None
        return dict(db_job.state or {})


class TestScheduling:
    def test_new_job_is_scheduled_but_not_run(self, store: Store) -> None:
        job_id = _job(store, config={"cron": "0 * * * *", "enabled": True})
        CronController(store=store)._tick()
        assert _runs(store) == []
        assert _state(store, job_id)["next_run_at"] > dt.datetime.now(dt.timezone.utc).isoformat()

    def test_due_job_creates_a_queued_run_and_reschedules(self, store: Store) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        job_id = _job(
            store,
            config={"cron": "0 * * * *", "enabled": True},
            state={"next_run_at": now.isoformat()},
        )
        CronController(store=store)._tick()
        runs = _runs(store)
        assert [run.status for run in runs] == ["queued"]
        assert runs[0].component_id == job_id
        assert _state(store, job_id)["next_run_at"] > now.isoformat()

    def test_too_late_job_is_skipped_but_rescheduled(self, store: Store) -> None:
        stale = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)
        job_id = _job(
            store,
            config={"cron": "0 * * * *", "enabled": True},
            state={"next_run_at": stale.isoformat()},
        )
        CronController(store=store, max_execution_delay=60)._tick()
        assert _runs(store) == []
        assert _state(store, job_id)["next_run_at"] > stale.isoformat()

    def test_disabled_job_is_ignored(self, store: Store) -> None:
        _job(store, config={"cron": "0 * * * *", "enabled": False})
        CronController(store=store)._tick()
        assert _runs(store) == []

    def test_partitioned_job_creates_a_backfill_window(self, store: Store) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        job_id = _job(
            store,
            config={"cron": "0 * * * *", "enabled": True, "partitioned": True, "backfill_days": 3},
            state={"next_run_at": now.isoformat()},
        )
        CronController(store=store)._tick()

        with Session(store.engine) as session:
            backfill = session.exec(select(Backfill)).one()
        assert backfill.component_id == job_id
        assert backfill.partitions == 3
        assert backfill.end_date == now.date() - dt.timedelta(days=1)
        assert backfill.start_date == backfill.end_date - dt.timedelta(days=2)
        runs = _runs(store)
        assert len(runs) == 3
        assert all(run.status == "queued" and run.backfill_id == backfill.id for run in runs)


class TestConfig:
    def test_delay_must_cover_the_reconcile_interval(self, store: Store) -> None:
        with pytest.raises(ConfigError, match="max_execution_delay"):
            CronController(store=store, reconcile_interval=10, max_execution_delay=5)
