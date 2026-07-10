"""Tests for the queue controller (``interloper_scheduler.queue``)."""

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
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, select

from interloper_scheduler.launcher import Launcher
from interloper_scheduler.queue import QueueController

_ORG = uuid4()


class _FakeLauncher(Launcher):
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.launched: list[UUID] = []

    def launch(self, run_id: UUID) -> None:
        if self.fail:
            raise RuntimeError("no docker daemon")
        self.launched.append(run_id)


@pytest.fixture
def store() -> Iterator[Store]:
    """A store over an in-memory database with the run tables."""
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


def _statuses(store: Store) -> dict[UUID, str]:
    with Session(store.engine) as session:
        return {run.id: run.status for run in session.exec(select(Run)).all() if run.id}


def test_tick_drains_the_queue(store: Store) -> None:
    first = store.create_run(_ORG)
    second = store.create_run(_ORG)
    launcher = _FakeLauncher()

    QueueController(launcher=launcher, store=store)._tick()

    assert set(launcher.launched) == {first.id, second.id}
    assert set(_statuses(store).values()) == {"dispatched"}


def test_empty_queue_is_a_noop(store: Store) -> None:
    launcher = _FakeLauncher()
    QueueController(launcher=launcher, store=store)._tick()
    assert launcher.launched == []


def test_failed_launch_takes_the_terminal_path(store: Store) -> None:
    """A run that can't launch is completed like any failed run.

    Pins the backfill-stall fix: each failed dispatch releases its
    concurrency slot (promoting the pending sibling), and the backfill
    finalizes instead of sitting on "running" forever.
    """
    backfill = store.create_backfill(
        _ORG,
        start_date=dt.date(2026, 1, 1),
        end_date=dt.date(2026, 1, 2),
        concurrency=1,
    )
    QueueController(launcher=_FakeLauncher(fail=True), store=store)._tick()

    assert set(_statuses(store).values()) == {"failed"}
    refreshed = store.get_backfill(backfill.id)
    assert refreshed.status == "failed"
    assert refreshed.completed_at is not None
