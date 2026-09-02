"""Tests for the queue controller (``interloper_scheduler.queue``)."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper_db import Store
from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Component, ComponentRelation, Event, Quota, Run, Usage
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
    """A store over an in-memory database with the run tables.

    Yields:
        The store bound to that database, disposed once the test finishes.
    """
    eng = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(eng, "connect")
    def _sqlite_uuid(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    for model in (Component, ComponentRelation, Run, Backfill, Event, Quota, Usage):
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
    first = store.runs.create(_ORG)
    second = store.runs.create(_ORG)
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
    backfill = store.runs.create_backfill(
        _ORG,
        start_key="2026-01-01",
        end_key="2026-01-02",
        concurrency=1,
    )
    QueueController(launcher=_FakeLauncher(fail=True), store=store)._tick()

    assert set(_statuses(store).values()) == {"failed"}
    refreshed = store.runs.get_backfill(backfill.id)
    assert refreshed.status == "failed"
    assert refreshed.completed_at is not None


def test_launch_emits_a_span_per_claimed_run(store: Store, span_exporter: Any) -> None:
    store.runs.create(_ORG)
    launcher = _FakeLauncher()

    QueueController(launcher=launcher, store=store)._tick()

    spans = [s for s in span_exporter.get_finished_spans() if s.name == "interloper.launcher.launch"]
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes["interloper.run.id"] == str(launcher.launched[0])
    assert spans[0].attributes["interloper.launcher.type"] == "_FakeLauncher"


def test_empty_tick_emits_no_launch_spans(store: Store, span_exporter: Any) -> None:
    QueueController(launcher=_FakeLauncher(), store=store)._tick()
    assert not [s for s in span_exporter.get_finished_spans() if s.name == "interloper.launcher.launch"]


# -- Run quota at dispatch -----------------------------------------------------


def _exhaust_quota(store: Store, limit: int = 1) -> None:
    """Give the org a limit and a ledger already at it."""
    from types import SimpleNamespace

    from interloper_db.store.quotas import METRIC_SUCCESSFUL_RUNS, UsageLedger

    store._quota_defaults = SimpleNamespace(max_successful_runs_per_month=limit)
    with Session(store.engine) as session:
        ledger = UsageLedger(session)
        ledger.increment(_ORG, METRIC_SUCCESSFUL_RUNS, ledger.current_period(), used=limit)
        session.commit()


def test_dispatch_reserves_a_quota_slot(store: Store) -> None:
    from types import SimpleNamespace

    from interloper_db.models import Usage

    store._quota_defaults = SimpleNamespace(max_successful_runs_per_month=5)
    run = store.runs.create(_ORG)
    launcher = _FakeLauncher()

    QueueController(launcher=launcher, store=store)._tick()

    assert launcher.launched == [run.id]
    with Session(store.engine) as session:
        dispatched = session.get(Run, run.id)
        assert dispatched is not None and dispatched.quota_reserved_at is not None
        usage = session.exec(select(Usage)).one()
        assert (usage.used, usage.reserved) == (0, 1)


def test_quota_denied_claim_cancels_instead_of_blocking(store: Store) -> None:
    first = store.runs.create(_ORG)
    second = store.runs.create(_ORG)
    _exhaust_quota(store)
    launcher = _FakeLauncher()

    QueueController(launcher=launcher, store=store)._tick()

    assert launcher.launched == []
    statuses = _statuses(store)
    assert statuses[first.id] == "canceled"
    assert statuses[second.id] == "canceled"
    with Session(store.engine) as session:
        from interloper_db.models import Event

        messages = [e.message for e in session.exec(select(Event)).all()]
    assert len(messages) == 2 and all(m and "quota" in m for m in messages)


def test_quota_denied_backfill_run_cancels_the_whole_backfill(store: Store) -> None:
    backfill = store.runs.create_backfill(
        _ORG,
        start_key="2026-01-01",
        end_key="2026-01-03",
        concurrency=1,
    )
    _exhaust_quota(store)
    launcher = _FakeLauncher()

    QueueController(launcher=launcher, store=store)._tick()

    assert launcher.launched == []
    assert set(_statuses(store).values()) == {"canceled"}
    refreshed = store.runs.get_backfill(backfill.id)
    assert refreshed.status == "canceled"
    assert refreshed.completed_at is not None


def test_unlimited_org_dispatches_without_touching_the_ledger(store: Store) -> None:
    from interloper_db.models import Usage

    run = store.runs.create(_ORG)
    launcher = _FakeLauncher()

    QueueController(launcher=launcher, store=store)._tick()

    assert launcher.launched == [run.id]
    with Session(store.engine) as session:
        assert session.exec(select(Usage)).all() == []
