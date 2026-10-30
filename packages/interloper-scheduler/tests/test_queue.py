"""Tests for the queue controller (``interloper_scheduler.queue``)."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.dag import DAGSpec
from interloper_assets.demo.basic.source import DemoSource
from interloper_db import Store
from interloper_db import engine as engine_module
from interloper_db.crypto import make_cipher
from interloper_db.models import Backfill, Component, ComponentRelation, Event, Execution, Quota, Run, Usage
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, select

from interloper_scheduler.launcher import Launcher
from interloper_scheduler.queue import QueueController

_ORG = uuid4()


class EmptySource(il.Source):
    """A source that declares no assets."""


@il.source(key="entities_source")
class EntitiesSource(il.Source):
    """A source observing its vendor's entities beside a dated report."""

    @il.asset(partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Entity"])
    def entities(self) -> list:
        return []

    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def report(self) -> list:
        return []


_CATALOG = il.Catalog.from_assets([DemoSource, EmptySource, EntitiesSource])


class _FakeLauncher(Launcher):
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.launched: list[tuple[UUID, DAGSpec, str]] = []

    def launch(self, run: Run, spec: DAGSpec, token: str) -> None:
        if self.fail:
            raise RuntimeError("no docker daemon")
        self.launched.append((run.id, spec, token))


@pytest.fixture
def store() -> Iterator[Store]:
    """A store over an in-memory database with the run tables and the demo source in its catalog.

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

    for model in (Component, ComponentRelation, Run, Backfill, Event, Execution, Quota, Usage):
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        encrypt, decrypt = make_cipher("test-key")
        yield Store(catalog=_CATALOG, encrypt=encrypt, decrypt=decrypt)
    finally:
        eng.dispose()
        engine_module._engine = None


def _statuses(store: Store) -> dict[UUID, str]:
    with Session(store.engine) as session:
        return {run.id: run.status for run in session.exec(select(Run)).all() if run.id}


def _source(store: Store) -> UUID:
    """Create the demo source, whose ``a -> b,c,d -> e`` assets a run executes.

    Args:
        store: The store to create it through.

    Returns:
        The source's id.
    """
    source_id = store.components.create(_ORG, kind="source", key="demo_source").id
    assert source_id is not None
    return source_id


def test_tick_hands_each_run_its_spec_and_token(store: Store) -> None:
    source_id = _source(store)
    first = store.runs.create(_ORG, component_id=source_id, partition_key="2026-10-25")
    second = store.runs.create(_ORG, component_id=source_id, partition_key="2026-10-25")
    launcher = _FakeLauncher()

    QueueController(launcher=launcher, store=store)._tick()

    assert {run_id for run_id, _spec, _token in launcher.launched} == {first.id, second.id}
    assert set(_statuses(store).values()) == {"dispatched"}
    _, spec, token = launcher.launched[0]
    assert token.startswith("ilr_")
    assert len(il.DAG.from_spec(spec, catalog=_CATALOG).operations) == 5


def test_empty_queue_is_a_noop(store: Store) -> None:
    launcher = _FakeLauncher()
    QueueController(launcher=launcher, store=store)._tick()
    assert launcher.launched == []


def test_a_failed_scope_retry_disables_the_prior_successes(store: Store) -> None:
    source_id = _source(store)
    first = store.runs.create(_ORG, component_id=source_id, partition_key="2026-10-25")
    with Session(store.engine) as session:
        asset_a = session.exec(select(Component).where(Component.parent_id == source_id, Component.key == "a")).one()
        asset_id = asset_a.id
        session.add(Execution(run_id=first.id, component_id=asset_id, org_id=_ORG, status="success", attempts=1))
        session.commit()
    store.runs.fail(first.id, "boom")
    store.runs.retry(first.id, scope="failed")
    launcher = _FakeLauncher()

    QueueController(launcher=launcher, store=store)._tick()

    ((_run_id, spec, _token),) = launcher.launched
    disabled = {
        operation.id for operation in il.DAG.from_spec(spec, catalog=_CATALOG).operations if not operation.enabled
    }
    assert str(asset_id) in disabled


def test_a_backfill_observes_its_snapshots_once_in_its_newest_run(store: Store) -> None:
    source = store.components.create(_ORG, kind="source", key="entities_source")
    store.backfills.create(_ORG, component_id=source.id, start_key="2026-10-26", end_key="2026-10-28", concurrency=3)
    launcher = _FakeLauncher()

    QueueController(launcher=launcher, store=store)._tick()

    with Session(store.engine) as session:
        keys = {run.id: run.partition_key for run in session.exec(select(Run)).all()}
    enabled = {
        keys[run_id]: sorted(
            operation.key for operation in il.DAG.from_spec(spec, catalog=_CATALOG).operations if operation.enabled
        )
        for run_id, spec, _token in launcher.launched
    }
    assert enabled == {
        "2026-10-26": ["report"],
        "2026-10-27": ["report"],
        "2026-10-28": ["entities", "report"],
    }


def test_a_run_that_cannot_be_assembled_fails_with_the_reason(store: Store) -> None:
    run = store.runs.create(_ORG)
    launcher = _FakeLauncher()

    QueueController(launcher=launcher, store=store)._tick()

    assert launcher.launched == []
    assert _statuses(store)[run.id] == "failed"
    assert "deleted" in (store.runs.get(run.id).error or "")


def test_a_run_whose_window_its_operations_refuse_fails_without_a_launch(store: Store) -> None:
    run = store.runs.create(_ORG, component_id=_source(store))
    launcher = _FakeLauncher()

    QueueController(launcher=launcher, store=store)._tick()

    assert launcher.launched == []
    failed = store.runs.get(run.id)
    assert failed.status == "failed"
    assert "not valid for every operation" in (failed.error or "")


def test_a_target_selecting_nothing_succeeds_without_a_launch(store: Store) -> None:
    empty_source = store.components.create(_ORG, kind="source", key="empty_source").id
    run = store.runs.create(_ORG, component_id=empty_source)
    launcher = _FakeLauncher()

    QueueController(launcher=launcher, store=store)._tick()

    assert launcher.launched == []
    assert _statuses(store)[run.id] == "success"


def test_failed_launch_takes_the_terminal_path(store: Store) -> None:
    """A run that can't launch is failed like any failed run, with the reason.

    Pins the backfill-stall fix: each failed dispatch releases its
    concurrency slot (promoting the pending sibling), and the backfill
    finalizes instead of sitting on "running" forever.
    """
    backfill = store.backfills.create(
        _ORG,
        component_id=_source(store),
        start_key="2026-01-01",
        end_key="2026-01-02",
        concurrency=1,
    )
    QueueController(launcher=_FakeLauncher(fail=True), store=store)._tick()

    assert set(_statuses(store).values()) == {"failed"}
    refreshed = store.backfills.get(backfill.id)
    assert refreshed.status == "failed"
    assert refreshed.completed_at is not None


def test_launch_emits_a_span_per_claimed_run(store: Store, span_exporter: Any) -> None:
    store.runs.create(_ORG, component_id=_source(store), partition_key="2026-10-25")
    launcher = _FakeLauncher()

    QueueController(launcher=launcher, store=store)._tick()

    spans = [s for s in span_exporter.get_finished_spans() if s.name == "interloper.launcher.launch"]
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes["interloper.run.id"] == str(launcher.launched[0][0])
    assert spans[0].attributes["interloper.launcher.type"] == "_FakeLauncher"


def test_empty_tick_emits_no_launch_spans(store: Store, span_exporter: Any) -> None:
    QueueController(launcher=_FakeLauncher(), store=store)._tick()
    assert not [s for s in span_exporter.get_finished_spans() if s.name == "interloper.launcher.launch"]
