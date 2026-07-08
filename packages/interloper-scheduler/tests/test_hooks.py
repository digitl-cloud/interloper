"""Tests for the hook evaluator (``interloper_scheduler.hooks``).

SQLite stands in for Postgres; ``save_event`` (a pg-dialect upsert) is
replaced with a plain insert so the claim-dedup reads work.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper_assets.demo.source import DemoSource, demo_asset
from interloper_db import Store
from interloper_db import engine as engine_module
from interloper_db.models import Component, ComponentRelation, Run
from interloper_db.models import Event as EventRow
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, select

from interloper_scheduler.hooks import HookController

_ORG = uuid4()
_PAST = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)


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

    for model in (Component, ComponentRelation, Run, EventRow):
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]

    store = Store(catalog=il.Catalog.from_assets([DemoSource, demo_asset]))

    def sqlite_save_event(event: il.Event, org_id: UUID, run_id: UUID | None = None) -> EventRow:
        row = EventRow(
            id=UUID(event.id),
            org_id=org_id,
            run_id=run_id,
            event_type=event.type.value,
            message=event.metadata.get("message"),
            error=event.metadata.get("error"),
            timestamp=event.timestamp,
        )
        with Session(eng) as session:
            if session.get(EventRow, row.id) is None:
                session.add(row)
                session.commit()
        return row

    monkeypatch.setattr(store, "save_event", sqlite_save_event)
    try:
        yield store
    finally:
        eng.dispose()
        engine_module._engine = None


def _terminal_run(store: Store, component_id: UUID, *, status: str = "success") -> Run:
    run = store.create_run(_ORG, component_id=component_id, partition_date=dt.date(2026, 7, 6))
    with Session(engine_module.get_engine()) as session:
        db_run = session.get(Run, run.id)
        assert db_run is not None
        db_run.status = status
        db_run.completed_at = dt.datetime.now(dt.timezone.utc)
        session.add(db_run)
        session.commit()
        session.refresh(db_run)
        return db_run


def _sweep(store: Store) -> HookController:
    controller = HookController(store=store, poll_interval=999)
    controller._watermark = _PAST
    controller._process()
    return controller


class TestHookEvaluation:
    def test_trigger_hook_cascades_with_partition(self, store: Store):
        source = store.create_component(_ORG, kind="source", key="demo_source", name="Demo")
        leaf = next(c for c in source.children if c.key == "e")
        root = next(c for c in source.children if c.key == "a")
        store.create_component(
            _ORG, kind="hook", key="trigger_hook", name="Cascade",
            config={"events": ["run_completed"]},
            relations={"watch": [(leaf.id, "")], "target": [(root.id, "")]},
        )
        run = _terminal_run(store, leaf.id)

        _sweep(store)

        with Session(engine_module.get_engine()) as session:
            queued = session.exec(select(Run).where(Run.status == "queued")).all()
            assert len(queued) == 1
            assert queued[0].component_id == root.id
            assert queued[0].partition_date == run.partition_date
            events = session.exec(select(EventRow)).all()
            assert [e.event_type for e in events] == ["hook_fired"]
            assert events[0].run_id == run.id

    def test_claim_prevents_refiring(self, store: Store):
        source = store.create_component(_ORG, kind="source", key="demo_source", name="Demo")
        leaf = next(c for c in source.children if c.key == "e")
        root = next(c for c in source.children if c.key == "a")
        store.create_component(
            _ORG, kind="hook", key="trigger_hook", name="Cascade",
            config={"events": ["run_completed"]},
            relations={"watch": [(leaf.id, "")], "target": [(root.id, "")]},
        )
        _terminal_run(store, leaf.id)

        controller = _sweep(store)
        controller._watermark = _PAST
        controller._process()  # second sweep over the same window

        with Session(engine_module.get_engine()) as session:
            assert len(session.exec(select(Run).where(Run.status == "queued")).all()) == 1
            assert len(session.exec(select(EventRow)).all()) == 1

    def test_event_type_mismatch_does_not_fire(self, store: Store):
        source = store.create_component(_ORG, kind="source", key="demo_source", name="Demo")
        leaf = next(c for c in source.children if c.key == "e")
        root = next(c for c in source.children if c.key == "a")
        store.create_component(
            _ORG, kind="hook", key="trigger_hook", name="OnFailureOnly",
            config={"events": ["run_failed"]},
            relations={"watch": [(leaf.id, "")], "target": [(root.id, "")]},
        )
        _terminal_run(store, leaf.id, status="success")

        _sweep(store)

        with Session(engine_module.get_engine()) as session:
            assert session.exec(select(Run).where(Run.status == "queued")).all() == []
            assert session.exec(select(EventRow)).all() == []

    def test_watching_parent_matches_child_run(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        import httpx

        posted: list[str] = []

        def fake_post(url: str, **kwargs: Any) -> httpx.Response:
            posted.append(url)
            return httpx.Response(200, request=httpx.Request("POST", url))

        monkeypatch.setattr(httpx, "post", fake_post)

        source = store.create_component(_ORG, kind="source", key="demo_source", name="Demo")
        child = next(c for c in source.children if c.key == "a")
        store.create_component(
            _ORG, kind="hook", key="webhook_hook", name="OnAnyAsset",
            config={"events": ["run_completed"], "url": "https://example.test/n"},
            relations={"watch": [(source.id, "")]},
        )
        _terminal_run(store, child.id)

        _sweep(store)

        assert posted == ["https://example.test/n"]

    def test_failure_recorded_on_claim(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        import httpx

        def boom(*args: Any, **kwargs: Any) -> None:
            raise httpx.ConnectError("no route")

        monkeypatch.setattr(httpx, "post", boom)

        source = store.create_component(_ORG, kind="source", key="demo_source", name="Demo")
        store.create_component(
            _ORG, kind="hook", key="webhook_hook", name="Notify",
            config={"events": ["run_failed"], "url": "https://example.test/x"},
            relations={"watch": [(source.id, "")]},
        )
        run = _terminal_run(store, source.id, status="failed")

        _sweep(store)
        _sweep(store)  # failure claim is terminal: no retry

        with Session(engine_module.get_engine()) as session:
            events = session.exec(select(EventRow)).all()
            assert [e.event_type for e in events] == ["hook_failed"]
            assert events[0].error is not None and "no route" in events[0].error
            hook_row = session.exec(select(Component).where(Component.kind == "hook")).one()
            assert (hook_row.state or {}).get("last_run_id") == str(run.id)

    def test_self_targeting_trigger_is_refused(self, store: Store):
        source = store.create_component(_ORG, kind="source", key="demo_source", name="Demo")
        # Watches the source AND targets it: would loop forever without the guard.
        store.create_component(
            _ORG, kind="hook", key="trigger_hook", name="Ouroboros",
            config={"events": ["run_completed"]},
            relations={"watch": [(source.id, "")], "target": [(source.id, "")]},
        )
        _terminal_run(store, source.id)

        _sweep(store)

        with Session(engine_module.get_engine()) as session:
            assert session.exec(select(Run).where(Run.status == "queued")).all() == []
            events = session.exec(select(EventRow)).all()
            assert [e.event_type for e in events] == ["hook_failed"]
            assert events[0].error is not None and "loop" in events[0].error

    def test_chain_to_unwatched_target_is_allowed(self, store: Store):
        source = store.create_component(_ORG, kind="source", key="demo_source", name="Demo")
        leaf = next(c for c in source.children if c.key == "e")
        root = next(c for c in source.children if c.key == "a")
        # Watches only the leaf asset; targets the root asset: no loop possible
        # through this hook (the triggered run never matches its watch set...
        # except via the shared parent — which is exactly what the guard checks).
        store.create_component(
            _ORG, kind="hook", key="trigger_hook", name="Chain",
            config={"events": ["run_completed"]},
            relations={"watch": [(leaf.id, "")], "target": [(root.id, "")]},
        )
        _terminal_run(store, leaf.id)

        _sweep(store)

        with Session(engine_module.get_engine()) as session:
            queued = session.exec(select(Run).where(Run.status == "queued")).all()
            assert [q.component_id for q in queued] == [root.id]
