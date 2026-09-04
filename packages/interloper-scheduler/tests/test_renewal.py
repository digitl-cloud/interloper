"""Tests for connection credential renewal: the controller and the run-borne renewal.

SQLite stands in for Postgres (``with_for_update`` is ignored there); the
scheduling and dispatch logic is what these tests pin.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from typing import Any
from uuid import UUID, uuid4

import httpx
import interloper as il
import pytest
from interloper_db import Store
from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Component, ComponentRelation, Event, Quota, Run, Usage
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, select

from interloper_scheduler.executor import RunExecutor
from interloper_scheduler.renewal import RenewalController

_ORG = uuid4()


class RenewableConn(il.Connection):
    """Renewable test connection; ``renew`` is monkeypatched per test."""

    token: str = "old"

    def renew(self) -> il.Renewal:
        return il.Renewal()


class StaticConn(il.Connection):
    """A connection with nothing to renew."""

    api_key: str = "k"


_CATALOG = il.Catalog(
    components={"renewable_conn": RenewableConn.definition(), "static_conn": StaticConn.definition()}
)


@pytest.fixture
def store() -> Iterator[Store]:
    """A store over an in-memory database with the scheduling tables.

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
        yield Store(catalog=_CATALOG)
    finally:
        eng.dispose()
        engine_module._engine = None


def _connection(store: Store, *, key: str = "renewable_conn", config: dict[str, Any] | None = None,
                state: dict[str, Any] | None = None) -> UUID:
    row = store.components.create(_ORG, kind="connection", key=key, name="C", config=config or {}, encrypted=False)
    if state is not None:
        with Session(store.engine) as session:
            db_row = session.get(Component, row.id)
            assert db_row is not None
            db_row.state = state
            session.add(db_row)
            session.commit()
    assert row.id is not None
    return row.id


def _runs(store: Store) -> list[Run]:
    with Session(store.engine) as session:
        return list(session.exec(select(Run)).all())


def _state(store: Store, component_id: UUID) -> dict[str, Any]:
    with Session(store.engine) as session:
        db_row = session.get(Component, component_id)
        assert db_row is not None
        return dict(db_row.state or {})


class TestRenewalController:
    def test_new_renewable_connection_is_enqueued(self, store: Store):
        conn_id = _connection(store)

        RenewalController(catalog=_CATALOG, store=store)._tick()

        (run,) = _runs(store)
        assert run.component_id == conn_id
        assert run.status == "queued"
        assert run.partition_key is None
        assert run.billable is False
        # Provisional slot: re-arms only if the run vanishes.
        assert _state(store, conn_id)["next_renewal_at"] > dt.datetime.now(dt.timezone.utc).isoformat()

    def test_non_renewable_connection_is_never_scanned(self, store: Store):
        conn_id = _connection(store, key="static_conn")

        RenewalController(catalog=_CATALOG, store=store)._tick()

        assert _runs(store) == []
        assert _state(store, conn_id) == {}

    def test_opted_out_connection_is_rechecked_later(self, store: Store):
        conn_id = _connection(store, config={"auto_renew": False})

        RenewalController(catalog=_CATALOG, store=store)._tick()

        assert _runs(store) == []
        stamped = _state(store, conn_id)["next_renewal_at"]
        # Reconsidered on the recheck horizon, not every tick.
        assert stamped > (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=12)).isoformat()

    def test_not_due_connection_is_untouched(self, store: Store):
        future = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=6)).isoformat()
        conn_id = _connection(store, state={"next_renewal_at": future})

        RenewalController(catalog=_CATALOG, store=store)._tick()

        assert _runs(store) == []
        assert _state(store, conn_id)["next_renewal_at"] == future

    def test_open_run_blocks_a_second_enqueue(self, store: Store):
        past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)).isoformat()
        conn_id = _connection(store, state={"next_renewal_at": past})
        with Session(store.engine) as session:
            session.add(Run(component_id=conn_id, org_id=_ORG, status="running"))
            session.commit()

        RenewalController(catalog=_CATALOG, store=store)._tick()

        assert len(_runs(store)) == 1
        # The slot still advances so the tick doesn't respin on this row.
        assert _state(store, conn_id)["next_renewal_at"] > past


class TestRenewalRuns:
    def _queued_run(self, store: Store, conn_id: UUID) -> UUID:
        with Session(store.engine) as session:
            run = Run(component_id=conn_id, org_id=_ORG, status="queued")
            session.add(run)
            session.commit()
            assert run.id is not None
            return run.id

    def test_successful_renewal_persists_and_schedules(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        conn_id = _connection(store, config={"token": "old"})
        run_id = self._queued_run(store, conn_id)
        monkeypatch.setattr(
            RenewableConn, "renew", lambda self: il.Renewal(fields={"token": "NEW"}, expires_in=7200)
        )

        assert RunExecutor(store=store).execute(run_id) is True

        with Session(store.engine) as session:
            db_run = session.get(Run, run_id)
            assert db_run is not None and db_run.status == "success"
            db_conn = session.get(Component, conn_id)
            assert db_conn is not None
            assert store.components.decode_config(db_conn)["token"] == "NEW"
        state = _state(store, conn_id)
        assert state["last_renewed_at"] is not None
        assert state["last_renewal_error"] is None
        # expires_in/2 with the reported 7200s validity.
        due = dt.datetime.fromisoformat(state["next_renewal_at"])
        assert dt.timedelta(minutes=55) < due - dt.datetime.now(dt.timezone.utc) < dt.timedelta(minutes=65)

    def test_failed_renewal_stamps_curated_error_and_fails_the_run(
        self, store: Store, monkeypatch: pytest.MonkeyPatch
    ):
        conn_id = _connection(store)
        run_id = self._queued_run(store, conn_id)

        def boom(self: RenewableConn) -> il.Renewal:
            request = httpx.Request("GET", "https://provider/exchange?client_secret=SECRET")
            raise httpx.HTTPStatusError("boom", request=request, response=httpx.Response(400, request=request))

        monkeypatch.setattr(RenewableConn, "renew", boom)

        assert RunExecutor(store=store).execute(run_id) is False

        with Session(store.engine) as session:
            db_run = session.get(Run, run_id)
            assert db_run is not None and db_run.status == "failed"
            events = list(session.exec(select(Event).where(Event.run_id == run_id)))
        state = _state(store, conn_id)
        assert state["last_renewal_error"] == "The provider rejected the renewal (HTTP 400)."
        assert "SECRET" not in str(state)
        assert state["next_renewal_at"] is not None
        # The run's failure event carries the curated message only.
        assert events and all("SECRET" not in str(e.data) for e in events)


class TestNoRenewableKeys:
    """A catalog with nothing renewable does no work at all."""

    def test_the_tick_returns_immediately(self, store: Store):
        controller = RenewalController(catalog=il.Catalog(components={}), store=store)

        controller._tick()

        assert _runs(store) == []


class TestAutoRenewDecode:
    """``auto_renew`` is read off the stored config, defaulting to on."""

    def test_an_undecodable_config_skips_the_connection(
        self, store: Store, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        # A connection whose config will not decode would fail the run too.
        component_id = _connection(store)
        controller = RenewalController(catalog=_CATALOG, store=store)

        def broken_decode(component: Any) -> dict[str, Any]:
            raise RuntimeError("cannot decrypt")

        monkeypatch.setattr(store.components, "decode_config", broken_decode)

        with Session(store.engine) as session:
            row = session.get(Component, component_id)
            assert row is not None
            with caplog.at_level("WARNING", logger="interloper_scheduler.renewal"):
                assert controller._auto_renew(row) is False

        assert f"Cannot decode config of connection {component_id}" in caplog.text

    def test_it_defaults_to_on(self, store: Store):
        component_id = _connection(store, config={})
        controller = RenewalController(catalog=_CATALOG, store=store)

        with Session(store.engine) as session:
            row = session.get(Component, component_id)
            assert row is not None
            assert controller._auto_renew(row) is True

    def test_an_explicit_opt_out_is_honoured(self, store: Store):
        component_id = _connection(store, config={"auto_renew": False})
        controller = RenewalController(catalog=_CATALOG, store=store)

        with Session(store.engine) as session:
            row = session.get(Component, component_id)
            assert row is not None
            assert controller._auto_renew(row) is False
