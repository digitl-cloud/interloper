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
from interloper_db.models import Backfill, Component, ComponentRelation, Quota, Run, Usage
from interloper_db.models import Event as EventRow
from interloper_db.store.events import EventStore
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, select

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
    """A store over an in-memory database, with a SQLite-friendly ``save``.

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

    for model in (Component, ComponentRelation, Run, Backfill, EventRow, Quota, Usage):
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]

    store = Store(catalog=il.Catalog(components={}))

    def sqlite_save(event: il.Event, org_id: UUID, run_id: UUID | None = None) -> EventRow:
        row = EventRow(**EventStore._event_values(event, org_id, run_id))
        with Session(eng) as session:
            session.add(row)
            session.commit()
        return row

    monkeypatch.setattr(store.events, "save", sqlite_save)
    try:
        yield store
    finally:
        eng.dispose()
        engine_module._engine = None


def _dispatched_run(store: Store, *, age_seconds: int = 0) -> UUID:
    run = store.runs.create(_ORG)
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
    return store.runs.get(run_id).status


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


class TestTargetContext:
    def test_reaped_run_event_carries_target_context_in_data(self, store: Store) -> None:
        with Session(store.engine) as session:
            target = Component(org_id=_ORG, kind="job", key="nightly", name="Nightly sync")
            session.add(target)
            session.commit()
            target_id = target.id
        run = store.runs.create(_ORG, component_id=target_id)
        assert run.id is not None
        with Session(store.engine) as session:
            db_run = session.get(Run, run.id)
            assert db_run is not None
            db_run.status = "dispatched"
            db_run.created_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1200)
            session.add(db_run)
            session.commit()

        assert Reaper(store=store, launcher=None, timeout=600)._reap() == 1

        with Session(store.engine) as session:
            reaped_event = session.exec(select(EventRow).where(EventRow.run_id == run.id)).one()
            assert reaped_event.event_type == "run_failed"
            assert reaped_event.data == {
                "target_id": str(target_id),
                "target_kind": "job",
                "target_key": "nightly",
                "target_name": "Nightly sync",
            }


class TestTick:
    """One scan, plus the hourly usage reconciliation that rides the loop."""

    def test_a_reaped_run_is_logged(self, store: Store, caplog: pytest.LogCaptureFixture) -> None:
        _dispatched_run(store, age_seconds=7200)
        reaper = Reaper(store=store, launcher=_FakeLauncher(None), timeout=3600)

        with caplog.at_level("INFO", logger="interloper_scheduler.reaper"):
            reaper._tick()

        assert "Reaped 1 dispatched run(s)" in caplog.text

    def test_nothing_to_reap_logs_nothing(self, store: Store, caplog: pytest.LogCaptureFixture) -> None:
        reaper = Reaper(store=store, launcher=_FakeLauncher(None), timeout=3600)
        reaper._ticks_since_reconcile = 0

        with caplog.at_level("INFO", logger="interloper_scheduler.reaper"):
            reaper._tick()

        assert "Reaped" not in caplog.text

    def test_the_first_tick_reconciles(self, store: Store, monkeypatch: pytest.MonkeyPatch) -> None:
        # ``_ticks_since_reconcile`` starts at the threshold on purpose.
        calls: list[bool] = []
        reaper = Reaper(store=store, launcher=_FakeLauncher(None))
        monkeypatch.setattr(reaper, "_reconcile_usage", lambda: calls.append(True))

        reaper._tick()

        assert calls == [True]

    def test_later_ticks_wait_for_the_interval(self, store: Store, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[bool] = []
        reaper = Reaper(store=store, launcher=_FakeLauncher(None), poll_interval=60)
        monkeypatch.setattr(reaper, "_reconcile_usage", lambda: calls.append(True))

        reaper._tick()  # the first one reconciles
        reaper._tick()

        assert calls == [True]

    def test_the_interval_is_about_an_hour_of_ticks(self, store: Store) -> None:
        assert Reaper(store=store, launcher=_FakeLauncher(None), poll_interval=60)._reconcile_every == 60
        assert Reaper(store=store, launcher=_FakeLauncher(None), poll_interval=3600)._reconcile_every == 1

    def test_a_zero_poll_interval_does_not_divide_by_zero(self, store: Store) -> None:
        assert Reaper(store=store, launcher=_FakeLauncher(None), poll_interval=0)._reconcile_every == 3600


class TestReconcileUsage:
    """Ledger drift is advisory: warned about, never corrected."""

    def test_drift_is_warned_about(
        self, store: Store, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        drift = {"org_id": _ORG, "period_start": dt.date(2026, 6, 1), "ledger": 5, "recomputed": 7}
        monkeypatch.setattr(store.quotas, "reconcile_usage", lambda: [drift])
        reaper = Reaper(store=store, launcher=_FakeLauncher(None))

        with caplog.at_level("WARNING", logger="interloper_scheduler.reaper"):
            reaper._reconcile_usage()

        assert "Usage ledger drift" in caplog.text
        assert "ledger=5" in caplog.text
        assert "runs table=7" in caplog.text

    def test_no_drift_warns_nothing(
        self, store: Store, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setattr(store.quotas, "reconcile_usage", list)
        reaper = Reaper(store=store, launcher=_FakeLauncher(None))

        with caplog.at_level("WARNING", logger="interloper_scheduler.reaper"):
            reaper._reconcile_usage()

        assert "Usage ledger drift" not in caplog.text

    def test_a_failed_reconciliation_does_not_stop_the_loop(
        self, store: Store, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Housekeeping must never take the reaper down.
        def broken() -> list[dict[str, Any]]:
            raise RuntimeError("query failed")

        monkeypatch.setattr(store.quotas, "reconcile_usage", broken)
        reaper = Reaper(store=store, launcher=_FakeLauncher(None))

        with caplog.at_level("ERROR", logger="interloper_scheduler.reaper"):
            reaper._reconcile_usage()

        assert "Usage reconciliation failed" in caplog.text


class TestLauncherFailure:
    """A launcher that cannot answer falls through to the timeout."""

    def test_a_describe_failure_is_logged_and_survived(
        self, store: Store, caplog: pytest.LogCaptureFixture
    ) -> None:
        class BrokenLauncher(Launcher):
            """Launcher whose ``describe_run`` always raises."""

            def launch(self, run_id: UUID) -> None:  # pragma: no cover - unused
                raise NotImplementedError

            def describe_run(self, run_id: UUID) -> RunState | None:
                raise RuntimeError("api unreachable")

        run_id = _dispatched_run(store, age_seconds=7200)
        reaper = Reaper(store=store, launcher=BrokenLauncher(), timeout=3600)

        with caplog.at_level("ERROR", logger="interloper_scheduler.reaper"):
            assert reaper._reap() == 1

        assert f"Failed to describe run {run_id}" in caplog.text
        assert _status(store, run_id) == "failed"


class TestFailureReportingIsBestEffort:
    """Neither half of the failure record may take the reaper down."""

    def test_a_failed_event_save_is_logged_and_survived(
        self, store: Store, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        run_id = _dispatched_run(store, age_seconds=7200)

        def broken_save(event: il.Event, org_id: UUID, run_id: UUID | None = None) -> None:
            raise RuntimeError("events table unreachable")

        monkeypatch.setattr(store.events, "save", broken_save)
        reaper = Reaper(store=store, launcher=_FakeLauncher(None), timeout=3600)

        with caplog.at_level("ERROR", logger="interloper_scheduler.reaper"):
            assert reaper._reap() == 1

        assert f"Failed to save RUN_FAILED event for run {run_id}" in caplog.text
        # The run is still marked failed, which is what unsticks it.
        assert _status(store, run_id) == "failed"

    def test_a_failed_completion_is_logged_and_survived(
        self, store: Store, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        run_id = _dispatched_run(store, age_seconds=7200)

        def broken_complete(run_id: UUID, success: bool) -> None:
            raise RuntimeError("runs table unreachable")

        monkeypatch.setattr(store.runs, "complete", broken_complete)
        reaper = Reaper(store=store, launcher=_FakeLauncher(None), timeout=3600)

        with caplog.at_level("ERROR", logger="interloper_scheduler.reaper"):
            assert reaper._reap() == 1

        assert f"Failed to mark run {run_id} as failed" in caplog.text
