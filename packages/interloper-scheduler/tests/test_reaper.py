"""Tests for the reaper (``interloper_scheduler.reaper``)."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper_db import RunStatus, Store
from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Component, ComponentRelation, Quota, Run, Usage
from interloper_db.models import Event as EventRow
from interloper_db.store import UsageDrift
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, select

from interloper_scheduler.launcher import Launcher
from interloper_scheduler.reaper import Reaper

_ORG = uuid4()


class _FakeLauncher(Launcher):
    """Answers ``diagnose`` with one canned reason."""

    def __init__(self, diagnosis: str | None) -> None:
        self._diagnosis = diagnosis

    def launch(self, run_id: UUID) -> None:  # pragma: no cover - unused
        raise NotImplementedError

    def diagnose(self, run_id: UUID) -> str | None:
        return self._diagnosis


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

    try:
        yield store
    finally:
        eng.dispose()
        engine_module._engine = None


def _ago(seconds: float) -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=seconds)


def _run(store: Store, status: RunStatus, *, component_id: UUID | None = None, **fields: Any) -> UUID:
    """Stage a run in *status*, its columns overwritten with *fields*.

    Returns:
        The run's id.
    """
    run = store.runs.create(_ORG, component_id=component_id)
    with Session(store.engine) as session:
        db_run = session.get(Run, run.id)
        assert db_run is not None
        db_run.status = status
        for name, value in {"heartbeat_at": _ago(0), "started_at": _ago(0), **fields}.items():
            setattr(db_run, name, value)
        session.add(db_run)
        session.commit()
    return run.id


def _status(store: Store, run_id: UUID) -> str:
    return store.runs.get(run_id).status


def _error(store: Store, run_id: UUID) -> str | None:
    with Session(store.engine) as session:
        failure = select(EventRow.error).where(EventRow.run_id == run_id, EventRow.event_type == "run_failed")
        return session.exec(failure).one()


class TestRules:
    """Each overdue run is failed with its reason; runs in good standing are left alone."""

    def test_a_run_that_never_started_is_reaped(self, store: Store) -> None:
        stuck = _run(store, RunStatus.DISPATCHED, heartbeat_at=_ago(700))
        fresh = _run(store, RunStatus.DISPATCHED)

        assert Reaper(store=store, startup_timeout=600)._reap() == 1
        assert (_status(store, stuck), _status(store, fresh)) == ("failed", "dispatched")
        assert _error(store, stuck) == "Run did not start within 600s"

    def test_a_silent_run_is_reaped_as_lost(self, store: Store) -> None:
        silent = _run(store, RunStatus.RUNNING, heartbeat_at=_ago(200))
        beating = _run(store, RunStatus.RUNNING)

        assert Reaper(store=store, heartbeat_timeout=90)._reap() == 1
        assert (_status(store, silent), _status(store, beating)) == ("failed", "running")
        assert (_error(store, silent) or "").startswith("No heartbeat since ")

    def test_a_run_past_its_deadline_is_reaped(self, store: Store) -> None:
        run_id = _run(store, RunStatus.RUNNING, started_at=_ago(4000))

        assert Reaper(store=store, run_timeout=3600)._reap() == 1
        assert _error(store, run_id) == "Timed out after 3600s"


class TestDiagnosis:
    """The launcher's diagnosis is detail on the reason, never a decision."""

    def test_the_diagnosis_is_appended(self, store: Store) -> None:
        run_id = _run(store, RunStatus.RUNNING, heartbeat_at=_ago(200))

        Reaper(store=store, launcher=_FakeLauncher("reason=OOMKilled exit_code=137"))._reap()

        assert (_error(store, run_id) or "").endswith("(reason=OOMKilled exit_code=137)")

    def test_a_failing_diagnosis_leaves_the_plain_reason(
        self, store: Store, caplog: pytest.LogCaptureFixture
    ) -> None:
        class BrokenLauncher(_FakeLauncher):
            """Launcher whose ``diagnose`` always raises."""

            def diagnose(self, run_id: UUID) -> str | None:
                raise RuntimeError("api unreachable")

        run_id = _run(store, RunStatus.DISPATCHED, heartbeat_at=_ago(700))

        with caplog.at_level("WARNING", logger="interloper_scheduler.reaper"):
            assert Reaper(store=store, launcher=BrokenLauncher(None))._reap() == 1

        assert f"Could not diagnose run {run_id}" in caplog.text
        assert _error(store, run_id) == "Run did not start within 600s"


class TestTargetContext:
    def test_reaped_run_event_carries_target_context_in_data(self, store: Store) -> None:
        with Session(store.engine) as session:
            target = Component(org_id=_ORG, kind="job", key="nightly", name="Nightly sync")
            session.add(target)
            session.commit()
            target_id = target.id
        run_id = _run(store, RunStatus.DISPATCHED, component_id=target_id, heartbeat_at=_ago(1200))

        assert Reaper(store=store)._reap() == 1

        with Session(store.engine) as session:
            reaped_event = session.exec(
                select(EventRow).where(EventRow.run_id == run_id, EventRow.event_type == "run_failed")
            ).one()
            assert reaped_event.data == {
                "target_id": str(target_id),
                "target_kind": "job",
                "target_key": "nightly",
                "target_name": "Nightly sync",
            }


class TestTick:
    """One scan, plus the hourly usage reconciliation that rides the loop."""

    def test_a_reaped_run_is_logged(self, store: Store, caplog: pytest.LogCaptureFixture) -> None:
        _run(store, RunStatus.RUNNING, heartbeat_at=_ago(7200))
        reaper = Reaper(store=store, launcher=_FakeLauncher(None))

        with caplog.at_level("INFO", logger="interloper_scheduler.reaper"):
            reaper._tick()

        assert "Reaped 1 overdue run(s)" in caplog.text

    def test_nothing_to_reap_logs_nothing(self, store: Store, caplog: pytest.LogCaptureFixture) -> None:
        reaper = Reaper(store=store, launcher=_FakeLauncher(None))
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
        drift = UsageDrift(org_id=_ORG, period_start=dt.date(2026, 6, 1), ledger=5, recomputed=7)
        monkeypatch.setattr(store.usage, "reconcile", lambda: [drift])
        reaper = Reaper(store=store, launcher=_FakeLauncher(None))

        with caplog.at_level("WARNING", logger="interloper_scheduler.reaper"):
            reaper._reconcile_usage()

        assert "Usage ledger drift" in caplog.text
        assert "ledger=5" in caplog.text
        assert "runs table=7" in caplog.text

    def test_no_drift_warns_nothing(
        self, store: Store, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setattr(store.usage, "reconcile", list)
        reaper = Reaper(store=store, launcher=_FakeLauncher(None))

        with caplog.at_level("WARNING", logger="interloper_scheduler.reaper"):
            reaper._reconcile_usage()

        assert "Usage ledger drift" not in caplog.text

    def test_a_failed_reconciliation_does_not_stop_the_loop(
        self, store: Store, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Housekeeping must never take the reaper down.
        def broken() -> list[UsageDrift]:
            raise RuntimeError("query failed")

        monkeypatch.setattr(store.usage, "reconcile", broken)
        reaper = Reaper(store=store, launcher=_FakeLauncher(None))

        with caplog.at_level("ERROR", logger="interloper_scheduler.reaper"):
            reaper._reconcile_usage()

        assert "Usage reconciliation failed" in caplog.text


class TestFailureReportingIsBestEffort:
    """A failure that cannot be recorded never takes the reaper down."""

    def test_an_unrecordable_reason_leaves_the_run_for_the_next_sweep(
        self, store: Store, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        # The reason and the verdict commit together, so a run is never failed
        # without saying why; one the reaper could not record stays as it was
        # and is reaped again on the next tick.
        run_id = _run(store, RunStatus.RUNNING, heartbeat_at=_ago(7200))

        def broken_save(event: il.Event, org_id: UUID, run_id: UUID | None = None) -> None:
            raise RuntimeError("events table unreachable")

        monkeypatch.setattr(store.events, "save", broken_save)

        with caplog.at_level("ERROR", logger="interloper_scheduler.reaper"):
            assert Reaper(store=store)._reap() == 0

        assert f"Failed to reap run {run_id}" in caplog.text
        assert _status(store, run_id) == "running"

    def test_a_run_that_moved_after_the_read_is_left_alone(
        self, store: Store, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        run_id = _run(store, RunStatus.RUNNING, heartbeat_at=_ago(7200))
        reaper = Reaper(store=store, launcher=_FakeLauncher(None))
        diagnose = reaper._explain

        def beat_then_explain(run: UUID, reason: str) -> str:
            store.runs.heartbeat(run)
            return diagnose(run, reason)

        monkeypatch.setattr(reaper, "_explain", beat_then_explain)

        assert reaper._reap() == 0
        assert _status(store, run_id) == "running"
