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
from zoneinfo import ZoneInfo

import interloper as il
import pytest
from interloper.errors import ConfigError
from interloper_db import Store
from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Component, ComponentRelation, Quota, Run, Usage
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, select

from interloper_scheduler.cron import CronController

_ORG = uuid4()


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

    for model in (Component, ComponentRelation, Run, Backfill, Quota, Usage):
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        yield Store(catalog=il.Catalog(components={}))
    finally:
        eng.dispose()
        engine_module._engine = None


def _job(store: Store, *, config: dict[str, Any], state: dict[str, Any] | None = None) -> UUID:
    row = store.components.create(_ORG, kind="job", key="cron_job", name="J", config=config)
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
        store = _catalog_store()
        now = dt.datetime.now(dt.timezone.utc)
        job_id = _job_targeting(
            store,
            "daily_source",
            config={"cron": "0 * * * *", "enabled": True, "lookback": 3},
        )
        CronController(store=store)._tick()

        with Session(store.engine) as session:
            backfill = session.exec(select(Backfill)).one()
        assert backfill.component_id == job_id
        assert backfill.partitions == 3
        # offset defaults to 1: the window ends on the last complete partition.
        assert backfill.end_key == (now.date() - dt.timedelta(days=1)).isoformat()
        assert backfill.start_key == (now.date() - dt.timedelta(days=3)).isoformat()
        runs = _runs(store)
        assert len(runs) == 3
        assert all(run.status == "queued" and run.backfill_id == backfill.id for run in runs)

    def test_offset_shifts_the_window_back(self, store: Store) -> None:
        store = _catalog_store()
        now = dt.datetime.now(dt.timezone.utc)
        _job_targeting(
            store,
            "daily_source",
            config={"cron": "0 * * * *", "enabled": True, "lookback": 2, "offset": 3},
        )
        CronController(store=store)._tick()

        with Session(store.engine) as session:
            backfill = session.exec(select(Backfill)).one()
        assert backfill.end_key == (now.date() - dt.timedelta(days=3)).isoformat()
        assert backfill.start_key == (now.date() - dt.timedelta(days=4)).isoformat()
        assert backfill.partitions == 2
        assert {run.partition_key for run in _runs(store)} == {backfill.start_key, backfill.end_key}

    def test_zero_offset_covers_the_current_partition(self, store: Store) -> None:
        store = _catalog_store()
        now = dt.datetime.now(dt.timezone.utc)
        _job_targeting(
            store,
            "daily_source",
            config={"cron": "0 * * * *", "enabled": True, "lookback": 1, "offset": 0},
        )
        CronController(store=store)._tick()

        with Session(store.engine) as session:
            backfill = session.exec(select(Backfill)).one()
        assert backfill.start_key == backfill.end_key == now.date().isoformat()

    def test_partitioned_job_without_lookback_creates_one_plain_run(self, store: Store) -> None:
        store = _catalog_store()
        _job_targeting(
            store,
            "daily_source",
            config={"cron": "0 * * * *", "enabled": True, "lookback": None},
        )
        CronController(store=store)._tick()

        runs = _runs(store)
        assert len(runs) == 1
        assert runs[0].partition_key is None


class TestConfig:
    def test_delay_must_cover_the_reconcile_interval(self, store: Store) -> None:
        with pytest.raises(ConfigError, match="max_execution_delay"):
            CronController(store=store, reconcile_interval=10, max_execution_delay=5)


class TestRunQuota:
    def test_exhausted_org_skips_run_but_advances_schedule(self, store: Store) -> None:
        from types import SimpleNamespace

        from interloper_db.store.quotas import METRIC_SUCCESSFUL_RUNS, UsageLedger

        store._quota_defaults = SimpleNamespace(max_successful_runs_per_month=1)
        with Session(store.engine) as session:
            ledger = UsageLedger(session)
            ledger.increment(_ORG, METRIC_SUCCESSFUL_RUNS, ledger.current_period(), used=1)
            session.commit()

        now = dt.datetime.now(dt.timezone.utc)
        job_id = _job(
            store,
            config={"cron": "0 * * * *", "enabled": True},
            state={"next_run_at": now.isoformat()},
        )
        CronController(store=store)._tick()

        assert _runs(store) == []
        # The schedule still advances so the job doesn't re-fire every tick.
        assert _state(store, job_id)["next_run_at"] > now.isoformat()


# -- Granularity resolution ----------------------------------------------------


@il.source
def monthly_source():
    @il.asset(partitioning=il.TimePartitionConfig(column="date", granularity=il.TimeGranularity.MONTH))
    def monthly_stats(context: il.ExecutionContext) -> list:
        return []

    return [monthly_stats]


@il.source
def daily_source():
    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def daily_stats(context: il.ExecutionContext) -> list:
        return []

    return [daily_stats]


@il.source
def hourly_source():
    @il.asset(partitioning=il.TimePartitionConfig(column="date", granularity=il.TimeGranularity.HOUR))
    def hourly_stats(context: il.ExecutionContext) -> list:
        return []

    return [hourly_stats]


def _catalog_store() -> Store:
    """A store over the fixture engine whose catalog knows the test sources.

    Returns:
        The store, resolving the module's source definitions.
    """
    return Store(
        catalog=il.Catalog(
            components={
                "monthly_source": monthly_source.definition(),
                "daily_source": daily_source.definition(),
                "hourly_source": hourly_source.definition(),
            }
        )
    )


def _job_targeting(store: Store, *source_keys: str, config: dict[str, Any]) -> UUID:
    targets = [
        store.components.create(_ORG, kind="source", key=key, name=key).id for key in source_keys
    ]
    row = store.components.create(
        _ORG,
        kind="job",
        key="cron_job",
        name="J",
        config=config,
        relations={"target": [(tid, "") for tid in targets]},
    )
    with Session(store.engine) as session:
        db_job = session.get(Component, row.id)
        assert db_job is not None
        db_job.state = {"next_run_at": dt.datetime.now(dt.timezone.utc).isoformat()}
        session.add(db_job)
        session.commit()
    assert row.id is not None
    return row.id


class TestTimezone:
    """Cron evaluation on the job's wall clock — storage stays UTC everywhere."""

    BERLIN = ZoneInfo("Europe/Berlin")

    def _next(self, store: Store, expr: str, base: dt.datetime) -> dt.datetime:
        return CronController(store=store)._calculate_next_run(expr, base, self.BERLIN)

    def test_daily_fire_tracks_wall_time_across_spring_forward(self, store: Store) -> None:
        # Berlin flips CET -> CEST on 2026-03-29: 06:00 wall time moves from
        # 05:00 UTC to 04:00 UTC.
        before = self._next(store, "0 6 * * *", dt.datetime(2026, 3, 27, 12, 0, tzinfo=dt.timezone.utc))
        assert before == dt.datetime(2026, 3, 28, 5, 0, tzinfo=dt.timezone.utc)
        on_flip = self._next(store, "0 6 * * *", before)
        assert on_flip == dt.datetime(2026, 3, 29, 4, 0, tzinfo=dt.timezone.utc)

    def test_daily_fire_tracks_wall_time_across_fall_back(self, store: Store) -> None:
        # CEST -> CET on 2026-10-25: 06:00 wall time moves from 04:00 UTC back
        # to 05:00 UTC.
        before = self._next(store, "0 6 * * *", dt.datetime(2026, 10, 23, 12, 0, tzinfo=dt.timezone.utc))
        assert before == dt.datetime(2026, 10, 24, 4, 0, tzinfo=dt.timezone.utc)
        on_flip = self._next(store, "0 6 * * *", before)
        assert on_flip == dt.datetime(2026, 10, 25, 5, 0, tzinfo=dt.timezone.utc)

    def test_spring_forward_gap_fires_once_after_the_gap(self, store: Store) -> None:
        # 02:30 does not exist on 2026-03-29 (02:00 CET jumps to 03:00 CEST):
        # the fire slides to the first instant after the gap, once.
        fire = self._next(store, "30 2 * * *", dt.datetime(2026, 3, 28, 23, 0, tzinfo=dt.timezone.utc))
        assert fire == dt.datetime(2026, 3, 29, 1, 0, tzinfo=dt.timezone.utc)  # 03:00 CEST
        assert self._next(store, "30 2 * * *", fire) == dt.datetime(2026, 3, 30, 0, 30, tzinfo=dt.timezone.utc)

    def test_fall_back_fold_fires_in_both_folds(self, store: Store) -> None:
        # 02:30 happens twice on 2026-10-25: the wall-clock schedule yields
        # both instants (02:30 CEST, then 02:30 CET an hour later).
        first = self._next(store, "30 2 * * *", dt.datetime(2026, 10, 24, 22, 0, tzinfo=dt.timezone.utc))
        assert first == dt.datetime(2026, 10, 25, 0, 30, tzinfo=dt.timezone.utc)
        second = self._next(store, "30 2 * * *", first)
        assert second == dt.datetime(2026, 10, 25, 1, 30, tzinfo=dt.timezone.utc)

    def test_tick_stores_utc_that_is_the_jobs_wall_time(self, store: Store) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        job_id = _job(
            store,
            config={"cron": "0 6 * * *", "enabled": True, "timezone": "Europe/Berlin"},
            state={"next_run_at": now.isoformat()},
        )
        CronController(store=store)._tick()
        parsed = dt.datetime.fromisoformat(_state(store, job_id)["next_run_at"])
        assert parsed.utcoffset() == dt.timedelta(0)
        local = parsed.astimezone(self.BERLIN)
        assert (local.hour, local.minute) == (6, 0)

    def test_daily_window_covers_the_job_zones_yesterday(self, store: Store) -> None:
        store = _catalog_store()
        now = dt.datetime.now(dt.timezone.utc)
        # Pick a zone whose local date currently differs from UTC's, so the
        # assertion can't pass through the UTC calendar by accident.
        zone_name = "Etc/GMT-14" if now.hour >= 12 else "Etc/GMT+12"
        local_today = now.astimezone(ZoneInfo(zone_name)).date()
        assert local_today != now.date()
        _job_targeting(
            store,
            "daily_source",
            config={"cron": "0 * * * *", "enabled": True, "lookback": 1, "timezone": zone_name},
        )
        CronController(store=store)._tick()

        with Session(store.engine) as session:
            backfill = session.exec(select(Backfill)).one()
        assert backfill.end_key == (local_today - dt.timedelta(days=1)).isoformat()

    def test_hourly_window_stays_utc_regardless_of_job_timezone(self, store: Store) -> None:
        store = _catalog_store()
        now = dt.datetime.now(dt.timezone.utc)
        _job_targeting(
            store,
            "hourly_source",
            config={"cron": "0 * * * *", "enabled": True, "lookback": 1, "timezone": "Asia/Kathmandu"},
        )
        CronController(store=store)._tick()

        with Session(store.engine) as session:
            backfill = session.exec(select(Backfill)).one()
        assert backfill.end_key == (now - dt.timedelta(hours=1)).strftime("%Y-%m-%dT%H")

    def test_unknown_timezone_falls_back_to_utc(self, store: Store) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        job_id = _job(
            store,
            config={"cron": "0 * * * *", "enabled": True, "timezone": "Not/AZone"},
            state={"next_run_at": now.isoformat()},
        )
        CronController(store=store)._tick()
        assert [run.status for run in _runs(store)] == ["queued"]
        assert _state(store, job_id)["next_run_at"] > now.isoformat()


class TestGranularityResolution:
    """The cron window is stepped in the granularity of the job's targets."""

    def test_monthly_targets_yield_a_monthly_window(self, store: Store) -> None:
        store = _catalog_store()
        now = dt.datetime.now(dt.timezone.utc)
        _job_targeting(
            store,
            "monthly_source",
            config={"cron": "0 * * * *", "enabled": True, "lookback": 2},
        )
        CronController(store=store)._tick()

        with Session(store.engine) as session:
            backfill = session.exec(select(Backfill)).one()
        this_month = now.date().replace(day=1)
        last_month = (this_month - dt.timedelta(days=1)).replace(day=1)
        month_before = (last_month - dt.timedelta(days=1)).replace(day=1)
        assert backfill.end_key == last_month.strftime("%Y-%m")
        assert backfill.start_key == month_before.strftime("%Y-%m")
        assert backfill.partitions == 2
        assert sorted(r.partition_key for r in _runs(store)) == sorted(
            [month_before.strftime("%Y-%m"), last_month.strftime("%Y-%m")]
        )

    def test_disagreeing_targets_skip_the_job(self, store: Store) -> None:
        store = _catalog_store()
        job_id = _job_targeting(
            store,
            "monthly_source",
            "daily_source",
            config={"cron": "0 * * * *", "enabled": True, "lookback": 1},
        )
        before = _state(store, job_id)["next_run_at"]
        CronController(store=store)._tick()

        # Fail closed, but the schedule still advances so the broken job does not re-fire every tick.
        assert _runs(store) == []
        with Session(store.engine) as session:
            assert session.exec(select(Backfill)).all() == []
        assert _state(store, job_id)["next_run_at"] > before

    def test_job_without_partitioned_targets_runs_unwindowed(self, store: Store) -> None:
        # Partitioning is derived from the targets: nothing partitioned in
        # scope means a single plain run, regardless of lookback.
        now = dt.datetime.now(dt.timezone.utc)
        _job(
            store,
            config={"cron": "0 * * * *", "enabled": True, "lookback": 1},
            state={"next_run_at": now.isoformat()},
        )
        CronController(store=store)._tick()

        with Session(store.engine) as session:
            assert session.exec(select(Backfill)).all() == []
        runs = _runs(store)
        assert len(runs) == 1
        assert runs[0].partition_key is None
