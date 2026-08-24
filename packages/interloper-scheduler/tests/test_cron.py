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
from interloper_db.models import Backfill, Component, ComponentRelation, Quota, Run, Usage
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

    for model in (Component, ComponentRelation, Run, Backfill, Quota, Usage):
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

        from interloper_db.store.quotas import METRIC_SUCCESSFUL_RUNS, db_now, increment_usage, month_start

        store._quota_defaults = SimpleNamespace(max_successful_runs_per_month=1)
        with Session(store.engine) as session:
            increment_usage(session, _ORG, METRIC_SUCCESSFUL_RUNS, month_start(db_now(session)), used=1)
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


# -- Granularity resolution ------------------------------------------------------


@il.source
def monthly_source():  # noqa: D103
    @il.asset(partitioning=il.TimePartitionConfig(column="date", granularity=il.TimeGranularity.MONTH))
    def monthly_stats(context: il.ExecutionContext) -> list:
        return []

    return [monthly_stats]


@il.source
def daily_source():  # noqa: D103
    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def daily_stats(context: il.ExecutionContext) -> list:
        return []

    return [daily_stats]


def _catalog_store() -> Store:
    """A store over the fixture engine whose catalog knows both test sources."""
    return Store(
        catalog=il.Catalog(
            components={
                "monthly_source": monthly_source.definition(),
                "daily_source": daily_source.definition(),
            }
        )
    )


def _job_targeting(store: Store, *source_keys: str, config: dict[str, Any]) -> UUID:
    targets = [
        store.create_component(_ORG, kind="source", key=key, name=key).id for key in source_keys
    ]
    row = store.create_component(
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
