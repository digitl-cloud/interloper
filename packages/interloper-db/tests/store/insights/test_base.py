"""Tests for ``interloper_db.store.insights.base``: the facet's queries over runs, events and executions.

SQLite stands in for Postgres; the executions read model's table definition
doubles as the view's schema.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.errors import ConfigError
from interloper.partitioning.time import TimeGranularity
from interloper_assets.demo.source import DemoMonthlySource, DemoSource, demo_asset
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session

from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Component, ComponentRelation, Event, Execution, Quota, Run, Usage
from interloper_db.store import Store
from interloper_db.store.insights import base as insights_base

_ORG = uuid4()
_T0 = dt.datetime(2026, 6, 4, 12, tzinfo=dt.timezone.utc)


class HourlySource(il.Source):
    """Source whose hourly asset declares where its partitions start."""

    @il.asset(
        partitioning=il.TimePartitionConfig(
            column="date", granularity=TimeGranularity.HOUR, start=dt.datetime(2026, 1, 15)
        )
    )
    def clicks(self) -> list[dict]:
        return []

    @il.asset
    def accounts(self) -> list[dict]:
        return []


@pytest.fixture
def store() -> Iterator[Store]:
    """A store over a fresh in-memory database with every table the facet reads.

    Yields:
        The store, its database disposed once the test finishes.
    """
    engine = engine_module.init_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)

    @event.listens_for(engine, "connect")
    def _sqlite_uuid(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    for model in (Component, ComponentRelation, Backfill, Run, Event, Execution, Quota, Usage):
        model.__table__.create(engine)  # ty: ignore[unresolved-attribute]
    try:
        yield Store(catalog=il.Catalog.from_assets([DemoSource, DemoMonthlySource, HourlySource, demo_asset]))
    finally:
        engine.dispose()
        engine_module._engine = None


def _add(*rows: Any) -> None:
    with Session(engine_module.get_engine()) as session:
        session.add_all(rows)
        session.commit()


def _component(kind: str) -> UUID:
    component_id = uuid4()
    _add(Component(id=component_id, org_id=_ORG, kind=kind, key=kind, name=kind))
    return component_id


def _run(
    target: UUID | None = None,
    *,
    status: str = "failed",
    created: dt.datetime = _T0,
    partition_key: str | None = None,
    org_id: UUID = _ORG,
    root: UUID | None = None,
    attempt: int = 1,
    backfill_id: UUID | None = None,
) -> UUID:
    run_id = uuid4()
    run = Run(
        id=run_id,
        org_id=org_id,
        component_id=target,
        status=status,
        created_at=created,
        partition_key=partition_key,
        attempt=attempt,
        backfill_id=backfill_id,
    )
    if root is not None:
        run.root_run_id = root
    _add(run)
    return run_id


def _error(run_id: UUID, event_type: str, error: str | None, *, second: int = 0, org_id: UUID = _ORG) -> Event:
    return Event(
        org_id=org_id,
        run_id=run_id,
        event_type=event_type,
        component_key="orders",
        error=error,
        timestamp=_T0 + dt.timedelta(seconds=second),
    )


def _execution(run_id: UUID, asset_id: UUID, status: str) -> None:
    _add(Execution(run_id=run_id, component_id=asset_id, org_id=_ORG, component_key="orders", status=status))


class TestLatestByTarget:
    """One run per target: its most recently created attempt, whatever its stack."""

    def test_the_most_recently_created_attempt_wins(self, store: Store) -> None:
        job = _component("job")
        _run(job, status="success")
        first = _run(job, created=_T0 + dt.timedelta(hours=1))
        retry = _run(job, status="success", created=_T0 + dt.timedelta(hours=2), root=first, attempt=2)

        assert [(run.id, run.status) for run in store.insights._latest_by_target(_ORG)] == [(retry, "success")]

    def test_an_interleaved_retry_is_the_most_recent_attempt(self, store: Store) -> None:
        job = _component("job")
        first = _run(job)
        _run(job, status="success", created=_T0 + dt.timedelta(hours=1))
        retry = _run(job, created=_T0 + dt.timedelta(hours=2), root=first, attempt=2)

        assert [run.id for run in store.insights._latest_by_target(_ORG)] == [retry]

    def test_kind_filter_and_org_scoping(self, store: Store) -> None:
        job, source = _component("job"), _component("source")
        _run(job)
        _run(source, status="success")
        _run(job, created=_T0 + dt.timedelta(hours=1), org_id=uuid4())

        jobs_only = store.insights._latest_by_target(_ORG, kind="job")

        assert [(run.component_id, run.status) for run in jobs_only] == [(job, "failed")]
        assert {run.org_id for run in store.insights._latest_by_target(_ORG)} == {_ORG}
        assert store.insights._latest_by_target(uuid4()) == []

    def test_a_creation_tie_goes_to_the_later_partition(self, store: Store) -> None:
        job = _component("job")
        later = _run(job, partition_key="2026-01-02")
        _run(job, status="success", partition_key="2026-01-01")
        _run(job, status="success")

        assert [run.id for run in store.insights._latest_by_target(_ORG)] == [later]

    def test_a_deleted_target_is_left_out(self, store: Store) -> None:
        _run(None)

        assert store.insights._latest_by_target(_ORG) == []


class TestErrorGroups:
    """Failed attempts collapse per job, run, asset, type and text in the database, then merge by cause."""

    def test_identical_texts_collapse_per_run(self, store: Store) -> None:
        job = _component("job")
        run = _run(job)
        _add(*[_error(run, "operation_retried", "HTTPStatusError: 429", second=s) for s in range(3)])
        _add(_error(run, "operation_failed", "HTTPStatusError: 429", second=9), _error(run, "asset_data_failed", "x"))

        rows = store.insights._error_rows(_ORG, since=None, until=None, job_id=None, backfill_id=None, run_id=None)

        assert [(row.event_type, row.count, row.job_id) for row in rows] == [
            ("operation_retried", 3, job),
            ("operation_failed", 1, job),
        ]
        assert rows[0].first_seen < rows[0].last_seen

    def test_the_window_and_scope_filters_narrow_the_scan(self, store: Store) -> None:
        job, other_job, backfill = uuid4(), uuid4(), uuid4()
        mine, theirs = _run(job), _run(other_job, backfill_id=backfill)
        _add(_error(mine, "operation_failed", "early"), _error(mine, "operation_failed", "late", second=60))
        _add(_error(theirs, "operation_failed", "other job", second=60))
        _add(_error(_run(org_id=uuid4()), "operation_failed", "other org", org_id=uuid4(), second=60))
        middle = _T0 + dt.timedelta(seconds=30)

        def samples(**scope: Any) -> set[str]:
            groups = store.insights.error_groups(_ORG, group_by=("job", "asset"), **scope).groups
            return {group.sample for group in groups}

        assert samples(since=middle) == {"late", "other job"}
        assert samples(until=middle) == {"early"}
        assert samples(job_id=job) == {"late"}
        assert samples(run_id=theirs) == samples(backfill_id=backfill) == {"other job"}

    def test_the_cap_reports_that_it_cut_rows_off(self, store: Store, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(insights_base, "MAX_ERROR_ROWS", 2)
        run = _run()
        _add(*[_error(run, "operation_failed", f"error {i}", second=i) for i in range(3)])

        groups = store.insights.error_groups(_ORG)

        assert (groups.rows, groups.truncated) == (2, True)

    def test_an_unknown_grouping_key_is_rejected(self, store: Store) -> None:
        with pytest.raises(ConfigError, match="Unknown group_by"):
            store.insights.error_groups(_ORG, group_by=("job", "colour"))


class TestCoverageRows:
    """Per asset and time partition, from runs of any target."""

    def test_every_time_granularity_of_every_period_is_read(self, store: Store) -> None:
        job, asset = uuid4(), uuid4()
        keys = ["2026-07-02", "2026-07-01T13", "2026-06", "2026", "2024-01-05", "2019"]
        for key in [*keys, "eu"]:
            _execution(_run(job, partition_key=key), asset, "success")

        rows = store.insights._coverage_rows(_ORG)

        assert sorted(row.partition_key for row in rows) == sorted(keys)
        assert all(row.asset_id == asset and row.succeeded for row in rows)

    def test_runs_of_every_target_fold_into_one_row_per_asset_and_partition(self, store: Store) -> None:
        asset = uuid4()
        by_job = _run(uuid4(), partition_key="2026-07-01")
        by_a_deleted_target = _run(None, partition_key="2026-07-01")
        _execution(by_job, asset, "failed")
        _execution(_run(asset, partition_key="2026-07-01"), asset, "success")
        _execution(by_a_deleted_target, asset, "failed")

        [row] = store.insights._coverage_rows(_ORG)

        assert (row.asset_id, row.partition_key, row.succeeded, row.failed) == (asset, "2026-07-01", True, True)
        assert row.failed_run_id == max(by_job, by_a_deleted_target)

    def test_only_a_failed_execution_marks_the_row_failed(self, store: Store) -> None:
        job, asset = uuid4(), uuid4()
        for key, status in (("2026-07-01", "failed"), ("2026-07-02", "running"), ("2026-07-03", "canceled")):
            _execution(_run(job, partition_key=key), asset, status)

        rows = {row.partition_key: (row.succeeded, row.failed) for row in store.insights._coverage_rows(_ORG)}

        assert rows == {"2026-07-01": (False, True), "2026-07-02": (False, False), "2026-07-03": (False, False)}

    def test_unpartitioned_runs_and_other_orgs_stay_out(self, store: Store) -> None:
        asset = uuid4()
        _execution(_run(uuid4()), asset, "success")
        _execution(_run(uuid4(), partition_key="2026-07-01", org_id=uuid4()), asset, "success")

        assert store.insights._coverage_rows(_ORG) == []


class TestCoverageByKey:
    """A job's target assets over a key range, any target's runs counting."""

    def test_every_target_asset_reports_its_covered_and_failed_keys(self, store: Store) -> None:
        source = store.components.create(_ORG, kind="source", key="demo_source")
        job = store.components.create(_ORG, kind="job", key="cron_job", relations={"targets": [source.id]})
        assets = {child.key: child.id for child in source.children}
        _execution(_run(job.id, partition_key="2026-07-01"), assets["a"], "failed")
        _execution(_run(assets["a"], partition_key="2026-07-01"), assets["a"], "success")
        _execution(_run(job.id, partition_key="2026-07-02"), assets["a"], "failed")
        _execution(_run(job.id, partition_key="2026-07-03"), assets["a"], "success")

        coverage = store.insights.coverage_by_key(_ORG, job.id, start_key="2026-07-01", end_key="2026-07-02")

        assert coverage.keys == ["2026-07-01", "2026-07-02"]
        by_key = {asset.asset_key: asset for asset in coverage.assets}
        assert set(by_key) == set(assets)
        assert (by_key["a"].covered, by_key["a"].failed) == ({"2026-07-01"}, {"2026-07-02"})

    def test_keys_of_different_granularities_are_rejected(self, store: Store) -> None:
        job = store.components.create(_ORG, kind="job", key="cron_job")

        with pytest.raises(ConfigError, match="different granularities"):
            store.insights.coverage_by_key(_ORG, job.id, start_key="2026-07-01", end_key="2026-07")


class TestAssetPartitionings:
    """Every partitioned asset row resolves its catalog partitioning."""

    def test_owned_and_standalone_assets_resolve_their_partitioning(self, store: Store) -> None:
        daily = store.components.create(_ORG, kind="source", key="demo_source")
        monthly = store.components.create(_ORG, kind="source", key="demo_monthly_source")
        hourly = store.components.create(_ORG, kind="source", key="hourly_source")
        standalone = store.components.create(_ORG, kind="asset", key="demo_asset")
        store.components.create(uuid4(), kind="source", key="demo_source")

        day = il.TimePartitionConfig(column="date")
        assert store.insights._asset_partitionings(_ORG) == {
            **{child.id: day for child in daily.children},
            monthly.children[0].id: il.TimePartitionConfig(column="date", granularity=TimeGranularity.MONTH),
            next(child for child in hourly.children if child.key == "clicks").id: il.TimePartitionConfig(
                column="date", granularity=TimeGranularity.HOUR, start=dt.datetime(2026, 1, 15)
            ),
            standalone.id: day,
        }

    def test_a_drifted_asset_is_skipped(self, store: Store) -> None:
        daily = store.components.create(_ORG, kind="source", key="demo_source")
        # A key the source does not declare must not fall back to the standalone asset of that key.
        _add(Component(org_id=_ORG, kind="asset", key="demo_asset", parent_id=daily.id))

        assert set(store.insights._asset_partitionings(_ORG)) == {child.id for child in daily.children}


class TestHealth:
    """Each job's state and next firing."""

    def test_a_job_whose_stored_config_cannot_window_has_none(self, store: Store) -> None:
        source = store.components.create(_ORG, kind="source", key="demo_source")
        job = store.components.create(_ORG, kind="job", key="cron_job", relations={"targets": [source.id]})
        with Session(engine_module.get_engine()) as session:
            row = session.get(Component, job.id)
            assert row is not None
            row.config = {**(row.config or {}), "offset": -1}
            row.state = {"next_run_at": _T0.isoformat()}
            session.add(row)
            session.commit()

        [health] = store.insights.health(_ORG, now=_T0).jobs

        assert (health.next_run_at, health.window) == (_T0, None)
