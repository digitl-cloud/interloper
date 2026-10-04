"""Tests for ``interloper_db.store.executions``.

SQLite stands in for Postgres: the read model's table definition doubles as the
view's schema, so creating it as a table exercises the exact mapping the view
serves in production.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import interloper as il
import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session

from interloper_db import engine as engine_module
from interloper_db.models import Execution, Run
from interloper_db.store import ExecutionQuery, Store

_ORG_ID = uuid4()


@pytest.fixture
def store() -> Iterator[Store]:
    """A store over a fresh in-memory SQLite database carrying the runs and the executions read model.

    Yields:
        The store bound to that database, disposed once the test finishes.
    """
    engine = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    for model in (Run, Execution):
        model.__table__.create(engine)  # ty: ignore[unresolved-attribute]
    try:
        yield Store(catalog=il.Catalog(components={}), engine=engine)
    finally:
        engine.dispose()
        engine_module._engine = None


def _seed(executions: list[Execution]) -> None:
    with Session(engine_module.get_engine()) as session:
        session.add_all(executions)
        session.commit()


class TestList:
    """One run's executions, every run's, or each component's newest."""

    def test_maps_the_view(self, store: Store) -> None:
        run_id, asset_id = uuid4(), uuid4()
        _seed(
            [
                Execution(
                    run_id=run_id,
                    component_id=asset_id,
                    org_id=_ORG_ID,
                    component_key="a",
                    status="success",
                    completed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                )
            ]
        )

        rows = store.executions.list(_ORG_ID, ExecutionQuery(limit=None), run_id=run_id).items

        assert [(row.component_key, row.status) for row in rows] == [("a", "success")]
        assert store.executions.list(_ORG_ID, ExecutionQuery(limit=None), run_id=uuid4()).items == []

    def test_is_scoped_to_the_organisation(self, store: Store) -> None:
        run_id = uuid4()
        _seed([Execution(run_id=run_id, component_id=uuid4(), org_id=uuid4(), component_key="a", status="success")])

        page = store.executions.list(_ORG_ID, ExecutionQuery(limit=None), run_id=run_id)

        assert page.items == []
        assert page.total == 0

    def test_without_a_run_lists_every_run_oldest_first(self, store: Store) -> None:
        t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        older, newer = uuid4(), uuid4()
        _seed(
            [
                Execution(
                    run_id=newer,
                    component_id=uuid4(),
                    org_id=_ORG_ID,
                    status="success",
                    created_at=t0 + timedelta(hours=1),
                ),
                Execution(run_id=older, component_id=uuid4(), org_id=_ORG_ID, status="failed", created_at=t0),
            ]
        )

        page = store.executions.list(_ORG_ID, ExecutionQuery(limit=1))

        assert [row.run_id for row in page.items] == [older]
        assert page.total == 2

    def test_latest_keeps_the_newest_per_asset(self, store: Store) -> None:
        """One row per asset of the org: its most recent execution, older runs and other orgs dropped."""
        other_org = uuid4()
        asset_a, asset_b, foreign = uuid4(), uuid4(), uuid4()
        old_run, new_run = uuid4(), uuid4()
        t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            (old_run, asset_a, _ORG_ID, "a", "failed", t0),
            (new_run, asset_a, _ORG_ID, "a", "success", t0 + timedelta(hours=1)),
            (old_run, asset_b, _ORG_ID, "b", "running", t0),
            (old_run, foreign, other_org, "x", "success", t0),
        ]
        _seed(
            [
                Execution(run_id=run, component_id=asset, org_id=owner, component_key=key, status=status, created_at=at)
                for run, asset, owner, key, status, at in rows
            ]
        )

        page = store.executions.list(_ORG_ID, ExecutionQuery(latest=True, limit=None))

        assert {(row.component_id, row.run_id, row.status) for row in page.items} == {
            (asset_a, new_run, "success"),
            (asset_b, old_run, "running"),
        }
        assert page.total == 2
        assert store.executions.list(uuid4(), ExecutionQuery(latest=True, limit=None)).items == []

    def test_latest_is_one_row_per_component_under_a_window(self, store: Store) -> None:
        t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        components = [uuid4() for _ in range(3)]
        _seed(
            [
                Execution(
                    run_id=uuid4(),
                    component_id=component,
                    org_id=_ORG_ID,
                    status="success",
                    created_at=t0 + timedelta(hours=hour),
                )
                for component in components
                for hour in range(2)
            ]
        )

        first = store.executions.list(_ORG_ID, ExecutionQuery(latest=True, limit=2))
        rest = store.executions.list(_ORG_ID, ExecutionQuery(latest=True, limit=2, offset=2))

        listed = [row.component_id for row in first.items + rest.items]
        assert sorted(listed) == sorted(components)
        assert first.total == rest.total == 3
        # SQLite round-trips the column naive.
        newest = {row.created_at.replace(tzinfo=timezone.utc) for row in first.items + rest.items if row.created_at}
        assert newest == {t0 + timedelta(hours=1)}


class TestCounts:
    def test_groups_each_run_by_status(self, store: Store) -> None:
        """One count per run and status, for the requested runs only; a run with nothing yet is absent."""
        first, second, unrequested, idle = uuid4(), uuid4(), uuid4(), uuid4()
        rows = [
            (first, "a", "success"),
            (first, "b", "success"),
            (first, "c", "failed"),
            (second, "a", "running"),
            (unrequested, "a", "success"),
        ]
        _seed(
            [
                Execution(run_id=run, component_id=uuid4(), org_id=_ORG_ID, component_key=key, status=status)
                for run, key, status in rows
            ]
        )

        counts = store.executions.counts([first, second, idle])

        assert counts == {first: {"success": 2, "failed": 1}, second: {"running": 1}}
        assert store.executions.counts([]) == {}


def _run(*, org_id: UUID = _ORG_ID, job_id: UUID | None = None, partition_key: str | None = None) -> UUID:
    run_id = uuid4()
    with Session(engine_module.get_engine()) as session:
        session.add(Run(id=run_id, org_id=org_id, component_id=job_id, partition_key=partition_key, status="failed"))
        session.commit()
    return run_id


class TestPartitionCoverage:
    """Per partition of a job, whether each asset ever succeeded."""

    def _execution(self, run_id: UUID, asset_id: UUID, status: str) -> None:
        with Session(engine_module.get_engine()) as session:
            session.add(
                Execution(run_id=run_id, component_id=asset_id, org_id=_ORG_ID, component_key="orders", status=status)
            )
            session.commit()

    def test_any_successful_execution_covers_the_partition(self, store: Store) -> None:
        job, asset = uuid4(), uuid4()
        failed_first = _run(job_id=job, partition_key="2026-07-01")
        healed_later = _run(job_id=job, partition_key="2026-07-01")
        never_healed = _run(job_id=job, partition_key="2026-07-02")
        self._execution(failed_first, asset, "failed")
        self._execution(healed_later, asset, "success")
        self._execution(never_healed, asset, "failed")

        rows = store.executions.partition_coverage(_ORG_ID, job, "2026-07-01", "2026-07-02")

        assert sorted((r.partition_key, r.succeeded) for r in rows) == [("2026-07-01", True), ("2026-07-02", False)]

    def test_other_granularities_jobs_and_orgs_stay_out(self, store: Store) -> None:
        job, asset = uuid4(), uuid4()
        self._execution(_run(job_id=job, partition_key="2026-07-01T13"), asset, "success")
        self._execution(_run(job_id=uuid4(), partition_key="2026-07-01"), asset, "success")
        self._execution(_run(job_id=job, partition_key="2026-07-03"), asset, "success")

        assert store.executions.partition_coverage(_ORG_ID, job, "2026-07-01", "2026-07-02") == []
        assert store.executions.partition_coverage(uuid4(), job, "2026-07-01", "2026-07-03") == []

    def test_a_job_without_runs_has_no_coverage(self, store: Store) -> None:
        assert store.executions.partition_coverage(_ORG_ID, uuid4(), "2026-07-01", "2026-07-02") == []


def _asset_execution(run_id: UUID, asset_id: UUID, status: str) -> None:
    with Session(engine_module.get_engine()) as session:
        session.add(
            Execution(run_id=run_id, component_id=asset_id, org_id=_ORG_ID, component_key="orders", status=status)
        )
        session.commit()


class TestCoverageRows:
    """Org-wide coverage: every asset's time partitions, all-time, from runs of any target."""

    def test_every_time_granularity_of_every_period_is_read(self, store: Store) -> None:
        job, asset = uuid4(), uuid4()
        keys = ["2026-07-02", "2026-07-01T13", "2026-06", "2026", "2024-01-05", "2019"]
        for key in keys:
            _asset_execution(_run(job_id=job, partition_key=key), asset, "success")
        _asset_execution(_run(job_id=job, partition_key="eu"), asset, "success")

        rows = store.executions.coverage_rows(_ORG_ID)

        assert sorted(r.partition_key for r in rows) == sorted(keys)
        assert all(r.asset_id == asset and r.succeeded for r in rows)

    def test_runs_of_every_target_fold_into_one_row_per_asset_and_partition(self, store: Store) -> None:
        asset = uuid4()
        by_job = _run(job_id=uuid4(), partition_key="2026-07-01")
        by_the_asset = _run(job_id=asset, partition_key="2026-07-01")
        by_a_deleted_target = _run(job_id=None, partition_key="2026-07-01")
        _asset_execution(by_job, asset, "failed")
        _asset_execution(by_the_asset, asset, "success")
        _asset_execution(by_a_deleted_target, asset, "failed")

        [row] = store.executions.coverage_rows(_ORG_ID)

        assert (row.asset_id, row.partition_key, row.succeeded, row.failed) == (asset, "2026-07-01", True, True)
        assert row.failed_run_id == max(by_job, by_a_deleted_target)

    def test_a_failed_run_is_reported_until_an_execution_succeeds(self, store: Store) -> None:
        job, asset = uuid4(), uuid4()
        failed = _run(job_id=job, partition_key="2026-07-01")
        healed = _run(job_id=job, partition_key="2026-07-01")
        still_failed = _run(job_id=job, partition_key="2026-07-02")
        _asset_execution(failed, asset, "failed")
        _asset_execution(healed, asset, "success")
        _asset_execution(still_failed, asset, "failed")

        rows = {r.partition_key: r for r in store.executions.coverage_rows(_ORG_ID)}

        assert rows["2026-07-01"].succeeded and rows["2026-07-01"].failed_run_id == failed
        assert not rows["2026-07-02"].succeeded and rows["2026-07-02"].failed_run_id == still_failed

    def test_only_a_failed_execution_marks_the_row_failed(self, store: Store) -> None:
        job, asset = uuid4(), uuid4()
        _asset_execution(_run(job_id=job, partition_key="2026-07-01"), asset, "failed")
        _asset_execution(_run(job_id=job, partition_key="2026-07-02"), asset, "running")
        _asset_execution(_run(job_id=job, partition_key="2026-07-03"), asset, "canceled")

        rows = {r.partition_key: r for r in store.executions.coverage_rows(_ORG_ID)}

        assert {key: (row.succeeded, row.failed) for key, row in rows.items()} == {
            "2026-07-01": (False, True),
            "2026-07-02": (False, False),
            "2026-07-03": (False, False),
        }

    def test_unpartitioned_runs_and_other_orgs_stay_out(self, store: Store) -> None:
        asset = uuid4()
        _asset_execution(_run(job_id=uuid4(), partition_key=None), asset, "success")
        _asset_execution(_run(org_id=uuid4(), job_id=uuid4(), partition_key="2026-07-01"), asset, "success")

        assert store.executions.coverage_rows(_ORG_ID) == []
