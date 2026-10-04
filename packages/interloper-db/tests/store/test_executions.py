"""Tests for ``interloper_db.store.executions``.

SQLite stands in for Postgres: the read model's table definition doubles as the
view's schema, so creating it as a table exercises the exact mapping the view
serves in production.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from uuid import uuid4

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
