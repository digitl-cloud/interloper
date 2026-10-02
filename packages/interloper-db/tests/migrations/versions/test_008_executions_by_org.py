"""Tests for migration ``008_executions_by_org``: the organisation-scoped ``executions`` view.

Postgres-only, like the other migration tests: this module reads a server DSN
from ``INTERLOPER_TEST_POSTGRES_DSN``, provisions a throwaway database
migrated to head, and drops it afterwards; without the variable it skips.
"""

from __future__ import annotations

import datetime as dt
import os
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlparse, urlunparse
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, text
from sqlmodel import Session, select

from interloper_db import engine as engine_module
from interloper_db import provision
from interloper_db.models import Event, Execution, Run

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_db() -> Iterator[Engine]:
    """A throwaway Postgres database migrated to head.

    Yields:
        The global engine bound to that database, dropped once the module finishes.
    """
    server_dsn = os.getenv("INTERLOPER_TEST_POSTGRES_DSN")
    if not server_dsn:
        pytest.skip("INTERLOPER_TEST_POSTGRES_DSN not set")
    dsn = urlunparse(urlparse(server_dsn)._replace(path=f"/interloper_test_{uuid4().hex[:8]}"))
    provision.ensure_database(dsn)
    engine = engine_module.init_engine(dsn)
    try:
        provision.create_all(engine)
        yield engine
    finally:
        engine.dispose()
        engine_module._engine = None
        provision.drop_database(dsn)


def _execute(session: Session, org_id: UUID, event_type: str) -> UUID:
    """Record a run of an organisation with one operation event.

    Args:
        session: The session the rows are added to, left uncommitted.
        org_id: The organisation owning the run and its event.
        event_type: The operation event's type.

    Returns:
        The new run's id.
    """
    run = Run(org_id=org_id, status="running")
    session.add(run)
    session.flush()
    session.add(
        Event(
            id=uuid4(),
            org_id=org_id,
            run_id=run.id,
            event_type=event_type,
            component_id=uuid4(),
            component_key="orders",
            timestamp=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
        )
    )
    return run.id


def _events_scans(plan: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Walk an ``EXPLAIN (FORMAT JSON)`` plan for the nodes that read ``events``.

    Args:
        plan: A plan node, its children under ``Plans``.

    Yields:
        Each node, this one or below, whose relation is ``events``.
    """
    if plan.get("Relation Name") == "events":
        yield plan
    for child in plan.get("Plans", []):
        yield from _events_scans(child)


class TestOrganisationScope:
    def test_an_organisation_reads_only_its_own_executions(self, postgres_db: Engine) -> None:
        org_id, other_org_id = uuid4(), uuid4()
        with Session(postgres_db) as session:
            mine = _execute(session, org_id, "operation_completed")
            _execute(session, other_org_id, "operation_failed")
            session.commit()

        with Session(postgres_db) as session:
            rows = session.exec(select(Execution).where(Execution.org_id == org_id)).all()

        assert [(row.run_id, row.status) for row in rows] == [(mine, "success")]

    def test_the_organisation_filter_reaches_the_events_scan(self, postgres_db: Engine) -> None:
        with postgres_db.connect() as connection:
            [plan] = connection.execute(
                text("EXPLAIN (FORMAT JSON) SELECT * FROM executions WHERE org_id = :org_id"), {"org_id": uuid4()}
            ).scalar_one()

        [scan] = _events_scans(plan["Plan"])
        assert "org_id" in scan.get("Index Cond", "") + scan.get("Filter", "")


class TestDowngrade:
    def test_it_restores_the_previous_view_and_drops_the_index(self, postgres_db: Engine) -> None:
        def state() -> tuple[str, bool]:
            with postgres_db.connect() as connection:
                definition = connection.execute(text("SELECT pg_get_viewdef('executions')")).scalar_one()
                indexed = connection.execute(text("SELECT to_regclass('ix_events_executions') IS NOT NULL")).scalar()
            return definition, bool(indexed)

        scoped, _ = state()
        provision.downgrade(postgres_db, revision="007")
        try:
            definition, indexed = state()
            assert "PARTITION BY e.org_id" not in definition
            assert "PARTITION BY e.run_id, e.component_id" in definition
            assert not indexed
        finally:
            provision.upgrade(postgres_db)

        assert state() == (scoped, True)


class TestUpgrade:
    def test_it_rebuilds_an_index_a_failed_build_left_invalid(self, postgres_db: Engine) -> None:
        def valid() -> bool | None:
            with postgres_db.connect() as connection:
                return connection.execute(
                    text("SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('ix_events_executions')")
                ).scalar()

        provision.downgrade(postgres_db, revision="007")
        with postgres_db.begin() as connection:
            connection.execute(text("CREATE INDEX ix_events_executions ON events (org_id)"))
            connection.execute(
                text(
                    "UPDATE pg_index SET indisvalid = false "
                    "WHERE indexrelid = to_regclass('ix_events_executions')"
                )
            )
        assert valid() is False

        provision.upgrade(postgres_db)

        assert valid() is True
