"""Tests for migration ``010_settle_superseded_hooks``: superseded rows stop owing hooks.

Postgres-only, like the other migration tests: this module reads a server DSN
from ``INTERLOPER_TEST_POSTGRES_DSN``, provisions a throwaway database, and
drops it afterwards; without the variable it skips.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from urllib.parse import urlparse, urlunparse
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Connection, Engine, text

from interloper_db import engine as engine_module
from interloper_db import provision

pytestmark = pytest.mark.integration

_ORG_ID = uuid4()


@pytest.fixture
def postgres_db() -> Iterator[Engine]:
    """A throwaway Postgres database migrated to head.

    Yields:
        The global engine bound to that database, dropped once the test finishes.
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


def _insert_run(connection: Connection, status: str, root: UUID | None = None, attempt: int = 1) -> UUID:
    run_id = uuid4()
    connection.execute(
        text(
            "INSERT INTO runs (id, org_id, status, root_run_id, retry_of, attempt, billable, completed_at) "
            "VALUES (:id, :org, :status, :root, :retry_of, :attempt, true, now())"
        ),
        {"id": run_id, "org": _ORG_ID, "status": status, "root": root or run_id, "retry_of": root, "attempt": attempt},
    )
    return run_id


def _stamped(engine: Engine, run_ids: list[UUID]) -> list[bool]:
    with engine.connect() as connection:
        rows = connection.execute(
            text("SELECT id, hooks_evaluated_at IS NOT NULL FROM runs WHERE id = ANY(:ids)"), {"ids": run_ids}
        ).all()
    stamps = {run_id: stamped for run_id, stamped in rows}
    return [stamps[run_id] for run_id in run_ids]


def test_upgrade_stamps_canceled_and_retried_runs_and_leaves_verdicts(postgres_db: Engine) -> None:
    provision.downgrade(postgres_db, "009")
    with postgres_db.begin() as connection:
        canceled = _insert_run(connection, "canceled")
        retried = _insert_run(connection, "failed")
        retry = _insert_run(connection, "success", retried, attempt=2)
        final_failure = _insert_run(connection, "failed")

    provision.upgrade(postgres_db, "010")

    assert _stamped(postgres_db, [canceled, retried, retry, final_failure]) == [True, True, False, False]
