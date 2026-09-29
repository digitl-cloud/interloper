"""Tests for migration ``006_hook_cursors``: the hook evaluator's delivery cursor.

The migration stamps every terminal run and backfill as already evaluated, so
the first sweep after the deploy does not replay history, and leaves open rows
unstamped. Postgres-only, like the other migration tests: this module reads a
server DSN from ``INTERLOPER_TEST_POSTGRES_DSN``, provisions a throwaway
database, and drops it afterwards; without the variable it skips.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from urllib.parse import urlparse, urlunparse
from uuid import uuid4

import pytest
from sqlalchemy import Engine, text

from interloper_db import engine as engine_module
from interloper_db import provision

pytestmark = pytest.mark.integration

_ORG_ID = uuid4()


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


def _insert_run(connection, status: str, completed: bool) -> str:
    run_id = uuid4()
    connection.execute(
        text(
            "INSERT INTO runs (id, org_id, status, root_run_id, attempt, billable, completed_at) "
            "VALUES (:id, :org, :status, :id, 1, true, :completed_at)"
        ),
        {
            "id": run_id,
            "org": _ORG_ID,
            "status": status,
            "completed_at": "2026-09-01T00:00:00+00:00" if completed else None,
        },
    )
    return str(run_id)


def _insert_backfill(connection, status: str, completed: bool) -> str:
    backfill_id = uuid4()
    connection.execute(
        text(
            "INSERT INTO backfills (id, org_id, status, start_key, end_key, concurrency, fail_fast, partitions, "
            "completed_at) VALUES (:id, :org, :status, '2026-09-01', '2026-09-02', 1, false, 2, :completed_at)"
        ),
        {
            "id": backfill_id,
            "org": _ORG_ID,
            "status": status,
            "completed_at": "2026-09-02T00:00:00+00:00" if completed else None,
        },
    )
    return str(backfill_id)


def test_upgrade_stamps_terminal_rows_and_leaves_open_ones(postgres_db: Engine) -> None:
    provision.downgrade(postgres_db, "005")
    with postgres_db.begin() as connection:
        done_run = _insert_run(connection, "success", completed=True)
        failed_run = _insert_run(connection, "failed", completed=True)
        open_run = _insert_run(connection, "running", completed=False)
        done_backfill = _insert_backfill(connection, "failed", completed=True)
        open_backfill = _insert_backfill(connection, "running", completed=False)

    provision.upgrade(postgres_db, "006")

    with postgres_db.connect() as connection:
        stamped_runs = {
            str(row[0]): row[1]
            for row in connection.execute(text("SELECT id, hooks_evaluated_at FROM runs")).all()
        }
        stamped_backfills = {
            str(row[0]): row[1]
            for row in connection.execute(text("SELECT id, hooks_evaluated_at FROM backfills")).all()
        }
    assert stamped_runs[done_run] is not None and stamped_runs[failed_run] is not None
    assert stamped_runs[open_run] is None
    assert stamped_backfills[done_backfill] is not None
    assert stamped_backfills[open_backfill] is None
