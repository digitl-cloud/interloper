"""Tests for migration ``009_run_attempts_unique``: one run per attempt of a stack.

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
from sqlalchemy.exc import IntegrityError

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


def _insert_run(connection: Connection, root: UUID | None = None, attempt: int = 1) -> UUID:
    run_id = uuid4()
    connection.execute(
        text(
            "INSERT INTO runs (id, org_id, status, root_run_id, retry_of, attempt, billable) "
            "VALUES (:id, :org, 'failed', :root, :retry_of, :attempt, true)"
        ),
        {"id": run_id, "org": _ORG_ID, "root": root or run_id, "retry_of": root, "attempt": attempt},
    )
    return run_id


def _index_is_unique(engine: Engine) -> bool | None:
    with engine.connect() as connection:
        return connection.execute(
            text(
                "SELECT i.indisunique AND i.indisvalid FROM pg_index i "
                "JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'ix_runs_root_run_id_attempt'"
            )
        ).scalar()


def test_upgrade_makes_an_attempt_unique_within_its_stack(postgres_db: Engine) -> None:
    provision.downgrade(postgres_db, "008")
    assert _index_is_unique(postgres_db) is None

    provision.upgrade(postgres_db, "009")

    assert _index_is_unique(postgres_db) is True
    with postgres_db.begin() as connection:
        root = _insert_run(connection)
        _insert_run(connection, root, attempt=2)
        with pytest.raises(IntegrityError):
            _insert_run(connection, root, attempt=2)


def test_upgrade_refuses_a_branched_stack(postgres_db: Engine) -> None:
    provision.downgrade(postgres_db, "008")
    with postgres_db.begin() as connection:
        root = _insert_run(connection)
        _insert_run(connection, root, attempt=2)
        _insert_run(connection, root, attempt=2)

    with pytest.raises(RuntimeError, match="1 run stack"):
        provision.upgrade(postgres_db, "009")

    assert _index_is_unique(postgres_db) is None
