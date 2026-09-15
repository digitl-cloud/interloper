"""Tests for migration ``003_realtime_notify``: run and backfill notifications carry the target's identity.

The notify functions are Postgres triggers, so this module needs a live
server. It reads a server DSN from ``INTERLOPER_TEST_POSTGRES_DSN``,
provisions a throwaway database migrated to head, and drops it afterwards;
without the variable the module skips.
"""

from __future__ import annotations

import json
import os
import select
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlparse, urlunparse
from uuid import uuid4

import pytest
from sqlalchemy import Engine, text
from sqlmodel import Session, SQLModel

from interloper_db import engine as engine_module
from interloper_db import provision
from interloper_db.models import Backfill, Component, Run

pytestmark = pytest.mark.integration

_ORG_ID = uuid4()
_IDENTITY = ("component_kind", "component_key", "component_name")


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


class Listener:
    """A ``LISTEN table_changes`` connection handing back decoded payloads."""

    def __init__(self, engine: Engine) -> None:
        """Open the listening connection.

        Args:
            engine: The engine to take the connection from.
        """
        self._connection = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
        self._connection.execute(text("LISTEN table_changes"))

    def record(self, table: str, op: str, timeout: float = 5.0) -> dict[str, Any]:
        """The record of the one notification matching a table and operation.

        Args:
            table: The notifying table.
            op: The trigger operation (``INSERT``, ``UPDATE``, ``DELETE``).
            timeout: Seconds to wait for notifications to arrive.

        Returns:
            The notification's ``record`` payload.
        """
        # psycopg2 ships no stubs; the LISTEN/NOTIFY surface is driver-level.
        raw: Any = self._connection.connection.dbapi_connection
        select.select([raw], [], [], timeout)
        raw.poll()
        payloads = [json.loads(notify.payload) for notify in raw.notifies]
        raw.notifies.clear()
        (match,) = [p for p in payloads if p["table"] == table and p["op"] == op]
        return match["record"]

    def close(self) -> None:
        """Release the connection."""
        self._connection.close()


@pytest.fixture
def listener(postgres_db: Engine) -> Iterator[Listener]:
    """A listener subscribed before the test writes anything.

    Yields:
        The listener, closed once the test finishes.
    """
    listener = Listener(postgres_db)
    try:
        yield listener
    finally:
        listener.close()


def _insert(engine: Engine, *rows: SQLModel) -> None:
    """Persist rows in one transaction.

    Args:
        engine: The engine to write through.
        *rows: The rows to add.
    """
    with Session(engine, expire_on_commit=False) as session:
        session.add_all(rows)
        session.commit()


def _job(engine: Engine) -> Component:
    """A committed job component to target.

    Args:
        engine: The engine to write through.

    Returns:
        The component, with its id populated.
    """
    job = Component(org_id=_ORG_ID, kind="job", key="demo.job", name="Demo job")
    _insert(engine, job)
    return job


def _identity(record: dict[str, Any]) -> dict[str, Any]:
    """The target-identity fields of a notification record.

    Args:
        record: The notification's record payload.

    Returns:
        The three ``component_*`` identity fields.
    """
    return {key: record[key] for key in _IDENTITY}


@pytest.mark.parametrize(
    "make_row",
    [
        lambda job: Run(org_id=_ORG_ID, component_id=job.id),
        lambda job: Backfill(org_id=_ORG_ID, component_id=job.id, start_key="2026-01-01", end_key="2026-01-02"),
    ],
    ids=["runs", "backfills"],
)
def test_insert_notification_carries_the_target_identity(postgres_db: Engine, listener: Listener, make_row):
    job = _job(postgres_db)
    row = make_row(job)
    _insert(postgres_db, row)

    record = listener.record(row.__tablename__, "INSERT")
    assert record["component_id"] == str(job.id)
    assert _identity(record) == {"component_kind": "job", "component_key": "demo.job", "component_name": "Demo job"}


def test_deleting_the_target_notifies_a_deleted_identity(postgres_db: Engine, listener: Listener):
    job = _job(postgres_db)
    _insert(postgres_db, Run(org_id=_ORG_ID, component_id=job.id))
    listener.record("runs", "INSERT")

    with Session(postgres_db) as session:
        session.delete(session.get(Component, job.id))
        session.commit()

    record = listener.record("runs", "UPDATE")
    assert record["component_id"] is None
    assert _identity(record) == {"component_kind": None, "component_key": None, "component_name": None}
