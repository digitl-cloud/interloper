"""Tests for migration ``011_run_heartbeat``: a heartbeat alone never reaches the realtime channel.

The notify triggers are Postgres triggers, so this module needs a live
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

import interloper as il
import pytest
from sqlalchemy import Engine, text
from sqlmodel import Session

from interloper_db import engine as engine_module
from interloper_db import provision
from interloper_db.models import Run, RunStatus
from interloper_db.store import Store

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


def _notifications(engine: Engine, write: Any) -> list[dict[str, Any]]:
    """The ``runs`` notifications one write fires.

    Args:
        engine: The engine to listen and write through.
        write: Called with no arguments to perform the write.

    Returns:
        The decoded payloads of the ``runs`` notifications it fired.
    """
    connection = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
    try:
        connection.execute(text("LISTEN table_changes"))
        write()
        # psycopg2 ships no stubs; the LISTEN/NOTIFY surface is driver-level.
        raw: Any = connection.connection.dbapi_connection
        select.select([raw], [], [], 1.0)
        raw.poll()
        payloads = [json.loads(notify.payload) for notify in raw.notifies]
        raw.notifies.clear()
        return [payload for payload in payloads if payload["table"] == "runs"]
    finally:
        # The connection goes back to the pool, which would keep it subscribed.
        connection.execute(text("UNLISTEN table_changes"))
        connection.close()


def _running_run(engine: Engine) -> Run:
    run = Run(org_id=uuid4(), status=RunStatus.RUNNING)
    with Session(engine, expire_on_commit=False) as session:
        session.add(run)
        session.commit()
    return run


def test_a_heartbeat_does_not_notify(postgres_db: Engine) -> None:
    run = _running_run(postgres_db)
    store = Store(catalog=il.Catalog(components={}), engine=postgres_db)
    renewed: list[bool] = []

    assert _notifications(postgres_db, lambda: renewed.append(store.runs.heartbeat(run.id))) == []
    assert renewed == [True]


def test_a_status_change_still_notifies(postgres_db: Engine) -> None:
    run = _running_run(postgres_db)

    def fail() -> None:
        with postgres_db.begin() as connection:
            connection.execute(
                text("UPDATE runs SET status = 'failed', heartbeat_at = now() WHERE id = :id"), {"id": run.id}
            )

    [notification] = _notifications(postgres_db, fail)
    assert (notification["op"], notification["record"]["status"]) == ("UPDATE", "failed")


def test_an_insert_still_notifies(postgres_db: Engine) -> None:
    [notification] = _notifications(postgres_db, lambda: _running_run(postgres_db))

    assert notification["op"] == "INSERT"
