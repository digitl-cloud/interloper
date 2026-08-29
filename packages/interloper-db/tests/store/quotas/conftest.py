"""Shared fixtures for the quota tests: a fresh database and the rows to poke at it."""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterator
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, select

from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Component, Quota, Run, Usage
from interloper_db.store import Store

_ORG_ID = uuid4()


@pytest.fixture
def org_id() -> UUID:
    """The organisation every quota test works against.

    Returns:
        A fixed organisation id; the database is fresh per test, so one value
        serves the whole suite.
    """
    return _ORG_ID


@pytest.fixture
def store() -> Iterator[Store]:
    """A store over a fresh in-memory SQLite database carrying the quota tables.

    Yields:
        The store bound to that database, disposed once the test finishes.
    """
    engine = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _sqlite_uuid(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    for model in (Component, Backfill, Run, Quota, Usage):
        model.__table__.create(engine)  # ty: ignore[unresolved-attribute]
    try:
        yield Store(catalog=il.Catalog(components={}), engine=engine)
    finally:
        engine.dispose()
        engine_module._engine = None


@pytest.fixture
def usage_rows(store: Store, org_id: UUID) -> Callable[[], dict[dt.date, tuple[int, int]]]:
    """Read the organisation's ledger rows as ``{period: (used, reserved)}``.

    Args:
        store: The store under test.
        org_id: Organisation whose ledger is read.

    Returns:
        A zero-argument reader, callable again after every mutation.
    """

    def read() -> dict[dt.date, tuple[int, int]]:
        with Session(store.engine) as session:
            rows = session.exec(select(Usage).where(Usage.org_id == org_id)).all()
            return {row.period_start: (row.used, row.reserved) for row in rows}

    return read


@pytest.fixture
def make_run(store: Store, org_id: UUID) -> Callable[..., Run]:
    """Insert a running run for the organisation and hand it back.

    Args:
        store: The store under test.
        org_id: Organisation the run belongs to.

    Returns:
        A factory taking an optional ``quota_reserved_at`` stamp.
    """

    def create(*, quota_reserved_at: datetime | None = None) -> Run:
        with Session(store.engine) as session:
            run = Run(id=uuid4(), org_id=org_id, status="running", quota_reserved_at=quota_reserved_at)
            session.add(run)
            session.commit()
            session.refresh(run)
            return run

    return create
