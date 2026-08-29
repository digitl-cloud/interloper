"""Shared fixtures: in-memory SQLite databases carrying the tables under test.

SQLite stands in for Postgres in unit tests (the models use portable types
on purpose). Foreign keys are switched on per connection so ON DELETE
CASCADE behaves like production.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from uuid import uuid4

import interloper as il
import pytest
from sqlalchemy import Engine, event
from sqlalchemy.pool import StaticPool

from interloper_db import engine as engine_module
from interloper_db.models import (
    AuthSession,
    Backfill,
    Component,
    ComponentRelation,
    Event,
    Invitation,
    Organisation,
    PersonalAccessToken,
    Profile,
    Quota,
    Run,
    Usage,
    UserOrganisation,
)
from interloper_db.store import Store


@pytest.fixture
def component_db() -> Iterator[Engine]:
    """A fresh in-memory database with the two component tables, FKs enforced.

    Yields:
        The engine bound to that database, disposed once the test finishes.
    """
    eng = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(eng, "connect")
    def _enable_foreign_keys(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.execute("PRAGMA foreign_keys=ON")

    for model in (Component, ComponentRelation, Quota, Usage):
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        yield eng
    finally:
        eng.dispose()
        engine_module._engine = None


@pytest.fixture
def auth_db() -> Iterator[Engine]:
    """A fresh in-memory database with the auth tables, FKs enforced.

    Yields:
        The engine bound to that database, disposed once the test finishes.
    """
    eng = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(eng, "connect")
    def _configure_connection(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.execute("PRAGMA foreign_keys=ON")
        # The auth tables use Postgres' gen_random_uuid() as server default.
        # Dashless hex to match how SQLAlchemy's Uuid type binds values on SQLite.
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    auth_models = (Profile, Organisation, UserOrganisation, Invitation, AuthSession, PersonalAccessToken)
    org_data_models = (Component, ComponentRelation, Backfill, Run, Event, Quota, Usage)
    for model in auth_models + org_data_models:
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        yield eng
    finally:
        eng.dispose()
        engine_module._engine = None


@pytest.fixture
def store(auth_db: Engine) -> Store:
    """A store over the in-memory database (no catalog needed for these).

    Returns:
        A store with an empty catalog, reading and writing the fixture database.
    """
    return Store(catalog=il.Catalog(components={}))
