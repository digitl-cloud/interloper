"""Shared fixtures: an in-memory SQLite database with the component tables.

SQLite stands in for Postgres in unit tests (the models use portable types
on purpose). Foreign keys are switched on per connection so ON DELETE
CASCADE behaves like production.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import Engine, event
from sqlalchemy.pool import StaticPool

from interloper_db import engine as engine_module
from interloper_db.models import Component, ComponentRelation


@pytest.fixture
def component_db() -> Iterator[Engine]:
    """A fresh in-memory database with the two component tables, FKs enforced."""
    eng = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(eng, "connect")
    def _enable_foreign_keys(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.execute("PRAGMA foreign_keys=ON")

    Component.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    ComponentRelation.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        yield eng
    finally:
        eng.dispose()
        engine_module._engine = None
