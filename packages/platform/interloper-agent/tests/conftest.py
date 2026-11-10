"""Shared fixtures: a real store over in-memory SQLite and an editor's context."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from uuid import uuid4

import interloper as il
import pytest
from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Component, ComponentRelation, Event, Execution, Quota, Run, Usage
from interloper_db.store import Store
from interloper_toolkit import ToolkitContext
from sqlalchemy import Engine, event
from sqlalchemy.pool import StaticPool


class DemoConnection(il.Connection):
    """The connection the test source binds."""


class ShopSource(il.Source):
    """A source with one asset, enough for creation flows."""

    connection: DemoConnection

    class Orders(il.Asset):
        """Order rows."""

        def data(self, context: il.ExecutionContext) -> list[dict]:
            return []


@pytest.fixture
def agent_db() -> Iterator[Engine]:
    """A fresh in-memory database with the tables the toolkit reads.

    Yields:
        The engine bound to that database, disposed once the test finishes.
    """
    eng = engine_module.init_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)

    @event.listens_for(eng, "connect")
    def _configure_connection(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.execute("PRAGMA foreign_keys=ON")
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    for model in (Component, ComponentRelation, Backfill, Run, Event, Execution, Quota, Usage):
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        yield eng
    finally:
        eng.dispose()
        engine_module._engine = None


@pytest.fixture
def store(agent_db: Engine) -> Store:
    catalog = il.Catalog.from_assets([ShopSource])
    return Store(catalog=catalog, encrypt=lambda b: b, decrypt=lambda b: b)


@pytest.fixture
def ctx(store: Store) -> ToolkitContext:
    catalog = il.Catalog.from_assets([ShopSource]).dump()
    return ToolkitContext(store=store, catalog=catalog, org_id=uuid4(), role="editor")
