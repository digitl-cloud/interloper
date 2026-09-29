"""Shared fixtures for the toolkit tests.

A real Store over in-memory SQLite plus a hand-built dumped catalog. The
store's catalog knows two real sources whose assets relate by name, so the
lineage and relation tools run against genuinely declared relations.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Component, ComponentRelation, Event, Execution, Quota, Run, Usage
from interloper_db.store import Store
from sqlalchemy import Engine, event
from sqlalchemy.pool import StaticPool

from interloper_toolkit import ToolkitContext


class DemoConnection(il.Connection):
    """Connection the lineage-by-name test sources bind."""


class ShopSource(il.Source):
    """Source owning the asset a cross-source relation points at by name."""

    connection: DemoConnection

    class Orders(il.Asset):
        """Order rows another source's asset depends on."""

        def data(self, context: il.ExecutionContext) -> list[dict]:
            return []


class FinanceSource(il.Source):
    """Source whose report asset depends on shop_source's orders by name."""

    connection: DemoConnection

    class Revenue(il.Asset):
        """Revenue rows, computed from shop_source's orders."""

        orders = il.Relation("asset", "shop_source.orders")

        def data(self, context: il.ExecutionContext, orders: il.Upstream) -> list[dict]:
            return []


CATALOG_DUMP: dict[str, Any] = {
    "facebook_ads": {
        "kind": "source",
        "name": "Facebook Ads",
        "config_schema": {
            "$defs": {
                "MaterializationStrategy": {"enum": ["strict", "reconcile"], "type": "string"},
            },
            "properties": {
                "materialization_strategy": {"$ref": "#/$defs/MaterializationStrategy", "default": "reconcile"},
            },
        },
        "assets": [
            {
                "key": "ads",
                "asset_schema": {
                    "properties": {
                        "campaign_id": {"type": "string", "description": "Campaign identifier"},
                        "spend": {"type": "number", "description": "Total spend"},
                    }
                },
            },
        ],
    },
    "google_ads": {
        "kind": "source",
        "name": "Google Ads",
        "assets": [
            {
                "key": "campaigns",
                "asset_schema": {
                    "properties": {
                        "campaign_id": {"type": "string", "description": "Campaign identifier"},
                        "clicks": {"type": "integer", "description": "Click count"},
                    }
                },
            },
        ],
    },
}


@pytest.fixture
def toolkit_db() -> Iterator[Engine]:
    """A fresh in-memory database with the tables the toolkit reads.

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
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    # Execution maps a Postgres view; its table definition doubles as the view's
    # schema, so SQLite gets it as a plain table the tests write rows into.
    for model in (Component, ComponentRelation, Backfill, Run, Event, Execution, Quota, Usage):
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        yield eng
    finally:
        eng.dispose()
        engine_module._engine = None


@pytest.fixture
def store(toolkit_db: Engine) -> Store:
    """A store whose catalog knows the lineage-by-name test sources.

    Returns:
        A store reading and writing the fixture database.

    """
    # An identity cipher: connections are encrypted by default, and the
    # write tools create them the way production does.
    return Store(catalog=il.Catalog.from_assets([ShopSource, FinanceSource]), encrypt=lambda b: b, decrypt=lambda b: b)


@pytest.fixture
def create_source(store: Store) -> Callable[[UUID, str], Component]:
    """A factory for source rows, each bound to the connection its class requires.

    Returns:
        A callable taking the organisation id and the source's catalog key.
    """

    def create(org_id: UUID, key: str) -> Component:
        connection = store.components.create(
            org_id, kind="connection", key="demo_connection", config={}, encrypted=False
        )
        return store.components.create(org_id, kind="source", key=key, relations={"connection": [connection.id]})

    return create


@pytest.fixture
def ctx(store: Store) -> ToolkitContext:
    """An editor's context over the hand-built dump plus the store's own definitions.

    Returns:
        The context; ``dataclasses.replace(ctx, role=...)`` narrows the role.
    """
    catalog = {**CATALOG_DUMP, **il.Catalog.from_assets([ShopSource, FinanceSource]).dump()}
    return ToolkitContext(store=store, catalog=catalog, org_id=uuid4(), role="editor")
