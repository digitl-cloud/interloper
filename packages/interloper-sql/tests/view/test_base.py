"""Tests for SqlView: Ibis tables in, a compiled definition out."""

from __future__ import annotations

import datetime as dt
from typing import Any, ClassVar

import ibis
import interloper as il
import pytest
from interloper.destination.database import DatabaseDestination
from interloper.errors import ConfigError
from interloper.runner.results import ExecutionStatus

from interloper_sql import sql_view, union


class RecordingWarehouse(DatabaseDestination):
    dialect: ClassVar[str | None] = "duckdb"
    views: ClassVar[list[tuple[str, str | None, str]]] = []

    def insert(self, table, dataset, data, context):
        pass

    def delete(self, table, dataset, where):
        pass

    def select(self, table, dataset, where):
        return []

    def count(self, table, dataset, column):
        return {}

    def table_parts(self, table, dataset):
        return (dataset or "main", table)

    def replace_view(self, table, dataset, sql):
        self.views.append((table, dataset, sql))


class Orders(il.Schema):
    date: dt.date | None
    total: float | None


class Totals(il.Schema):
    date: dt.date | None
    total: float | None


@il.source
class Shop(il.Source):
    account_id: str = il.InputField(default="", discriminator=True)

    @il.asset(schema=Orders, partitioning=il.TimePartitionConfig(column="date"))
    def orders(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        return []


@il.source
class Reporting(il.Source):
    @sql_view(schema=Totals, relations={"orders": il.Relation("asset", "shop.orders", many=True)})
    def totals(self, orders: list[il.Upstream]) -> ibis.Table:
        rows = union(orders)
        return rows.group_by(rows.date).aggregate(total=rows.total.sum())


@pytest.fixture(autouse=True)
def _reset() -> None:
    RecordingWarehouse.views.clear()


def _run(view_source: il.Source, *shops: il.Source) -> str:
    il.DAG(*shops, view_source).materialize()
    [(_, _, sql)] = RecordingWarehouse.views
    return sql


def test_upstreams_arrive_as_typed_ibis_tables_and_the_definition_compiles_for_the_dialect():
    warehouse = RecordingWarehouse(id="warehouse")
    sql = _run(Reporting(destinations=[warehouse]), Shop(account_id="1", destinations=[warehouse], select=[]))
    assert '"shop"."orders__1"' in sql
    assert "SUM(" in sql
    assert isinstance(Reporting.totals, type) and issubclass(Reporting.totals, il.View)  # ty: ignore[unresolved-attribute]


def test_union_stamps_a_column_per_upstream():
    class Stamped(il.Schema):
        date: dt.date | None
        total: float | None
        account_id: str | None

    @il.source
    class StampedReporting(il.Source):
        @sql_view(schema=Stamped, relations={"orders": il.Relation("asset", "shop.orders", many=True)})
        def totals(self, orders: list[il.Upstream]) -> ibis.Table:
            return union(orders, account_id=lambda upstream: upstream.asset.source.discriminator)  # ty: ignore[unresolved-attribute]

    warehouse = RecordingWarehouse(id="warehouse")
    sql = _run(
        StampedReporting(destinations=[warehouse]),
        Shop(account_id="1", destinations=[warehouse], select=[]),
        Shop(account_id="2", destinations=[warehouse], select=[]),
    )
    assert "'1'" in sql and "'2'" in sql and "UNION ALL" in sql


def test_union_of_nothing_is_a_config_error():
    with pytest.raises(ConfigError, match="no upstream bound"):
        union([])


def test_an_expression_that_misses_a_declared_column_is_refused():
    @il.source
    class Wrong(il.Source):
        @sql_view(schema=Totals, relations={"orders": il.Relation("asset", "shop.orders", many=True)})
        def totals(self, orders: list[il.Upstream]) -> ibis.Table:
            return union(orders).select("date")

    warehouse = RecordingWarehouse(id="warehouse")
    shop = Shop(account_id="1", destinations=[warehouse], select=[])
    wrong = Wrong(destinations=[warehouse])
    result = il.DAG(shop, wrong).materialize()
    assert result.status is ExecutionStatus.FAILED
    error = result.executions[wrong.totals.id].error or ""
    assert "declares columns" in error and "missing ['total']" in error
    assert RecordingWarehouse.views == []


def test_an_upstream_without_a_schema_is_refused():
    @il.source
    class Untyped(il.Source):
        @il.asset
        def rows(self) -> list[dict[str, Any]]:
            return []

    @il.source
    class Over(il.Source):
        @sql_view(schema=Totals, relations={"rows": il.Relation("asset", "untyped.rows")})
        def totals(self, rows: il.Upstream) -> ibis.Table:
            return rows.data

    warehouse = RecordingWarehouse(id="warehouse")
    untyped = Untyped(destinations=[warehouse], select=[])
    over = Over(destinations=[warehouse])
    result = il.DAG(untyped, over).materialize()
    assert result.status is ExecutionStatus.FAILED
    assert "declares no schema" in (result.executions[over.totals.id].error or "")
    assert RecordingWarehouse.views == []
