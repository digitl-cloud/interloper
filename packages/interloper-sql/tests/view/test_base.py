"""Tests for IbisView: Ibis tables in, a compiled definition out."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any, ClassVar

import ibis
import interloper as il
import pytest
from interloper.destination import DatabaseDestination
from interloper.errors import ConfigError
from interloper.runner import ExecutionStatus

from interloper_sql import ibis_view, latest, union
from interloper_sql.view.base import IbisDefinition
from interloper_sql.view.types import IBIS_DIALECTS


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
    @ibis_view(schema=Totals, relations={"orders": il.Relation("asset", "shop.orders", many=True)})
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
    sql = _run(Reporting(destinations=[warehouse]), Shop(account_id="1", destinations=[warehouse], enabled=False))
    assert '"shop"."orders__1"' in sql
    assert "SUM(" in sql
    assert isinstance(Reporting.totals, type) and issubclass(Reporting.totals, il.View)


def test_union_stamps_a_column_per_upstream():
    class Stamped(il.Schema):
        date: dt.date | None
        total: float | None
        account_id: str | None

    @il.source
    class StampedReporting(il.Source):
        @ibis_view(schema=Stamped, relations={"orders": il.Relation("asset", "shop.orders", many=True)})
        def totals(self, orders: list[il.Upstream]) -> ibis.Table:
            return union(orders, account_id=lambda upstream: upstream.asset.source.discriminator)  # ty: ignore[unresolved-attribute]

    warehouse = RecordingWarehouse(id="warehouse")
    sql = _run(
        StampedReporting(destinations=[warehouse]),
        Shop(account_id="1", destinations=[warehouse], enabled=False),
        Shop(account_id="2", destinations=[warehouse], enabled=False),
    )
    assert "'1'" in sql and "'2'" in sql and "UNION ALL" in sql


def test_union_stamps_an_integer_as_int64():
    class Numbered(il.Schema):
        date: dt.date | None
        total: float | None
        account_number: int | None

    @il.source
    class NumberedReporting(il.Source):
        @ibis_view(schema=Numbered, relations={"orders": il.Relation("asset", "shop.orders", many=True)})
        def totals(self, orders: list[il.Upstream]) -> ibis.Table:
            return union(orders, account_number=lambda upstream: 1)

    warehouse = RecordingWarehouse(id="warehouse")
    shop = Shop(account_id="1", destinations=[warehouse], enabled=False)
    sql = _run(NumberedReporting(destinations=[warehouse]), shop)
    assert "1 AS" in sql


def test_union_of_nothing_is_a_config_error():
    with pytest.raises(ConfigError, match="no upstream bound"):
        union([])


def test_an_expression_that_misses_a_declared_column_is_refused():
    @il.source
    class Wrong(il.Source):
        @ibis_view(schema=Totals, relations={"orders": il.Relation("asset", "shop.orders", many=True)})
        def totals(self, orders: list[il.Upstream]) -> ibis.Table:
            return union(orders).select("date")

    warehouse = RecordingWarehouse(id="warehouse")
    shop = Shop(account_id="1", destinations=[warehouse], enabled=False)
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
        @ibis_view(schema=Totals, relations={"rows": il.Relation("asset", "untyped.rows")})
        def totals(self, rows: il.Upstream) -> ibis.Table:
            return rows.data

    warehouse = RecordingWarehouse(id="warehouse")
    untyped = Untyped(destinations=[warehouse], enabled=False)
    over = Over(destinations=[warehouse])
    result = il.DAG(untyped, over).materialize()
    assert result.status is ExecutionStatus.FAILED
    assert "declares no schema" in (result.executions[over.totals.id].error or "")
    assert RecordingWarehouse.views == []


@pytest.mark.parametrize("dialect", sorted(IBIS_DIALECTS))
def test_a_definition_compiles_for_every_dialect(dialect: str):
    warehouse = RecordingWarehouse(id="warehouse")
    shop = Shop(account_id="1", destinations=[warehouse], enabled=False)
    reporting = Reporting(destinations=[warehouse])
    dag = il.DAG(shop, reporting)
    view = reporting.totals
    definition = il.run(view.run_async(None, dag, {}))
    assert "SELECT" in definition.compile(dialect)


def test_a_ibis_view_runs_on_duckdb(tmp_path: Path):
    duckdb = pytest.importorskip("interloper_duckdb")
    warehouse = duckdb.DuckDBDestination(
        id="warehouse", connection=duckdb.DuckDBConnection(id="duckdb", database=str(tmp_path / "t.duckdb"))
    )
    client = warehouse.connection.client
    client.execute("CREATE SCHEMA shop")
    client.execute(
        "CREATE TABLE shop.orders__1 AS SELECT * FROM (VALUES "
        "(DATE '2026-10-01', 10.0), (DATE '2026-10-01', 5.0), (DATE '2026-10-02', 2.0)) AS t(date, total)"
    )
    client.execute("CREATE TABLE shop.orders__2 AS SELECT * FROM (VALUES (3.0, DATE '2026-10-02')) AS t(total, date)")
    result = il.DAG(
        Shop(account_id="1", destinations=[warehouse], enabled=False),
        Shop(account_id="2", destinations=[warehouse], enabled=False),
        Reporting(destinations=[warehouse]),
    ).materialize()
    assert result.status is ExecutionStatus.COMPLETED
    frame = warehouse.select("totals", "reporting", None).sort_values("date")
    assert frame["total"].tolist() == [15.0, 5.0]


def test_latest_keeps_the_first_row_per_key_in_the_given_order():
    table = ibis.table({"key": "string", "tier": "int64", "date": "date"}, name="rows")
    kept = latest(table, "key", table["tier"].asc(), table["date"].desc())
    assert list(kept.columns) == ["key", "tier", "date"]
    sql = ibis.to_sql(kept, dialect="duckdb")
    assert "ROW_NUMBER() OVER" in sql and "PARTITION BY" in sql and "ORDER BY" in sql


def test_latest_drops_the_rows_without_a_key():
    duckdb = pytest.importorskip("duckdb")
    rows = ibis.table({"key": "string", "tier": "int64"}, name="candidates")
    kept = latest(rows, "key", rows["tier"].asc())
    client = duckdb.connect()
    client.execute("CREATE TABLE candidates AS SELECT * FROM (VALUES ('a', 1), (NULL, 1), (NULL, 2)) AS t(key, tier)")
    assert client.execute(ibis.to_sql(kept, dialect="duckdb")).fetchall() == [("a", 1)]


def test_union_keeps_only_the_named_columns():
    table = ibis.table({"date": "date", "total": "float64"}, name="orders__1")
    rows = union([il.Upstream(asset=Shop(account_id="1").orders, data=table)], "total")
    assert list(rows.columns) == ["total"]


def test_a_dialect_without_an_ibis_counterpart_is_a_config_error():
    definition = IbisDefinition(ibis.table({"a": "int64"}, name="t"))
    with pytest.raises(ConfigError, match="No Ibis dialect for 'nope'"):
        definition.compile("nope")


def test_a_three_part_table_name_sets_the_catalog():
    class ThreePartWarehouse(RecordingWarehouse):
        def table_parts(self, table, dataset):
            return ("proj", dataset or "main", table)

    warehouse = ThreePartWarehouse(id="warehouse")
    sql = _run(Reporting(destinations=[warehouse]), Shop(account_id="1", destinations=[warehouse], enabled=False))
    assert '"proj"."shop"."orders__1"' in sql


def test_an_upstream_that_is_not_a_table_is_refused():
    class Opaque(RecordingWarehouse):
        def reference(self, asset):
            return "not a table"

    warehouse = Opaque(id="warehouse")
    shop = Shop(account_id="1", destinations=[warehouse], enabled=False)
    reporting = Reporting(destinations=[warehouse])
    result = il.DAG(shop, reporting).materialize()
    assert result.status is ExecutionStatus.FAILED
    assert "is not a table" in (result.executions[reporting.totals.id].error or "")


def test_a_result_that_is_not_an_ibis_table_is_refused():
    @il.source
    class Textual(il.Source):
        @ibis_view(schema=Totals, relations={"orders": il.Relation("asset", "shop.orders", many=True)})
        def totals(self, orders: list[il.Upstream]) -> str:
            return "SELECT 1"

    warehouse = RecordingWarehouse(id="warehouse")
    shop = Shop(account_id="1", destinations=[warehouse], enabled=False)
    textual = Textual(destinations=[warehouse])
    result = il.DAG(shop, textual).materialize()
    assert result.status is ExecutionStatus.FAILED
    assert "returned str, not an Ibis table" in (result.executions[textual.totals.id].error or "")
    assert RecordingWarehouse.views == []


def test_a_view_without_a_schema_is_refused():
    @il.source
    class Unschematic(il.Source):
        @ibis_view(relations={"orders": il.Relation("asset", "shop.orders", many=True)})
        def totals(self, orders: list[il.Upstream]) -> ibis.Table:
            return union(orders)

    warehouse = RecordingWarehouse(id="warehouse")
    shop = Shop(account_id="1", destinations=[warehouse], enabled=False)
    unschematic = Unschematic(destinations=[warehouse])
    result = il.DAG(shop, unschematic).materialize()
    assert result.status is ExecutionStatus.FAILED
    assert "declares no schema" in (result.executions[unschematic.totals.id].error or "")
    assert RecordingWarehouse.views == []
