"""Tests for views: assets whose definition, not data, is materialized."""

from __future__ import annotations

import datetime as dt
from typing import Any, ClassVar

import pytest

import interloper as il
from interloper.destination.database import DatabaseDestination, TableReference
from interloper.errors import ConfigError
from interloper.runner.results import ExecutionStatus


class RecordingWarehouse(DatabaseDestination):
    """Warehouse double recording the views it is asked to create."""

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


class BigQueryLike(RecordingWarehouse):
    """Warehouse double of another dialect."""

    dialect: ClassVar[str | None] = "bigquery"


@il.source
class Shop(il.Source):
    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def orders(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        return [{"date": context.partition_date, "total": 1.0}]


@il.source
class Reporting(il.Source):
    @il.view(relations={"orders": il.Relation("asset", "shop.orders", many=True)})
    def order_totals(self, orders: list[il.Upstream]) -> str:
        return " UNION ALL ".join(f"SELECT date, total FROM {upstream.data.name}" for upstream in orders)

    @il.view
    def daily_totals(self, order_totals: il.Upstream) -> str:
        return f"SELECT date, SUM(total) AS total FROM {order_totals.data.name} GROUP BY date"


@pytest.fixture(autouse=True)
def _reset() -> None:
    RecordingWarehouse.views.clear()
    il.MemoryDestination.clear()


class TestDefinition:
    def test_a_view_is_an_asset_that_reads_no_upstream_data(self):
        assert issubclass(il.View, il.Asset)
        assert il.View.reads_upstream_data is False
        assert il.View.kind == "asset"
        assert Reporting.order_totals.dialect is None  # ty: ignore[unresolved-attribute]

    def test_upstream_relations_are_inferred_as_for_assets(self):
        relation = Reporting.daily_totals.relations["order_totals"]
        assert (relation.kind, relation.key, relation.many) == ("asset", "order_totals", False)


class TestRun:
    def test_a_view_references_an_upstream_absent_from_the_dag(self):
        @il.asset
        def unrelated() -> list[dict[str, Any]]:
            return []

        warehouse = RecordingWarehouse(id="warehouse")
        shop = Shop(destinations=[warehouse], enabled=False)
        reporting = Reporting(destinations=[warehouse])
        reporting.order_totals.bind("orders", shop.orders)
        definition = il.run(reporting.order_totals.run_async(None, il.DAG(unrelated()), {}))
        assert '"orders"' in definition

    def test_a_run_hands_data_references_and_creates_the_view(self):
        warehouse = RecordingWarehouse(id="warehouse")
        shop_a = Shop(id="a", destinations=[warehouse], enabled=False)
        shop_b = Shop(id="b", destinations=[warehouse], enabled=False)
        reporting = Reporting(destinations=[warehouse])

        il.DAG(shop_a, shop_b, reporting).materialize()

        assert RecordingWarehouse.views == [
            (
                "order_totals",
                "reporting",
                'SELECT date, total FROM "shop"."orders" UNION ALL SELECT date, total FROM "shop"."orders"',
            ),
            (
                "daily_totals",
                "reporting",
                'SELECT date, SUM(total) AS total FROM "reporting"."order_totals" GROUP BY date',
            ),
        ]

    def test_upstream_data_is_the_destinations_reference(self):
        warehouse = RecordingWarehouse(id="warehouse")
        reporting = Reporting(destinations=[warehouse])
        seen: list[Any] = []

        @il.view(relations={"order_totals": il.Relation("asset", "reporting.order_totals")})
        def probe(order_totals: il.Upstream) -> str:
            seen.append(order_totals.data)
            return "SELECT 1"

        view = probe(destinations=[warehouse])
        view.bind("order_totals", reporting.order_totals)
        il.DAG(view).materialize()
        assert seen == [
            TableReference(name='"reporting"."order_totals"', parts=("reporting", "order_totals"), schema=None)
        ]

    def test_a_partitioned_run_recreates_the_same_view(self):
        warehouse = RecordingWarehouse(id="warehouse")
        reporting = Reporting(destinations=[warehouse])
        shop = Shop(destinations=[warehouse], enabled=False)
        dag = il.DAG(shop, reporting)
        dag.materialize(il.TimePartition(dt.date(2026, 1, 1)))
        dag.materialize(il.TimePartition(dt.date(2026, 1, 2)))
        created = [sql for table, _, sql in RecordingWarehouse.views if table == "order_totals"]
        assert len(created) == 2 and created[0] == created[1]

    def test_a_failed_creation_fails_the_view(self):
        class Refusing(RecordingWarehouse):
            def replace_view(self, table, dataset, sql):
                raise RuntimeError("column date not found")

        warehouse = Refusing(id="warehouse")
        reporting = Reporting(destinations=[warehouse])
        shop = Shop(destinations=[warehouse], enabled=False)
        result = il.DAG(shop, reporting).materialize()
        assert result.status is ExecutionStatus.FAILED
        assert result.executions[reporting.order_totals.id].error == "RuntimeError: column date not found"
        assert result.executions[reporting.daily_totals.id].status is ExecutionStatus.CANCELED

    def test_a_disabled_view_creates_nothing(self):
        warehouse = RecordingWarehouse(id="warehouse")
        reporting = Reporting(destinations=[warehouse], enabled=False)
        assert il.run(reporting.order_totals.materialize_async(None, None, {})) is None
        assert RecordingWarehouse.views == []

    def test_a_run_without_exactly_one_destination_is_a_config_error(self):
        @il.view
        def constant() -> str:
            return "SELECT 1"

        with pytest.raises(ConfigError, match="needs exactly one destination, has 0"):
            il.run(constant().materialize_async(None, None, {}))


class TestValidation:
    def test_an_upstream_in_an_unreachable_destination_is_a_config_error(self):
        here = RecordingWarehouse(id="here")
        elsewhere = RecordingWarehouse(id="elsewhere")
        shop = Shop(destinations=[elsewhere])
        reporting = Reporting(destinations=[here])
        with pytest.raises(ConfigError, match=r"shop\.orders"):
            il.DAG(shop, reporting)

    def test_an_upstream_in_a_viewless_destination_is_a_config_error(self):
        warehouse = RecordingWarehouse(id="warehouse")
        shop = Shop(destinations=[il.MemoryDestination(id="memory")])
        reporting = Reporting(destinations=[warehouse])
        with pytest.raises(ConfigError, match=r"shop\.orders"):
            il.DAG(shop, reporting)

    def test_a_view_needs_exactly_one_destination(self):
        shop = Shop(destinations=[RecordingWarehouse(id="a")])
        with pytest.raises(ConfigError, match="exactly one destination"):
            il.DAG(shop, Reporting(destinations=[RecordingWarehouse(id="a"), RecordingWarehouse(id="b")]))
        with pytest.raises(ConfigError, match="exactly one destination"):
            il.DAG(shop, Reporting())

    def test_a_raw_ibis_view_must_match_its_destinations_dialect(self):
        @il.view(dialect="bigquery")
        def sharded() -> str:
            return "SELECT * FROM `p.d.events_*`"

        with pytest.raises(ConfigError, match="dialect 'bigquery'"):
            il.DAG(sharded(destinations=[RecordingWarehouse(id="duck")]))
        il.DAG(sharded(destinations=[BigQueryLike(id="bq")]))

    def test_a_view_source_round_trips_through_specs_without_a_destination(self):
        reporting = Reporting()
        rebuilt = il.Component.from_spec(reporting.to_spec())
        assert isinstance(rebuilt, Reporting)
        assert isinstance(rebuilt.order_totals, il.View)
