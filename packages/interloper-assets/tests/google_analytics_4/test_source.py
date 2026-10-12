"""Tests for the GA4 export source: a reference to the export and a BigQuery-only view over it."""

from __future__ import annotations

from typing import ClassVar

import interloper as il
import pytest
from interloper.destination.database import DatabaseDestination
from interloper.errors import ConfigError

from interloper_assets.google_analytics_4.source import GoogleAnalytics4Export


class RecordingWarehouse(DatabaseDestination):
    key: ClassVar[str] = "bigquery_destination"
    dialect: ClassVar[str | None] = "bigquery"
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
        return ("proj", dataset or "main", table)

    def qualified_name(self, table, dataset):
        return f"`{'.'.join(self.table_parts(table, dataset))}`"

    def replace_view(self, table, dataset, sql):
        self.views.append((table, dataset, sql))


class DuckLike(RecordingWarehouse):
    dialect: ClassVar[str | None] = "duckdb"


class MisspokenWarehouse(RecordingWarehouse):
    """A destination carrying the BigQuery key but another dialect, to isolate the view's dialect check."""

    key: ClassVar[str] = "bigquery_destination"
    dialect: ClassVar[str | None] = "duckdb"


@pytest.fixture(autouse=True)
def _reset() -> None:
    RecordingWarehouse.views.clear()


def test_the_events_reference_lives_in_the_property_dataset():
    source = GoogleAnalytics4Export(property_id="123")
    assert source.events.table == "events_*"
    assert source.events.dataset == "analytics_123"
    assert source.fact_daily_events.dataset == "google_analytics4_export"
    assert isinstance(source.events, il.Reference)


def test_the_view_is_pinned_to_bigquery_and_reads_the_export():
    warehouse = RecordingWarehouse(id="bq")
    source = GoogleAnalytics4Export(property_id="123", destinations=[warehouse])
    il.DAG(source).materialize()
    [(table, dataset, sql)] = RecordingWarehouse.views
    assert (table, dataset) == ("fact_daily_events__123", "google_analytics4_export")
    assert "`proj.analytics_123.events_*`" in sql
    assert "_TABLE_SUFFIX" in sql


def test_the_source_binds_bigquery_destinations_only():
    assert GoogleAnalytics4Export.relations["destinations"].keys == ["bigquery_destination"]
    with pytest.raises(ConfigError, match="does not accept"):
        GoogleAnalytics4Export(property_id="123", destinations=[DuckLike(id="duck")])


def test_the_view_refuses_another_dialect():
    with pytest.raises(ConfigError, match="dialect 'bigquery'"):
        il.DAG(GoogleAnalytics4Export(property_id="123", destinations=[MisspokenWarehouse(id="duck")]))
