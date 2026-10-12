"""Tests for references: assets interloper reads but never writes."""

from __future__ import annotations

import datetime as dt

import pytest

import interloper as il


class Events(il.Schema):
    event_date: str
    event_name: str


@il.source
class Export(il.Source):
    property_id: str = il.InputField(default="", discriminator=True)

    @il.reference("events_*", schema=Events, tags=["Event"])
    def events(self) -> None: ...


class TestReference:
    def test_a_reference_is_an_asset_that_reads_no_upstream_data(self):
        assert issubclass(il.Reference, il.Asset)
        assert il.Reference.reads_upstream_data is False
        assert il.Reference.kind == "asset"

    def test_the_table_is_the_given_name_as_written(self):
        assert Export(property_id="123").events.table == "events_*"

        @il.source
        class Dated(il.Source):
            @il.reference("events-2026")
            def events(self) -> None: ...

        assert Dated().events.table == "events-2026"

    def test_without_a_table_name_a_reference_is_named_like_any_asset(self):
        @il.source
        class Plain(il.Source):
            property_id: str = il.InputField(default="", discriminator=True)

            class Events(il.Reference):
                pass

        assert Plain(property_id="123").events.table == "events__123"

    def test_the_dataset_is_the_sources(self):
        source = Export(property_id="123", dataset="analytics_123")
        assert source.events.dataset == "analytics_123"

    def test_the_dataset_can_be_the_references_own(self):
        @il.source
        class Exported(il.Source):
            @il.reference("events_*", dataset="exports")
            def events(self) -> None: ...

        assert Exported().events.dataset == "exports"

    def test_the_decorator_routes_overrides(self):
        events = Export.asset_types[0]
        assert events.schema is Events
        assert events.tags == ["Event"]
        assert events.key == "events"

    def test_materializing_does_nothing(self):
        memory = il.MemoryDestination()
        source = Export(property_id="123", destinations=[memory])
        il.DAG(source).materialize(il.TimePartition(dt.date(2026, 1, 1)))
        assert memory._storage == {}

    def test_a_reference_is_read_like_any_asset(self):
        memory = il.MemoryDestination()
        source = Export(property_id="123", destinations=[memory])
        context = il.IOContext(asset=source.events)
        memory.write(context, [{"event_date": "20260101", "event_name": "page_view"}])
        assert memory.read(context) == [{"event_date": "20260101", "event_name": "page_view"}]


@pytest.fixture(autouse=True)
def _clear_memory() -> None:
    il.MemoryDestination.clear()
