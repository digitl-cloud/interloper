"""Tests for references: assets interloper reads but never writes."""

from __future__ import annotations

import datetime as dt

import pytest

import interloper as il
from interloper.errors import ConfigError


class Events(il.Schema):
    event_date: str
    event_name: str


@il.source
class Export(il.Source):
    property_id: str = il.InputField(default="", discriminator=True)

    events = il.reference("events_*", key="events", schema=Events, tags=["Event"])


class TestReference:
    def test_a_reference_is_an_asset_that_reads_no_upstream_data(self):
        assert issubclass(il.Reference, il.Asset)
        assert il.Reference.reads_upstream_data is False
        assert il.Reference.kind == "asset"

    def test_the_table_is_the_given_name_formatted_with_the_source_fields(self):
        source = Export(property_id="123")
        assert source.events.table == "events_*"

        @il.source
        class Sharded(il.Source):
            property_id: str = il.InputField(default="", discriminator=True)
            events = il.reference("events_{property_id}", key="events")  # noqa: RUF027

        assert Sharded(property_id="123").events.table == "events_123"

    def test_the_dataset_is_the_sources(self):
        source = Export(property_id="123", dataset="analytics_123")
        assert source.events.dataset == "analytics_123"

    def test_the_dataset_is_formatted_with_the_source_fields(self):
        @il.source
        class Sharded(il.Source):
            property_id: str = il.InputField(default="", discriminator=True)
            events = il.reference("events_*", key="events", dataset="analytics_{property_id}")  # noqa: RUF027

        assert Sharded(property_id="123").events.dataset == "analytics_123"

    def test_a_formatted_name_that_is_not_an_identifier_is_refused(self):
        @il.source
        class Sharded(il.Source):
            property_id: str = il.InputField(default="", discriminator=True)
            events = il.reference("events_{property_id}", key="events")  # noqa: RUF027

        with pytest.raises(ValueError, match="invalid"):
            Sharded(property_id="12-3")

    def test_a_placeholder_without_a_source_is_a_config_error(self):
        events = il.reference("events_{property_id}", key="events")
        with pytest.raises(ConfigError, match=r"events.*property_id"):
            events().table

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
