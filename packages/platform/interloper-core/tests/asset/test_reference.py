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

    @il.reference(table="events_*", schema=Events, tags=["Event"])
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
            @il.reference(table="events-2026")
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
            @il.reference(table="events_*", dataset="exports")
            def events(self) -> None: ...

        assert Exported().events.dataset == "exports"

    def test_the_table_and_the_dataset_name_the_sources_fields(self):
        @il.source
        class Sharded(il.Source):
            property_id: str = il.InputField(default="", discriminator=True)

            @il.reference(table="events_{property_id}", dataset="analytics_{property_id}")  # noqa: RUF027
            def events(self) -> None: ...

        source = Sharded(property_id="123")
        assert source.events.table == "events_123"
        assert source.events.dataset == "analytics_123"

    def test_a_placeholder_that_is_not_a_field_is_a_config_error(self):
        @il.source
        class Sharded(il.Source):
            @il.reference(table="events_*", dataset="analytics_{property_id}")
            def events(self) -> None: ...

        with pytest.raises(ValueError, match=r"Source 'sharded'.*property_id"):
            Sharded()

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

    def test_the_decorator_needs_no_table(self):
        @il.source
        class Plain(il.Source):
            property_id: str = il.InputField(default="", discriminator=True)

            @il.reference(schema=Events)
            def events(self) -> None: ...

        assert Plain(property_id="123").events.table == "events__123"


class Bucket(il.Schema):
    bucket_id: str
    name: str


@il.source
class Matcher(il.Source):
    @il.reference(editable=True, schema=Bucket)
    def buckets(self) -> None:
        """The buckets people sort campaigns into."""

    @il.asset
    def matches(self, buckets: il.Upstream | None = None) -> list:
        return buckets.data if buckets is not None else []


class TestEditable:
    def test_editable_is_a_class_attribute(self):
        assert issubclass(Matcher.buckets, il.Reference)
        assert Matcher.buckets.editable is True
        assert Export.events.editable is False

    def test_the_definition_says_which_assets_are_editable(self):
        editable = {asset.key: asset.editable for asset in Matcher.definition().assets}
        assert editable == {"buckets": True, "matches": False}

    def test_an_editable_reference_without_a_schema_is_refused(self):
        with pytest.raises(TypeError, match="Editable reference 'notes' declares no schema"):

            @il.reference(editable=True)
            def notes() -> None: ...

    def test_a_partitioned_editable_reference_is_refused(self):
        with pytest.raises(TypeError, match="Editable reference 'notes' declares a partitioning"):

            @il.reference(editable=True, schema=Bucket, partitioning=il.TimePartitionConfig(column="date"))
            def notes() -> None: ...

    def test_a_subclass_is_checked_too(self):
        with pytest.raises(TypeError, match="declares no schema"):

            class Notes(il.Reference):
                editable = True

    def test_a_run_writes_nothing_and_downstream_reads_the_rows(self):
        memory = il.MemoryDestination()
        source = Matcher(destinations=[memory])
        memory.write_partition(il.IOContext(asset=source.buckets), None, [{"bucket_id": "b1", "name": "Sale"}])
        il.DAG(source).materialize()
        assert memory.read(il.IOContext(asset=source.matches)) == [{"bucket_id": "b1", "name": "Sale"}]

    def test_a_table_never_written_reads_as_no_data(self):
        memory = il.MemoryDestination()
        source = Matcher(destinations=[memory])
        il.DAG(source).materialize()
        assert memory._storage == {}


@pytest.fixture(autouse=True)
def _clear_memory() -> None:
    il.MemoryDestination.clear()
