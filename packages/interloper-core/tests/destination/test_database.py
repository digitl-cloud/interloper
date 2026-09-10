"""Tests for ``interloper.destination.database``."""

import datetime
from typing import Any, ClassVar

import pytest

import interloper as il
from interloper.destination import IOContext
from interloper.destination.database import DatabaseDestination, PartitionFilter
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.representation import Representation


class RecordingDatabase(DatabaseDestination):
    """Database destination capturing every hook call."""

    calls: ClassVar[list[tuple[str, Any]]] = []

    def model_post_init(self, context: Any) -> None:
        super().model_post_init(context)
        object.__setattr__(self, "calls", [])

    def insert(self, table, dataset, data, context):
        self.calls.append(("insert", (table, dataset, Representation.of(data).to_records(data))))

    def delete(self, table, dataset, where):
        self.calls.append(("delete", (table, dataset, where)))

    def select(self, table, dataset, where):
        self.calls.append(("select", (table, dataset, where)))
        return []

    def count(self, table, dataset, column):
        return {}


@il.asset(partitioning=il.TimePartitionConfig(column="date"))
def partitioned_asset(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset
def plain_asset() -> list:  # noqa: D103
    return []


class DiscriminatedSource(il.Source):
    """Source whose assets materialize under instance-suffixed table names."""

    account_id: str = il.InputField(default="", discriminator=True)

    class DiscriminatedRows(il.Asset):
        """Asset carrying the instance discriminator in its table name."""


def make_io_context(asset: il.Asset, partition_or_window=None, schema=None) -> IOContext:  # noqa: D103
    return IOContext(asset=asset, partition_or_window=partition_or_window, schema=schema)


class TestWrite:
    """Partition-aware write dispatch."""

    def test_replace_without_partition_deletes_all_then_inserts(self):
        destination = RecordingDatabase(id="db")
        rows = [{"a": 1}]
        destination.write(make_io_context(plain_asset()), rows)
        assert destination.calls == [("delete", ("plain_asset", None, None)), ("insert", ("plain_asset", None, rows))]

    def test_single_time_partition_deletes_its_bounds(self):
        # A time partition's rows may carry any value inside the period, so
        # replacement deletes by half-open bounds rather than id equality.
        destination = RecordingDatabase(id="db")
        partition = TimePartition(datetime.date(2024, 1, 1))
        destination.write(make_io_context(partitioned_asset(), partition), [{"date": "2024-01-01"}])
        where = PartitionFilter("date", bounds=(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)))
        assert destination.calls[0] == ("delete", ("partitioned_asset", None, where))
        assert destination.calls[1][0] == "insert"

    def test_non_time_partition_deletes_by_id(self):
        from interloper.partitioning.base import Partition, PartitionConfig

        @il.asset(partitioning=PartitionConfig(column="region"))
        def regional(context: il.ExecutionContext) -> list:
            return []

        destination = RecordingDatabase(id="db")
        destination.write(make_io_context(regional(), Partition("eu")), [{"region": "eu"}])
        assert destination.calls[0] == ("delete", ("regional", None, PartitionFilter("region", value="eu")))

    def test_window_deletes_each_partition_inserts_once(self):
        destination = RecordingDatabase(id="db")
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 3))
        rows = [{"date": "2024-01-01"}, {"date": "2024-01-02"}, {"date": "2024-01-03"}]
        destination.write(make_io_context(partitioned_asset(), window), rows)
        deletes = [c for c in destination.calls if c[0] == "delete"]
        inserts = [c for c in destination.calls if c[0] == "insert"]
        assert len(deletes) == 3
        assert len(inserts) == 1

    def test_monthly_partition_bounds_span_the_month(self):
        @il.asset(partitioning=il.TimePartitionConfig(column="date", granularity=il.TimeGranularity.MONTH))
        def monthly(context: il.ExecutionContext) -> list:
            return []

        destination = RecordingDatabase(id="db")
        partition = TimePartition(datetime.date(2024, 2, 10), il.TimeGranularity.MONTH)
        destination.write(make_io_context(monthly(), partition), [{"date": "2024-02-10"}])
        where = PartitionFilter("date", bounds=(datetime.date(2024, 2, 1), datetime.date(2024, 3, 1)))
        assert destination.calls[0] == ("delete", ("monthly", None, where))

    def test_time_partition_reads_by_bounds(self):
        destination = RecordingDatabase(id="db")
        partition = TimePartition(datetime.date(2024, 1, 1))
        destination.read(make_io_context(partitioned_asset(), partition))
        where = PartitionFilter("date", bounds=(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)))
        assert destination.calls[0] == ("select", ("partitioned_asset", None, where))

    def test_empty_data_is_a_noop(self):
        destination = RecordingDatabase(id="db")
        destination.write(make_io_context(plain_asset()), [])
        assert destination.calls == []

    def test_write_targets_instance_aliased_table(self):
        source = DiscriminatedSource(account_id="42")
        (asset,) = source.assets
        destination = RecordingDatabase(id="db")
        rows = [{"a": 1}]
        destination.write(make_io_context(asset), rows)
        assert destination.calls == [
            ("delete", ("discriminated_rows__42", "discriminated_source", None)),
            ("insert", ("discriminated_rows__42", "discriminated_source", rows)),
        ]

    def test_dataframe_converts_via_null_safe_fallback(self):
        pd = pytest.importorskip("pandas")
        import numpy as np

        destination = RecordingDatabase(id="db")
        df = pd.DataFrame([{"a": 1, "b": np.nan}])
        destination.write(make_io_context(plain_asset()), df)
        inserted = next(c for c in destination.calls if c[0] == "insert")[1][2]
        assert inserted == [{"a": 1, "b": None}]

    def test_missing_partition_column_warns(self):
        destination = RecordingDatabase(id="db")
        partition = TimePartition(datetime.date(2024, 1, 1))
        with pytest.warns(UserWarning, match="Partition column 'date' not found"):
            destination.write(make_io_context(partitioned_asset(), partition), [{"other": 1}])


class TestInsertDataHook:
    """The insert hook receives the data natively, with the effective schema on the context."""

    def test_override_receives_native_data(self):
        captured: dict[str, Any] = {}

        class NativeDatabase(RecordingDatabase):
            def insert(self, table, dataset, data, context):
                captured["data"] = data
                captured["schema"] = context.schema

        class MySchema(il.Schema):
            a: int | None

        destination = NativeDatabase(id="db")
        sentinel = object()
        destination.write(make_io_context(plain_asset(), schema=MySchema), [sentinel])
        assert captured["data"] == [sentinel]
        assert captured["schema"] is MySchema


class TestPartitionFilter:
    """The base resolves a partition into the filter a backend renders."""

    def test_the_whole_table_is_no_filter(self):
        assert RecordingDatabase(id="db")._filter(make_io_context(plain_asset()), None) is None

    def test_a_time_partition_filters_by_half_open_bounds(self):
        partition = TimePartition(datetime.date(2024, 1, 1))
        where = RecordingDatabase(id="db")._filter(make_io_context(partitioned_asset(), partition), partition)
        assert where == PartitionFilter("date", bounds=(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)))

    def test_any_other_partition_filters_by_its_id(self):
        from interloper.partitioning.base import Partition, PartitionConfig

        @il.asset(partitioning=PartitionConfig(column="region"))
        def regional(context: il.ExecutionContext) -> list:
            return []

        where = RecordingDatabase(id="db")._filter(make_io_context(regional(), Partition("eu")), Partition("eu"))
        assert where == PartitionFilter("region", value="eu")


class TestNativeReads:
    """A read hands back whatever the backend returns; the base converts nothing."""

    def test_read_returns_the_select_result_as_is(self):
        class FrameDatabase(RecordingDatabase):
            def select(self, table, dataset, where):
                return {"native": (table, where)}

        result = FrameDatabase(id="db").read(make_io_context(plain_asset()))
        assert result == {"native": ("plain_asset", None)}


class DateSchema(il.Schema):
    """Schema with a date field, mirroring API rows that carry ISO strings."""

    name: str | None = None
    day: datetime.date | None = None


class TestMaterializationStrategy:
    """Write-time schema enforcement declared as a backend trait."""

    def test_auto_trusts_conformed_data(self):
        destination = RecordingDatabase(id="db")
        rows = [{"name": "a", "day": "2026-07-13"}]
        destination.write(make_io_context(plain_asset(), schema=DateSchema), rows)
        assert destination.calls[-1][1][2] == rows

    def test_reconcile_coerces_rows_to_the_effective_schema(self):
        class ReconcilingDatabase(RecordingDatabase):
            materialization_strategy: il.MaterializationStrategy = il.MaterializationStrategy.RECONCILE

        destination = ReconcilingDatabase(id="db")
        destination.write(make_io_context(plain_asset(), schema=DateSchema), [{"name": "a", "day": "2026-07-13"}])
        inserted = destination.calls[-1][1][2]
        assert inserted == [{"name": "a", "day": datetime.date(2026, 7, 13)}]

    def test_reconcile_coerces_dataframes(self):
        pd = pytest.importorskip("pandas")

        class ReconcilingDatabase(RecordingDatabase):
            materialization_strategy: il.MaterializationStrategy = il.MaterializationStrategy.RECONCILE

        destination = ReconcilingDatabase(id="db")
        destination.write(
            make_io_context(plain_asset(), schema=DateSchema), pd.DataFrame([{"name": "a", "day": "2026-07-13"}])
        )
        inserted = destination.calls[-1][1][2]
        assert inserted[0]["day"] == datetime.date(2026, 7, 13)

    def test_reconcile_without_schema_is_a_noop(self):
        class ReconcilingDatabase(RecordingDatabase):
            materialization_strategy: il.MaterializationStrategy = il.MaterializationStrategy.RECONCILE

        destination = ReconcilingDatabase(id="db")
        rows = [{"name": "a", "day": "2026-07-13"}]
        destination.write(make_io_context(plain_asset()), rows)
        assert destination.calls[-1][1][2] == rows

    def test_decorator_sets_the_field_default(self):
        from interloper.destination import destination

        @destination(materialization_strategy=il.MaterializationStrategy.RECONCILE)
        class DecoratedDatabase(RecordingDatabase):
            pass

        assert DecoratedDatabase(id="db").materialization_strategy is il.MaterializationStrategy.RECONCILE
        assert RecordingDatabase(id="db").materialization_strategy is il.MaterializationStrategy.AUTO

    def test_decorator_default_override_keeps_field_metadata(self):
        # Regression: the decorator's default override used to rebuild the
        # FieldInfo from scratch, dropping title/description/x-info — the UI
        # then fell back to the enum's class name and docstring.
        from interloper.destination import destination

        @destination(materialization_strategy=il.MaterializationStrategy.RECONCILE)
        class DecoratedDatabase(RecordingDatabase):
            pass

        prop = DecoratedDatabase.config_schema()["properties"]["materialization_strategy"]
        assert prop["default"] == "reconcile"
        assert prop["title"] == "Materialization Strategy"
        assert prop["description"] == "How strictly written data must match the effective schema."
        assert "'Reconcile' aligns columns" in prop["x-info"]

    def test_instance_override_beats_the_class_default(self):
        destination = RecordingDatabase(id="db", materialization_strategy=il.MaterializationStrategy.RECONCILE)
        destination.write(make_io_context(plain_asset(), schema=DateSchema), [{"name": "a", "day": "2026-07-13"}])
        assert destination.calls[-1][1][2] == [{"name": "a", "day": datetime.date(2026, 7, 13)}]

    def test_strategy_renders_in_the_config_schema(self):
        schema = RecordingDatabase.config_schema()
        prop = schema["properties"]["materialization_strategy"]
        ref = prop.get("$ref") or prop.get("allOf", [{}])[0].get("$ref", "")
        enum_def = schema["$defs"][ref.split("/")[-1]]
        assert set(enum_def["enum"]) == {"auto", "strict", "reconcile"}
        # Short inline description; the long per-value text lives in the tooltip.
        assert prop["title"] == "Materialization Strategy"
        assert prop["description"] == "How strictly written data must match the effective schema."
        assert "'Reconcile' aligns columns" in prop["x-info"]
