"""Tests for ``interloper.destination.database``."""

import datetime
from typing import Any, ClassVar

import pytest

import interloper as il
from interloper.destination import IOContext
from interloper.destination.database import DatabaseDestination, WriteDisposition
from interloper.errors import NormalizerError
from interloper.partitioning.time import TimePartition, TimePartitionWindow


class RecordingDatabase(DatabaseDestination):
    """Database destination capturing every hook call."""

    calls: ClassVar[list[tuple[str, Any]]] = []

    def model_post_init(self, context: Any) -> None:
        super().model_post_init(context)
        object.__setattr__(self, "calls", [])

    def _insert(self, table, schema, rows):
        self.calls.append(("insert", (table, schema, rows)))

    def _delete_all(self, table, schema):
        self.calls.append(("delete_all", (table, schema)))

    def _delete_partition(self, table, schema, column, value):
        self.calls.append(("delete_partition", (table, schema, column, value)))

    def _delete_partition_range(self, table, schema, column, start, end):
        self.calls.append(("delete_partition_range", (table, schema, column, start, end)))

    def _select_all(self, table, schema):
        return []

    def _select_partition(self, table, schema, column, value):
        self.calls.append(("select_partition", (table, schema, column, value)))
        return []

    def _select_partition_range(self, table, schema, column, start, end):
        self.calls.append(("select_partition_range", (table, schema, column, start, end)))
        return []

    def _count_by_partition(self, table, schema, column):
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
        assert [c[0] for c in destination.calls] == ["delete_all", "insert"]
        assert destination.calls[1][1][2] == rows

    def test_append_skips_deletes(self):
        class AppendDatabase(RecordingDatabase):
            write_disposition = WriteDisposition.APPEND

        destination = AppendDatabase(id="db")
        destination.write(make_io_context(plain_asset()), [{"a": 1}])
        assert [c[0] for c in destination.calls] == ["insert"]

    def test_single_time_partition_deletes_its_bounds(self):
        # A time partition's rows may carry any value inside the period, so
        # replacement deletes by half-open bounds rather than id equality.
        destination = RecordingDatabase(id="db")
        partition = TimePartition(datetime.date(2024, 1, 1))
        destination.write(make_io_context(partitioned_asset(), partition), [{"date": "2024-01-01"}])
        assert destination.calls[0] == (
            "delete_partition_range",
            ("partitioned_asset", None, "date", datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)),
        )
        assert destination.calls[1][0] == "insert"

    def test_non_time_partition_deletes_by_id(self):
        from interloper.partitioning.base import Partition, PartitionConfig

        @il.asset(partitioning=PartitionConfig(column="region"))
        def regional(context: il.ExecutionContext) -> list:
            return []

        destination = RecordingDatabase(id="db")
        destination.write(make_io_context(regional(), Partition("eu")), [{"region": "eu"}])
        assert destination.calls[0] == ("delete_partition", ("regional", None, "region", "eu"))

    def test_window_deletes_each_partition_inserts_once(self):
        destination = RecordingDatabase(id="db")
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 3))
        rows = [{"date": "2024-01-01"}, {"date": "2024-01-02"}, {"date": "2024-01-03"}]
        destination.write(make_io_context(partitioned_asset(), window), rows)
        deletes = [c for c in destination.calls if c[0] == "delete_partition_range"]
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
        assert destination.calls[0] == (
            "delete_partition_range",
            ("monthly", None, "date", datetime.date(2024, 2, 1), datetime.date(2024, 3, 1)),
        )

    def test_time_partition_reads_by_bounds(self):
        destination = RecordingDatabase(id="db")
        partition = TimePartition(datetime.date(2024, 1, 1))
        destination.read(make_io_context(partitioned_asset(), partition))
        assert destination.calls[0] == (
            "select_partition_range",
            ("partitioned_asset", None, "date", datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)),
        )

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
            ("delete_all", ("discriminated_rows__42", "discriminated_source")),
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
    """Native-format insert hook."""

    def test_default_converts_to_rows(self):
        destination = RecordingDatabase(id="db")
        destination._insert_data("t", None, [{"a": 1}], make_io_context(plain_asset()))
        assert destination.calls == [("insert", ("t", None, [{"a": 1}]))]

    def test_override_receives_native_data(self):
        captured: dict[str, Any] = {}

        class NativeDatabase(RecordingDatabase):
            def _insert_data(self, table, schema, data, context):
                captured["data"] = data
                captured["schema"] = context.schema

        class MySchema(il.Schema):
            a: int | None

        destination = NativeDatabase(id="db")
        sentinel = object()
        destination.write(make_io_context(plain_asset(), schema=MySchema), [sentinel])
        assert captured["data"] == [sentinel]
        assert captured["schema"] is MySchema


class TestClassLevelTraits:
    """write_disposition / read_representation are backend traits, not config."""

    def test_traits_are_not_config_schema_fields(self):
        # Regression: as pydantic fields they leaked into the UI config form.
        properties = RecordingDatabase.definition().config_schema.get("properties", {})
        assert "read_representation" not in properties
        assert "write_disposition" not in properties

    def test_traits_are_not_model_fields(self):
        assert "read_representation" not in RecordingDatabase.model_fields
        assert "write_disposition" not in RecordingDatabase.model_fields

    def test_read_representation_via_decorator(self):
        from interloper.destination import destination

        @destination(name="Traited", read_representation="dataframe")
        class TraitedDB(RecordingDatabase):
            pass

        assert TraitedDB.read_representation == "dataframe"
        assert "read_representation" not in TraitedDB.definition().config_schema.get("properties", {})


class TestRecordsConversion:
    """Data converts to records through its representation."""

    def test_insert_data_converts_dataframe(self):
        pd = pytest.importorskip("pandas")

        destination = RecordingDatabase(id="db")
        destination._insert_data("t", None, pd.DataFrame([{"a": 1}]), make_io_context(plain_asset()))
        assert destination.calls == [("insert", ("t", None, [{"a": 1}]))]

    def test_insert_data_passes_rows_through(self):
        destination = RecordingDatabase(id="db")
        destination._insert_data("t", None, [{"a": 1}], make_io_context(plain_asset()))
        assert destination.calls == [("insert", ("t", None, [{"a": 1}]))]

    def test_unsupported_type_raises(self):
        destination = RecordingDatabase(id="db")
        with pytest.raises(NormalizerError, match="does not support type"):
            destination._insert_data("t", None, 42, make_io_context(plain_asset()))

    def test_from_rows_uses_read_representation(self):
        pd = pytest.importorskip("pandas")

        class DataFrameReadDatabase(RecordingDatabase):
            read_representation = "dataframe"

        out = DataFrameReadDatabase(id="db")._from_rows([{"a": 1}])
        assert isinstance(out, pd.DataFrame)
        assert RecordingDatabase(id="db")._from_rows([{"a": 1}]) == [{"a": 1}]


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
