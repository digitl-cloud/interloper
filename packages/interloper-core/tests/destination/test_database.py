"""Tests for ``interloper.destination.database``."""

import datetime
from typing import Any, ClassVar

import pytest

import interloper as il
from interloper.destination import IOContext
from interloper.destination.database import DatabaseDestination, WriteDisposition
from interloper.errors import NormalizerError
from interloper.partitioning.time import TimePartition, TimePartitionWindow


class RecordingDB(DatabaseDestination):
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

    def _select_all(self, table, schema):
        return []

    def _select_partition(self, table, schema, column, value):
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


def make_ctx(asset: il.Asset, partition_or_window=None, schema=None) -> IOContext:  # noqa: D103
    return IOContext(asset=asset, partition_or_window=partition_or_window, schema=schema)


class TestWrite:
    """Partition-aware write dispatch."""

    def test_replace_without_partition_deletes_all_then_inserts(self):
        dest = RecordingDB(id="db")
        rows = [{"a": 1}]
        dest.write(make_ctx(plain_asset()), rows)
        assert [c[0] for c in dest.calls] == ["delete_all", "insert"]
        assert dest.calls[1][1][2] == rows

    def test_append_skips_deletes(self):
        class AppendDB(RecordingDB):
            write_disposition = WriteDisposition.APPEND

        dest = AppendDB(id="db")
        dest.write(make_ctx(plain_asset()), [{"a": 1}])
        assert [c[0] for c in dest.calls] == ["insert"]

    def test_single_partition_deletes_partition(self):
        dest = RecordingDB(id="db")
        partition = TimePartition(datetime.date(2024, 1, 1))
        dest.write(make_ctx(partitioned_asset(), partition), [{"date": "2024-01-01"}])
        assert dest.calls[0] == ("delete_partition", ("partitioned_asset", None, "date", "2024-01-01"))
        assert dest.calls[1][0] == "insert"

    def test_window_deletes_each_partition_inserts_once(self):
        dest = RecordingDB(id="db")
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 3))
        rows = [{"date": "2024-01-01"}, {"date": "2024-01-02"}, {"date": "2024-01-03"}]
        dest.write(make_ctx(partitioned_asset(), window), rows)
        deletes = [c for c in dest.calls if c[0] == "delete_partition"]
        inserts = [c for c in dest.calls if c[0] == "insert"]
        assert len(deletes) == 3
        assert len(inserts) == 1

    def test_empty_data_is_a_noop(self):
        dest = RecordingDB(id="db")
        dest.write(make_ctx(plain_asset()), [])
        assert dest.calls == []

    def test_write_targets_instance_aliased_table(self):
        source = DiscriminatedSource(account_id="42")
        (asset,) = source.assets
        dest = RecordingDB(id="db")
        rows = [{"a": 1}]
        dest.write(make_ctx(asset), rows)
        assert dest.calls == [
            ("delete_all", ("discriminated_rows__42", "discriminated_source")),
            ("insert", ("discriminated_rows__42", "discriminated_source", rows)),
        ]

    def test_dataframe_converts_via_null_safe_fallback(self):
        pd = pytest.importorskip("pandas")
        import numpy as np

        dest = RecordingDB(id="db")
        df = pd.DataFrame([{"a": 1, "b": np.nan}])
        dest.write(make_ctx(plain_asset()), df)
        inserted = next(c for c in dest.calls if c[0] == "insert")[1][2]
        assert inserted == [{"a": 1, "b": None}]

    def test_missing_partition_column_warns(self):
        dest = RecordingDB(id="db")
        partition = TimePartition(datetime.date(2024, 1, 1))
        with pytest.warns(UserWarning, match="Partition column 'date' not found"):
            dest.write(make_ctx(partitioned_asset(), partition), [{"other": 1}])


class TestInsertDataHook:
    """Native-format insert hook."""

    def test_default_converts_to_rows(self):
        dest = RecordingDB(id="db")
        dest._insert_data("t", None, [{"a": 1}], make_ctx(plain_asset()))
        assert dest.calls == [("insert", ("t", None, [{"a": 1}]))]

    def test_override_receives_native_data(self):
        captured: dict[str, Any] = {}

        class NativeDB(RecordingDB):
            def _insert_data(self, table, schema, data, context):
                captured["data"] = data
                captured["schema"] = context.schema

        class MySchema(il.Schema):
            a: int | None

        dest = NativeDB(id="db")
        sentinel = object()
        dest.write(make_ctx(plain_asset(), schema=MySchema), [sentinel])
        assert captured["data"] == [sentinel]
        assert captured["schema"] is MySchema


class TestClassLevelTraits:
    """write_disposition / read_representation are backend traits, not config."""

    def test_traits_are_not_config_schema_fields(self):
        # Regression: as pydantic fields they leaked into the UI config form.
        properties = RecordingDB.definition().config_schema.get("properties", {})
        assert "read_representation" not in properties
        assert "write_disposition" not in properties

    def test_traits_are_not_model_fields(self):
        assert "read_representation" not in RecordingDB.model_fields
        assert "write_disposition" not in RecordingDB.model_fields

    def test_read_representation_via_decorator(self):
        from interloper.destination import destination

        @destination(name="Traited", read_representation="dataframe")
        class TraitedDB(RecordingDB):
            pass

        assert TraitedDB.read_representation == "dataframe"
        assert "read_representation" not in TraitedDB.definition().config_schema.get("properties", {})


class TestRecordsConversion:
    """Data converts to records through its representation."""

    def test_insert_data_converts_dataframe(self):
        pd = pytest.importorskip("pandas")

        dest = RecordingDB(id="db")
        dest._insert_data("t", None, pd.DataFrame([{"a": 1}]), make_ctx(plain_asset()))
        assert dest.calls == [("insert", ("t", None, [{"a": 1}]))]

    def test_insert_data_passes_rows_through(self):
        dest = RecordingDB(id="db")
        dest._insert_data("t", None, [{"a": 1}], make_ctx(plain_asset()))
        assert dest.calls == [("insert", ("t", None, [{"a": 1}]))]

    def test_unsupported_type_raises(self):
        dest = RecordingDB(id="db")
        with pytest.raises(NormalizerError, match="does not support type"):
            dest._insert_data("t", None, 42, make_ctx(plain_asset()))

    def test_from_rows_uses_read_representation(self):
        pd = pytest.importorskip("pandas")

        class DataFrameReadDB(RecordingDB):
            read_representation = "dataframe"

        out = DataFrameReadDB(id="db")._from_rows([{"a": 1}])
        assert isinstance(out, pd.DataFrame)
        assert RecordingDB(id="db")._from_rows([{"a": 1}]) == [{"a": 1}]
