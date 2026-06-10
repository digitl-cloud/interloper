"""Tests for ``interloper.destination.database``."""

import datetime
from typing import Any, ClassVar

import pytest

import interloper as il
from interloper.destination import IOContext
from interloper.destination.adapter import DataAdapter
from interloper.destination.database import DatabaseDestination, WriteDisposition
from interloper.errors import AdapterError
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
        dest = RecordingDB(id="db", write_disposition=WriteDisposition.APPEND)
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


class TestToRows:
    """Adapter dispatch in _to_rows."""

    def test_adapter_dispatch_uses_can_handle(self):
        class TupleAdapter(DataAdapter[tuple]):
            def can_handle(self, data: Any) -> bool:
                return isinstance(data, tuple)

            def to_rows(self, data: tuple) -> list[dict[str, Any]]:
                return [dict(item) for item in data]

            def from_rows(self, rows: list[dict[str, Any]]) -> tuple:
                return tuple(rows)

        class TupleDB(RecordingDB):
            @property
            def adapters(self) -> list[DataAdapter]:
                return [TupleAdapter()]

        dest = TupleDB(id="db")
        assert dest._to_rows(({"a": 1},)) == [{"a": 1}]
        assert dest._to_rows([{"b": 2}]) == [{"b": 2}]  # list passthrough still works

    def test_unsupported_type_raises(self):
        dest = RecordingDB(id="db")
        with pytest.raises(AdapterError, match="could not handle|could handle|No adapter"):
            dest._to_rows(42)
