"""Tests for ``interloper.destination.object_store``."""

import datetime
from collections.abc import Iterator
from typing import Any, ClassVar

import pytest
from pydantic import Field

import interloper as il
from interloper.destination import IOContext, ObjectStoreDestination, StoredObject
from interloper.destination.formats import CSVFormat, JSONLFormat, ParquetFormat
from interloper.errors import DataNotFoundError
from interloper.schema import Schema


class FakeBucket(ObjectStoreDestination):
    """Object-store destination over an in-memory dict of name to (payload, content type, metadata)."""

    objects: ClassVar[dict[str, tuple[bytes, str, dict[str, str]]]] = {}

    def model_post_init(self, context: Any) -> None:
        super().model_post_init(context)
        object.__setattr__(self, "objects", {})  # noqa: PLC2801 - bypasses pydantic's __setattr__

    def put_object(self, name, payload, content_type, metadata):
        self.objects[name] = (payload, content_type, metadata)

    def get_object(self, name):
        stored = self.objects.get(name)
        return stored[0] if stored else None

    def list_objects(self, prefix) -> Iterator[StoredObject]:
        for name, (_, _, metadata) in self.objects.items():
            if name.startswith(prefix):
                yield StoredObject(name=name, metadata=metadata)

    def object_uri(self, name):
        return f"fake://bucket/{name}"


class _RowSchema(Schema):
    id: int | None = Field(..., description="Row id")
    cost: float | None = Field(...)
    day: datetime.date | None = Field(...)


@il.asset
def plain_asset() -> list:  # noqa: D103
    return []


@il.asset(partitioning=il.TimePartitionConfig(column="day"))
def partitioned_asset() -> list:  # noqa: D103
    return []


def _ctx(asset: Any, schema: type[Schema] | None = None, scope: Any = None) -> IOContext:
    return IOContext(asset=asset, partition_or_window=scope, schema=schema)


def _day(day: int) -> il.TimePartition:
    return il.TimePartition(datetime.date(2024, 1, day))


class TestContract:
    def test_hooks_are_abstract(self):
        class Incomplete(ObjectStoreDestination):
            def put_object(self, name, payload, content_type, metadata):
                pass

        with pytest.raises(TypeError, match="abstract"):
            Incomplete()

    def test_defaults(self):
        dest = FakeBucket()
        assert dest.format == "parquet"
        assert dest.prefix is None


class TestLayout:
    """Hive-partitioned object naming."""

    def test_unpartitioned(self):
        assert FakeBucket()._object_name(_ctx(plain_asset()), None) == "plain_asset/data.parquet"

    def test_partition(self):
        name = FakeBucket(format="jsonl")._object_name(_ctx(partitioned_asset()), _day(2))
        assert name == "partitioned_asset/day=2024-01-02/data.jsonl"

    def test_prefix_and_dataset(self):
        asset = plain_asset()
        object.__setattr__(asset, "dataset", "ds")  # noqa: PLC2801 - bypasses pydantic's __setattr__
        assert FakeBucket(prefix="/lake/raw/")._object_name(_ctx(asset), None) == "lake/raw/ds/plain_asset/data.parquet"

    @pytest.mark.parametrize(("fmt", "ext"), [("parquet", "parquet"), ("jsonl", "jsonl"), ("csv", "csv")])
    def test_extension_follows_format(self, fmt, ext):
        assert FakeBucket(format=fmt)._object_name(_ctx(plain_asset()), None) == f"plain_asset/data.{ext}"


class TestWrite:
    """Writes drop the partition column, stamp the row count and split windows."""

    def test_unpartitioned_write(self):
        dest = FakeBucket(format="jsonl")
        dest.write(_ctx(plain_asset()), [{"id": 1}])

        payload, content_type, metadata = dest.objects["plain_asset/data.jsonl"]
        assert JSONLFormat().deserialize(payload) == [{"id": 1}]
        assert content_type == "application/jsonl"
        assert metadata == {"row_count": "1"}

    def test_partition_column_dropped_from_contents(self):
        dest = FakeBucket(format="jsonl")
        rows = [{"id": 1, "day": "2024-01-02"}, {"id": 2, "day": "2024-01-02"}]
        dest.write(_ctx(partitioned_asset(), _RowSchema, _day(2)), rows)

        payload, _, metadata = dest.objects["partitioned_asset/day=2024-01-02/data.jsonl"]
        assert JSONLFormat().deserialize(payload) == [{"id": 1}, {"id": 2}]
        assert metadata == {"row_count": "2"}

    def test_partition_column_dropped_from_parquet_schema(self):
        dest = FakeBucket()
        dest.write(_ctx(partitioned_asset(), _RowSchema, _day(2)), [{"id": 1, "cost": 1.0, "day": "2024-01-02"}])

        payload, _, _ = dest.objects["partitioned_asset/day=2024-01-02/data.parquet"]
        assert ParquetFormat().deserialize(payload) == [{"id": 1, "cost": 1.0}]

    def test_window_write_splits_per_partition(self):
        dest = FakeBucket(format="jsonl")
        window = il.TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        dest.write(
            _ctx(partitioned_asset(), None, window), [{"id": 1, "day": "2024-01-01"}, {"id": 2, "day": "2024-01-02"}]
        )

        assert {name: JSONLFormat().deserialize(payload) for name, (payload, _, _) in dest.objects.items()} == {
            "partitioned_asset/day=2024-01-01/data.jsonl": [{"id": 1}],
            "partitioned_asset/day=2024-01-02/data.jsonl": [{"id": 2}],
        }

    def test_rewrite_overwrites(self):
        dest = FakeBucket(format="jsonl")
        dest.write(_ctx(plain_asset()), [{"id": 1}, {"id": 2}])
        dest.write(_ctx(plain_asset()), [{"id": 3}])
        assert dest.read(_ctx(plain_asset())) == [{"id": 3}]

    def test_content_type_follows_format(self):
        dest = FakeBucket(format="csv")
        dest.write(_ctx(plain_asset()), [{"id": 1}])
        assert dest.objects["plain_asset/data.csv"][1] == "text/csv"


class TestRead:
    """Reads re-inject the partition column and reconcile against the schema."""

    def test_unpartitioned_read_reconciles_schema(self):
        dest = FakeBucket(format="csv")
        dest.objects["plain_asset/data.csv"] = (
            CSVFormat().serialize([{"id": 1, "cost": 1.5, "day": datetime.date(2024, 1, 1)}], None),
            "text/csv",
            {},
        )
        assert dest.read(_ctx(plain_asset(), _RowSchema)) == [{"id": 1, "cost": 1.5, "day": datetime.date(2024, 1, 1)}]

    def test_partition_round_trip_reinjects_column(self):
        dest = FakeBucket()
        context = _ctx(partitioned_asset(), _RowSchema, _day(2))
        dest.write(context, [{"id": 1, "cost": 2.0, "day": datetime.date(2024, 1, 2)}])
        assert dest.read(context) == [{"id": 1, "cost": 2.0, "day": datetime.date(2024, 1, 2)}]

    def test_partition_read_without_schema_injects_id_string(self):
        dest = FakeBucket(format="jsonl")
        dest.write(_ctx(partitioned_asset(), None, _day(2)), [{"id": 1, "day": "2024-01-02"}])
        assert dest.read(_ctx(partitioned_asset(), None, _day(2))) == [{"id": 1, "day": "2024-01-02"}]

    def test_window_read_returns_one_result_per_partition(self):
        dest = FakeBucket(format="jsonl")
        window = il.TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        dest.write(
            _ctx(partitioned_asset(), None, window), [{"id": 1, "day": "2024-01-01"}, {"id": 2, "day": "2024-01-02"}]
        )
        assert dest.read(_ctx(partitioned_asset(), None, window)) == [
            [{"id": 2, "day": "2024-01-02"}],
            [{"id": 1, "day": "2024-01-01"}],
        ]

    def test_missing_object_raises_with_uri(self):
        with pytest.raises(DataNotFoundError, match=r"'fake://bucket/plain_asset/data\.parquet' does not exist"):
            FakeBucket().read(_ctx(plain_asset()))


class TestPartitionRowCounts:
    """Row counts come from object metadata, downloading only as a fallback."""

    def test_counts_from_metadata(self):
        dest = FakeBucket(format="jsonl")
        dest.objects["partitioned_asset/day=2024-01-01/data.jsonl"] = (b"", "", {"row_count": "3"})
        dest.objects["partitioned_asset/day=2024-01-02/data.jsonl"] = (b"", "", {"row_count": "5"})
        assert dest.partition_row_counts(_ctx(partitioned_asset())) == {"2024-01-01": 3, "2024-01-02": 5}

    def test_counts_after_write(self):
        dest = FakeBucket()
        window = il.TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        rows = [{"id": 1, "day": "2024-01-01"}, {"id": 2, "day": "2024-01-01"}, {"id": 3, "day": "2024-01-02"}]
        dest.write(_ctx(partitioned_asset(), None, window), rows)
        assert dest.partition_row_counts(_ctx(partitioned_asset())) == {"2024-01-01": 2, "2024-01-02": 1}

    def test_fallback_downloads_and_counts(self):
        dest = FakeBucket(format="jsonl")
        payload = JSONLFormat().serialize([{"id": 1}, {"id": 2}], None)
        dest.objects["partitioned_asset/day=2024-01-01/data.jsonl"] = (payload, "", {})
        assert dest.partition_row_counts(_ctx(partitioned_asset())) == {"2024-01-01": 2}

    def test_non_partition_objects_ignored(self):
        dest = FakeBucket(format="jsonl")
        dest.objects["partitioned_asset/data.jsonl"] = (b"", "", {"row_count": "9"})
        assert dest.partition_row_counts(_ctx(partitioned_asset())) == {}

    def test_other_assets_ignored(self):
        dest = FakeBucket(format="jsonl")
        dest.objects["partitioned_asset_v2/day=2024-01-01/data.jsonl"] = (b"", "", {"row_count": "9"})
        assert dest.partition_row_counts(_ctx(partitioned_asset())) == {}
