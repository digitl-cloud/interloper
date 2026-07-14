"""Tests for GCSDestination and its file formats."""

import datetime
import json
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import interloper as il
import pytest
from google.cloud.exceptions import NotFound
from interloper.destination import IOContext
from interloper.errors import DataNotFoundError
from interloper.schema import Schema
from pydantic import Field

from interloper_google_cloud.connection import GoogleCloudConnection
from interloper_google_cloud.gcs.destination import GCSDestination
from interloper_google_cloud.gcs.formats import FORMATS, CSVFormat, JSONLFormat, ParquetFormat

# A minimal service account key JSON for testing.
_SA_KEY = json.dumps({"type": "service_account", "project_id": "test-proj"})


class _RowSchema(Schema):
    id: int | None = Field(..., description="Row id")
    cost: float | None = Field(...)
    day: datetime.date | None = Field(...)


def _ctx(asset: Any, schema: type[Schema] | None, scope: Any = None) -> IOContext:
    return IOContext(asset=asset, partition_or_window=scope, schema=schema)


@il.asset
def _plain_asset() -> list:
    return []


@il.asset(partitioning=il.TimePartitionConfig(column="day"))
def _partitioned_asset() -> list:
    return []


def _make_destination(**overrides: Any) -> tuple[GCSDestination, MagicMock, MagicMock]:
    """Create a GCSDestination with a mocked storage client.

    Returns:
        The destination, the mocked client, and the mocked blob.
    """
    mock_client = MagicMock()
    mock_blob = mock_client.bucket.return_value.blob.return_value

    conn = MagicMock(spec=GoogleCloudConnection)
    conn.service_account_key = _SA_KEY

    dest = GCSDestination(  # ty: ignore[missing-argument]
        id="test",
        bucket="test-bucket",
        format=overrides.get("format", "parquet"),
        prefix=overrides.get("prefix", None),
        resources={"connection": conn},
    )
    object.__setattr__(dest, "client", mock_client)
    return dest, mock_client, mock_blob


# -- Formats ---------------------------------------------------------------------


class TestJSONLFormat:
    """NDJSON serialization."""

    def test_round_trip(self):
        rows = [{"id": 1, "name": "a"}, {"id": 2, "name": None}]
        payload = JSONLFormat().serialize(rows, None)
        assert JSONLFormat().deserialize(payload) == rows

    def test_one_object_per_line(self):
        payload = JSONLFormat().serialize([{"a": 1}, {"a": 2}], None)
        assert payload == b'{"a": 1}\n{"a": 2}'

    def test_dates_and_decimals_serialized(self):
        rows = [{"day": datetime.date(2024, 1, 2), "cost": Decimal("9.99")}]
        assert JSONLFormat().deserialize(JSONLFormat().serialize(rows, None)) == [
            {"day": "2024-01-02", "cost": "9.99"}
        ]

    def test_nan_becomes_null(self):
        payload = JSONLFormat().serialize([{"cost": float("nan")}], None)
        assert JSONLFormat().deserialize(payload) == [{"cost": None}]

    def test_empty(self):
        assert JSONLFormat().serialize([], None) == b""
        assert JSONLFormat().deserialize(b"") == []


class TestCSVFormat:
    """CSV serialization."""

    def test_round_trip_as_strings(self):
        rows = [{"id": 1, "name": "a"}]
        payload = CSVFormat().serialize(rows, None)
        assert CSVFormat().deserialize(payload) == [{"id": "1", "name": "a"}]

    def test_empty_cell_reads_as_none(self):
        payload = CSVFormat().serialize([{"id": 1, "name": None}], None)
        assert CSVFormat().deserialize(payload) == [{"id": "1", "name": None}]

    def test_empty(self):
        assert CSVFormat().serialize([], None) == b""
        assert CSVFormat().deserialize(b"") == []


class TestParquetFormat:
    """Parquet serialization."""

    def test_round_trip_types_preserved_with_specs(self):
        rows = [
            {"id": 1, "cost": 1.5, "day": datetime.date(2024, 1, 1)},
            {"id": None, "cost": None, "day": None},
        ]
        payload = ParquetFormat().serialize(rows, _RowSchema.field_specs())
        assert ParquetFormat().deserialize(payload) == rows

    def test_round_trip_without_specs_infers(self):
        rows = [{"id": 1, "name": "a"}]
        payload = ParquetFormat().serialize(rows, None)
        assert ParquetFormat().deserialize(payload) == rows

    def test_schema_stable_when_column_all_null(self):
        """Specs pin column types; an all-null column keeps its declared type."""
        payload = ParquetFormat().serialize([{"id": None, "cost": None, "day": None}], _RowSchema.field_specs())
        assert ParquetFormat().deserialize(payload) == [{"id": None, "cost": None, "day": None}]

    def test_untyped_field_stringified(self):
        class AnySchema(Schema):
            val: Any = Field(None)

        payload = ParquetFormat().serialize([{"val": 42}], AnySchema.field_specs())
        assert ParquetFormat().deserialize(payload) == [{"val": "42"}]

    def test_repeated_field(self):
        class TagsSchema(Schema):
            tags: list[str]

        rows = [{"tags": ["a", "b"]}]
        payload = ParquetFormat().serialize(rows, TagsSchema.field_specs())
        assert ParquetFormat().deserialize(payload) == rows

    def test_empty_with_specs(self):
        payload = ParquetFormat().serialize([], _RowSchema.field_specs())
        assert ParquetFormat().deserialize(payload) == []


class TestFormatsRegistry:
    def test_all_formats_registered(self):
        assert set(FORMATS) == {"parquet", "jsonl", "csv"}


# -- Blob layout -----------------------------------------------------------------


class TestBlobName:
    """Hive-partitioned object naming."""

    def test_unpartitioned(self):
        dest, _, _ = _make_destination()
        assert dest._blob_name(_ctx(_plain_asset(), None), None) == "plain_asset/data.parquet"

    def test_partition_scope(self):
        dest, _, _ = _make_destination(format="jsonl")
        partition = il.TimePartition(datetime.date(2024, 1, 2))
        name = dest._blob_name(_ctx(_partitioned_asset(), None), partition)
        assert name == "partitioned_asset/day=2024-01-02/data.jsonl"

    def test_prefix_and_dataset(self):
        dest, _, _ = _make_destination(prefix="lake/raw/")
        asset = _plain_asset()
        object.__setattr__(asset, "dataset", "ds")
        assert dest._blob_name(_ctx(asset, None), None) == "lake/raw/ds/plain_asset/data.parquet"


# -- Write -----------------------------------------------------------------------


class TestWrite:
    """Scope writes: hive column dropping, metadata stamping, window splitting."""

    def test_unpartitioned_write(self):
        dest, mock_client, mock_blob = _make_destination(format="jsonl")
        dest.write(_ctx(_plain_asset(), None), [{"id": 1}])

        mock_client.bucket.assert_called_with("test-bucket")
        mock_client.bucket.return_value.blob.assert_called_with("plain_asset/data.jsonl")
        payload = mock_blob.upload_from_string.call_args[0][0]
        assert JSONLFormat().deserialize(payload) == [{"id": 1}]
        assert mock_blob.metadata == {"row_count": "1"}

    def test_partition_column_dropped_from_contents(self):
        dest, mock_client, mock_blob = _make_destination(format="jsonl")
        scope = il.TimePartition(datetime.date(2024, 1, 2))
        rows = [{"id": 1, "day": "2024-01-02"}, {"id": 2, "day": "2024-01-02"}]
        dest.write(_ctx(_partitioned_asset(), _RowSchema, scope), rows)

        mock_client.bucket.return_value.blob.assert_called_with("partitioned_asset/day=2024-01-02/data.jsonl")
        payload = mock_blob.upload_from_string.call_args[0][0]
        assert JSONLFormat().deserialize(payload) == [{"id": 1}, {"id": 2}]
        assert mock_blob.metadata == {"row_count": "2"}

    def test_partition_column_dropped_from_parquet_schema(self):
        dest, _, mock_blob = _make_destination()
        scope = il.TimePartition(datetime.date(2024, 1, 2))
        dest.write(_ctx(_partitioned_asset(), _RowSchema, scope), [{"id": 1, "cost": 1.0, "day": "2024-01-02"}])

        payload = mock_blob.upload_from_string.call_args[0][0]
        assert ParquetFormat().deserialize(payload) == [{"id": 1, "cost": 1.0}]

    def test_window_write_splits_per_partition(self):
        dest, mock_client, mock_blob = _make_destination(format="jsonl")
        window = il.TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        rows = [
            {"id": 1, "day": "2024-01-01"},
            {"id": 2, "day": "2024-01-02"},
        ]
        dest.write(_ctx(_partitioned_asset(), None, window), rows)

        names = [call.args[0] for call in mock_client.bucket.return_value.blob.call_args_list]
        assert names == [
            "partitioned_asset/day=2024-01-02/data.jsonl",
            "partitioned_asset/day=2024-01-01/data.jsonl",
        ]
        payloads = [JSONLFormat().deserialize(call.args[0]) for call in mock_blob.upload_from_string.call_args_list]
        assert payloads == [[{"id": 2}], [{"id": 1}]]

    def test_content_type(self):
        dest, _, mock_blob = _make_destination(format="csv")
        dest.write(_ctx(_plain_asset(), None), [{"id": 1}])
        assert mock_blob.upload_from_string.call_args.kwargs["content_type"] == "text/csv"


# -- Read ------------------------------------------------------------------------


class TestRead:
    """Scope reads: partition re-injection and schema reconciliation."""

    def test_unpartitioned_read_reconciles_schema(self):
        dest, _, mock_blob = _make_destination(format="csv")
        mock_blob.download_as_bytes.return_value = CSVFormat().serialize(
            [{"id": 1, "cost": 1.5, "day": datetime.date(2024, 1, 1)}], None
        )
        rows = dest.read(_ctx(_plain_asset(), _RowSchema))
        assert rows == [{"id": 1, "cost": 1.5, "day": datetime.date(2024, 1, 1)}]

    def test_partition_read_reinjects_column(self):
        dest, mock_client, mock_blob = _make_destination(format="jsonl")
        mock_blob.download_as_bytes.return_value = JSONLFormat().serialize([{"id": 1, "cost": 2.0}], None)
        scope = il.TimePartition(datetime.date(2024, 1, 2))
        rows = dest.read(_ctx(_partitioned_asset(), _RowSchema, scope))

        mock_client.bucket.return_value.blob.assert_called_with("partitioned_asset/day=2024-01-02/data.jsonl")
        assert rows == [{"id": 1, "cost": 2.0, "day": datetime.date(2024, 1, 2)}]

    def test_partition_read_without_schema_injects_id_string(self):
        dest, _, mock_blob = _make_destination(format="jsonl")
        mock_blob.download_as_bytes.return_value = JSONLFormat().serialize([{"id": 1}], None)
        scope = il.TimePartition(datetime.date(2024, 1, 2))
        assert dest.read(_ctx(_partitioned_asset(), None, scope)) == [{"id": 1, "day": "2024-01-02"}]

    def test_window_read_returns_one_result_per_partition(self):
        dest, _, mock_blob = _make_destination(format="jsonl")
        mock_blob.download_as_bytes.side_effect = [
            JSONLFormat().serialize([{"id": 2}], None),
            JSONLFormat().serialize([{"id": 1}], None),
        ]
        window = il.TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        rows = dest.read(_ctx(_partitioned_asset(), None, window))
        assert rows == [
            [{"id": 2, "day": "2024-01-02"}],
            [{"id": 1, "day": "2024-01-01"}],
        ]

    def test_missing_object_raises(self):
        dest, _, mock_blob = _make_destination()
        mock_blob.download_as_bytes.side_effect = NotFound("nope")
        with pytest.raises(DataNotFoundError, match="gs://test-bucket/plain_asset/data.parquet"):
            dest.read(_ctx(_plain_asset(), None))


# -- Introspection -----------------------------------------------------------------


def _listed_blob(name: str, metadata: dict[str, str] | None, payload: bytes | None = None) -> MagicMock:
    blob = MagicMock()
    blob.name = name
    blob.metadata = metadata
    if payload is not None:
        blob.download_as_bytes.return_value = payload
    return blob


class TestPartitionRowCounts:
    """Row counts come from blob metadata, downloading only as a fallback."""

    def test_counts_from_metadata(self):
        dest, mock_client, _ = _make_destination(format="jsonl")
        mock_client.list_blobs.return_value = [
            _listed_blob("partitioned_asset/day=2024-01-01/data.jsonl", {"row_count": "3"}),
            _listed_blob("partitioned_asset/day=2024-01-02/data.jsonl", {"row_count": "5"}),
        ]
        counts = dest.partition_row_counts(_ctx(_partitioned_asset(), None))
        assert counts == {"2024-01-01": 3, "2024-01-02": 5}
        mock_client.list_blobs.assert_called_once_with("test-bucket", prefix="partitioned_asset/")

    def test_fallback_downloads_and_counts(self):
        dest, mock_client, _ = _make_destination(format="jsonl")
        payload = JSONLFormat().serialize([{"id": 1}, {"id": 2}], None)
        mock_client.list_blobs.return_value = [
            _listed_blob("partitioned_asset/day=2024-01-01/data.jsonl", None, payload),
        ]
        assert dest.partition_row_counts(_ctx(_partitioned_asset(), None)) == {"2024-01-01": 2}

    def test_non_partition_objects_ignored(self):
        dest, mock_client, _ = _make_destination(format="jsonl")
        mock_client.list_blobs.return_value = [
            _listed_blob("partitioned_asset/data.jsonl", {"row_count": "9"}),
        ]
        assert dest.partition_row_counts(_ctx(_partitioned_asset(), None)) == {}
