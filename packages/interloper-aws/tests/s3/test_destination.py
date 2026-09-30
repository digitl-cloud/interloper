"""Tests for ``interloper_aws.s3.destination``."""

import datetime
from typing import Any
from unittest.mock import MagicMock

import interloper as il
import pytest
from botocore.exceptions import ClientError
from interloper.destination import IOContext, ObjectStoreDestination, StoredObject
from interloper.errors import DataNotFoundError
from interloper.schema import Schema
from pydantic import Field

from interloper_aws import AWSConnection, S3Destination


def _client_error(code: str, operation: str = "GetObject") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


def _make_destination(client: Any, **overrides: Any) -> S3Destination:
    dest = S3Destination(id="test", bucket="test-bucket", connection=AWSConnection(id="aws"), **overrides)
    object.__setattr__(dest, "client", client)  # noqa: PLC2801 - bypasses pydantic's __setattr__
    return dest


class FakeS3:
    """A dict-backed stand-in for the four S3 calls the destination makes."""

    def __init__(self) -> None:
        """Start with an empty bucket store keyed by ``(bucket, key)``."""
        self.objects: dict[tuple[str, str], dict[str, Any]] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType, Metadata):
        self.objects[(Bucket, Key)] = {"Body": Body, "ContentType": ContentType, "Metadata": Metadata}

    def get_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise _client_error("NoSuchKey")
        body = MagicMock()
        body.read.return_value = self.objects[(Bucket, Key)]["Body"]
        return {"Body": body}

    def head_object(self, *, Bucket, Key):
        return {"Metadata": dict(self.objects[(Bucket, Key)]["Metadata"])}

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        paginator = MagicMock()
        paginator.paginate.side_effect = lambda *, Bucket, Prefix: [
            {"Contents": [{"Key": key} for bucket, key in self.objects if bucket == Bucket and key.startswith(Prefix)]}
        ]
        return paginator


def test_is_an_object_store_destination():
    assert issubclass(S3Destination, ObjectStoreDestination)


class TestClient:
    def test_comes_from_the_connection(self):
        dest = S3Destination(id="test", bucket="test-bucket", connection=AWSConnection(id="aws"))
        session = MagicMock()
        object.__setattr__(dest.connection, "session", session)  # noqa: PLC2801 - bypasses pydantic's __setattr__

        assert dest.client is session.client.return_value
        assert dest.client is dest.client
        session.client.assert_called_once_with("s3")


class TestHooks:
    def test_put_object(self):
        client = MagicMock()
        _make_destination(client).put_object("a/data.csv", b"id\n1\n", "text/csv", {"row_count": "1"})

        client.put_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="a/data.csv",
            Body=b"id\n1\n",
            ContentType="text/csv",
            Metadata={"row_count": "1"},
        )

    def test_get_object(self):
        client = MagicMock()
        client.get_object.return_value["Body"].read.return_value = b"payload"

        assert _make_destination(client).get_object("a/data.csv") == b"payload"
        client.get_object.assert_called_once_with(Bucket="test-bucket", Key="a/data.csv")

    def test_get_missing_object_is_none(self):
        client = MagicMock()
        client.get_object.side_effect = _client_error("NoSuchKey")
        assert _make_destination(client).get_object("a/data.csv") is None

    def test_get_object_reraises_other_errors(self):
        client = MagicMock()
        client.get_object.side_effect = _client_error("AccessDenied")
        with pytest.raises(ClientError, match="AccessDenied"):
            _make_destination(client).get_object("a/data.csv")

    def test_list_objects_pages_and_heads_each_key(self):
        client = MagicMock()
        client.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": "a/day=1/data.csv"}, {"Key": "a/day=2/data.csv"}]},
            {"Contents": [{"Key": "a/day=3/data.csv"}]},
            {},
        ]
        client.head_object.side_effect = [
            {"Metadata": {"row_count": "3"}},
            {},
            {"Metadata": {"row_count": "1"}},
        ]

        assert list(_make_destination(client).list_objects("a/")) == [
            StoredObject(name="a/day=1/data.csv", metadata={"row_count": "3"}),
            StoredObject(name="a/day=2/data.csv", metadata={}),
            StoredObject(name="a/day=3/data.csv", metadata={"row_count": "1"}),
        ]
        client.get_paginator.assert_called_once_with("list_objects_v2")
        client.get_paginator.return_value.paginate.assert_called_once_with(Bucket="test-bucket", Prefix="a/")
        assert [call.kwargs for call in client.head_object.call_args_list] == [
            {"Bucket": "test-bucket", "Key": "a/day=1/data.csv"},
            {"Bucket": "test-bucket", "Key": "a/day=2/data.csv"},
            {"Bucket": "test-bucket", "Key": "a/day=3/data.csv"},
        ]

    def test_object_uri(self):
        assert _make_destination(MagicMock()).object_uri("a/data.parquet") == "s3://test-bucket/a/data.parquet"


class _RowSchema(Schema):
    id: int | None = Field(...)
    cost: float | None = Field(...)
    day: datetime.date | None = Field(...)


@il.asset(partitioning=il.TimePartitionConfig(column="day"))
def partitioned_asset() -> list:
    return []


@il.asset
def plain_asset() -> list:
    return []


class TestThroughTheBase:
    """End to end over a dict-backed S3: layout, round trip, row counts, missing objects."""

    @pytest.mark.parametrize("fmt", ["parquet", "jsonl", "csv"])
    def test_time_partitioned_window_round_trip(self, fmt):
        s3 = FakeS3()
        dest = _make_destination(s3, format=fmt, prefix="lake")
        window = il.TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        rows = [
            {"id": 1, "cost": 1.5, "day": datetime.date(2024, 1, 1)},
            {"id": 2, "cost": 2.5, "day": datetime.date(2024, 1, 1)},
            {"id": 3, "cost": None, "day": datetime.date(2024, 1, 2)},
        ]
        dest.write(IOContext(asset=partitioned_asset(), partition_or_window=window, schema=_RowSchema), rows)

        assert sorted(key for _, key in s3.objects) == [
            f"lake/partitioned_asset/day=2024-01-01/data.{fmt}",
            f"lake/partitioned_asset/day=2024-01-02/data.{fmt}",
        ]
        assert s3.objects[("test-bucket", f"lake/partitioned_asset/day=2024-01-01/data.{fmt}")]["Metadata"] == {
            "row_count": "2"
        }

        partition = il.TimePartition(datetime.date(2024, 1, 1))
        assert dest.read(IOContext(asset=partitioned_asset(), partition_or_window=partition, schema=_RowSchema)) == [
            rows[0],
            rows[1],
        ]
        assert dest.partition_row_counts(IOContext(asset=partitioned_asset())) == {"2024-01-01": 2, "2024-01-02": 1}

    def test_missing_object_raises_with_s3_uri(self):
        dest = _make_destination(FakeS3())
        with pytest.raises(DataNotFoundError, match=r"'s3://test-bucket/plain_asset/data\.parquet' does not exist"):
            dest.read(IOContext(asset=plain_asset()))
