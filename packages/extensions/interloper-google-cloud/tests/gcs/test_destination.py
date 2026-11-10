"""Tests for GCSDestination: the Cloud Storage client and the four object hooks."""

import json
from typing import Any
from unittest.mock import MagicMock, patch

from google.cloud.exceptions import NotFound
from interloper.destination import ObjectStoreDestination, StoredObject

from interloper_google_cloud.connection import GoogleCloudConnection
from interloper_google_cloud.gcs import destination as gcs_module
from interloper_google_cloud.gcs.destination import GCSDestination

# A minimal service account key JSON for testing.
_SA_KEY = json.dumps({"type": "service_account", "project_id": "test-proj"})


def _make_destination(**overrides: Any) -> tuple[GCSDestination, MagicMock]:
    """Create a GCSDestination with a mocked storage client.

    Returns:
        The destination and the mocked client.
    """
    mock_client = MagicMock()
    dest = GCSDestination(
        id="test",
        bucket="test-bucket",
        connection=GoogleCloudConnection(id="test-connection", service_account_key=_SA_KEY),
        **overrides,
    )
    object.__setattr__(dest, "client", mock_client)  # noqa: PLC2801 - bypasses pydantic's __setattr__
    return dest, mock_client


def test_is_an_object_store_destination():
    assert issubclass(GCSDestination, ObjectStoreDestination)


class TestClient:
    """The client comes from the connection's key, else from ambient credentials."""

    def test_from_service_account_key(self):
        dest = GCSDestination(
            id="test",
            bucket="test-bucket",
            connection=GoogleCloudConnection(id="c", service_account_key=_SA_KEY),
        )
        with (
            patch.object(gcs_module.service_account.Credentials, "from_service_account_info") as from_info,
            patch.object(gcs_module.storage, "Client") as client_cls,
        ):
            assert dest.client is client_cls.return_value
        from_info.assert_called_once_with(json.loads(_SA_KEY))
        client_cls.assert_called_once_with(project="test-proj", credentials=from_info.return_value)

    def test_from_ambient_credentials(self):
        dest = GCSDestination(
            id="test",
            bucket="test-bucket",
            connection=GoogleCloudConnection(id="c", service_account_key=_SA_KEY),
        )
        object.__setattr__(dest.connection, "service_account_key", "")  # noqa: PLC2801 - bypasses pydantic's __setattr__
        credentials = MagicMock()
        with (
            patch.object(gcs_module.google.auth, "default", return_value=(credentials, "ambient-proj")),
            patch.object(gcs_module.storage, "Client") as client_cls,
        ):
            assert dest.client is client_cls.return_value
        client_cls.assert_called_once_with(project="ambient-proj", credentials=credentials)


class TestHooks:
    """The four object hooks over ``google.cloud.storage``."""

    def test_put_object(self):
        dest, client = _make_destination()
        dest.put_object("a/data.csv", b"id\n1\n", "text/csv", {"row_count": "1"})

        client.bucket.assert_called_once_with("test-bucket")
        client.bucket.return_value.blob.assert_called_once_with("a/data.csv")
        blob = client.bucket.return_value.blob.return_value
        assert blob.metadata == {"row_count": "1"}
        blob.upload_from_string.assert_called_once_with(b"id\n1\n", content_type="text/csv")

    def test_get_object(self):
        dest, client = _make_destination()
        blob = client.bucket.return_value.blob.return_value
        blob.download_as_bytes.return_value = b"payload"

        assert dest.get_object("a/data.csv") == b"payload"
        client.bucket.assert_called_once_with("test-bucket")
        client.bucket.return_value.blob.assert_called_once_with("a/data.csv")

    def test_get_missing_object_is_none(self):
        dest, client = _make_destination()
        client.bucket.return_value.blob.return_value.download_as_bytes.side_effect = NotFound("nope")
        assert dest.get_object("a/data.csv") is None

    def test_list_objects(self):
        dest, client = _make_destination()
        with_metadata, without_metadata = MagicMock(), MagicMock()
        with_metadata.name, with_metadata.metadata = "a/day=1/data.csv", {"row_count": "3"}
        without_metadata.name, without_metadata.metadata = "a/day=2/data.csv", None
        client.list_blobs.return_value = [with_metadata, without_metadata]

        assert list(dest.list_objects("a/")) == [
            StoredObject(name="a/day=1/data.csv", metadata={"row_count": "3"}),
            StoredObject(name="a/day=2/data.csv", metadata={}),
        ]
        client.list_blobs.assert_called_once_with("test-bucket", prefix="a/")

    def test_object_uri(self):
        dest, _ = _make_destination()
        assert dest.object_uri("a/data.parquet") == "gs://test-bucket/a/data.parquet"
