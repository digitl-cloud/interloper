"""Google Cloud Storage destination implementation."""

from __future__ import annotations

import json
from collections.abc import Iterator
from functools import cached_property

import google.auth
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from interloper.destination import ObjectStoreDestination, StoredObject, destination
from interloper.resource.fields import FetchField

from interloper_google_cloud.connection import GoogleCloudConnection


@destination(
    key="gcs_destination",
    name="Google Cloud Storage",
    icon="icon:gcs",
    tags=["Cloud"],
)
class GCSDestination(ObjectStoreDestination):
    """Google Cloud Storage destination.

    An :class:`~interloper.destination.ObjectStoreDestination` over Cloud
    Storage: the base owns the hive layout, the formats and the row-count
    metadata, so objects land at::

        gs://{bucket}/{prefix}/{dataset}/{table}/data.{ext}
        gs://{bucket}/{prefix}/{dataset}/{table}/{column}={partition}/data.{ext}
    """

    connection: GoogleCloudConnection

    bucket: str = FetchField(
        provider="connection.buckets",
        label_key="name",
        value_key="name",
        description="Cloud Storage bucket",
        discriminator=True,
    )

    @cached_property
    def client(self) -> storage.Client:
        """The Cloud Storage client every read and write goes through.

        Built from the connection's service-account key when there is one,
        else from ambient credentials (workload identity in-cluster).

        Returns:
            The client, cached per destination instance.
        """
        if self.connection and self.connection.service_account_key:
            key_info = json.loads(self.connection.service_account_key)
            credentials = service_account.Credentials.from_service_account_info(key_info)
            return storage.Client(project=key_info.get("project_id"), credentials=credentials)
        credentials, project = google.auth.default()
        return storage.Client(project=project, credentials=credentials)

    # -- Object hooks ----------------------------------------------------------

    def put_object(self, name: str, payload: bytes, content_type: str, metadata: dict[str, str]) -> None:
        """Upload a blob, overwriting any blob of the same name.

        Args:
            name: The blob name, relative to the bucket root.
            payload: The blob's bytes.
            content_type: The payload's media type.
            metadata: Custom metadata to store on the blob.
        """
        blob = self.client.bucket(self.bucket).blob(name)
        blob.metadata = metadata
        blob.upload_from_string(payload, content_type=content_type)

    def get_object(self, name: str) -> bytes | None:
        """Download a blob.

        Args:
            name: The blob name, relative to the bucket root.

        Returns:
            The blob's bytes, or ``None`` when no blob has that name.
        """
        try:
            return self.client.bucket(self.bucket).blob(name).download_as_bytes()
        except NotFound:
            return None

    def list_objects(self, prefix: str) -> Iterator[StoredObject]:
        """List the blobs under a name prefix; the listing carries their metadata.

        Args:
            prefix: The name prefix, ending with ``/``.

        Yields:
            One stored object per blob.
        """
        for blob in self.client.list_blobs(self.bucket, prefix=prefix):
            yield StoredObject(name=blob.name, metadata=dict(blob.metadata or {}))

    def object_uri(self, name: str) -> str:
        """Return the ``gs://`` URI of a blob.

        Args:
            name: The blob name, relative to the bucket root.

        Returns:
            ``gs://{bucket}/{name}``.
        """
        return f"gs://{self.bucket}/{name}"
