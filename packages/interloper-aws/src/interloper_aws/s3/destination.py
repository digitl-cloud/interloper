"""Amazon S3 destination implementation."""

from __future__ import annotations

from collections.abc import Iterator
from functools import cached_property
from typing import Any

from botocore.exceptions import ClientError
from interloper.destination import ObjectStoreDestination, StoredObject, destination
from interloper.resource.fields import FetchField

from interloper_aws.connection import AWSConnection


@destination(
    key="s3_destination",
    name="Amazon S3",
    icon="icon:s3",
    tags=["Cloud"],
)
class S3Destination(ObjectStoreDestination):
    """Amazon S3 destination.

    An :class:`~interloper.destination.ObjectStoreDestination` over S3: the
    base owns the hive layout, the formats and the row-count metadata, so
    objects land at::

        s3://{bucket}/{prefix}/{dataset}/{table}/data.{ext}
        s3://{bucket}/{prefix}/{dataset}/{table}/{column}={partition}/data.{ext}
    """

    connection: AWSConnection

    bucket: str = FetchField(
        provider="connection.buckets",
        label_key="name",
        value_key="name",
        description="S3 bucket",
        discriminator=True,
    )

    @cached_property
    def client(self) -> Any:
        """The S3 client every read and write goes through.

        Returns:
            The client, cached per destination instance.
        """
        return self.connection.client("s3")

    # -- Object hooks ----------------------------------------------------------

    def put_object(self, name: str, payload: bytes, content_type: str, metadata: dict[str, str]) -> None:
        """Upload an object, overwriting any object of the same key.

        Args:
            name: The object key.
            payload: The object's bytes.
            content_type: The payload's media type.
            metadata: User metadata to store with the object.
        """
        self.client.put_object(
            Bucket=self.bucket,
            Key=name,
            Body=payload,
            ContentType=content_type,
            Metadata=metadata,
        )

    def get_object(self, name: str) -> bytes | None:
        """Download an object.

        Args:
            name: The object key.

        Returns:
            The object's bytes, or ``None`` when no object has that key.

        Raises:
            ClientError: For any S3 error other than a missing key.
        """
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=name)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "NoSuchKey":
                return None
            raise
        return response["Body"].read()

    def list_objects(self, prefix: str) -> Iterator[StoredObject]:
        """List the objects under a key prefix, with their user metadata.

        ``ListObjectsV2`` carries no user metadata, so each listed key is
        followed by a ``HeadObject``.

        Args:
            prefix: The key prefix, ending with ``/``.

        Yields:
            One stored object per key.
        """
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for listed in page.get("Contents", []):
                head = self.client.head_object(Bucket=self.bucket, Key=listed["Key"])
                yield StoredObject(name=listed["Key"], metadata=dict(head.get("Metadata", {})))

    def object_uri(self, name: str) -> str:
        """Return the ``s3://`` URI of an object.

        Args:
            name: The object key.

        Returns:
            ``s3://{bucket}/{name}``.
        """
        return f"s3://{self.bucket}/{name}"
