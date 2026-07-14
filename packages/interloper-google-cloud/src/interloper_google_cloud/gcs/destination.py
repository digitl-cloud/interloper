"""Google Cloud Storage destination implementation."""

from __future__ import annotations

import json
from functools import cached_property
from typing import Any

import google.auth
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from interloper.destination import IOContext, destination
from interloper.destination.partitioned import PartitionedDestination
from interloper.errors import DataNotFoundError
from interloper.partitioning import Partition
from interloper.representation import Representation
from interloper.resource.fields import FetchField, InputField, SelectField
from interloper.schema import FieldSpec

from interloper_google_cloud.connection import GoogleCloudConnection
from interloper_google_cloud.gcs.formats import FORMATS, FileFormat

# Custom blob metadata key carrying the scope's row count, so
# partition_row_counts introspects from a single list call without downloads.
_ROW_COUNT_METADATA_KEY = "row_count"


@destination(
    key="gcs_destination",
    name="Google Cloud Storage",
    icon="icon:gcs",
    tags=["Cloud"],
)
class GCSDestination(PartitionedDestination):
    """Google Cloud Storage destination.

    Writes one object per scope in a hive-partitioned layout::

        gs://{bucket}/{prefix}/{dataset}/{table}/data.{ext}
        gs://{bucket}/{prefix}/{dataset}/{table}/{column}={partition}/data.{ext}

    Following the hive convention, the partition column lives in the *path
    only*: it is dropped from partitioned file contents on write (external
    readers like BigQuery external tables and DuckDB reject a duplicate
    partition column) and re-injected from the partition scope on read, so
    interloper round-trips stay lossless.
    """

    connection: GoogleCloudConnection

    bucket: str = FetchField(
        provider="connection.buckets",
        label_key="name",
        value_key="name",
        description="Cloud Storage bucket",
        discriminator=True,
    )
    format: str = SelectField(
        default="parquet",
        description="Output file format",
        options=[
            {"label": "Parquet", "value": "parquet"},
            {"label": "JSONL", "value": "jsonl"},
            {"label": "CSV", "value": "csv"},
        ],
    )
    prefix: str | None = InputField(default=None, description="Path prefix inside the bucket")

    @cached_property
    def client(self) -> storage.Client:
        if self.connection and self.connection.service_account_key:
            key_info = json.loads(self.connection.service_account_key)
            credentials = service_account.Credentials.from_service_account_info(key_info)
            return storage.Client(project=key_info.get("project_id"), credentials=credentials)
        credentials, project = google.auth.default()
        return storage.Client(project=project, credentials=credentials)

    # -- Helpers ---------------------------------------------------------------

    @property
    def _format(self) -> FileFormat:
        """The configured file format strategy."""
        return FORMATS[self.format]

    def _asset_prefix(self, context: IOContext) -> str:
        """Return the object-name prefix for an asset (no trailing slash)."""
        parts = [self.prefix or "", context.asset.dataset or "", context.asset.table]
        return "/".join(part.strip("/") for part in parts if part and part.strip("/"))

    def _blob_name(self, context: IOContext, partition: Partition | None) -> str:
        """Build the object name for a scope.

        Returns:
            ``.../data.{ext}``, inside a ``{column}={id}`` segment for
            partition scopes.
        """
        parts = [self._asset_prefix(context)]
        if partition is not None:
            assert context.asset.partitioning
            parts.append(f"{context.asset.partitioning.column}={partition.id}")
        parts.append(f"data.{self._format.extension}")
        return "/".join(parts)

    def _effective_specs(self, context: IOContext, partition: Partition | None) -> list[FieldSpec] | None:
        """Return the field specs for a scope's file contents.

        Partition scopes exclude the partition column (its value lives in the
        object path).

        Returns:
            The specs, or ``None`` when the context carries no schema.
        """
        if context.schema is None:
            return None
        specs = context.schema.field_specs()
        if partition is not None:
            assert context.asset.partitioning
            specs = [spec for spec in specs if spec.name != context.asset.partitioning.column]
        return specs

    # -- PartitionedDestination hooks --------------------------------------------

    def _write_scope(self, context: IOContext, partition: Partition | None, data: Any) -> None:
        """Serialize one scope's data and upload it, overwriting the scope's object.

        The row count is stamped as blob metadata so introspection never has
        to download data.
        """
        rows = Representation.of(data).to_records(data)
        if partition is not None:
            assert context.asset.partitioning
            column = context.asset.partitioning.column
            rows = [{k: v for k, v in row.items() if k != column} for row in rows]

        payload = self._format.serialize(rows, self._effective_specs(context, partition))
        blob = self.client.bucket(self.bucket).blob(self._blob_name(context, partition))
        blob.metadata = {_ROW_COUNT_METADATA_KEY: str(len(rows))}
        blob.upload_from_string(payload, content_type=self._format.content_type)

    def _read_scope(self, context: IOContext, partition: Partition | None) -> list[dict[str, Any]]:
        """Download and parse one scope's object.

        The partition column is re-injected from the scope, and rows are
        reconciled against the context schema when one is set (restoring the
        declared types — text formats read everything back as strings).

        Returns:
            Rows as a list of dicts.

        Raises:
            DataNotFoundError: If the scope's object does not exist.
        """
        name = self._blob_name(context, partition)
        try:
            payload = self.client.bucket(self.bucket).blob(name).download_as_bytes()
        except NotFound:
            raise DataNotFoundError(
                f"Object 'gs://{self.bucket}/{name}' does not exist. Has the asset been materialized?"
            ) from None

        rows = self._format.deserialize(payload)
        if partition is not None:
            assert context.asset.partitioning
            column = context.asset.partitioning.column
            rows = [{**row, column: partition.id} for row in rows]
        if context.schema is not None:
            rows = context.schema.reconcile(rows)
        return rows

    # -- Introspection ---------------------------------------------------------

    def partition_row_counts(self, context: IOContext) -> dict[str, int]:
        """Return row counts grouped by partition from a single list call.

        Counts come from the ``row_count`` blob metadata stamped at write
        time; objects missing it (written by other tools) are downloaded and
        counted.

        Returns:
            Mapping from partition value (as string) to row count.
        """
        assert context.asset.partitioning is not None
        column = context.asset.partitioning.column
        prefix = self._asset_prefix(context) + "/"

        counts: dict[str, int] = {}
        for blob in self.client.list_blobs(self.bucket, prefix=prefix):
            scope = blob.name[len(prefix) :].split("/", 1)[0]
            if not scope.startswith(f"{column}="):
                continue
            value = scope.split("=", 1)[1]
            row_count = (blob.metadata or {}).get(_ROW_COUNT_METADATA_KEY)
            if row_count is None:
                row_count = len(self._format.deserialize(blob.download_as_bytes()))
            counts[value] = counts.get(value, 0) + int(row_count)
        return counts

    # -- Lifecycle -------------------------------------------------------------

    def dispose(self) -> None:
        if self.client:
            self.client.close()
