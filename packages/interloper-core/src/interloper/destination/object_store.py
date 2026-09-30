"""Object-store destinations: partitions are objects in a hive-partitioned bucket layout."""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from interloper.destination.base import Destination
from interloper.destination.context import IOContext
from interloper.destination.formats import FORMATS, FileFormat
from interloper.errors import DataNotFoundError
from interloper.partitioning.base import Partition
from interloper.representation import Representation
from interloper.resource.fields import InputField, SelectField
from interloper.schema import FieldSpec

# Custom object metadata key carrying the partition's row count, so
# partition_row_counts introspects from a single listing without downloads.
_ROW_COUNT_METADATA_KEY = "row_count"


@dataclass(frozen=True)
class StoredObject:
    """One object a backend lists: its name inside the bucket and its custom metadata.

    Attributes:
        name: The object name, relative to the bucket root.
        metadata: The object's custom (user) metadata, empty when it carries none.
    """

    name: str
    metadata: dict[str, str]


class ObjectStoreDestination(Destination):
    """A destination whose partitions are objects in a bucket.

    Writes one object per partition in a hive-partitioned layout::

        {prefix}/{dataset}/{table}/data.{ext}
        {prefix}/{dataset}/{table}/{column}={partition}/data.{ext}

    Following the hive convention, the partition column lives in the *path
    only*: it is dropped from partitioned file contents on write (external
    readers like BigQuery external tables and DuckDB reject a duplicate
    partition column) and re-injected from the partition on read, so
    interloper round-trips stay lossless. Each object carries its row count
    as ``row_count`` metadata, so :meth:`partition_row_counts` needs one
    listing and no downloads.

    A backend implements storage and nothing else, in four hooks:
    :meth:`put_object`, :meth:`get_object`, :meth:`list_objects` and
    :meth:`object_uri`. The destination instance holds no table identity and
    is shared across assets: ``asset.table`` and ``asset.dataset`` name the
    target at call time.
    """

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

    # -- Backend hooks ---------------------------------------------------------

    @abstractmethod
    def put_object(self, name: str, payload: bytes, content_type: str, metadata: dict[str, str]) -> None:
        """Upload an object, overwriting any object of the same name.

        Args:
            name: The object name, relative to the bucket root.
            payload: The object's bytes.
            content_type: The payload's media type.
            metadata: Custom metadata to store with the object.
        """

    @abstractmethod
    def get_object(self, name: str) -> bytes | None:
        """Download an object.

        Args:
            name: The object name, relative to the bucket root.

        Returns:
            The object's bytes, or ``None`` when no object has that name.
        """

    @abstractmethod
    def list_objects(self, prefix: str) -> Iterator[StoredObject]:
        """List every object under a name prefix, with its custom metadata.

        Args:
            prefix: The name prefix, ending with ``/``.

        Returns:
            The objects under the prefix, in any order.
        """

    @abstractmethod
    def object_uri(self, name: str) -> str:
        """Return the URI naming an object, for messages.

        Args:
            name: The object name, relative to the bucket root.

        Returns:
            The backend's URI for the object, such as ``s3://bucket/name``.
        """

    # -- Helpers ---------------------------------------------------------------

    @property
    def _format(self) -> FileFormat:
        """The configured file format strategy.

        Returns:
            The strategy for the configured format.
        """
        return FORMATS[self.format]

    def _asset_prefix(self, context: IOContext) -> str:
        """Return the object-name prefix for an asset (no trailing slash).

        Args:
            context: The IO context naming the asset.

        Returns:
            The prefix every object for the asset sits under.
        """
        parts = [self.prefix or "", context.asset.dataset or "", context.asset.table]
        return "/".join(part.strip("/") for part in parts if part and part.strip("/"))

    def _object_name(self, context: IOContext, partition: Partition | None) -> str:
        """Build the object name for a partition.

        Args:
            context: The IO context naming the asset.
            partition: The partition being addressed, or ``None`` for the whole.

        Returns:
            ``.../data.{ext}``, inside a ``{column}={id}`` segment for
            partitions.
        """
        parts = [self._asset_prefix(context)]
        if partition is not None:
            assert context.asset.partitioning
            parts.append(f"{context.asset.partitioning.column}={partition.id}")
        parts.append(f"data.{self._format.extension}")
        return "/".join(parts)

    def _effective_specs(self, context: IOContext, partition: Partition | None) -> list[FieldSpec] | None:
        """Return the field specs for a partition's file contents.

        Partitions exclude the partition column (its value lives in the
        object path).

        Args:
            context: The IO context carrying the schema.
            partition: The partition being addressed, or ``None`` for the whole.

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

    # -- Partition hooks -------------------------------------------------------

    def write_partition(self, context: IOContext, partition: Partition | None, data: Any) -> None:
        """Serialize one partition's data and upload it, overwriting its object.

        The row count is stamped as object metadata so introspection never has
        to download data.

        Args:
            context: The IO context naming the asset.
            partition: The partition being written, or ``None`` for the whole.
            data: The rows to write.
        """
        rows = Representation.of(data).records
        if partition is not None:
            assert context.asset.partitioning
            column = context.asset.partitioning.column
            rows = [{k: v for k, v in row.items() if k != column} for row in rows]

        payload = self._format.serialize(rows, self._effective_specs(context, partition))
        self.put_object(
            self._object_name(context, partition),
            payload,
            self._format.content_type,
            {_ROW_COUNT_METADATA_KEY: str(len(rows))},
        )

    def read_partition(self, context: IOContext, partition: Partition | None) -> list[dict[str, Any]]:
        """Download and parse one partition's object.

        The partition column is re-injected from the partition, and rows are
        reconciled against the context schema when one is set (restoring the
        declared types, since text formats read everything back as strings).

        Args:
            context: The IO context carrying the schema.
            partition: The partition being read, or ``None`` for the whole.

        Returns:
            Rows as a list of dicts.

        Raises:
            DataNotFoundError: If the partition's object does not exist.
        """
        name = self._object_name(context, partition)
        payload = self.get_object(name)
        if payload is None:
            raise DataNotFoundError(
                f"Object '{self.object_uri(name)}' does not exist. Has the asset been materialized?"
            )

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
        """Return row counts grouped by partition from a single listing.

        Counts come from the ``row_count`` object metadata stamped at write
        time; objects missing it (written by other tools) are downloaded and
        counted.

        Args:
            context: The IO context naming the asset.

        Returns:
            Mapping from partition value (as string) to row count.
        """
        assert context.asset.partitioning is not None
        column = context.asset.partitioning.column
        prefix = self._asset_prefix(context) + "/"

        counts: dict[str, int] = {}
        for stored in self.list_objects(prefix):
            segment = stored.name[len(prefix) :].split("/", 1)[0]
            if not segment.startswith(f"{column}="):
                continue
            value = segment.split("=", 1)[1]
            row_count = stored.metadata.get(_ROW_COUNT_METADATA_KEY)
            if row_count is None:
                payload = self.get_object(stored.name)
                if payload is None:
                    continue
                row_count = len(self._format.deserialize(payload))
            counts[value] = counts.get(value, 0) + int(row_count)
        return counts
