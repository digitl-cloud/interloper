"""Filesystem-backed destination using pickle serialization."""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any

from interloper.destination.context import IOContext
from interloper.destination.decorator import destination
from interloper.destination.partitioned import PartitionedDestination
from interloper.partitioning.base import Partition


@destination(name="File")
class FileDestination(PartitionedDestination):
    """Destination that reads and writes pickle files on the local filesystem.

    Data is stored under ``{base_path}/{dataset}/{table}/data.pkl``
    (or ``{base_path}/{table}/data.pkl`` when no dataset is set).
    Partitioned assets add a ``{column}={id}`` subdirectory; the partition
    dispatch (including window splitting) comes from
    :class:`PartitionedDestination`.

    Unlike :class:`~interloper.destination.csv.CSVDestination` this stores
    whatever the asset returned, tabular or not — so a window write of a
    non-tabular object, which no representation can split, stores that whole
    object under each partition.
    """

    base_path: str = ""

    def _asset_path(self, context: IOContext) -> Path:
        """Return the base directory for an asset.

        Args:
            context: IO context whose asset supplies the dataset and table
                segments of the path.

        Returns:
            The asset's directory, with the dataset segment omitted when unset.
        """
        return Path(self.base_path) / (context.asset.dataset or "") / context.asset.table

    def _scope_path(self, context: IOContext, partition: Partition | None) -> Path:
        """Return the data file path for a scope.

        Args:
            context: IO context whose asset supplies the dataset, table, and
                partition column.
            partition: The scope's partition, or ``None`` for the unpartitioned
                whole.

        Returns:
            ``.../data.pkl``, inside a ``{column}={id}`` subdirectory for
            partition scopes.
        """
        base = self._asset_path(context)
        if partition is None:
            return base / "data.pkl"
        assert context.asset.partitioning
        return base / f"{context.asset.partitioning.column}={partition.id}" / "data.pkl"

    def _write_scope(self, context: IOContext, partition: Partition | None, data: Any) -> None:
        """Pickle one scope's data to its file.

        Args:
            context: IO context whose asset supplies the dataset, table, and
                partition column.
            partition: The partition being written, or ``None`` for the
                unpartitioned whole.
            data: The scope's slice of the data, stored as-is.
        """
        path = self._scope_path(context, partition)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as f:
            pickle.dump(data, f)

    def _read_scope(self, context: IOContext, partition: Partition | None) -> Any:
        """Unpickle one scope's file.

        Args:
            context: IO context whose asset supplies the dataset, table, and
                partition column.
            partition: The partition to read, or ``None`` for the unpartitioned
                whole.

        Returns:
            The deserialized data.

        Raises:
            FileNotFoundError: If the scope's data file does not exist.
        """
        path = self._scope_path(context, partition)
        if not path.exists():
            raise FileNotFoundError(f"No data file for '{context.asset}': {path}")
        with path.open("rb") as f:
            return pickle.load(f)

    def partition_row_counts(self, context: IOContext) -> dict[str, int]:
        """Return row counts grouped by partition by scanning pickle files on disk.

        Counts items (``len(data)`` for lists, ``1`` otherwise), since a
        pickled scope need not be tabular.

        Args:
            context: IO context whose asset supplies the dataset, table, and
                partition column.

        Returns:
            Mapping from partition value (as string) to row count.
        """
        assert context.asset.partitioning is not None
        column = context.asset.partitioning.column
        base = self._asset_path(context)

        counts: dict[str, int] = {}
        if not base.exists():
            return counts

        for entry in sorted(base.iterdir()):
            if entry.is_dir() and entry.name.startswith(f"{column}="):
                partition_value = entry.name.split("=", 1)[1]
                data_file = entry / "data.pkl"
                if data_file.exists():
                    with data_file.open("rb") as f:
                        data = pickle.load(f)
                    counts[partition_value] = len(data) if isinstance(data, list) else 1
        return counts
