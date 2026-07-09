"""Filesystem-backed destination using CSV serialization."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from interloper.destination.context import IOContext
from interloper.destination.decorator import destination
from interloper.destination.partitioned import PartitionedDestination
from interloper.partitioning.base import Partition
from interloper.representation import Representation


@destination(name="CSV")
class CSVDestination(PartitionedDestination):
    """Destination that reads and writes CSV files on the local filesystem.

    Data is stored under ``{base_path}/{dataset}/{asset_key}/data.csv``
    (or ``{base_path}/{asset_key}/data.csv`` when no dataset is set).
    Partitioned assets add a ``{column}={id}`` subdirectory; the partition
    dispatch (including window splitting) comes from
    :class:`PartitionedDestination`.

    Data is viewed as ``list[dict]`` records through its registered
    representation on write (each dict is a row; the keys of the first dict
    determine the CSV headers).

    CSV stores every value as a string.  When the IO context carries a schema,
    reads reconcile the rows against it, restoring the declared types.
    """

    base_path: str = ""

    def _asset_path(self, context: IOContext) -> Path:
        """Return the base directory for an asset."""
        return Path(self.base_path) / (context.asset.dataset or "") / type(context.asset).key

    def _scope_path(self, context: IOContext, partition: Partition | None) -> Path:
        """Return the data file path for a scope.

        Returns:
            ``.../data.csv``, inside a ``{column}={id}`` subdirectory for
            partition scopes.
        """
        base = self._asset_path(context)
        if partition is None:
            return base / "data.csv"
        assert context.asset.partitioning
        return base / f"{context.asset.partitioning.column}={partition.id}" / "data.csv"

    def _write_scope(self, context: IOContext, partition: Partition | None, data: Any) -> None:
        """Write one scope's data as CSV (converted to records through its representation)."""
        rows = Representation.of(data).to_records(data)
        self._write_csv(self._scope_path(context, partition), rows)

    def _read_scope(self, context: IOContext, partition: Partition | None) -> list[dict[str, Any]]:
        """Read one scope's CSV file.

        Returns:
            Rows as a list of dicts (typed via the context schema when set).
        """
        return self._read_csv(self._scope_path(context, partition), context)

    def _write_csv(self, file_path: Path, data: list[dict[str, Any]]) -> None:
        """Write a list of row dicts to a CSV file."""
        file_path.parent.mkdir(parents=True, exist_ok=True)
        if not data:
            file_path.write_text("")
            return
        fieldnames = list(data[0].keys())
        with file_path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

    def _read_csv(self, file_path: Path, context: IOContext) -> list[dict[str, Any]]:
        """Read a CSV file and return a list of row dicts.

        When the context carries a schema, rows are reconciled against it to
        restore types (CSV reads everything back as strings); empty strings
        are treated as ``None``.

        Returns:
            Rows as a list of dicts.

        Raises:
            FileNotFoundError: If the CSV file does not exist.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"Data file not found: {file_path}")
        with file_path.open(newline="") as f:
            reader = csv.DictReader(f)
            rows: list[dict[str, Any]] = list(reader)
        if context.schema is not None:
            rows = [{k: (None if v == "" else v) for k, v in row.items()} for row in rows]
            rows = context.schema.reconcile(rows)
        return rows

    def partition_row_counts(self, context: IOContext) -> dict[str, int]:
        """Return row counts grouped by partition by scanning CSV files on disk."""
        assert context.asset.partitioning is not None
        column = context.asset.partitioning.column
        base = self._asset_path(context)

        counts: dict[str, int] = {}
        if not base.exists():
            return counts

        for entry in sorted(base.iterdir()):
            if entry.is_dir() and entry.name.startswith(f"{column}="):
                partition_value = entry.name.split("=", 1)[1]
                data_file = entry / "data.csv"
                if data_file.exists():
                    with data_file.open(newline="") as f:
                        counts[partition_value] = sum(1 for _ in csv.DictReader(f))
        return counts
