"""Filesystem-backed destination using CSV serialization."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from interloper.destination.base import Destination
from interloper.destination.context import IOContext
from interloper.destination.decorator import destination
from interloper.partitioning.base import Partition, PartitionWindow


@destination(name="CSV")
class CSVDestination(Destination):
    """Destination that reads and writes CSV files on the local filesystem.

    Data is stored under ``{base_path}/{dataset}/{asset_key}/data.csv``
    (or ``{base_path}/{asset_key}/data.csv`` when no dataset is set).
    Partitioned assets add a ``{column}={id}`` subdirectory.

    Data must be ``list[dict]`` — each dict represents a row, and the keys
    of the first dict determine the CSV column headers.
    """

    base_path: str = ""

    def _asset_path(self, context: IOContext) -> Path:
        """Return the base directory for an asset."""
        return Path(self.base_path) / (context.asset.dataset or "") / type(context.asset).key

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

    def _read_csv(self, file_path: Path) -> list[dict[str, str]]:
        """Read a CSV file and return a list of row dicts.

        Returns:
            Rows as a list of dicts.

        Raises:
            FileNotFoundError: If the CSV file does not exist.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"Data file not found: {file_path}")
        with file_path.open(newline="") as f:
            reader = csv.DictReader(f)
            return list(reader)

    def write(self, context: IOContext, data: list[dict[str, Any]]) -> None:
        """Write row data to a CSV file, creating partition subdirectories as needed."""
        base = self._asset_path(context)

        if context.partition_or_window is None:
            self._write_csv(base / "data.csv", data)

        elif isinstance(context.partition_or_window, PartitionWindow):
            assert context.asset.partitioning
            for partition in context.partition_or_window:
                partition_path = base / f"{context.asset.partitioning.column}={partition.id}"
                self._write_csv(partition_path / "data.csv", data)

        else:
            assert isinstance(context.partition_or_window, Partition)
            assert context.asset.partitioning
            partition_path = base / f"{context.asset.partitioning.column}={context.partition_or_window.id}"
            self._write_csv(partition_path / "data.csv", data)

    def read(self, context: IOContext) -> list[dict[str, str]] | list[list[dict[str, str]]]:
        """Read row data from a CSV file.

        Returns:
            A list of row dicts, or a list of lists for partition windows.
        """
        base = self._asset_path(context)

        if context.partition_or_window is None:
            return self._read_csv(base / "data.csv")

        elif isinstance(context.partition_or_window, PartitionWindow):
            assert context.asset.partitioning
            return [
                self._read_csv(base / f"{context.asset.partitioning.column}={p.id}" / "data.csv")
                for p in context.partition_or_window
            ]

        else:
            assert isinstance(context.partition_or_window, Partition)
            assert context.asset.partitioning
            return self._read_csv(
                base / f"{context.asset.partitioning.column}={context.partition_or_window.id}" / "data.csv"
            )

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
                    rows = self._read_csv(data_file)
                    counts[partition_value] = len(rows)
        return counts
