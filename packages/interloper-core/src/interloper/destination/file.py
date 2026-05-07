"""Filesystem-backed destination using pickle serialization."""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any

from interloper.destination.base import Destination
from interloper.destination.context import IOContext
from interloper.destination.decorator import destination


@destination(name="File")
class FileDestination(Destination):
    """Destination that reads and writes pickle files on the local filesystem.

    Data is stored under ``{base_path}/{dataset}/{asset_key}/data.pkl``.
    """

    base_path: str = ""

    def _path(self, context: IOContext) -> Path:
        """Build the file path from the destination context.

        Returns:
            The resolved path to the data file.
        """
        dataset = context.asset.dataset or type(context.asset).key
        return Path(self.base_path) / dataset / type(context.asset).key / "data.pkl"

    def read(self, context: IOContext) -> Any:
        """Unpickle data from a file.

        Args:
            context: Destination context with asset, partition, and metadata.

        Returns:
            The deserialized data.

        Raises:
            FileNotFoundError: If the data file does not exist.
        """
        path = self._path(context)
        if not path.exists():
            raise FileNotFoundError(f"No data file for '{context.asset}': {path}")
        with path.open("rb") as f:
            return pickle.load(f)

    def write(self, context: IOContext, data: Any) -> None:
        """Pickle data to a file.

        Args:
            context: Destination context with asset, partition, and metadata.
            data: The data to write.
        """
        path = self._path(context)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as f:
            pickle.dump(data, f)

    def partition_row_counts(self, context: IOContext) -> dict[str, int]:
        """Return row counts grouped by partition from the filesystem.

        Scans partition subdirectories (``{column}={value}/data.pkl``),
        unpickles each file, and counts items (``len(data)`` for lists,
        ``1`` otherwise).

        Args:
            context: Destination context with asset and partition information.

        Returns:
            Mapping from partition value (as string) to row count.
        """
        assert context.asset.partitioning is not None
        column = context.asset.partitioning.column
        dataset = context.asset.dataset or type(context.asset).key
        base_path = Path(self.base_path) / dataset / type(context.asset).key

        counts: dict[str, int] = {}
        if not base_path.exists():
            return counts

        for entry in sorted(base_path.iterdir()):
            if entry.is_dir() and entry.name.startswith(f"{column}="):
                partition_value = entry.name.split("=", 1)[1]
                data_file = entry / "data.pkl"
                if data_file.exists():
                    with data_file.open("rb") as f:
                        data = pickle.load(f)
                    counts[partition_value] = len(data) if isinstance(data, list) else 1
        return counts
