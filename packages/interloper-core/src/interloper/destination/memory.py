"""In-memory destination backed by a class-level dict, mainly useful for testing."""

from __future__ import annotations

from typing import Any, ClassVar

from interloper.destination.context import IOContext
from interloper.destination.decorator import destination
from interloper.destination.partitioned import PartitionedDestination
from interloper.errors import DataNotFoundError
from interloper.partitioning.base import Partition, PartitionConfig


@destination(name="Memory")
class MemoryDestination(PartitionedDestination):
    """Destination that stores data in a class-level dict keyed by ``{dataset}/{table}/{partition}``.

    All instances share a single ``_storage`` dict so data written by one
    asset is visible to others.  The partition dispatch (including window
    splitting) comes from :class:`PartitionedDestination`.  Call
    :meth:`clear` between test runs.
    """

    _storage: ClassVar[dict[str, Any]] = {}

    def _write_scope(self, context: IOContext, partition: Partition | None, data: Any) -> None:
        """Store one scope's data under its path-style key."""
        self._storage[self._scope_key(context, partition)] = data

    def _read_scope(self, context: IOContext, partition: Partition | None) -> Any:
        """Retrieve one scope's data.

        Returns:
            The stored data.

        Raises:
            DataNotFoundError: If no data exists for the resolved key.
        """
        key = self._scope_key(context, partition)
        if key not in self._storage:
            raise DataNotFoundError(f"No data found in memory for: {key}")
        return self._storage[key]

    def _scope_key(self, context: IOContext, partition: Partition | None) -> str:
        """Build the storage key for a scope.

        Returns:
            The constructed storage key string.
        """
        return self._build_key(context.asset.table, context.asset.dataset, context.asset.partitioning, partition)

    def _build_key(
        self,
        name: str,
        dataset: str,
        partitioning: PartitionConfig | None,
        partition: Partition | None,
    ) -> str:
        """Build a ``/``-joined storage key from the asset identity and partition.

        Returns:
            The constructed storage key string.
        """
        parts = []
        if dataset:
            parts.append(dataset)
        parts.append(name)

        if partitioning is not None and partition is not None:
            parts.append(f"{partitioning.column}={partition.id}")

        return "/".join(parts)

    def partition_row_counts(self, context: IOContext) -> dict[str, int]:
        """Return row counts grouped by partition from in-memory storage."""
        assert context.asset.partitioning is not None
        column = context.asset.partitioning.column
        prefix = self._build_key(context.asset.table, context.asset.dataset, None, None)
        partition_prefix = f"{prefix}/{column}="

        counts: dict[str, int] = {}
        for key, data in self._storage.items():
            if key.startswith(partition_prefix):
                partition_value = key[len(partition_prefix) :]
                counts[partition_value] = len(data) if isinstance(data, list) else 1
        return counts

    @classmethod
    def clear(cls) -> None:
        """Clear all stored data (useful for testing)."""
        cls._storage.clear()
