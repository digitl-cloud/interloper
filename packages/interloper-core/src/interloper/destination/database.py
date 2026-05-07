"""Abstract base class for database-backed destination implementations."""

from __future__ import annotations

import warnings
from abc import abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from enum import Enum
from typing import Any

from interloper.destination.adapter import DataAdapter
from interloper.destination.base import Destination
from interloper.destination.context import IOContext
from interloper.errors import AdapterError
from interloper.partitioning.base import Partition, PartitionWindow


class WriteDisposition(str, Enum):
    """How a write operation should behave relative to existing data.

    Members:
        REPLACE: Delete existing rows (scoped to the active partition when
            partitioned) before inserting new data.
        APPEND: Insert new rows without touching existing data.
    """

    REPLACE = "replace"
    APPEND = "append"


class DatabaseDestination(Destination):
    """Abstract base class for database-backed destination implementations.

    Provides the partition-aware write/read dispatch logic that is common to any
    database backend (SQL, NoSQL, data-warehouse, etc.).  Subclasses only need to
    implement a small set of abstract hooks for the actual database operations.

    The target table name and schema are derived from the asset at call time
    (``asset.id`` -> table, ``asset.dataset`` -> schema) and passed as
    parameters to every hook.  The destination instance itself holds **no** table
    identity and can be safely shared across multiple assets.

    One or more :class:`~interloper.destination.adapter.DataAdapter` instances can be
    provided to convert between the asset's data type (e.g. a DataFrame) and
    the universal ``list[dict]`` row format used internally by every database
    hook.  When multiple adapters are configured, writes try each in order
    until one succeeds; reads use the first adapter.
    """

    write_disposition: WriteDisposition = WriteDisposition.REPLACE
    chunk_size: int = 1000

    # ------------------------------------------------------------------
    # Transaction hook
    # ------------------------------------------------------------------

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        """Context manager wrapping write operations.

        Override to provide transactional guarantees (e.g. SQL
        ``BEGIN ... COMMIT``).  The default implementation is a no-op.
        """
        yield

    # ------------------------------------------------------------------
    # Abstract database operations
    # ------------------------------------------------------------------

    @abstractmethod
    def _insert(self, table: str, schema: str | None, rows: list[dict[str, Any]]) -> None:
        """Insert rows into the target table."""

    @abstractmethod
    def _delete_all(self, table: str, schema: str | None) -> None:
        """Delete all rows from the target table."""

    @abstractmethod
    def _delete_partition(self, table: str, schema: str | None, column: str, value: Any) -> None:
        """Delete rows matching a single partition value."""

    @abstractmethod
    def _select_all(self, table: str, schema: str | None) -> list[dict[str, Any]]:
        """Select all rows from the target table."""

    @abstractmethod
    def _select_partition(
        self,
        table: str,
        schema: str | None,
        column: str,
        value: Any,
    ) -> list[dict[str, Any]]:
        """Select rows matching a single partition value."""

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @abstractmethod
    def _count_by_partition(
        self,
        table: str,
        schema: str | None,
        column: str,
    ) -> dict[str, int]:
        """Return row counts grouped by the values of the given column."""

    def partition_row_counts(self, context: IOContext) -> dict[str, int]:
        """Return row counts grouped by the asset's partition column."""
        assert context.asset.partitioning is not None
        return self._count_by_partition(
            type(context.asset).key,
            context.asset.dataset or None,
            context.asset.partitioning.column,
        )

    # ------------------------------------------------------------------
    # Data conversion
    # ------------------------------------------------------------------

    @property
    def adapters(self) -> list[DataAdapter]:
        """Return the list of adapters for this destination."""
        return []

    def _to_rows(self, data: Any) -> list[dict[str, Any]]:
        """Convert input data to a list of row dicts.

        When adapters are configured, tries each in order until one succeeds.
        Falls back to accepting ``list[dict]`` directly if no adapter handles
        the data (or if no adapters are configured).

        Returns:
            Data as list of dicts.

        Raises:
            AdapterError: If the data type is not supported.
        """
        if self.adapters is not None:
            for adapter in self.adapters:
                try:
                    return adapter.to_rows(data)
                except AdapterError:
                    continue
        if isinstance(data, list):
            return data
        configured = ", ".join(type(a).__name__ for a in self.adapters) if self.adapters else "none"
        raise AdapterError(
            f"No adapter on {type(self).__name__} could handle {type(data).__name__} "
            f"(configured: [{configured}]). "
            f"Either pass list[dict] or configure a suitable DataAdapter."
        )

    def _from_rows(self, rows: list[dict[str, Any]]) -> Any:
        """Convert database rows back to the configured data format.

        When adapters are configured, the first adapter is used.
        Otherwise returns the raw ``list[dict]``.

        Returns:
            Data in the first adapter's format, or raw ``list[dict]``.
        """
        if self.adapters:
            return self.adapters[0].from_rows(rows)
        return rows

    # ------------------------------------------------------------------
    # Destination interface
    # ------------------------------------------------------------------

    def write(self, context: IOContext, data: Any) -> None:
        """Write data to the database table.

        With ``REPLACE``, deletes matching rows before inserting.
        With ``APPEND``, rows are inserted without any prior deletion.
        """
        table = type(context.asset).key
        schema = context.asset.dataset or None
        rows = self._to_rows(data)

        if not rows:
            return

        if context.partition_or_window is not None and context.asset.partitioning is not None:
            col = context.asset.partitioning.column
            if col not in rows[0]:
                warnings.warn(
                    f"Partition column '{col}' not found in data for asset "
                    f"'{type(context.asset).key}'. Columns present: {sorted(rows[0].keys())}. "
                    f"Downstream reads by partition will fail.",
                    UserWarning,
                    stacklevel=2,
                )

        replacing = self.write_disposition is WriteDisposition.REPLACE

        with self._transaction():
            if context.partition_or_window is None:
                if replacing:
                    self._delete_all(table, schema)
                self._insert(table, schema, rows)

            elif isinstance(context.partition_or_window, PartitionWindow):
                assert context.asset.partitioning
                col = context.asset.partitioning.column
                if replacing:
                    for partition in context.partition_or_window:
                        self._delete_partition(table, schema, col, partition.id)
                self._insert(table, schema, rows)

            else:
                assert isinstance(context.partition_or_window, Partition)
                assert context.asset.partitioning
                col = context.asset.partitioning.column
                if replacing:
                    self._delete_partition(table, schema, col, context.partition_or_window.id)
                self._insert(table, schema, rows)

    def read(self, context: IOContext) -> Any:
        """Read data from the database table.

        Returns:
            A list of results for partition windows, single result otherwise.
        """
        table = type(context.asset).key
        schema = context.asset.dataset or None

        if context.partition_or_window is None:
            return self._from_rows(self._select_all(table, schema))

        if isinstance(context.partition_or_window, PartitionWindow):
            assert context.asset.partitioning
            col = context.asset.partitioning.column
            return [
                self._from_rows(self._select_partition(table, schema, col, p.id)) for p in context.partition_or_window
            ]

        assert isinstance(context.partition_or_window, Partition)
        assert context.asset.partitioning
        col = context.asset.partitioning.column
        return self._from_rows(self._select_partition(table, schema, col, context.partition_or_window.id))
