"""Abstract base class for database-backed destination implementations."""

from __future__ import annotations

import warnings
from abc import abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from enum import Enum
from typing import Any, ClassVar

from interloper.destination.context import IOContext
from interloper.destination.partitioned import PartitionedDestination
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.representation import REPRESENTATIONS, Representation
from interloper.utils.data import is_empty


class WriteDisposition(str, Enum):
    """How a write operation should behave relative to existing data.

    Members:
        REPLACE: Delete existing rows (scoped to the active partition when
            partitioned) before inserting new data.
        APPEND: Insert new rows without touching existing data.
    """

    REPLACE = "replace"
    APPEND = "append"


class DatabaseDestination(PartitionedDestination):
    """Abstract base class for database-backed destination implementations.

    Provides the partition-aware write/read dispatch logic that is common to any
    database backend (SQL, NoSQL, data-warehouse, etc.).  Subclasses only need to
    implement a small set of abstract hooks for the actual database operations.

    The target table name and schema are derived from the asset at call time
    (``asset.id`` -> table, ``asset.dataset`` -> schema) and passed as
    parameters to every hook.  The destination instance itself holds **no** table
    identity and can be safely shared across multiple assets.

    Data converts to the universal ``list[dict]`` row format through its
    registered :class:`~interloper.representation.Representation`.  Reads
    materialize rows into the representation named by ``read_representation``
    (``"rows"`` by default; e.g. ``"dataframe"`` for pandas-native backends).
    """

    # Backend traits, not instance configuration: class-level so they never
    # appear in the destination's config schema (the UI form) or its specs.
    write_disposition: ClassVar[WriteDisposition] = WriteDisposition.REPLACE
    read_representation: ClassVar[str] = "rows"

    # -- Transaction hook ------------------------------------------------------

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        """Context manager wrapping write operations.

        Override to provide transactional guarantees (e.g. SQL
        ``BEGIN ... COMMIT``).  The default implementation is a no-op.
        """
        yield

    # -- Abstract database operations ------------------------------------------

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

    # -- Introspection ---------------------------------------------------------

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

    # -- Data conversion -------------------------------------------------------

    def _from_rows(self, rows: list[dict[str, Any]]) -> Any:
        """Materialize database rows into the configured read representation.

        Returns:
            Data in the ``read_representation`` format.
        """
        return REPRESENTATIONS[self.read_representation].from_records(rows)

    # -- Destination interface -------------------------------------------------

    def _insert_data(self, table: str, schema: str | None, data: Any, context: IOContext) -> None:
        """Insert data in its native format.

        The default implementation views the data as records through its
        representation and delegates to :meth:`_insert`.  Columnar backends
        can override this to
        consume the data natively (e.g. a DataFrame straight into a Parquet
        load job) and use ``context.schema`` for typed loads.

        Args:
            table: Target table name.
            schema: Database schema.
            data: The data in its native format (DataFrame, list[dict], ...).
            context: IO context carrying the asset and effective schema.
        """
        rows = Representation.of(data).to_records(data)
        if rows:
            self._insert(table, schema, rows)

    def write(self, context: IOContext, data: Any) -> None:
        """Write data to the database table.

        Overrides the :class:`PartitionedDestination` template: database
        backends delete per scope but insert the whole batch once (rows
        carry the partition column), instead of storing per-partition
        slices.  With ``REPLACE``, matching rows are deleted before
        inserting; with ``APPEND``, rows are inserted without any prior
        deletion.
        """
        table = type(context.asset).key
        schema = context.asset.dataset or None

        if is_empty(data):
            return

        if context.partition_or_window is not None and context.asset.partitioning is not None:
            col = context.asset.partitioning.column
            columns = Representation.of(data).columns(data)
            if columns and col not in columns:
                warnings.warn(
                    f"Partition column '{col}' not found in data for asset "
                    f"'{type(context.asset).key}'. Columns present: {sorted(columns)}. "
                    f"Downstream reads by partition will fail.",
                    UserWarning,
                    stacklevel=2,
                )

        replacing = self.write_disposition is WriteDisposition.REPLACE

        with self._transaction():
            if context.partition_or_window is None:
                if replacing:
                    self._delete_all(table, schema)

            elif isinstance(context.partition_or_window, PartitionWindow):
                assert context.asset.partitioning
                col = context.asset.partitioning.column
                if replacing:
                    for partition in context.partition_or_window:
                        self._delete_partition(table, schema, col, partition.id)

            else:
                assert isinstance(context.partition_or_window, Partition)
                assert context.asset.partitioning
                col = context.asset.partitioning.column
                if replacing:
                    self._delete_partition(table, schema, col, context.partition_or_window.id)

            self._insert_data(table, schema, data, context)

    def _read_scope(self, context: IOContext, partition: Partition | None) -> Any:
        """Load one scope from the database table.

        Returns:
            The scope's rows, materialized into the read representation.
        """
        table = type(context.asset).key
        schema = context.asset.dataset or None
        if partition is None:
            return self._from_rows(self._select_all(table, schema))
        assert context.asset.partitioning
        column = context.asset.partitioning.column
        return self._from_rows(self._select_partition(table, schema, column, partition.id))
