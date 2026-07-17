"""Abstract base class for database-backed destination implementations."""

from __future__ import annotations

import warnings
from abc import abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from enum import Enum
from typing import Any, ClassVar

from pydantic import Field

from interloper.destination.context import IOContext
from interloper.destination.partitioned import PartitionedDestination
from interloper.normalizer import MaterializationStrategy
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
    (``asset.table`` -> table, ``asset.dataset`` -> schema) and passed as
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

    # Instance configuration: backends set a default via the @destination
    # decorator; users can override it per configured destination in the UI.
    materialization_strategy: MaterializationStrategy = Field(
        default=MaterializationStrategy.AUTO,
        title="Materialization Strategy",
        description=(
            "How strictly written data must match the effective schema: "
            "'auto' trusts the conformed data as-is, 'strict' validates it "
            "and fails on mismatch, 'reconcile' aligns columns and coerces "
            "values to the schema before writing."
        ),
    )

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
            context.asset.table,
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
        table = context.asset.table
        schema = context.asset.dataset or None

        if is_empty(data):
            return

        data = self._apply_materialization_strategy(data, context)

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

    def _apply_materialization_strategy(self, data: Any, context: IOContext) -> Any:
        """Enforce this backend's write-time schema strategy.

        A backend declares how strictly its physical types demand
        schema-shaped data: ``AUTO`` trusts the conformed data as-is,
        ``STRICT`` validates it against the effective schema (failing
        loudly), and ``RECONCILE`` aligns columns and coerces values — for
        backends whose typed load path rejects representations that lax
        validation lets through (e.g. ISO date strings against a DATE
        column). No-op when no effective schema was resolved.

        Returns:
            The data to write, coerced under ``RECONCILE``.
        """
        strategy = self.materialization_strategy
        if strategy is MaterializationStrategy.AUTO or context.schema is None:
            return data
        conformer = Representation.of(data).conformer
        if strategy is MaterializationStrategy.RECONCILE:
            return conformer.reconcile(data, context.schema)
        conformer.validate(data, context.schema, strict=True)
        return data

    def _read_scope(self, context: IOContext, partition: Partition | None) -> Any:
        """Load one scope from the database table.

        Returns:
            The scope's rows, materialized into the read representation.
        """
        table = context.asset.table
        schema = context.asset.dataset or None
        if partition is None:
            return self._from_rows(self._select_all(table, schema))
        assert context.asset.partitioning
        column = context.asset.partitioning.column
        return self._from_rows(self._select_partition(table, schema, column, partition.id))
