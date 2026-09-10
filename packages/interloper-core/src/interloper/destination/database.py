"""Database-backed destinations: partitions are rows selected by a filter in a table."""

from __future__ import annotations

import warnings
from abc import abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from interloper.destination.base import Destination
from interloper.destination.context import IOContext
from interloper.errors import ConfigError
from interloper.partitioning.base import Partition
from interloper.partitioning.time import TimePartition
from interloper.representation import Representation
from interloper.utils.data import is_empty


@dataclass(frozen=True)
class PartitionFilter:
    """The rows one partition covers: a column, and either a value or half-open bounds.

    Exactly one of ``value`` and ``bounds`` is set. A backend renders it in its
    dialect: ``column = value``, or ``column >= start AND column < end``.

    Attributes:
        column: The partition column.
        value: The partition id the rows carry, for a partition matched by equality.
        bounds: The ``(start, end)`` of a time partition, ``end`` excluded.
    """

    column: str
    value: Any = None
    bounds: tuple[Any, Any] | None = None


class DatabaseDestination(Destination):
    """A destination whose partitions are rows selected by a filter in a table.

    A backend writes its SQL dialect and nothing else: :meth:`insert`,
    :meth:`delete`, :meth:`select` and :meth:`count`, each receiving the
    table and the dataset the asset resolves to and, where a partition is
    involved, a :class:`PartitionFilter` the base has already resolved
    (half-open bounds for a time partition, equality otherwise). The base
    owns replacing: a partition's rows are deleted before its data is
    inserted, a window in one batch. Reads come back in whatever
    representation the backend holds natively; a consumer that wants records
    asks :attr:`~interloper.asset.upstream.Upstream.records`.

    The destination instance holds no table identity and is shared across
    assets: ``asset.table`` and ``asset.dataset`` name the target at call time.
    """

    # -- Backend hooks ---------------------------------------------------------

    @abstractmethod
    def insert(self, table: str, dataset: str | None, data: Any, context: IOContext) -> None:
        """Insert data in its native representation, creating the table if the backend must.

        The one hook that receives the whole context: a table created on first
        write takes its columns from ``context.schema``, its partitioning and
        description from ``context.asset``. A row backend views the data as
        records through ``Representation.of(data).records``; a columnar one
        converts with ``Representation.of(data).to("dataframe")`` and loads
        natively.

        Args:
            table: Target table name.
            dataset: The dataset (database schema) holding the table, or ``None`` for the backend default.
            data: The data to insert (rows, a DataFrame, ...).
            context: IO context carrying the asset and the effective schema.
        """

    @abstractmethod
    def delete(self, table: str, dataset: str | None, where: PartitionFilter | None) -> None:
        """Delete the rows a filter selects, or every row.

        Args:
            table: Target table name.
            dataset: The dataset (database schema) holding the table, or ``None`` for the backend default.
            where: The rows to delete; ``None`` for the whole table.
        """

    @abstractmethod
    def select(self, table: str, dataset: str | None, where: PartitionFilter | None) -> Any:
        """Select the rows a filter selects, or every row, in the backend's native representation.

        Args:
            table: Target table name.
            dataset: The dataset (database schema) holding the table, or ``None`` for the backend default.
            where: The rows to select; ``None`` for the whole table.

        Returns:
            The rows, as whatever table type the backend produces natively.
        """

    @abstractmethod
    def count(self, table: str, dataset: str | None, column: str) -> dict[str, int]:
        """Count rows grouped by the values of a column.

        Args:
            table: Target table name.
            dataset: The dataset (database schema) holding the table, or ``None`` for the backend default.
            column: The column to group by.

        Returns:
            Each distinct value, as a string, to its row count.
        """

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Wrap one write, a delete followed by an insert.

        Override to make the pair atomic (``BEGIN ... COMMIT``); the default
        does nothing.

        Yields:
            ``None``; the write runs inside the block.
        """
        yield

    # -- Destination interface -------------------------------------------------

    def write(self, context: IOContext, data: Any) -> None:
        """Write data to the table, a window as one batch.

        Rows carry the partition column, so a database need not store per
        partition: a window deletes every partition it covers and inserts the
        whole batch once, instead of one :meth:`write_partition` per partition
        (one load job rather than one per day). A single partition, or the
        whole, is one :meth:`write_partition`. The write-time schema strategy
        is applied to the data once, before either path.

        Args:
            context: IO context carrying the target asset, the partition or window,
                and the effective schema.
            data: The data to write, in its native representation.
        """
        if is_empty(data):
            return
        self._warn_missing_partition_column(data, context)
        if not context.window:
            self.write_partition(context, context.partitions[0], data)
            return
        table, dataset = self._target(context)
        with self.transaction():
            for partition in context.partitions:
                self.delete(table, dataset, self._filter(context, partition))
            self.insert(table, dataset, data, context)

    def write_partition(self, context: IOContext, partition: Partition | None, data: Any) -> None:
        """Replace one partition's rows: delete them, then insert the data.

        Args:
            context: IO context carrying the target asset and the effective schema.
            partition: The partition being stored, or ``None`` for the whole table.
            data: The data to store, in its native representation.
        """
        table, dataset = self._target(context)
        with self.transaction():
            self.delete(table, dataset, self._filter(context, partition))
            self.insert(table, dataset, data, context)

    def read_partition(self, context: IOContext, partition: Partition | None) -> Any:
        """Load one partition from the table.

        Args:
            context: IO context whose asset supplies the table, dataset, and
                partition column.
            partition: The partition to load, or ``None`` for the whole table.

        Returns:
            The partition's rows, in the backend's native representation.
        """
        table, dataset = self._target(context)
        return self.select(table, dataset, self._filter(context, partition))

    def partition_row_counts(self, context: IOContext) -> dict[str, int]:
        """Return row counts grouped by the asset's partition column.

        Args:
            context: IO context whose asset supplies the table, dataset, and
                partition column.

        Returns:
            Each partition value, as a string, to its row count.
        """
        table, dataset = self._target(context)
        return self.count(table, dataset, self._partition_column(context))

    # -- Internals -------------------------------------------------------------

    @staticmethod
    def _target(context: IOContext) -> tuple[str, str | None]:
        """The table and dataset the context's asset resolves to.

        Args:
            context: IO context carrying the asset.

        Returns:
            The table name and the dataset, ``None`` when the asset has none.
        """
        return context.asset.table, context.asset.dataset or None

    @staticmethod
    def _partition_column(context: IOContext) -> str:
        """The asset's partition column, which a partitioned write or read guarantees exists.

        Args:
            context: IO context whose asset is partitioned.

        Returns:
            The partition column name.

        Raises:
            ConfigError: If the asset declares no partitioning.
        """
        if context.asset.partitioning is None:
            raise ConfigError(f"Asset '{context.asset.key}' is not partitioned")
        return context.asset.partitioning.column

    def _filter(self, context: IOContext, partition: Partition | None) -> PartitionFilter | None:
        """The rows one partition covers, resolved for a backend.

        A time partition's rows may carry values anywhere inside its period
        (a monthly partition whose rows hold daily dates), so equality on the
        period start would miss them; its filter is the half-open bounds.
        Any other partition matches its id.

        Args:
            context: IO context whose asset supplies the partition column.
            partition: The partition, or ``None`` for the whole table.

        Returns:
            The filter, or ``None`` for the whole table.
        """
        if partition is None:
            return None
        column = self._partition_column(context)
        if isinstance(partition, TimePartition):
            return PartitionFilter(column, bounds=partition.bounds)
        return PartitionFilter(column, value=partition.id)

    def _warn_missing_partition_column(self, data: Any, context: IOContext) -> None:
        """Warn when partitioned data lacks its partition column, since reads by partition would find nothing.

        Args:
            data: The data about to be written.
            context: IO context whose asset supplies the partition column.
        """
        if context.partition_or_window is None or context.asset.partitioning is None:
            return
        column = context.asset.partitioning.column
        columns = Representation.of(data).columns
        if columns and column not in columns:
            warnings.warn(
                f"Partition column '{column}' not found in data for asset "
                f"'{context.asset.key}'. Columns present: {sorted(columns)}. "
                f"Downstream reads by partition will fail.",
                UserWarning,
                stacklevel=3,
            )

