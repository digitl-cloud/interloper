"""Database-backed destinations: partitions are rows selected by a filter in a table."""

from __future__ import annotations

import warnings
from abc import abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, runtime_checkable

from interloper.destination.base import Destination
from interloper.destination.context import IOContext
from interloper.errors import ConfigError
from interloper.partitioning import PartitionFilter
from interloper.partitioning.base import Partition
from interloper.representation import Representation
from interloper.utils.data import is_empty

if TYPE_CHECKING:
    from interloper.asset.base import Asset
    from interloper.schema import Schema


@dataclass(frozen=True)
class TableReference:
    """A table a SQL destination can query, as a view builds on it.

    Attributes:
        name: The qualified table name, quoted the way this destination's
            dialect wants it, ready to drop into SQL text.
        parts: The unquoted path to the table, outermost first (a BigQuery
            project, dataset and table; a DuckDB schema and table), for a
            builder that composes references itself.
        schema: The asset's declared schema, ``None`` when it declares none.
    """

    name: str
    parts: tuple[str, ...]
    schema: type[Schema] | None


@runtime_checkable
class Compilable(Protocol):
    """A view definition that renders itself for a dialect."""

    def compile(self, dialect: str) -> str:
        """Render the definition as SQL text for one dialect.

        Args:
            dialect: The destination's dialect name.

        Returns:
            The SQL text of the view's query.
        """
        ...


class DatabaseDestination(Destination):
    """A destination whose partitions are rows selected by a filter in a table.

    A backend writes its SQL dialect and nothing else: `insert`,
    `delete`, `select` and `count`, each receiving the
    table and the dataset the asset resolves to and, where a partition is
    involved, a `PartitionFilter` the base has already resolved
    (as the partition renders it: bounds for a time partition, membership for a list one). The base
    owns replacing: a partition's rows are deleted before its data is
    inserted, a window in one batch. Reads come back in whatever
    representation the backend holds natively; a consumer that wants records
    asks `records`.

    Views are the SQL family's own: a `reference` is a
    `TableReference`, a definition is SQL text in this destination's
    ``dialect`` or a `Compilable` rendered for it, and `create_view`
    replaces the view under the asset's table. A backend supports views by
    naming its ``dialect`` and implementing `table_parts` and
    `replace_view`; `reaches` says which other destinations' tables a view
    here may query, the same instance by default.

    The destination instance holds no table identity and is shared across
    assets: ``asset.table`` and ``asset.dataset`` name the target at call time.
    """

    dialect: ClassVar[str | None] = None

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

    # -- Views -----------------------------------------------------------------

    def table_parts(self, table: str, dataset: str | None) -> tuple[str, ...]:
        """The unquoted path to a table, outermost first, such as ``(project, dataset, table)``.

        Args:
            table: Table name.
            dataset: The asset's dataset, or ``None`` for the destination's default.

        Raises:
            ConfigError: Always; this backend does not support views.
        """
        raise ConfigError(f"Destination '{self.key}' does not support views")

    def qualified_name(self, table: str, dataset: str | None) -> str:
        """The qualified table name, quoted for this dialect.

        Double-quotes every part, the SQL standard; a backend whose dialect
        quotes differently overrides this.

        Args:
            table: Table name.
            dataset: The asset's dataset, or ``None`` for the destination's default.

        Returns:
            The quoted, dot-joined path.
        """
        return ".".join('"' + part.replace('"', '""') + '"' for part in self.table_parts(table, dataset))

    def replace_view(self, table: str, dataset: str | None, sql: str) -> None:
        """Create or replace a view from a query.

        Args:
            table: The view's name.
            dataset: The asset's dataset, or ``None`` for the destination's default.
            sql: The query, in this destination's dialect.

        Raises:
            ConfigError: Always; this backend does not support views.
        """
        raise ConfigError(f"Destination '{self.key}' does not support views")

    def reaches(self, other: Destination) -> bool:
        """Whether a view in this destination may query tables in *other*.

        Decided from configuration alone, never a network call: the same
        instance by default (the same object, or the same non-empty ``id``;
        a destination whose ``id`` is empty is told apart by identity),
        the same warehouse and location where a backend knows better.

        Args:
            other: The destination holding an upstream's table.

        Returns:
            True when a query here can name tables there.
        """
        return other is self or (bool(self.id) and other.id == self.id)

    def reference(self, asset: Asset) -> TableReference:
        """The table an asset materializes to, as a view here may query it.

        The asset's ``default_destination_key`` destination is preferred,
        then the first one this destination `reaches`.

        Args:
            asset: The upstream asset.

        Returns:
            The table reference.

        Raises:
            ConfigError: If the asset has no destination, or none this
                destination reaches.
        """
        destinations: list[Destination] = asset.destinations
        if not destinations:
            raise ConfigError(f"Asset '{asset.qualified_key}' has no destination to reference")
        ordered = sorted(destinations, key=lambda candidate: candidate.key != asset.default_destination_key)
        for candidate in ordered:
            if isinstance(candidate, DatabaseDestination) and self.reaches(candidate):
                return TableReference(
                    name=candidate.qualified_name(asset.table, asset.dataset or None),
                    parts=candidate.table_parts(asset.table, asset.dataset or None),
                    schema=asset.schema,
                )
        raise ConfigError(
            f"Destination '{self.key}' cannot query asset '{asset.qualified_key}': "
            f"none of its destinations ({', '.join(d.key for d in destinations)}) is reachable"
        )

    def create_view(self, asset: Asset, definition: str | Compilable) -> None:
        """Replace the view under the asset's table with a definition.

        Args:
            asset: The view being materialized.
            definition: SQL text in this destination's dialect, or a
                `Compilable` rendered for it.

        Raises:
            ConfigError: If a `Compilable` is given and this destination
                declares no dialect.
        """
        if isinstance(definition, str):
            sql = definition
        elif self.dialect is None:
            raise ConfigError(f"Destination '{self.key}' declares no dialect to compile a view for")
        else:
            sql = definition.compile(self.dialect)
        self.replace_view(asset.table, asset.dataset or None, sql)

    # -- Destination interface -------------------------------------------------

    def write(self, context: IOContext, data: Any) -> None:
        """Replace every partition the context covers with the data, in one batch.

        Rows carry the partition column, so a database need not store per partition: the rows of
        every partition the window covers are deleted, then the data is inserted once (one load
        job rather than one per day). The unpartitioned whole is the same path with no filter.

        Args:
            context: IO context carrying the target asset, the window and the effective schema.
            data: The data to write, in its native representation.
        """
        if is_empty(data):
            return
        self._warn_missing_partition_column(data, context)
        table, dataset = self._target(context)
        with self.transaction():
            for partition in context.partitions:
                self.delete(table, dataset, self._filter(context, partition))
            self.insert(table, dataset, data, context)

    def write_partition(self, context: IOContext, partition: Partition | None, data: Any) -> None:
        """Replace one partition's rows: `write` over the partition's own window.

        Args:
            context: IO context carrying the target asset and the effective schema.
            partition: The partition being stored, or ``None`` for the whole table.
            data: The data to store, in its native representation.
        """
        self.write(replace(context, window=partition.window if partition is not None else None), data)

    def read(self, context: IOContext) -> Any:
        """Select the rows the context covers, a window as one bounded select.

        Args:
            context: IO context carrying the target asset, the window and the effective schema.

        Returns:
            The rows, in the backend's native representation.
        """
        table, dataset = self._target(context)
        where = None if context.window is None else context.window.filter(self._partition_column(context))
        return self.select(table, dataset, where)

    def read_partition(self, context: IOContext, partition: Partition | None) -> Any:
        """Load one partition: `read` over the partition's own window.

        Args:
            context: IO context whose asset supplies the table, dataset, and partition column.
            partition: The partition to load, or ``None`` for the whole table.

        Returns:
            The partition's rows, in the backend's native representation.
        """
        return self.read(replace(context, window=partition.window if partition is not None else None))

    def partition_row_counts(self, context: IOContext) -> dict[str, int]:
        """Return row counts grouped by the asset's partition column.

        Args:
            context: IO context whose asset supplies the table, dataset, and partition column.

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
        """The rows one partition covers, as the partition renders them.

        Args:
            context: IO context whose asset supplies the partition column.
            partition: The partition, or ``None`` for the whole table.

        Returns:
            The filter, or ``None`` for the whole table.
        """
        if partition is None:
            return None
        return partition.filter(self._partition_column(context))

    def _warn_missing_partition_column(self, data: Any, context: IOContext) -> None:
        """Warn when partitioned data lacks its partition column, since reads by partition would find nothing.

        Args:
            data: The data about to be written.
            context: IO context whose asset supplies the partition column.
        """
        if context.window is None or context.asset.partitioning is None:
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
