"""DuckDB destination: tables in a DuckDB file or a MotherDuck database."""

from __future__ import annotations

import datetime
import threading
import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import duckdb
import pandas as pd
from interloper.destination import IOContext, destination
from interloper.destination.database import DatabaseDestination, PartitionFilter
from interloper.errors import DataNotFoundError
from interloper.representation import Representation
from interloper.resource.fields import InputField
from interloper.schema import FieldSpec
from pydantic import PrivateAttr

from interloper_duckdb.connection import DuckDBConnection
from interloper_duckdb.types import column_type

DEFAULT_SCHEMA = "main"

_BATCH = "interloper_batch"

# DuckDB reports two overlapping creates of one schema or table as a
# write-write conflict even with IF NOT EXISTS, and table creation is rare.
_DDL_LOCK = threading.Lock()


@dataclass
class _Transaction:
    """One write's transaction: the cursor it runs on, and whether it has begun.

    Attributes:
        cursor: The cursor every statement of the write runs on.
        begun: Whether ``BEGIN TRANSACTION`` has been issued on it.
    """

    cursor: duckdb.DuckDBPyConnection
    begun: bool = False


@destination(
    key="duckdb_destination",
    name="DuckDB",
    icon="icon:duckdb",
    tags=["Database"],
    maturity="alpha",
)
class DuckDBDestination(DatabaseDestination):
    """DuckDB destination.

    A dataset is a DuckDB schema: the asset's dataset, else ``default_dataset``,
    else ``main``. Tables are created on first write with typed columns and
    are never altered afterwards.
    """

    connection: DuckDBConnection

    default_dataset: str | None = InputField(default=None, description="Default schema for assets without a dataset")

    _transactions: dict[int, _Transaction] = PrivateAttr(default_factory=dict)

    # -- Helpers ---------------------------------------------------------------

    def _schema(self, dataset: str | None) -> str:
        """Return the schema a dataset resolves to.

        Args:
            dataset: The asset's dataset, or ``None`` to fall back to the destination's default.

        Returns:
            The schema name.
        """
        return dataset or self.default_dataset or DEFAULT_SCHEMA

    def _ref(self, table: str, dataset: str | None) -> str:
        """Build the quoted, schema-qualified table reference.

        Args:
            table: Table name.
            dataset: The schema, or ``None`` for the destination's default.

        Returns:
            ``"schema"."table"``.
        """
        return f"{_quote(self._schema(dataset))}.{_quote(table)}"

    @contextmanager
    def _cursor(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Yield the cursor an operation runs on.

        Inside :meth:`transaction` that is the thread's transaction cursor, so
        the delete and the insert commit together; otherwise a fresh cursor,
        closed after use.

        Yields:
            The cursor.
        """
        held = self._transactions.get(threading.get_ident())
        if held is not None:
            yield held.cursor
            return
        cursor = self.connection.client.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def _begin(self) -> None:
        """Begin the calling thread's transaction, if it holds one that has not begun.

        Called right before the first data change rather than on entering
        :meth:`transaction`: a DuckDB transaction reads the catalog as of its
        first statement, so a table created before it begins is visible to it.
        """
        held = self._transactions.get(threading.get_ident())
        if held is not None and not held.begun:
            held.cursor.execute("BEGIN TRANSACTION")
            held.begun = True

    def _columns(self, cursor: duckdb.DuckDBPyConnection, table: str, dataset: str | None) -> dict[str, str]:
        """Read a table's columns and their types.

        Args:
            cursor: The cursor to query through.
            table: Table name.
            dataset: The schema, or ``None`` for the destination's default.

        Returns:
            Column name to DuckDB type in table order, empty when the table does not exist.
        """
        rows = cursor.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_catalog = current_database() AND table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position",
            [self._schema(dataset), table],
        ).fetchall()
        return dict(rows)

    def _create_table(
        self, cursor: duckdb.DuckDBPyConnection, table: str, dataset: str | None, specs: list[FieldSpec]
    ) -> None:
        """Create the schema and the table, unless they already exist.

        Runs before the write's transaction begins, each statement committing
        on its own, and one creation at a time in the process, so concurrent
        assets writing to a new schema (or partitions to a new table) never
        race on it.

        Args:
            cursor: The cursor to run the statements on, outside a transaction.
            table: Table name.
            dataset: The schema, or ``None`` for the destination's default.
            specs: The table's field specs.
        """
        columns = ", ".join(
            f"{_quote(spec.name)} {column_type(spec)}{'' if spec.nullable else ' NOT NULL'}" for spec in specs
        )
        statements = (
            f"CREATE SCHEMA IF NOT EXISTS {_quote(self._schema(dataset))}",
            f"CREATE TABLE IF NOT EXISTS {self._ref(table, dataset)} ({columns})",
        )
        with _DDL_LOCK:
            for statement in statements:
                cursor.execute(statement)

    def _predicate(self, where: PartitionFilter, types: dict[str, str]) -> tuple[str, list[Any]]:
        """Render a partition filter as a parameterised predicate.

        Each parameter is cast to the column's type, so a partition id that
        arrives as a string compares against a ``DATE`` or ``BIGINT`` column.
        Against a ``VARCHAR`` column, date and datetime bounds are rendered in
        ISO 8601 (``T`` separator), the form the rows carry, since DuckDB's
        own cast to text separates with a space and would compare out of order.

        Args:
            where: The filter to render.
            types: The table's column types.

        Returns:
            The predicate text and its parameters.
        """
        column = _quote(where.column)
        column_type = types.get(where.column)
        placeholder = f"CAST(? AS {column_type})" if column_type is not None else "?"
        if where.bounds is None:
            return f"{column} = {placeholder}", [where.value]
        start, end = where.bounds
        if column_type == "VARCHAR":
            start, end = (_iso(bound) for bound in (start, end))
        return f"{column} >= {placeholder} AND {column} < {placeholder}", [start, end]

    # -- DatabaseDestination hooks ---------------------------------------------

    def insert(self, table: str, dataset: str | None, data: Any, context: IOContext) -> None:
        """Insert data through a registered DataFrame, creating the table on first write.

        A missing table is created from the effective schema (declared on the
        asset, or inferred during conform), else from a schema inferred from
        the data here, so the table is always typed. Columns the table does
        not have are dropped with a warning, and the rest are inserted by name.

        Args:
            table: Target table name.
            dataset: The schema, or ``None`` for the destination's default.
            data: The data in its native representation.
            context: IO context carrying the asset and effective schema.

        Raises:
            RuntimeError: If the table is still missing after creating it.
        """
        with self._cursor() as cursor:
            columns = self._columns(cursor, table, dataset)
            if not columns:
                schema = context.schema or Representation.of(data).infer()
                self._create_table(cursor, table, dataset, schema.field_specs())
                columns = self._columns(cursor, table, dataset)
            if not columns:
                raise RuntimeError(f"Table '{self._schema(dataset)}.{table}' could not be created.")

            ref = self._ref(table, dataset)
            frame = Representation.of(data).to("dataframe")
            extras = [str(c) for c in frame.columns if str(c) not in columns]
            if extras:
                warnings.warn(
                    f"Columns {extras} are not in the schema for '{self._schema(dataset)}.{table}' "
                    "and will not be written.",
                    UserWarning,
                    stacklevel=2,
                )
            present = ", ".join(_quote(c) for c in columns if c in frame.columns)
            if not present:
                return
            self._begin()
            cursor.register(_BATCH, frame)
            try:
                cursor.execute(f"INSERT INTO {ref} ({present}) SELECT {present} FROM {_BATCH}")
            finally:
                cursor.unregister(_BATCH)

    def delete(self, table: str, dataset: str | None, where: PartitionFilter | None) -> None:
        """Delete the rows a filter selects, or every row.

        A table that does not exist has nothing to delete.

        Args:
            table: Target table name.
            dataset: The schema, or ``None`` for the destination's default.
            where: The rows to delete; ``None`` for the whole table.
        """
        with self._cursor() as cursor:
            types = self._columns(cursor, table, dataset)
            if not types:
                return
            self._begin()
            ref = self._ref(table, dataset)
            if where is None:
                cursor.execute(f"DELETE FROM {ref}")
                return
            predicate, parameters = self._predicate(where, types)
            cursor.execute(f"DELETE FROM {ref} WHERE {predicate}", parameters)

    def select(self, table: str, dataset: str | None, where: PartitionFilter | None) -> pd.DataFrame:
        """Select the rows a filter selects, or every row, as a DataFrame.

        Args:
            table: Target table name.
            dataset: The schema, or ``None`` for the destination's default.
            where: The rows to select; ``None`` for the whole table.

        Returns:
            The selected rows.

        Raises:
            DataNotFoundError: If the table does not exist yet.
        """
        with self._cursor() as cursor:
            types = self._columns(cursor, table, dataset)
            if not types:
                raise DataNotFoundError(
                    f"Table '{self._schema(dataset)}.{table}' does not exist. Has the asset been materialized?"
                )
            ref = self._ref(table, dataset)
            if where is None:
                return cursor.execute(f"SELECT * FROM {ref}").df()
            predicate, parameters = self._predicate(where, types)
            return cursor.execute(f"SELECT * FROM {ref} WHERE {predicate}", parameters).df()

    def count(self, table: str, dataset: str | None, column: str) -> dict[str, int]:
        """Return row counts grouped by a column.

        Args:
            table: Target table name.
            dataset: The schema, or ``None`` for the destination's default.
            column: Column to group by.

        Returns:
            Mapping from the column's value (as a string) to its row count.

        Raises:
            DataNotFoundError: If the table does not exist.
        """
        with self._cursor() as cursor:
            if not self._columns(cursor, table, dataset):
                raise DataNotFoundError(
                    f"Table '{self._schema(dataset)}.{table}' does not exist. Has the asset been materialized?"
                )
            rows = cursor.execute(
                f"SELECT CAST({_quote(column)} AS VARCHAR) AS partition_value, COUNT(*) AS cnt "
                f"FROM {self._ref(table, dataset)} GROUP BY 1"
            ).fetchall()
        return dict(rows)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Run one write, a delete followed by an insert, as one DuckDB transaction.

        The transaction lives on one cursor held for the calling thread, which
        the hooks pick up through :meth:`_cursor`; the engine runs a whole
        write on one thread, and concurrent writes each hold their own. It
        begins at the first data change (see :meth:`_begin`), so creating a
        missing table is not part of it and survives a rollback.

        Yields:
            ``None``; the write runs inside the block, committed on success
            and rolled back on any exception.
        """
        ident = threading.get_ident()
        held = _Transaction(self.connection.client.cursor())
        self._transactions[ident] = held
        try:
            yield
        except BaseException:
            if held.begun:
                held.cursor.execute("ROLLBACK")
            raise
        else:
            if held.begun:
                held.cursor.execute("COMMIT")
        finally:
            del self._transactions[ident]
            held.cursor.close()


def _iso(value: Any) -> Any:
    """Render a date or datetime as ISO 8601, leaving any other value as is.

    Args:
        value: A partition bound.

    Returns:
        The ISO string for a date or datetime, else *value*.
    """
    return value.isoformat() if isinstance(value, datetime.date) else value


def _quote(identifier: str) -> str:
    """Quote an identifier for DuckDB, escaping embedded double quotes.

    Args:
        identifier: A schema, table or column name.

    Returns:
        The double-quoted identifier.
    """
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'
