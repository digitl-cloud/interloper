"""SQL database destination implementation over SQLAlchemy Core."""

from __future__ import annotations

import datetime
import math
import threading
import warnings
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

from interloper.destination import IOContext, destination
from interloper.destination.database import DatabaseDestination, PartitionFilter
from interloper.errors import DataNotFoundError
from interloper.representation import Representation
from interloper.resource.fields import InputField
from interloper.schema import FieldSpec
from pydantic import PrivateAttr
from sqlalchemy import Column, MetaData, String, Table, and_, cast, func, inspect, select
from sqlalchemy.engine import Connection as SAConnection
from sqlalchemy.schema import CreateSchema
from sqlalchemy.sql.elements import ColumnElement

from interloper_sql.connection import SQLConnection
from interloper_sql.types import column_type

_BATCH_SIZE = 1000


@destination(
    key="sql_destination",
    name="SQL database",
    icon="icon:sql",
    tags=["Database"],
    maturity="alpha",
)
class SQLDestination(DatabaseDestination):
    """SQL database destination.

    Each asset is a table; its dataset is the SQL schema holding it. A table
    is created from the effective schema on first write and never altered
    afterwards.
    """

    connection: SQLConnection

    default_dataset: str | None = InputField(default=None, description="Default schema for assets without a dataset")

    # Keyed by thread because one instance serves every asset of a run, and
    # each write runs whole on its own worker thread: a single slot would let
    # concurrent writes swap each other's transaction. A plain dict, unlike
    # threading.local, still deep-copies and pickles.
    _held: dict[int, SAConnection] = PrivateAttr(default_factory=dict)

    # -- Helpers ---------------------------------------------------------------

    def _resolve_dataset(self, dataset: str | None) -> str | None:
        """Return the schema to use.

        Args:
            dataset: The asset's dataset, or ``None`` to fall back to ``default_dataset``.

        Returns:
            The schema name, or ``None`` for the connection's default schema,
            which every SQL database has.
        """
        return dataset or self.default_dataset

    @contextmanager
    def _begin(self) -> Iterator[SAConnection]:
        """Yield the connection held by :meth:`transaction`, or a fresh one in its own transaction.

        Yields:
            A connection inside a transaction.
        """
        held = self._held.get(threading.get_ident())
        if held is not None:
            yield held
            return
        with self.connection.engine.begin() as conn:
            yield conn

    @staticmethod
    def _table(table: str, dataset: str | None, conn: SAConnection) -> Table | None:
        """Reflect an existing table.

        Args:
            table: Table name.
            dataset: The resolved schema, or ``None`` for the default schema.
            conn: The connection to reflect through.

        Returns:
            The reflected table, or ``None`` if it does not exist.
        """
        if not inspect(conn).has_table(table, schema=dataset):
            return None
        return Table(table, MetaData(), schema=dataset, autoload_with=conn)

    @staticmethod
    def _needs_schema(dataset: str | None, conn: SAConnection) -> bool:
        """Whether the schema must be created before a table can go in it.

        Args:
            dataset: The resolved schema, or ``None`` for the default schema.
            conn: The connection to inspect through.

        Returns:
            True when a schema is named, the dialect has schemas, and it does not exist yet.
        """
        if dataset is None or not getattr(conn.dialect, "supports_schemas", False):
            return False
        return not inspect(conn).has_schema(dataset)

    def _create_table(self, table: str, dataset: str | None, specs: Sequence[FieldSpec], conn: SAConnection) -> Table:
        """Create a table from field specs, creating its schema first when missing.

        Args:
            table: Table name.
            dataset: The resolved schema, or ``None`` for the default schema.
            specs: The field specs the columns are built from.
            conn: The connection to create through.

        Returns:
            The created table.
        """
        if dataset is not None and self._needs_schema(dataset, conn):
            conn.execute(CreateSchema(dataset, if_not_exists=True))
        columns = [Column(spec.name, column_type(spec), nullable=True) for spec in specs]
        sql_table = Table(table, MetaData(), *columns, schema=dataset)
        sql_table.create(conn, checkfirst=True)
        return sql_table

    @staticmethod
    def _ref(table: str, dataset: str | None) -> str:
        """Name a table for messages.

        Args:
            table: Table name.
            dataset: The resolved schema, or ``None`` for the default schema.

        Returns:
            ``schema.table``, or ``table`` alone in the default schema.
        """
        return f"{dataset}.{table}" if dataset else table

    def _not_found(self, table: str, dataset: str | None) -> str:
        """Word the error a read raises on a table that does not exist.

        Args:
            table: Table name.
            dataset: The resolved schema, or ``None`` for the default schema.

        Returns:
            The error message.
        """
        return f"Table '{self._ref(table, dataset)}' does not exist. Has the asset been materialized?"

    # -- DatabaseDestination hooks ---------------------------------------------

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Run a delete and its insert in one database transaction.

        Yields:
            ``None``; the hooks called inside the block share its connection.
        """
        with self.connection.engine.begin() as conn:
            self._held[threading.get_ident()] = conn
            try:
                yield
            finally:
                del self._held[threading.get_ident()]

    def insert(self, table: str, dataset: str | None, data: Any, context: IOContext) -> None:
        """Insert rows, creating the table from the effective schema on first write.

        Without a schema on the context, one is inferred from the data so the
        table is still typed. Columns the table does not have are dropped with
        a warning, and non-finite floats are written as ``NULL``.

        Args:
            table: Target table name.
            dataset: The asset's schema, or ``None`` for ``default_dataset``.
            data: The data in its native representation.
            context: IO context carrying the effective schema.
        """
        dataset = self._resolve_dataset(dataset)
        view = Representation.of(data)
        with self._begin() as conn:
            sql_table = self._table(table, dataset, conn)
            if sql_table is None:
                schema = context.schema if context.schema is not None else view.infer()
                sql_table = self._create_table(table, dataset, schema.field_specs(), conn)
            rows = _align(view.records, [c.name for c in sql_table.columns], self._ref(table, dataset))
            for start in range(0, len(rows), _BATCH_SIZE):
                conn.execute(sql_table.insert(), rows[start : start + _BATCH_SIZE])

    def delete(self, table: str, dataset: str | None, where: PartitionFilter | None) -> None:
        """Delete the rows a filter selects, or every row.

        A table that does not exist has nothing to delete.

        Args:
            table: Target table name.
            dataset: The asset's schema, or ``None`` for ``default_dataset``.
            where: The rows to delete; ``None`` for the whole table.
        """
        dataset = self._resolve_dataset(dataset)
        with self._begin() as conn:
            sql_table = self._table(table, dataset, conn)
            if sql_table is None:
                return
            statement = sql_table.delete()
            if where is not None:
                statement = statement.where(_predicate(sql_table, where))
            conn.execute(statement)

    def select(self, table: str, dataset: str | None, where: PartitionFilter | None) -> list[dict[str, Any]]:
        """Select the rows a filter selects, or every row, as records.

        Args:
            table: Target table name.
            dataset: The asset's schema, or ``None`` for ``default_dataset``.
            where: The rows to select; ``None`` for the whole table.

        Returns:
            The selected rows.

        Raises:
            DataNotFoundError: If the table does not exist yet.
        """
        dataset = self._resolve_dataset(dataset)
        with self._begin() as conn:
            sql_table = self._table(table, dataset, conn)
            if sql_table is None:
                raise DataNotFoundError(self._not_found(table, dataset))
            statement = select(sql_table)
            if where is not None:
                statement = statement.where(_predicate(sql_table, where))
            return [dict(row._mapping) for row in conn.execute(statement)]

    def count(self, table: str, dataset: str | None, column: str) -> dict[str, int]:
        """Return row counts grouped by a column's values, cast to strings.

        Args:
            table: Target table name.
            dataset: The asset's schema, or ``None`` for ``default_dataset``.
            column: Column to group by.

        Returns:
            Mapping from partition value (as string) to row count.

        Raises:
            DataNotFoundError: If the table does not exist.
        """
        dataset = self._resolve_dataset(dataset)
        with self._begin() as conn:
            sql_table = self._table(table, dataset, conn)
            if sql_table is None:
                raise DataNotFoundError(self._not_found(table, dataset))
            value = cast(sql_table.c[column], String).label("partition_value")
            statement = select(value, func.count().label("cnt")).group_by(value)
            return {row.partition_value: row.cnt for row in conn.execute(statement)}


# -- Utility functions ---------------------------------------------------------


def _align(records: list[dict[str, Any]], columns: list[str], ref: str) -> list[dict[str, Any]]:
    """Shape records to the table's columns for one executemany.

    Every row gets the same keys, the table columns any record carries, so
    a column no record mentions keeps its server default.

    Args:
        records: The rows to write.
        columns: The table's column names, in table order.
        ref: The table's name, for the warning.

    Returns:
        The aligned rows, non-finite floats replaced by ``None``.
    """
    present = dict.fromkeys(key for record in records for key in record)
    extras = [str(key) for key in present if key not in columns]
    if extras:
        warnings.warn(
            f"Columns {extras} are not in the schema for '{ref}' and will not be written.",
            UserWarning,
            stacklevel=3,
        )
    keys = [name for name in columns if name in present]
    return [{key: _finite(record.get(key)) for key in keys} for record in records]


def _finite(value: Any) -> Any:
    """Replace a NaN or infinite float with ``None``, which no SQL column type rejects.

    Args:
        value: A cell value.

    Returns:
        ``None`` for a non-finite float, else the value unchanged.
    """
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _predicate(table: Table, where: PartitionFilter) -> ColumnElement[bool]:
    """Render a partition filter against a reflected table.

    Args:
        table: The reflected table.
        where: The filter: equality on ``value``, or half-open ``bounds``.

    Returns:
        The ``WHERE`` clause.
    """
    column = table.c[where.column]
    if where.bounds is not None:
        start, end = where.bounds
        return and_(column >= _coerce(column, start), column < _coerce(column, end))
    return column == _coerce(column, where.value)


def _coerce(column: Column[Any], value: Any) -> Any:
    """Coerce a filter value to the Python type a date or timestamp column binds.

    A partition id arrives as a string (``"2024-01-01"``) and a day's bounds
    as dates; bound as-is against a ``DATE`` or ``TIMESTAMP`` column, they
    would be sent as ``VARCHAR`` or rejected by the dialect's type.

    Args:
        column: The column the value is compared with.
        value: The filter value.

    Returns:
        The value as a ``date`` or ``datetime`` when the column holds one, else unchanged.
    """
    try:
        python_type = column.type.python_type
    except NotImplementedError:
        return value
    if python_type is datetime.datetime:
        if isinstance(value, str):
            return datetime.datetime.fromisoformat(value)
        if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
            return datetime.datetime.combine(value, datetime.time())
    elif python_type is datetime.date and isinstance(value, str):
        return datetime.date.fromisoformat(value)
    return value
