"""Microsoft Fabric Warehouse destination over the warehouse's SQL (TDS) endpoint."""

from __future__ import annotations

import datetime
import json
import math
import threading
import warnings
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import mssql_python
from interloper.destination import IOContext, destination
from interloper.destination.database import DatabaseDestination, PartitionFilter
from interloper.errors import DataNotFoundError
from interloper.representation import Representation
from interloper.resource.fields import FetchField, InputField
from interloper.schema import FieldSpec
from interloper.utils.json import json_default, replace_non_finite
from pydantic import PrivateAttr

from interloper_azure.connection import AzureConnection
from interloper_azure.fabric.types import column_type

DEFAULT_SCHEMA = "dbo"

# SQL Server caps a request at 2100 parameters; stay one under it, as
# SQLAlchemy's mssql dialect does.
MAX_PARAMETERS = 2099

# SQL Server's row cap for an INSERT ... VALUES list; Fabric documents none, so keep it.
MAX_ROWS = 1000

# Two writes creating the same schema, or partitions of one asset creating its
# table, would both pass the catalog check and the second CREATE would fail.
_DDL_LOCK = threading.Lock()


@dataclass
class _Transaction:
    """One write's transaction: the session it runs on, and whether it has begun.

    Attributes:
        session: The driver connection every statement of the write runs on.
        begun: Whether ``BEGIN TRANSACTION`` has been issued on it.
    """

    session: mssql_python.Connection
    begun: bool = False


@destination(
    key="fabric_warehouse_destination",
    name="Microsoft Fabric Warehouse",
    icon="icon:fabric",
    tags=["Cloud"],
)
class FabricWarehouseDestination(DatabaseDestination):
    """Microsoft Fabric Warehouse destination.

    A dataset is a schema of the warehouse: the asset's dataset, else
    ``default_dataset``, else ``dbo``. Tables are created on first write with
    typed columns and are never altered afterwards. Only a Warehouse accepts
    writes; a Lakehouse's SQL analytics endpoint is read-only.

    Statements go through Microsoft's ``mssql-python`` driver, signed in as
    the connection's service principal with a Microsoft Entra access token.
    """

    connection: AzureConnection

    server: str = FetchField(
        provider="connection.workspaces",
        label_key="name",
        value_key="server",
        label="Workspace",
        description="Workspace holding the warehouse",
        info=(
            "Stores the workspace's SQL connection string (…datawarehouse.fabric.microsoft.com), the one "
            "shown in the warehouse's settings; every warehouse of a workspace shares it."
        ),
    )
    warehouse: str = InputField(description="Warehouse name, as shown in the workspace", discriminator=True)
    default_dataset: str | None = InputField(
        default=None, description="Default schema for assets without a dataset; dbo when empty"
    )

    _transactions: dict[int, _Transaction] = PrivateAttr(default_factory=dict)

    # -- Session ---------------------------------------------------------------

    def connect(self) -> mssql_python.Connection:
        """Open a session on the warehouse, signed in as the connection's service principal.

        Autocommit stays on so a lone statement commits by itself; a write
        opens an explicit ``BEGIN TRANSACTION`` (see :meth:`transaction`). The
        driver takes its token from the connection's credential, which caches
        and renews it, so every new session signs in with a valid one.

        Returns:
            The new driver connection.
        """
        return mssql_python.connect(
            f"Server={_odbc(self.server)};Database={_odbc(self.warehouse)};Encrypt=yes;TrustServerCertificate=no",
            autocommit=True,
            token_provider=self.connection.credential,
        )

    @contextmanager
    def _session(self) -> Iterator[mssql_python.Connection]:
        """Yield the session an operation runs on.

        Inside :meth:`transaction` that is the thread's transaction session,
        so the delete and the insert commit together; otherwise a fresh
        session, closed after use.

        Yields:
            The driver connection.
        """
        held = self._transactions.get(threading.get_ident())
        if held is not None:
            yield held.session
            return
        session = self.connect()
        try:
            yield session
        finally:
            session.close()

    def _begin(self) -> None:
        """Begin the calling thread's transaction, if it holds one that has not begun.

        Called right before the first data change rather than on entering
        :meth:`transaction`, so a table that write creates is created, and
        committed, before the transaction opens. DDL inside a transaction is
        allowed by the warehouse but holds locks on its system catalog views
        until commit, blocking every other write's existence checks.
        """
        held = self._transactions.get(threading.get_ident())
        if held is not None and not held.begun:
            _run(held.session, "BEGIN TRANSACTION")
            held.begun = True

    # -- Naming ----------------------------------------------------------------

    def _schema(self, dataset: str | None) -> str:
        """Return the schema a dataset resolves to.

        Args:
            dataset: The asset's dataset, or ``None`` to fall back to the destination's default.

        Returns:
            The schema name.
        """
        return dataset or self.default_dataset or DEFAULT_SCHEMA

    @staticmethod
    def _ref(table: str, schema: str) -> str:
        """Build the quoted, schema-qualified table reference.

        Args:
            table: Table name.
            schema: The resolved schema.

        Returns:
            ``[schema].[table]``.
        """
        return f"{_quote(schema)}.{_quote(table)}"

    @staticmethod
    def _not_found(table: str, schema: str) -> str:
        """Word the error a read raises on a table that does not exist.

        Args:
            table: Table name.
            schema: The resolved schema.

        Returns:
            The error message.
        """
        return f"Table '{schema}.{table}' does not exist. Has the asset been materialized?"

    # -- Catalog ---------------------------------------------------------------

    @staticmethod
    def _columns(session: mssql_python.Connection, table: str, schema: str) -> list[str]:
        """Read a table's column names from the catalog views.

        The query reads catalog views only: the warehouse refuses a query
        that mixes system and user tables.

        Args:
            session: The session to query through.
            table: Table name.
            schema: The resolved schema.

        Returns:
            The column names in table order, empty when the table does not exist.
        """
        _, rows = _fetch(
            session,
            "SELECT c.name FROM sys.columns AS c "
            "JOIN sys.tables AS t ON c.object_id = t.object_id "
            "JOIN sys.schemas AS s ON t.schema_id = s.schema_id "
            "WHERE s.name = ? AND t.name = ? ORDER BY c.column_id",
            [schema, table],
        )
        return [row[0] for row in rows]

    def _create_table(
        self, session: mssql_python.Connection, table: str, schema: str, specs: Sequence[FieldSpec]
    ) -> None:
        """Create the schema and the table, unless they already exist.

        T-SQL has no ``CREATE SCHEMA IF NOT EXISTS``, so each creation is
        guarded by a catalog check, one creation at a time in the process.
        ``CREATE SCHEMA`` must be alone in its batch, so it runs as its own
        statement. Runs before the write's transaction begins, so each
        statement commits by itself.

        Args:
            session: The session to run the statements on, outside a transaction.
            table: Table name.
            schema: The resolved schema.
            specs: The table's field specs.
        """
        with _DDL_LOCK:
            if self._columns(session, table, schema):
                return
            _, found = _fetch(session, "SELECT 1 FROM sys.schemas WHERE name = ?", [schema])
            if not found:
                _run(session, f"CREATE SCHEMA {_quote(schema)}")
            _run(session, f"CREATE TABLE {self._ref(table, schema)} ({_columns_ddl(specs)})")

    # -- DatabaseDestination hooks ---------------------------------------------

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Run one write, a delete followed by an insert, as ``BEGIN ... COMMIT``.

        The transaction lives on one session held for the calling thread,
        which the hooks pick up through :meth:`_session`: one instance serves
        every asset of a run, each write running whole on its own worker
        thread, so concurrent writes each hold their own. It begins at the
        first data change (see :meth:`_begin`). The warehouse runs every
        transaction under snapshot isolation, so readers keep seeing the old
        rows until the commit.

        Yields:
            ``None``; the write runs inside the block, committed on success and
            rolled back on any exception.
        """
        ident = threading.get_ident()
        held = _Transaction(self.connect())
        self._transactions[ident] = held
        try:
            yield
        except BaseException:
            if held.begun:
                _run(held.session, "ROLLBACK TRANSACTION")
            raise
        else:
            if held.begun:
                _run(held.session, "COMMIT TRANSACTION")
        finally:
            del self._transactions[ident]
            held.session.close()

    def insert(self, table: str, dataset: str | None, data: Any, context: IOContext) -> None:
        """Insert rows in multi-row ``INSERT ... VALUES`` batches, creating the table on first write.

        A missing table is created from the effective schema (declared on the
        asset, or inferred during conform), else from a schema inferred from
        the data here, so the table is always typed. Columns the table does
        not have are dropped with a warning. Each batch carries as many rows
        as the parameter and row caps allow (see :func:`batch_size`).

        Args:
            table: Target table name.
            dataset: The schema, or ``None`` for the destination's default.
            data: The data in its native representation.
            context: IO context carrying the asset and effective schema.
        """
        schema = self._schema(dataset)
        view = Representation.of(data)
        with self._session() as session:
            columns = self._columns(session, table, schema)
            if not columns:
                specs = (context.schema or view.infer()).field_specs()
                self._create_table(session, table, schema, specs)
                columns = self._columns(session, table, schema)
            keys, rows = _align(view.records, columns, f"{schema}.{table}")
            if not keys or not rows:
                return
            self._begin()
            names = ", ".join(_quote(key) for key in keys)
            row = "(" + ", ".join("?" for _ in keys) + ")"
            size = batch_size(len(keys))
            for start in range(0, len(rows), size):
                batch = rows[start : start + size]
                _run(
                    session,
                    f"INSERT INTO {self._ref(table, schema)} ({names}) VALUES {', '.join(row for _ in batch)}",
                    [value for values in batch for value in values],
                )

    def delete(self, table: str, dataset: str | None, where: PartitionFilter | None) -> None:
        """Delete the rows a filter selects, or every row.

        A whole-table replace deletes rather than truncates: ``TRUNCATE``
        takes a schema-modification lock that blocks readers until the
        commit, a ``DELETE`` does not. A table that does not exist has nothing
        to delete.

        Args:
            table: Target table name.
            dataset: The schema, or ``None`` for the destination's default.
            where: The rows to delete; ``None`` for the whole table.
        """
        schema = self._schema(dataset)
        with self._session() as session:
            if not self._columns(session, table, schema):
                return
            self._begin()
            ref = self._ref(table, schema)
            if where is None:
                _run(session, f"DELETE FROM {ref}")
                return
            predicate, parameters = _predicate(where)
            _run(session, f"DELETE FROM {ref} WHERE {predicate}", parameters)

    def select(self, table: str, dataset: str | None, where: PartitionFilter | None) -> list[dict[str, Any]]:
        """Select the rows a filter selects, or every row, as records.

        Nested and repeated values come back as the JSON text they are stored as.

        Args:
            table: Target table name.
            dataset: The schema, or ``None`` for the destination's default.
            where: The rows to select; ``None`` for the whole table.

        Returns:
            The selected rows.

        Raises:
            DataNotFoundError: If the table does not exist yet.
        """
        schema = self._schema(dataset)
        with self._session() as session:
            if not self._columns(session, table, schema):
                raise DataNotFoundError(self._not_found(table, schema))
            ref = self._ref(table, schema)
            if where is None:
                names, rows = _fetch(session, f"SELECT * FROM {ref}")
            else:
                predicate, parameters = _predicate(where)
                names, rows = _fetch(session, f"SELECT * FROM {ref} WHERE {predicate}", parameters)
        return [dict(zip(names, row, strict=True)) for row in rows]

    def count(self, table: str, dataset: str | None, column: str) -> dict[str, int]:
        """Return row counts grouped by a column's values, cast to text.

        Args:
            table: Target table name.
            dataset: The schema, or ``None`` for the destination's default.
            column: Column to group by.

        Returns:
            Mapping from the column's value (as a string) to its row count.

        Raises:
            DataNotFoundError: If the table does not exist.
        """
        schema = self._schema(dataset)
        value = f"CAST({_quote(column)} AS varchar(max))"
        with self._session() as session:
            if not self._columns(session, table, schema):
                raise DataNotFoundError(self._not_found(table, schema))
            _, rows = _fetch(
                session,
                f"SELECT {value} AS partition_value, COUNT(*) AS cnt FROM {self._ref(table, schema)} GROUP BY {value}",
            )
        return {row[0]: row[1] for row in rows}


# -- Utility functions ---------------------------------------------------------


def batch_size(columns: int) -> int:
    """Return how many rows one ``INSERT ... VALUES`` statement carries.

    Every value is one parameter, so a batch is bounded by
    :data:`MAX_PARAMETERS` divided by the column count, and by
    :data:`MAX_ROWS` for narrow tables.

    Args:
        columns: The number of columns each row binds.

    Returns:
        The rows per statement, at least one.
    """
    return max(1, min(MAX_ROWS, MAX_PARAMETERS // max(columns, 1)))


def _quote(identifier: str) -> str:
    """Quote an identifier in T-SQL brackets, escaping embedded closing brackets.

    Args:
        identifier: A schema, table or column name.

    Returns:
        The bracketed identifier.
    """
    return "[" + identifier.replace("]", "]]") + "]"


def _odbc(value: str) -> str:
    """Brace a connection string value so separators inside it are taken literally.

    Args:
        value: A server or database name.

    Returns:
        The value in braces, embedded closing braces doubled.
    """
    return "{" + value.replace("}", "}}") + "}"


def _columns_ddl(specs: Sequence[FieldSpec]) -> str:
    """Render field specs as the column list of a ``CREATE TABLE``.

    Args:
        specs: The table's field specs.

    Returns:
        Comma-separated bracketed column definitions. Every column is
        nullable: conform already enforces the schema's nullability.
    """
    return ", ".join(f"{_quote(spec.name)} {column_type(spec)} NULL" for spec in specs)


def _run(session: mssql_python.Connection, sql: str, parameters: Sequence[Any] | None = None) -> None:
    """Run one statement that returns no rows.

    Args:
        session: The session to run it on.
        sql: The statement, with ``?`` placeholders for *parameters*.
        parameters: The statement's parameters; defaults to none.
    """
    cursor = session.cursor()
    try:
        if parameters is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, list(parameters))
    finally:
        cursor.close()


def _fetch(
    session: mssql_python.Connection, sql: str, parameters: Sequence[Any] | None = None
) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Run one query and fetch its rows.

    Args:
        session: The session to run it on.
        sql: The query, with ``?`` placeholders for *parameters*.
        parameters: The query's parameters; defaults to none.

    Returns:
        The result's column names and its rows, as plain tuples.
    """
    cursor = session.cursor()
    try:
        if parameters is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, list(parameters))
        names = [column[0] for column in cursor.description or []]
        return names, [tuple(row) for row in cursor.fetchall()]
    finally:
        cursor.close()


def _predicate(where: PartitionFilter) -> tuple[str, list[Any]]:
    """Render a partition filter as a parameterised predicate.

    No cast is needed: T-SQL converts a parameter to the column's type when
    that type ranks higher, so a ``'2024-01-01'`` partition id compares as a
    ``date``.

    Args:
        where: The filter to render.

    Returns:
        The predicate text and the parameters its ``?`` placeholders name.
    """
    column = _quote(where.column)
    if where.bounds is None:
        return f"{column} = ?", [_bind(where.value)]
    start, end = where.bounds
    return f"{column} >= ? AND {column} < ?", [_bind(start), _bind(end)]


def _align(records: list[dict[str, Any]], columns: list[str], ref: str) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Shape records to the table's columns for one batch of inserts.

    Every row binds the same columns, the table columns any record carries,
    so a column no record mentions is left ``NULL``.

    Args:
        records: The rows to write.
        columns: The table's column names, in table order.
        ref: The table's name, for the warning.

    Returns:
        The columns written, and each row's values in that order, bound for the driver.
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
    return keys, [tuple(_bind(record.get(key)) for key in keys) for record in records]


def _bind(value: Any) -> Any:
    """Turn a conformed value into what the driver binds for its column.

    A nested or repeated value becomes JSON text; a non-finite float becomes
    ``NULL``, which no column rejects; a timezone-aware datetime becomes naive
    UTC, since ``datetime2`` holds no offset and the driver would otherwise
    send a ``datetimeoffset`` that the conversion truncates to local time.

    Args:
        value: A cell value.

    Returns:
        The value to bind.
    """
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(replace_non_finite(value), default=json_default)
    if isinstance(value, datetime.datetime) and value.tzinfo is not None:
        return value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return value
