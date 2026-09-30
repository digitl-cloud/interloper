"""Snowflake destination implementation."""

from __future__ import annotations

import threading
import warnings
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

import pandas as pd
from interloper.destination import IOContext, destination
from interloper.destination.database import DatabaseDestination, PartitionFilter
from interloper.errors import ConfigError, DataNotFoundError
from interloper.representation import Representation
from interloper.resource.fields import FetchField, InputField
from interloper.schema import FieldSpec
from pydantic import PrivateAttr
from snowflake.connector import SnowflakeConnection as Session
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.pandas_tools import write_pandas

from interloper_snowflake.connection import SnowflakeConnection
from interloper_snowflake.types import column_type


def _quote(identifier: str) -> str:
    """Quote an identifier so Snowflake keeps it exactly as written.

    Args:
        identifier: A database, schema, table or column name.

    Returns:
        The identifier in double quotes, embedded quotes doubled.
    """
    return '"' + identifier.replace('"', '""') + '"'


@destination(
    key="snowflake_destination",
    name="Snowflake",
    icon="icon:snowflake",
    tags=["Cloud"],
)
class SnowflakeDestination(DatabaseDestination):
    """Snowflake destination.

    A dataset is a Snowflake schema inside the destination's database. Every
    identifier is quoted, so tables and columns keep the case the asset gives
    them.

    The connection's session is shared by every asset written through this
    destination, and a Snowflake transaction belongs to the session, so
    transactions are serialised: two concurrent writes would otherwise commit
    or roll back each other's statements.
    """

    connection: SnowflakeConnection

    database: str = FetchField(
        provider="connection.databases",
        label_key="name",
        value_key="name",
        description="Snowflake database",
        discriminator=True,
    )
    warehouse: str = FetchField(
        provider="connection.warehouses",
        label_key="name",
        value_key="name",
        description="Virtual warehouse running the loads and queries",
    )
    default_dataset: str | None = InputField(default=None, description="Default schema for assets without a dataset")

    _warehouse_in_use: bool = PrivateAttr(default=False)
    _lock: Any = PrivateAttr(default_factory=threading.RLock)
    _transaction: threading.local = PrivateAttr(default_factory=threading.local)

    # -- Session -----------------------------------------------------------------

    @property
    def _session(self) -> Session:
        """The connection's session, with this destination's warehouse selected.

        Returns:
            The connector session.
        """
        session = self.connection.client
        if not self._warehouse_in_use:
            cursor = session.cursor()
            try:
                cursor.execute(f"USE WAREHOUSE {_quote(self.warehouse)}")
            finally:
                cursor.close()
            self._warehouse_in_use = True
        return session

    def _execute(self, sql: str, params: Sequence[Any] | None = None) -> SnowflakeCursor:
        """Run one statement, on the open transaction's cursor or on a fresh one.

        Args:
            sql: The statement, with ``%s`` placeholders for *params*.
            params: The statement's parameters; defaults to none.

        Returns:
            The cursor the statement ran on, ready to fetch from.
        """
        cursor = getattr(self._transaction, "cursor", None)
        if cursor is None:
            cursor = self._session.cursor()
        cursor.execute(sql, params)
        return cursor

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Run one write, a delete followed by an insert, as ``BEGIN ... COMMIT``.

        Yields:
            ``None``; the write runs inside the block, rolled back and
            re-raised if it raises.
        """
        with self._lock:
            cursor = self._session.cursor()
            cursor.execute("BEGIN")
            self._transaction.cursor = cursor
            try:
                yield
            except BaseException:
                cursor.execute("ROLLBACK")
                raise
            else:
                cursor.execute("COMMIT")
            finally:
                self._transaction.cursor = None
                cursor.close()

    # -- Naming --------------------------------------------------------------------

    def _resolve_dataset(self, dataset: str | None) -> str:
        """Return the Snowflake schema to use.

        Args:
            dataset: The asset's dataset, or ``None`` to fall back to the destination's default.

        Returns:
            The resolved schema name.

        Raises:
            ConfigError: If neither the asset nor the destination names a dataset.
        """
        schema = dataset or self.default_dataset
        if schema is None:
            raise ConfigError(
                "SnowflakeDestination requires a dataset. Either set 'dataset' on the asset "
                "or provide 'default_dataset' on the destination."
            )
        return schema

    def _table_ref(self, table: str, schema: str) -> str:
        """Build a fully-qualified, quoted table reference.

        Args:
            table: Table name.
            schema: The resolved schema name.

        Returns:
            ``"database"."schema"."table"``.
        """
        return f"{_quote(self.database)}.{_quote(schema)}.{_quote(table)}"

    def _table_exists(self, table: str, schema: str) -> bool:
        """Check whether a table exists, through the database's information schema.

        Args:
            table: Table name.
            schema: The resolved schema name.

        Returns:
            ``True`` if the table exists, ``False`` otherwise.
        """
        cursor = self._execute(
            f"SELECT 1 FROM {_quote(self.database)}.information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            (schema, table),
        )
        return bool(cursor.fetchall())

    # -- DatabaseDestination hooks ---------------------------------------------------

    def insert(self, table: str, dataset: str | None, data: Any, context: IOContext) -> None:
        """Insert data through one ``write_pandas`` load, creating the schema and table first if missing.

        A new table is typed from the effective schema (declared on the asset,
        or inferred during conform), or from a schema inferred from the data
        when the context carries none. The frame is aligned to the table's
        columns: an extra column is dropped with a warning, since an existing
        table is never altered.

        Args:
            table: Target table name.
            dataset: The Snowflake schema, or ``None`` for the destination's default.
            data: The data in its native representation.
            context: IO context carrying the asset and effective schema.
        """
        schema = self._resolve_dataset(dataset)
        specs = (context.schema or Representation.of(data).infer()).field_specs()
        if not self._table_exists(table, schema):
            self._execute(f"CREATE SCHEMA IF NOT EXISTS {_quote(self.database)}.{_quote(schema)}")
            self._execute(f"CREATE TABLE IF NOT EXISTS {self._table_ref(table, schema)} ({_columns_ddl(specs)})")

        frame = Representation.of(data).to("dataframe")
        columns = [spec.name for spec in specs]
        extras = [str(c) for c in frame.columns if str(c) not in columns]
        if extras:
            warnings.warn(
                f"Columns {extras} are not in the schema for '{self._table_ref(table, schema)}' "
                "and will not be written.",
                UserWarning,
                stacklevel=2,
            )
        frame = frame[[c for c in columns if c in frame.columns]].reset_index(drop=True)
        self._load(frame, table, schema)

    def _load(self, frame: pd.DataFrame, table: str, schema: str) -> None:
        """Load a DataFrame into an existing table through a stage and ``COPY INTO``.

        Args:
            frame: The DataFrame to load, aligned to the table's columns.
            table: Target table name.
            schema: The resolved schema name.
        """
        write_pandas(
            self._session,
            frame,
            table_name=table,
            database=self.database,
            schema=schema,
            quote_identifiers=True,
            auto_create_table=False,
            use_logical_type=True,
        )

    def delete(self, table: str, dataset: str | None, where: PartitionFilter | None) -> None:
        """Delete the rows a filter selects, or truncate the table.

        A table that does not exist has nothing to delete.

        Args:
            table: Target table name.
            dataset: The Snowflake schema, or ``None`` for the destination's default.
            where: The rows to delete; ``None`` for the whole table.
        """
        schema = self._resolve_dataset(dataset)
        if not self._table_exists(table, schema):
            return
        ref = self._table_ref(table, schema)
        if where is None:
            self._execute(f"TRUNCATE TABLE {ref}")
            return
        predicate, params = _predicate(where)
        self._execute(f"DELETE FROM {ref} WHERE {predicate}", params)

    def select(self, table: str, dataset: str | None, where: PartitionFilter | None) -> pd.DataFrame:
        """Select the rows a filter selects, or every row, as a DataFrame.

        The connector builds the frame from the result's Arrow batches, so
        column types survive the read without a pass through Python records.

        Args:
            table: Target table name.
            dataset: The Snowflake schema, or ``None`` for the destination's default.
            where: The rows to select; ``None`` for the whole table.

        Returns:
            The selected rows.

        Raises:
            DataNotFoundError: If the table does not exist yet.
        """
        schema = self._resolve_dataset(dataset)
        ref = self._table_ref(table, schema)
        if not self._table_exists(table, schema):
            raise DataNotFoundError(f"Table '{ref}' does not exist. Has the asset been materialized?")
        if where is None:
            return self._execute(f"SELECT * FROM {ref}").fetch_pandas_all()
        predicate, params = _predicate(where)
        return self._execute(f"SELECT * FROM {ref} WHERE {predicate}", params).fetch_pandas_all()

    def count(self, table: str, dataset: str | None, column: str) -> dict[str, int]:
        """Return row counts grouped by a column.

        Args:
            table: Target table name.
            dataset: The Snowflake schema, or ``None`` for the destination's default.
            column: Column to group by.

        Returns:
            Mapping from the column's value (as string) to row count.

        Raises:
            DataNotFoundError: If the table does not exist yet.
        """
        schema = self._resolve_dataset(dataset)
        ref = self._table_ref(table, schema)
        if not self._table_exists(table, schema):
            raise DataNotFoundError(f"Table '{ref}' does not exist. Has the asset been materialized?")
        cursor = self._execute(
            f"SELECT TO_VARCHAR({_quote(column)}) AS partition_value, COUNT(*) AS cnt FROM {ref} GROUP BY 1"
        )
        return {value: count for value, count in cursor.fetchall()}


# -- Utility functions ---------------------------------------------------------------


def _columns_ddl(specs: Sequence[FieldSpec]) -> str:
    """Render field specs as the column list of a ``CREATE TABLE``.

    Args:
        specs: The table's field specs.

    Returns:
        Comma-separated quoted column definitions. Every column is nullable:
        conform already enforces the schema's nullability, and a constraint
        here would only turn a schema change into a failed load.
    """
    return ", ".join(f"{_quote(spec.name)} {column_type(spec)}" for spec in specs)


def _predicate(where: PartitionFilter) -> tuple[str, tuple[Any, ...]]:
    """Render a partition filter as a parameterised SQL predicate.

    Args:
        where: The filter to render.

    Returns:
        The predicate text and the parameters its ``%s`` placeholders name.
    """
    column = _quote(where.column)
    if where.bounds is None:
        return f"{column} = %s", (where.value,)
    start, end = where.bounds
    return f"{column} >= %s AND {column} < %s", (start, end)
