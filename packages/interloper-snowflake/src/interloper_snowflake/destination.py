"""Snowflake destination implementation."""

from __future__ import annotations

import tempfile
import threading
import uuid
import warnings
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any

import pandas as pd
from interloper.destination import IOContext, destination
from interloper.destination.database import DatabaseDestination, PartitionFilter
from interloper.errors import ConfigError, DataNotFoundError
from interloper.representation import Representation
from interloper.resource.fields import FetchField, InputField
from interloper.schema import FieldSpec
from interloper.utils.data import is_empty
from pydantic import PrivateAttr
from snowflake.connector import SnowflakeConnection as Session
from snowflake.connector.cursor import SnowflakeCursor

from interloper_snowflake.connection import SnowflakeConnection
from interloper_snowflake.types import column_type

_STAGE = "interloper_load"


def _quote(identifier: str) -> str:
    """Quote an identifier so Snowflake keeps it exactly as written.

    Args:
        identifier: A database, schema, table or column name.

    Returns:
        The identifier in double quotes, embedded quotes doubled.
    """
    return '"' + identifier.replace('"', '""') + '"'


@dataclass(frozen=True)
class _Staged:
    """Data uploaded to a stage, ready to be copied into its table.

    Attributes:
        table: The target table name.
        schema: The resolved schema name.
        location: The stage path holding the file, ``@stage/prefix``.
        columns: The file's columns, all of them the table's.
    """

    table: str
    schema: str
    location: str
    columns: tuple[str, ...]


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

    The destination opens its own session on its warehouse and database. That
    session is still shared by every asset written through the destination,
    and a Snowflake transaction belongs to the session, so writes are
    serialised: two concurrent writes would otherwise commit or roll back each
    other's statements.
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

    _stages: set[str] = PrivateAttr(default_factory=set)
    _lock: Any = PrivateAttr(default_factory=threading.RLock)
    _local: threading.local = PrivateAttr(default_factory=threading.local)

    @cached_property
    def client(self) -> Session:
        """The destination's own session, on its warehouse and database.

        Returns:
            The connector session, cached per destination instance.
        """
        return self.connection.connect(warehouse=self.warehouse, database=self.database)

    # -- Session -----------------------------------------------------------------

    def _execute(self, sql: str, params: Sequence[Any] | None = None) -> SnowflakeCursor:
        """Run one statement, on the open transaction's cursor or on a fresh one.

        Args:
            sql: The statement, with ``%s`` placeholders for *params*.
            params: The statement's parameters; defaults to none.

        Returns:
            The cursor the statement ran on, ready to fetch from.
        """
        cursor = getattr(self._local, "cursor", None)
        if cursor is None:
            cursor = self.client.cursor()
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
            cursor = self.client.cursor()
            cursor.execute("BEGIN")
            self._local.cursor = cursor
            try:
                yield
            except BaseException:
                cursor.execute("ROLLBACK")
                raise
            else:
                cursor.execute("COMMIT")
            finally:
                self._local.cursor = None
                cursor.close()

    # -- Destination interface -----------------------------------------------------

    def write(self, context: IOContext, data: Any) -> None:
        """Stage the data, then let the base replace its partitions.

        Snowflake commits an open transaction whenever it runs DDL, and the
        base calls :meth:`insert` inside :meth:`transaction`. So everything
        that is DDL or file transfer (creating the schema, the table and the
        stage, and the ``PUT``) runs here, before the base opens ``BEGIN``;
        inside it, only the ``DELETE`` and the ``COPY INTO`` run. Both of the
        base's paths, a single partition and a window, pass through here.

        The whole write holds the destination's lock: the session is shared,
        so another write's DDL would otherwise commit this one's open
        transaction.

        Args:
            context: IO context carrying the target asset, the partition or window,
                and the effective schema.
            data: The data to write, in its native representation.
        """
        if is_empty(data):
            return
        table, dataset = self._target(context)
        with self._lock:
            self._local.staged = self._stage(table, dataset, data, context)
            try:
                super().write(context, data)
            finally:
                self._local.staged = None

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

    # -- Staging -------------------------------------------------------------------

    def _ensure_table(self, table: str, schema: str, specs: Sequence[FieldSpec]) -> None:
        """Create the schema and a typed table when the table does not exist.

        Args:
            table: Table name.
            schema: The resolved schema name.
            specs: The table's field specs.
        """
        if self._table_exists(table, schema):
            return
        self._execute(f"CREATE SCHEMA IF NOT EXISTS {_quote(self.database)}.{_quote(schema)}")
        self._execute(f"CREATE TABLE IF NOT EXISTS {self._table_ref(table, schema)} ({_columns_ddl(specs)})")

    def _ensure_stage(self, schema: str) -> str:
        """Create the session's temporary stage in a schema, once per session.

        Args:
            schema: The resolved schema name.

        Returns:
            The stage's fully-qualified, quoted name.
        """
        stage = f"{_quote(self.database)}.{_quote(schema)}.{_quote(_STAGE)}"
        if schema not in self._stages:
            self._execute(f"CREATE TEMPORARY STAGE IF NOT EXISTS {stage}")
            self._stages.add(schema)
        return stage

    def _stage(self, table: str, dataset: str | None, data: Any, context: IOContext) -> _Staged:
        """Create what the load needs and upload the data as one Parquet file.

        A new table is typed from the effective schema (declared on the asset,
        or inferred during conform), or from a schema inferred from the data
        when the context carries none. The frame is aligned to the table's
        columns: an extra column is dropped with a warning, since an existing
        table is never altered. The file goes under a prefix of its own, so
        concurrent writes never load each other's files.

        Args:
            table: Target table name.
            dataset: The Snowflake schema, or ``None`` for the destination's default.
            data: The data in its native representation.
            context: IO context carrying the asset and effective schema.

        Returns:
            Where the file was staged and the columns it carries.
        """
        schema = self._resolve_dataset(dataset)
        specs = (context.schema or Representation.of(data).infer()).field_specs()
        self._ensure_table(table, schema, specs)
        stage = self._ensure_stage(schema)

        frame = Representation.of(data).to("dataframe")
        names = [spec.name for spec in specs]
        extras = [str(c) for c in frame.columns if str(c) not in names]
        if extras:
            warnings.warn(
                f"Columns {extras} are not in the schema for '{self._table_ref(table, schema)}' "
                "and will not be written.",
                UserWarning,
                stacklevel=3,
            )
        columns = tuple(c for c in names if c in frame.columns)

        location = f"@{stage}/{uuid.uuid4().hex}"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "data.parquet"
            frame[list(columns)].to_parquet(path, index=False)
            uri = path.as_posix().replace("'", "\\'")
            self._execute(f"PUT 'file://{uri}' {location} OVERWRITE=TRUE AUTO_COMPRESS=FALSE")
        return _Staged(table=table, schema=schema, location=location, columns=columns)

    # -- DatabaseDestination hooks ---------------------------------------------------

    def insert(self, table: str, dataset: str | None, data: Any, context: IOContext) -> None:
        """Copy the staged data into the table.

        :meth:`write` has already staged the data outside the transaction;
        called any other way, the data is staged here first.

        Args:
            table: Target table name.
            dataset: The Snowflake schema, or ``None`` for the destination's default.
            data: The data in its native representation.
            context: IO context carrying the asset and effective schema.
        """
        staged = getattr(self._local, "staged", None)
        if staged is None or staged.table != table or staged.schema != self._resolve_dataset(dataset):
            staged = self._stage(table, dataset, data, context)
        self._copy(staged)

    def _copy(self, staged: _Staged) -> None:
        """Load a staged Parquet file with ``COPY INTO``, projecting its columns by name.

        Parquet data is one ``$1`` object per row, so each column is read as
        ``$1:"name"`` and the file's column order does not matter. Binary
        columns stay binary, and logical types (dates, timestamps, decimals)
        are honoured. The file is purged once loaded.

        Args:
            staged: The staged file and its columns.
        """
        targets = ", ".join(_quote(c) for c in staged.columns)
        projection = ", ".join(f"$1:{_quote(c)}" for c in staged.columns)
        self._execute(
            f"COPY INTO {self._table_ref(staged.table, staged.schema)} ({targets}) "
            f"FROM (SELECT {projection} FROM {staged.location}) "
            "FILE_FORMAT=(TYPE=PARQUET USE_LOGICAL_TYPE=TRUE BINARY_AS_TEXT=FALSE) PURGE=TRUE"
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
