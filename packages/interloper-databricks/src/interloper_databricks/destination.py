"""Databricks destination implementation."""

from __future__ import annotations

import datetime
import inspect
import io
import json
import math
import threading
import uuid
import warnings
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from functools import cached_property
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from databricks.sql.client import Connection as Session
from databricks.sql.client import Cursor
from interloper.destination import IOContext, destination
from interloper.destination.database import DatabaseDestination, PartitionFilter
from interloper.errors import ConfigError, DataNotFoundError
from interloper.partitioning.base import Partition
from interloper.representation import Representation
from interloper.resource.fields import FetchField, InputField
from interloper.schema import FieldSpec
from interloper.utils.data import is_empty
from interloper.utils.json import json_default, replace_non_finite
from pydantic import PrivateAttr, field_validator
from typing_extensions import Self

from interloper_databricks.connection import DatabricksConnection
from interloper_databricks.types import TIMESTAMP, VARIANT, clusterable, column_type

#: Delta collects statistics on a table's first 32 columns, and a clustering
#: key must be one of them.
_STATS_COLUMNS = 32


class _Lock:
    """A re-entrant lock that a deep copy replaces with a fresh one.

    A destination is deep-copied with the source it is bound to, and a bare
    ``threading.RLock`` cannot be copied.
    """

    def __init__(self) -> None:
        """Create the underlying lock."""
        self._lock = threading.RLock()

    def __enter__(self) -> Self:
        """Acquire the lock.

        Returns:
            The lock.
        """
        self._lock.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        """Release the lock.

        Args:
            *exc: The exception details, if the block raised; unused.
        """
        self._lock.release()

    def __deepcopy__(self, memo: dict[int, Any]) -> _Lock:
        """Copy as a new, unheld lock.

        Args:
            memo: The deep-copy memo; unused.

        Returns:
            A fresh lock.
        """
        return _Lock()


@dataclass(frozen=True)
class _Staged:
    """Data uploaded to the staging volume, ready to be inserted.

    Attributes:
        path: The file's ``/Volumes/...`` path.
        query: The ``SELECT`` reading the file, its columns cast to the table's types.
    """

    path: str
    query: str


@destination(
    key="databricks_destination",
    name="Databricks",
    icon="icon:databricks",
    tags=["Cloud"],
    maturity="alpha",
)
class DatabricksDestination(DatabaseDestination):
    """Databricks destination writing Delta tables in Unity Catalog through a SQL warehouse.

    A dataset is a schema inside the destination's catalog. Every write
    uploads the data as one Parquet file to the staging volume and loads it
    with a single statement: ``INSERT INTO ... REPLACE WHERE`` for a
    partition or a window, ``INSERT OVERWRITE`` for an unpartitioned asset.

    That is why :meth:`write` and :meth:`write_partition` are overridden
    rather than left to the base's delete-then-insert: Databricks has no
    generally available multi-statement transaction, so a ``DELETE``
    followed by an insert could leave a partition empty when the insert
    fails, while ``REPLACE WHERE`` deletes and inserts in one Delta commit.
    The rows to replace still come from the base's partition filters, and
    :meth:`delete`, :meth:`select` and :meth:`count` remain the base's hooks.

    The destination opens one SQL session, shared by every asset written
    through it. The connector's sessions are not thread-safe, so every
    statement runs under the destination's lock, and a write holds it from
    the upload to the cleanup.
    """

    connection: DatabricksConnection

    warehouse: str = FetchField(
        provider="connection.warehouses",
        label_key="name",
        value_key="path",
        label="SQL warehouse",
        description="SQL warehouse running the loads and queries",
    )
    catalog: str = FetchField(
        provider="connection.catalogs",
        label_key="name",
        value_key="name",
        description="Unity Catalog catalog",
        discriminator=True,
    )
    default_dataset: str | None = InputField(default=None, description="Default schema for assets without a dataset")
    staging_volume: str = InputField(
        label="Staging volume",
        description="Unity Catalog volume the loads stage files in, as catalog.schema.volume",
    )

    _lock: _Lock = PrivateAttr(default_factory=_Lock)

    @field_validator("staging_volume")
    @classmethod
    def three_part_volume(cls, value: str) -> str:
        """Require a fully qualified volume name.

        Args:
            value: The volume name.

        Returns:
            The volume name, unchanged.

        Raises:
            ValueError: If the name is not ``catalog.schema.volume``.
        """
        parts = value.split(".")
        if len(parts) != 3 or not all(parts):
            raise ValueError(f"staging_volume must be 'catalog.schema.volume', got '{value}'")
        return value

    @property
    def volume_path(self) -> str:
        """The staging volume's path.

        Returns:
            ``/Volumes/<catalog>/<schema>/<volume>``.
        """
        return "/Volumes/" + "/".join(self.staging_volume.split("."))

    @cached_property
    def client(self) -> Session:
        """The destination's own session, on its warehouse and catalog.

        Returns:
            The connector session, cached per destination instance.
        """
        return self.connection.connect(http_path=self.warehouse, catalog=self.catalog)

    # -- Session -----------------------------------------------------------------

    def _execute(
        self,
        sql: str,
        *,
        fetch: Callable[[Cursor], Any] | None = None,
        input_stream: io.BytesIO | None = None,
    ) -> Any:
        """Run one statement on a fresh cursor, under the destination's lock.

        Args:
            sql: The statement.
            fetch: Reads the result off the cursor; ``None`` when the
                statement returns nothing of interest.
            input_stream: The bytes a ``PUT '__input_stream__'`` uploads.

        Returns:
            What *fetch* returns, or ``None``.
        """
        with self._lock:
            cursor = self.client.cursor()
            try:
                cursor.execute(sql, input_stream=input_stream)
                return fetch(cursor) if fetch is not None else None
            finally:
                cursor.close()

    # -- Destination interface -----------------------------------------------------

    def write(self, context: IOContext, data: Any) -> None:
        """Replace the partitions the context covers with the data, in one statement.

        Args:
            context: IO context carrying the target asset, the partition or window,
                and the effective schema.
            data: The data to write, in its native representation.
        """
        if is_empty(data):
            return
        self._warn_missing_partition_column(data, context)
        self._replace(context, context.partitions, data)

    def write_partition(self, context: IOContext, partition: Partition | None, data: Any) -> None:
        """Replace one partition's rows with the data, in one statement.

        Args:
            context: IO context carrying the target asset and the effective schema.
            partition: The partition being stored, or ``None`` for the whole table.
            data: The data to store, in its native representation.
        """
        self._replace(context, [partition], data)

    def _replace(self, context: IOContext, partitions: Sequence[Partition | None], data: Any) -> None:
        """Load the data in place of the rows the partitions cover.

        Args:
            context: IO context carrying the target asset and the effective schema.
            partitions: The partitions replaced; ``[None]`` for the whole table.
            data: The data, in its native representation.
        """
        table, dataset = self._target(context)
        filters = [self._filter(context, partition) for partition in partitions]
        if None in filters:
            self._load(table, dataset, data, context, "OVERWRITE")
            return
        where = _predicate([f for f in filters if f is not None])
        self._load(table, dataset, data, context, "INTO", f" REPLACE WHERE {where}")

    # -- Naming --------------------------------------------------------------------

    def _resolve_dataset(self, dataset: str | None) -> str:
        """Return the schema to use.

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
                "DatabricksDestination requires a dataset. Either set 'dataset' on the asset "
                "or provide 'default_dataset' on the destination."
            )
        return schema

    def _table_ref(self, table: str, schema: str) -> str:
        """Build a fully-qualified, quoted table reference.

        Args:
            table: Table name.
            schema: The resolved schema name.

        Returns:
            ```catalog`.`schema`.`table```.
        """
        return f"{_quote(self.catalog)}.{_quote(schema)}.{_quote(table)}"

    def _table_exists(self, table: str, schema: str) -> bool:
        """Check whether a table exists, through the catalog's information schema.

        Unity Catalog stores object names in lower case, so the lookup does too.

        Args:
            table: Table name.
            schema: The resolved schema name.

        Returns:
            ``True`` if the table exists, ``False`` otherwise.
        """
        rows = self._execute(
            f"SELECT 1 FROM {_quote(self.catalog)}.information_schema.tables "
            f"WHERE table_schema = {_literal(schema.lower())} AND table_name = {_literal(table.lower())}",
            fetch=lambda cursor: cursor.fetchall(),
        )
        return bool(rows)

    # -- Loading -------------------------------------------------------------------

    def _ensure_table(self, table: str, schema: str, specs: Sequence[FieldSpec], context: IOContext) -> None:
        """Create the schema and a typed Delta table when the table does not exist.

        A partitioned asset's table is clustered on its partition column,
        which every replace and partition read filters on, when liquid
        clustering accepts that column as a key; Databricks recommends
        clustering over partitioning for tables of this size. Field
        descriptions become column comments, the asset's description the
        table comment.

        Args:
            table: Table name.
            schema: The resolved schema name.
            specs: The table's field specs.
            context: IO context carrying the asset.
        """
        if self._table_exists(table, schema):
            return
        self._execute(f"CREATE SCHEMA IF NOT EXISTS {_quote(self.catalog)}.{_quote(schema)}")
        columns = ", ".join(_column_ddl(spec) for spec in specs)
        sql = f"CREATE TABLE IF NOT EXISTS {self._table_ref(table, schema)} ({columns}) USING DELTA"
        partitioning = context.asset.partitioning
        keys = [spec for spec in specs[:_STATS_COLUMNS] if partitioning and spec.name == partitioning.column]
        if keys and clusterable(keys[0]):
            sql += f" CLUSTER BY ({_quote(keys[0].name)})"
        description = _asset_description(context.asset)
        if description:
            sql += f" COMMENT {_literal(description)}"
        self._execute(sql)

    def _stage(self, table: str, schema: str, data: Any, context: IOContext) -> _Staged:
        """Create what the load needs and upload the data as one Parquet file.

        A new table is typed from the effective schema (declared on the asset,
        or inferred during conform), or from a schema inferred from the data
        when the context carries none. The frame is aligned to the schema's
        columns: an extra column is dropped with a warning, since an existing
        table is never altered. Each file gets a name of its own, so
        concurrent writes never read each other's files.

        Args:
            table: Target table name.
            schema: The resolved schema name.
            data: The data in its native representation.
            context: IO context carrying the asset and effective schema.

        Returns:
            Where the file was staged and the query reading it.
        """
        specs = (context.schema or Representation.of(data).infer()).field_specs()
        self._ensure_table(table, schema, specs, context)

        frame = Representation.of(data).to("dataframe")
        names = [spec.name for spec in specs]
        extras = [str(c) for c in frame.columns if str(c) not in names]
        if extras:
            warnings.warn(
                f"Columns {extras} are not in the schema for '{self._table_ref(table, schema)}' "
                "and will not be written.",
                UserWarning,
                stacklevel=4,
            )
        kept = [spec for spec in specs if spec.name in frame.columns]

        path = f"{self.volume_path}/interloper/{uuid.uuid4().hex}.parquet"
        self._execute(
            f"PUT '__input_stream__' INTO {_literal(path)} OVERWRITE",
            input_stream=io.BytesIO(_parquet(frame, kept)),
        )
        projection = ", ".join(_projection(spec) for spec in kept)
        return _Staged(path=path, query=f"SELECT {projection} FROM read_files({_literal(path)}, format => 'parquet')")

    def _load(
        self,
        table: str,
        dataset: str | None,
        data: Any,
        context: IOContext,
        mode: str,
        clause: str = "",
    ) -> None:
        """Stage the data and insert it with one statement, removing the file afterwards.

        The file is removed whether or not the insert succeeds; a failed
        removal only warns, so it never hides the insert's own outcome.

        Args:
            table: Target table name.
            dataset: The schema, or ``None`` for the destination's default.
            data: The data in its native representation.
            context: IO context carrying the asset and effective schema.
            mode: ``INTO`` to add rows, ``OVERWRITE`` to replace the whole table.
            clause: What follows the column matching, such as a ``REPLACE WHERE``.
        """
        schema = self._resolve_dataset(dataset)
        with self._lock:
            staged = self._stage(table, schema, data, context)
            try:
                self._execute(f"INSERT {mode} {self._table_ref(table, schema)} BY NAME{clause} {staged.query}")
            finally:
                try:
                    self._execute(f"REMOVE {_literal(staged.path)}")
                except Exception as error:  # noqa: BLE001
                    warnings.warn(
                        f"Could not remove the staged file '{staged.path}': {error}", UserWarning, stacklevel=2
                    )

    # -- DatabaseDestination hooks ---------------------------------------------------

    def insert(self, table: str, dataset: str | None, data: Any, context: IOContext) -> None:
        """Append the data to the table through a staged Parquet file.

        Args:
            table: Target table name.
            dataset: The schema, or ``None`` for the destination's default.
            data: The data in its native representation.
            context: IO context carrying the asset and effective schema.
        """
        self._load(table, dataset, data, context, "INTO")

    def delete(self, table: str, dataset: str | None, where: PartitionFilter | None) -> None:
        """Delete the rows a filter selects, or every row.

        A table that does not exist has nothing to delete.

        Args:
            table: Target table name.
            dataset: The schema, or ``None`` for the destination's default.
            where: The rows to delete; ``None`` for the whole table.
        """
        schema = self._resolve_dataset(dataset)
        if not self._table_exists(table, schema):
            return
        sql = f"DELETE FROM {self._table_ref(table, schema)}"
        self._execute(sql if where is None else f"{sql} WHERE {_predicate([where])}")

    def select(self, table: str, dataset: str | None, where: PartitionFilter | None) -> pd.DataFrame:
        """Select the rows a filter selects, or every row, as a DataFrame.

        The result arrives as Arrow, so column types survive the read without
        a pass through Python records.

        Args:
            table: Target table name.
            dataset: The schema, or ``None`` for the destination's default.
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
        sql = f"SELECT * FROM {ref}" if where is None else f"SELECT * FROM {ref} WHERE {_predicate([where])}"
        return self._execute(sql, fetch=lambda cursor: cursor.fetchall_arrow().to_pandas())

    def count(self, table: str, dataset: str | None, column: str) -> dict[str, int]:
        """Return row counts grouped by a column.

        Args:
            table: Target table name.
            dataset: The schema, or ``None`` for the destination's default.
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
        rows = self._execute(
            f"SELECT CAST({_quote(column)} AS STRING) AS partition_value, COUNT(*) AS cnt FROM {ref} GROUP BY 1",
            fetch=lambda cursor: cursor.fetchall(),
        )
        return {row[0]: row[1] for row in rows}


# -- Utility functions ---------------------------------------------------------------


def _quote(identifier: str) -> str:
    """Quote an identifier with backticks.

    Args:
        identifier: A catalog, schema, table or column name.

    Returns:
        The identifier in backticks, embedded backticks doubled.
    """
    return "`" + identifier.replace("`", "``") + "`"


def _literal(value: Any) -> str:
    """Render a value as a Databricks SQL literal.

    Every value a statement here carries is a literal: ``REPLACE WHERE``
    admits literal values in its predicate, and ``PUT``, ``REMOVE`` and
    ``read_files`` take their paths as string literals, so one renderer
    serves them all. A naive datetime is read as UTC, as the load writes it.

    Args:
        value: A string, date, datetime, number, boolean or ``None``.

    Returns:
        The literal: a backslash-escaped string, ``DATE'...'``,
        ``TIMESTAMP'...'``, a bare number, ``TRUE``/``FALSE`` or ``NULL``.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, datetime.datetime):
        stamp = value.isoformat() if value.tzinfo is not None else f"{value.isoformat()}Z"
        return f"TIMESTAMP'{stamp}'"
    if isinstance(value, datetime.date):
        return f"DATE'{value.isoformat()}'"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _predicate(filters: Sequence[PartitionFilter]) -> str:
    """Render the rows several partition filters cover as one predicate.

    Time partitions contribute their half-open bounds, merged where they
    touch, so a contiguous window becomes the single range from its first
    start to its last end. Other partitions match their ids by equality.

    Args:
        filters: The filters, all on the same column.

    Returns:
        The predicate text.
    """
    column = _quote(filters[0].column)
    ranges: list[tuple[Any, Any]] = []
    for start, end in sorted(f.bounds for f in filters if f.bounds is not None):
        if ranges and start <= ranges[-1][1]:
            ranges[-1] = (ranges[-1][0], max(ranges[-1][1], end))
        else:
            ranges.append((start, end))
    parts = [f"{column} >= {_literal(start)} AND {column} < {_literal(end)}" for start, end in ranges]
    values = [f.value for f in filters if f.bounds is None]
    if len(values) == 1:
        parts.append(f"{column} = {_literal(values[0])}")
    elif values:
        parts.append(f"{column} IN ({', '.join(_literal(v) for v in values)})")
    return parts[0] if len(parts) == 1 else " OR ".join(f"({part})" for part in parts)


def _column_ddl(spec: FieldSpec) -> str:
    """Render a field spec as a column definition.

    Args:
        spec: The field spec.

    Returns:
        The quoted column and its type, with its description as a comment.
        Every column is nullable: conform already enforces the schema's
        nullability, and a constraint here would only turn a schema change
        into a failed load.
    """
    ddl = f"{_quote(spec.name)} {column_type(spec)}"
    return f"{ddl} COMMENT {_literal(spec.description)}" if spec.description else ddl


def _projection(spec: FieldSpec) -> str:
    """Read one staged column as the table's type.

    Args:
        spec: The column's field spec.

    Returns:
        ``PARSE_JSON`` of the JSON text for a ``VARIANT``, a ``CAST`` to the
        column type otherwise, aliased to the column so ``BY NAME`` matches it.
    """
    name = _quote(spec.name)
    kind = column_type(spec)
    expression = f"PARSE_JSON({name})" if kind == VARIANT else f"CAST({name} AS {kind})"
    return f"{expression} AS {name}"


def _parquet(frame: pd.DataFrame, specs: Sequence[FieldSpec]) -> bytes:
    """Encode the columns a load keeps as one Parquet file.

    This only chooses the file's encoding; the values are already conformed.
    ``VARIANT`` columns travel as JSON text for ``PARSE_JSON``, and
    ``TIMESTAMP`` columns as UTC instants in microseconds (a naive datetime
    read as UTC, so the session's time zone never shifts them; Databricks
    timestamps hold microseconds, and Spark does not read nanosecond Parquet
    timestamps as timestamps). A column with no
    values at all is written as strings, since Parquet's null type is not
    something every reader accepts; the load casts it to its real type.

    Args:
        frame: The data as a DataFrame.
        specs: The kept columns' field specs, in table order.

    Returns:
        The Parquet file's bytes.
    """
    columns: dict[str, pd.Series] = {}
    for spec in specs:
        series = frame[spec.name]
        kind = column_type(spec)
        if kind == VARIANT:
            series = series.map(_json)
        elif kind == TIMESTAMP:
            series = pd.to_datetime(series, utc=True)
        columns[spec.name] = series
    table = pa.Table.from_pandas(pd.DataFrame(columns, index=frame.index), preserve_index=False)
    for index, field in enumerate(table.schema):
        if pa.types.is_null(field.type):
            table = table.set_column(index, field.name, table.column(index).cast(pa.string()))
    buffer = io.BytesIO()
    pq.write_table(table, buffer, coerce_timestamps="us", allow_truncated_timestamps=True)
    return buffer.getvalue()


def _json(value: Any) -> str | None:
    """Encode one nested value as JSON text.

    Args:
        value: A dict, a list, an array or a missing value.

    Returns:
        The JSON text, or ``None`` for a missing value.
    """
    if value is None or value is pd.NA or (isinstance(value, float) and math.isnan(value)):
        return None
    if hasattr(value, "tolist"):
        value = value.tolist()
    return json.dumps(replace_non_finite(value), default=json_default)


def _asset_description(asset: Any) -> str | None:
    """Return the asset's description (its class docstring), cleaned.

    This mirrors how ``Component.definition()`` derives descriptions.

    Args:
        asset: The asset whose docstring is read.

    Returns:
        The cleaned docstring, or ``None`` when the asset has none.
    """
    doc = type(asset).__doc__
    return inspect.cleandoc(doc) if doc else None
