"""ClickHouse destination: MergeTree tables whose partitions are replaced atomically."""

from __future__ import annotations

import datetime
import json
import math
import uuid
import warnings
from collections.abc import Sequence
from typing import Any

import pandas as pd
from interloper.destination import IOContext, destination
from interloper.destination.database import DatabaseDestination, PartitionFilter
from interloper.errors import ConfigError, DataNotFoundError
from interloper.partitioning import PartitionConfig
from interloper.partitioning.base import Partition
from interloper.partitioning.time import TimePartition
from interloper.representation import Representation
from interloper.resource.fields import InputField
from interloper.schema import FieldSpec
from interloper.utils.data import is_empty
from interloper.utils.json import json_default, replace_non_finite

from interloper_clickhouse.connection import ClickHouseConnection
from interloper_clickhouse.partitioning import partition_key, same_key
from interloper_clickhouse.types import STRING, base_type, column_type, is_json, is_nullable

DEFAULT_DATABASE = "default"

_STAGING_PREFIX = "_interloper_staging_"

# A window's batch spans one partition per period; ClickHouse refuses an
# insert touching more than 100 partitions unless this is lifted (0 = no limit).
_STAGING_SETTINGS = {"max_partitions_per_insert_block": 0}


@destination(
    key="clickhouse_destination",
    name="ClickHouse",
    icon="icon:clickhouse",
    tags=["Database"],
    maturity="alpha",
)
class ClickHouseDestination(DatabaseDestination):
    """ClickHouse destination.

    A dataset is a ClickHouse database: the asset's dataset, else
    ``default_dataset``, else ``default``. A table is created on first write
    as a ``MergeTree`` partitioned so that one interloper partition is exactly
    one ClickHouse partition (see :func:`~interloper_clickhouse.partitioning.partition_key`),
    and is never altered afterwards.

    ClickHouse has no multi-statement transactions, so the base's
    delete-then-insert inside :meth:`transaction` would not be atomic, and a
    ``DELETE`` is a mutation rather than a cheap row operation. :meth:`write`
    and :meth:`write_partition` are therefore overridden: the batch is loaded
    into a staging table created ``AS`` the target, and each partition the
    write covers is swapped in with ``ALTER TABLE ... REPLACE PARTITION``,
    which is atomic per partition, or dropped when the batch holds no rows for
    it. The staging table is dropped afterwards, also on failure.
    :meth:`select`, :meth:`count` and :meth:`delete` remain the hooks the base
    reads and deletes through.
    """

    connection: ClickHouseConnection

    default_dataset: str | None = InputField(
        default=None, description="Default database for assets without a dataset; 'default' when empty"
    )

    # -- Destination interface ---------------------------------------------------

    def write(self, context: IOContext, data: Any) -> None:
        """Replace every partition the context covers with the data, a window as one batch.

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
        """Replace one partition, or the whole table, with the data.

        Args:
            context: IO context carrying the target asset and the effective schema.
            partition: The partition being stored, or ``None`` for the whole table.
            data: The data to store, in its native representation.
        """
        self._replace(context, [partition], data)

    # -- Naming ------------------------------------------------------------------

    def _database(self, dataset: str | None) -> str:
        """Return the database a dataset resolves to.

        Args:
            dataset: The asset's dataset, or ``None`` to fall back to the destination's default.

        Returns:
            The database name.
        """
        return dataset or self.default_dataset or DEFAULT_DATABASE

    @staticmethod
    def _ref(table: str, database: str) -> str:
        """Build the quoted, database-qualified table reference.

        Args:
            table: Table name.
            database: The resolved database name.

        Returns:
            The database and the table, each backtick-quoted, joined by a dot.
        """
        return f"{_quote(database)}.{_quote(table)}"

    # -- Catalog -----------------------------------------------------------------

    def _columns(self, table: str, database: str) -> dict[str, str]:
        """Read a table's columns and their types.

        Args:
            table: Table name.
            database: The resolved database name.

        Returns:
            Column name to ClickHouse type in table order, empty when the table does not exist.
        """
        result = self.connection.client.query(
            "SELECT name, type FROM system.columns WHERE database = {database:String} AND table = {table:String} "
            "ORDER BY position",
            parameters={"database": database, "table": table},
        )
        return {name: type_name for name, type_name in result.result_rows}

    def _existing(self, table: str, database: str) -> dict[str, str]:
        """Read the columns of a table that must exist.

        Args:
            table: Table name.
            database: The resolved database name.

        Returns:
            Column name to ClickHouse type in table order.

        Raises:
            DataNotFoundError: If the table does not exist yet.
        """
        columns = self._columns(table, database)
        if not columns:
            raise DataNotFoundError(f"Table '{database}.{table}' does not exist. Has the asset been materialized?")
        return columns

    def _ensure_table(
        self, table: str, database: str, specs: Sequence[FieldSpec], config: PartitionConfig | None
    ) -> dict[str, str]:
        """Create the database and the table when the table does not exist, and check its partition key.

        Columns are ``Nullable`` when their field is, except the partition
        column: ClickHouse keeps ``Nullable`` columns out of partition and
        sorting keys, and a row without a partition value belongs to no
        partition. An existing table is checked rather than altered: a
        partition key other than the one the asset's partitioning needs would
        make a partition replace touch the wrong rows.

        Args:
            table: Table name.
            database: The resolved database name.
            specs: The table's field specs.
            config: The asset's partitioning, or ``None``.

        Returns:
            Column name to ClickHouse type in table order.

        Raises:
            ConfigError: If the partition column is not in the schema, or an
                existing table is partitioned differently.
        """
        columns = self._columns(table, database)
        if not columns:
            self._create_table(table, database, specs, config)
            columns = self._columns(table, database)
        if config is not None and config.column not in columns:
            raise ConfigError(f"Partition column '{config.column}' is not a column of '{database}.{table}'.")
        expected = None if config is None else partition_key(config, _quote(config.column), columns[config.column])
        result = self.connection.client.query(
            "SELECT partition_key FROM system.tables WHERE database = {database:String} AND name = {table:String}",
            parameters={"database": database, "table": table},
        )
        actual = result.result_rows[0][0]
        if not same_key(actual, expected):
            raise ConfigError(
                f"Table '{database}.{table}' is partitioned by '{actual or 'nothing'}', but the asset's "
                f"partitioning needs '{expected or 'nothing'}'. Recreate the table, or partition the asset to match."
            )
        return columns

    def _create_table(
        self, table: str, database: str, specs: Sequence[FieldSpec], config: PartitionConfig | None
    ) -> None:
        """Create the database and a typed ``MergeTree`` table, unless they already exist.

        Args:
            table: Table name.
            database: The resolved database name.
            specs: The table's field specs.
            config: The asset's partitioning, or ``None``.

        Raises:
            ConfigError: If the partition column is not in the schema.
        """
        column = None if config is None else config.column
        if column is not None and column not in {spec.name for spec in specs}:
            raise ConfigError(f"Partition column '{column}' is not in the schema of '{database}.{table}'.")
        definitions = []
        key = None
        for spec in specs:
            type_name = column_type(spec)
            if spec.name == column:
                key = partition_key(config, _quote(column), type_name)
            elif is_nullable(spec):
                type_name = f"Nullable({type_name})"
            definitions.append(f"{_quote(spec.name)} {type_name}")
        layout = "ENGINE = MergeTree"
        if key is not None and column is not None:
            layout += f" PARTITION BY {key} ORDER BY {_quote(column)}"
        else:
            layout += " ORDER BY tuple()"
        client = self.connection.client
        client.command(f"CREATE DATABASE IF NOT EXISTS {_quote(database)}")
        client.command(f"CREATE TABLE IF NOT EXISTS {self._ref(table, database)} ({', '.join(definitions)}) {layout}")

    # -- Loading -----------------------------------------------------------------

    @staticmethod
    def _specs(data: Any, context: IOContext) -> list[FieldSpec]:
        """Return the field specs a table is typed from.

        Args:
            data: The data being written.
            context: IO context carrying the effective schema.

        Returns:
            The effective schema's specs, or those of a schema inferred from the data.
        """
        return list((context.schema or Representation.of(data).infer()).field_specs())

    def _load(
        self,
        ref: str,
        columns: dict[str, str],
        specs: Sequence[FieldSpec],
        data: Any,
        settings: dict[str, Any] | None = None,
    ) -> None:
        """Insert data into a table in one ``insert_df``, aligned to the table's columns.

        A column the table does not have is dropped with a warning, since an
        existing table is never altered. A field stored as JSON text is
        encoded here; every other value is sent as conformed.

        Args:
            ref: The quoted table reference to insert into.
            columns: The table's column name to ClickHouse type.
            specs: The field specs the data was conformed to.
            data: The data in its native representation.
            settings: ClickHouse settings for the insert; defaults to none.
        """
        frame = Representation.of(data).to("dataframe")
        extras = [str(c) for c in frame.columns if str(c) not in columns]
        if extras:
            warnings.warn(
                f"Columns {extras} are not in the schema for '{ref}' and will not be written.",
                UserWarning,
                stacklevel=4,
            )
        present = [c for c in columns if c in frame.columns]
        if not present or frame.empty:
            return
        frame = frame[present]
        encoded = [
            spec.name
            for spec in specs
            if spec.name in present and is_json(spec) and base_type(columns[spec.name]) == STRING
        ]
        if encoded:
            frame = frame.assign(**{name: frame[name].map(_to_json) for name in encoded})
        self.connection.client.insert_df(
            table=ref,
            df=frame,
            column_names=present,
            column_type_names=[columns[c] for c in present],
            settings=settings,
        )

    # -- Replacing ---------------------------------------------------------------

    def _replace(self, context: IOContext, partitions: Sequence[Partition | None], data: Any) -> None:
        """Replace partitions of the target with the data, through a staging table.

        The data goes into a fresh staging table created ``AS`` the target, so
        it shares its structure and partition key, as ``REPLACE PARTITION``
        requires. Each partition then moves in with ``REPLACE PARTITION``,
        atomic per partition; one the staging table holds no rows for is
        dropped instead, since newer servers refuse to replace from an empty
        partition. The staging table is dropped whatever happens.

        Args:
            context: IO context carrying the target asset and the effective schema.
            partitions: The partitions to replace, ``[None]`` for the whole table.
            data: The data to write, in its native representation.
        """
        table, dataset = self._target(context)
        database = self._database(dataset)
        config = context.asset.partitioning
        specs = self._specs(data, context)
        columns = self._ensure_table(table, database, specs, config)
        target = self._ref(table, database)
        staging = self._ref(f"{_STAGING_PREFIX}{table}_{uuid.uuid4().hex[:16]}", database)
        client = self.connection.client
        client.command(f"CREATE TABLE {staging} AS {target}")
        try:
            self._load(staging, columns, specs, data, settings=_STAGING_SETTINGS)
            loaded = self._loaded_ids(staging)
            for clause, has_rows in self._clauses(target, config, columns, partitions, loaded):
                if has_rows:
                    client.command(f"ALTER TABLE {target} REPLACE PARTITION {clause} FROM {staging}")
                else:
                    client.command(f"ALTER TABLE {target} DROP PARTITION {clause}")
        finally:
            client.command(f"DROP TABLE IF EXISTS {staging} SYNC")

    def _loaded_ids(self, ref: str) -> set[str]:
        """Read the ids of the partitions a table holds rows in.

        Args:
            ref: The quoted table reference.

        Returns:
            The partition ids, from the ``_partition_id`` virtual column.
        """
        result = self.connection.client.query(f"SELECT DISTINCT _partition_id FROM {ref}")
        return {partition_id for (partition_id,) in result.result_rows}

    def _clauses(
        self,
        target: str,
        config: PartitionConfig | None,
        columns: dict[str, str],
        partitions: Sequence[Partition | None],
        loaded: set[str],
    ) -> list[tuple[str, bool]]:
        """Name the target partitions a write replaces, and whether the staging table holds rows for each.

        An unpartitioned table is the single partition ``tuple()``. A whole
        write to a partitioned table covers every partition the target or the
        staging table holds. Otherwise the write covers its partitions, and
        rows the staging table holds outside them are not moved, with a
        warning.

        Args:
            target: The quoted target table reference.
            config: The asset's partitioning, or ``None``.
            columns: The target's column name to ClickHouse type.
            partitions: The partitions to replace, ``[None]`` for the whole table.
            loaded: The ids of the partitions the staging table holds rows in.

        Returns:
            ``(PARTITION clause, has rows)`` pairs, in write order.
        """
        if config is None:
            return [("tuple()", bool(loaded))]
        if list(partitions) == [None]:
            ids = sorted(loaded | self._loaded_ids(target))
        else:
            ids = self._partition_ids(config, columns[config.column], partitions)
            stray = loaded - set(ids)
            if stray:
                warnings.warn(
                    f"The data holds rows in {len(stray)} partition(s) outside the ones being written to "
                    f"'{target}'; those rows were not written.",
                    UserWarning,
                    stacklevel=4,
                )
        return [(f"ID {_literal(partition_id)}", partition_id in loaded) for partition_id in ids]

    def _partition_ids(
        self, config: PartitionConfig, column_type_name: str, partitions: Sequence[Partition | None]
    ) -> list[str]:
        """Compute the ClickHouse partition id of each interloper partition.

        ClickHouse computes them itself (``partitionId``) over the table's own
        partition key applied to each partition's value, so the ids match the
        ones the parts carry whatever the key's type.

        Args:
            config: The asset's partitioning.
            column_type_name: The partition column's ClickHouse type.
            partitions: The partitions to identify.

        Returns:
            One id per distinct partition, in order.
        """
        values = [_partition_value(p) for p in partitions if p is not None]
        key = partition_key(config, f"CAST(v AS {column_type_name})", column_type_name)
        result = self.connection.client.query(
            f"SELECT arrayMap(v -> partitionId({key}), {{values:Array(String)}})",
            parameters={"values": values},
        )
        return list(dict.fromkeys(result.result_rows[0][0]))

    # -- DatabaseDestination hooks -------------------------------------------------

    def insert(self, table: str, dataset: str | None, data: Any, context: IOContext) -> None:
        """Append data to the table, creating it on first write.

        :meth:`write` replaces through a staging table instead; this hook adds
        rows without removing any.

        Args:
            table: Target table name.
            dataset: The database, or ``None`` for the destination's default.
            data: The data in its native representation.
            context: IO context carrying the asset and effective schema.
        """
        database = self._database(dataset)
        specs = self._specs(data, context)
        columns = self._ensure_table(table, database, specs, context.asset.partitioning)
        self._load(self._ref(table, database), columns, specs, data)

    def delete(self, table: str, dataset: str | None, where: PartitionFilter | None) -> None:
        """Delete the rows a filter selects with a lightweight ``DELETE``, or truncate the table.

        :meth:`write` never deletes row by row; this hook serves callers that
        remove rows outside a write. A table that does not exist has nothing
        to delete.

        Args:
            table: Target table name.
            dataset: The database, or ``None`` for the destination's default.
            where: The rows to delete; ``None`` for the whole table.
        """
        database = self._database(dataset)
        columns = self._columns(table, database)
        if not columns:
            return
        ref = self._ref(table, database)
        if where is None:
            self.connection.client.command(f"TRUNCATE TABLE {ref}")
            return
        predicate, parameters = _predicate(where, columns)
        self.connection.client.command(f"DELETE FROM {ref} WHERE {predicate}", parameters=parameters)

    def select(self, table: str, dataset: str | None, where: PartitionFilter | None) -> pd.DataFrame:
        """Select the rows a filter selects, or every row, as a DataFrame.

        Args:
            table: Target table name.
            dataset: The database, or ``None`` for the destination's default.
            where: The rows to select; ``None`` for the whole table.

        Returns:
            The selected rows.
        """
        database = self._database(dataset)
        columns = self._existing(table, database)
        ref = self._ref(table, database)
        if where is None:
            return self.connection.client.query_df(f"SELECT * FROM {ref}")
        predicate, parameters = _predicate(where, columns)
        return self.connection.client.query_df(f"SELECT * FROM {ref} WHERE {predicate}", parameters=parameters)

    def count(self, table: str, dataset: str | None, column: str) -> dict[str, int]:
        """Return row counts grouped by a column.

        Args:
            table: Target table name.
            dataset: The database, or ``None`` for the destination's default.
            column: Column to group by.

        Returns:
            Mapping from the column's value (as a string) to its row count.
        """
        database = self._database(dataset)
        self._existing(table, database)
        result = self.connection.client.query(
            f"SELECT toString({_quote(column)}) AS partition_value, count() AS cnt "
            f"FROM {self._ref(table, database)} GROUP BY partition_value"
        )
        return {value: count for value, count in result.result_rows}


# -- Utility functions -------------------------------------------------------------


def _quote(identifier: str) -> str:
    """Quote an identifier for ClickHouse, escaping backslashes and backticks.

    Args:
        identifier: A database, table or column name.

    Returns:
        The backtick-quoted identifier.
    """
    escaped = identifier.replace("\\", "\\\\").replace("`", "\\`")
    return f"`{escaped}`"


def _literal(value: str) -> str:
    """Render a string literal for ClickHouse, escaping backslashes and quotes.

    Args:
        value: The string.

    Returns:
        The single-quoted literal.
    """
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _partition_value(partition: Partition) -> str:
    """Render the value a partition's rows carry in its partition column, as text ClickHouse casts.

    Args:
        partition: The partition.

    Returns:
        A time partition's period start, a datetime with a space separator;
        any other partition's id.
    """
    if isinstance(partition, TimePartition):
        value = partition.value
        if isinstance(value, datetime.datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return value.isoformat()
    return partition.id


def _predicate(where: PartitionFilter, columns: dict[str, str]) -> tuple[str, dict[str, Any]]:
    """Render a partition filter as a predicate with server-side bound parameters.

    Each parameter is typed as the column, so ClickHouse parses a partition id
    that arrives as text into a ``Date32`` or an ``Int64``. Against a
    ``String`` column, date and datetime bounds are rendered in ISO 8601, the
    form the rows carry.

    Args:
        where: The filter to render.
        columns: The table's column name to ClickHouse type.

    Returns:
        The predicate text and its parameters.
    """
    column = _quote(where.column)
    type_name = columns.get(where.column, STRING)
    if where.bounds is None:
        return f"{column} = {{value:{type_name}}}", {"value": where.value}
    start, end = where.bounds
    if base_type(type_name) == STRING:
        start, end = (_iso(bound) for bound in (start, end))
    return f"{column} >= {{start:{type_name}}} AND {column} < {{end:{type_name}}}", {"start": start, "end": end}


def _iso(value: Any) -> Any:
    """Render a date or datetime as ISO 8601, leaving any other value as is.

    Args:
        value: A partition bound.

    Returns:
        The ISO string for a date or datetime, else *value*.
    """
    return value.isoformat() if isinstance(value, datetime.date) else value


def _to_json(value: Any) -> Any:
    """Encode a value as JSON text for a ``String`` column.

    Args:
        value: A cell of a field stored as JSON text.

    Returns:
        ``None`` for a missing value, text and bytes unchanged, the JSON
        encoding of anything else.
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, (str, bytes)):
        return value
    if hasattr(value, "tolist"):
        value = value.tolist()
    return json.dumps(replace_non_finite(value), default=json_default)
