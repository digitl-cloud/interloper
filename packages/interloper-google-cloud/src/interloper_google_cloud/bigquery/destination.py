"""BigQuery destination implementation."""

from __future__ import annotations

import datetime
import inspect
import json
import warnings
from collections.abc import Sequence
from decimal import Decimal
from functools import cached_property
from typing import Any

import google.auth
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import Conflict, NotFound
from google.oauth2 import service_account
from interloper.destination import IOContext, destination
from interloper.destination.database import DatabaseDestination
from interloper.errors import ConfigError, DataNotFoundError
from interloper.partitioning import PartitionConfig, TimePartitionConfig
from interloper.representation import Representation
from interloper.resource.fields import FetchField, InputField, SelectField
from interloper.schema import FieldSpec, Schema

from interloper_google_cloud.connection import GoogleCloudConnection
from interloper_google_cloud.serialization import json_default, replace_non_finite


@destination(
    key="bigquery_destination",
    name="BigQuery",
    icon="icon:bigquery",
    tags=["Cloud"],
    read_representation="dataframe",
)
class BigQueryDestination(DatabaseDestination):
    """BigQuery destination."""

    connection: GoogleCloudConnection

    project: str = FetchField(
        provider="connection.projects",
        label_key="name",
        value_key="project_id",
        description="Google Cloud project ID",
        discriminator=True,
    )
    location: str = SelectField(
        description="BigQuery dataset location",
        options=[
            {"label": "EU", "value": "EU"},
            {"label": "US", "value": "US"},
        ],
    )
    default_dataset: str | None = InputField(default=None, description="Default BigQuery dataset")

    @cached_property
    def client(self) -> bigquery.Client:
        if self.connection and self.connection.service_account_key:
            key_info = json.loads(self.connection.service_account_key)
            credentials = service_account.Credentials.from_service_account_info(key_info)
        else:
            credentials, _ = google.auth.default()

        return bigquery.Client(
            project=self.project,
            credentials=credentials,
            location=self.location,
        )

    # -- Helpers ---------------------------------------------------------------

    def _resolve_dataset(self, schema: str | None) -> str:
        """Return the BigQuery dataset to use.

        Prefers ``schema`` (from the asset's ``dataset``).  Falls back to
        the destination's ``dataset`` field.

        Args:
            schema: Schema parameter from the asset context.

        Returns:
            The resolved dataset name.

        Raises:
            ConfigError: If neither *schema* nor *dataset* is set.
        """
        ds = schema or self.default_dataset
        if ds is None:
            raise ConfigError(
                "BigQueryDestination requires a dataset. Either set 'dataset' on the asset "
                "or provide 'default_dataset' on the destination."
            )
        return ds

    def _table_ref(self, table: str, schema: str | None) -> str:
        """Build a fully-qualified BigQuery table reference.

        Args:
            table: Table name.
            schema: Schema (dataset) override.

        Returns:
            ``project.dataset.table`` string.
        """
        ds = self._resolve_dataset(schema)
        return f"{self.project}.{ds}.{table}"

    def _get_table(self, table: str, schema: str | None) -> bigquery.Table | None:
        """Fetch a BigQuery table.

        Args:
            table: Table name.
            schema: Schema (dataset) override.

        Returns:
            The table, or ``None`` if it does not exist.
        """
        try:
            return self.client.get_table(self._table_ref(table, schema))
        except NotFound:
            return None

    def _table_exists(self, table: str, schema: str | None) -> bool:
        """Check whether a BigQuery table exists.

        Args:
            table: Table name.
            schema: Schema (dataset) override.

        Returns:
            ``True`` if the table exists, ``False`` otherwise.
        """
        return self._get_table(table, schema) is not None

    def _create_table(
        self,
        table: str,
        schema: str | None,
        bq_schema: list[bigquery.SchemaField],
        time_partitioning: bigquery.TimePartitioning | None = None,
        description: str | None = None,
    ) -> None:
        """Create a BigQuery table with an explicit schema.

        Args:
            table: Target table name.
            schema: Database schema (dataset).
            bq_schema: BigQuery field definitions.
            time_partitioning: Time partitioning spec, if the asset is partitioned.
            description: Table description (the asset's description).
        """
        bq_table = bigquery.Table(self._table_ref(table, schema), schema=bq_schema)
        bq_table.time_partitioning = time_partitioning
        bq_table.description = description
        self.client.create_table(bq_table)

    def _sync_table_metadata(
        self,
        bq_table: bigquery.Table,
        bq_schema: list[bigquery.SchemaField] | None,
        description: str | None,
    ) -> None:
        """Push field and table descriptions onto an existing table.

        Keeps BigQuery metadata in sync when descriptions change on the asset
        or its schema after the table was created.  Only descriptions are
        updated (types, modes, and partitioning are immutable here), empty
        descriptions never clear existing ones, and no API call is made when
        nothing changed.

        Args:
            bq_table: The existing table.
            bq_schema: Field definitions carrying the wanted descriptions.
            description: Wanted table description (the asset's description).
        """
        update_fields = []
        if bq_schema is not None:
            merged, changed = _merge_field_descriptions(bq_table.schema, bq_schema)
            if changed:
                bq_table.schema = merged
                update_fields.append("schema")
        if description and bq_table.description != description:
            bq_table.description = description
            update_fields.append("description")
        if update_fields:
            self.client.update_table(bq_table, update_fields)

    def _ensure_dataset(self, schema: str | None) -> None:
        """Create the BigQuery dataset if it does not already exist.

        Args:
            schema: Schema (dataset) override.
        """
        ds = self._resolve_dataset(schema)
        dataset_ref = bigquery.DatasetReference(self.project, ds)
        try:
            self.client.get_dataset(dataset_ref)
        except NotFound:
            bq_dataset = bigquery.Dataset(dataset_ref)
            bq_dataset.location = self.client.location
            try:
                self.client.create_dataset(bq_dataset)
            except Conflict:
                pass  # Created by a concurrent asset — already exists

    # -- DatabaseDestination hooks ---------------------------------------------

    def _insert_data(self, table: str, schema: str | None, data: Any, context: IOContext) -> None:
        """Insert data into BigQuery, schema-driven when a schema is available.

        The effective schema from the IO context (declared on the asset, or
        inferred during conform) drives both table DDL and the load job's
        schema, so the table shape is deterministic and stable across
        partitions.  DataFrames load natively via Parquet
        (``load_table_from_dataframe``); other data falls back to the
        JSON-rows path.

        New tables are created with the asset's partitioning (daily time
        partitioning on the partition column) and carry field and table
        descriptions; on existing tables, descriptions are kept in sync.

        Args:
            table: Target table name.
            schema: Database schema (dataset).
            data: The data in its native format.
            context: IO context carrying the asset and effective schema.
        """
        bq_schema = _schema_to_bq_fields(context.schema) if context.schema is not None else None
        description = _asset_description(context.asset)
        partitioning = context.asset.partitioning

        bq_table = self._get_table(table, schema)
        creating = bq_table is None
        if creating:
            self._ensure_dataset(schema)
            if bq_schema is not None:
                tp = _time_partitioning(partitioning, bq_schema)
                self._create_table(table, schema, bq_schema, time_partitioning=tp, description=description)
            elif not isinstance(data, pd.DataFrame):
                rows = Representation.of(data).to_records(data)
                if not rows:
                    return
                inferred = _infer_bq_schema(rows)
                tp = _time_partitioning(partitioning, inferred)
                self._create_table(table, schema, inferred, time_partitioning=tp, description=description)
            # DataFrame without schema: the load job creates the table from dtypes,
            # with the partitioning spec passed on the job config below.
        else:
            self._sync_table_metadata(bq_table, bq_schema, description)

        ref = self._table_ref(table, schema)
        if isinstance(data, pd.DataFrame):
            tp = _time_partitioning(partitioning, bq_schema) if creating and bq_schema is None else None
            self._load_dataframe(ref, data, bq_schema, time_partitioning=tp)
        else:
            rows = Representation.of(data).to_records(data)
            if rows:
                self._load_rows(ref, rows, bq_schema)

    def _load_dataframe(
        self,
        ref: str,
        df: pd.DataFrame,
        bq_schema: list[bigquery.SchemaField] | None,
        time_partitioning: bigquery.TimePartitioning | None = None,
    ) -> None:
        """Load a DataFrame via a Parquet load job.

        When a schema is available, columns are aligned to it: extra columns
        are dropped (with a warning) and the load job receives explicit field
        types, so pyarrow casts values (including ``NaN`` → ``NULL``) instead
        of relying on dtype autodetection.

        Args:
            ref: Fully-qualified table reference.
            df: The DataFrame to load.
            bq_schema: BigQuery field definitions, or ``None`` to autodetect.
            time_partitioning: Partitioning spec for the table the load job is
                about to create; ``None`` when the table already exists.
        """
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        if time_partitioning is not None:
            job_config.time_partitioning = time_partitioning

        if bq_schema is not None:
            schema_columns = [field.name for field in bq_schema]
            extras = [str(c) for c in df.columns if str(c) not in schema_columns]
            if extras:
                warnings.warn(
                    f"Columns {extras} are not in the schema for '{ref}' and will not be written.",
                    UserWarning,
                    stacklevel=2,
                )
            present = [c for c in schema_columns if c in df.columns]
            df = df[present]
            job_config.schema = [field for field in bq_schema if field.name in present]

        job = self.client.load_table_from_dataframe(df, ref, job_config=job_config)
        job.result()

    def _load_rows(self, ref: str, rows: list[dict[str, Any]], bq_schema: list[bigquery.SchemaField] | None) -> None:
        """Load row dicts via a newline-delimited JSON load job.

        Args:
            ref: Fully-qualified table reference.
            rows: Row data as list of dicts.
            bq_schema: BigQuery field definitions, or ``None`` to autodetect.
        """
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        if bq_schema is not None:
            job_config.schema = bq_schema
        safe_rows = [replace_non_finite(json.loads(json.dumps(row, default=json_default))) for row in rows]

        job = self.client.load_table_from_json(safe_rows, ref, job_config=job_config)
        job.result()

    def _insert(self, table: str, schema: str | None, rows: list[dict[str, Any]]) -> None:
        """Insert rows into BigQuery using a JSON load job.

        Args:
            table: Target table name.
            schema: Database schema (dataset).
            rows: Row data as list of dicts.
        """
        self._load_rows(self._table_ref(table, schema), rows, None)

    def _delete_all(self, table: str, schema: str | None) -> None:
        """Truncate all rows from the BigQuery table.

        No-op when the table does not exist yet.

        Args:
            table: Target table name.
            schema: Database schema (dataset).
        """
        if not self._table_exists(table, schema):
            return
        ref = self._table_ref(table, schema)
        self.client.query(f"TRUNCATE TABLE `{ref}`").result()

    def _delete_partition(self, table: str, schema: str | None, column: str, value: Any) -> None:
        """Delete rows matching a partition value.

        No-op when the table does not exist yet.

        Args:
            table: Target table name.
            schema: Database schema (dataset).
            column: Partition column name.
            value: Partition value to match.
        """
        bq_table = self._get_table(table, schema)
        if bq_table is None:
            return
        ref = self._table_ref(table, schema)
        query = f"DELETE FROM `{ref}` WHERE `{column}` = @partition_value"
        job_config = bigquery.QueryJobConfig(query_parameters=[_partition_param(bq_table, column, value)])
        self.client.query(query, job_config=job_config).result()

    def _select_all(self, table: str, schema: str | None) -> list[dict[str, Any]]:
        """Select all rows from the BigQuery table.

        Args:
            table: Target table name.
            schema: Database schema (dataset).

        Returns:
            All rows as list of dicts.

        Raises:
            DataNotFoundError: If the table does not exist.
        """
        if not self._table_exists(table, schema):
            qualified = self._table_ref(table, schema)
            raise DataNotFoundError(f"Table '{qualified}' does not exist. Has the asset been materialized?")
        ref = self._table_ref(table, schema)
        rows = self.client.query(f"SELECT * FROM `{ref}`").result()
        return [dict(row) for row in rows]

    def _select_partition(
        self,
        table: str,
        schema: str | None,
        column: str,
        value: Any,
    ) -> list[dict[str, Any]]:
        """Select rows matching a partition value.

        Args:
            table: Target table name.
            schema: Database schema (dataset).
            column: Partition column name.
            value: Partition value to match.

        Returns:
            Matching rows as list of dicts.

        Raises:
            DataNotFoundError: If the table does not exist.
        """
        bq_table = self._get_table(table, schema)
        if bq_table is None:
            qualified = self._table_ref(table, schema)
            raise DataNotFoundError(f"Table '{qualified}' does not exist. Has the asset been materialized?")
        ref = self._table_ref(table, schema)
        query = f"SELECT * FROM `{ref}` WHERE `{column}` = @partition_value"
        job_config = bigquery.QueryJobConfig(query_parameters=[_partition_param(bq_table, column, value)])
        rows = self.client.query(query, job_config=job_config).result()
        return [dict(row) for row in rows]

    # -- Introspection ---------------------------------------------------------

    def _count_by_partition(
        self,
        table: str,
        schema: str | None,
        column: str,
    ) -> dict[str, int]:
        """Return row counts grouped by partition column via BigQuery SQL.

        Args:
            table: Target table name.
            schema: Database schema (dataset).
            column: Column to group by.

        Returns:
            Mapping from partition value (as string) to row count.

        Raises:
            DataNotFoundError: If the table does not exist.
        """
        if not self._table_exists(table, schema):
            ref = self._table_ref(table, schema)
            raise DataNotFoundError(f"Table '{ref}' does not exist. Has the asset been materialized?")

        ref = self._table_ref(table, schema)
        query = f"SELECT CAST(`{column}` AS STRING) AS partition_value, COUNT(*) AS cnt FROM `{ref}` GROUP BY 1"
        rows = self.client.query(query).result()
        return {row["partition_value"]: row["cnt"] for row in rows}

    # -- Lifecycle -------------------------------------------------------------

    def dispose(self) -> None:
        if self.client:
            self.client.close()


# -- Utility functions ---------------------------------------------------------


def _schema_to_bq_fields(schema: type[Schema]) -> list[bigquery.SchemaField]:
    """Map an interloper Schema to BigQuery field definitions.

    Args:
        schema: The schema class to map.

    Returns:
        One ``SchemaField`` per data field, nested models as ``RECORD``.
    """
    return _specs_to_bq_fields(schema.field_specs())


def _specs_to_bq_fields(specs: list[FieldSpec] | tuple[FieldSpec, ...]) -> list[bigquery.SchemaField]:
    """Map field specs to BigQuery field definitions.

    Returns:
        One ``SchemaField`` per spec, carrying the spec's description.
    """
    fields = []
    for spec in specs:
        mode = "REPEATED" if spec.repeated else ("NULLABLE" if spec.nullable else "REQUIRED")
        # SchemaField's description default is a sentinel, not None — only pass it when set.
        described: dict[str, Any] = {"description": spec.description} if spec.description else {}
        if spec.fields is not None:
            fields.append(
                bigquery.SchemaField(
                    spec.name, "RECORD", mode=mode, fields=_specs_to_bq_fields(spec.fields), **described
                )
            )
        else:
            fields.append(bigquery.SchemaField(spec.name, _py_type_to_bq_type(spec.type), mode=mode, **described))
    return fields


def _asset_description(asset: Any) -> str | None:
    """Return the asset's description (its class docstring), cleaned.

    This mirrors how ``Component.definition()`` derives descriptions.

    Returns:
        The cleaned docstring, or ``None`` when the asset has none.
    """
    doc = type(asset).__doc__
    return inspect.cleandoc(doc) if doc else None


# BigQuery time partitioning only supports DATE / DATETIME / TIMESTAMP columns.
_TIME_PARTITIONABLE_TYPES = {"DATE", "DATETIME", "TIMESTAMP"}


def _time_partitioning(
    config: PartitionConfig | None,
    bq_schema: list[bigquery.SchemaField] | None,
) -> bigquery.TimePartitioning | None:
    """Resolve the asset's partition config into a BigQuery time partitioning spec.

    Daily partitioning on the partition column, when BigQuery supports it:
    the column must be DATE / DATETIME / TIMESTAMP.  When the column type is
    known (from the schema) and is not time-partitionable, or the config is
    not time-based and no schema is available, returns ``None`` — the table
    is created unpartitioned, which is always valid.

    Args:
        config: The asset's partition config, if any.
        bq_schema: The table's field definitions, when known.

    Returns:
        A daily ``TimePartitioning`` on the partition column, or ``None``.
    """
    if config is None:
        return None

    if bq_schema is not None:
        field = next((f for f in bq_schema if f.name == config.column), None)
        if field is None or field.field_type not in _TIME_PARTITIONABLE_TYPES:
            if isinstance(config, TimePartitionConfig):
                warnings.warn(
                    f"Partition column '{config.column}' is "
                    f"{'missing from the schema' if field is None else f'of type {field.field_type}'}; "
                    "the BigQuery table will not be time-partitioned.",
                    UserWarning,
                    stacklevel=2,
                )
            return None
    elif not isinstance(config, TimePartitionConfig):
        return None

    return bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field=config.column)


def _merge_field_descriptions(
    existing: Sequence[bigquery.SchemaField],
    desired: Sequence[bigquery.SchemaField],
) -> tuple[list[bigquery.SchemaField], bool]:
    """Overlay desired field descriptions onto an existing table schema.

    Only descriptions are touched — types and modes stay as they are in the
    table, so the result is always a valid ``update_table`` schema.  Empty
    desired descriptions never clear an existing one (e.g. set manually in
    the BigQuery console).

    Args:
        existing: The table's current field definitions.
        desired: Field definitions carrying the wanted descriptions.

    Returns:
        The merged field list and whether anything changed.
    """
    desired_by_name = {f.name: f for f in desired}
    merged: list[bigquery.SchemaField] = []
    changed = False
    for field in existing:
        want = desired_by_name.get(field.name)
        if want is None:
            merged.append(field)
            continue
        api = field.to_api_repr()
        if want.description and want.description != field.description:
            api["description"] = want.description
            changed = True
        if field.field_type == "RECORD" and want.fields:
            sub_fields, sub_changed = _merge_field_descriptions(field.fields, want.fields)
            if sub_changed:
                api["fields"] = [f.to_api_repr() for f in sub_fields]
                changed = True
        merged.append(bigquery.SchemaField.from_api_repr(api))
    return merged, changed


# BigQuery column type -> query parameter type, for partition predicates.
_BQ_FIELD_TO_PARAM_TYPE = {
    "BOOLEAN": "BOOL",
    "BOOL": "BOOL",
    "INTEGER": "INT64",
    "INT64": "INT64",
    "FLOAT": "FLOAT64",
    "FLOAT64": "FLOAT64",
    "NUMERIC": "NUMERIC",
    "DATE": "DATE",
    "DATETIME": "DATETIME",
    "TIMESTAMP": "TIMESTAMP",
    "BYTES": "BYTES",
    "STRING": "STRING",
}


def _partition_param(
    bq_table: bigquery.Table,
    column: str,
    value: Any,
) -> bigquery.ScalarQueryParameter:
    """Build the partition-predicate query parameter, typed from the table.

    Partition values arrive as strings (``Partition.id``), but the column may
    be DATE / TIMESTAMP / INTEGER — BigQuery does not coerce a STRING
    parameter in an equality predicate, so the parameter type must match the
    actual column type.  Falls back to inferring from the value when the
    column is not in the table schema.

    Args:
        bq_table: The target table (for column type lookup).
        column: Partition column name.
        value: Partition value.

    Returns:
        A ``partition_value`` scalar parameter with the matching type.
    """
    field = next((f for f in bq_table.schema if f.name == column), None)
    if field is not None:
        param_type = _BQ_FIELD_TO_PARAM_TYPE.get(field.field_type, "STRING")
    else:
        param_type = _bq_to_py_type(value)
    if param_type in ("TIMESTAMP", "DATETIME") and isinstance(value, str):
        # The client serializes these from datetime objects; a date-only
        # string like "2024-01-01" (a TimePartition id) fails to format.
        try:
            value = datetime.datetime.fromisoformat(value)
        except ValueError:
            pass
    return bigquery.ScalarQueryParameter("partition_value", param_type, value)


def _py_type_to_bq_type(py_type: Any) -> str:
    """Map a Python *type* (from a FieldSpec) to a BigQuery column type."""
    if not isinstance(py_type, type):
        return "STRING"  # typing.Any or unresolvable annotations
    if issubclass(py_type, bool):
        return "BOOLEAN"
    if issubclass(py_type, int):
        return "INTEGER"
    if issubclass(py_type, float):
        return "FLOAT"
    if issubclass(py_type, Decimal):
        return "NUMERIC"
    if issubclass(py_type, datetime.datetime):
        return "TIMESTAMP"
    if issubclass(py_type, datetime.date):
        return "DATE"
    if issubclass(py_type, bytes):
        return "BYTES"
    return "STRING"


def _infer_bq_schema(rows: list[dict[str, Any]]) -> list[bigquery.SchemaField]:
    """Infer a BigQuery schema from row values, scanning all rows.

    Unlike first-row inference, a column that is ``None`` in early rows takes
    its type from the first non-null value anywhere; ``int`` widens to
    ``FLOAT`` when mixed with floats.  All-null columns fall back to STRING.

    Args:
        rows: Row data (non-empty).

    Returns:
        Inferred field definitions, all NULLABLE.
    """
    types_seen: dict[str, set[str]] = {}
    for row in rows:
        for key, value in row.items():
            seen = types_seen.setdefault(key, set())
            if value is not None:
                seen.add(_py_to_bq_type(value))

    fields = []
    for key, seen in types_seen.items():
        if not seen:
            bq_type = "STRING"
        elif len(seen) == 1:
            bq_type = next(iter(seen))
        elif seen == {"INTEGER", "FLOAT"}:
            bq_type = "FLOAT"
        else:
            bq_type = "STRING"
        fields.append(bigquery.SchemaField(key, bq_type, mode="NULLABLE"))
    return fields


def _py_to_bq_type(value: Any) -> str:
    """Infer a BigQuery field type from a Python value."""
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "FLOAT"
    if isinstance(value, Decimal):
        return "NUMERIC"
    if isinstance(value, datetime.datetime):
        return "TIMESTAMP"
    if isinstance(value, datetime.date):
        return "DATE"
    if isinstance(value, bytes):
        return "BYTES"
    return "STRING"


def _bq_to_py_type(value: Any) -> str:
    """Map a Python value to a BigQuery query parameter type."""
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, float):
        return "FLOAT64"
    if isinstance(value, Decimal):
        return "NUMERIC"
    if isinstance(value, datetime.datetime):
        return "TIMESTAMP"
    if isinstance(value, datetime.date):
        return "DATE"
    if isinstance(value, bytes):
        return "BYTES"
    return "STRING"
