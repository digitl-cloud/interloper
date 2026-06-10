"""BigQuery destination implementation."""

from __future__ import annotations

import datetime
import json
import math
import warnings
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
from interloper.representation import representation_for
from interloper.resource.fields import InputField, SelectField
from interloper.schema import FieldSpec, Schema

from interloper_google_cloud.connection import GoogleCloudConnection


@destination(
    key="bigquery_destination",
    name="BigQuery",
    icon="icon:bigquery",
    tags=["Cloud"],
)
class BigQueryDestination(DatabaseDestination):
    """BigQuery destination."""

    connection: GoogleCloudConnection

    # Reads materialize as DataFrames for downstream assets.
    read_representation: str = "dataframe"

    # Config fields (previously on BigQueryConfig)
    project: str = InputField(description="Google Cloud project ID")
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

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

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

    def _table_exists(self, table: str, schema: str | None) -> bool:
        """Check whether a BigQuery table exists.

        Args:
            table: Table name.
            schema: Schema (dataset) override.

        Returns:
            ``True`` if the table exists, ``False`` otherwise.
        """
        try:
            self.client.get_table(self._table_ref(table, schema))
        except NotFound:
            return False
        return True

    def _create_table(self, table: str, schema: str | None, bq_schema: list[bigquery.SchemaField]) -> None:
        """Create a BigQuery table with an explicit schema.

        Args:
            table: Target table name.
            schema: Database schema (dataset).
            bq_schema: BigQuery field definitions.
        """
        bq_table = bigquery.Table(self._table_ref(table, schema), schema=bq_schema)
        self.client.create_table(bq_table)

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

    # ------------------------------------------------------------------
    # DatabaseDestination hooks
    # ------------------------------------------------------------------

    def _insert_data(self, table: str, schema: str | None, data: Any, context: IOContext) -> None:
        """Insert data into BigQuery, schema-driven when a schema is available.

        The effective schema from the IO context (declared on the asset, or
        inferred during conform) drives both table DDL and the load job's
        schema, so the table shape is deterministic and stable across
        partitions.  DataFrames load natively via Parquet
        (``load_table_from_dataframe``); other data falls back to the
        JSON-rows path.

        Args:
            table: Target table name.
            schema: Database schema (dataset).
            data: The data in its native format.
            context: IO context carrying the asset and effective schema.
        """
        bq_schema = _schema_to_bq_fields(context.schema) if context.schema is not None else None

        if not self._table_exists(table, schema):
            self._ensure_dataset(schema)
            if bq_schema is not None:
                self._create_table(table, schema, bq_schema)
            elif not isinstance(data, pd.DataFrame):
                rows = representation_for(data).to_records(data)
                if not rows:
                    return
                self._create_table(table, schema, _infer_bq_schema(rows))
            # DataFrame without schema: the load job creates the table from dtypes.

        ref = self._table_ref(table, schema)
        if isinstance(data, pd.DataFrame):
            self._load_dataframe(ref, data, bq_schema)
        else:
            rows = representation_for(data).to_records(data)
            if rows:
                self._load_rows(ref, rows, bq_schema)

    def _load_dataframe(self, ref: str, df: pd.DataFrame, bq_schema: list[bigquery.SchemaField] | None) -> None:
        """Load a DataFrame via a Parquet load job.

        When a schema is available, columns are aligned to it: extra columns
        are dropped (with a warning) and the load job receives explicit field
        types, so pyarrow casts values (including ``NaN`` → ``NULL``) instead
        of relying on dtype autodetection.

        Args:
            ref: Fully-qualified table reference.
            df: The DataFrame to load.
            bq_schema: BigQuery field definitions, or ``None`` to autodetect.
        """
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

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
        safe_rows = [_replace_non_finite(json.loads(json.dumps(row, default=_json_default))) for row in rows]

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
        if not self._table_exists(table, schema):
            return
        ref = self._table_ref(table, schema)
        query = f"DELETE FROM `{ref}` WHERE `{column}` = @partition_value"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("partition_value", _bq_to_py_type(value), value)],
        )
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
        if not self._table_exists(table, schema):
            qualified = self._table_ref(table, schema)
            raise DataNotFoundError(f"Table '{qualified}' does not exist. Has the asset been materialized?")
        ref = self._table_ref(table, schema)
        query = f"SELECT * FROM `{ref}` WHERE `{column}` = @partition_value"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("partition_value", _bq_to_py_type(value), value)],
        )
        rows = self.client.query(query, job_config=job_config).result()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def dispose(self) -> None:
        if self.client:
            self.client.close()


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------


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
        One ``SchemaField`` per spec.
    """
    fields = []
    for spec in specs:
        mode = "REPEATED" if spec.repeated else ("NULLABLE" if spec.nullable else "REQUIRED")
        if spec.fields is not None:
            fields.append(
                bigquery.SchemaField(spec.name, "RECORD", mode=mode, fields=_specs_to_bq_fields(spec.fields))
            )
        else:
            fields.append(bigquery.SchemaField(spec.name, _py_type_to_bq_type(spec.type), mode=mode))
    return fields


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


def _replace_non_finite(obj: Any) -> Any:
    """Recursively replace non-finite floats (``NaN``, ``Infinity``) with ``None``.

    pandas represents missing numeric values as ``float('nan')``. Python's
    ``json.dumps`` (used by ``load_table_from_json``) serialises these to the
    bare tokens ``NaN`` / ``Infinity``, which are invalid JSON — BigQuery's load
    parser rejects them with "Parser terminated before end of string". BigQuery
    has no NaN concept on the load path, so map non-finite floats to SQL ``NULL``.
    """
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _replace_non_finite(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_replace_non_finite(v) for v in obj]
    return obj


def _json_default(o: Any) -> Any:
    """JSON serializer for types not handled by the default encoder."""
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return str(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


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
