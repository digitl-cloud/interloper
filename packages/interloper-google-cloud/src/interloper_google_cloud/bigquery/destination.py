"""BigQuery destination implementation."""

from __future__ import annotations

import datetime
import json
from decimal import Decimal
from functools import cached_property
from typing import Any

import google.auth
from google.cloud import bigquery
from google.cloud.exceptions import Conflict, NotFound
from google.oauth2 import service_account
from interloper.destination import destination
from interloper.destination.adapter import DataAdapter
from interloper.destination.database import DatabaseDestination
from interloper.errors import ConfigError, DataNotFoundError
from interloper.resource.fields import InputField, SelectField
from interloper_pandas import DataFrameAdapter

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

    @property
    def adapters(self) -> list[DataAdapter]:
        return [DataFrameAdapter()]

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

    def _create_table(self, table: str, schema: str | None, rows: list[dict[str, Any]]) -> None:
        """Create a BigQuery table from sample row data.

        Column types are inferred from the Python values in the first row.

        Args:
            table: Target table name.
            schema: Database schema (dataset).
            rows: Row data (at least one row required for schema inference).
        """
        sample = rows[0]
        bq_schema = [bigquery.SchemaField(name, _py_to_bq_type(value)) for name, value in sample.items()]
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

    def _insert(self, table: str, schema: str | None, rows: list[dict[str, Any]]) -> None:
        """Insert rows into BigQuery using a load job.

        If the table does not exist yet, the dataset is ensured and the table is
        created from the row data before loading.

        Args:
            table: Target table name.
            schema: Database schema (dataset).
            rows: Row data as list of dicts.
        """
        if not self._table_exists(table, schema):
            self._ensure_dataset(schema)
            self._create_table(table, schema, rows)

        ref = self._table_ref(table, schema)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        safe_rows = [json.loads(json.dumps(row, default=_json_default)) for row in rows]

        job = self.client.load_table_from_json(safe_rows, ref, job_config=job_config)
        job.result()

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
