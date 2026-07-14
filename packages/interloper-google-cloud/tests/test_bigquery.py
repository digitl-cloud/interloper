"""Tests for BigQueryDestination."""

import datetime
import json
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import interloper as il
import pytest
from google.cloud import bigquery
from interloper.destination import IOContext
from interloper.errors import ConfigError
from interloper.schema import Schema
from pydantic import BaseModel, Field

from interloper_google_cloud.bigquery.destination import (
    BigQueryDestination,
    _bq_to_py_type,
    _infer_bq_schema,
    _merge_field_descriptions,
    _partition_param,
    _py_to_bq_type,
    _schema_to_bq_fields,
    _time_partitioning,
)
from interloper_google_cloud.connection import GoogleCloudConnection

# A minimal service account key JSON for testing.
_SA_KEY = json.dumps({"type": "service_account", "project_id": "test-proj"})


def _make_destination(**overrides: Any) -> tuple[BigQueryDestination, MagicMock]:
    """Create a BigQueryDestination with mocked GCP clients."""
    project = overrides.pop("_project", "test-proj")
    mock_client = MagicMock()
    mock_client.project = project
    mock_client.location = "EU"

    # Build mock connection resource
    conn = MagicMock(spec=GoogleCloudConnection)
    conn.service_account_key = _SA_KEY

    dest = BigQueryDestination(  # ty: ignore[missing-argument]
        id="test",
        project=project,
        location="EU",
        default_dataset=overrides.get("dataset", None),
        resources={"connection": conn},
    )
    object.__setattr__(dest, "client", mock_client)

    # By default, the table "exists" with an empty schema and no description,
    # so the metadata-sync path is a no-op unless a test configures otherwise.
    mock_client.get_table.return_value.schema = []
    mock_client.get_table.return_value.description = None

    return dest, mock_client


# -- _py_to_bq_type ------------------------------------------------------------


class TestPyToBqType:
    """Map BigQuery field types from Python values."""

    def test_bool(self):
        assert _py_to_bq_type(True) == "BOOLEAN"
        assert _py_to_bq_type(False) == "BOOLEAN"

    def test_int(self):
        assert _py_to_bq_type(42) == "INTEGER"
        assert _py_to_bq_type(0) == "INTEGER"
        assert _py_to_bq_type(-1) == "INTEGER"

    def test_float(self):
        assert _py_to_bq_type(3.14) == "FLOAT"
        assert _py_to_bq_type(0.0) == "FLOAT"

    def test_decimal(self):
        assert _py_to_bq_type(Decimal("9.99")) == "NUMERIC"

    def test_datetime(self):
        assert _py_to_bq_type(datetime.datetime(2024, 1, 1, 12, 0)) == "TIMESTAMP"

    def test_date(self):
        assert _py_to_bq_type(datetime.date(2024, 1, 1)) == "DATE"

    def test_bytes(self):
        assert _py_to_bq_type(b"raw") == "BYTES"

    def test_string(self):
        assert _py_to_bq_type("hello") == "STRING"

    def test_none_falls_back_to_string(self):
        assert _py_to_bq_type(None) == "STRING"

    def test_list_falls_back_to_string(self):
        assert _py_to_bq_type([1, 2, 3]) == "STRING"

    def test_dict_falls_back_to_string(self):
        assert _py_to_bq_type({"a": 1}) == "STRING"

    def test_bool_before_int(self):
        """bool is a subclass of int -- ensure bool wins."""
        assert _py_to_bq_type(True) == "BOOLEAN"
        assert _py_to_bq_type(True) != "INTEGER"


# -- _bq_to_py_type ------------------------------------------------------------


class TestBqToPyType:
    """Map Python values to BigQuery query parameter types."""

    def test_bool(self):
        assert _bq_to_py_type(True) == "BOOL"
        assert _bq_to_py_type(False) == "BOOL"

    def test_int(self):
        assert _bq_to_py_type(42) == "INT64"

    def test_float(self):
        assert _bq_to_py_type(3.14) == "FLOAT64"

    def test_decimal(self):
        assert _bq_to_py_type(Decimal("1.5")) == "NUMERIC"

    def test_datetime(self):
        assert _bq_to_py_type(datetime.datetime(2024, 6, 15, 8, 30)) == "TIMESTAMP"

    def test_date(self):
        assert _bq_to_py_type(datetime.date(2024, 6, 15)) == "DATE"

    def test_bytes(self):
        assert _bq_to_py_type(b"\x00") == "BYTES"

    def test_string(self):
        assert _bq_to_py_type("text") == "STRING"

    def test_none_falls_back_to_string(self):
        assert _bq_to_py_type(None) == "STRING"

    def test_bool_before_int(self):
        """bool is a subclass of int -- ensure bool wins."""
        assert _bq_to_py_type(True) == "BOOL"


# -- _insert -------------------------------------------------------------------


class TestInsert:
    """Load-job payload construction."""

    def test_nan_sent_to_bigquery_as_null(self):
        """pandas NaN must reach the load job as JSON null, not the invalid token NaN."""
        dest, mock_client = _make_destination(dataset="ds")
        rows = [
            {"campaign_id": 123, "cost": float("nan"), "clicks": 5.0},
            {"campaign_id": 456, "cost": 1.5, "clicks": float("nan")},
        ]

        dest._insert("tbl", "ds", rows)

        sent_rows = mock_client.load_table_from_json.call_args.args[0]
        assert sent_rows == [
            {"campaign_id": 123, "cost": None, "clicks": 5.0},
            {"campaign_id": 456, "cost": 1.5, "clicks": None},
        ]
        # The serialised payload must be valid JSON (no bare NaN token).
        for row in sent_rows:
            assert "NaN" not in json.dumps(row)


# -- _resolve_dataset ----------------------------------------------------------


class TestResolveDataset:
    """Dataset resolution from schema parameter and dataset."""

    def test_schema_takes_precedence(self):
        dest, _ = _make_destination(dataset="fallback")
        assert dest._resolve_dataset("explicit") == "explicit"

    def test_falls_back_to_dataset(self):
        dest, _ = _make_destination(dataset="fallback")
        assert dest._resolve_dataset(None) == "fallback"

    def test_raises_when_both_none(self):
        dest, _ = _make_destination()
        with pytest.raises(ConfigError, match="BigQueryDestination requires a dataset"):
            dest._resolve_dataset(None)

    def test_empty_string_schema_falls_back(self):
        """Empty string is falsy, so it should fall back to dataset."""
        dest, _ = _make_destination(dataset="fallback")
        assert dest._resolve_dataset("") == "fallback"


# -- _table_ref ----------------------------------------------------------------


class TestTableRef:
    """Fully-qualified BigQuery table reference construction."""

    def test_with_explicit_schema(self):
        dest, _ = _make_destination(dataset="default_ds")
        assert dest._table_ref("my_table", "explicit_ds") == "test-proj.explicit_ds.my_table"

    def test_with_dataset(self):
        dest, _ = _make_destination(dataset="default_ds")
        assert dest._table_ref("my_table", None) == "test-proj.default_ds.my_table"

    def test_raises_without_dataset(self):
        dest, _ = _make_destination()
        with pytest.raises(ConfigError):
            dest._table_ref("my_table", None)


# -- _table_exists -------------------------------------------------------------


class TestTableExists:
    """Table existence check delegates to the BQ client."""

    def test_returns_true_when_found(self):
        dest, mock_client = _make_destination(dataset="ds")
        assert dest._table_exists("tbl", None) is True
        mock_client.get_table.assert_called_once_with("test-proj.ds.tbl")

    def test_returns_false_when_not_found(self):
        from google.cloud.exceptions import NotFound

        dest, mock_client = _make_destination(dataset="ds")
        mock_client.get_table.side_effect = NotFound("nope")
        assert dest._table_exists("tbl", None) is False


# -- dispose -------------------------------------------------------------------


class TestDispose:
    """Lifecycle: dispose closes the client."""

    def test_dispose_closes_client(self):
        dest, mock_client = _make_destination()
        dest.dispose()
        mock_client.close.assert_called_once()


# -- Schema-driven loads -------------------------------------------------------


class _RowSchema(Schema):
    id: int | None = Field(..., description="Row id")
    cost: float | None = Field(...)
    day: datetime.date | None = Field(...)


class _NestedModel(BaseModel):
    city: str = Field(description="City name")
    zip: str | None


class _NestedSchema(Schema):
    name: str
    tags: list[str]
    address: _NestedModel | None = Field(None, description="Postal address")


def _ctx(asset: Any, schema: type[Schema] | None) -> IOContext:
    return IOContext(asset=asset, schema=schema)


@il.asset
def _plain_asset() -> list:
    return []


@il.asset(partitioning=il.TimePartitionConfig(column="day"))
def _partitioned_asset() -> list:
    """Daily rows."""
    return []


class TestSchemaToBqFields:
    """Schema → SchemaField mapping."""

    def test_scalar_types_and_modes(self):
        fields = _schema_to_bq_fields(_RowSchema)
        assert [(f.name, f.field_type, f.mode) for f in fields] == [
            ("id", "INTEGER", "NULLABLE"),
            ("cost", "FLOAT", "NULLABLE"),
            ("day", "DATE", "NULLABLE"),
        ]

    def test_nested_and_repeated(self):
        fields = {f.name: f for f in _schema_to_bq_fields(_NestedSchema)}
        assert fields["name"].mode == "REQUIRED"
        assert fields["tags"].mode == "REPEATED"
        assert fields["tags"].field_type == "STRING"
        assert fields["address"].field_type == "RECORD"
        assert [sub.name for sub in fields["address"].fields] == ["city", "zip"]

    def test_descriptions_carried(self):
        fields = {f.name: f for f in _schema_to_bq_fields(_RowSchema)}
        assert fields["id"].description == "Row id"
        assert fields["cost"].description is None

    def test_nested_descriptions_carried(self):
        fields = {f.name: f for f in _schema_to_bq_fields(_NestedSchema)}
        assert fields["address"].description == "Postal address"
        assert fields["address"].fields[0].description == "City name"
        assert fields["address"].fields[1].description is None


class TestInferBqSchema:
    """Value-based fallback inference scans all rows."""

    def test_none_in_first_row_resolved_from_later_rows(self):
        fields = {f.name: f.field_type for f in _infer_bq_schema([{"a": None, "b": 1}, {"a": 2.5, "b": 2}])}
        assert fields == {"a": "FLOAT", "b": "INTEGER"}

    def test_int_widens_to_float(self):
        fields = {f.name: f.field_type for f in _infer_bq_schema([{"a": 1}, {"a": 2.5}])}
        assert fields == {"a": "FLOAT"}

    def test_all_null_column_falls_back_to_string(self):
        fields = {f.name: f.field_type for f in _infer_bq_schema([{"a": None}])}
        assert fields == {"a": "STRING"}


class TestInsertData:
    """Native insert dispatch: DataFrame → Parquet load, rows → JSON load."""

    def test_dataframe_loads_with_explicit_schema(self):
        import numpy as np
        import pandas as pd

        dest, mock_client = _make_destination(dataset="ds")
        df = pd.DataFrame([{"id": 1, "cost": np.nan, "day": datetime.date(2024, 1, 1)}])

        dest._insert_data("tbl", "ds", df, _ctx(_plain_asset(), _RowSchema))

        call = mock_client.load_table_from_dataframe.call_args
        sent_df, ref = call.args
        assert ref == "test-proj.ds.tbl"
        assert list(sent_df.columns) == ["id", "cost", "day"]
        assert [f.name for f in call.kwargs["job_config"].schema] == ["id", "cost", "day"]

    def test_dataframe_extra_columns_dropped_with_warning(self):
        import pandas as pd

        dest, mock_client = _make_destination(dataset="ds")
        df = pd.DataFrame([{"id": 1, "cost": 1.0, "day": datetime.date(2024, 1, 1), "extra": "x"}])

        with pytest.warns(UserWarning, match="not in the schema"):
            dest._insert_data("tbl", "ds", df, _ctx(_plain_asset(), _RowSchema))

        sent_df = mock_client.load_table_from_dataframe.call_args.args[0]
        assert "extra" not in sent_df.columns

    def test_table_created_from_schema_when_missing(self):
        from google.cloud.exceptions import NotFound

        dest, mock_client = _make_destination(dataset="ds")
        mock_client.get_table.side_effect = NotFound("nope")

        dest._insert_data("tbl", "ds", [{"id": 1, "cost": 1.0, "day": None}], _ctx(_plain_asset(), _RowSchema))

        created = mock_client.create_table.call_args.args[0]
        assert [f.name for f in created.schema] == ["id", "cost", "day"]

    def test_table_created_from_inference_without_schema(self):
        from google.cloud.exceptions import NotFound

        dest, mock_client = _make_destination(dataset="ds")
        mock_client.get_table.side_effect = NotFound("nope")

        dest._insert_data("tbl", "ds", [{"a": None}, {"a": 2}], _ctx(_plain_asset(), None))

        created = mock_client.create_table.call_args.args[0]
        assert [(f.name, f.field_type) for f in created.schema] == [("a", "INTEGER")]

    def test_dataframe_without_schema_lets_load_job_create_table(self):
        import pandas as pd
        from google.cloud.exceptions import NotFound

        dest, mock_client = _make_destination(dataset="ds")
        mock_client.get_table.side_effect = NotFound("nope")

        dest._insert_data("tbl", "ds", pd.DataFrame([{"a": 1}]), _ctx(_plain_asset(), None))

        assert not mock_client.create_table.called
        assert mock_client.load_table_from_dataframe.called

    def test_rows_load_carries_schema(self):
        dest, mock_client = _make_destination(dataset="ds")
        dest._insert_data("tbl", "ds", [{"id": 1, "cost": float("nan"), "day": None}], _ctx(_plain_asset(), _RowSchema))

        call = mock_client.load_table_from_json.call_args
        assert call.args[0] == [{"id": 1, "cost": None, "day": None}]  # NaN sanitized
        assert [f.name for f in call.kwargs["job_config"].schema] == ["id", "cost", "day"]


# -- Partitioning and table metadata -------------------------------------------


class TestTimePartitioning:
    """Partition config → BigQuery time partitioning spec."""

    def test_none_without_config(self):
        assert _time_partitioning(None, None) is None

    def test_time_config_with_date_column(self):
        tp = _time_partitioning(il.TimePartitionConfig(column="day"), _schema_to_bq_fields(_RowSchema))
        assert tp is not None
        assert tp.type_ == "DAY"
        assert tp.field == "day"

    def test_time_config_without_schema(self):
        tp = _time_partitioning(il.TimePartitionConfig(column="date"), None)
        assert tp is not None
        assert tp.field == "date"

    def test_generic_config_with_date_column(self):
        tp = _time_partitioning(il.PartitionConfig(column="day"), _schema_to_bq_fields(_RowSchema))
        assert tp is not None
        assert tp.field == "day"

    def test_generic_config_without_schema(self):
        assert _time_partitioning(il.PartitionConfig(column="x"), None) is None

    def test_non_time_column_warns_and_skips(self):
        with pytest.warns(UserWarning, match="not be time-partitioned"):
            tp = _time_partitioning(il.TimePartitionConfig(column="id"), _schema_to_bq_fields(_RowSchema))
        assert tp is None

    def test_missing_column_warns_and_skips(self):
        with pytest.warns(UserWarning, match="not be time-partitioned"):
            tp = _time_partitioning(il.TimePartitionConfig(column="nope"), _schema_to_bq_fields(_RowSchema))
        assert tp is None


class TestCreateTableMetadata:
    """New tables carry partitioning, field descriptions, and table description."""

    def test_table_created_with_partitioning_and_descriptions(self):
        from google.cloud.exceptions import NotFound

        dest, mock_client = _make_destination(dataset="ds")
        mock_client.get_table.side_effect = NotFound("nope")

        dest._insert_data("tbl", "ds", [{"id": 1, "cost": 1.0, "day": None}], _ctx(_partitioned_asset(), _RowSchema))

        created = mock_client.create_table.call_args.args[0]
        assert created.time_partitioning is not None
        assert created.time_partitioning.type_ == "DAY"
        assert created.time_partitioning.field == "day"
        assert created.description == "Daily rows."
        assert {f.name: f.description for f in created.schema}["id"] == "Row id"

    def test_unpartitioned_asset_creates_unpartitioned_table(self):
        from google.cloud.exceptions import NotFound

        dest, mock_client = _make_destination(dataset="ds")
        mock_client.get_table.side_effect = NotFound("nope")

        dest._insert_data("tbl", "ds", [{"id": 1, "cost": 1.0, "day": None}], _ctx(_plain_asset(), _RowSchema))

        created = mock_client.create_table.call_args.args[0]
        assert created.time_partitioning is None
        assert created.description is None

    def test_inferred_schema_table_partitioned_on_date_column(self):
        from google.cloud.exceptions import NotFound

        dest, mock_client = _make_destination(dataset="ds")
        mock_client.get_table.side_effect = NotFound("nope")

        rows = [{"id": 1, "day": datetime.date(2024, 1, 1)}]
        dest._insert_data("tbl", "ds", rows, _ctx(_partitioned_asset(), None))

        created = mock_client.create_table.call_args.args[0]
        assert created.time_partitioning is not None
        assert created.time_partitioning.field == "day"

    def test_dataframe_without_schema_partitions_via_load_job(self):
        import pandas as pd
        from google.cloud.exceptions import NotFound

        dest, mock_client = _make_destination(dataset="ds")
        mock_client.get_table.side_effect = NotFound("nope")

        df = pd.DataFrame([{"id": 1, "day": datetime.date(2024, 1, 1)}])
        dest._insert_data("tbl", "ds", df, _ctx(_partitioned_asset(), None))

        assert not mock_client.create_table.called
        job_config = mock_client.load_table_from_dataframe.call_args.kwargs["job_config"]
        assert job_config.time_partitioning is not None
        assert job_config.time_partitioning.field == "day"

    def test_load_job_into_existing_table_has_no_partitioning_spec(self):
        import pandas as pd

        dest, mock_client = _make_destination(dataset="ds")

        df = pd.DataFrame([{"id": 1, "day": datetime.date(2024, 1, 1)}])
        dest._insert_data("tbl", "ds", df, _ctx(_partitioned_asset(), None))

        job_config = mock_client.load_table_from_dataframe.call_args.kwargs["job_config"]
        assert job_config.time_partitioning is None


class TestMergeFieldDescriptions:
    """Overlaying schema descriptions onto an existing table schema."""

    def test_sets_missing_description(self):
        existing = [bigquery.SchemaField("id", "INTEGER", mode="NULLABLE")]
        desired = [bigquery.SchemaField("id", "INTEGER", mode="NULLABLE", description="Row id")]
        merged, changed = _merge_field_descriptions(existing, desired)
        assert changed is True
        assert merged[0].description == "Row id"
        assert merged[0].field_type == "INTEGER"

    def test_empty_desired_never_clears_existing(self):
        existing = [bigquery.SchemaField("id", "INTEGER", description="Set in console")]
        desired = [bigquery.SchemaField("id", "INTEGER")]
        merged, changed = _merge_field_descriptions(existing, desired)
        assert changed is False
        assert merged[0].description == "Set in console"

    def test_unknown_fields_kept_untouched(self):
        existing = [bigquery.SchemaField("legacy", "STRING")]
        desired = [bigquery.SchemaField("id", "INTEGER", description="Row id")]
        merged, changed = _merge_field_descriptions(existing, desired)
        assert changed is False
        assert merged == existing

    def test_nested_record_descriptions(self):
        existing = [bigquery.SchemaField("address", "RECORD", fields=[bigquery.SchemaField("city", "STRING")])]
        desired = [
            bigquery.SchemaField(
                "address",
                "RECORD",
                fields=[bigquery.SchemaField("city", "STRING", description="City name")],
            )
        ]
        merged, changed = _merge_field_descriptions(existing, desired)
        assert changed is True
        assert merged[0].fields[0].description == "City name"

    def test_existing_table_metadata_synced_on_write(self):
        dest, mock_client = _make_destination(dataset="ds")
        existing = mock_client.get_table.return_value
        existing.schema = [
            bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("cost", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("day", "DATE", mode="NULLABLE"),
        ]

        dest._insert_data("tbl", "ds", [{"id": 1, "cost": 1.0, "day": None}], _ctx(_partitioned_asset(), _RowSchema))

        table, update_fields = mock_client.update_table.call_args.args
        assert set(update_fields) == {"schema", "description"}
        assert {f.name: f.description for f in table.schema}["id"] == "Row id"
        assert table.description == "Daily rows."

    def test_no_update_when_metadata_already_in_sync(self):
        dest, mock_client = _make_destination(dataset="ds")
        existing = mock_client.get_table.return_value
        existing.schema = [
            bigquery.SchemaField("id", "INTEGER", mode="NULLABLE", description="Row id"),
            bigquery.SchemaField("cost", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("day", "DATE", mode="NULLABLE"),
        ]
        existing.description = "Daily rows."

        dest._insert_data("tbl", "ds", [{"id": 1, "cost": 1.0, "day": None}], _ctx(_partitioned_asset(), _RowSchema))

        assert not mock_client.update_table.called


class TestPartitionParam:
    """Partition predicates are typed from the table's column type."""

    def test_param_typed_from_table_column(self):
        table = MagicMock()
        table.schema = [bigquery.SchemaField("day", "DATE")]
        param = _partition_param(table, "day", "2024-01-01")
        assert param.type_ == "DATE"
        assert param.value == "2024-01-01"

    def test_param_falls_back_to_value_type(self):
        table = MagicMock()
        table.schema = []
        assert _partition_param(table, "day", "2024-01-01").type_ == "STRING"
        assert _partition_param(table, "n", 3).type_ == "INT64"

    def test_delete_partition_uses_column_type(self):
        dest, mock_client = _make_destination(dataset="ds")
        mock_client.get_table.return_value.schema = [bigquery.SchemaField("day", "DATE")]

        dest._delete_partition("tbl", "ds", "day", "2024-01-01")

        job_config = mock_client.query.call_args.kwargs["job_config"]
        assert job_config.query_parameters[0].type_ == "DATE"

    def test_select_partition_uses_column_type(self):
        dest, mock_client = _make_destination(dataset="ds")
        mock_client.get_table.return_value.schema = [bigquery.SchemaField("day", "TIMESTAMP")]
        mock_client.query.return_value.result.return_value = []

        dest._select_partition("tbl", "ds", "day", "2024-01-01")

        job_config = mock_client.query.call_args.kwargs["job_config"]
        param = job_config.query_parameters[0]
        assert param.type_ == "TIMESTAMP"
        # Date-only strings are coerced to datetime for client serialization
        # (which normalizes them to UTC).
        assert param.value == datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)


class TestDefinition:
    def test_project_is_a_fetch_field(self):
        """The project field resolves its options from the connection via x-fetch."""
        properties = BigQueryDestination.definition().config_schema["properties"]

        assert properties["project"]["x-widget"] == "fetch"
        assert properties["project"]["x-fetch"] == {
            "provider": "connection.projects",
            "label_key": "name",
            "value_key": "project_id",
        }
