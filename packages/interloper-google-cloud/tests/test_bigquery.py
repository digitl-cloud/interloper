"""Tests for BigQueryDestination."""

import datetime
import json
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import interloper as il
import pytest
from interloper.destination import IOContext
from interloper.destination.database import WriteDisposition
from interloper.errors import ConfigError
from interloper.schema import Schema
from pydantic import BaseModel, Field

from interloper_google_cloud.bigquery.destination import (
    BigQueryDestination,
    _bq_to_py_type,
    _infer_bq_schema,
    _py_to_bq_type,
    _replace_non_finite,
    _schema_to_bq_fields,
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
        write_disposition=overrides.get("write_disposition", WriteDisposition.REPLACE),
        resources={"connection": conn},
    )
    object.__setattr__(dest, "client", mock_client)

    return dest, mock_client


# ------------------------------------------------------------------
# _py_to_bq_type
# ------------------------------------------------------------------


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


# ------------------------------------------------------------------
# _bq_to_py_type
# ------------------------------------------------------------------


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


# ------------------------------------------------------------------
# _replace_non_finite
# ------------------------------------------------------------------


class TestReplaceNonFinite:
    """Non-finite floats are mapped to None so BigQuery gets valid JSON."""

    def test_nan_becomes_none(self):
        assert _replace_non_finite(float("nan")) is None

    def test_inf_becomes_none(self):
        assert _replace_non_finite(float("inf")) is None
        assert _replace_non_finite(float("-inf")) is None

    def test_finite_float_unchanged(self):
        assert _replace_non_finite(3.14) == 3.14
        assert _replace_non_finite(0.0) == 0.0

    def test_non_float_unchanged(self):
        assert _replace_non_finite(42) == 42
        assert _replace_non_finite("text") == "text"
        assert _replace_non_finite(None) is None
        assert _replace_non_finite(True) is True

    def test_nested_dict(self):
        assert _replace_non_finite({"a": float("nan"), "b": 1.5}) == {"a": None, "b": 1.5}

    def test_nested_list(self):
        assert _replace_non_finite([float("nan"), 1.0, float("inf")]) == [None, 1.0, None]

    def test_deeply_nested(self):
        result = _replace_non_finite({"rows": [{"cost": float("nan")}, {"cost": 2.0}]})
        assert result == {"rows": [{"cost": None}, {"cost": 2.0}]}


# ------------------------------------------------------------------
# _insert
# ------------------------------------------------------------------


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


# ------------------------------------------------------------------
# _resolve_dataset
# ------------------------------------------------------------------


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


# ------------------------------------------------------------------
# _table_ref
# ------------------------------------------------------------------


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


# ------------------------------------------------------------------
# _table_exists
# ------------------------------------------------------------------


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


# ------------------------------------------------------------------
# dispose
# ------------------------------------------------------------------


class TestDispose:
    """Lifecycle: dispose closes the client."""

    def test_dispose_closes_client(self):
        dest, mock_client = _make_destination()
        dest.dispose()
        mock_client.close.assert_called_once()


# ------------------------------------------------------------------
# Schema-driven loads
# ------------------------------------------------------------------


class _RowSchema(Schema):
    id: int | None = Field(...)
    cost: float | None = Field(...)
    day: datetime.date | None = Field(...)


class _NestedModel(BaseModel):
    city: str
    zip: str | None


class _NestedSchema(Schema):
    name: str
    tags: list[str]
    address: _NestedModel | None


def _ctx(asset: Any, schema: type[Schema] | None) -> IOContext:
    return IOContext(asset=asset, schema=schema)


@il.asset
def _plain_asset() -> list:
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
