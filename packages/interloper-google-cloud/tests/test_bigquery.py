"""Tests for BigQueryDestination."""

import datetime
import json
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from interloper.destination.database import WriteDisposition
from interloper.errors import ConfigError

from interloper_google_cloud.bigquery.destination import BigQueryDestination, _bq_to_py_type, _py_to_bq_type
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
        chunk_size=overrides.get("chunk_size", 1000),
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
