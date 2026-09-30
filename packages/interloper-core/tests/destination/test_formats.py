"""Tests for ``interloper.destination.formats``."""

import builtins
import datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import Field

from interloper.destination.formats import FORMATS, CSVFormat, JSONLFormat, ParquetFormat
from interloper.schema import Schema


class _RowSchema(Schema):
    id: int | None = Field(..., description="Row id")
    cost: float | None = Field(...)
    day: datetime.date | None = Field(...)


class TestJSONLFormat:
    """NDJSON serialization."""

    def test_round_trip(self):
        rows = [{"id": 1, "name": "a"}, {"id": 2, "name": None}]
        payload = JSONLFormat().serialize(rows, None)
        assert JSONLFormat().deserialize(payload) == rows

    def test_one_object_per_line(self):
        payload = JSONLFormat().serialize([{"a": 1}, {"a": 2}], None)
        assert payload == b'{"a": 1}\n{"a": 2}'

    def test_dates_and_decimals_serialized(self):
        rows = [{"day": datetime.date(2024, 1, 2), "cost": Decimal("9.99")}]
        assert JSONLFormat().deserialize(JSONLFormat().serialize(rows, None)) == [
            {"day": "2024-01-02", "cost": "9.99"}
        ]

    def test_nan_becomes_null(self):
        payload = JSONLFormat().serialize([{"cost": float("nan")}], None)
        assert JSONLFormat().deserialize(payload) == [{"cost": None}]

    def test_empty(self):
        assert JSONLFormat().serialize([], None) == b""
        assert JSONLFormat().deserialize(b"") == []


class TestCSVFormat:
    """CSV serialization."""

    def test_round_trip_as_strings(self):
        rows = [{"id": 1, "name": "a"}]
        payload = CSVFormat().serialize(rows, None)
        assert CSVFormat().deserialize(payload) == [{"id": "1", "name": "a"}]

    def test_empty_cell_reads_as_none(self):
        payload = CSVFormat().serialize([{"id": 1, "name": None}], None)
        assert CSVFormat().deserialize(payload) == [{"id": "1", "name": None}]

    def test_empty(self):
        assert CSVFormat().serialize([], None) == b""
        assert CSVFormat().deserialize(b"") == []


class TestParquetFormat:
    """Parquet serialization."""

    def test_round_trip_types_preserved_with_specs(self):
        rows = [
            {"id": 1, "cost": 1.5, "day": datetime.date(2024, 1, 1)},
            {"id": None, "cost": None, "day": None},
        ]
        payload = ParquetFormat().serialize(rows, _RowSchema.field_specs())
        assert ParquetFormat().deserialize(payload) == rows

    def test_round_trip_without_specs_infers(self):
        rows = [{"id": 1, "name": "a"}]
        payload = ParquetFormat().serialize(rows, None)
        assert ParquetFormat().deserialize(payload) == rows

    def test_schema_stable_when_column_all_null(self):
        """Specs pin column types; an all-null column keeps its declared type."""
        payload = ParquetFormat().serialize([{"id": None, "cost": None, "day": None}], _RowSchema.field_specs())
        assert ParquetFormat().deserialize(payload) == [{"id": None, "cost": None, "day": None}]

    def test_untyped_field_stringified(self):
        class AnySchema(Schema):
            val: Any = Field(None)

        payload = ParquetFormat().serialize([{"val": 42}], AnySchema.field_specs())
        assert ParquetFormat().deserialize(payload) == [{"val": "42"}]

    def test_repeated_field(self):
        class TagsSchema(Schema):
            tags: list[str]

        rows = [{"tags": ["a", "b"]}]
        payload = ParquetFormat().serialize(rows, TagsSchema.field_specs())
        assert ParquetFormat().deserialize(payload) == rows

    def test_empty_with_specs(self):
        payload = ParquetFormat().serialize([], _RowSchema.field_specs())
        assert ParquetFormat().deserialize(payload) == []


class TestFormatsRegistry:
    def test_all_formats_registered(self):
        assert set(FORMATS) == {"parquet", "jsonl", "csv"}


class TestParquetWithoutPyarrow:
    """Core does not depend on pyarrow; Parquet says what to install."""

    @pytest.fixture
    def no_pyarrow(self, monkeypatch):
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "pyarrow" or name.startswith("pyarrow."):
                raise ImportError(name)
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)

    @pytest.mark.usefixtures("no_pyarrow")
    def test_serialize_names_the_install(self):
        with pytest.raises(ImportError, match="Parquet needs pyarrow: pip install pyarrow"):
            ParquetFormat().serialize([{"id": 1}], None)

    @pytest.mark.usefixtures("no_pyarrow")
    def test_deserialize_names_the_install(self):
        with pytest.raises(ImportError, match="Parquet needs pyarrow: pip install pyarrow"):
            ParquetFormat().deserialize(b"")

    @pytest.mark.usefixtures("no_pyarrow")
    def test_text_formats_unaffected(self):
        assert JSONLFormat().deserialize(JSONLFormat().serialize([{"id": 1}], None)) == [{"id": 1}]
