"""Tests for ``interloper_google_cloud.bigquery.types``."""

import datetime
from decimal import Decimal
from typing import Any

from interloper.schema import Schema
from pydantic import BaseModel, Field

from interloper_google_cloud.bigquery.types import TIME_PARTITIONABLE, column_type, parameter_type, to_fields


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


class TestColumnType:
    """One ordered table from Python types to BigQuery standard SQL type names."""

    def test_scalars(self):
        assert column_type(bool) == "BOOL"
        assert column_type(int) == "INT64"
        assert column_type(float) == "FLOAT64"
        assert column_type(Decimal) == "NUMERIC"
        assert column_type(datetime.datetime) == "TIMESTAMP"
        assert column_type(datetime.date) == "DATE"
        assert column_type(bytes) == "BYTES"
        assert column_type(str) == "STRING"

    def test_subclasses_follow_their_base(self):
        class Money(Decimal):
            pass

        assert column_type(Money) == "NUMERIC"

    def test_bool_wins_over_int_and_datetime_over_date(self):
        # bool subclasses int and datetime subclasses date; the table is ordered so the narrower type wins.
        assert column_type(bool) == "BOOL"
        assert column_type(datetime.datetime) == "TIMESTAMP"

    def test_unknown_and_any_are_strings(self):
        assert column_type(Any) == "STRING"
        assert column_type(type(None)) == "STRING"
        assert column_type(dict) == "STRING"


class TestParameterType:
    """A column type read back from a table names its query-parameter type in standard SQL."""

    def test_legacy_names_map_to_standard(self):
        assert parameter_type("INTEGER") == "INT64"
        assert parameter_type("FLOAT") == "FLOAT64"
        assert parameter_type("BOOLEAN") == "BOOL"

    def test_standard_names_pass_through(self):
        assert parameter_type("INT64") == "INT64"
        assert parameter_type("DATE") == "DATE"
        assert parameter_type("TIMESTAMP") == "TIMESTAMP"

    def test_unknown_falls_back_to_string(self):
        assert parameter_type("GEOGRAPHY") == "GEOGRAPHY"
        assert parameter_type("NOT_A_TYPE") == "STRING"


class TestToFields:
    """Field specs become SchemaFields, nested models as RECORD, descriptions carried."""

    def test_scalar_types_and_modes(self):
        fields = to_fields(_RowSchema.field_specs())
        assert [(f.name, f.field_type, f.mode) for f in fields] == [
            ("id", "INT64", "NULLABLE"),
            ("cost", "FLOAT64", "NULLABLE"),
            ("day", "DATE", "NULLABLE"),
        ]

    def test_nested_and_repeated(self):
        fields = {f.name: f for f in to_fields(_NestedSchema.field_specs())}
        assert fields["name"].mode == "REQUIRED"
        assert fields["tags"].mode == "REPEATED"
        assert fields["tags"].field_type == "STRING"
        assert fields["address"].field_type == "RECORD"
        assert [sub.name for sub in fields["address"].fields] == ["city", "zip"]

    def test_descriptions_carried(self):
        fields = {f.name: f for f in to_fields(_RowSchema.field_specs())}
        assert fields["id"].description == "Row id"
        assert fields["cost"].description is None

    def test_nested_descriptions_carried(self):
        fields = {f.name: f for f in to_fields(_NestedSchema.field_specs())}
        assert fields["address"].description == "Postal address"
        assert fields["address"].fields[0].description == "City name"
        assert fields["address"].fields[1].description is None


class TestTimePartitionable:
    def test_the_time_types(self):
        assert TIME_PARTITIONABLE == {"DATE", "DATETIME", "TIMESTAMP"}
