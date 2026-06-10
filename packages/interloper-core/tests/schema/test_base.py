"""Tests for ``interloper.schema.base``."""

import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field

from interloper.schema import FieldSpec, Schema


class Nested(BaseModel):
    city: str = Field(description="City name")
    zip: str | None


class FullSchema(Schema):
    plain_int: int = Field(description="A plain integer")
    nullable_float: float | None = Field(...)
    a_date: datetime.date | None
    a_datetime: datetime.datetime
    a_decimal: Decimal
    a_bytes: bytes | None
    repeated_str: list[str]
    nullable_repeated: list[int] | None
    nested: Nested | None
    untyped: Any


class ShadowingSchema(Schema):
    """Fields that shadow Component attributes must keep declaration order."""

    id: int | None = Field(...)
    cost: float | None = Field(...)
    name: str | None = Field(...)
    day: str | None = Field(...)


def spec_by_name(schema: type[Schema], name: str) -> FieldSpec:
    """Return the spec with the given name."""
    return next(s for s in schema.field_specs() if s.name == name)


class TestFieldSpecs:
    """FieldSpec extraction from Schema subclasses."""

    def test_plain_type(self):
        spec = spec_by_name(FullSchema, "plain_int")
        assert spec.type is int
        assert spec.nullable is False
        assert spec.repeated is False
        assert spec.fields is None

    def test_nullable_unwraps_optional(self):
        spec = spec_by_name(FullSchema, "nullable_float")
        assert spec.type is float
        assert spec.nullable is True

    def test_temporal_and_decimal_types(self):
        assert spec_by_name(FullSchema, "a_date").type is datetime.date
        assert spec_by_name(FullSchema, "a_datetime").type is datetime.datetime
        assert spec_by_name(FullSchema, "a_decimal").type is Decimal
        assert spec_by_name(FullSchema, "a_bytes").type is bytes

    def test_repeated_unwraps_list(self):
        spec = spec_by_name(FullSchema, "repeated_str")
        assert spec.type is str
        assert spec.repeated is True
        assert spec.nullable is False

    def test_nullable_repeated(self):
        spec = spec_by_name(FullSchema, "nullable_repeated")
        assert spec.type is int
        assert spec.repeated is True
        assert spec.nullable is True

    def test_nested_model_yields_sub_fields(self):
        spec = spec_by_name(FullSchema, "nested")
        assert spec.fields is not None
        assert [f.name for f in spec.fields] == ["city", "zip"]
        assert spec.fields[1].nullable is True

    def test_any_type(self):
        assert spec_by_name(FullSchema, "untyped").type is Any

    def test_description_extracted(self):
        assert spec_by_name(FullSchema, "plain_int").description == "A plain integer"
        assert spec_by_name(FullSchema, "nullable_float").description is None

    def test_nested_description_extracted(self):
        spec = spec_by_name(FullSchema, "nested")
        assert spec.fields is not None
        assert spec.fields[0].description == "City name"
        assert spec.fields[1].description is None

    def test_component_fields_excluded(self):
        names = [s.name for s in FullSchema.field_specs()]
        assert "resources" not in names

    def test_declaration_order_preserved_with_shadowing_names(self):
        assert [s.name for s in ShadowingSchema.field_specs()] == ["id", "cost", "name", "day"]

    def test_inferred_schema_has_specs(self):
        inferred = Schema.infer([{"a": 1, "b": "x"}, {"a": None, "b": "y"}])
        specs = {s.name: s for s in inferred.field_specs()}
        assert specs["a"].type is int
        assert specs["a"].nullable is True  # inferred fields are always optional
        assert specs["b"].type is str
