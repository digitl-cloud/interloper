"""Tests for ``interloper.schema.base``."""

import datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import BaseModel, Field

from interloper.errors import SchemaError
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


class StrictRowSchema(Schema):
    a: int
    b: str


class OptionalRowSchema(Schema):
    a: int
    b: str = "fallback"


class RepeatedRowSchema(Schema):
    tags: list[str] | None = None


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

    def test_model_fields_in_declaration_order(self):
        assert list(ShadowingSchema.model_fields) == ["id", "cost", "name", "day"]

    def test_subclass_keeps_parent_order_and_appends(self):
        class Extended(ShadowingSchema):
            extra: str | None = Field(...)

        assert [s.name for s in Extended.field_specs()] == ["id", "cost", "name", "day", "extra"]

    def test_inferred_schema_keeps_key_order_with_shadowing_name(self):
        inferred = Schema.infer([{"a": 1, "name": "x", "b": 2}])
        assert [s.name for s in inferred.field_specs()] == ["a", "name", "b"]

    def test_reconciled_rows_in_declaration_order(self):
        rows = ShadowingSchema.reconcile([{"name": "x", "id": 1, "cost": 2.0, "day": "mon"}])
        assert list(rows[0]) == ["id", "cost", "name", "day"]

    def test_inferred_schema_has_specs(self):
        inferred = Schema.infer([{"a": 1, "b": "x"}, {"a": None, "b": "y"}])
        specs = {s.name: s for s in inferred.field_specs()}
        assert specs["a"].type is int
        assert specs["a"].nullable is True  # inferred fields are always optional
        assert specs["b"].type is str


class TestReconcile:
    """Row-wise reconciliation coerces values and aligns fields."""

    def test_int_values_coerced_to_str_fields(self):
        # Pydantic's lax mode never coerces int -> str; reconcile must, to
        # match the DataFrame conformer's astype("string").
        rows = ShadowingSchema.reconcile([{"id": 1, "cost": 2.0, "name": 42, "day": "mon"}])
        assert rows[0]["name"] == "42"

    def test_nested_values_json_encoded_for_str_fields(self):
        rows = ShadowingSchema.reconcile([{"id": 1, "cost": 2.0, "name": [{"a": 1}], "day": "mon"}])
        assert rows[0]["name"] == '[{"a": 1}]'

    def test_none_passes_through_str_coercion(self):
        rows = ShadowingSchema.reconcile([{"id": 1, "cost": 2.0, "name": None, "day": "mon"}])
        assert rows[0]["name"] is None

    def test_dropped_fields_logged(self, caplog):
        with caplog.at_level("WARNING", logger="interloper.schema.base"):
            ShadowingSchema.reconcile([{"id": 1, "cost": 2.0, "name": "x", "day": "mon", "extra": True}])
        assert "dropped fields" in caplog.text
        assert "extra" in caplog.text


class TestJsonSchema:
    """JSON Schema generation restricted to data fields."""

    def test_component_fields_excluded(self):
        assert "resources" not in FullSchema.json_schema()["properties"]

    def test_properties_in_declaration_order(self):
        props = list(ShadowingSchema.json_schema()["properties"])
        assert props == ["id", "cost", "name", "day"]

    def test_required_excludes_component_fields(self):
        assert "resources" not in FullSchema.json_schema().get("required", [])

    def test_required_dropped_when_empty(self):
        # A schema whose only required field is the inherited ``resources``
        # must not surface a ``required`` key referencing it.
        class AllOptional(Schema):
            a: int | None = None
            b: str | None = None

        assert "required" not in AllOptional.json_schema()


class TestInfer:
    """Schema inference from row dicts, used at the IO boundary."""

    def test_types_are_read_from_the_values(self):
        inferred = Schema.infer([{"clicks": 1, "cost": 1.5, "campaign": "x"}])
        specs = {spec.name: spec for spec in inferred.field_specs()}

        assert specs["clicks"].type is int
        assert specs["cost"].type is float
        assert specs["campaign"].type is str

    def test_every_field_is_nullable(self):
        # Any key may be absent from any row.
        inferred = Schema.infer([{"clicks": 1}])

        assert all(spec.nullable for spec in inferred.field_specs())

    def test_a_key_absent_from_some_rows_is_still_inferred(self):
        inferred = Schema.infer([{"a": 1}, {"b": "x"}])

        assert {spec.name for spec in inferred.field_specs()} == {"a", "b"}

    def test_int_and_float_widen_to_float(self):
        inferred = Schema.infer([{"v": 1}, {"v": 1.5}])
        spec = next(spec for spec in inferred.field_specs() if spec.name == "v")

        assert spec.type is float

    def test_conflicting_types_fall_back_to_any(self):
        inferred = Schema.infer([{"v": 1}, {"v": "x"}])
        spec = next(spec for spec in inferred.field_specs() if spec.name == "v")

        assert spec.type is Any

    def test_an_all_none_column_falls_back_to_any(self):
        inferred = Schema.infer([{"v": None}])
        spec = next(spec for spec in inferred.field_specs() if spec.name == "v")

        assert spec.type is Any

    def test_the_class_name_is_configurable(self):
        assert Schema.infer([{"a": 1}], name="AdsStats").__name__ == "AdsStats"

    def test_empty_rows_cannot_be_inferred_from(self):
        with pytest.raises(SchemaError, match="Cannot infer schema from empty data"):
            Schema.infer([])


class TestValidateRows:
    """Strict validation is the STRICT materialization strategy's contract."""

    def test_matching_rows_pass(self):
        StrictRowSchema.validate_rows([{"a": 1, "b": "x"}], strict=True)

    def test_extra_fields_are_rejected_in_strict_mode(self):
        with pytest.raises(SchemaError, match=r"row 0: extra fields not in schema: \['c'\]"):
            StrictRowSchema.validate_rows([{"a": 1, "b": "x", "c": 2}], strict=True)

    def test_missing_required_fields_are_rejected_in_strict_mode(self):
        with pytest.raises(SchemaError, match=r"row 0: missing required fields: \['b'\]"):
            StrictRowSchema.validate_rows([{"a": 1}], strict=True)

    def test_extra_fields_are_tolerated_without_strict(self):
        StrictRowSchema.validate_rows([{"a": 1, "b": "x", "c": 2}])

    def test_the_failing_row_is_named(self):
        with pytest.raises(SchemaError, match="row 1"):
            StrictRowSchema.validate_rows([{"a": 1, "b": "x"}, {"a": 1}], strict=True)


class TestReconcileDefaults:
    """Reconcile fills absent optional fields and drops unknown ones."""

    def test_an_absent_optional_field_takes_its_default(self):
        rows = OptionalRowSchema.reconcile([{"a": 1}])

        assert rows == [{"a": 1, "b": "fallback"}]

    def test_no_rows_returns_no_rows(self):
        assert OptionalRowSchema.reconcile([]) == []

    def test_a_repeated_field_survives(self):
        rows = RepeatedRowSchema.reconcile([{"tags": ["a", "b"]}])

        assert rows == [{"tags": ["a", "b"]}]
