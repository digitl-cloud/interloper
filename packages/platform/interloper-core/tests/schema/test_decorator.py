"""Tests for ``interloper.schema.decorator``."""

from __future__ import annotations

import datetime as dt

from interloper.schema import Schema, schema


class TestBareForm:
    """``@schema`` on a plain class."""

    def test_a_plain_class_becomes_a_schema_subclass(self) -> None:
        @schema
        class User:
            """Plain class carrying only annotations."""

            id: int
            email: str
            signed_up: dt.date | None

        assert issubclass(User, Schema)
        assert set(User.model_fields) == {"id", "email", "signed_up"}

    def test_the_key_is_derived_from_the_class_name(self) -> None:
        @schema
        class AdsStats:
            """Plain class with a multi-word name."""

            clicks: int

        assert AdsStats.key == "ads_stats"

    def test_field_defaults_are_kept(self) -> None:
        @schema
        class Row:
            """Plain class with a defaulted field."""

            clicks: int = 0

        assert Row().clicks == 0  # ty: ignore[unresolved-attribute]


class TestParameterizedForm:
    """``@schema(...)`` with metadata overrides."""

    def test_key_and_name_are_applied(self) -> None:
        @schema(key="custom", name="Custom Schema")
        class Other:
            """Plain class decorated with full metadata."""

            value: float

        assert Other.key == "custom"
        assert Other.name == "Custom Schema"
        assert set(Other.model_fields) == {"value"}

    def test_omitted_metadata_falls_back_to_the_defaults(self) -> None:
        @schema(name="Just A Name")
        class Other:
            """Plain class decorated with a name only."""

            value: float

        assert Other.key == "other"
        assert Other.name == "Just A Name"

    def test_the_built_schema_can_describe_its_fields(self) -> None:
        @schema(key="row")
        class Row:
            """Plain class whose field specs are read back."""

            clicks: int
            cost: float | None

        specs = {spec.name: spec for spec in Row.field_specs()}

        assert set(specs) == {"clicks", "cost"}
        assert specs["clicks"].nullable is False
        assert specs["cost"].nullable is True
