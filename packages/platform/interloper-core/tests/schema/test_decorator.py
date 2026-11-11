"""Tests for ``interloper.schema.decorator``."""

from __future__ import annotations

import datetime as dt

import pytest

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

    def test_columns_named_like_component_settings_are_columns(self) -> None:
        @schema
        class Account:
            """Plain class whose columns are called ``key`` and ``name``."""

            key: str
            name: str | None

        assert list(Account.model_fields) == ["key", "name"]

    def test_field_defaults_are_kept(self) -> None:
        @schema
        class Row:
            """Plain class with a defaulted field."""

            clicks: int = 0

        assert Row().clicks == 0  # ty: ignore[unresolved-attribute]

    def test_the_built_schema_can_describe_its_fields(self) -> None:
        @schema
        class Row:
            """Plain class whose field specs are read back."""

            clicks: int
            cost: float | None

        specs = {spec.name: spec for spec in Row.field_specs()}

        assert set(specs) == {"clicks", "cost"}
        assert specs["clicks"].nullable is False
        assert specs["cost"].nullable is True


class TestParameterizedForm:
    """``@schema(...)``: a schema declares no settings to set."""

    def test_a_setting_is_refused(self) -> None:
        with pytest.raises(TypeError, match=r"Schema does not accept 'key'"):

            @schema(key="custom")
            class Other:
                """Plain class decorated with a setting schemas do not have."""

                value: float
