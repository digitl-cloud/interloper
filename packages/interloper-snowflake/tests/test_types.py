"""Tests for ``interloper_snowflake.types``."""

import datetime
from decimal import Decimal
from typing import Any

import pytest
from interloper.schema import FieldSpec

from interloper_snowflake.types import column_type


@pytest.mark.parametrize(
    ("python_type", "expected"),
    [
        (bool, "BOOLEAN"),
        (int, "NUMBER(38,0)"),
        (float, "FLOAT"),
        (Decimal, "NUMBER(38,9)"),
        (datetime.datetime, "TIMESTAMP_NTZ"),
        (datetime.date, "DATE"),
        (bytes, "BINARY"),
        (str, "VARCHAR"),
        (Any, "VARCHAR"),
        (dict, "VARIANT"),
        (list, "VARIANT"),
    ],
)
def test_scalar_types(python_type, expected):
    assert column_type(FieldSpec("x", python_type, nullable=True)) == expected


def test_nested_is_variant():
    nested = FieldSpec("x", object, nullable=True, fields=(FieldSpec("y", int, nullable=True),))
    assert column_type(nested) == "VARIANT"


def test_repeated_is_variant():
    assert column_type(FieldSpec("x", int, nullable=True, repeated=True)) == "VARIANT"
