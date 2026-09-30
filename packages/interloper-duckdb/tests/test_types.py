"""Tests for ``interloper_duckdb.types``."""

import datetime
from decimal import Decimal
from typing import Any

import pytest
from interloper.schema import FieldSpec

from interloper_duckdb.types import column_type


@pytest.mark.parametrize(
    ("python_type", "expected"),
    [
        (bool, "BOOLEAN"),
        (int, "BIGINT"),
        (float, "DOUBLE"),
        (Decimal, "DECIMAL(38,9)"),
        (datetime.datetime, "TIMESTAMP"),
        (datetime.date, "DATE"),
        (bytes, "BLOB"),
        (str, "VARCHAR"),
        (Any, "VARCHAR"),
    ],
)
def test_scalar_types(python_type, expected):
    assert column_type(FieldSpec("c", python_type, nullable=True)) == expected


def test_nested_is_json():
    inner = (FieldSpec("x", int, nullable=True),)
    assert column_type(FieldSpec("c", dict, nullable=True, fields=inner)) == "JSON"


def test_repeated_is_json():
    assert column_type(FieldSpec("c", int, nullable=True, repeated=True)) == "JSON"
