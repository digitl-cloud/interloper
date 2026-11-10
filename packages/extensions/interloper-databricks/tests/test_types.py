"""Tests for ``interloper_databricks.types``."""

import datetime
from decimal import Decimal
from typing import Any

import pytest
from interloper.schema import FieldSpec

from interloper_databricks.types import clusterable, column_type


@pytest.mark.parametrize(
    ("python_type", "expected"),
    [
        (bool, "BOOLEAN"),
        (int, "BIGINT"),
        (float, "DOUBLE"),
        (Decimal, "DECIMAL(38,9)"),
        (datetime.datetime, "TIMESTAMP"),
        (datetime.date, "DATE"),
        (bytes, "BINARY"),
        (str, "STRING"),
        (Any, "STRING"),
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


@pytest.mark.parametrize(
    ("python_type", "expected"),
    [
        (datetime.date, True),
        (datetime.datetime, True),
        (str, True),
        (int, True),
        (float, True),
        (Decimal, True),
        (Any, True),
        (bool, False),
        (bytes, False),
        (dict, False),
    ],
)
def test_clusterable(python_type, expected):
    assert clusterable(FieldSpec("x", python_type, nullable=True)) is expected


def test_repeated_is_not_clusterable():
    assert clusterable(FieldSpec("x", str, nullable=True, repeated=True)) is False
