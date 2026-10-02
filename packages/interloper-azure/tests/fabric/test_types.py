"""Tests for ``interloper_azure.fabric.types``."""

import datetime
from decimal import Decimal
from typing import Any

import pytest
from interloper.schema import FieldSpec

from interloper_azure.fabric.types import JSON, column_type


@pytest.mark.parametrize(
    ("python_type", "expected"),
    [
        (bool, "bit"),
        (int, "bigint"),
        (float, "float"),
        (Decimal, "decimal(38,9)"),
        (datetime.datetime, "datetime2(6)"),
        (datetime.date, "date"),
        (bytes, "varbinary(max)"),
        (str, "varchar(max)"),
        (Any, "varchar(max)"),
        (dict, "varchar(max)"),
        (list, "varchar(max)"),
    ],
)
def test_scalar_types(python_type, expected):
    assert column_type(FieldSpec("x", python_type, nullable=True)) == expected


def test_bool_is_not_bigint():
    # bool subclasses int, so the table is read in order.
    assert column_type(FieldSpec("x", bool, nullable=False)) == "bit"


def test_datetime_is_not_date():
    assert column_type(FieldSpec("x", datetime.datetime, nullable=False)) == "datetime2(6)"


def test_nested_is_json_text():
    nested = FieldSpec("x", object, nullable=True, fields=(FieldSpec("y", int, nullable=True),))
    assert column_type(nested) == JSON == "varchar(max)"


def test_repeated_is_json_text():
    assert column_type(FieldSpec("x", int, nullable=True, repeated=True)) == JSON


def test_unknown_class_is_text():
    class Custom:
        pass

    assert column_type(FieldSpec("x", Custom, nullable=True)) == "varchar(max)"
