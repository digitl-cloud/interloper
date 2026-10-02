"""Tests for ``interloper_clickhouse.types``."""

import datetime
from decimal import Decimal
from typing import Any

import pytest
from interloper.schema import FieldSpec

from interloper_clickhouse.types import base_type, column_type, is_json, is_nullable


@pytest.mark.parametrize(
    ("python_type", "expected"),
    [
        (bool, "Bool"),
        (int, "Int64"),
        (float, "Float64"),
        (Decimal, "Decimal(38, 9)"),
        (datetime.datetime, "DateTime64(6, 'UTC')"),
        (datetime.date, "Date32"),
        (bytes, "String"),
        (str, "String"),
        (Any, "String"),
        (dict, "String"),
        (list, "String"),
        (complex, "String"),
    ],
)
def test_scalar_types(python_type, expected):
    assert column_type(FieldSpec("x", python_type, nullable=True)) == expected


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        (FieldSpec("x", object, nullable=True, fields=(FieldSpec("y", int, nullable=True),)), True),
        (FieldSpec("x", int, nullable=True, repeated=True), True),
        (FieldSpec("x", dict, nullable=True), True),
        (FieldSpec("x", list, nullable=True), True),
        (FieldSpec("x", Any, nullable=True), True),
        (FieldSpec("x", str, nullable=True), False),
        (FieldSpec("x", bytes, nullable=True), False),
        (FieldSpec("x", int, nullable=True), False),
    ],
)
def test_is_json(spec, expected):
    assert is_json(spec) is expected


def test_nested_and_repeated_are_strings():
    nested = FieldSpec("x", object, nullable=True, fields=(FieldSpec("y", int, nullable=True),))
    assert column_type(nested) == "String"
    assert column_type(FieldSpec("x", int, nullable=True, repeated=True)) == "String"


@pytest.mark.parametrize(
    ("type_name", "expected"),
    [
        ("Nullable(Int64)", "Int64"),
        ("Nullable(DateTime64(6, 'UTC'))", "DateTime64(6, 'UTC')"),
        ("Date32", "Date32"),
    ],
)
def test_base_type(type_name, expected):
    assert base_type(type_name) == expected


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        (FieldSpec("x", int, nullable=True), True),
        (FieldSpec("x", int, nullable=False), False),
        (FieldSpec("x", Any, nullable=False), True),
    ],
)
def test_is_nullable(spec, expected):
    assert is_nullable(spec) is expected
