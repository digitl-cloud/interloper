"""Tests for ``interloper_sql.types``."""

import datetime
import typing
from decimal import Decimal

import pytest
from interloper.schema import FieldSpec
from sqlalchemy import JSON, BigInteger, Boolean, Date, DateTime, Double, LargeBinary, Numeric, Text

from interloper_sql.types import column_type


def _spec(python_type, **kwargs) -> FieldSpec:
    return FieldSpec(name="x", type=python_type, nullable=True, **kwargs)


class TestColumnType:
    @pytest.mark.parametrize(
        ("python_type", "expected"),
        [
            (bool, Boolean),
            (int, BigInteger),
            (float, Double),
            (Decimal, Numeric),
            (datetime.datetime, DateTime),
            (datetime.date, Date),
            (bytes, LargeBinary),
            (str, Text),
            (typing.Any, Text),
        ],
    )
    def test_scalar(self, python_type, expected):
        assert type(column_type(_spec(python_type))) is expected

    def test_bool_is_not_an_integer(self):
        assert isinstance(column_type(_spec(bool)), Boolean)

    def test_datetime_is_not_a_date(self):
        assert isinstance(column_type(_spec(datetime.datetime)), DateTime)

    def test_datetime_keeps_its_timezone(self):
        timestamp = column_type(_spec(datetime.datetime))
        assert isinstance(timestamp, DateTime)
        assert timestamp.timezone is True

    def test_decimal_precision_and_scale(self):
        numeric = column_type(_spec(Decimal))
        assert isinstance(numeric, Numeric)
        assert (numeric.precision, numeric.scale) == (38, 9)

    def test_nested_is_json(self):
        assert isinstance(column_type(_spec(dict, fields=(_spec(int),))), JSON)

    def test_repeated_is_json(self):
        assert isinstance(column_type(_spec(int, repeated=True)), JSON)

    def test_unknown_class_is_text(self):
        assert isinstance(column_type(_spec(complex)), Text)
