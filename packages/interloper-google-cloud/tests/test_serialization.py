"""Tests for the shared JSON serialization helpers."""

import datetime
from decimal import Decimal

import pytest

from interloper_google_cloud.serialization import json_default, replace_non_finite


class TestReplaceNonFinite:
    """Non-finite floats are mapped to None so load payloads stay valid JSON."""

    def test_nan_becomes_none(self):
        assert replace_non_finite(float("nan")) is None

    def test_inf_becomes_none(self):
        assert replace_non_finite(float("inf")) is None
        assert replace_non_finite(float("-inf")) is None

    def test_finite_float_unchanged(self):
        assert replace_non_finite(3.14) == 3.14
        assert replace_non_finite(0.0) == 0.0

    def test_non_float_unchanged(self):
        assert replace_non_finite(42) == 42
        assert replace_non_finite("text") == "text"
        assert replace_non_finite(None) is None
        assert replace_non_finite(True) is True

    def test_nested_dict(self):
        assert replace_non_finite({"a": float("nan"), "b": 1.5}) == {"a": None, "b": 1.5}

    def test_nested_list(self):
        assert replace_non_finite([float("nan"), 1.0, float("inf")]) == [None, 1.0, None]

    def test_deeply_nested(self):
        result = replace_non_finite({"rows": [{"cost": float("nan")}, {"cost": 2.0}]})
        assert result == {"rows": [{"cost": None}, {"cost": 2.0}]}


class TestJsonDefault:
    """Fallback encoder for non-JSON-native types."""

    def test_date(self):
        assert json_default(datetime.date(2024, 1, 2)) == "2024-01-02"

    def test_datetime(self):
        assert json_default(datetime.datetime(2024, 1, 2, 3, 4, 5)) == "2024-01-02T03:04:05"

    def test_decimal(self):
        assert json_default(Decimal("9.99")) == "9.99"

    def test_unsupported_raises(self):
        with pytest.raises(TypeError):
            json_default(object())
