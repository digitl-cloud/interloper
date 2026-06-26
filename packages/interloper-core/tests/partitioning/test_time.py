"""Tests for ``interloper.partitioning.time``."""

import datetime as dt

import pytest

from interloper.partitioning.time import TimePartition, coerce_to_date


class TestCoerceToDate:
    def test_returns_date_unchanged(self) -> None:
        date = dt.date(2026, 1, 1)
        assert coerce_to_date(date) is date

    def test_datetime_is_reduced_to_date(self) -> None:
        assert coerce_to_date(dt.datetime(2026, 1, 1, 12, 30, tzinfo=dt.timezone.utc)) == dt.date(2026, 1, 1)

    def test_iso_string_is_parsed(self) -> None:
        assert coerce_to_date("2026-01-01") == dt.date(2026, 1, 1)

    def test_invalid_string_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="ISO-8601 date string"):
            coerce_to_date("not-a-date")

    def test_unsupported_type_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="must be a `datetime.date`"):
            coerce_to_date(20260101)


class TestTimePartition:
    def test_keeps_date_value(self) -> None:
        assert TimePartition(dt.date(2026, 1, 1)).value == dt.date(2026, 1, 1)

    def test_coerces_string_value(self) -> None:
        partition = TimePartition("2026-01-01")  # ty: ignore[invalid-argument-type]
        assert partition.value == dt.date(2026, 1, 1)
        assert isinstance(partition.value, dt.date)

    def test_coerces_datetime_value(self) -> None:
        assert TimePartition(dt.datetime(2026, 1, 1, 9, 0, tzinfo=dt.timezone.utc)).value == dt.date(2026, 1, 1)

    def test_rejects_invalid_value(self) -> None:
        with pytest.raises(TypeError):
            TimePartition("nope")  # ty: ignore[invalid-argument-type]
