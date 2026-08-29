"""Tests for ``interloper.utils.time``."""

import datetime as dt

import pytest

from interloper.utils import add_months, assume_utc, coerce_to_date, coerce_to_datetime, month_start


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
        with pytest.raises(TypeError, match="Expected a `datetime.date`"):
            coerce_to_date(20260101)


class TestCoerceToDatetime:
    def test_returns_naive_datetime_unchanged(self) -> None:
        value = dt.datetime(2026, 1, 1, 9, 30)  # noqa: DTZ001
        assert coerce_to_datetime(value) is value

    def test_aware_datetime_becomes_naive_utc(self) -> None:
        # Labels are UTC: mixing aware and naive values would poison every
        # comparison downstream (bounds, clamps, window ordering).
        cet = dt.timezone(dt.timedelta(hours=2))
        assert coerce_to_datetime(dt.datetime(2026, 1, 1, 9, 30, tzinfo=cet)) == dt.datetime(2026, 1, 1, 7, 30)  # noqa: DTZ001

    def test_date_becomes_midnight(self) -> None:
        assert coerce_to_datetime(dt.date(2026, 1, 1)) == dt.datetime(2026, 1, 1)  # noqa: DTZ001

    def test_iso_string_is_parsed(self) -> None:
        assert coerce_to_datetime("2026-01-01T09:30") == dt.datetime(2026, 1, 1, 9, 30)  # noqa: DTZ001

    def test_invalid_string_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="ISO-8601 datetime string"):
            coerce_to_datetime("not-a-datetime")


class TestAssumeUtc:
    def test_naive_is_labelled_utc(self) -> None:
        assert assume_utc(dt.datetime(2026, 7, 1, 9, 30)).tzinfo is dt.timezone.utc  # noqa: DTZ001

    def test_aware_is_left_alone(self) -> None:
        berlin = dt.timezone(dt.timedelta(hours=2))
        value = dt.datetime(2026, 7, 1, 9, 30, tzinfo=berlin)
        assert assume_utc(value) is value


class TestAddMonths:
    def test_rolls_the_year(self) -> None:
        assert add_months(dt.date(2026, 12, 1), 1) == dt.date(2027, 1, 1)
        assert add_months(dt.date(2026, 1, 1), -1) == dt.date(2025, 12, 1)

    def test_day_is_clamped_to_the_target_month(self) -> None:
        assert add_months(dt.date(2026, 1, 31), 1) == dt.date(2026, 2, 28)


class TestMonthStart:
    def test_normalizes_to_utc(self) -> None:
        # 00:30 in Berlin is still the previous month in UTC.
        berlin = dt.timezone(dt.timedelta(hours=2))
        assert month_start(dt.datetime(2026, 7, 1, 0, 30, tzinfo=berlin)) == dt.date(2026, 6, 1)

    def test_naive_is_read_as_utc(self) -> None:
        assert month_start(dt.datetime(2026, 7, 1, 0, 30)) == dt.date(2026, 7, 1)  # noqa: DTZ001
