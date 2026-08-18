"""Tests for ``interloper.partitioning.time``."""

import datetime as dt

import pytest

from interloper.partitioning.time import (
    SUPPORTED_GRANULARITIES,
    TimeGranularity,
    TimePartition,
    TimePartitionConfig,
    TimePartitionWindow,
    coerce_to_date,
    coerce_to_datetime,
    period_range,
)


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


class TestCoerceToDatetime:
    def test_returns_datetime_unchanged(self) -> None:
        value = dt.datetime(2026, 1, 1, 9, 30, tzinfo=dt.timezone.utc)
        assert coerce_to_datetime(value) is value

    def test_date_becomes_midnight(self) -> None:
        assert coerce_to_datetime(dt.date(2026, 1, 1)) == dt.datetime(2026, 1, 1)  # noqa: DTZ001

    def test_iso_string_is_parsed(self) -> None:
        assert coerce_to_datetime("2026-01-01T09:30") == dt.datetime(2026, 1, 1, 9, 30)  # noqa: DTZ001

    def test_invalid_string_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="ISO-8601 datetime string"):
            coerce_to_datetime("not-a-datetime")


class TestTimeGranularityTruncate:
    @pytest.mark.parametrize(
        ("granularity", "expected"),
        [
            (TimeGranularity.DAY, dt.date(2026, 5, 20)),
            # 2026-05-20 is a Wednesday; weeks start on Monday.
            (TimeGranularity.WEEK, dt.date(2026, 5, 18)),
            (TimeGranularity.MONTH, dt.date(2026, 5, 1)),
            (TimeGranularity.QUARTER, dt.date(2026, 4, 1)),
            (TimeGranularity.YEAR, dt.date(2026, 1, 1)),
        ],
    )
    def test_truncates_to_period_start(self, granularity: TimeGranularity, expected: dt.date) -> None:
        assert granularity.truncate(dt.date(2026, 5, 20)) == expected

    def test_hour_keeps_time_and_drops_minutes(self) -> None:
        truncated = TimeGranularity.HOUR.truncate(dt.datetime(2026, 5, 20, 13, 47, 12))  # noqa: DTZ001
        assert truncated == dt.datetime(2026, 5, 20, 13, 0)  # noqa: DTZ001

    def test_coerces_iso_string(self) -> None:
        assert TimeGranularity.MONTH.truncate("2026-05-20") == dt.date(2026, 5, 1)


class TestTimeGranularityAdvance:
    @pytest.mark.parametrize(
        ("granularity", "periods", "expected"),
        [
            (TimeGranularity.DAY, 1, dt.date(2026, 1, 2)),
            (TimeGranularity.DAY, -1, dt.date(2025, 12, 31)),
            (TimeGranularity.WEEK, 2, dt.date(2026, 1, 12)),
            (TimeGranularity.MONTH, 1, dt.date(2026, 2, 1)),
            (TimeGranularity.MONTH, -1, dt.date(2025, 12, 1)),
            (TimeGranularity.QUARTER, 1, dt.date(2026, 4, 1)),
            (TimeGranularity.YEAR, 1, dt.date(2027, 1, 1)),
        ],
    )
    def test_advances_whole_periods(self, granularity: TimeGranularity, periods: int, expected: dt.date) -> None:
        # 2025-12-29 is the Monday of the week containing 2026-01-01.
        assert granularity.advance(dt.date(2026, 1, 1), periods) == expected

    def test_zero_is_a_truncation(self) -> None:
        assert TimeGranularity.MONTH.advance(dt.date(2026, 5, 20), 0) == dt.date(2026, 5, 1)

    def test_month_arithmetic_crosses_years(self) -> None:
        assert TimeGranularity.MONTH.advance(dt.date(2026, 2, 1), -14) == dt.date(2024, 12, 1)

    def test_hour_advances_by_hours(self) -> None:
        start = dt.datetime(2026, 1, 1, 23, 0)  # noqa: DTZ001
        assert TimeGranularity.HOUR.advance(start, 2) == dt.datetime(2026, 1, 2, 1, 0)  # noqa: DTZ001


class TestTimeGranularityBounds:
    def test_day_bounds_are_half_open(self) -> None:
        assert TimeGranularity.DAY.bounds(dt.date(2026, 1, 1)) == (dt.date(2026, 1, 1), dt.date(2026, 1, 2))

    def test_month_bounds_span_the_month(self) -> None:
        assert TimeGranularity.MONTH.bounds(dt.date(2026, 2, 15)) == (dt.date(2026, 2, 1), dt.date(2026, 3, 1))


class TestTimeGranularityPeriodsBetween:
    @pytest.mark.parametrize(
        ("granularity", "start", "end", "expected"),
        [
            (TimeGranularity.DAY, dt.date(2026, 1, 1), dt.date(2026, 1, 8), 7),
            (TimeGranularity.DAY, dt.date(2026, 1, 8), dt.date(2026, 1, 1), -7),
            (TimeGranularity.WEEK, dt.date(2026, 1, 1), dt.date(2026, 1, 22), 3),
            (TimeGranularity.MONTH, dt.date(2025, 11, 3), dt.date(2026, 2, 27), 3),
            (TimeGranularity.QUARTER, dt.date(2026, 1, 1), dt.date(2026, 10, 1), 3),
            (TimeGranularity.YEAR, dt.date(2024, 6, 1), dt.date(2026, 2, 1), 2),
        ],
    )
    def test_counts_whole_periods(
        self, granularity: TimeGranularity, start: dt.date, end: dt.date, expected: int
    ) -> None:
        assert granularity.periods_between(start, end) == expected

    def test_hour_counts_hours(self) -> None:
        start = dt.datetime(2026, 1, 1, 0, 0)  # noqa: DTZ001
        end = dt.datetime(2026, 1, 1, 5, 30)  # noqa: DTZ001
        assert TimeGranularity.HOUR.periods_between(start, end) == 5


class TestTimeGranularityIdentity:
    def test_day_formats_as_iso_date(self) -> None:
        assert TimeGranularity.DAY.format(dt.date(2026, 1, 1)) == "2026-01-01"

    def test_day_parse_round_trips(self) -> None:
        assert TimeGranularity.DAY.parse("2026-01-01") == dt.date(2026, 1, 1)

    @pytest.mark.parametrize(
        "granularity",
        [TimeGranularity.HOUR, TimeGranularity.WEEK, TimeGranularity.MONTH],
    )
    def test_other_granularities_have_no_id_format_yet(self, granularity: TimeGranularity) -> None:
        with pytest.raises(NotImplementedError, match="partition id format"):
            granularity.format(dt.date(2026, 1, 1))
        with pytest.raises(NotImplementedError, match="partition id format"):
            granularity.parse("2026-01-01")


class TestPeriodRange:
    def test_yields_each_day_inclusive(self) -> None:
        values = list(period_range(dt.date(2026, 1, 1), dt.date(2026, 1, 3)))
        assert values == [dt.date(2026, 1, 1), dt.date(2026, 1, 2), dt.date(2026, 1, 3)]

    def test_reversed_yields_newest_first(self) -> None:
        values = list(period_range(dt.date(2026, 1, 1), dt.date(2026, 1, 3), reversed=True))
        assert values == [dt.date(2026, 1, 3), dt.date(2026, 1, 2), dt.date(2026, 1, 1)]

    def test_steps_by_granularity(self) -> None:
        values = list(period_range(dt.date(2026, 1, 15), dt.date(2026, 3, 2), TimeGranularity.MONTH))
        assert values == [dt.date(2026, 1, 1), dt.date(2026, 2, 1), dt.date(2026, 3, 1)]

    def test_single_period_range(self) -> None:
        assert list(period_range(dt.date(2026, 1, 1), dt.date(2026, 1, 1))) == [dt.date(2026, 1, 1)]


class TestTimePartitionConfig:
    def test_defaults_to_daily(self) -> None:
        assert TimePartitionConfig(column="date").granularity is TimeGranularity.DAY

    def test_day_is_the_only_supported_granularity(self) -> None:
        assert SUPPORTED_GRANULARITIES == {TimeGranularity.DAY}

    @pytest.mark.parametrize(
        "granularity",
        [g for g in TimeGranularity if g not in SUPPORTED_GRANULARITIES],
    )
    def test_unsupported_granularity_is_rejected(self, granularity: TimeGranularity) -> None:
        with pytest.raises(ValueError, match="is not supported yet"):
            TimePartitionConfig(column="date", granularity=granularity)

    def test_start_is_normalized_to_a_period_start(self) -> None:
        # At daily granularity a date is already a period start; the
        # normalization only shifts the value at coarser granularities.
        assert TimePartitionConfig(column="date", start=dt.date(2026, 1, 15)).start == dt.date(2026, 1, 15)

    def test_start_accepts_a_coercible_value(self) -> None:
        config = TimePartitionConfig(column="date", start="2026-01-15")  # ty: ignore[invalid-argument-type]
        assert config.start == dt.date(2026, 1, 15)

    def test_start_defaults_to_none(self) -> None:
        assert TimePartitionConfig(column="date").start is None


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

    def test_defaults_to_daily(self) -> None:
        assert TimePartition(dt.date(2026, 1, 1)).granularity is TimeGranularity.DAY

    def test_id_is_the_iso_date(self) -> None:
        assert TimePartition(dt.date(2026, 1, 1)).id == "2026-01-01"

    def test_value_is_truncated_to_the_period_start(self) -> None:
        partition = TimePartition(dt.date(2026, 5, 20), TimeGranularity.MONTH)
        assert partition.value == dt.date(2026, 5, 1)

    def test_bounds_are_half_open(self) -> None:
        assert TimePartition(dt.date(2026, 1, 1)).bounds == (dt.date(2026, 1, 1), dt.date(2026, 1, 2))


class TestTimePartitionWindow:
    def test_iterates_newest_first(self) -> None:
        window = TimePartitionWindow(start=dt.date(2026, 1, 1), end=dt.date(2026, 1, 3))
        assert [p.value for p in window] == [dt.date(2026, 1, 3), dt.date(2026, 1, 2), dt.date(2026, 1, 1)]

    def test_partitions_carry_the_window_granularity(self) -> None:
        window = TimePartitionWindow(start=dt.date(2026, 1, 1), end=dt.date(2026, 1, 2))
        assert all(p.granularity is TimeGranularity.DAY for p in window)

    def test_partition_count_is_inclusive(self) -> None:
        window = TimePartitionWindow(start=dt.date(2026, 1, 1), end=dt.date(2026, 1, 3))
        assert window.partition_count() == 3

    def test_single_partition_window(self) -> None:
        window = TimePartitionWindow(start=dt.date(2026, 1, 1), end=dt.date(2026, 1, 1))
        assert window.partition_count() == 1
        assert [p.value for p in window] == [dt.date(2026, 1, 1)]

    def test_bounds_are_truncated(self) -> None:
        window = TimePartitionWindow(
            start=dt.date(2026, 1, 15),
            end=dt.date(2026, 3, 20),
            granularity=TimeGranularity.MONTH,
        )
        assert (window.start, window.end) == (dt.date(2026, 1, 1), dt.date(2026, 3, 1))
        assert window.partition_count() == 3

    def test_inverted_window_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="ends before it starts"):
            TimePartitionWindow(start=dt.date(2026, 1, 3), end=dt.date(2026, 1, 1))

    def test_str_and_repr_are_iso(self) -> None:
        window = TimePartitionWindow(start=dt.date(2026, 1, 1), end=dt.date(2026, 1, 3))
        assert str(window) == "2026-01-01:2026-01-03"
        assert repr(window) == "2026-01-01 to 2026-01-03"
