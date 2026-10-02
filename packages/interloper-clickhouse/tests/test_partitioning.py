"""Tests for ``interloper_clickhouse.partitioning``."""

import pytest
from interloper.errors import ConfigError
from interloper.partitioning import PartitionConfig, TimeGranularity, TimePartitionConfig

from interloper_clickhouse.partitioning import partition_key, same_key

DAY = TimePartitionConfig(column="d")
HOUR = TimePartitionConfig(column="d", granularity=TimeGranularity.HOUR)
MONTH = TimePartitionConfig(column="d", granularity=TimeGranularity.MONTH)
YEAR = TimePartitionConfig(column="d", granularity=TimeGranularity.YEAR)
PARSED = "parseDateTime64BestEffort(`d`, 6, 'UTC')"


@pytest.mark.parametrize(
    ("config", "column_type", "expected"),
    [
        (DAY, "Date32", "`d`"),
        (DAY, "Date", "`d`"),
        (MONTH, "Date32", "toStartOfMonth(`d`)"),
        (YEAR, "Date32", "toStartOfYear(`d`)"),
        (DAY, "DateTime64(6, 'UTC')", "toDate(`d`)"),
        (HOUR, "DateTime64(6, 'UTC')", "toStartOfHour(`d`)"),
        (MONTH, "DateTime64(6, 'UTC')", "toStartOfMonth(`d`)"),
        (YEAR, "DateTime", "toStartOfYear(`d`)"),
        (DAY, "String", f"toDate({PARSED})"),
        (HOUR, "String", f"toStartOfHour({PARSED})"),
        (MONTH, "String", f"toStartOfMonth({PARSED})"),
        (YEAR, "Nullable(String)", f"toStartOfYear({PARSED})"),
        (DAY, "Nullable(Date32)", "`d`"),
    ],
)
def test_time_partitioning(config, column_type, expected):
    assert partition_key(config, "`d`", column_type) == expected


def test_non_time_partitioning_is_the_column():
    assert partition_key(PartitionConfig(column="region"), "`region`", "String") == "`region`"


def test_unpartitioned_has_no_key():
    assert partition_key(None, "`d`", "Date32") is None


def test_renders_over_any_expression():
    assert partition_key(MONTH, "CAST(v AS Date32)", "Date32") == "toStartOfMonth(CAST(v AS Date32))"


def test_hourly_on_a_date_raises():
    with pytest.raises(ConfigError, match="cannot hold hourly partitions"):
        partition_key(HOUR, "`d`", "Date32")


def test_time_partitioning_on_a_number_raises():
    with pytest.raises(ConfigError, match="time partitioning needs a date, a datetime or text"):
        partition_key(DAY, "`d`", "Int64")


@pytest.mark.parametrize(
    ("actual", "expected", "same"),
    [
        ("toStartOfMonth(day)", "toStartOfMonth(`day`)", True),
        ("toStartOfMonth(parseDateTime64BestEffort(d, 6, 'UTC'))", f"toStartOfMonth({PARSED})", True),
        ("", None, True),
        ("day", "toStartOfMonth(`day`)", False),
        ("day", None, False),
        ("", "`day`", False),
    ],
)
def test_same_key(actual, expected, same):
    assert same_key(actual, expected) is same
