"""Tests for ``interloper.destination.context``: the partitions an IO context spells out."""

import datetime

import pytest

import interloper as il
from interloper.destination import IOContext
from interloper.errors import ConfigError
from interloper.partitioning.time import TimePartition, TimePartitionWindow


@il.asset(partitioning=il.TimePartitionConfig(column="date"))
def daily(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset
def whole() -> list:  # noqa: D103
    return []


class TestPartitions:
    def test_the_unpartitioned_whole_is_one_partition_none(self):
        context = IOContext(asset=whole())
        assert context.partitions == [None]
        assert context.window is False

    def test_a_partition_is_itself(self):
        partition = TimePartition(datetime.date(2026, 9, 7))
        context = IOContext(asset=daily(), partition_or_window=partition)
        assert context.partitions == [partition]
        assert context.window is False

    def test_a_window_is_its_partitions_in_window_order(self):
        window = TimePartitionWindow(datetime.date(2026, 9, 7), datetime.date(2026, 9, 9))
        context = IOContext(asset=daily(), partition_or_window=window)
        assert [p.id if p else None for p in context.partitions] == ["2026-09-09", "2026-09-08", "2026-09-07"]
        assert context.window is True


class TestSlices:
    def test_a_single_partition_receives_the_data_whole(self):
        rows = [{"date": "2026-09-07"}, {"date": "2026-09-08"}]
        assert IOContext(asset=whole()).slices(rows) == [(None, rows)]
        partition = TimePartition(datetime.date(2026, 9, 7))
        assert IOContext(asset=daily(), partition_or_window=partition).slices(rows) == [(partition, rows)]

    def test_a_window_slices_on_the_partition_column(self):
        rows = [{"date": "2026-09-07", "v": 1}, {"date": "2026-09-08", "v": 2}, {"date": "2026-09-08", "v": 3}]
        window = TimePartitionWindow(datetime.date(2026, 9, 7), datetime.date(2026, 9, 8))
        pairs = IOContext(asset=daily(), partition_or_window=window).slices(rows)
        sliced = {p.id if p else None: chunk for p, chunk in pairs}
        assert sliced == {"2026-09-07": [rows[0]], "2026-09-08": rows[1:]}

    def test_a_window_over_an_unpartitioned_asset_is_a_config_error(self):
        window = TimePartitionWindow(datetime.date(2026, 9, 7), datetime.date(2026, 9, 8))
        with pytest.raises(ConfigError, match="not partitioned"):
            IOContext(asset=whole(), partition_or_window=window).slices([])
