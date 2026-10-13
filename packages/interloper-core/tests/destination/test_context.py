"""Tests for ``interloper.destination.context``: the partitions an IO context spells out."""

import datetime

import pytest

import interloper as il
from interloper.destination import IOContext
from interloper.errors import ConfigError
from interloper.partitioning import (
    ListPartition,
    ListPartitionConfig,
    ListPartitionWindow,
    TimePartition,
    TimePartitionWindow,
)


@il.asset(partitioning=il.TimePartitionConfig(column="date"))
def daily(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset(partitioning=ListPartitionConfig(column="region", values=("eu", "us", "apac")))
def regional(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset
def whole() -> list:  # noqa: D103
    return []


class TestPartitions:
    def test_the_unpartitioned_whole_is_one_partition_none(self):
        assert IOContext(asset=whole()).partitions == [None]

    def test_a_window_of_one_is_its_partition(self):
        partition = TimePartition(datetime.date(2026, 9, 7))
        assert IOContext(asset=daily(), window=partition.window).partitions == [partition]

    def test_a_window_is_its_partitions_oldest_first(self):
        window = TimePartitionWindow(datetime.date(2026, 9, 7), datetime.date(2026, 9, 9))
        context = IOContext(asset=daily(), window=window)
        assert [p.id if p else None for p in context.partitions] == ["2026-09-07", "2026-09-08", "2026-09-09"]


class TestSlices:
    def test_a_single_partition_receives_the_data_whole(self):
        rows = [{"date": "2026-09-07"}, {"date": "2026-09-08"}]
        assert IOContext(asset=whole()).slices(rows) == [(None, rows)]
        partition = TimePartition(datetime.date(2026, 9, 7))
        assert IOContext(asset=daily(), window=partition.window).slices(rows) == [(partition, rows)]

    def test_a_window_slices_on_the_partition_column(self):
        rows = [{"date": "2026-09-07", "v": 1}, {"date": "2026-09-08", "v": 2}, {"date": "2026-09-08", "v": 3}]
        window = TimePartitionWindow(datetime.date(2026, 9, 7), datetime.date(2026, 9, 8))
        pairs = IOContext(asset=daily(), window=window).slices(rows)
        sliced = {p.id if p else None: chunk for p, chunk in pairs}
        assert sliced == {"2026-09-07": [rows[0]], "2026-09-08": rows[1:]}

    def test_a_list_window_slices_by_value(self):
        rows = [{"region": "eu", "v": 1}, {"region": "us", "v": 2}, {"region": "apac", "v": 3}]
        window = ListPartitionWindow("eu", "us", ("eu", "us", "apac"))
        pairs = IOContext(asset=regional(), window=window).slices(rows)
        assert [(p.id if p else None, chunk) for p, chunk in pairs] == [("eu", [rows[0]]), ("us", [rows[1]])]

    def test_a_window_over_an_unpartitioned_asset_is_a_config_error(self):
        window = TimePartitionWindow(datetime.date(2026, 9, 7), datetime.date(2026, 9, 8))
        with pytest.raises(ConfigError, match="not partitioned"):
            IOContext(asset=whole(), window=window).slices([])


def test_a_list_partition_keys_memory_storage_by_its_id() -> None:
    """The built-in destinations key storage by ``partition.id``, which a list partition provides."""
    il.MemoryDestination.clear()
    destination = il.MemoryDestination()
    context = IOContext(asset=regional(), window=ListPartition("eu").window)
    destination.write(context, [{"region": "eu", "v": 1}])
    assert destination.read(context) == [{"region": "eu", "v": 1}]
