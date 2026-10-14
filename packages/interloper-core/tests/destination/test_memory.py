"""Tests for ``interloper.destination.memory``."""

import datetime

import pytest

import interloper as il
from interloper.destination import IOContext, MemoryDestination
from interloper.errors import ConfigError
from interloper.partitioning import TimePartition, TimePartitionWindow


@il.asset(partitioning=il.TimePartitionConfig(column="date"))
def partitioned_asset(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@pytest.fixture(autouse=True)
def clear_storage():  # noqa: D103
    MemoryDestination.clear()
    yield
    MemoryDestination.clear()


class TestWindowWrites:
    """Partition-window writes split tabular data per partition."""

    def test_window_splits_list_rows(self):
        dest = MemoryDestination(id="mem")
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        rows = [
            {"date": "2024-01-01", "v": 1},
            {"date": "2024-01-02", "v": 2},
            {"date": "2024-01-02", "v": 3},
        ]
        dest.write(IOContext(asset=partitioned_asset(), window=window), rows)

        part1 = TimePartition(datetime.date(2024, 1, 1))
        part2 = TimePartition(datetime.date(2024, 1, 2))
        day1 = dest.read(IOContext(asset=partitioned_asset(), window=part1.window))
        day2 = dest.read(IOContext(asset=partitioned_asset(), window=part2.window))
        assert day1 == [{"date": "2024-01-01", "v": 1}]
        assert day2 == [{"date": "2024-01-02", "v": 2}, {"date": "2024-01-02", "v": 3}]

    def test_window_splits_dataframe(self):
        pd = pytest.importorskip("pandas")

        dest = MemoryDestination(id="mem")
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        df = pd.DataFrame([{"date": "2024-01-01", "v": 1}, {"date": "2024-01-02", "v": 2}])
        dest.write(IOContext(asset=partitioned_asset(), window=window), df)

        part1 = TimePartition(datetime.date(2024, 1, 1))
        day1 = dest.read(IOContext(asset=partitioned_asset(), window=part1.window))
        assert len(day1) == 1
        assert day1.iloc[0]["v"] == 1


class TestViews:
    """Memory holds no views: both hooks refuse."""

    def test_reference_is_refused(self):
        destination = il.MemoryDestination(id="memory")

        @il.asset
        def rows() -> list:
            return []

        with pytest.raises(ConfigError, match="cannot reference"):
            destination.reference(rows())

    def test_create_view_is_refused(self):
        destination = il.MemoryDestination(id="memory")

        @il.asset
        def rows() -> list:
            return []

        with pytest.raises(ConfigError, match="does not support views"):
            destination.create_view(rows(), "SELECT 1")


def test_storage_paths_are_unchanged_by_the_rename():
    """A partitioned write lands under ``<column>=<partition key>``, as before."""

    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def daily() -> list[dict]:
        return [{"date": datetime.date(2026, 1, 15), "x": 1}]

    destination = MemoryDestination()

    il.DAG(daily(destinations=[destination])).materialize(TimePartition(datetime.date(2026, 1, 15)))

    assert any("date=2026-01-15" in path for path in MemoryDestination._storage)
