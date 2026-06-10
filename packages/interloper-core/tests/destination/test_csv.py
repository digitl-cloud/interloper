"""Tests for ``interloper.destination.csv``."""

import datetime

from pydantic import Field

import interloper as il
from interloper.destination import IOContext
from interloper.destination.csv import CSVDestination
from interloper.partitioning.time import TimePartition, TimePartitionWindow


@il.asset(partitioning=il.TimePartitionConfig(column="date"))
def partitioned_asset(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset
def plain_asset() -> list:  # noqa: D103
    return []


class RowSchema(il.Schema):
    date: datetime.date | None = Field(...)
    clicks: int | None = Field(...)
    cost: float | None = Field(...)
    name: str | None = Field(...)


class TestRoundtrip:
    """Write/read roundtrips."""

    def test_unpartitioned_roundtrip(self, tmp_path):
        dest = CSVDestination(id="csv", base_path=str(tmp_path))
        ctx = IOContext(asset=plain_asset())
        dest.write(ctx, [{"a": "1", "b": "x"}])
        assert dest.read(ctx) == [{"a": "1", "b": "x"}]

    def test_schema_read_restores_types(self, tmp_path):
        dest = CSVDestination(id="csv", base_path=str(tmp_path))
        ctx = IOContext(asset=plain_asset(), schema=RowSchema)
        dest.write(ctx, [{"date": datetime.date(2024, 1, 1), "clicks": 3, "cost": 1.5, "name": None}])
        rows = dest.read(ctx)
        assert rows == [{"date": datetime.date(2024, 1, 1), "clicks": 3, "cost": 1.5, "name": None}]

    def test_dataframe_write_accepted(self, tmp_path):
        import pandas as pd

        dest = CSVDestination(id="csv", base_path=str(tmp_path))
        ctx = IOContext(asset=plain_asset())
        dest.write(ctx, pd.DataFrame([{"a": 1}]))
        assert dest.read(ctx) == [{"a": "1"}]


class TestWindowWrites:
    """Partition-window writes split rows per partition."""

    def test_window_splits_rows_by_partition(self, tmp_path):
        dest = CSVDestination(id="csv", base_path=str(tmp_path))
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        rows = [
            {"date": "2024-01-01", "v": "a"},
            {"date": "2024-01-02", "v": "b"},
            {"date": "2024-01-02", "v": "c"},
        ]
        dest.write(IOContext(asset=partitioned_asset(), partition_or_window=window), rows)

        part1 = TimePartition(datetime.date(2024, 1, 1))
        part2 = TimePartition(datetime.date(2024, 1, 2))
        day1 = dest.read(IOContext(asset=partitioned_asset(), partition_or_window=part1))
        day2 = dest.read(IOContext(asset=partitioned_asset(), partition_or_window=part2))
        assert day1 == [{"date": "2024-01-01", "v": "a"}]
        assert day2 == [{"date": "2024-01-02", "v": "b"}, {"date": "2024-01-02", "v": "c"}]

    def test_partition_row_counts(self, tmp_path):
        dest = CSVDestination(id="csv", base_path=str(tmp_path))
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        rows = [
            {"date": "2024-01-01", "v": "a"},
            {"date": "2024-01-02", "v": "b"},
            {"date": "2024-01-02", "v": "c"},
        ]
        ctx = IOContext(asset=partitioned_asset(), partition_or_window=window)
        dest.write(ctx, rows)
        assert dest.partition_row_counts(ctx) == {"2024-01-01": 1, "2024-01-02": 2}
