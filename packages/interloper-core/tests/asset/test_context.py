"""Tests for ``interloper.asset.context``."""

import datetime as dt

import pytest

from interloper.asset.context import ExecutionContext
from interloper.partitioning.base import Partition
from interloper.partitioning.time import TimePartition, TimePartitionConfig, TimePartitionWindow


def _context(partition_or_window: object, allow_window: bool = False) -> ExecutionContext:
    return ExecutionContext(
        asset_key="asset",
        partitioning=TimePartitionConfig(column="date", allow_window=allow_window),
        partition_or_window=partition_or_window,  # ty: ignore[invalid-argument-type]
    )


class TestPartitionDate:
    def test_returns_date_from_time_partition(self) -> None:
        ctx = _context(TimePartition(dt.date(2026, 1, 1)))
        assert ctx.partition_date == dt.date(2026, 1, 1)

    def test_coerces_string_value_from_base_partition(self) -> None:
        # A base ``Partition`` does not type its value, so a string can slip
        # through; the context must still hand the asset a real ``date``.
        ctx = _context(Partition("2026-01-01"))
        assert ctx.partition_date == dt.date(2026, 1, 1)

    def test_raises_type_error_on_unparseable_value(self) -> None:
        ctx = _context(Partition("not-a-date"))
        with pytest.raises(TypeError):
            ctx.partition_date


class TestPartitionDateWindow:
    def test_returns_dates_from_window(self) -> None:
        ctx = _context(
            TimePartitionWindow(start=dt.date(2026, 1, 1), end=dt.date(2026, 1, 3)),
            allow_window=True,
        )
        assert ctx.partition_date_window == (dt.date(2026, 1, 1), dt.date(2026, 1, 3))

    def test_coerces_string_value_from_base_partition(self) -> None:
        ctx = _context(Partition("2026-01-01"), allow_window=True)
        assert ctx.partition_date_window == (dt.date(2026, 1, 1), dt.date(2026, 1, 1))
