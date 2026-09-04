"""Tests for ``interloper.partitioning.base``."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from interloper.partitioning.base import Partition, PartitionConfig, PartitionWindow


@dataclass(frozen=True)
class StringPartition(Partition):
    """Minimal concrete partition keyed by a plain string."""


@dataclass(frozen=True)
class StringPartitionWindow(PartitionWindow):
    """Minimal concrete window over two string bounds."""

    def __iter__(self) -> Iterator[Partition]:
        """Yield both bounds as partitions.

        Yields:
            One partition per bound.
        """
        yield StringPartition(self.start)
        yield StringPartition(self.end)


class TestPartitionConfig:
    """The declaration an asset carries."""

    def test_windows_are_opt_in(self):
        assert PartitionConfig(column="date").allow_window is False

    def test_the_column_is_required(self):
        assert PartitionConfig(column="date", allow_window=True).column == "date"


class TestPartitionIdentity:
    """The id is what reaches destination paths and partition keys."""

    def test_derived_from_the_value(self):
        assert StringPartition("2026-06-01").id == "2026-06-01"

    def test_a_non_string_value_is_stringified(self):
        assert StringPartition(42).id == "42"


class TestPartitionSlice:
    """Windowed writes are split per partition by slicing on the column."""

    def test_rows_are_filtered_to_the_partitions_own_value(self):
        rows = [
            {"date": "2026-06-01", "v": 1},
            {"date": "2026-06-02", "v": 2},
            {"date": "2026-06-01", "v": 3},
        ]

        sliced = StringPartition("2026-06-01").slice(rows, "date")

        assert sliced == [{"date": "2026-06-01", "v": 1}, {"date": "2026-06-01", "v": 3}]

    def test_no_matching_rows_yields_nothing(self):
        rows = [{"date": "2026-06-02", "v": 2}]

        assert StringPartition("2026-06-01").slice(rows, "date") == []

    def test_unrecognised_data_passes_through_unchanged(self):
        # Nothing can split an arbitrary object, so it reaches the
        # destination whole rather than being dropped.
        payload = object()

        assert StringPartition("2026-06-01").slice(payload, "date") is payload


class TestPartitionWindowIdentity:
    """A window's id names both of its inclusive bounds."""

    def test_derived_from_both_bounds(self):
        window = StringPartitionWindow(start="2026-06-01", end="2026-06-03")

        assert window.id == "2026-06-01-2026-06-03"

    def test_iteration_is_the_subclass_contract(self):
        window = StringPartitionWindow(start="2026-06-01", end="2026-06-03")

        assert [partition.id for partition in window] == ["2026-06-01", "2026-06-03"]


def test_a_window_and_a_partition_are_distinguishable_by_type() -> None:
    """Runners branch on the scope's type, so the hierarchy must stay disjoint."""
    partition: Any = StringPartition("2026-06-01")
    window: Any = StringPartitionWindow(start="2026-06-01", end="2026-06-03")

    assert isinstance(partition, Partition) and not isinstance(partition, PartitionWindow)
    assert isinstance(window, PartitionWindow) and not isinstance(window, Partition)
