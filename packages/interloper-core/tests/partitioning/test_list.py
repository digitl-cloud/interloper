"""Tests for ``interloper.partitioning.list``."""

from __future__ import annotations

import pytest

from interloper.errors import PartitionError
from interloper.partitioning.base import PartitionFilter
from interloper.partitioning.list import ListPartition, ListPartitionConfig, ListPartitionWindow

REGIONS = ("eu", "us", "apac")


class TestListPartition:
    def test_id_is_the_value(self):
        assert ListPartition("eu").id == "eu"

    def test_filter_is_membership_of_its_value(self):
        assert ListPartition("eu").filter("region") == PartitionFilter("region", values=("eu",))

    def test_slice_selects_rows_by_equality(self):
        rows = [{"region": "eu", "v": 1}, {"region": "us", "v": 2}, {"region": "eu", "v": 3}]
        assert ListPartition("eu").slice(rows, "region") == [rows[0], rows[2]]

    def test_window_covers_only_itself(self):
        window = ListPartition("eu").window
        assert window == ListPartitionWindow("eu", "eu", ("eu",))


class TestListPartitionWindow:
    def test_partitions_are_the_declared_slice_in_order(self):
        window = ListPartitionWindow("us", "apac", REGIONS)
        assert window.partitions == [ListPartition("us"), ListPartition("apac")]
        assert window.first == ListPartition("us")
        assert window.last == ListPartition("apac")
        assert len(window) == 2

    def test_filter_lists_every_id(self):
        window = ListPartitionWindow("eu", "us", REGIONS)
        assert window.filter("region") == PartitionFilter("region", values=("eu", "us"))

    def test_bounds_must_be_declared_values(self):
        with pytest.raises(ValueError, match="not one of the declared values"):
            ListPartitionWindow("eu", "mars", REGIONS)

    def test_bounds_must_be_in_declared_order(self):
        with pytest.raises(ValueError, match="before it starts"):
            ListPartitionWindow("apac", "eu", REGIONS)

    def test_values_are_normalised_to_a_tuple(self):
        assert ListPartitionWindow("eu", "us", list(REGIONS)).values == REGIONS  # ty: ignore[invalid-argument-type]


class TestListPartitionConfig:
    def test_values_are_normalised_and_validated(self):
        assert ListPartitionConfig(column="region", values=["eu", "us"]).values == ("eu", "us")  # ty: ignore[invalid-argument-type]
        with pytest.raises(ValueError, match="at least one"):
            ListPartitionConfig(column="region", values=())
        with pytest.raises(ValueError, match="duplicate"):
            ListPartitionConfig(column="region", values=("eu", "eu"))
        with pytest.raises(ValueError, match="empty"):
            ListPartitionConfig(column="region", values=("eu", ""))

    def test_partitions_are_every_value_in_order(self):
        config = ListPartitionConfig(column="region", values=REGIONS)
        assert [partition.id for partition in config.partitions] == list(REGIONS)

    def test_partition_parses_a_declared_key(self):
        assert ListPartitionConfig(column="region", values=REGIONS).partition("us") == ListPartition("us")

    def test_partition_rejects_an_undeclared_key(self):
        with pytest.raises(PartitionError, match="'mars' is not one of the declared partitions"):
            ListPartitionConfig(column="region", values=REGIONS).partition("mars")

    def test_window_rejects_an_inverted_range(self):
        with pytest.raises(PartitionError, match="before it starts"):
            ListPartitionConfig(column="region", values=REGIONS).window("apac", "eu")

    def test_check_accepts_a_window_of_declared_values(self):
        config = ListPartitionConfig(column="region", values=REGIONS, allow_window=True)
        config.check(ListPartitionWindow("eu", "us", REGIONS))

    def test_a_window_of_one_built_from_a_partition_passes(self):
        # A bare partition's window carries only its own value; membership is what is checked.
        ListPartitionConfig(column="region", values=REGIONS).check(ListPartition("us").window)

    def test_check_rejects_an_undeclared_partition(self):
        config = ListPartitionConfig(column="region", values=("eu", "us"))
        with pytest.raises(PartitionError, match="'apac' is not one of the declared partitions"):
            config.check(ListPartitionWindow("apac", "apac", ("apac",)))

    def test_check_rejects_a_window_of_another_kind(self):
        import datetime as dt

        from interloper.partitioning.time import TimePartition

        with pytest.raises(PartitionError, match="expects a list window"):
            ListPartitionConfig(column="region", values=REGIONS).check(TimePartition(dt.date(2026, 1, 1)).window)

    def test_compatible_means_the_same_values(self):
        config = ListPartitionConfig(column="region", values=REGIONS)
        assert config.compatible(ListPartitionConfig(column="market", values=REGIONS)) is True
        assert config.compatible(ListPartitionConfig(column="region", values=("eu",))) is False
