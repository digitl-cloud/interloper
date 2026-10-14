"""Tests for ``interloper.partitioning.base``: the contracts every kind implements, through the list kind."""

from __future__ import annotations

import datetime as dt

import pytest

from interloper.errors import ConfigError
from interloper.partitioning import (
    ListPartition,
    ListPartitionConfig,
    ListPartitionWindow,
    Partition,
    PartitionConfig,
    PartitionFilter,
    PartitionWindow,
    TimePartition,
    TimePartitionConfig,
    TimePartitionWindow,
)


class TestPartitionFilter:
    def test_carries_values_or_bounds(self):
        assert PartitionFilter("region", values=("eu",)).bounds is None
        assert PartitionFilter("date", bounds=(dt.date(2026, 1, 1), dt.date(2026, 1, 2))).values is None

    def test_rejects_both_and_neither(self):
        with pytest.raises(ValueError, match="exactly one"):
            PartitionFilter("region")
        with pytest.raises(ValueError, match="exactly one"):
            PartitionFilter("region", values=("eu",), bounds=("a", "b"))


class TestPartition:
    def test_str_is_the_id(self):
        assert str(ListPartition("eu")) == "eu"
        assert str(TimePartition(dt.date(2026, 1, 15))) == "2026-01-15"

    def test_window_is_the_window_of_one(self):
        window = ListPartition("eu").window
        assert isinstance(window, PartitionWindow)
        assert list(window) == [ListPartition("eu")]

    def test_the_bases_are_abstract(self):
        with pytest.raises(TypeError):
            Partition()
        with pytest.raises(TypeError):
            PartitionWindow("a", "b")
        with pytest.raises(TypeError):
            PartitionConfig(column="x")


class TestPartitionWindow:
    def test_of_a_partition_is_its_window(self):
        partition = TimePartition(dt.date(2026, 1, 15))
        assert PartitionWindow.of(partition) == partition.window

    def test_of_a_window_is_the_window(self):
        window = TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 3))
        assert PartitionWindow.of(window) is window

    def test_of_none_is_none(self):
        assert PartitionWindow.of(None) is None

    def test_str_names_one_partition_by_its_id(self):
        assert str(ListPartitionWindow("eu", "eu", ("eu", "us"))) == "eu"
        assert str(TimePartition(dt.date(2026, 1, 15)).window) == "2026-01-15"

    def test_str_names_a_range_by_its_bounds(self):
        assert str(ListPartitionWindow("eu", "apac", ("eu", "us", "apac"))) == "eu..apac"
        window = TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 15))
        assert str(window) == "2026-01-01..2026-01-15"

    def test_iteration_and_length_agree(self):
        window = ListPartitionWindow("eu", "apac", ("eu", "us", "apac"))
        assert len(window) == 3
        assert [partition.key for partition in window] == ["eu", "us", "apac"]


class TestPartitionConfig:
    def test_a_window_is_required(self):
        with pytest.raises(ConfigError, match="given no window"):
            ListPartitionConfig(column="region", values=("eu",)).check(None)

    def test_several_partitions_need_allow_window(self):
        config = ListPartitionConfig(column="region", values=("eu", "us"))
        with pytest.raises(ConfigError, match="allow_window=False"):
            config.check(ListPartitionWindow("eu", "us", ("eu", "us")))
        ListPartitionConfig(column="region", values=("eu", "us"), allow_window=True).check(
            ListPartitionWindow("eu", "us", ("eu", "us"))
        )

    def test_window_parses_both_keys(self):
        config = ListPartitionConfig(column="region", values=("eu", "us", "apac"))
        window = config.window("us", "apac")
        assert [partition.key for partition in window] == ["us", "apac"]

    def test_definition_round_trips_through_json_like_values(self):
        config = TimePartitionConfig(column="date", allow_window=True, start=dt.date(2026, 1, 1))
        definition = config.to_definition()
        assert definition["kind"] == "time"
        loosened = {**definition, "granularity": "day", "start": "2026-01-01"}
        assert PartitionConfig.from_definition(loosened) == config

    def test_definition_of_the_list_kind_round_trips(self):
        config = ListPartitionConfig(column="region", values=("eu", "us"))
        expected = {"kind": "list", "column": "region", "allow_window": False, "values": ("eu", "us")}
        assert config.to_definition() == expected
        assert PartitionConfig.from_definition({**config.to_definition(), "values": ["eu", "us"]}) == config

    def test_an_unknown_kind_is_a_config_error(self):
        with pytest.raises(ConfigError, match="no known kind"):
            PartitionConfig.from_definition({"column": "x"})
        with pytest.raises(ConfigError, match="no known kind"):
            PartitionConfig.from_definition({"kind": "hash", "column": "x"})

    def test_kinds_are_never_compatible_across(self):
        time = TimePartitionConfig(column="date")
        regions = ListPartitionConfig(column="region", values=("eu",))
        assert time.compatible(regions) is False
        assert regions.compatible(time) is False


def test_an_unknown_field_in_a_definition_is_a_config_error():
    """A definition carrying a field its kind does not declare is a configuration error naming it."""
    with pytest.raises(ConfigError, match=r"does not declare: \['bogus'\]"):
        PartitionConfig.from_definition({"kind": "time", "column": "date", "bogus": 1})


def test_a_missing_field_in_a_definition_is_not_reported_as_undeclared():
    """A definition lacking a required field fails on that field, not as an undeclared one."""
    with pytest.raises(TypeError, match=r"column"):
        PartitionConfig.from_definition({"kind": "time", "start": "2026-01-01"})
