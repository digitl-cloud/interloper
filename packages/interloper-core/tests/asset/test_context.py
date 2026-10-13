"""Tests for ``interloper.asset.context``."""

from __future__ import annotations

import datetime as dt

import pytest

import interloper as il
from interloper.asset.context import ExecutionContext
from interloper.partitioning import (
    ListPartition,
    ListPartitionConfig,
    TimeGranularity,
    TimePartition,
    TimePartitionConfig,
    TimePartitionWindow,
)


@il.asset(partitioning=TimePartitionConfig(column="date"))
def daily(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset(partitioning=TimePartitionConfig(column="date", allow_window=True))
def windowed(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset(partitioning=TimePartitionConfig(column="date", granularity=TimeGranularity.MONTH))
def monthly(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset(partitioning=ListPartitionConfig(column="region", values=("eu", "us")))
def regional(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset
def whole() -> list:  # noqa: D103
    return []


JAN_1 = TimePartition(dt.date(2026, 1, 1))
JAN_1_TO_3 = TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 3))


class TestAssetAndMetadata:
    def test_hands_back_the_asset(self) -> None:
        asset = daily()
        assert ExecutionContext(asset).asset is asset

    def test_metadata_defaults_to_empty(self) -> None:
        assert ExecutionContext(daily()).metadata == {}

    def test_metadata_is_handed_back_as_given(self) -> None:
        assert ExecutionContext(daily(), metadata={"run_id": "r1"}).metadata == {"run_id": "r1"}


class TestWindow:
    def test_hands_back_the_window(self) -> None:
        assert ExecutionContext(windowed(), JAN_1_TO_3).window is JAN_1_TO_3

    def test_a_window_of_one_is_readable_without_allow_window(self) -> None:
        # The config's check already bounds a non-windowed asset to one partition.
        window = ExecutionContext(daily(), JAN_1.window).window
        assert isinstance(window, TimePartitionWindow)
        assert (window.start, window.end) == (dt.date(2026, 1, 1), dt.date(2026, 1, 1))
        assert window.granularity is TimeGranularity.DAY

    def test_raises_when_unpartitioned(self) -> None:
        with pytest.raises(AttributeError, match=r"`context\.window` is not available, asset is not partitioned"):
            ExecutionContext(whole(), JAN_1.window).window

    def test_raises_without_a_window(self) -> None:
        with pytest.raises(AttributeError, match=r"`context\.window` is not available, no window provided"):
            ExecutionContext(daily()).window


class TestPartition:
    def test_hands_back_the_only_partition(self) -> None:
        assert ExecutionContext(daily(), JAN_1.window).partition == JAN_1

    def test_the_partition_answers_its_own_scope(self) -> None:
        partition = ExecutionContext(daily(), JAN_1.window).partition
        assert isinstance(partition, TimePartition)
        assert partition.id == "2026-01-01"
        assert partition.bounds == (dt.date(2026, 1, 1), dt.date(2026, 1, 2))

    def test_works_for_a_list_partition(self) -> None:
        assert ExecutionContext(regional(), ListPartition("eu").window).partition.id == "eu"

    def test_raises_when_unpartitioned(self) -> None:
        with pytest.raises(AttributeError, match="asset is not partitioned"):
            ExecutionContext(whole()).partition

    def test_raises_without_a_window(self) -> None:
        with pytest.raises(AttributeError, match="no window provided"):
            ExecutionContext(daily()).partition

    def test_raises_on_several_partitions(self) -> None:
        with pytest.raises(AttributeError, match=r"`context\.partition` is not available, the run covers 3 partitions"):
            ExecutionContext(windowed(), JAN_1_TO_3).partition


class TestPartitionDate:
    def test_returns_the_day(self) -> None:
        assert ExecutionContext(daily(), JAN_1.window).partition_date == dt.date(2026, 1, 1)

    def test_a_monthly_asset_is_pointed_at_partition_instead(self) -> None:
        context = ExecutionContext(monthly(), TimePartition(dt.date(2026, 6, 1), TimeGranularity.MONTH).window)
        with pytest.raises(AttributeError, match=r"partitioned by month\. Use `context\.partition`"):
            context.partition_date

    def test_a_list_asset_is_not_time_partitioned(self) -> None:
        with pytest.raises(AttributeError, match="asset is not time-partitioned"):
            ExecutionContext(regional(), ListPartition("eu").window).partition_date

    def test_raises_on_several_partitions(self) -> None:
        with pytest.raises(AttributeError, match="covers 3 partitions"):
            ExecutionContext(windowed(), JAN_1_TO_3).partition_date

    def test_an_unpartitioned_asset_names_the_accessor(self) -> None:
        message = r"`context\.partition_date` is not available, asset is not partitioned"
        with pytest.raises(AttributeError, match=message):
            ExecutionContext(whole()).partition_date


class TestLogger:
    def test_it_is_built_once_and_reused(self) -> None:
        context = ExecutionContext(daily())
        assert context.logger is context.logger

    def test_messages_become_log_events_attributed_to_the_asset(self) -> None:
        from interloper.events import Event, EventBus, EventType

        asset = daily(id="a1")
        context = ExecutionContext(asset, JAN_1.window, metadata={"run_id": "r1"})
        captured: list[Event] = []
        EventBus.subscribe(captured.append)
        try:
            context.logger.info("fetching page 1")
            EventBus.flush(timeout=5.0)
        finally:
            EventBus.unsubscribe(captured.append)

        logs = [event for event in captured if event.type is EventType.LOG]
        assert len(logs) == 1
        assert logs[0].metadata["message"] == "fetching page 1"
        assert logs[0].metadata["component_id"] == "a1"
        assert logs[0].metadata["component_key"] == "daily"
        assert logs[0].metadata["run_id"] == "r1"
