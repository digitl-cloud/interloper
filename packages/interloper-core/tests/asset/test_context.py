"""Tests for ``interloper.asset.context``."""

import datetime as dt

import pytest

from interloper.asset.context import ExecutionContext
from interloper.partitioning.base import Partition, PartitionConfig
from interloper.partitioning.time import TimeGranularity, TimePartition, TimePartitionConfig, TimePartitionWindow


def _context(partition_or_window: object, allow_window: bool = False) -> ExecutionContext:
    return ExecutionContext(
        asset_key="asset",
        partitioning=TimePartitionConfig(column="date", allow_window=allow_window),
        partition_or_window=partition_or_window,  # ty: ignore[invalid-argument-type]
    )


class TestPartition:
    def test_hands_back_the_partition(self) -> None:
        partition = TimePartition(dt.date(2026, 1, 1))
        assert _context(partition).partition is partition

    def test_the_partition_answers_its_own_scope(self) -> None:
        # The point of handing over the object: no per-granularity accessor on
        # the context re-derives what the partition already knows.
        partition = _context(TimePartition(dt.date(2026, 1, 1))).partition
        assert isinstance(partition, TimePartition)
        assert partition.id == "2026-01-01"
        assert partition.granularity is TimeGranularity.DAY
        assert partition.bounds == (dt.date(2026, 1, 1), dt.date(2026, 1, 2))

    def test_works_for_a_non_time_partition(self) -> None:
        context = ExecutionContext(
            asset_key="asset",
            partitioning=PartitionConfig(column="region"),
            partition_or_window=Partition("eu"),
        )
        assert context.partition.id == "eu"

    def test_raises_when_unpartitioned(self) -> None:
        with pytest.raises(AttributeError, match="asset is not partitioned"):
            ExecutionContext(asset_key="asset").partition

    def test_raises_without_a_partition(self) -> None:
        with pytest.raises(AttributeError, match="no partition provided"):
            _context(None).partition

    def test_raises_on_a_window(self) -> None:
        context = _context(
            TimePartitionWindow(start=dt.date(2026, 1, 1), end=dt.date(2026, 1, 3)),
            allow_window=True,
        )
        with pytest.raises(AttributeError, match="partition window, not a partition"):
            context.partition


class TestWindow:
    def test_hands_back_the_window(self) -> None:
        window = TimePartitionWindow(start=dt.date(2026, 1, 1), end=dt.date(2026, 1, 3))
        assert _context(window, allow_window=True).window is window

    def test_a_single_partition_normalizes_to_one_partition_window(self) -> None:
        # A windowed asset is still run per partition by the platform, so
        # reading `window` must not depend on how the run was scoped.
        window = _context(TimePartition(dt.date(2026, 1, 1)), allow_window=True).window
        assert isinstance(window, TimePartitionWindow)
        assert (window.start, window.end) == (dt.date(2026, 1, 1), dt.date(2026, 1, 1))
        assert window.partition_count() == 1

    def test_normalized_window_keeps_the_granularity(self) -> None:
        window = _context(TimePartition(dt.date(2026, 1, 1)), allow_window=True).window
        assert isinstance(window, TimePartitionWindow)
        assert window.granularity is TimeGranularity.DAY

    def test_raises_when_windows_are_not_allowed(self) -> None:
        context = _context(TimePartition(dt.date(2026, 1, 1)))
        with pytest.raises(AttributeError, match="does not allow windows"):
            context.window

    def test_raises_when_not_time_partitioned(self) -> None:
        context = ExecutionContext(
            asset_key="asset",
            partitioning=PartitionConfig(column="region"),
            partition_or_window=Partition("eu"),
        )
        with pytest.raises(AttributeError, match="not time-partitioned"):
            context.window

    def test_raises_without_a_partition(self) -> None:
        with pytest.raises(AttributeError, match="no partition provided"):
            _context(None, allow_window=True).window


class TestPartitionDate:
    def test_returns_date_from_time_partition(self) -> None:
        context = _context(TimePartition(dt.date(2026, 1, 1)))
        assert context.partition_date == dt.date(2026, 1, 1)

    def test_returns_the_period_start(self) -> None:
        # `TimePartition` truncated the value on construction, so the context
        # has nothing left to coerce: it hands back what the partition holds.
        context = _context(TimePartition(dt.datetime(2026, 1, 1, 9, 30, tzinfo=dt.timezone.utc)))
        assert context.partition_date == dt.date(2026, 1, 1)

    def test_raises_on_a_window(self) -> None:
        context = _context(
            TimePartitionWindow(start=dt.date(2026, 1, 1), end=dt.date(2026, 1, 3)),
            allow_window=True,
        )
        with pytest.raises(AttributeError, match="partition window, not a partition"):
            context.partition_date


class TestIdentityAndMetadata:
    """What the context exposes about the asset it runs for."""

    def test_the_asset_key_is_available(self) -> None:
        context = ExecutionContext(asset_key="source.asset")

        assert context.asset_key == "source.asset"

    def test_metadata_defaults_to_empty(self) -> None:
        assert ExecutionContext(asset_key="asset").metadata == {}

    def test_metadata_is_handed_back_as_given(self) -> None:
        context = ExecutionContext(asset_key="asset", metadata={"run_id": "r1"})

        assert context.metadata == {"run_id": "r1"}


class TestLogger:
    """The event-emitting logger the asset's ``data()`` writes through."""

    def test_it_is_built_once_and_reused(self) -> None:
        context = ExecutionContext(asset_key="asset")

        assert context.logger is context.logger

    def test_messages_become_log_events(self) -> None:
        from interloper.events import Event, EventBus, EventType

        context = ExecutionContext(
            asset_key="source.asset",
            metadata={"run_id": "r1"},
            asset_id="a1",
            source_id="s1",
        )
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
        assert logs[0].metadata["source_id"] == "s1"
        assert logs[0].metadata["run_id"] == "r1"


class TestPartitionDateGranularity:
    """``partition_date`` is the day-granularity accessor."""

    def test_a_monthly_asset_is_pointed_at_partition_instead(self) -> None:
        context = ExecutionContext(
            asset_key="asset",
            partitioning=TimePartitionConfig(column="date", granularity=TimeGranularity.MONTH),
            partition_or_window=TimePartition(dt.date(2026, 6, 1), TimeGranularity.MONTH),
        )

        with pytest.raises(AttributeError, match="partitioned by month. Use `context.partition`"):
            _ = context.partition_date


def test_an_unpartitioned_asset_explains_which_accessor_is_unavailable() -> None:
    """The error names the accessor that was reached for."""
    context = ExecutionContext(asset_key="asset")

    with pytest.raises(AttributeError, match=r"`context.partition_date` is not available, asset is not partitioned"):
        _ = context.partition_date
