"""Tests for ``interloper.partitioning.snapshot``."""

import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from interloper.partitioning import (
    ListPartition,
    PartitionConfig,
    SnapshotPartitionConfig,
    TimeGranularity,
    TimePartition,
    TimePartitionConfig,
    TimePartitionWindow,
)

BERLIN = ZoneInfo("Europe/Berlin")


class TestSnapshotPartitionConfig:
    def test_defaults_to_daily_and_writes_no_window(self) -> None:
        config = SnapshotPartitionConfig(column="date")

        assert (config.granularity, config.windowed) == (TimeGranularity.DAY, False)
        assert TimePartitionConfig.windowed is True

    def test_it_accepts_any_window_or_none(self) -> None:
        config = SnapshotPartitionConfig(column="date")

        config.check(None)
        config.check(TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 31)))
        config.check(TimePartition(dt.date(2026, 1, 1), TimeGranularity.MONTH).window)
        config.check(ListPartition("eu").window)

    def test_the_definition_round_trips(self) -> None:
        config = SnapshotPartitionConfig(column="date", granularity=TimeGranularity.MONTH)

        assert config.to_definition()["kind"] == "snapshot"
        assert PartitionConfig.from_definition(config.to_definition()) == config


class TestObservedPeriod:
    """The one partition a snapshot writes is the period of its run's observation instant."""

    def test_a_past_window_writes_the_observation_day(self) -> None:
        past = TimePartitionWindow(dt.date(2026, 10, 26), dt.date(2026, 10, 28))
        observed_at = dt.datetime(2026, 10, 29, 6, tzinfo=dt.timezone.utc)

        window = SnapshotPartitionConfig(column="date").effective_window(past, observed_at)

        assert window == TimePartition(dt.date(2026, 10, 29)).window

    @pytest.mark.parametrize(
        ("granularity", "key"),
        [
            (TimeGranularity.HOUR, "2026-10-30T23"),
            (TimeGranularity.DAY, "2026-10-31"),
            (TimeGranularity.MONTH, "2026-10"),
            (TimeGranularity.YEAR, "2026"),
        ],
    )
    def test_each_granularity_truncates_the_instant(self, granularity: TimeGranularity, key: str) -> None:
        # 00:30 in Berlin on 31 October is 23:30 UTC on the 30th: the day is Berlin's, the hour UTC's
        observed_at = dt.datetime(2026, 10, 31, 0, 30, tzinfo=BERLIN)

        window = SnapshotPartitionConfig(column="date", granularity=granularity).effective_window(None, observed_at)

        assert (len(window), window.first.key) == (1, key)

    def test_the_day_is_the_zone_of_the_instant(self) -> None:
        late_in_utc = dt.datetime(2026, 10, 29, 23, 30, tzinfo=dt.timezone.utc)

        utc = SnapshotPartitionConfig(column="date").effective_window(None, late_in_utc)
        berlin = SnapshotPartitionConfig(column="date").effective_window(None, late_in_utc.astimezone(BERLIN))

        assert (utc.first.key, berlin.first.key) == ("2026-10-29", "2026-10-30")

    def test_a_time_partitioning_consumes_the_run_window(self) -> None:
        window = TimePartition(dt.date(2026, 10, 26)).window
        observed_at = dt.datetime(2026, 10, 29, tzinfo=dt.timezone.utc)

        assert TimePartitionConfig(column="date").effective_window(window, observed_at) is window


class TestCompatibility:
    def test_only_a_snapshot_of_the_same_granularity_is_compatible(self) -> None:
        daily = SnapshotPartitionConfig(column="date")

        assert daily.compatible(SnapshotPartitionConfig(column="as_of")) is True
        assert daily.compatible(SnapshotPartitionConfig(column="date", granularity=TimeGranularity.MONTH)) is False
        assert daily.compatible(TimePartitionConfig(column="date")) is False
        assert TimePartitionConfig(column="date").compatible(daily) is False
