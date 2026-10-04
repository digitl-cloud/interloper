"""Tests for ``interloper.job.cron`` — the CronJob config model."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from interloper.job.cron import CronJob
from interloper.partitioning.time import TimeGranularity


class TestTimezone:
    def test_defaults_to_utc(self) -> None:
        job = CronJob(cron="0 6 * * *")
        assert job.timezone == "UTC"

    def test_accepts_an_iana_zone(self) -> None:
        job = CronJob(cron="0 6 * * *", timezone="Europe/Berlin")
        assert job.timezone == "Europe/Berlin"

    def test_rejects_an_unknown_zone(self) -> None:
        with pytest.raises(ValueError, match="Unknown IANA timezone"):
            CronJob(cron="0 6 * * *", timezone="Mars/Olympus_Mons")

    def test_schema_carries_the_timezone_widget(self) -> None:
        prop = CronJob.model_json_schema()["properties"]["timezone"]
        assert prop["x-widget"] == "timezone"
        assert prop["default"] == "UTC"


class TestConcurrency:
    def test_defaults_to_one(self) -> None:
        assert CronJob(cron="0 6 * * *").concurrency == 1

    def test_rejects_zero(self) -> None:
        with pytest.raises(ValueError, match="greater than or equal to 1"):
            CronJob(cron="0 6 * * *", concurrency=0)

    def test_sits_in_the_operation_section(self) -> None:
        prop = CronJob.config_schema()["properties"]["concurrency"]
        assert prop["x-section"] == "Operation"
        assert prop["title"] == "Concurrency"


class TestZone:
    """A stored config's timezone resolves to its zone, anything unusable to UTC."""

    def test_a_known_name_resolves(self) -> None:
        assert CronJob.zone({"timezone": "Europe/Berlin"}) == ZoneInfo("Europe/Berlin")

    @pytest.mark.parametrize("name", [None, "", "Not/AZone"])
    def test_a_missing_or_unknown_name_falls_back_to_utc(self, name: str | None) -> None:
        assert CronJob.zone({"timezone": name}).utcoffset(dt.datetime(2026, 1, 1)) == dt.timedelta(0)


class TestWindow:
    """The partitions a firing covers, counted back from the firing in the job's zone."""

    def test_the_lookback_ends_offset_periods_before_the_local_firing(self) -> None:
        fires_at = dt.datetime(2026, 3, 9, 23, 30, tzinfo=dt.timezone.utc)
        config = {"timezone": "Europe/Berlin", "lookback": 3, "offset": 1}

        window = CronJob.window(config, fires_at=fires_at, granularity=TimeGranularity.DAY)

        assert window is not None
        assert (window.start, window.end) == (dt.date(2026, 3, 7), dt.date(2026, 3, 9))

    @pytest.mark.parametrize(("config", "granularity"), [({"lookback": 0}, TimeGranularity.DAY), ({}, None)])
    def test_no_lookback_or_an_unpartitioned_job_has_no_window(
        self, config: dict, granularity: TimeGranularity | None
    ) -> None:
        assert CronJob.window(config, fires_at=dt.datetime.now(dt.timezone.utc), granularity=granularity) is None
