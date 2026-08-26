"""Tests for ``interloper.job.cron`` — the CronJob config model."""

from __future__ import annotations

import pytest

from interloper.job.cron import CronJob


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
