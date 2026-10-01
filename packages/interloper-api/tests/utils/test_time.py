"""Tests for ``interloper_api.utils.time``."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from interloper_api.utils import format_duration, job_zone


class TestJobZone:
    """A job's timezone name resolves to its zone, anything unusable to UTC."""

    def test_a_known_name_resolves(self):
        assert job_zone("Europe/Berlin") == ZoneInfo("Europe/Berlin")

    @pytest.mark.parametrize("name", [None, "", "Not/AZone"])
    def test_a_missing_or_unknown_name_falls_back_to_utc(self, name: str | None):
        assert job_zone(name).utcoffset(dt.datetime(2026, 1, 1)) == dt.timedelta(0)


class TestFormatDuration:
    """A duration reads as its two largest units."""

    @pytest.mark.parametrize(
        ("delta", "label"),
        [
            (dt.timedelta(minutes=12, seconds=59), "12m"),
            (dt.timedelta(hours=5, minutes=12), "5h 12m"),
            (dt.timedelta(days=2, hours=3, minutes=40), "2d 3h"),
        ],
    )
    def test_it_keeps_the_two_largest_units(self, delta: dt.timedelta, label: str):
        assert format_duration(delta) == label
