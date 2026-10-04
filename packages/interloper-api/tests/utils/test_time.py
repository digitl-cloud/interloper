"""Tests for ``interloper_api.utils.time``."""

from __future__ import annotations

import datetime as dt

import pytest

from interloper_api.utils import format_duration


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
