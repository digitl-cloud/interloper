"""Tests for ``interloper_toolkit.stats``."""

from __future__ import annotations

import datetime

from interloper_toolkit.stats import max_concurrent, percentile, window

_T0 = datetime.datetime(2026, 9, 29, 4, 0, tzinfo=datetime.timezone.utc)


def _at(minutes: float) -> datetime.datetime:
    return _T0 + datetime.timedelta(minutes=minutes)


class TestWindow:
    def test_dates_and_datetimes_resolve_to_aware_instants(self):
        start, end = window("2026-09-01", "2026-09-02T12:00:00Z", default_days=7)

        assert start == datetime.datetime(2026, 9, 1, tzinfo=datetime.timezone.utc)
        assert end == datetime.datetime(2026, 9, 2, 12, tzinfo=datetime.timezone.utc)

    def test_defaults_open_days_ago_or_stay_open(self):
        start, end = window(None, None, default_days=1)
        assert end is None
        assert start is not None
        assert datetime.timedelta(hours=23) < datetime.datetime.now(tz=datetime.timezone.utc) - start

        assert window(None, None, default_days=None) == (None, None)


class TestPercentile:
    def test_nearest_rank(self):
        values = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]

        assert percentile(values, 50) == 50.0
        assert percentile(values, 90) == 90.0
        assert percentile(values, 100) == 100.0
        assert percentile([7.0], 90) == 7.0
        assert percentile([], 50) is None


class TestMaxConcurrent:
    def test_peak_overlap_counts_open_spans_until_now(self):
        spans = [(_at(0), _at(10)), (_at(5), _at(15)), (_at(10), _at(20)), (_at(12), None)]

        assert max_concurrent(spans) == 3
        assert max_concurrent([(_at(0), _at(10)), (_at(10), _at(20))]) == 1
        assert max_concurrent([]) == 0
