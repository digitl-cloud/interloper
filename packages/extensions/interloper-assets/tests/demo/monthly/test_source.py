"""Tests for ``interloper_assets.demo.monthly.source``."""

from __future__ import annotations

import datetime as dt

import interloper as il

from interloper_assets.demo.monthly.source import DemoMonthlySource


def test_one_row_per_month_stamped_with_its_first_day():
    asset = next(iter(DemoMonthlySource().assets))
    month = il.TimePartition(value=dt.date(2026, 11, 1), granularity=il.TimeGranularity.MONTH)

    frame = asset.data(context=il.ExecutionContext(asset, month.window))

    assert frame.to_dict("records") == [{"date": dt.date(2026, 11, 1), "hello": "monthly"}]
