"""Tests for ``interloper_assets.demo.assets``."""

from __future__ import annotations

import datetime as dt

import interloper as il

from interloper_assets.demo.assets import demo_asset


def test_returns_one_greeting_stamped_with_the_partition_day():
    asset = demo_asset()
    day = dt.date(2026, 11, 20)

    frame = asset.data(context=il.ExecutionContext(asset, il.TimePartition(value=day).window))

    assert frame.to_dict("records") == [{"date": day, "hello": "world"}]
