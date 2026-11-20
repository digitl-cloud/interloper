"""Tests for ``interloper_assets.demo.basic.source``."""

from __future__ import annotations

import datetime as dt
from typing import Any

import interloper as il
import pytest

from interloper_assets.demo.basic.source import DemoSource

DAY = dt.date(2026, 11, 20)


def _run_root(source: DemoSource) -> Any:
    """Run the source's root asset ``a`` for one day.

    Args:
        source: The demo source.

    Returns:
        What the asset returned.
    """
    asset = next(asset for asset in source.assets if asset.key == "a")
    return asset.data(context=il.ExecutionContext(asset, il.TimePartition(value=DAY).window))


@pytest.fixture(autouse=True)
def _no_pause(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the pause each demo asset takes to look like real work last no time.

    Args:
        monkeypatch: Pytest's monkeypatch fixture.
    """
    monkeypatch.setattr("random.uniform", lambda low, high: 0.0)


def test_an_asset_returns_the_configured_greeting_for_its_day():
    frame = _run_root(DemoSource(hello="there"))

    assert frame.to_dict("records") == [{"date": DAY, "hello": "there"}]


def test_a_failure_probability_of_one_always_fails():
    with pytest.raises(RuntimeError, match="Random failure"):
        _run_root(DemoSource(random_failure_probability=1.0))
