"""Tests for ``interloper.asset.upstream``."""

from __future__ import annotations

from typing import Any

import interloper as il
from interloper.asset.upstream import Upstream


def test_upstream_carries_asset_and_data():
    """``Upstream`` pairs the leg's asset with the data read for it, and is exported as ``il.Upstream``."""

    class Leg(il.Asset):
        """Fixture asset."""

    leg = Leg()
    upstream = Upstream(asset=leg, data=[{"id": 1}])
    assert upstream.asset is leg
    assert upstream.data == [{"id": 1}]
    assert il.Upstream is Upstream


def test_upstream_data_may_be_none():
    """``data`` is ``None`` for a bound leg with no data for the partition."""

    class Leg(il.Asset):
        """Fixture asset."""

    missing: Any = Upstream(asset=Leg(), data=None)
    assert missing.data is None
