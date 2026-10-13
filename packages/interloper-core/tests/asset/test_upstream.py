"""Tests for ``interloper.asset.upstream``."""

from __future__ import annotations

from typing import Any

import pytest

import interloper as il
from interloper.asset.upstream import Upstream
from interloper.events import Event, EventBus


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


class TestRecords:
    """``records`` is the one line a consumer needs to be indifferent to the upstream's destination."""

    def test_rows_come_back_as_they_are(self):
        class Leg(il.Asset):
            """Fixture asset."""

        assert Upstream(asset=Leg(), data=[{"id": 1}]).records == [{"id": 1}]

    def test_a_dataframe_comes_back_as_records(self):
        pd = pytest.importorskip("pandas")

        class Leg(il.Asset):
            """Fixture asset."""

        assert Upstream(asset=Leg(), data=pd.DataFrame([{"id": 1}, {"id": 2}])).records == [{"id": 1}, {"id": 2}]

    def test_nothing_materialized_is_no_records(self):
        class Leg(il.Asset):
            """Fixture asset."""

        assert Upstream(asset=Leg(), data=None).records == []


@pytest.fixture(autouse=True)
def _clear_memory() -> None:
    il.MemoryDestination.clear()


class TestRead:
    async def test_reads_the_upstreams_default_destination(self):
        destination = il.MemoryDestination()

        @il.asset
        def orders() -> list[dict[str, Any]]:
            return [{"id": 1}]

        @il.asset
        def revenue(orders: il.Upstream) -> list[dict[str, Any]]:
            return []

        upstream = orders(destinations=[destination])
        await upstream.materialize_async()
        leg = await Upstream.read(upstream, revenue(), None, {})
        assert leg.asset is upstream
        assert leg.records == [{"id": 1}]

    async def test_missing_data_is_none_with_a_warning(self):
        warnings_seen: list[Event] = []

        def handler(event: Event) -> None:
            if event.metadata.get("level") == "WARNING":
                warnings_seen.append(event)

        @il.asset
        def orders() -> list[dict[str, Any]]:
            return []

        @il.asset
        def revenue(orders: il.Upstream) -> list[dict[str, Any]]:
            return []

        EventBus.subscribe(handler)
        try:
            leg = await Upstream.read(orders(destinations=[il.MemoryDestination()]), revenue(), None, {})
            EventBus.flush(timeout=5.0)
        finally:
            EventBus.unsubscribe(handler)
        assert leg.data is None
        assert any("found no data in upstream" in event.metadata.get("message", "") for event in warnings_seen)


class TestReference:
    def test_the_data_is_the_destinations_reference(self):
        class Referencing(il.MemoryDestination):
            def reference(self, asset: il.Asset) -> Any:
                return f"ref:{asset.key}"

        @il.asset
        def orders() -> list[dict[str, Any]]:
            return []

        leg = Upstream.reference(orders(), Referencing())
        assert leg.data == "ref:orders"
