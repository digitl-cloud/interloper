"""Tests for ``interloper.asset.upstream``."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, ClassVar

import pytest

import interloper as il
from interloper.asset.upstream import Upstream
from interloper.errors import DataNotFoundError
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
    """``rows`` is the one line a consumer needs to be indifferent to the upstream's destination."""

    def test_rows_come_back_as_they_are(self):
        class Leg(il.Asset):
            """Fixture asset."""

        assert Upstream(asset=Leg(), data=[{"id": 1}]).rows == [{"id": 1}]

    def test_another_table_type_comes_back_as_rows(self, columnar):
        class Leg(il.Asset):
            """Fixture asset."""

        assert Upstream(asset=Leg(), data=columnar({"id": [1, 2]})).rows == [{"id": 1}, {"id": 2}]

    def test_nothing_materialized_is_no_rows(self):
        class Leg(il.Asset):
            """Fixture asset."""

        assert Upstream(asset=Leg(), data=None).rows == []


@pytest.fixture(autouse=True)
def _clear_memory() -> None:
    il.MemoryDestination.clear()


@contextmanager
def _warnings() -> Generator[list[str], None, None]:
    """Collect the messages of the WARNING events emitted inside the block.

    Yields:
        The messages, filled once the block exits.
    """
    seen: list[str] = []

    def handler(event: Event) -> None:
        if event.metadata.get("level") == "WARNING":
            seen.append(event.metadata.get("message", ""))

    EventBus.subscribe(handler)
    try:
        yield seen
        EventBus.flush(timeout=5.0)
    finally:
        EventBus.unsubscribe(handler)


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
        leg = await Upstream.read(upstream, revenue(), None, {}, parameter="orders")
        assert leg.asset is upstream
        assert leg.rows == [{"id": 1}]

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
            leg = await Upstream.read(
                orders(destinations=[il.MemoryDestination()]), revenue(), None, {}, parameter="orders"
            )
            EventBus.flush(timeout=5.0)
        finally:
            EventBus.unsubscribe(handler)
        assert leg.data is None
        assert any("found no data in upstream" in event.metadata.get("message", "") for event in warnings_seen)

    async def test_read_of_an_upstream_without_destination_is_passed_as_none_with_a_warning(self):
        @il.asset
        def orders() -> list[dict[str, Any]]:
            return []

        @il.asset
        def revenue(orders: il.Upstream) -> list[dict[str, Any]]:
            return []

        with _warnings() as seen:
            leg = await Upstream.read(orders(), revenue(), None, {}, parameter="orders")
        assert leg.data is None
        assert any("found no data in upstream 'orders'" in message for message in seen)


class TestReference:
    async def test_the_data_is_the_readers_destinations_reference(self):
        class Referencing(il.MemoryDestination):
            def reference(self, asset: il.Asset) -> Any:
                return f"ref:{asset.key}"

        @il.asset
        def orders() -> list[dict[str, Any]]:
            return []

        @il.asset
        def revenue(orders: il.Upstream) -> list[dict[str, Any]]:
            return []

        reader = revenue(destinations=[Referencing()])
        leg = await Upstream.reference(orders(), reader, None, {}, parameter="orders")
        assert leg.data == "ref:orders"

    async def test_reference_to_a_missing_table_is_passed_as_none_with_a_warning(self):
        class Tableless(il.MemoryDestination):
            def reference(self, asset: il.Asset) -> Any:
                raise DataNotFoundError(f"'{asset.key}' has no table")

        @il.asset
        def orders() -> list[dict[str, Any]]:
            return []

        @il.asset
        def revenue(orders: il.Upstream) -> list[dict[str, Any]]:
            return []

        with _warnings() as seen:
            leg = await Upstream.reference(orders(), revenue(destinations=[Tableless()]), None, {}, parameter="orders")
        assert leg.data is None
        assert any("'orders'" in message and "parameter 'orders'" in message for message in seen)

    async def test_the_reference_warning_says_why_and_that_the_upstream_is_left_out(self):
        class Unreachable(il.MemoryDestination):
            def reference(self, asset: il.Asset) -> Any:
                raise DataNotFoundError("none of its destinations is reachable")

        @il.asset
        def orders() -> list[dict[str, Any]]:
            return []

        @il.asset
        def revenue(orders: il.Upstream) -> list[dict[str, Any]]:
            return []

        with _warnings() as seen:
            await Upstream.reference(orders(), revenue(destinations=[Unreachable()]), None, {}, parameter="orders")
        assert any("none of its destinations is reachable" in message and "left out" in message for message in seen)


class _Rows(il.Asset):
    """An asset handing rows over."""

    def data(self) -> list[dict[str, Any]]:
        """Return one row.

        Returns:
            The rows.
        """
        return [{"a": 1}]


class _Shop(il.Source):
    """A source owning one asset."""

    asset_types: ClassVar[list[type[il.Asset]]] = [_Rows]


def test_rows_reads_the_upstream_data_as_rows():
    """``Upstream.rows`` reads the upstream's data as rows; ``records`` is gone."""
    upstream = Upstream(asset=_Rows(), data=[{"a": 1}])

    assert upstream.rows == [{"a": 1}]
    assert not hasattr(upstream, "records")


def test_a_spec_round_trips_through_the_rename():
    """A source spec round-trips unchanged: the rename touches no persisted shape."""
    source = _Shop()

    assert il.Source.from_spec(source.to_spec()).to_spec() == source.to_spec()


async def test_the_missing_data_warning_names_the_parameter():
    """The warning for an upstream with no data names the parameter it was bound to."""
    il.MemoryDestination.clear()
    upstream = _Rows(id="orders", destinations=[il.MemoryDestination()])
    reader = _Rows(id="reader", destinations=[il.MemoryDestination()])
    captured: list[Event] = []
    EventBus.subscribe(captured.append)
    try:
        result = await Upstream.read(upstream, reader, None, {}, parameter="orders")
        EventBus.flush(timeout=5.0)
    finally:
        EventBus.unsubscribe(captured.append)

    assert result.data is None
    assert any("for parameter 'orders'" in (event.metadata.get("message") or "") for event in captured)
