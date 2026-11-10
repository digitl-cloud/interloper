"""Tests for ``interloper.events.logger``."""

from __future__ import annotations

import datetime as dt

import interloper as il
from interloper.events import Event, EventBus, EventType
from interloper.events.logger import EventLogger
from interloper.partitioning import TimePartition


@il.asset(partitioning=il.TimePartitionConfig(column="date"))
def orders(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


def _capture(emit: object) -> list[Event]:
    captured: list[Event] = []
    EventBus.subscribe(captured.append)
    try:
        emit()  # ty: ignore[call-non-callable]
        EventBus.flush(timeout=5.0)
    finally:
        EventBus.unsubscribe(captured.append)
    return [event for event in captured if event.type is EventType.LOG]


def test_log_events_carry_the_component_identity_the_run_and_the_window() -> None:
    """Every log line is built through the component, so it has the shape of every other event."""
    asset = orders(id="a1")
    logger = EventLogger(asset, {"run_id": "r1"}, TimePartition(dt.date(2026, 1, 1)).window)

    [event] = _capture(lambda: logger.info("hello"))

    assert event.metadata == {
        "run_id": "r1",
        "component_id": "a1",
        "component_kind": "asset",
        "component_key": "orders",
        "qualified_key": "orders",
        "window": "2026-01-01",
        "level": "INFO",
        "message": "hello",
    }


def test_each_level_is_its_name() -> None:
    """The level travels as the standard logging level name."""
    logger = EventLogger(orders(), {})
    events = _capture(lambda: (logger.debug("d"), logger.warning("w"), logger.error("e")))
    levels = [event.metadata["level"] for event in events]
    assert levels == ["DEBUG", "WARNING", "ERROR"]
