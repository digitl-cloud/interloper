"""Tests for :class:`EventLogger` event attribution."""

from __future__ import annotations

from interloper.events import Event, EventBus, EventType
from interloper.events.logger import EventLogger


def _capture(emit: object) -> list[Event]:
    captured: list[Event] = []

    def handler(event: Event) -> None:
        captured.append(event)

    EventBus.subscribe(handler)
    try:
        emit()  # ty: ignore[call-non-callable]
        EventBus.flush(timeout=5.0)
    finally:
        EventBus.unsubscribe(handler)
    return [e for e in captured if e.type == EventType.LOG]


def test_log_event_carries_asset_and_source_id() -> None:
    """``LOG`` events inherit the asset/source identity so they can be filtered by asset."""
    logger = EventLogger(
        "demo.a",
        {"run_id": "run-1"},
        asset_id="asset-1",
        source_id="source-1",
    )

    events = _capture(lambda: logger.info("hello"))

    assert len(events) == 1
    meta = events[0].metadata
    assert meta["asset_id"] == "asset-1"
    assert meta["source_id"] == "source-1"
    assert meta["asset_key"] == "demo.a"
    assert meta["message"] == "hello"
    assert meta["run_id"] == "run-1"


def test_log_event_omits_ids_when_unset() -> None:
    """Without an asset/source id, no null keys leak into the metadata."""
    logger = EventLogger("demo.a", {"run_id": "run-1"})

    events = _capture(lambda: logger.warning("careful"))

    assert len(events) == 1
    meta = events[0].metadata
    assert "asset_id" not in meta
    assert "source_id" not in meta
