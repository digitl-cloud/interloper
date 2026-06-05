"""Tests for :class:`Event` identity and serialization round-trips."""

from __future__ import annotations

from interloper.events import Event, EventBus, EventType


def test_events_get_unique_ids_by_default() -> None:
    """Each freshly constructed event gets its own id."""
    e1 = Event(type=EventType.LOG)
    e2 = Event(type=EventType.LOG)
    assert e1.id
    assert e2.id
    assert e1.id != e2.id


def test_to_dict_includes_event_id() -> None:
    """``event_id`` is serialized as a top-level key alongside type/timestamp."""
    event = Event(type=EventType.ASSET_STARTED, metadata={"asset_key": "foo"})
    data = event.to_dict()
    assert data["event_id"] == event.id
    assert data["type"] == "asset_started"
    assert data["asset_key"] == "foo"


def test_json_round_trip_preserves_id_and_timestamp() -> None:
    """Serializing and parsing an event keeps its id, type, timestamp and metadata."""
    event = Event(type=EventType.ASSET_FAILED, metadata={"asset_key": "foo", "error": "boom"})

    restored = Event.from_json(event.to_json())

    assert restored.id == event.id
    assert restored.type == event.type
    assert restored.timestamp == event.timestamp
    assert restored.metadata["asset_key"] == "foo"
    assert restored.metadata["error"] == "boom"
    # event_id is a top-level field, not metadata.
    assert "event_id" not in restored.metadata


def test_from_dict_without_event_id_generates_one() -> None:
    """A legacy payload lacking ``event_id`` still yields an event with an id."""
    event = Event.from_dict({"type": "log", "timestamp": "2026-06-04T12:00:00+00:00"})
    assert event.id


def test_emit_event_preserves_identity() -> None:
    """``emit_event`` delivers the event unchanged, keeping its id and timestamp."""
    captured: list[Event] = []

    def handler(event: Event) -> None:
        captured.append(event)

    EventBus.subscribe(handler)
    try:
        original = Event(type=EventType.LOG, metadata={"message": "hi"})
        EventBus.emit_event(original)
        EventBus.flush(timeout=5.0)
    finally:
        EventBus.unsubscribe(handler)

    match = [e for e in captured if e.id == original.id]
    assert match, "emit_event should deliver an event preserving its id"
    assert match[0].timestamp == original.timestamp
