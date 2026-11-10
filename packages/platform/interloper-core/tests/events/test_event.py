"""Tests for :class:`Event` identity and serialization round-trips."""

from __future__ import annotations

import datetime as dt

import pytest

from interloper.errors import EventError
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
    event = Event(type=EventType.OPERATION_STARTED, metadata={"component_key": "foo"})
    data = event.to_dict()
    assert data["event_id"] == event.id
    assert data["type"] == "operation_started"
    assert data["component_key"] == "foo"


def test_json_round_trip_preserves_id_and_timestamp() -> None:
    """Serializing and parsing an event keeps its id, type, timestamp and metadata."""
    event = Event(type=EventType.OPERATION_FAILED, metadata={"component_key": "foo", "error": "boom"})

    restored = Event.from_json(event.to_json())

    assert restored.id == event.id
    assert restored.type == event.type
    assert restored.timestamp == event.timestamp
    assert restored.metadata["component_key"] == "foo"
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


def test_str_prefers_qualified_key_then_bare_key() -> None:
    """``__str__`` falls back to ``component_key`` when no qualified key is set."""
    qualified = Event(type=EventType.OPERATION_STARTED, metadata={"qualified_key": "src.foo", "component_key": "foo"})
    bare = Event(type=EventType.OPERATION_STARTED, metadata={"component_key": "foo"})
    neither = Event(type=EventType.RUN_STARTED)

    assert "src.foo" in str(qualified)
    assert "  foo  " in str(bare)
    assert "  -  " in str(neither)


def test_str_falls_back_to_error_when_no_message() -> None:
    """Failure events without a ``message`` show their ``error`` instead."""
    event = Event(type=EventType.OPERATION_FAILED, metadata={"component_key": "foo", "error": "boom"})
    assert "boom" in str(event)


def test_str_labels_log_events_with_level() -> None:
    """LOG events render as ``LOG.<LEVEL>`` instead of the bare type."""
    event = Event(type=EventType.LOG, metadata={"component_key": "foo", "message": "hi", "level": "WARNING"})
    assert "LOG.WARNING" in str(event)


class TestFromDictValidation:
    """Deserialization rejects malformed payloads with a clear error."""

    def test_a_missing_type_is_rejected(self) -> None:
        with pytest.raises(EventError, match="Missing required field 'type'"):
            Event.from_dict({"timestamp": "2026-01-01T00:00:00+00:00"})

    def test_an_unknown_type_is_rejected(self) -> None:
        with pytest.raises(EventError, match="Invalid event type 'not_a_type'"):
            Event.from_dict({"type": "not_a_type", "timestamp": "2026-01-01T00:00:00+00:00"})

    def test_a_missing_timestamp_is_rejected(self) -> None:
        with pytest.raises(EventError, match="Missing required field 'timestamp'"):
            Event.from_dict({"type": EventType.RUN_STARTED.value})

    def test_an_unparseable_timestamp_is_rejected(self) -> None:
        with pytest.raises(EventError, match="Invalid timestamp format"):
            Event.from_dict({"type": EventType.RUN_STARTED.value, "timestamp": "yesterday"})

    def test_a_non_temporal_timestamp_is_rejected(self) -> None:
        with pytest.raises(EventError, match="Invalid timestamp value"):
            Event.from_dict({"type": EventType.RUN_STARTED.value, "timestamp": 1767225600})

    def test_a_datetime_timestamp_is_taken_as_is(self) -> None:
        timestamp = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)

        event = Event.from_dict({"type": EventType.RUN_STARTED.value, "timestamp": timestamp})

        assert event.timestamp == timestamp

    def test_every_other_key_becomes_metadata(self) -> None:
        event = Event.from_dict(
            {
                "event_id": "e1",
                "type": EventType.RUN_STARTED.value,
                "timestamp": "2026-01-01T00:00:00+00:00",
                "run_id": "r1",
                "message": "hello",
            }
        )

        assert event.id == "e1"
        assert event.metadata == {"run_id": "r1", "message": "hello"}


def test_subject_and_summary_fall_back_in_order():
    """``subject`` falls back to the component key, ``summary`` to the error, both to ``"-"``."""
    event = Event(type=EventType.LOG, metadata={"component_key": "orders", "error": "boom"})

    assert (event.subject, event.summary) == ("orders", "boom")
    assert (Event(type=EventType.LOG).subject, Event(type=EventType.LOG).summary) == ("-", "-")
