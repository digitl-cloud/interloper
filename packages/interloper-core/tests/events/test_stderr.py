"""Tests for ``interloper.events.stderr``."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from interloper.events import Event, EventBus, EventLogReader, EventType
from interloper.events.stderr import EVENT_LINE_PREFIX


@contextmanager
def _capture() -> Iterator[list[Event]]:
    captured: list[Event] = []
    EventBus.flush(timeout=5.0)
    EventBus.subscribe(captured.append)
    try:
        yield captured
        EventBus.flush(timeout=5.0)
    finally:
        EventBus.unsubscribe(captured.append)


def _line(event_type: EventType, **metadata: Any) -> bytes:
    return (EVENT_LINE_PREFIX + Event(type=event_type, metadata=metadata).to_json() + "\n").encode()


class TestEventLogReader:
    def test_forwards_the_operations_events_across_split_chunks(self):
        line = _line(EventType.ASSET_DATA_STARTED, component_id="asset-1")
        with _capture() as captured:
            reader = EventLogReader("asset-1", label="container x")
            reader.feed(line[:10])
            reader.feed(line[10:])
            reader.close()
        assert [event.type for event in captured] == [EventType.ASSET_DATA_STARTED]

    def test_keeps_the_events_identity(self):
        event = Event(type=EventType.ASSET_DATA_STARTED, metadata={"component_id": "asset-1"})
        with _capture() as captured:
            reader = EventLogReader("asset-1", label="container x")
            reader.feed((EVENT_LINE_PREFIX + event.to_json() + "\n").encode())
        assert [forwarded.id for forwarded in captured] == [event.id]

    def test_drops_run_events_other_operations_and_plain_output(self):
        with _capture() as captured:
            reader = EventLogReader("asset-1", label="container x")
            reader.feed(_line(EventType.RUN_STARTED, component_id="asset-1"))
            reader.feed(_line(EventType.ASSET_DATA_STARTED, component_id="upstream"))
            reader.feed(b"plain log output\n")
            reader.close()
        assert captured == []

    def test_tracks_the_last_attempt_the_operation_reached(self):
        with _capture():
            reader = EventLogReader("asset-1", label="container x")
            assert reader.attempt == 1
            reader.feed(_line(EventType.OPERATION_STARTED, component_id="asset-1", attempt=1))
            reader.feed(_line(EventType.OPERATION_RETRIED, component_id="asset-1", attempt=1))
            reader.feed(_line(EventType.OPERATION_STARTED, component_id="asset-1", attempt=2))
            reader.feed(_line(EventType.OPERATION_STARTED, component_id="upstream", attempt=5))
            reader.close()
        assert reader.attempt == 2

    def test_a_retry_reaches_the_next_attempt_before_it_starts(self):
        with _capture():
            reader = EventLogReader("asset-1", label="container x")
            reader.feed(_line(EventType.OPERATION_RETRIED, component_id="asset-1", attempt=1))
            reader.close()
        assert reader.attempt == 2

    def test_a_stopped_reader_forwards_nothing_more(self):
        with _capture() as captured:
            reader = EventLogReader("asset-1", label="pod x")
            reader.stop()
            reader.feed(_line(EventType.OPERATION_STARTED, component_id="asset-1", attempt=2))
            reader.close()
        assert captured == []
        assert reader.attempt == 1

    def test_close_reads_a_trailing_line_without_newline(self):
        with _capture() as captured:
            reader = EventLogReader("asset-1", label="container x")
            reader.feed(_line(EventType.ASSET_DATA_COMPLETED, component_id="asset-1").rstrip(b"\n"))
            reader.close()
        assert [event.type for event in captured] == [EventType.ASSET_DATA_COMPLETED]
