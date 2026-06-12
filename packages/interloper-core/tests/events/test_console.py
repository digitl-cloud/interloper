"""Tests for :class:`ConsoleEventHandler`."""

from __future__ import annotations

import json
import logging

import pytest

from interloper.events import ConsoleEventHandler, Event, EventType
from interloper.events.console import EVENT_LOGGER_NAME, event_level


def test_events_become_log_records(caplog: pytest.LogCaptureFixture) -> None:
    """Each event is emitted as a record on the ``interloper.run`` logger."""
    handler = ConsoleEventHandler()
    with caplog.at_level(logging.DEBUG, logger=EVENT_LOGGER_NAME):
        handler(Event(type=EventType.ASSET_COMPLETED, metadata={"asset_key": "foo", "message": "done"}))

    record = caplog.records[-1]
    assert record.name == EVENT_LOGGER_NAME
    assert record.levelno == logging.INFO
    assert "ASSET_COMPLETED" in record.message
    assert "foo" in record.message


def test_event_levels_map_to_logging_levels() -> None:
    """Failures are errors, cancellations warnings, lifecycle info, chatter debug."""
    assert event_level(Event(type=EventType.ASSET_FAILED)) == logging.ERROR
    assert event_level(Event(type=EventType.RUN_FAILED)) == logging.ERROR
    assert event_level(Event(type=EventType.ASSET_CANCELED)) == logging.WARNING
    assert event_level(Event(type=EventType.ASSET_STARTED)) == logging.INFO
    assert event_level(Event(type=EventType.ASSET_QUEUED)) == logging.DEBUG
    assert event_level(Event(type=EventType.DEST_WRITE_STARTED)) == logging.DEBUG


def test_log_events_carry_their_authored_level(caplog: pytest.LogCaptureFixture) -> None:
    """``context.logger`` levels travel onto the record; message reads ``asset: message``."""
    handler = ConsoleEventHandler()
    with caplog.at_level(logging.DEBUG, logger=EVENT_LOGGER_NAME):
        handler(Event(type=EventType.LOG, metadata={"asset_key": "foo", "message": "hi", "level": "WARNING"}))

    record = caplog.records[-1]
    assert record.levelno == logging.WARNING
    assert record.message == "foo: hi"


def test_log_event_with_unknown_level_defaults_to_info() -> None:
    """An unparsable level string falls back to INFO instead of crashing."""
    event = Event(type=EventType.LOG, metadata={"level": "NOT_A_LEVEL"})
    assert event_level(event) == logging.INFO


def test_respects_logger_level(caplog: pytest.LogCaptureFixture) -> None:
    """DEBUG chatter is dropped when the logger sits at INFO."""
    handler = ConsoleEventHandler()
    with caplog.at_level(logging.INFO, logger=EVENT_LOGGER_NAME):
        handler(Event(type=EventType.DEST_WRITE_STARTED, metadata={"asset_key": "foo"}))
        handler(Event(type=EventType.ASSET_QUEUED, metadata={"asset_key": "foo"}))

    assert not caplog.records


def test_record_timestamp_comes_from_the_event(caplog: pytest.LogCaptureFixture) -> None:
    """The record is stamped with the event's own time, not delivery time."""
    handler = ConsoleEventHandler()
    event = Event(type=EventType.ASSET_STARTED, metadata={"asset_key": "foo"})
    with caplog.at_level(logging.INFO, logger=EVENT_LOGGER_NAME):
        handler(event)

    assert caplog.records[-1].created == pytest.approx(event.timestamp.timestamp())


def test_json_lines_mode_streams_to_stdout(
    caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    """JSON mode bypasses logging and writes one ``Event.to_json()`` document per line."""
    handler = ConsoleEventHandler(json_lines=True)
    event = Event(type=EventType.LOG, metadata={"asset_key": "foo", "message": "hi", "level": "INFO"})
    with caplog.at_level(logging.DEBUG, logger=EVENT_LOGGER_NAME):
        handler(event)

    captured = capsys.readouterr()
    assert not caplog.records
    assert captured.err == ""
    data = json.loads(captured.out)
    assert data["event_id"] == event.id
    assert data["type"] == "log"
    assert data["message"] == "hi"
