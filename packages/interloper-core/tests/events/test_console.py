"""Tests for :class:`ConsoleEventHandler`."""

from __future__ import annotations

import json
import logging

import pytest

from interloper.events import ConsoleEventHandler, Event, EventType
from interloper.events.console import EVENT_LOGGER_NAME


def test_events_become_log_records(caplog: pytest.LogCaptureFixture) -> None:
    """Each event is emitted as a record on the ``interloper.run`` logger."""
    handler = ConsoleEventHandler()
    with caplog.at_level(logging.DEBUG, logger=EVENT_LOGGER_NAME):
        handler(Event(type=EventType.OPERATION_COMPLETED, metadata={"component_key": "foo", "message": "done"}))

    record = caplog.records[-1]
    assert record.name == EVENT_LOGGER_NAME
    assert record.levelno == logging.INFO
    assert "OPERATION_COMPLETED" in record.message
    assert "foo" in record.message


def test_event_levels_map_to_logging_levels() -> None:
    """Failures are errors, cancellations warnings, lifecycle info, chatter debug."""
    assert ConsoleEventHandler._level(Event(type=EventType.OPERATION_FAILED)) == logging.ERROR
    assert ConsoleEventHandler._level(Event(type=EventType.RUN_FAILED)) == logging.ERROR
    assert ConsoleEventHandler._level(Event(type=EventType.OPERATION_CANCELED)) == logging.WARNING
    assert ConsoleEventHandler._level(Event(type=EventType.OPERATION_STARTED)) == logging.INFO
    assert ConsoleEventHandler._level(Event(type=EventType.OPERATION_QUEUED)) == logging.DEBUG
    assert ConsoleEventHandler._level(Event(type=EventType.DEST_WRITE_STARTED)) == logging.DEBUG


def test_log_events_carry_their_authored_level(caplog: pytest.LogCaptureFixture) -> None:
    """``context.logger`` levels travel onto the record; message reads ``asset: message``."""
    handler = ConsoleEventHandler()
    with caplog.at_level(logging.DEBUG, logger=EVENT_LOGGER_NAME):
        handler(Event(type=EventType.LOG, metadata={"component_key": "foo", "message": "hi", "level": "WARNING"}))

    record = caplog.records[-1]
    assert record.levelno == logging.WARNING
    assert record.message == "foo: hi"


def test_log_event_with_unknown_level_defaults_to_info() -> None:
    """An unparsable level string falls back to INFO instead of crashing."""
    event = Event(type=EventType.LOG, metadata={"level": "NOT_A_LEVEL"})
    assert ConsoleEventHandler._level(event) == logging.INFO


def test_respects_logger_level(caplog: pytest.LogCaptureFixture) -> None:
    """DEBUG chatter is dropped when the logger sits at INFO."""
    handler = ConsoleEventHandler()
    with caplog.at_level(logging.INFO, logger=EVENT_LOGGER_NAME):
        handler(Event(type=EventType.DEST_WRITE_STARTED, metadata={"component_key": "foo"}))
        handler(Event(type=EventType.OPERATION_QUEUED, metadata={"component_key": "foo"}))

    assert not caplog.records


def test_record_timestamp_comes_from_the_event(caplog: pytest.LogCaptureFixture) -> None:
    """The record is stamped with the event's own time, not delivery time."""
    handler = ConsoleEventHandler()
    event = Event(type=EventType.OPERATION_STARTED, metadata={"component_key": "foo"})
    with caplog.at_level(logging.INFO, logger=EVENT_LOGGER_NAME):
        handler(event)

    assert caplog.records[-1].created == pytest.approx(event.timestamp.timestamp())


def test_record_carries_the_trace_of_the_run_that_emitted_the_event(caplog: pytest.LogCaptureFixture) -> None:
    """The bus worker has no current span, so the record takes the run's from the event."""
    traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    event = Event(type=EventType.OPERATION_STARTED, metadata={"component_key": "foo", "traceparent": traceparent})
    with caplog.at_level(logging.INFO, logger=EVENT_LOGGER_NAME):
        ConsoleEventHandler()(event)

    record = caplog.records[-1]
    assert (vars(record)["trace_id"], vars(record)["span_id"]) == (
        "4bf92f3577b34da6a3ce929d0e0e4736",
        "00f067aa0ba902b7",
    )


def test_record_without_a_run_trace_has_no_ids(caplog: pytest.LogCaptureFixture) -> None:
    """An unparsable traceparent leaves the record without ids rather than failing."""
    event = Event(type=EventType.OPERATION_STARTED, metadata={"component_key": "foo", "traceparent": "garbage"})
    with caplog.at_level(logging.INFO, logger=EVENT_LOGGER_NAME):
        ConsoleEventHandler()(event)

    assert not hasattr(caplog.records[-1], "trace_id")


def test_json_lines_mode_streams_to_stdout(
    caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    """JSON mode bypasses logging and writes one ``Event.to_json()`` document per line."""
    handler = ConsoleEventHandler(json_lines=True)
    event = Event(type=EventType.LOG, metadata={"component_key": "foo", "message": "hi", "level": "INFO"})
    with caplog.at_level(logging.DEBUG, logger=EVENT_LOGGER_NAME):
        handler(event)

    captured = capsys.readouterr()
    assert not caplog.records
    assert captured.err == ""
    data = json.loads(captured.out)
    assert data["event_id"] == event.id
    assert data["type"] == "log"
    assert data["message"] == "hi"
