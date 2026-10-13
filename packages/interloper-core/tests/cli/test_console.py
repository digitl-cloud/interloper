"""Tests for ``interloper.cli.console``."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from typing import Any

import pytest

from interloper.cli.console import configure_console_logging


class _Recorder(logging.Handler):
    """Stands in for a handler someone else attached to the root logger."""

    def __init__(self) -> None:
        """Start with no records."""
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        """Keep the record.

        Args:
            record: The record the root logger passed on.
        """
        self.records.append(record)


@pytest.fixture(autouse=True)
def restore_root_logger() -> Iterator[None]:
    """Put the root logger's handlers and level back after each test.

    Yields:
        Nothing; the restore runs on teardown.
    """
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


def _console_handlers() -> list[logging.Handler]:
    return [handler for handler in logging.getLogger().handlers if type(handler) is logging.StreamHandler]


class TestConfigureConsoleLogging:
    def test_records_reach_stderr_beside_a_handler_attached_first(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        exporter = _Recorder()
        logging.getLogger().addHandler(exporter)

        configure_console_logging(logging.INFO, "%(levelname)s %(message)s")
        logging.getLogger("interloper.test").info("scheduler ticked")

        assert "INFO scheduler ticked" in capsys.readouterr().err
        assert [record.getMessage() for record in exporter.records] == ["scheduler ticked"]

    def test_a_second_call_replaces_the_console_handler_rather_than_adding_one(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure_console_logging(logging.INFO, "first %(message)s")
        configure_console_logging(logging.INFO, "second %(message)s")
        logging.getLogger("interloper.test").info("once")

        assert len(_console_handlers()) == 1
        assert capsys.readouterr().err == "second once\n"

    def test_the_level_gates_what_is_printed(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_console_logging(logging.WARNING, "%(message)s")
        logging.getLogger("interloper.test").info("hidden")
        logging.getLogger("interloper.test").warning("shown")

        assert capsys.readouterr().err == "shown\n"

    def test_the_date_format_is_applied(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_console_logging(logging.INFO, "%(asctime)s|%(message)s", "%Y")
        logging.getLogger("interloper.test").info("dated")

        stamp, message = capsys.readouterr().err.strip().split("|")
        assert len(stamp) == 4 and stamp.isdigit()
        assert message == "dated"


class TestJsonFormat:
    def test_a_record_is_one_json_object_per_line(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_console_logging(logging.INFO, "%(message)s", log_format="json")
        logging.getLogger("interloper.test").warning("disk %s", "full")

        (line,) = capsys.readouterr().err.splitlines()
        entry = json.loads(line)
        assert entry["severity"] == "WARNING"
        assert entry["logger"] == "interloper.test"
        assert entry["message"] == "disk full"
        assert entry["timestamp"].endswith("+00:00")
        assert "trace_id" not in entry and "span_id" not in entry

    def test_a_record_inside_a_span_carries_its_ids(
        self, capsys: pytest.CaptureFixture[str], span_exporter: Any
    ) -> None:
        from opentelemetry import trace

        configure_console_logging(logging.INFO, "%(message)s", log_format="json")
        with trace.get_tracer("test").start_as_current_span("work") as span:
            logging.getLogger("interloper.test").info("inside")
        context = span.get_span_context()

        entry = json.loads(capsys.readouterr().err)
        assert entry["trace_id"] == trace.format_trace_id(context.trace_id)
        assert entry["span_id"] == trace.format_span_id(context.span_id)

    def test_ids_already_on_the_record_win_over_the_current_span(
        self, capsys: pytest.CaptureFixture[str], span_exporter: Any
    ) -> None:
        from opentelemetry import trace

        configure_console_logging(logging.INFO, "%(message)s", log_format="json")
        with trace.get_tracer("test").start_as_current_span("unrelated"):
            logging.getLogger("interloper.test").info(
                "from the bus", extra={"trace_id": "4bf92f3577b34da6a3ce929d0e0e4736", "span_id": "00f067aa0ba902b7"}
            )

        entry = json.loads(capsys.readouterr().err)
        assert (entry["trace_id"], entry["span_id"]) == ("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7")

    def test_an_exception_is_kept_in_one_line(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_console_logging(logging.INFO, "%(message)s", log_format="json")
        try:
            raise ValueError("boom")
        except ValueError:
            logging.getLogger("interloper.test").exception("failed")

        (line,) = capsys.readouterr().err.splitlines()
        entry = json.loads(line)
        assert entry["message"] == "failed"
        assert "ValueError: boom" in entry["exception"]
