"""Tests for ``interloper.cli.console``."""

from __future__ import annotations

import logging
from collections.abc import Iterator

import pytest

from interloper.cli.console import configure_console_logging


class _Recorder(logging.Handler):
    """Stands in for the OTLP handler telemetry puts on the root logger."""

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
    def test_records_reach_stderr_beside_a_handler_telemetry_attached_first(
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
