"""Tests for ``interloper.cli.console``."""

from __future__ import annotations

import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from interloper.cli.console import ConsoleFormatter, configure_console_logging


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


def _console_handlers() -> list[logging.Handler]:
    return [handler for handler in logging.getLogger().handlers if type(handler) is logging.StreamHandler]


def _record(name: str, level: int, message: str, *, exc_info: Any = None) -> logging.LogRecord:
    record = logging.LogRecord(name, level, __file__, 1, message, None, exc_info)
    record.created = time.mktime((2026, 11, 24, 19, 53, 44, 0, 0, -1))
    return record


class TestConsoleFormatter:
    def test_a_record_reads_time_level_source_message(self) -> None:
        line = ConsoleFormatter().format(_record("interloper_db.provision", logging.INFO, "Database up to date"))

        assert line == "19:53:44  INFO   db         Database up to date"

    @pytest.mark.parametrize(
        ("name", "source"),
        [
            ("interloper_api.routes.providers", "api"),
            ("interloper.run", "run"),
            ("interloper.utils.supervisor", "utils"),
            ("alembic.runtime.migration", "alembic"),
            ("acme_interloper.source", "acme_interloper"),
            ("interloper", "interloper"),
            ("interloper_db", "db"),
            ("__main__", "__main__"),
            ("root", "root"),
        ],
    )
    def test_the_source_is_the_package_without_the_interloper_prefix(self, name: str, source: str) -> None:
        assert ConsoleFormatter.source(name) == source

    def test_levels_print_in_five_letters(self) -> None:
        formatter = ConsoleFormatter()

        assert formatter.format(_record("nuxt", logging.WARNING, "w")).split()[1] == "WARN"
        assert formatter.format(_record("nuxt", logging.CRITICAL, "c")).split()[1] == "FATAL"
        assert formatter.format(_record("nuxt", logging.DEBUG, "d")).split()[1] == "DEBUG"

    def test_a_long_source_pushes_the_message_rather_than_being_cut(self) -> None:
        line = ConsoleFormatter().format(_record("acme_interloper.source", logging.INFO, "hello"))

        assert line.endswith("  acme_interloper  hello")

    def test_continuation_lines_start_at_the_message_column(self) -> None:
        line = ConsoleFormatter().format(_record("interloper_app.cli", logging.INFO, "Ready\napi, cron"))

        assert line.splitlines() == ["19:53:44  INFO   app        Ready", " " * 28 + "api, cron"]

    def test_a_traceback_is_indented_under_the_message(self) -> None:
        try:
            raise ValueError("boom")
        except ValueError:
            record = _record("interloper_api.app", logging.ERROR, "failed", exc_info=sys.exc_info())

        first, *rest = ConsoleFormatter().format(record).splitlines()

        assert first == "19:53:44  ERROR  api        failed"
        assert rest[0] == " " * 28 + "Traceback (most recent call last):"
        assert rest[-1] == " " * 28 + "ValueError: boom"
        assert all(line.startswith(" " * 28) for line in rest)

    def test_colour_wraps_only_the_level(self) -> None:
        plain = ConsoleFormatter().format(_record("nuxt", logging.WARNING, "w"))
        coloured = ConsoleFormatter(color=True).format(_record("nuxt", logging.WARNING, "w"))

        assert "\x1b" not in plain
        assert coloured.startswith("19:53:44  \x1b[")
        assert re.sub(r"\x1b\[[0-9;]*m", "", coloured) == plain


class TestConfigureConsoleLogging:
    def test_records_reach_stderr_beside_a_handler_attached_first(self, capsys: pytest.CaptureFixture[str]) -> None:
        exporter = _Recorder()
        logging.getLogger().addHandler(exporter)

        configure_console_logging()
        logging.getLogger("interloper.test").info("scheduler ticked")

        assert capsys.readouterr().err.endswith("  INFO   test       scheduler ticked\n")
        assert [record.getMessage() for record in exporter.records] == ["scheduler ticked"]

    def test_a_second_call_replaces_the_console_handler_rather_than_adding_one(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure_console_logging()
        configure_console_logging()
        logging.getLogger("interloper.test").info("once")

        assert len(_console_handlers()) == 1
        assert capsys.readouterr().err.count("once") == 1

    def test_own_packages_print_info_and_libraries_only_warnings(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_console_logging()
        logging.getLogger("interloper_api.routes").info("ours")
        logging.getLogger("snowflake.connector").info("theirs")
        logging.getLogger("snowflake.connector").warning("their warning")

        printed = capsys.readouterr().err
        assert "ours" in printed and "their warning" in printed
        assert "theirs" not in printed

    def test_uvicorn_follows_the_library_level(self) -> None:
        configure_console_logging()

        assert logging.getLogger("uvicorn.error").getEffectiveLevel() == logging.WARNING

    def test_a_project_s_own_modules_print_info_like_ours(self, tmp_path: Path) -> None:
        (tmp_path / "weather.py").write_text("")
        (tmp_path / "shop").mkdir()
        (tmp_path / "shop" / "__init__.py").write_text("")
        (tmp_path / "src" / "acme").mkdir(parents=True)
        (tmp_path / "src" / "acme" / "__init__.py").write_text("")
        (tmp_path / "notes").mkdir()

        configure_console_logging(project=tmp_path)

        levels = {name: logging.getLogger(name).getEffectiveLevel() for name in ("weather", "shop.models", "acme")}
        assert levels == {"weather": logging.INFO, "shop.models": logging.INFO, "acme": logging.INFO}
        assert logging.getLogger("notes").getEffectiveLevel() == logging.WARNING

    def test_without_a_project_only_the_manifests_packages_print_info(self, tmp_path: Path) -> None:
        (tmp_path / "weather.py").write_text("")

        configure_console_logging()

        assert logging.getLogger("weather").getEffectiveLevel() == logging.WARNING

    def test_verbose_lowers_both(self) -> None:
        configure_console_logging(logging.DEBUG)

        assert logging.getLogger("interloper_scheduler.cron").getEffectiveLevel() == logging.DEBUG
        assert logging.getLogger("snowflake.connector").getEffectiveLevel() == logging.INFO

    def test_quiet_raises_both(self) -> None:
        configure_console_logging(logging.WARNING)

        assert logging.getLogger("interloper.run").getEffectiveLevel() == logging.WARNING
        assert logging.getLogger("snowflake.connector").getEffectiveLevel() == logging.WARNING

    def test_no_colour_off_a_terminal(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_console_logging()
        logging.getLogger("interloper.test").warning("plain")

        assert "\x1b" not in capsys.readouterr().err

    def test_colour_on_a_terminal_unless_no_color(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sys.stderr, "isatty", lambda: True, raising=False)
        monkeypatch.delenv("NO_COLOR", raising=False)
        configure_console_logging()
        (handler,) = _console_handlers()
        assert isinstance(handler.formatter, ConsoleFormatter) and handler.formatter.color

        monkeypatch.setenv("NO_COLOR", "1")
        configure_console_logging()
        (handler,) = _console_handlers()
        assert isinstance(handler.formatter, ConsoleFormatter) and not handler.formatter.color


class TestJsonFormat:
    def test_the_level_rule_applies_to_json_too(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_console_logging(log_format="json")
        logging.getLogger("snowflake.connector").info("theirs")
        logging.getLogger("interloper_api.routes").info("ours")

        (line,) = capsys.readouterr().err.splitlines()
        assert json.loads(line)["message"] == "ours"

    def test_a_record_is_one_json_object_per_line(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_console_logging(log_format="json")
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

        configure_console_logging(log_format="json")
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

        configure_console_logging(log_format="json")
        with trace.get_tracer("test").start_as_current_span("unrelated"):
            logging.getLogger("interloper.test").info(
                "from the bus", extra={"trace_id": "4bf92f3577b34da6a3ce929d0e0e4736", "span_id": "00f067aa0ba902b7"}
            )

        entry = json.loads(capsys.readouterr().err)
        assert (entry["trace_id"], entry["span_id"]) == ("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7")

    def test_an_exception_is_kept_in_one_line(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_console_logging(log_format="json")
        try:
            raise ValueError("boom")
        except ValueError:
            logging.getLogger("interloper.test").exception("failed")

        (line,) = capsys.readouterr().err.splitlines()
        entry = json.loads(line)
        assert entry["message"] == "failed"
        assert "ValueError: boom" in entry["exception"]
