"""Console logging for the CLI commands: one line shape for a terminal, JSON for a log collector.

Every command prints through `configure_console_logging`, so a line reads the
same whoever writes it::

    19:53:44  INFO   db         Database up to date
    19:53:48  INFO   app        Ready in 4.4s on http://127.0.0.1:3000
                                api, cron, hooks, renewal, worker, reaper · 88 definitions

Only the packages the manifests name speak at the command's level; every
other library is held to warnings, so a start reads as ours.
"""

from __future__ import annotations

import datetime as dt
import functools
import json
import logging
import os
import sys
from pathlib import Path
from typing import ClassVar, Literal

from opentelemetry import trace


class ConsoleFormatter(logging.Formatter):
    """``time  level  source  message``, in columns, the level coloured on a terminal.

    The source is who speaks: the logger's package without its ``interloper``
    prefix (`source`). A message of several lines, or one carrying a
    traceback, continues under the message column.

    Attributes:
        color: Whether the level is wrapped in ANSI colour codes.
    """

    SOURCE_WIDTH: ClassVar[int] = 9
    MESSAGE_COLUMN: ClassVar[int] = 8 + 2 + 5 + 2 + SOURCE_WIDTH + 2
    LEVELS: ClassVar[dict[int, tuple[str, str]]] = {
        logging.DEBUG: ("DEBUG", "2"),
        logging.INFO: ("INFO", "32"),
        logging.WARNING: ("WARN", "33"),
        logging.ERROR: ("ERROR", "31"),
        logging.CRITICAL: ("FATAL", "1;31"),
    }

    def __init__(self, color: bool = False) -> None:
        """Format records as columns, in local time.

        Args:
            color: Whether to colour the level, for a terminal.
        """
        super().__init__(datefmt="%H:%M:%S")
        self.color = color

    def format(self, record: logging.LogRecord) -> str:
        """Render the record as one line, continued under the message column when it holds several.

        Args:
            record: The record to render.

        Returns:
            The line, with any traceback or stack after the message.
        """
        message = record.getMessage()
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            message = f"{message}\n{record.exc_text}"
        if record.stack_info:
            message = f"{message}\n{self.formatStack(record.stack_info)}"
        name, color = self.LEVELS.get(record.levelno, (record.levelname[:5], "0"))
        level = f"{name:<5}"
        if self.color:
            level = f"\x1b[{color}m{level}\x1b[0m"
        source = f"{self.source(record.name):<{self.SOURCE_WIDTH}}"
        indent = "\n" + " " * self.MESSAGE_COLUMN
        return f"{self.formatTime(record, self.datefmt)}  {level}  {source}  {indent.join(message.splitlines())}"

    @staticmethod
    @functools.cache
    def source(name: str) -> str:
        """Name who speaks from a logger's name: its package, without the ``interloper`` prefix.

        Args:
            name: The logger's name (``interloper_api.routes.providers``).

        Returns:
            The name's first part once ``interloper_`` or ``interloper.`` is
            dropped: ``api``, ``run`` for ``interloper.run``, ``alembic`` for
            ``alembic.runtime.migration``.
        """
        for prefix in ("interloper_", "interloper."):
            if name.startswith(prefix) and len(name) > len(prefix):
                name = name.removeprefix(prefix)
                break
        return name.partition(".")[0]


class JsonLogFormatter(logging.Formatter):
    """One JSON object per record, with the trace and span it was written under.

    A record that carries ``trace_id`` and ``span_id`` attributes keeps them:
    the console event handler sets them from the run, since events reach it on
    the bus's own thread. Any other record takes the current span, which is
    the one it was written in because handlers format in the thread that logs.
    The ids are absent outside a span and when telemetry is off.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Render the record as a single JSON line.

        Args:
            record: The record to render.

        Returns:
            The JSON object: ``timestamp`` (ISO 8601, UTC), ``severity``,
            ``logger`` and ``message``, plus ``trace_id`` and ``span_id``
            inside a span and ``exception`` when the record carries one.
        """
        entry: dict[str, str] = {
            "timestamp": dt.datetime.fromtimestamp(record.created, dt.UTC).isoformat(),
            "severity": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        trace_id, span_id = getattr(record, "trace_id", None), getattr(record, "span_id", None)
        if trace_id is None or span_id is None:
            span = trace.get_current_span().get_span_context()
            if span.is_valid:
                trace_id, span_id = trace.format_trace_id(span.trace_id), trace.format_span_id(span.span_id)
        if trace_id is not None and span_id is not None:
            entry["trace_id"], entry["span_id"] = trace_id, span_id
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def configure_console_logging(
    level: int = logging.INFO, *, log_format: Literal["text", "json"] = "text", project: Path | None = None
) -> None:
    """Print log records to stderr, ours at *level* and every library's from warnings up.

    The packages the manifests name (the platform's and every installed
    extension's) log at *level*, and so do the project's own modules when a
    command serves or runs one; every other logger at WARNING, or INFO when
    *level* is DEBUG, so ``-v`` lowers both and ``--quiet`` raises both. The
    levels are set on the loggers, so JSON follows the same rule.

    Only the console handler is replaced: every plain ``StreamHandler`` on the
    root goes, the rest stay (pytest's capture, or a handler the embedding
    application attached). The new handler binds the current ``sys.stderr``,
    so repeated in-process invocations and captured streams write where they
    expect.

    Args:
        level: The level of the platform's and the extensions' packages:
            INFO by default, DEBUG for ``-v``, WARNING for ``--quiet``.
        log_format: ``text`` for a terminal (see `ConsoleFormatter`, coloured
            when stderr is a terminal and ``NO_COLOR`` is unset), ``json`` for
            a log collector (see `JsonLogFormatter`).
        project: The directory of the project the command serves or runs:
            the modules and packages at its top and under its ``src/`` log at
            *level* too. ``None`` for a command that serves no project of its own.
    """
    from interloper.extension import MANIFESTS

    root = logging.getLogger()
    for handler in [handler for handler in root.handlers if type(handler) is logging.StreamHandler]:
        root.removeHandler(handler)
        handler.close()
    console = logging.StreamHandler(sys.stderr)
    if log_format == "json":
        console.setFormatter(JsonLogFormatter())
    else:
        console.setFormatter(ConsoleFormatter(color=sys.stderr.isatty() and not os.environ.get("NO_COLOR")))
    root.addHandler(console)
    root.setLevel(logging.INFO if level <= logging.DEBUG else max(level, logging.WARNING))
    packages = {extension.package.partition(".")[0] for extension in MANIFESTS.extensions()}
    for folder in (project, project / "src") if project is not None else ():
        modules = [*folder.glob("*.py"), *(path.parent for path in folder.glob("*/*.py"))]
        packages |= {module.stem for module in modules if module.stem.isidentifier()}
    for package in packages:
        logging.getLogger(package).setLevel(level)
