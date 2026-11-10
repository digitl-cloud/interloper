"""Console logging for the CLI commands: text for a terminal, JSON for a log collector."""

from __future__ import annotations

import datetime as dt
import json
import logging
import sys
from typing import Literal

from opentelemetry import trace


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
            "timestamp": dt.datetime.fromtimestamp(record.created, dt.timezone.utc).isoformat(),
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
    level: int,
    text_format: str,
    datefmt: str | None = None,
    *,
    log_format: Literal["text", "json"] = "text",
) -> None:
    """Print log records to stderr at *level*, leaving the root logger's other handlers in place.

    Only the console handler is replaced: every plain ``StreamHandler`` on the
    root goes, the rest stay (pytest's capture, or a handler the embedding
    application attached). The new handler binds the current ``sys.stderr``,
    so repeated in-process invocations and captured streams write where they
    expect.

    Args:
        level: The root logger's level, which gates every handler.
        text_format: The ``logging.Formatter`` format string for ``text`` lines.
        datefmt: The ``asctime`` format of ``text`` lines, or ``None`` for the logging default.
        log_format: ``text`` for a terminal, ``json`` for a log collector (see
            :class:`JsonLogFormatter`).
    """
    root = logging.getLogger()
    for handler in [handler for handler in root.handlers if type(handler) is logging.StreamHandler]:
        root.removeHandler(handler)
        handler.close()
    console = logging.StreamHandler(sys.stderr)
    console.setFormatter(JsonLogFormatter() if log_format == "json" else logging.Formatter(text_format, datefmt))
    root.addHandler(console)
    root.setLevel(level)
