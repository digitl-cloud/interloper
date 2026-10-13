"""Console logging for the CLI commands."""

from __future__ import annotations

import logging
import sys


def configure_console_logging(level: int, line_format: str, datefmt: str | None = None) -> None:
    """Print log records to stderr at *level*, leaving the root logger's other handlers in place.

    ``logging.basicConfig`` does nothing once the root logger has a handler, and
    telemetry attaches its OTLP handler before any command runs; with
    ``force=True`` it would remove that handler instead. So only the console
    handler is replaced: every plain ``StreamHandler`` on the root goes, the
    rest stay. The new handler binds the current ``sys.stderr``, so repeated
    in-process invocations and captured streams write where they expect.

    Args:
        level: The root logger's level, which gates every handler.
        line_format: The ``logging.Formatter`` format string for console lines.
        datefmt: The ``asctime`` format, or ``None`` for the logging default.
    """
    root = logging.getLogger()
    for handler in [handler for handler in root.handlers if type(handler) is logging.StreamHandler]:
        root.removeHandler(handler)
        handler.close()
    console = logging.StreamHandler(sys.stderr)
    console.setFormatter(logging.Formatter(line_format, datefmt))
    root.addHandler(console)
    root.setLevel(level)
