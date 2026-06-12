"""Console event handler: events rendered through the standard logging stack.

Events are emitted as :class:`logging.LogRecord` instances on the
``interloper.run`` logger, so they share whatever format, stream, and level
configuration the application (or CLI) sets up — no second output system to
keep in sync with regular log lines.
"""

from __future__ import annotations

import logging
import sys

from interloper.events.event import Event
from interloper.events.types import EventType

#: Logger that carries event records; configure it like any other logger.
EVENT_LOGGER_NAME = "interloper.run"

# Failures are errors, cancellations warnings, run/asset lifecycle is the
# INFO narrative, and the high-frequency execution / destination-I/O chatter
# only shows at DEBUG.
_EVENT_LEVELS: dict[EventType, int] = {
    EventType.RUN_FAILED: logging.ERROR,
    EventType.ASSET_FAILED: logging.ERROR,
    EventType.BACKFILL_FAILED: logging.ERROR,
    EventType.ASSET_CANCELED: logging.WARNING,
    EventType.ASSET_QUEUED: logging.DEBUG,
    EventType.ASSET_EXEC_STARTED: logging.DEBUG,
    EventType.ASSET_EXEC_COMPLETED: logging.DEBUG,
    EventType.ASSET_EXEC_FAILED: logging.DEBUG,
    EventType.DEST_READ_STARTED: logging.DEBUG,
    EventType.DEST_READ_COMPLETED: logging.DEBUG,
    EventType.DEST_READ_FAILED: logging.DEBUG,
    EventType.DEST_WRITE_STARTED: logging.DEBUG,
    EventType.DEST_WRITE_COMPLETED: logging.DEBUG,
    EventType.DEST_WRITE_FAILED: logging.DEBUG,
}


def event_level(event: Event) -> int:
    """Map an event to its logging level.

    ``LOG`` events carry the level the asset author used (via
    ``context.logger``); every other type has a fixed level from
    ``_EVENT_LEVELS``, defaulting to ``INFO``.

    Args:
        event: The event to classify.

    Returns:
        A :mod:`logging` level constant.
    """
    if event.type is EventType.LOG:
        level = logging.getLevelName(str(event.metadata.get("level", "INFO")))
        return level if isinstance(level, int) else logging.INFO
    return _EVENT_LEVELS.get(event.type, logging.INFO)


def _format(event: Event) -> str:
    """Render an event as a log message (without timestamp/level — the formatter adds those).

    ``LOG`` events read as ``<asset>: <message>`` since their level already
    travels on the record; lifecycle events keep the columnar
    ``TYPE  asset  message`` form.

    Args:
        event: The event to render.

    Returns:
        The formatted message string.
    """
    m = event.metadata
    asset_key = m.get("asset_qualified_key") or m.get("asset_key") or "-"
    message = m.get("message") or m.get("error") or "-"
    if event.type is EventType.LOG:
        return f"{asset_key}: {message}"
    return f"{event.type.value.upper():<16} {asset_key}  {message}"


class ConsoleEventHandler:
    """Forward events to the logging stack (or raw JSON lines on stdout).

    Designed to be passed as a runner's ``on_event`` (which subscribes it to
    the :class:`~interloper.events.bus.EventBus` for the duration of the
    run).  In the default mode each event becomes a record on the
    ``interloper.run`` logger — its level mapped via :func:`event_level`, its
    timestamp taken from the event itself (not delivery time) — so events
    and ordinary log lines share one format, stream, and verbosity knob.

    With ``json_lines=True`` the logging stack is bypassed entirely and each
    event is written as a raw ``Event.to_json()`` line on **stdout**, ready
    to be piped into a file or another tool; level filtering does not apply.

    Args:
        json_lines: Emit raw JSON lines on stdout instead of log records.
    """

    def __init__(self, json_lines: bool = False) -> None:
        """Initialize the handler with its output mode."""
        self.json_lines = json_lines
        self._logger = logging.getLogger(EVENT_LOGGER_NAME)

    def __call__(self, event: Event) -> None:
        """Forward *event* to the configured output.

        Args:
            event: The event to forward.
        """
        if self.json_lines:
            sys.stdout.write(f"{event.to_json()}\n")
            sys.stdout.flush()
            return

        level = event_level(event)
        if not self._logger.isEnabledFor(level):
            return
        record = logging.LogRecord(self._logger.name, level, "", 0, _format(event), None, None)
        # Stamp the record with the event's own time (delivery via the bus
        # worker lags slightly); %(asctime)s renders it in local time like
        # every other log line.
        created = event.timestamp.timestamp()
        record.created = created
        record.msecs = (created - int(created)) * 1000
        self._logger.handle(record)
