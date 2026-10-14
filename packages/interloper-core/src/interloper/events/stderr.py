"""Events across a process boundary, as prefixed JSON lines on stderr.

Inside a Docker or Kubernetes container, `StderrEventHandler` writes each
event as a line the host-side runner reads back from the container's log
with an `EventLogReader`. The ``@EVENT:`` prefix makes event lines reliably
distinguishable from regular application log output in the multiplexed
stream:

```py
@EVENT:{"type":"asset_data_started","timestamp":"...","component_key":"..."}
```

Enable by setting the ``INTERLOPER_EVENTS_TO_STDERR=true`` environment
variable: the CLI ``run`` command detects it and subscribes the handler to
the global `EventBus`.
"""

from __future__ import annotations

import logging
import sys

from interloper.events.bus import EventBus
from interloper.events.event import Event
from interloper.events.types import EventType

logger = logging.getLogger(__name__)

EVENT_LINE_PREFIX = "@EVENT:"
"""Prefix prepended to every JSON event line written to stderr."""

_RUN_EVENTS = frozenset({EventType.RUN_STARTED, EventType.RUN_COMPLETED, EventType.RUN_FAILED})


class StderrEventHandler:
    """Write each event as a ``@EVENT:{json}`` line to stderr.

    Designed to be passed to `EventBus.subscribe` inside a
    container process. The host-side runner reads the lines back with an
    `EventLogReader`, which re-emits each event on the host EventBus.
    """

    def __call__(self, event: Event) -> None:
        """Serialize *event* to a prefixed JSON line on stderr.

        Args:
            event: The event to forward.
        """
        sys.stderr.write(f"{EVENT_LINE_PREFIX}{event.to_json()}\n")
        sys.stderr.flush()


class EventLogReader:
    """Read one child's log and forward its operation's events to the host's EventBus.

    The other side of `StderrEventHandler`: a child process started for one
    operation writes each event as an ``@EVENT:`` line, and the host feeds the
    child's log through this reader as it arrives, in chunks of any size. The
    child's own ``RUN_*`` events and events about other operations (the
    read-only upstreams of its mini-DAG) are dropped; a line that is not an
    event is logged at debug level. A forwarded event keeps its id, so it
    persists idempotently.
    """

    def __init__(self, component_id: str, label: str) -> None:
        """Start reading the log of one operation's child.

        Args:
            component_id: The operation whose events are forwarded.
            label: Names the child in debug log lines, e.g. ``"pod interloper-run-1a2b"``.
        """
        self.component_id = component_id
        self.label = label
        self.pending = ""

    def feed(self, chunk: bytes) -> None:
        """Read a chunk of the log, forwarding every complete line it ends.

        Args:
            chunk: The next bytes of the log, split anywhere.
        """
        self.pending += chunk.decode("utf-8", errors="ignore")
        *lines, self.pending = self.pending.split("\n")
        for line in lines:
            self._read(line)

    def close(self) -> None:
        """Read the last line, which the log may end without a newline."""
        self._read(self.pending)
        self.pending = ""

    def _read(self, line: str) -> None:
        """Forward one line when it is an event about this operation.

        Args:
            line: One line of the log.
        """
        line = line.rstrip()
        if not line:
            return
        event = Event.from_log_line(line)
        if event is None:
            logger.debug("[%s] %s", self.label, line)
            return
        if event.type in _RUN_EVENTS or event.metadata.get("component_id") not in (None, self.component_id):
            return
        EventBus.emit_event(event)
