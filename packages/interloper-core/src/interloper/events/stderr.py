"""EventBus handler that serialises events as prefixed JSON lines to stderr.

Used inside Docker / K8s containers so that the host-side runner can
parse structured events back from the container's log stream.  The
``@EVENT:`` prefix makes event lines reliably distinguishable from
regular application log output in the multiplexed stream::

    @EVENT:{"type":"asset_exec_started","timestamp":"...","asset_key":"..."}

Enable by setting the ``INTERLOPER_EVENTS_TO_STDERR=true`` environment
variable — the CLI ``run`` command detects it and subscribes this
handler to the global :class:`~interloper.events.EventBus`.
"""

from __future__ import annotations

import sys

from interloper.events.event import Event

EVENT_LINE_PREFIX = "@EVENT:"
"""Prefix prepended to every JSON event line written to stderr."""


class StderrEventHandler:
    """Write each event as a ``@EVENT:{json}`` line to stderr.

    Designed to be passed to :meth:`EventBus.subscribe` inside a
    container process.  The host-side runner's log-streaming thread
    recognises the prefix via
    :func:`~interloper.events.event.parse_event_from_log_line` and
    re-emits the event on the host EventBus.
    """

    def __call__(self, event: Event) -> None:
        """Serialize *event* to a prefixed JSON line on stderr.

        Args:
            event: The event to forward.
        """
        sys.stderr.write(f"{EVENT_LINE_PREFIX}{event.to_json()}\n")
        sys.stderr.flush()
