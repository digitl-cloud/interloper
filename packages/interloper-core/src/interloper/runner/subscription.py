"""RunSubscription: one run's events delivered to a handler while the run lasts."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from typing_extensions import Self

from interloper.events import Event, EventBus


class RunSubscription:
    """Deliver one run's events to a handler while the run lasts.

    The EventBus is process-wide, so several runs in one process (the
    in-process launcher) would otherwise reach every handler: only events
    carrying this run's id are delivered. Leaving the block flushes what is
    still queued, so every event the run emitted has reached the handler when
    the run returns.
    """

    def __init__(self, handler: Callable[[Event], None], run_id: str) -> None:
        """Bind the handler to one run.

        Args:
            handler: Receives each of the run's events.
            run_id: The run whose events are delivered.
        """
        self.handler = handler
        self.run_id = run_id

    def __call__(self, event: Event) -> None:
        """Deliver an event when it belongs to this run.

        Args:
            event: Any event the bus dispatches.
        """
        if event.metadata.get("run_id") == self.run_id:
            self.handler(event)

    async def __aenter__(self) -> Self:
        """Subscribe for the length of the run.

        Returns:
            This subscription.
        """
        EventBus.subscribe(self)
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Deliver what is still queued, then unsubscribe.

        Args:
            *exc_info: The exception leaving the block, if any; not suppressed.
        """
        await asyncio.to_thread(EventBus.flush, 5.0)
        EventBus.unsubscribe(self)
