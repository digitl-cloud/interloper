"""Base controller: the scheduler's background-loop skeleton."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from threading import Event

logger = logging.getLogger(__name__)


class Controller(ABC):
    """A stoppable background loop ticking at a fixed interval.

    Subclasses implement one :meth:`_tick`; the base owns the loop: a tick
    that raises is logged and the loop carries on, ``stop()`` (or
    ``KeyboardInterrupt``) ends it. Each controller runs as a singleton
    thread — concurrency safety across *processes* comes from the ticks
    themselves (``SKIP LOCKED`` claims, idempotent upserts), not the loop.
    """

    def __init__(self, poll_interval: int) -> None:
        """Initialize the loop.

        Args:
            poll_interval: Seconds between ticks.
        """
        self._poll_interval = poll_interval
        self._stop_event = Event()

    def start(self) -> None:
        """Run the loop until stopped."""
        logger.info("Starting %s (poll=%ds)...", type(self).__name__, self._poll_interval)
        try:
            while not self._stop_event.is_set():
                try:
                    self._tick()
                except Exception:
                    logger.exception("%s tick failed", type(self).__name__)

                if self._stop_event.wait(self._poll_interval):
                    break
        except KeyboardInterrupt:
            logger.info("Shutting down %s...", type(self).__name__)

    def stop(self) -> None:
        """Signal the loop to stop."""
        self._stop_event.set()

    @abstractmethod
    def _tick(self) -> None:
        """One cycle of the controller's work."""
