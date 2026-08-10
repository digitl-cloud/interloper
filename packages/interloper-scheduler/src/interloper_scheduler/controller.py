"""Base controller: the scheduler's background-loop skeleton."""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from threading import Event

from interloper.telemetry.tracer import meter
from opentelemetry.metrics import CallbackOptions, Observation

logger = logging.getLogger(__name__)

#: Unix timestamp of each loop's last successful tick, keyed by loop name.
#: One process hosts each controller as a singleton thread, so a plain dict
#: (guarded by the GIL for single-key writes) is enough.
_last_tick: dict[str, float] = {}
_gauge_registered = False


def _observe_ticks(_options: CallbackOptions) -> list[Observation]:
    return [Observation(ts, {"loop": name}) for name, ts in _last_tick.items()]


def _register_tick_gauge() -> None:
    """Register the liveness gauge once per process.

    A gauge (a level, not a flow) survives every sparse-counter pitfall:
    dashboards and alerts read staleness as ``time() - value`` per loop.
    Only *successful* ticks are recorded, so a loop that keeps failing goes
    stale even though its thread is alive.
    """
    global _gauge_registered
    if _gauge_registered:
        return
    _gauge_registered = True
    meter().create_observable_gauge(
        "interloper.scheduler.tick",
        callbacks=[_observe_ticks],
        unit="s",
        description="Unix time of each scheduler loop's last successful tick",
    )


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
        _register_tick_gauge()

    @property
    def _loop_name(self) -> str:
        """This loop's name on the liveness gauge (``CronController`` -> ``cron``)."""
        return type(self).__name__.removesuffix("Controller").lower()

    def start(self) -> None:
        """Run the loop until stopped."""
        logger.info("Starting %s (poll=%ds)...", type(self).__name__, self._poll_interval)
        try:
            while not self._stop_event.is_set():
                try:
                    self._tick()
                    _last_tick[self._loop_name] = time.time()
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
