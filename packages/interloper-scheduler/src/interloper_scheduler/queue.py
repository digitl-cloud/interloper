"""Queue controller: polls for queued runs and dispatches them."""

from __future__ import annotations

import logging

from interloper.telemetry import attributes
from interloper.telemetry.tracer import meter, tracer
from interloper_db import Store

from interloper_scheduler.controller import Controller
from interloper_scheduler.launcher import InProcessLauncher, Launcher

logger = logging.getLogger(__name__)


class QueueController(Controller):
    """Claims queued runs and launches them.

    Each tick drains the queue: runs are claimed (:meth:`RunStore.claim_next`,
    safe for concurrent workers) and launched one at a time until none are
    left, then the controller sleeps.
    """

    def __init__(
        self,
        launcher: Launcher | None = None,
        store: Store | None = None,
        poll_interval: int = 5,
    ) -> None:
        """Initialize the queue controller.

        Args:
            launcher: The launcher to use for dispatching runs.
            store: The Store used to fail runs that cannot launch.
                Defaults to the settings-configured one.
            poll_interval: Seconds between poll cycles when the queue is empty.
        """
        super().__init__(poll_interval=poll_interval)
        self._launcher = launcher or InProcessLauncher()
        self._store = store or Store.from_settings()
        # The launch outcome emits no bus event, so this counter is inline.
        self._launched_counter = meter().create_counter(
            "interloper.runs.launched", unit="{run}", description="Runs dispatched by the queue"
        )

    def _tick(self) -> None:
        """Dispatch queued runs until the queue is drained."""
        while not self._stop_event.is_set():
            run = self._store.runs.claim_next()
            if run is None:
                return
            run_id = run.id
            try:
                logger.info("Launching run %s", run_id)
                # Dispatch trace root; the launched run starts its own trace
                # and links back to this span.
                with tracer().start_as_current_span(
                    "interloper.launcher.launch",
                    attributes={
                        attributes.RUN_ID: str(run_id),
                        attributes.LAUNCHER_TYPE: type(self._launcher).__name__,
                    },
                ):
                    self._launcher.launch(run_id)
                self._launched_counter.add(1, {"outcome": "launched"})
            except Exception:
                logger.exception("Failed to launch run %s", run_id)
                self._launched_counter.add(1, {"outcome": "failed"})
                # The same terminal path as any failed run: stamps the
                # component state and advances the backfill, so a failed
                # dispatch never wedges its backfill.
                self._store.runs.complete(run_id, success=False)
