"""Queue controller: claims queued runs, checks each can be built, and launches it."""

from __future__ import annotations

import logging

from interloper.errors import format_exception
from interloper.telemetry import attributes
from interloper.telemetry.tracer import meter, tracer
from interloper_db import Store

from interloper_scheduler.controller import Controller
from interloper_scheduler.launcher import InProcessLauncher, Launcher

logger = logging.getLogger(__name__)


class QueueController(Controller):
    """Claims queued runs, checks each can be built, and launches it.

    Each tick drains the queue: runs are claimed (:meth:`RunStore.claim_next`,
    safe for concurrent workers) and launched one at a time until none are
    left, then the controller sleeps. Each run's spec is built here first
    (:meth:`RunStore.spec`), so a run whose target is gone or whose window its
    operations refuse fails with the reason before anything is launched; the
    launched process builds it again when it starts, from its id. A run that
    cannot be built or launched is failed with the reason.
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
            store: The Store runs are claimed, checked and failed through.
                Defaults to the settings-configured one.
            poll_interval: Seconds between poll cycles when the queue is empty.
        """
        super().__init__(poll_interval=poll_interval)
        self._store = store or Store.from_settings()
        self._launcher = launcher or InProcessLauncher(store=self._store)
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
            try:
                if self._store.runs.spec(run.id) is None:
                    logger.info("Run %s selects no operations; completed", run.id)
                    self._store.runs.complete(run.id)
                    continue
                logger.info("Launching run %s", run.id)
                # Dispatch trace root; the launched run starts its own trace and links back to this span.
                with tracer().start_as_current_span(
                    "interloper.launcher.launch",
                    attributes={
                        attributes.RUN_ID: str(run.id),
                        attributes.LAUNCHER_TYPE: type(self._launcher).__name__,
                    },
                ):
                    self._launcher.launch(run, self._store.runs.token(run.id))
                self._launched_counter.add(1, {"outcome": "launched"})
            except Exception as error:
                logger.exception("Failed to launch run %s", run.id)
                self._launched_counter.add(1, {"outcome": "failed"})
                self._store.runs.fail(run.id, format_exception(error))
