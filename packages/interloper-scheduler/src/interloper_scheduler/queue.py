"""Queue controller: claims queued runs, hands each its inputs, and launches it."""

from __future__ import annotations

import logging
from uuid import UUID

import interloper as il
from interloper.dag import DAGSpec
from interloper.errors import format_exception
from interloper.telemetry import attributes
from interloper.telemetry.tracer import meter, tracer
from interloper_db import NotFoundError, Store
from interloper_db.models import Run

from interloper_scheduler.controller import Controller
from interloper_scheduler.launcher import InProcessLauncher, Launcher

logger = logging.getLogger(__name__)


class QueueController(Controller):
    """Claims queued runs, assembles what each executes, and launches it.

    Each tick drains the queue: runs are claimed (:meth:`RunStore.claim_next`,
    safe for concurrent workers, which mints each run's token) and launched one
    at a time until none are left, then the controller sleeps. A run is
    assembled here, where the store is: its target hydrated into the DAG spec
    its process executes, so that process needs nothing but its token to
    report back. A run that cannot be assembled or launched is failed with the
    reason.
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
            store: The Store runs are claimed, assembled and failed through.
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
                spec = self._assemble(run)
                if spec is None:
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
                    self._launcher.launch(run, spec, self._store.runs.token(run.id))
                self._launched_counter.add(1, {"outcome": "launched"})
            except Exception as error:
                logger.exception("Failed to launch run %s", run.id)
                self._launched_counter.add(1, {"outcome": "failed"})
                self._store.runs.fail(run.id, format_exception(error))

    def _assemble(self, run: Run) -> DAGSpec | None:
        """Hydrate the run's target into the DAG spec its process executes.

        A failed-scope retry's prior successes are disabled in the spec, so
        the run re-executes only what had not succeeded. A run whose window its
        operations refuse is refused here, before anything is launched.

        Args:
            run: The dispatched run, its target loaded.

        Returns:
            The spec, or ``None`` when the target selects no operations.

        Raises:
            NotFoundError: If the run's target was deleted.
            TypeError: If the target's kind declares no workload.
        """
        if run.component_id is None:
            raise NotFoundError(f"Run {run.id} targets a component that was deleted")
        target = self._store.components.load(run.component_id)
        if not isinstance(target, il.Workload):
            raise TypeError(f"Component kind '{type(target).kind}' declares no workload")
        operations = target.operations()
        if not operations:
            return None
        if run.retry_scope == "failed":
            successes = self._store.executions.prior_successes(run)
            for operation in operations:
                if UUID(operation.id) in successes:
                    operation.enabled = False
        dag = il.DAG(*operations)
        dag.check_window(
            il.PartitionWindow.of(il.TimePartition.from_key(run.partition_key)) if run.partition_key else None
        )
        return dag.to_spec()
