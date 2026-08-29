"""Queue controller: polls for queued runs and dispatches them."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID, uuid4

from interloper.telemetry import attributes
from interloper.telemetry.tracer import meter, tracer
from interloper_db import Store
from interloper_db.models import Backfill, Event, Run
from interloper_db.store.runs import cancel_backfill_runs
from sqlmodel import Session, col, select

from interloper_scheduler.controller import Controller
from interloper_scheduler.launcher import InProcessLauncher, Launcher

logger = logging.getLogger(__name__)


class QueueController(Controller):
    """Polls the runs table for queued runs and dispatches them.

    Uses ``SELECT FOR UPDATE SKIP LOCKED`` for safe concurrent polling.
    Each tick drains the queue: runs are claimed and launched one at a
    time until none are left, then the controller sleeps.
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
            run_id = self._claim_next()
            if run_id is None:
                return
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
            except Exception as e:
                logger.exception("Failed to launch run %s: %s", run_id, e)
                self._launched_counter.add(1, {"outcome": "failed"})
                # The same terminal path as any failed run: stamps the
                # component state and advances the backfill, so a failed
                # dispatch never wedges its backfill.
                self._store.runs.complete(run_id, success=False)

    def _claim_next(self) -> UUID | None:
        """Claim the oldest queued run, reserve its quota slot, and mark it dispatched.

        This is the authoritative run-quota gate: dispatch requires an atomic
        reservation, so an exhausted organisation can never execute past its
        limit. Denied runs are canceled (their whole backfill with them) and
        the loop moves on to the next queued run — canceling rather than
        skipping keeps an exhausted org from head-of-line-blocking the queue.

        Returns:
            The claimed run id, or ``None`` when the queue is empty.
        """
        while True:
            with self._store.transaction() as session:
                statement = (
                    select(Run)
                    .where(Run.status == "queued")
                    .order_by(col(Run.created_at).asc())
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                run = session.exec(statement).first()
                if not run or not run.id:
                    return None

                if self._store.quotas.try_reserve_run(run):
                    run.status = "dispatched"
                    session.add(run)
                    session.commit()
                    logger.info("Dispatched run %s", run.id)
                    return run.id

                self._cancel_over_quota(session, run)
                session.commit()
                logger.warning(
                    "Canceled run %s: monthly successful-run quota exhausted for org %s", run.id, run.org_id
                )

    @staticmethod
    def _cancel_over_quota(session: Session, run: Run) -> None:
        """Cancel a quota-denied run (and its backfill), with an explanatory event.

        A canceled run is never claimed again, so the event cannot double-write.
        """
        run.status = "canceled"
        session.add(run)
        if run.backfill_id:
            backfill = session.get(Backfill, run.backfill_id)
            if backfill and backfill.status in ("running", "queued"):
                cancel_backfill_runs(session, backfill)
        session.add(
            Event(
                id=uuid4(),
                org_id=run.org_id,
                run_id=run.id,
                component_id=run.component_id,
                event_type="log",
                level="warning",
                message="Run canceled: the organisation's monthly successful-run quota is exhausted",
                timestamp=datetime.now(timezone.utc),
            )
        )
