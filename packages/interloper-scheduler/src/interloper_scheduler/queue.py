"""Queue controller: polls for queued runs and dispatches them."""

from __future__ import annotations

import logging
from uuid import UUID

from interloper_db import Store
from interloper_db.models import Run
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

    def _tick(self) -> None:
        """Dispatch queued runs until the queue is drained."""
        while not self._stop_event.is_set():
            run_id = self._claim_next()
            if run_id is None:
                return
            try:
                logger.info("Launching run %s", run_id)
                self._launcher.launch(run_id)
            except Exception as e:
                logger.exception("Failed to launch run %s: %s", run_id, e)
                # The same terminal path as any failed run: stamps the
                # component state and advances the backfill, so a failed
                # dispatch never wedges its backfill.
                self._store.complete_run(run_id, success=False)

    def _claim_next(self) -> UUID | None:
        """Claim the oldest queued run and mark it dispatched.

        Returns:
            The claimed run id, or ``None`` when the queue is empty.
        """
        with Session(self._store.engine) as session:
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

            run.status = "dispatched"
            session.add(run)
            session.commit()
            logger.info("Dispatched run %s", run.id)
            return run.id
