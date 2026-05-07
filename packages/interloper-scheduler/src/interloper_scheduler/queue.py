"""Queue controller: polls for queued runs and dispatches them."""

from __future__ import annotations

import datetime as dt
import logging
import time
from threading import Event

from interloper_db import get_engine
from interloper_db.models import Run
from sqlmodel import Session, col, select

from interloper_scheduler.launcher import InProcessLauncher, Launcher

logger = logging.getLogger(__name__)


class QueueController:
    """Polls the runs table for queued runs and dispatches them.

    Uses ``SELECT FOR UPDATE SKIP LOCKED`` for safe concurrent polling.
    """

    def __init__(
        self,
        launcher: Launcher | None = None,
        poll_interval: int = 5,
    ) -> None:
        """Initialize the queue controller.

        Args:
            launcher: The launcher to use for dispatching runs.
            poll_interval: Seconds between poll cycles.
        """
        self._launcher = launcher or InProcessLauncher()
        self._poll_interval = poll_interval
        self._stop_event = Event()

    def start(self) -> None:
        """Run the polling loop until stopped."""
        logger.info("Starting queue controller...")

        while not self._stop_event.is_set():
            logger.info("Polling for queued runs...")

            try:
                with Session(get_engine()) as session:
                    statement = (
                        select(Run)
                        .where(Run.status == "queued")
                        .order_by(col(Run.created_at).asc())
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                    run = session.exec(statement).first()

                    if not run or not run.id:
                        if self._stop_event.wait(self._poll_interval):
                            break
                        continue

                    run_id = run.id
                    run.status = "dispatched"
                    session.add(run)
                    session.commit()
                    logger.info("Dispatched run %s", run_id)

                try:
                    logger.info("Launching run %s", run_id)
                    self._launcher.launch(run_id)
                except Exception as e:
                    logger.exception("Failed to launch run %s: %s", run_id, e)
                    with Session(get_engine()) as session:
                        failed_run = session.get(Run, run_id)
                        if failed_run:
                            failed_run.status = "failed"
                            failed_run.completed_at = dt.datetime.now(dt.timezone.utc)
                            session.add(failed_run)
                            session.commit()

            except Exception as e:
                logger.exception("Queue controller error: %s", e)
                time.sleep(5)

    def stop(self) -> None:
        """Signal the loop to stop."""
        self._stop_event.set()
