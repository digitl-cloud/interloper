"""Cron controller: evaluates cron jobs and creates queued runs."""

from __future__ import annotations

import datetime as dt
import logging
import os
from datetime import datetime, timezone
from threading import Event
from typing import cast

from croniter import croniter
from interloper_db import Store, get_engine
from interloper_db.models import Backfill, Job, Run
from sqlalchemy import or_
from sqlmodel import Session, col, select

logger = logging.getLogger(__name__)


class CronController:
    """Evaluates cron jobs and creates queued runs.

    Runs in a loop:
    1. ``SELECT FOR UPDATE SKIP LOCKED`` (lock jobs)
    2. ``UPDATE next_run_at`` (calculate next)
    3. ``INSERT run`` with ``status='queued'`` (create run)
    4. ``COMMIT`` (release locks)
    """

    def __init__(
        self,
        store: Store | None = None,
        reconcile_interval: int | None = None,
        max_execution_delay: int | None = None,
        batch_size: int = 50,
    ) -> None:
        """Initialize the cron controller.

        Args:
            store: The Store for creating backfills. Creates a default if not provided.
            reconcile_interval: Seconds between cron evaluation cycles.
            max_execution_delay: Max seconds a scheduled job can be late.
            batch_size: Number of jobs to process per cycle.
        """
        if store is None:
            from interloper.catalog import Catalog

            store = Store.from_settings(catalog=Catalog.from_settings())
        self._store = store
        self._batch_size = batch_size
        self._reconcile_interval = reconcile_interval or int(os.getenv("JOB_RECONCILE_INTERVAL", "10"))
        self._max_execution_delay = max_execution_delay or int(
            os.getenv("MAX_JOB_EXECUTION_DELAY", str(self._reconcile_interval))
        )
        if self._max_execution_delay < self._reconcile_interval:
            from interloper.errors import ConfigError

            raise ConfigError("MAX_JOB_EXECUTION_DELAY must be >= JOB_RECONCILE_INTERVAL")
        self._stop_event = Event()

    def start(self) -> None:
        """Run the cron evaluation loop until stopped."""
        logger.info("Starting cron controller...")

        try:
            while not self._stop_event.is_set():
                logger.info("Evaluating cron jobs...")
                try:
                    self._process_jobs()
                except Exception as e:
                    logger.error("Failed to process jobs: %s", e)

                if self._stop_event.wait(self._reconcile_interval):
                    break
        except KeyboardInterrupt:
            logger.info("Shutting down cron controller...")

    def stop(self) -> None:
        """Signal the loop to stop."""
        self._stop_event.set()

    def _process_jobs(self) -> None:
        """Process a batch of due jobs in a single transaction."""
        session = Session(get_engine())

        try:
            now = datetime.now(timezone.utc)

            statement = (
                select(Job)
                .where(Job.enabled)
                .where(or_(col(Job.next_run_at) <= now, col(Job.next_run_at).is_(None)))
                .order_by(col(Job.next_run_at).asc().nulls_last())
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )

            jobs = session.exec(statement).all()
            if not jobs:
                return

            logger.info("Found %d job(s) ready to run", len(jobs))

            for job in jobs:
                next_run_at = self._calculate_next_run(job.cron, now)

                # New job: schedule for the future, don't run yet
                if job.next_run_at is None:
                    job.next_run_at = next_run_at
                    session.add(job)
                    session.flush()
                    logger.info("Scheduling new job '%s' for %s", job.name, next_run_at)
                    continue

                # Check if too old to execute
                scheduled_time = job.next_run_at
                if scheduled_time.tzinfo is None:
                    scheduled_time = scheduled_time.replace(tzinfo=timezone.utc)

                delay_seconds = (now - scheduled_time).total_seconds()
                if delay_seconds > self._max_execution_delay:
                    logger.warning(
                        "Skipping job '%s' - too late (%ds > %ds)",
                        job.name,
                        int(delay_seconds),
                        self._max_execution_delay,
                    )
                    job.next_run_at = next_run_at
                    session.add(job)
                    session.flush()
                    continue

                job.next_run_at = next_run_at
                session.add(job)
                session.flush()

                if job.partitioned and job.backfill_days:
                    end_date = now.date() - dt.timedelta(days=1)
                    start_date = end_date - dt.timedelta(days=job.backfill_days - 1)
                    backfill = Backfill(
                        org_id=job.org_id,
                        job_id=job.id,
                        start_date=start_date,
                        end_date=end_date,
                        status="running",
                        started_at=now,
                    )
                    session.add(backfill)
                    session.flush()

                    count = 0
                    current = start_date
                    while current <= end_date:
                        run = Run(
                            job_id=job.id,
                            org_id=job.org_id,
                            backfill_id=backfill.id,
                            status="queued",
                            partition_date=current,
                        )
                        session.add(run)
                        count += 1
                        current += dt.timedelta(days=1)
                    backfill.partitions = count
                    session.add(backfill)
                else:
                    run = Run(
                        job_id=job.id,
                        org_id=job.org_id,
                        status="queued",
                    )
                    session.add(run)

            session.commit()
            logger.info("Processed %d job(s)", len(jobs))

        except Exception as e:
            logger.exception("Error processing jobs: %s", e)
            session.rollback()
            raise
        finally:
            session.close()

    def _calculate_next_run(self, cron_expr: str, base_time: datetime) -> datetime:
        """Calculate the next run time from a cron expression.

        Args:
            cron_expr: Cron expression string.
            base_time: The reference time.

        Returns:
            The next scheduled datetime (UTC).
        """
        itr = croniter(cron_expr, base_time)
        next_run = cast(datetime, itr.get_next(datetime))
        if next_run.tzinfo is None:
            return next_run.replace(tzinfo=timezone.utc)
        return next_run
