"""Cron controller: evaluates cron jobs and creates queued runs.

Jobs are component rows (``kind='job'``): their trigger lives in ``config``
(the spec, user-owned) and the controller writes only the ``state`` column
(machine-owned: ``next_run_at``/``last_run_at`` as UTC ISO-8601 strings).
State is a pure cache — wiping it just makes every job reschedule from its
cron expression on the next tick.

The ISO strings are written in one canonical form (timezone-aware UTC
``isoformat()``), which makes lexicographic string comparison in SQL a
correct chronological comparison — no JSON-to-timestamp casting needed.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
from datetime import datetime, timezone
from threading import Event
from typing import cast

from croniter import croniter
from interloper_db import Store, get_engine
from interloper_db.models import Backfill, Component, Run
from sqlalchemy import or_
from sqlmodel import Session, select

logger = logging.getLogger(__name__)


class CronController:
    """Evaluates cron jobs and creates queued runs.

    Runs in a loop:
    1. ``SELECT FOR UPDATE SKIP LOCKED`` (lock due job rows)
    2. update ``state.next_run_at`` (calculate next)
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

            next_run_at = Component.state["next_run_at"].as_string()  # ty: ignore[not-subscriptable]
            statement = (
                select(Component)
                .where(Component.kind == "job")
                .where(Component.config["enabled"].as_boolean())  # ty: ignore[not-subscriptable]
                .where(or_(next_run_at <= now.isoformat(), next_run_at.is_(None)))
                .order_by(next_run_at.asc().nulls_last())
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )

            jobs = session.exec(statement).all()
            if not jobs:
                return

            logger.info("Found %d job(s) ready to run", len(jobs))

            for job in jobs:
                config = job.config or {}
                cron_expr = config.get("cron")
                if not cron_expr:
                    continue

                next_run = self._calculate_next_run(cron_expr, now)
                scheduled_time = self._state_datetime(job, "next_run_at")

                # New job: schedule for the future, don't run yet
                if scheduled_time is None:
                    self._set_state(session, job, next_run_at=next_run)
                    logger.info("Scheduling new job '%s' for %s", job.name, next_run)
                    continue

                # Check if too old to execute
                delay_seconds = (now - scheduled_time).total_seconds()
                if delay_seconds > self._max_execution_delay:
                    logger.warning(
                        "Skipping job '%s' - too late (%ds > %ds)",
                        job.name,
                        int(delay_seconds),
                        self._max_execution_delay,
                    )
                    self._set_state(session, job, next_run_at=next_run)
                    continue

                self._set_state(session, job, next_run_at=next_run, last_run_at=now)

                # Create runs
                if config.get("partitioned") and config.get("backfill_days"):
                    end_date = now.date() - dt.timedelta(days=1)
                    start_date = end_date - dt.timedelta(days=config["backfill_days"] - 1)
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

    @staticmethod
    def _state_datetime(job: Component, key: str) -> datetime | None:
        """Parse a UTC ISO-8601 timestamp from a job's state."""
        value = (job.state or {}).get(key)
        if not value:
            return None
        parsed = datetime.fromisoformat(value)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    @staticmethod
    def _set_state(session: Session, job: Component, **timestamps: datetime) -> None:
        """Merge timestamps into the job's machine-owned state (spec untouched)."""
        job.state = {**(job.state or {}), **{key: value.isoformat() for key, value in timestamps.items()}}
        session.add(job)
        session.flush()

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
