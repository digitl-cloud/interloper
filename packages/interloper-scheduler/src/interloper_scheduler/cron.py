"""Cron controller: evaluates cron jobs and creates queued runs.

Jobs are component rows (``kind='job'``): their trigger lives in ``config``
(the spec, user-owned) and the controller writes only the ``state`` column
(machine-owned, UTC ISO-8601 strings): it advances ``next_run_at`` here;
``last_run_at`` is stamped by ``complete_run`` when a run finishes.
State is a pure cache — wiping it just makes every job reschedule from its
cron expression on the next tick, which is what the store does to a job's
``next_run_at`` whenever its config changes.

The ISO strings are written in one canonical form (timezone-aware UTC
``isoformat()``), which makes lexicographic string comparison in SQL a
correct chronological comparison — no JSON-to-timestamp casting needed.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, tzinfo
from typing import Any, cast

import interloper as il
from croniter import croniter
from interloper.errors import ConfigError, QuotaExceededError
from interloper.job.cron import CronJob
from interloper.partitioning.time import TimePartitionWindow
from interloper_db import Store
from interloper_db.models import Component

from interloper_scheduler.controller import Controller

logger = logging.getLogger(__name__)


class CronController(Controller):
    """Evaluates cron jobs and creates queued runs.

    Each tick, in one transaction: lock the due job rows, advance each
    one's ``state.next_run_at``, and create its firing's backfill (or single
    run), quota-checked like any other.
    """

    def __init__(
        self,
        store: Store | None = None,
        reconcile_interval: int = 10,
        max_execution_delay: int | None = None,
        batch_size: int = 50,
    ) -> None:
        """Initialize the cron controller.

        Args:
            store: The Store for creating backfills. Defaults to the
                settings-configured one.
            reconcile_interval: Seconds between cron evaluation cycles.
            max_execution_delay: Max seconds a scheduled job can be late.
                Defaults to the reconcile interval.
            batch_size: Number of jobs to process per cycle.

        Raises:
            ConfigError: If the max execution delay undercuts the
                reconcile interval.
        """
        super().__init__(poll_interval=reconcile_interval)
        self._store = store or Store.from_settings()
        self._batch_size = batch_size
        self._max_execution_delay = max_execution_delay if max_execution_delay is not None else reconcile_interval
        if self._max_execution_delay < reconcile_interval:
            raise ConfigError("cron.max_execution_delay must be >= cron.reconcile_interval")

    def _tick(self) -> None:
        """Process a batch of due jobs in a single transaction."""
        now = datetime.now(timezone.utc)
        with self._store.transaction():
            jobs = self._store.components.lock_due(
                "job", "next_run_at", now=now, limit=self._batch_size, enabled_only=True
            )
            if not jobs:
                return

            logger.info("Found %d job(s) ready to run", len(jobs))

            for job in jobs:
                config = job.config or {}
                cron_expression = config.get("cron")
                if not cron_expression:
                    continue

                zone = CronJob.zone(config)
                next_run = self._calculate_next_run(cron_expression, now, zone)
                scheduled_time = job.state_datetime("next_run_at")

                # New job: schedule for the future, don't run yet
                if scheduled_time is None:
                    self._store.components.stamp_state(job.id, next_run_at=next_run)
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
                    self._store.components.stamp_state(job.id, next_run_at=next_run)
                    continue

                self._store.components.stamp_state(job.id, next_run_at=next_run)

                try:
                    window = self._backfill_window(job, config, now.astimezone(zone))
                except ValueError as exc:
                    # Targets disagree on granularity: skip rather than
                    # backfill a window that is wrong for some of them.
                    logger.error("Skipping job '%s': %s", job.name, exc)
                    continue

                # The store calls join this tick's transaction, so the firing
                # commits atomically with the state advance above. A quota
                # rejection is raised before they write anything, and the
                # advanced next_run_at still commits: a blocked job must not
                # re-fire every tick.
                try:
                    if window is not None:
                        self._store.backfills.create(
                            job.org_id,
                            component_id=job.id,
                            start_key=window.granularity.format(window.start),
                            end_key=window.granularity.format(window.end),
                            concurrency=config.get("concurrency", 1),
                        )
                    else:
                        self._store.runs.create(job.org_id, component_id=job.id)
                except QuotaExceededError as exc:
                    self._skip_over_quota(job, exc)

            logger.info("Processed %d job(s)", len(jobs))

    # -- Internals -------------------------------------------------------------

    def _skip_over_quota(self, job: Component, error: QuotaExceededError) -> None:
        """Record a firing the organisation's quotas rejected, on the job itself.

        Args:
            job: The job whose firing was rejected.
            error: The rejection, carrying the quota and its message.
        """
        logger.warning("Skipping job '%s' for org %s: %s", job.name, job.org_id, error)
        event = il.Event(
            type=il.EventType.LOG,
            metadata={
                "component_id": str(job.id),
                "component_kind": job.kind,
                "component_key": job.key,
                "level": "warning",
                "message": f"Scheduled firing skipped: {error}",
                "quota": error.quota,
                "limit": error.limit,
                "used": error.used,
            },
        )
        self._store.events.save(event, org_id=job.org_id)

    def _backfill_window(
        self,
        job: Component,
        config: dict[str, Any],
        now: datetime,
    ) -> TimePartitionWindow | None:
        """Resolve the trailing window a partitioned job covers this tick.

        Whether a job is partitioned, and at which granularity, comes from its
        target assets' catalog definitions, never from the job's config (a
        denormalized copy could silently drift from the catalog): no partitioned
        target means a single unwindowed run.

        *now* is the tick instant on the job's wall clock, so a daily job's
        "yesterday" is its timezone's yesterday; HOUR windows normalize back
        to UTC inside the window arithmetic (see
        :meth:`TimePartitionWindow.lookback`).

        Resolving the granularity fails loudly when a job's targets disagree
        on one, since a window would be wrong for some of them.

        Args:
            job: The job row being evaluated.
            config: The job's raw config payload.
            now: The tick instant, on the job's wall clock.

        Returns:
            The window, or ``None`` for an unpartitioned job (or one whose
            lookback is explicitly null).
        """
        if not config.get("lookback", 1):
            return None
        granularity = self._store.components.job_partition_granularity(job.id)
        return CronJob.window(config, fires_at=now, granularity=granularity)

    def _calculate_next_run(self, cron_expression: str, base_time: datetime, zone: tzinfo) -> datetime:
        """Calculate the next run time from a cron expression.

        The expression is read on the wall clock of *zone* (croniter handles
        the DST transitions: a fire time inside the spring-forward gap slides
        to the first instant after it), and the result is converted back to
        UTC — state storage stays UTC everywhere.

        Args:
            cron_expression: Cron expression string.
            base_time: The reference time.
            zone: The timezone the expression is evaluated in.

        Returns:
            The next scheduled datetime (UTC).
        """
        iterator = croniter(cron_expression, base_time.astimezone(zone))
        next_run = cast(datetime, iterator.get_next(datetime))
        if next_run.tzinfo is None:
            next_run = next_run.replace(tzinfo=zone)
        return next_run.astimezone(timezone.utc)
