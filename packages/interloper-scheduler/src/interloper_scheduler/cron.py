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
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter
from interloper.errors import ConfigError
from interloper.partitioning.time import TimePartitionWindow
from interloper_db import Store
from interloper_db.models import Backfill, Component, Run
from sqlalchemy import or_
from sqlmodel import Session, select

from interloper_scheduler.controller import Controller

logger = logging.getLogger(__name__)


class CronController(Controller):
    """Evaluates cron jobs and creates queued runs.

    Each tick:
    1. ``SELECT FOR UPDATE SKIP LOCKED`` (lock due job rows)
    2. update ``state.next_run_at`` (calculate next)
    3. ``INSERT run`` with ``status='queued'`` (create run)
    4. ``COMMIT`` (release locks)
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
        with self._store.transaction() as session:
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
                cron_expression = config.get("cron")
                if not cron_expression:
                    continue

                zone = self._job_zone(job, config)
                next_run = self._calculate_next_run(cron_expression, now, zone)
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

                self._set_state(session, job, next_run_at=next_run)

                # Quota-exhausted orgs skip run creation but still advance
                # next_run_at (committed with this session) — otherwise the
                # blocked job would re-fire every tick forever.
                committed, limit = self._store.quotas.run_status(job.org_id)
                if limit is not None and committed >= limit:
                    logger.warning(
                        "Skipping job '%s' - monthly successful-run quota exhausted (%d/%d) for org %s",
                        job.name,
                        committed,
                        limit,
                        job.org_id,
                    )
                    continue

                # Create runs. The backfill is built inline rather than via
                # Store.runs.create_backfill: it must commit atomically with the
                # job's state advance (else a crash between the two would
                # re-create it next tick), and cron top-ups queue every
                # partition immediately instead of concurrency-gating. Because
                # every run is queued at once, the queue worker's FIFO claim
                # order (runs.created_at) decides execution order here.
                try:
                    window = self._backfill_window(session, job, config, now.astimezone(zone))
                except ValueError as exc:
                    # Targets disagree on granularity: skip rather than
                    # backfill a window that is wrong for some of them.
                    logger.error("Skipping job '%s': %s", job.name, exc)
                    continue
                if window is not None:
                    backfill = Backfill(
                        org_id=job.org_id,
                        component_id=job.id,
                        start_key=window.granularity.format(window.start),
                        end_key=window.granularity.format(window.end),
                        status="running",
                        started_at=now,
                    )
                    session.add(backfill)
                    session.flush()

                    for value in window.granularity.period_range(window.start, window.end):
                        run = Run(
                            component_id=job.id,
                            org_id=job.org_id,
                            backfill_id=backfill.id,
                            status="queued",
                            partition_key=window.granularity.format(value),
                        )
                        session.add(run)
                    backfill.partitions = window.partition_count()
                    session.add(backfill)
                else:
                    run = Run(
                        component_id=job.id,
                        org_id=job.org_id,
                        status="queued",
                    )
                    session.add(run)

            session.commit()
            logger.info("Processed %d job(s)", len(jobs))

    # -- Internals -------------------------------------------------------------

    def _backfill_window(
        self,
        session: Session,
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
            session: Open session the targets are resolved in.
            job: The job row being evaluated.
            config: The job's raw config payload.
            now: The tick instant, on the job's wall clock.

        Returns:
            The window, or ``None`` for an unpartitioned job (or one whose
            lookback is explicitly null).
        """
        # A missing key means the model default (1); an explicit null opts out.
        lookback = config.get("lookback", 1)
        if not lookback:
            return None
        granularity = self._store.components.job_partition_granularity(session, job.id)
        if granularity is None:
            return None
        return TimePartitionWindow.lookback(
            now,
            lookback=lookback,
            offset=config.get("offset", 1),
            granularity=granularity,
        )

    @staticmethod
    def _state_datetime(job: Component, key: str) -> datetime | None:
        """Parse a UTC ISO-8601 timestamp from a job's state.

        Args:
            job: The job row whose state is read.
            key: State key holding the timestamp.

        Returns:
            The timestamp as an aware UTC datetime, or ``None`` when the key
            is absent or empty.
        """
        value = (job.state or {}).get(key)
        if not value:
            return None
        parsed = datetime.fromisoformat(value)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    @staticmethod
    def _set_state(session: Session, job: Component, **timestamps: datetime) -> None:
        """Merge timestamps into the job's machine-owned state (spec untouched).

        Args:
            session: Open session the write joins.
            job: The job row to stamp.
            **timestamps: State fields to set, merged over the existing payload.
        """
        job.stamp_state(**timestamps)
        session.add(job)
        session.flush()

    @staticmethod
    def _job_zone(job: Component, config: dict[str, Any]) -> tzinfo:
        """Resolve the job's timezone from its raw config, defaulting to UTC.

        The config was zoneinfo-validated at write time, but the scheduler
        reads raw dicts — an unresolvable name (e.g. after a tzdata change)
        must degrade to UTC rather than wedge the whole tick.

        Args:
            job: The job row, named in the warning when its zone is unknown.
            config: The job's raw config payload.

        Returns:
            The job's zone, or UTC when the config carries none.
        """
        name = config.get("timezone") or "UTC"
        try:
            return ZoneInfo(name)
        except (ZoneInfoNotFoundError, ValueError, TypeError):
            logger.warning("Job '%s' has an unknown timezone %r - evaluating in UTC", job.name, name)
            return timezone.utc

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
