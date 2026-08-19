"""Cron controller: evaluates cron jobs and creates queued runs.

Jobs are component rows (``kind='job'``): their trigger lives in ``config``
(the spec, user-owned) and the controller writes only the ``state`` column
(machine-owned, UTC ISO-8601 strings): it advances ``next_run_at`` here;
``last_run_at`` is stamped by ``complete_run`` when a run finishes.
State is a pure cache — wiping it just makes every job reschedule from its
cron expression on the next tick.

The ISO strings are written in one canonical form (timezone-aware UTC
``isoformat()``), which makes lexicographic string comparison in SQL a
correct chronological comparison — no JSON-to-timestamp casting needed.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, cast

from croniter import croniter
from interloper.errors import ConfigError
from interloper.partitioning.time import (
    TimeGranularity,
    TimePartitionWindow,
    lookback_window,
    period_range,
)
from interloper_db import Store, stamp_component_state
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
        with Session(self._store.engine) as session:
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

                self._set_state(session, job, next_run_at=next_run)

                # Quota-exhausted orgs skip run creation but still advance
                # next_run_at (committed with this session) — otherwise the
                # blocked job would re-fire every tick forever.
                committed, limit = self._store.quotas.run_status(session, job.org_id)
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
                # Store.create_backfill: it must commit atomically with the
                # job's state advance (else a crash between the two would
                # re-create it next tick), and cron top-ups queue every
                # partition immediately instead of concurrency-gating. Because
                # every run is queued at once, the queue worker's FIFO claim
                # order (runs.created_at) decides execution order here.
                window = self._backfill_window(config, now)
                if window is not None:
                    backfill = Backfill(
                        org_id=job.org_id,
                        component_id=job.id,
                        start_date=window.start,
                        end_date=window.end,
                        status="running",
                        started_at=now,
                    )
                    session.add(backfill)
                    session.flush()

                    for partition_date in period_range(window.start, window.end, window.granularity):
                        run = Run(
                            component_id=job.id,
                            org_id=job.org_id,
                            backfill_id=backfill.id,
                            status="queued",
                            partition_date=partition_date,
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

    @staticmethod
    def _backfill_window(config: dict[str, Any], now: datetime) -> TimePartitionWindow | None:
        """Resolve the trailing window a partitioned job covers this tick.

        The granularity is pinned to ``DAY``: it belongs to the target assets,
        not to the job's config (which would be a third denormalized value able
        to drift from the catalog), and daily is the only granularity an asset
        may declare today. Resolving it from the targets is what changes here
        when that stops being true.

        Returns:
            The window, or ``None`` for an unpartitioned job (or one whose
            lookback is unset).
        """
        if not config.get("partitioned"):
            return None
        lookback = config.get("lookback")
        if not lookback:
            return None
        return lookback_window(
            now,
            lookback=lookback,
            offset=config.get("offset", 1),
            granularity=TimeGranularity.DAY,
        )

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
        stamp_component_state(job, **timestamps)
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
