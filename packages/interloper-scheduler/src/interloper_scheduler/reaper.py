"""Reaper: fails the runs that will never end on their own.

A running run renews its heartbeat (:mod:`~interloper_scheduler.heartbeat`),
so the reaper needs no launcher to know it is alive. Each poll it asks the
store which dispatched and running runs are overdue
(:meth:`~interloper_db.store.runs.RunStore.overdue`):

- a dispatched run whose pod has not started within ``startup_timeout``;
- a running run whose heartbeat has been silent for ``heartbeat_timeout``;
- a running run past its deadline: its job's ``timeout``, else ``run_timeout``.

Each is failed with its reason, the launcher's diagnosis of its workload
appended when it has one (an OOM kill, an exit code), through
:meth:`~interloper_db.store.runs.RunStore.reap`, which leaves alone a run that
started or renewed its heartbeat since it was read. A failed run's executor,
if it is still alive, learns on its next heartbeat and stops.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID

from interloper_db import Store

from interloper_scheduler.controller import Controller

if TYPE_CHECKING:
    from interloper_scheduler.launcher import Launcher

logger = logging.getLogger(__name__)


class Reaper(Controller):
    """Periodically fails the dispatched and running runs that are overdue.

    Designed to run in a background thread alongside the
    :class:`~interloper_scheduler.queue.QueueController`::

        reaper = Reaper(store=store, launcher=launcher)
        thread = threading.Thread(target=reaper.start, daemon=True)
        thread.start()
    """

    def __init__(
        self,
        store: Store,
        launcher: Launcher | None = None,
        *,
        startup_timeout: int = 600,
        heartbeat_timeout: int = 90,
        run_timeout: int | None = 43200,
        poll_interval: int = 15,
    ) -> None:
        """Initialize the reaper.

        Args:
            store: Store the overdue runs are read and failed through.
            launcher: Optional launcher asked to diagnose a dead run's
                workload; ``None`` records the reaper's reason alone.
            startup_timeout: Seconds a dispatched run may take to start.
            heartbeat_timeout: Seconds a running run may stay silent.
            run_timeout: Seconds a run may take when its job declares no
                ``timeout``; ``None`` sets no deadline for those runs.
            poll_interval: Seconds between reaper scans.
        """
        super().__init__(poll_interval=poll_interval)
        self._store = store
        self._launcher = launcher
        self._startup_timeout = startup_timeout
        self._heartbeat_timeout = heartbeat_timeout
        self._run_timeout = run_timeout
        # Usage reconciliation rides the reaper's loop (the singleton
        # housekeeping process) roughly hourly.
        self._reconcile_every = max(1, 3600 // max(1, poll_interval))
        self._ticks_since_reconcile = self._reconcile_every  # reconcile on first tick

    def _tick(self) -> None:
        """Scan once and log when anything was reaped."""
        reaped = self._reap()
        if reaped:
            logger.info("Reaped %d overdue run(s)", reaped)

        self._ticks_since_reconcile += 1
        if self._ticks_since_reconcile >= self._reconcile_every:
            self._ticks_since_reconcile = 0
            self._reconcile_usage()

    # -- Internals -------------------------------------------------------------

    def _reconcile_usage(self) -> None:
        """Warn when the usage ledger drifts from the runs table.

        Advisory only — nothing is corrected automatically. Transient
        off-by-ones can appear while runs are completing; drift that
        persists across cycles is a bug in the charging path.
        """
        try:
            drifts = self._store.usage.reconcile()
        except Exception:
            logger.exception("Usage reconciliation failed")
            return
        for drift in drifts:
            logger.warning(
                "Usage ledger drift for org %s (period %s): ledger=%d, runs table=%d",
                drift.org_id,
                drift.period_start,
                drift.ledger,
                drift.recomputed,
            )

    def _reap(self) -> int:
        """Fail every overdue run that has not moved since it was read.

        Returns:
            Number of runs reaped this cycle.
        """
        overdue = self._store.runs.overdue(
            startup_timeout=self._startup_timeout,
            heartbeat_timeout=self._heartbeat_timeout,
            run_timeout=self._run_timeout,
        )
        reaped = 0
        for run, reason in overdue:
            error = self._explain(run.id, reason)
            logger.warning("Reaping run %s: %s", run.id, error)
            try:
                if self._store.runs.reap(run.id, error, heartbeat_at=run.heartbeat_at) is not None:
                    reaped += 1
            except Exception:
                logger.exception("Failed to reap run %s", run.id)
        return reaped

    def _explain(self, run_id: UUID, reason: str) -> str:
        """Append the launcher's diagnosis of a run's workload to the reaper's reason.

        Args:
            run_id: The overdue run.
            reason: Why the reaper fails it.

        Returns:
            The reason, followed by the diagnosis in parentheses when the
            launcher has one.
        """
        if self._launcher is None:
            return reason
        try:
            diagnosis = self._launcher.diagnose(run_id)
        except Exception:
            logger.warning("Could not diagnose run %s", run_id, exc_info=True)
            return reason
        return f"{reason} ({diagnosis})" if diagnosis else reason
