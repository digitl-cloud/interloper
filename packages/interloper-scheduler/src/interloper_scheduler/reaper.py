"""Reaper: detects terminated runs via the launcher and marks them failed.

The reaper is a single background thread that periodically checks every
``dispatched`` run's authoritative state via
:meth:`~interloper_scheduler.launcher.Launcher.describe_run`:

- ``RUNNING`` → leave alone
- ``SUCCEEDED`` → weird (container said it succeeded but didn't update
  the DB) — mark as failed with a descriptive error
- ``FAILED`` → mark as failed immediately with the launcher's error
- ``NOT_FOUND`` → the launcher can't see it (gone, or not visible yet)
  — leave it to the ``timeout`` fallback below

A ``timeout`` fallback catches runs the launcher can't see (e.g. when
the launcher itself doesn't implement ``describe_run``, or the
infrastructure API is unreachable).  Runs older than ``timeout``
seconds in ``dispatched`` status are reaped regardless.

The pattern scales flat: one SQL query per poll cycle, plus one
launcher API call per dispatched run (which K8s/Docker can serve
from their local daemon cheaply).
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import TYPE_CHECKING

import interloper as il
from interloper_db import Store
from interloper_db.models import Component, Run
from sqlmodel import Session, select

from interloper_scheduler.controller import Controller
from interloper_scheduler.executor import run_event_metadata
from interloper_scheduler.launcher import RunStatus

if TYPE_CHECKING:
    from interloper_scheduler.launcher import Launcher

logger = logging.getLogger(__name__)


class Reaper(Controller):
    """Periodically reconciles dispatched runs with the launcher's truth.

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
        timeout: int = 600,
        poll_interval: int = 10,
    ) -> None:
        """Initialize the reaper.

        Args:
            store: Store used to persist the failure event and update
                the run status.
            launcher: Optional launcher consulted each poll cycle for
                authoritative run state.  Launchers without
                introspection (e.g. in-process) fall back to timeout.
            timeout: Fallback: seconds after which a ``dispatched`` run
                is reaped regardless of what the launcher says.
            poll_interval: Seconds between reaper scans.
        """
        super().__init__(poll_interval=poll_interval)
        self._store = store
        self._launcher = launcher
        self._timeout = timeout
        # Usage reconciliation rides the reaper's loop (the singleton
        # housekeeping process) roughly hourly.
        self._reconcile_every = max(1, 3600 // max(1, poll_interval))
        self._ticks_since_reconcile = self._reconcile_every  # reconcile on first tick

    def _tick(self) -> None:
        """Scan once and log when anything was reaped."""
        reaped = self._reap()
        if reaped:
            logger.info("Reaped %d dispatched run(s)", reaped)

        self._ticks_since_reconcile += 1
        if self._ticks_since_reconcile >= self._reconcile_every:
            self._ticks_since_reconcile = 0
            self._reconcile_usage()

    def _reconcile_usage(self) -> None:
        """Warn when the usage ledger drifts from the runs table.

        Advisory only — nothing is corrected automatically. Transient
        off-by-ones can appear while runs are completing; drift that
        persists across cycles is a bug in the charging path.
        """
        try:
            drifts = self._store.quotas.reconcile_usage()
        except Exception:
            logger.exception("Usage reconciliation failed")
            return
        for drift in drifts:
            logger.warning(
                "Usage ledger drift for org %s (period %s): ledger=%d, runs table=%d",
                drift["org_id"],
                drift["period_start"],
                drift["ledger"],
                drift["recomputed"],
            )

    def _reap(self) -> int:
        """Scan dispatched runs and reap any that have terminated.

        Returns:
            Number of runs reaped this cycle.
        """
        now = dt.datetime.now(dt.timezone.utc)
        timeout_cutoff = now - dt.timedelta(seconds=self._timeout)

        with Session(self._store.engine) as session:
            dispatched_runs = list(session.exec(select(Run).where(Run.status == "dispatched")).all())

        reaped = 0
        for run in dispatched_runs:
            if self._reap_run(run, now, timeout_cutoff):
                reaped += 1
        return reaped

    def _reap_run(self, run: Run, now: dt.datetime, timeout_cutoff: dt.datetime) -> bool:
        """Decide whether to reap a single run and do so if needed.

        Returns:
            ``True`` if the run was reaped.
        """
        assert run.id is not None

        # 1. Authoritative launcher state (preferred)
        state = None
        if self._launcher is not None:
            try:
                state = self._launcher.describe_run(run.id)
            except Exception:
                logger.exception("Failed to describe run %s", run.id)

        if state is not None:
            if state.status == RunStatus.RUNNING:
                return False  # Trust the launcher — still alive

            if state.status == RunStatus.SUCCEEDED:
                error = "Run container reported SUCCEEDED but never updated the DB. Possible connectivity issue."
                self._fail_run(run, error)
                return True

            if state.status == RunStatus.FAILED:
                error = state.error or "Run failed (no error reported by launcher)"
                self._fail_run(run, error)
                return True

            if state.status == RunStatus.NOT_FOUND:
                pass

        # 2. Timeout fallback — for launchers without introspection,
        # NOT_FOUND runs, or anything else. Naive timestamps (SQLite test
        # databases drop the offset) are treated as UTC.
        created_at = run.created_at
        if created_at is not None and created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=dt.timezone.utc)
        if created_at and created_at < timeout_cutoff:
            self._fail_run(run, f"Run timed out after {self._timeout}s (still 'dispatched')")
            return True

        return False

    def _fail_run(self, run: Run, error: str) -> None:
        """Mark a run as failed and emit a ``RUN_FAILED`` event."""
        assert run.id is not None
        logger.warning("Reaping run %s: %s", run.id, error)

        try:
            target = None
            if run.component_id:
                with Session(self._store.engine) as session:
                    target = session.get(Component, run.component_id)
            event = il.Event(
                type=il.EventType.RUN_FAILED,
                metadata={**run_event_metadata(run, target), "error": error},
            )
            self._store.runs.save_event(event, org_id=run.org_id, run_id=run.id)
        except Exception:
            logger.exception("Failed to save RUN_FAILED event for run %s", run.id)

        try:
            self._store.runs.complete(run.id, success=False)
        except Exception:
            logger.exception("Failed to mark run %s as failed", run.id)
