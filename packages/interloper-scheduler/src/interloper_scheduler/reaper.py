"""Reaper: detects terminated runs via the launcher and marks them failed.

The reaper is a single background thread that periodically checks every
``dispatched`` run's authoritative state via
:meth:`~interloper_scheduler.launcher.Launcher.describe_run`:

- ``RUNNING`` → leave alone
- ``SUCCEEDED`` → weird (container said it succeeded but didn't update
  the DB) — mark as failed with a descriptive error
- ``FAILED`` → mark as failed immediately with the launcher's error
- ``NOT_FOUND`` → container is gone without a trace — mark as failed

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
from threading import Event
from typing import TYPE_CHECKING

import interloper as il
from interloper_db import Store, get_engine
from interloper_db.models import Run
from sqlmodel import Session, select

from interloper_scheduler.launcher import RunStatus

if TYPE_CHECKING:
    from interloper_scheduler.launcher import Launcher

logger = logging.getLogger(__name__)


class Reaper:
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
        self._store = store
        self._launcher = launcher
        self._timeout = timeout
        self._poll_interval = poll_interval
        self._stop_event = Event()

    def start(self) -> None:
        """Run the reaper loop until stopped."""
        logger.info(
            "Starting reaper (poll=%ds, timeout=%ds)",
            self._poll_interval,
            self._timeout,
        )

        while not self._stop_event.is_set():
            try:
                reaped = self._reap()
                if reaped:
                    logger.info("Reaped %d dispatched run(s)", reaped)
            except Exception:
                logger.exception("Reaper error")

            if self._stop_event.wait(self._poll_interval):
                break

    def stop(self) -> None:
        """Signal the loop to stop."""
        self._stop_event.set()

    def _reap(self) -> int:
        """Scan dispatched runs and reap any that have terminated.

        Returns:
            Number of runs reaped this cycle.
        """
        now = dt.datetime.now(dt.timezone.utc)
        timeout_cutoff = now - dt.timedelta(seconds=self._timeout)

        with Session(get_engine()) as session:
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
        # NOT_FOUND runs, or anything else.
        if run.created_at and run.created_at < timeout_cutoff:
            self._fail_run(run, f"Run timed out after {self._timeout}s (still 'dispatched')")
            return True

        return False

    def _fail_run(self, run: Run, error: str) -> None:
        """Mark a run as failed and emit a ``RUN_FAILED`` event."""
        assert run.id is not None
        logger.warning("Reaping run %s: %s", run.id, error)

        try:
            event = il.Event(
                type=il.EventType.RUN_FAILED,
                metadata={
                    "run_id": str(run.id),
                    "backfill_id": str(run.backfill_id) if run.backfill_id else None,
                    "error": error,
                },
            )
            self._store.save_event(event, org_id=run.org_id, run_id=run.id)
        except Exception:
            logger.exception("Failed to save RUN_FAILED event for run %s", run.id)

        try:
            self._store.complete_run(run.id, success=False)
        except Exception:
            logger.exception("Failed to mark run %s as failed", run.id)
