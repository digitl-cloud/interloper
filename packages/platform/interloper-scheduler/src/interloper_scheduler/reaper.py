"""Reaper: fails the runs that will never end on their own.

A running run renews its heartbeat with every batch of events it reports
(:meth:`~interloper_db.store.runs.RunStore.ingest`), so the reaper needs no
launcher to know it is alive. Each poll it asks the
store which dispatched and running runs are overdue
(:meth:`~interloper_db.store.runs.RunStore.overdue`):

- a dispatched run whose pod has not started within ``startup_timeout``;
- a running run whose heartbeat has been silent for ``heartbeat_timeout``;
- a running run past its deadline: its job's ``timeout``, else ``run_timeout``.

Each is failed with its reason, the launcher's diagnosis of its workload
appended when it has one (an OOM kill, an exit code), through
:meth:`~interloper_db.store.runs.RunStore.reap`, which leaves alone a run that
started or renewed its heartbeat since it was read. A failed run's pod, if
it is still alive, learns from its next batch, delivers what it holds and
stops.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import UUID

from interloper.telemetry.tracer import meter
from interloper_db import Store
from opentelemetry.metrics import CallbackOptions, Observation

from interloper_scheduler.controller import Controller

if TYPE_CHECKING:
    from interloper_scheduler.launcher import Launcher

logger = logging.getLogger(__name__)

#: Ledger minus runs table for every organisation reconciled since startup,
#: keyed by organisation id. An organisation that recovered keeps reporting
#: zero, so the gauge never freezes on a stale drift.
_drift: dict[str, int] = {}
_gauge_registered = False


def _observe_drift(_options: CallbackOptions) -> list[Observation]:
    """Report each organisation's usage ledger drift, as the gauge callback.

    Args:
        _options: Collection options from the OTel SDK, unused.

    Returns:
        One observation per organisation reconciled since startup.
    """
    return [Observation(drift, {"org_id": org_id}) for org_id, drift in _drift.items()]


def _register_drift_gauge() -> None:
    """Register the usage drift gauge once per process."""
    global _gauge_registered
    if _gauge_registered:
        return
    _gauge_registered = True
    meter().create_observable_gauge(
        "interloper.usage.drift",
        callbacks=[_observe_drift],
        unit="{run}",
        description="Successful runs the usage ledger charged minus the runs table's count, per organisation",
    )


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
        oauth_client_unused_for: timedelta = timedelta(hours=24),
        oauth_client_ended_for: timedelta = timedelta(days=30),
        credential_retention: timedelta = timedelta(days=30),
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
            oauth_client_unused_for: How long an OAuth client that registered
                itself, or whose metadata document was read, may go without
                obtaining a grant before it is deleted.
            oauth_client_ended_for: How long such a client is kept after its
                last refresh token expired.
            credential_retention: How long a token, authorization code or
                refresh-token family is kept once it expired or was revoked.
        """
        super().__init__(poll_interval=poll_interval)
        self._store = store
        self._launcher = launcher
        self._reaped = meter().create_counter(
            "interloper.runs.reaped", unit="{run}", description="Runs the reaper failed, by the rule that caught them"
        )
        _register_drift_gauge()
        self._startup_timeout = startup_timeout
        self._heartbeat_timeout = heartbeat_timeout
        self._run_timeout = run_timeout
        self._oauth_client_unused_for = oauth_client_unused_for
        self._oauth_client_ended_for = oauth_client_ended_for
        self._credential_retention = credential_retention
        # Usage reconciliation and OAuth housekeeping ride the reaper's
        # loop (the singleton housekeeping process) roughly hourly.
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
            self._prune_oauth_clients()
            self._purge_credentials()

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
        _drift.update(dict.fromkeys(_drift, 0))
        _drift.update({str(drift.org_id): drift.ledger - drift.recomputed for drift in drifts})
        for drift in drifts:
            logger.warning(
                "Usage ledger drift for org %s (period %s): ledger=%d, runs table=%d",
                drift.org_id,
                drift.period_start,
                drift.ledger,
                drift.recomputed,
            )

    def _prune_oauth_clients(self) -> None:
        """Delete the OAuth clients that registered themselves, or came from a metadata document, and nobody uses.

        Registration is open to anyone, and so is naming a metadata document,
        so without this every abandoned client would stay forever.
        """
        try:
            deleted = self._store.oauth.prune_clients(
                unused_for=self._oauth_client_unused_for, ended_for=self._oauth_client_ended_for
            )
        except Exception:
            logger.exception("Pruning OAuth clients failed")
            return
        if deleted:
            logger.info("Deleted %d unused OAuth client(s)", len(deleted))

    def _purge_credentials(self) -> None:
        """Delete the tokens, codes and refresh tokens that ended longer ago than the retention window.

        They keep per-user records long after they stop being usable, so
        nothing should hold them past what an incident investigation needs.
        """
        try:
            deleted = self._store.tokens.purge(ended_for=self._credential_retention)
            deleted += self._store.oauth.purge(ended_for=self._credential_retention)
        except Exception:
            logger.exception("Purging ended credentials failed")
            return
        if deleted:
            logger.info("Purged %d ended credential(s)", deleted)

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
        for run, verdict in overdue:
            error = self._explain(run.id, verdict.reason)
            logger.warning("Reaping run %s: %s", run.id, error)
            try:
                if self._store.runs.reap(run.id, error, heartbeat_at=run.heartbeat_at) is not None:
                    reaped += 1
                    self._reaped.add(1, {"reason": verdict.kind})
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
