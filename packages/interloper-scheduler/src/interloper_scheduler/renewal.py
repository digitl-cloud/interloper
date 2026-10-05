"""Renewal controller: enqueues credential-renewal runs for due connections.

Connections are component rows (``kind='connection'``): whether one is
renewable comes from its catalog definition, opt-out from its (decoded)
config, and due-ness from the machine-owned ``state.next_renewal_at`` — the
same tick / ``SKIP LOCKED`` / stamp-and-enqueue mechanics as the cron
controller, pointed at connections. The controller only schedules: the
renewal itself executes in a run pod like any other run (``Connection``
is an operation, so the executor drives it through the operation
contract), which writes the real next due time; the stamp made here is a
provisional slot that re-arms the connection if that run never completes.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from interloper.catalog.base import Catalog
from interloper_db import Store
from interloper_db.models import Component

from interloper_scheduler.controller import Controller

logger = logging.getLogger(__name__)

#: Provisional slot stamped at enqueue: the run overwrites it on completion,
#: so this only fires when the run vanished without a terminal state.
_PENDING_TTL = timedelta(hours=1)

#: How long an opted-out connection waits before renewal is reconsidered.
_RECHECK_INTERVAL = timedelta(hours=24)


class RenewalController(Controller):
    """Enqueues a renewal run for every renewable, opted-in, due connection.

    Each tick, in one transaction: lock the due rows of renewable catalog
    keys, stamp ``state.next_renewal_at`` (the provisional pending slot), and
    queue a renewal run for each.
    """

    def __init__(
        self,
        catalog: Catalog,
        store: Store | None = None,
        reconcile_interval: int = 60,
        batch_size: int = 50,
    ) -> None:
        """Initialize the renewal controller.

        Args:
            catalog: The catalog renewability is derived from.
            store: The Store. Defaults to the settings-configured one.
            reconcile_interval: Seconds between evaluation cycles.
            batch_size: Number of connections to process per cycle.
        """
        super().__init__(poll_interval=reconcile_interval)
        self._store = store or Store.from_settings()
        self._batch_size = batch_size
        # Renewability is a class property, so the key set is static for the
        # process lifetime — computing it once keeps the tick query narrow
        # (non-renewable connections are never scanned or stamped).
        self._renewable_keys = [key for key, defn in catalog.components.items() if getattr(defn, "renewable", False)]

    def _tick(self) -> None:
        """Process a batch of due connections in a single transaction."""
        if not self._renewable_keys:
            return

        now = datetime.now(timezone.utc)
        with self._store.transaction():
            connections = self._store.components.lock_due(
                "connection", "next_renewal_at", now=now, limit=self._batch_size, keys=self._renewable_keys
            )
            for connection in connections:
                if not self._auto_renew(connection):
                    # Opted out: reconsider later rather than rescan every
                    # tick. Re-enabling the flag takes effect within this
                    # window.
                    self._store.components.stamp_state(connection.id, next_renewal_at=now + _RECHECK_INTERVAL)
                    continue

                self._store.components.stamp_state(connection.id, next_renewal_at=now + _PENDING_TTL)
                # An expired pending slot while the original run is still alive must not
                # queue a second renewal that could rotate a credential out from under it.
                if self._store.runs.has_open(connection.id):
                    continue

                self._store.runs.create(connection.org_id, component_id=connection.id)
                logger.info("Queued renewal for connection '%s' (%s)", connection.name, connection.id)

    def _auto_renew(self, connection: Component) -> bool:
        """Whether the connection's stored config opts into automatic renewal.

        Reading the flag needs the decoded (decrypted) payload; a payload
        that cannot be decoded is left alone — the renewal run would only
        fail at hydration for the same reason.

        Args:
            connection: The connection row whose config is read.

        Returns:
            The stored ``auto_renew`` value, defaulting to True.
        """
        config = self._store.components.read(connection).config
        if config is None:
            logger.warning("Cannot decode config of connection %s; skipping renewal", connection.id)
            return False
        return bool(config.get("auto_renew", True))
