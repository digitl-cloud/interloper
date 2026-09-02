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

from interloper import Connection
from interloper.catalog.base import Catalog
from interloper_db import Store
from interloper_db.models import Component, Run
from sqlalchemy import or_
from sqlmodel import Session, col, select

from interloper_scheduler.controller import Controller

logger = logging.getLogger(__name__)

#: Provisional slot stamped at enqueue: the run overwrites it on completion,
#: so this only fires when the run vanished without a terminal state.
_PENDING_TTL = timedelta(hours=1)

#: How long an opted-out connection waits before renewal is reconsidered.
_RECHECK_INTERVAL = timedelta(hours=24)


class RenewalController(Controller):
    """Enqueues a renewal run for every renewable, opted-in, due connection.

    Each tick:
    1. ``SELECT FOR UPDATE SKIP LOCKED`` due rows of renewable catalog keys
    2. stamp ``state.next_renewal_at`` (provisional pending slot)
    3. ``INSERT run`` with ``status='queued'``
    4. ``COMMIT`` (release locks)
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
        self._renewable_keys = [
            key for key, defn in catalog.components.items() if getattr(defn, "renewable", False)
        ]

    def _tick(self) -> None:
        """Process a batch of due connections in a single transaction."""
        if not self._renewable_keys:
            return

        with Session(self._store.engine) as session:
            now = datetime.now(timezone.utc)

            next_renewal_at = Component.state["next_renewal_at"].as_string()  # ty: ignore[not-subscriptable]
            statement = (
                select(Component)
                .where(Component.kind == "connection")
                .where(col(Component.key).in_(self._renewable_keys))
                .where(or_(next_renewal_at <= now.isoformat(), next_renewal_at.is_(None)))
                .order_by(next_renewal_at.asc().nulls_first())
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )

            connections = session.exec(statement).all()
            if not connections:
                return

            for connection in connections:
                if not self._auto_renew(connection):
                    # Opted out: reconsider later rather than rescan every
                    # tick. Re-enabling the flag takes effect within this
                    # window.
                    self._set_state(session, connection, next_renewal_at=now + _RECHECK_INTERVAL)
                    continue

                self._set_state(session, connection, next_renewal_at=now + _PENDING_TTL)
                if self._has_open_run(session, connection):
                    continue

                session.add(
                    Run(
                        component_id=connection.id,
                        org_id=connection.org_id,
                        status="queued",
                        billable=Connection.billable,
                    )
                )
                logger.info("Queued renewal for connection '%s' (%s)", connection.name, connection.id)

            session.commit()

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
        try:
            return bool(self._store.components.decode_config(connection).get("auto_renew", True))
        except Exception:  # noqa: BLE001 — any decode failure means the run would fail too
            logger.warning("Cannot decode config of connection %s; skipping renewal", connection.id)
            return False

    @staticmethod
    def _has_open_run(session: Session, connection: Component) -> bool:
        """Whether a run for this connection is already in flight.

        The provisional stamp prevents re-enqueueing in the normal flow;
        this guards the recovery path (a pending slot that expired while
        the original run is still alive), where a second concurrent renewal
        could rotate a credential out from under the first.

        Args:
            session: Open session the check is made through.
            connection: The connection row being considered for renewal.

        Returns:
            True when a queued/dispatched/running run exists.
        """
        statement = (
            select(Run.id)
            .where(Run.component_id == connection.id)
            .where(col(Run.status).in_(["queued", "dispatched", "running"]))
            .limit(1)
        )
        return session.exec(statement).first() is not None

    @staticmethod
    def _set_state(session: Session, connection: Component, **timestamps: datetime) -> None:
        """Merge timestamps into the connection's machine-owned state.

        Args:
            session: Open session the write joins.
            connection: The connection row to stamp.
            **timestamps: State fields to set, merged over the existing payload.
        """
        connection.stamp_state(**timestamps)
        session.add(connection)
        session.flush()
