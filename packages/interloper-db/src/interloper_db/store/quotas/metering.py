"""The usage ledger: per-organisation counters for what a period consumed.

``usage`` is the append-only ledger billing reads — one row per
``(org, metric, period)``, holding what has been charged (``used``) and what
dispatch has reserved but not yet settled (``reserved``). Periods are calendar
months in UTC, resolved with the *database* clock so every writer (API,
scheduler, child pods) agrees on the boundary.

The ledger knows nothing about limits: it counts, and :mod:`.definitions`
decides what a count means.
"""

from __future__ import annotations

import datetime as dt
from datetime import datetime, timezone
from uuid import UUID

from interloper.utils import month_start
from sqlalchemy import func, select
from sqlmodel import Session

from interloper_db.models import Run, Usage
from interloper_db.session import dialect_insert

METRIC_SUCCESSFUL_RUNS = "successful_runs"

#: Every metric the ledger accepts a charge under. A consumption quota
#: declares which one it charges, and is rejected at registration otherwise.
METRICS = frozenset({METRIC_SUCCESSFUL_RUNS})


class UsageLedger:
    """The ``usage`` table, scoped to one open session.

    Every method is part of the caller's transaction: the ledger writes but
    never commits, so a charge lands with the state change that caused it or
    not at all. Cheap to construct — bind one wherever a session is open.
    """

    def __init__(self, session: Session) -> None:
        """Bind the ledger to the session its work is done through.

        Args:
            session: Open session the reads and writes join.
        """
        self._session = session

    # -- Periods ---------------------------------------------------------------

    def now(self) -> datetime:
        """The database server's current time, as an aware UTC datetime.

        The DB clock is the single billing clock: completions are written by
        whatever process executes the run (for docker/k8s a child pod), whose
        wall clock is not trusted for month attribution.

        Returns:
            The server time, always tz-aware (UTC is assumed when the dialect
            returns a naive value).
        """
        value = self._session.scalar(select(func.current_timestamp()))
        if isinstance(value, str):  # SQLite returns text
            value = datetime.fromisoformat(value)
        assert value is not None
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    def current_period(self) -> dt.date:
        """The period charges are currently landing in, per the database clock.

        Returns:
            The first day of the current UTC calendar month.
        """
        return month_start(self.now())

    # -- Counters --------------------------------------------------------------

    def committed(self, org_id: UUID, metric: str) -> int:
        """An organisation's committed usage this period: ``used + reserved``.

        Args:
            org_id: Organisation whose ledger row is read.
            metric: Ledger metric to read.

        Returns:
            The committed count, or 0 when the period has no ledger row yet.
        """
        row = self._session.get(Usage, (org_id, metric, self.current_period()))
        return (row.used + row.reserved) if row else 0

    def increment(
        self,
        org_id: UUID,
        metric: str,
        period_start: dt.date,
        *,
        used: int = 0,
        reserved: int = 0,
    ) -> None:
        """Atomically add to a usage row, creating it on first touch.

        Upsert-based so concurrent writers never lose an increment.

        Args:
            org_id: Organisation the counters belong to.
            metric: Ledger metric being charged.
            period_start: First day of the UTC month the counters belong to.
            used: Delta applied to ``used``, defaulting to no change.
            reserved: Delta applied to ``reserved``, defaulting to no change;
                negative releases a reservation.

        Raises:
            ValueError: If ``metric`` is not a ledger metric.
        """
        if metric not in METRICS:
            raise ValueError(f"Unknown usage metric: {metric}")
        table = Usage.__table__  # ty: ignore[unresolved-attribute]
        statement = (
            dialect_insert(self._session)(table)
            .values(org_id=org_id, metric=metric, period_start=period_start, used=used, reserved=reserved)
            .on_conflict_do_update(
                index_elements=["org_id", "metric", "period_start"],
                set_={"used": table.c.used + used, "reserved": table.c.reserved + reserved},
            )
        )
        self._session.execute(statement)  # ty: ignore[deprecated]

    def reserve(self, org_id: UUID, metric: str, limit: int) -> datetime | None:
        """Atomically reserve one unit against the period's ledger row.

        A conditional upsert (``used + reserved < limit``), so concurrent
        reservers can never overshoot.

        Args:
            org_id: Organisation the unit is reserved for.
            metric: Ledger metric the unit is reserved under.
            limit: The effective limit the upsert is conditioned on; a limit of
                zero or less admits nothing.

        Returns:
            The reservation timestamp (DB clock), or None when exhausted.
        """
        if limit <= 0:
            return None
        now = self.now()
        table = Usage.__table__  # ty: ignore[unresolved-attribute]
        statement = (
            dialect_insert(self._session)(table)
            .values(org_id=org_id, metric=metric, period_start=month_start(now), used=0, reserved=1)
            .on_conflict_do_update(
                index_elements=["org_id", "metric", "period_start"],
                set_={"reserved": table.c.reserved + 1},
                where=(table.c.used + table.c.reserved < limit),
            )
        )
        result = self._session.execute(statement)  # ty: ignore[deprecated]
        return now if result.rowcount == 1 else None  # ty: ignore[unresolved-attribute]

    def settle_run(self, db_run: Run, *, success: bool) -> None:
        """Charge or release a completing run's usage.

        A successful run charges ``used`` in the month it completes (DB clock);
        a dispatch-time reservation is released in the month it was taken —
        those can differ across a month boundary.

        Args:
            db_run: The completing run, read for its org and its
                ``quota_reserved_at`` stamp (unset when nothing was reserved).
            success: Whether the run succeeded; only successes are charged.
        """
        reserved_period = month_start(db_run.quota_reserved_at) if db_run.quota_reserved_at else None
        charge_period = self.current_period() if success else None

        if charge_period is not None and charge_period == reserved_period:
            self.increment(db_run.org_id, METRIC_SUCCESSFUL_RUNS, charge_period, used=1, reserved=-1)
            return
        if charge_period is not None:
            self.increment(db_run.org_id, METRIC_SUCCESSFUL_RUNS, charge_period, used=1)
        if reserved_period is not None:
            self.increment(db_run.org_id, METRIC_SUCCESSFUL_RUNS, reserved_period, reserved=-1)
