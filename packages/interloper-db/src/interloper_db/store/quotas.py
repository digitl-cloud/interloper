"""Quota limits and usage metering.

``quotas`` holds per-organisation limits (null falls back to the global
default). ``usage`` is the append-only ledger of per-period counters billing
reads: successful runs are charged into ``used`` inside ``complete_run``'s
transaction, and ``reserved`` holds dispatch-time reservations until they
settle. Periods are calendar months in UTC, resolved with the database clock
so every writer (API, scheduler, child pods) agrees on the boundary.

The ledger is the admission primitive; the ``runs`` table is the audit
source of truth. ``count_successful_runs_by_org`` recomputes the ledger from
it so drift is always visible.
"""

from __future__ import annotations

import datetime as dt
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import func
from sqlalchemy import select as sa_select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlmodel import Session, col, select

from interloper_db.models import Component, Quota, Run, Usage
from interloper_db.store.base import StoreBase

METRIC_SUCCESSFUL_RUNS = "successful_runs"

#: The closed set of valid usage metrics — the column is a free string in the
#: DB, so writers must go through this to keep the ledger free of typo rows.
USAGE_METRICS = frozenset({METRIC_SUCCESSFUL_RUNS})


def month_start(moment: datetime) -> dt.date:
    """The first day of the UTC calendar month a moment falls in.

    Naive values are treated as UTC (SQLite round-trips columns naive).
    """
    if moment.tzinfo is not None:
        moment = moment.astimezone(timezone.utc)
    return dt.date(moment.year, moment.month, 1)


def next_month_start(period_start: dt.date) -> dt.date:
    """The first day of the month after ``period_start``."""
    if period_start.month == 12:
        return dt.date(period_start.year + 1, 1, 1)
    return dt.date(period_start.year, period_start.month + 1, 1)


def db_now(session: Session) -> datetime:
    """The database server's current time, as an aware UTC datetime.

    The DB clock is the single billing clock: completions are written by
    whatever process executes the run (for docker/k8s a child pod), whose
    wall clock is not trusted for month attribution.
    """
    value = session.scalar(sa_select(func.current_timestamp()))
    if isinstance(value, str):  # SQLite returns text
        value = datetime.fromisoformat(value)
    assert value is not None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value


def increment_usage(
    session: Session,
    org_id: UUID,
    metric: str,
    period_start: dt.date,
    *,
    used: int = 0,
    reserved: int = 0,
) -> None:
    """Atomically add to a usage row, creating it on first touch.

    Upsert-based so concurrent writers never lose an increment; part of the
    caller's transaction (the caller commits).
    """
    if metric not in USAGE_METRICS:
        raise ValueError(f"Unknown usage metric: {metric}")
    table = Usage.__table__  # ty: ignore[unresolved-attribute]
    insert_fn = pg_insert if session.get_bind().dialect.name == "postgresql" else sqlite_insert
    statement = (
        insert_fn(table)
        .values(org_id=org_id, metric=metric, period_start=period_start, used=used, reserved=reserved)
        .on_conflict_do_update(
            index_elements=["org_id", "metric", "period_start"],
            set_={"used": table.c.used + used, "reserved": table.c.reserved + reserved},
        )
    )
    session.execute(statement)  # ty: ignore[deprecated]


def settle_run_usage(session: Session, db_run: Run, *, success: bool) -> None:
    """Charge or release a completing run's usage, in the caller's transaction.

    A successful run charges ``used`` in the month it completes (DB clock);
    a dispatch-time reservation is released in the month it was taken —
    those can differ across a month boundary.
    """
    reserved_period = month_start(db_run.quota_reserved_at) if db_run.quota_reserved_at else None
    charge_period = month_start(db_now(session)) if success else None

    if charge_period is not None and charge_period == reserved_period:
        increment_usage(session, db_run.org_id, METRIC_SUCCESSFUL_RUNS, charge_period, used=1, reserved=-1)
        return
    if charge_period is not None:
        increment_usage(session, db_run.org_id, METRIC_SUCCESSFUL_RUNS, charge_period, used=1)
    if reserved_period is not None:
        increment_usage(session, db_run.org_id, METRIC_SUCCESSFUL_RUNS, reserved_period, reserved=-1)


class QuotaMixin(StoreBase):
    """Store methods for quota limits and usage reads."""

    def get_quota(self, org_id: UUID) -> Quota | None:
        """The organisation's quota overrides, or None if none are set."""
        with self._session() as session:
            return session.get(Quota, org_id)

    def list_quotas(self) -> list[Quota]:
        """All per-organisation quota override rows."""
        with self._session() as session:
            return list(session.exec(select(Quota)).all())

    def list_usage(self, *, period_start: dt.date | None = None, org_id: UUID | None = None) -> list[Usage]:
        """Usage ledger rows, optionally filtered by period and organisation."""
        with self._session() as session:
            statement = select(Usage)
            if period_start is not None:
                statement = statement.where(Usage.period_start == period_start)
            if org_id is not None:
                statement = statement.where(Usage.org_id == org_id)
            return list(session.exec(statement).all())

    def current_period_start(self) -> dt.date:
        """The current UTC calendar month, per the database clock."""
        with self._session() as session:
            return month_start(db_now(session))

    def count_sources_by_org(self) -> dict[UUID, int]:
        """Current number of sources per organisation."""
        with self._session() as session:
            rows = session.exec(
                select(Component.org_id, func.count())
                .where(Component.kind == "source")
                .group_by(Component.org_id)  # ty: ignore[invalid-argument-type]
            ).all()
            return dict(rows)

    def max_assets_per_source_by_org(self) -> dict[UUID, int]:
        """The largest child-asset count of any single source, per organisation."""
        with self._session() as session:
            per_source = (
                sa_select(col(Component.org_id).label("org_id"), func.count().label("n"))
                .where(col(Component.kind) == "asset", col(Component.parent_id).is_not(None))
                .group_by(col(Component.org_id), col(Component.parent_id))
            ).subquery()
            rows = session.execute(  # ty: ignore[deprecated]
                sa_select(per_source.c.org_id, func.max(per_source.c.n)).group_by(per_source.c.org_id)
            ).all()
            return {org_id: count for org_id, count in rows}

    def count_successful_runs_by_org(self, period_start: dt.date) -> dict[UUID, int]:
        """Recompute the ledger's truth: successful runs completed in the period.

        Counts on ``completed_at`` — the nearest column to the charge moment.
        Reconciliation compares this against ``usage.used``.
        """
        lower = datetime.combine(period_start, dt.time.min, tzinfo=timezone.utc)
        upper = datetime.combine(next_month_start(period_start), dt.time.min, tzinfo=timezone.utc)
        with self._session() as session:
            rows = session.exec(
                select(Run.org_id, func.count())
                .where(Run.status == "success", col(Run.completed_at) >= lower, col(Run.completed_at) < upper)
                .group_by(Run.org_id)  # ty: ignore[invalid-argument-type]
            ).all()
            return dict(rows)
