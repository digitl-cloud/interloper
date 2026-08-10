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
from typing import Any
from uuid import UUID

from interloper.errors import QuotaExceededError
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


def _insert_fn(session: Session) -> Any:
    """The dialect's upsert-capable insert constructor."""
    return pg_insert if session.get_bind().dialect.name == "postgresql" else sqlite_insert


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
    statement = (
        _insert_fn(session)(table)
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


# -- Enforcement ----------------------------------------------------------------
#
# All checks short-circuit without touching the database when neither the
# organisation nor the defaults set a limit, so unconfigured instances pay
# nothing. Capacity checks (sources, assets per source) serialize on the
# org's quotas row via SELECT FOR UPDATE — the count-then-insert race is
# the reason a plain count in application code is not enough. The run
# quota's authoritative gate is the atomic dispatch-time reservation in
# ``try_reserve_run``; the creation-time checks are advisory fail-fasts.


def _effective_limit(session: Session, org_id: UUID, field: str, defaults: Any) -> int | None:
    """Resolve a limit: org override wins over the global default; None = unlimited."""
    override = session.get(Quota, org_id)
    value = getattr(override, field, None) if override else None
    if value is None:
        value = getattr(defaults, field, None)
    return value


def _locked_effective_limit(session: Session, org_id: UUID, field: str, defaults: Any) -> int | None:
    """Resolve a limit while holding the org's quota-row lock.

    Upserts the (all-null) row first so there is always something to lock;
    the lock is released with the caller's transaction and serializes
    capacity checks per organisation.
    """
    table = Quota.__table__  # ty: ignore[unresolved-attribute]
    statement = _insert_fn(session)(table).values(org_id=org_id).on_conflict_do_nothing(index_elements=["org_id"])
    session.execute(statement)  # ty: ignore[deprecated]
    override = session.exec(select(Quota).where(Quota.org_id == org_id).with_for_update()).first()
    value = getattr(override, field, None) if override else None
    if value is None:
        value = getattr(defaults, field, None)
    return value


def check_source_quota(session: Session, org_id: UUID, defaults: Any) -> None:
    """Reject creating a source when the organisation is at its source limit.

    Part of the caller's transaction; call before inserting the new source.
    """
    if _effective_limit(session, org_id, "max_sources", defaults) is None:
        return
    limit = _locked_effective_limit(session, org_id, "max_sources", defaults)
    if limit is None:
        return
    used = session.exec(
        select(func.count())
        .select_from(Component)
        .where(col(Component.org_id) == org_id, col(Component.kind) == "source")
    ).one()
    if used >= limit:
        raise QuotaExceededError(
            f"Organisation is at its source limit ({used}/{limit}); delete a source or raise the quota",
            quota="max_sources",
            limit=limit,
            used=used,
        )


def check_asset_quota(session: Session, db_source: Component, asset_count: int, defaults: Any) -> None:
    """Reject a source child set larger than the assets-per-source limit.

    ``asset_count`` is the size of the *desired* final set, so the check is
    declarative — no counting race regardless of what the set is today.
    """
    if _effective_limit(session, db_source.org_id, "max_assets_per_source", defaults) is None:
        return
    limit = _locked_effective_limit(session, db_source.org_id, "max_assets_per_source", defaults)
    if limit is None or asset_count <= limit:
        return
    raise QuotaExceededError(
        f"Source '{db_source.name or db_source.key}' would have {asset_count} assets, "
        f"exceeding the limit of {limit}",
        quota="max_assets_per_source",
        limit=limit,
        used=asset_count,
    )


def run_quota_status(session: Session, org_id: UUID, defaults: Any) -> tuple[int, int | None]:
    """The org's committed run usage this period: ``(used + reserved, limit)``.

    Limit None means unlimited (and the ledger is not read at all).
    """
    limit = _effective_limit(session, org_id, "max_successful_runs_per_month", defaults)
    if limit is None:
        return 0, None
    period_start = month_start(db_now(session))
    row = session.get(Usage, (org_id, METRIC_SUCCESSFUL_RUNS, period_start))
    committed = (row.used + row.reserved) if row else 0
    return committed, limit


def check_run_quota(session: Session, org_id: UUID, defaults: Any, *, source: str = "run") -> None:
    """Advisory creation-time gate: reject new runs once the quota is exhausted.

    Fail-fast only — runs admitted here can still be denied at dispatch by
    ``try_reserve_run``, the authoritative gate.
    """
    committed, limit = run_quota_status(session, org_id, defaults)
    if limit is not None and committed >= limit:
        raise QuotaExceededError(
            f"Cannot queue {source}: the monthly successful-run quota is exhausted ({committed}/{limit})",
            quota="max_successful_runs_per_month",
            limit=limit,
            used=committed,
        )


def try_reserve_run(session: Session, db_run: Run, defaults: Any) -> bool:
    """Atomically reserve a run-quota slot at dispatch time.

    The reservation is a conditional upsert (``used + reserved < limit``),
    so concurrent claimers can never overshoot; on success the run is
    stamped with ``quota_reserved_at`` so settlement releases the right
    period. Unlimited orgs are admitted without touching the ledger.

    Returns:
        True if the run may dispatch, False when the quota is exhausted.
    """
    limit = _effective_limit(session, db_run.org_id, "max_successful_runs_per_month", defaults)
    if limit is None:
        return True
    if limit <= 0:
        return False
    now = db_now(session)
    period_start = month_start(now)
    table = Usage.__table__  # ty: ignore[unresolved-attribute]
    statement = (
        _insert_fn(session)(table)
        .values(org_id=db_run.org_id, metric=METRIC_SUCCESSFUL_RUNS, period_start=period_start, used=0, reserved=1)
        .on_conflict_do_update(
            index_elements=["org_id", "metric", "period_start"],
            set_={"reserved": table.c.reserved + 1},
            where=(table.c.used + table.c.reserved < limit),
        )
    )
    result = session.execute(statement)  # ty: ignore[deprecated]
    if result.rowcount != 1:  # ty: ignore[unresolved-attribute]
        return False
    db_run.quota_reserved_at = now
    session.add(db_run)
    return True


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

    def reconcile_usage(self) -> list[dict[str, Any]]:
        """Compare the current period's ledger against the runs table.

        Returns one entry per organisation whose ``usage.used`` differs from
        the recomputed successful-run count. Both sides move in the same
        transaction on completion, so persistent drift is a bug signal;
        transient off-by-ones are possible (the two reads are separate
        queries, and charge months are DB-clock while ``completed_at`` is
        the executor's clock at the boundary).
        """
        period_start = self.current_period_start()
        recomputed = self.count_successful_runs_by_org(period_start)
        ledger = {
            row.org_id: row.used
            for row in self.list_usage(period_start=period_start)
            if row.metric == METRIC_SUCCESSFUL_RUNS
        }
        return [
            {
                "org_id": org_id,
                "period_start": period_start,
                "ledger": ledger.get(org_id, 0),
                "recomputed": recomputed.get(org_id, 0),
            }
            for org_id in sorted(set(recomputed) | set(ledger), key=str)
            if recomputed.get(org_id, 0) != ledger.get(org_id, 0)
        ]
