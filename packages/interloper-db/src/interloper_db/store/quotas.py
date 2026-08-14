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

Quotas themselves are declared in the :data:`QUOTAS` registry — one
:class:`QuotaDefinition` per key — and enforced through the store's
:class:`QuotaService` (``store.quotas``), which owns limit resolution and
the gates. Metering stays in module functions: the ledger never depends on
limits.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from interloper.errors import QuotaExceededError
from interloper.registry import Registry
from sqlalchemy import func
from sqlalchemy import select as sa_select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlmodel import Session, col, select

from interloper_db.models import Component, Quota, Run, Usage
from interloper_db.store.base import StoreBase

METRIC_SUCCESSFUL_RUNS = "successful_runs"

QUOTA_MAX_SOURCES = "max_sources"
QUOTA_MAX_ASSETS_PER_SOURCE = "max_assets_per_source"
QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH = "max_successful_runs_per_month"


@dataclass(frozen=True)
class QuotaDefinition:
    """One per-organisation quota: its key and how usage is measured.

    Capacity quotas carry a ``count`` of current usage (a live query —
    they are never metered); consumption quotas carry the ``metric`` their
    usage is charged under in the ledger.
    """

    key: str
    #: Ledger metric for consumption quotas; None for capacity quotas.
    metric: str | None = None
    #: Live usage count for capacity quotas; None when the gate supplies
    #: the prospective usage itself (declarative checks).
    count: Callable[[Session, UUID], int] | None = None


#: Every quota, by key. Adding one is a registration plus its enforcement
#: site — limits are stored one row per key, so there is no schema change.
#: Code-registered (no entry-point group): enforcement is welded into the
#: store, so quotas are not a plugin surface. Instance defaults live as
#: same-named optional fields on ``QuotaSettings``.
QUOTAS: Registry[QuotaDefinition] = Registry()


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
    if all(definition.metric != metric for definition in QUOTAS.values()):
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


def _count_sources(session: Session, org_id: UUID) -> int:
    """Current number of sources — the usage side of ``max_sources``."""
    return session.exec(
        select(func.count())
        .select_from(Component)
        .where(col(Component.org_id) == org_id, col(Component.kind) == "source")
    ).one()


class QuotaService:
    """Limit resolution and enforcement gates over the :data:`QUOTAS` registry.

    Constructed by the store (exposed as ``store.quotas``) with a defaults
    provider, read per call so reconfiguration is always visible. All gates
    short-circuit without touching the database when neither the
    organisation nor the defaults set a limit, so unconfigured instances
    pay nothing. Capacity gates serialize on the ``(org, key)`` quota-row
    lock — the count-then-insert race is the reason a plain count in
    application code is not enough. The run quota's authoritative gate is
    the atomic dispatch-time reservation in :meth:`try_reserve_run`; the
    creation-time checks are advisory fail-fasts.
    """

    def __init__(self, defaults: Callable[[], Any]) -> None:
        """Initialize the service.

        Args:
            defaults: Zero-arg provider of the QuotaSettings-shaped global
                defaults (or None = everything unlimited).
        """
        self._defaults = defaults

    def effective(self, session: Session, org_id: UUID, key: str, *, lock: bool = False) -> int | None:
        """Resolve a limit: org override wins over the global default; None = unlimited.

        With ``lock`` the ``(org, key)`` row is upserted (null-limit lock
        anchor) and held ``FOR UPDATE`` until the caller's transaction ends,
        serializing checks per organisation *and* key so independent quotas
        never block each other.

        Raises:
            KeyError: If the key is not a registered quota.
        """
        QUOTAS[key]  # loud failure on unregistered keys  # noqa: B018
        if lock:
            table = Quota.__table__  # ty: ignore[unresolved-attribute]
            statement = (
                _insert_fn(session)(table)
                .values(org_id=org_id, key=key)
                .on_conflict_do_nothing(index_elements=["org_id", "key"])
            )
            session.execute(statement)  # ty: ignore[deprecated]
            override = session.exec(
                select(Quota).where(Quota.org_id == org_id, Quota.key == key).with_for_update()
            ).first()
        else:
            override = session.get(Quota, (org_id, key))
        value = override.limit if override else None
        if value is None:
            value = getattr(self._defaults(), key, None)
        return value

    def check_source(self, session: Session, org_id: UUID) -> None:
        """Reject creating a source when the organisation is at its source limit.

        Part of the caller's transaction; call before inserting the new source.
        """
        if self.effective(session, org_id, QUOTA_MAX_SOURCES) is None:
            return
        limit = self.effective(session, org_id, QUOTA_MAX_SOURCES, lock=True)
        if limit is None:
            return
        count = QUOTAS[QUOTA_MAX_SOURCES].count
        assert count is not None  # capacity quota, registered with its counter
        used = count(session, org_id)
        if used >= limit:
            raise QuotaExceededError(
                f"Organisation is at its source limit ({used}/{limit})",
                quota=QUOTA_MAX_SOURCES,
                limit=limit,
                used=used,
            )

    def check_assets(self, session: Session, db_source: Component, asset_count: int) -> None:
        """Reject a source child set larger than the assets-per-source limit.

        ``asset_count`` is the size of the *desired* final set, so the check
        is declarative — no counting race regardless of what the set is today.
        """
        if self.effective(session, db_source.org_id, QUOTA_MAX_ASSETS_PER_SOURCE) is None:
            return
        limit = self.effective(session, db_source.org_id, QUOTA_MAX_ASSETS_PER_SOURCE, lock=True)
        if limit is None or asset_count <= limit:
            return
        raise QuotaExceededError(
            f"Source '{db_source.name or db_source.key}' would have {asset_count} assets, "
            f"exceeding the limit of {limit}",
            quota=QUOTA_MAX_ASSETS_PER_SOURCE,
            limit=limit,
            used=asset_count,
        )

    def run_status(self, session: Session, org_id: UUID) -> tuple[int, int | None]:
        """The org's committed run usage this period: ``(used + reserved, limit)``.

        Limit None means unlimited (and the ledger is not read at all).
        """
        limit = self.effective(session, org_id, QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH)
        if limit is None:
            return 0, None
        period_start = month_start(db_now(session))
        row = session.get(Usage, (org_id, METRIC_SUCCESSFUL_RUNS, period_start))
        committed = (row.used + row.reserved) if row else 0
        return committed, limit

    def check_run_admission(self, session: Session, org_id: UUID, *, source: str = "run") -> None:
        """Advisory creation-time gate: reject new runs once the quota is exhausted.

        Fail-fast only — runs admitted here can still be denied at dispatch
        by :meth:`try_reserve_run`, the authoritative gate.
        """
        committed, limit = self.run_status(session, org_id)
        if limit is not None and committed >= limit:
            raise QuotaExceededError(
                f"Cannot queue {source}: the monthly successful-run quota is exhausted ({committed}/{limit})",
                quota=QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH,
                limit=limit,
                used=committed,
            )

    def try_reserve_run(self, session: Session, db_run: Run) -> bool:
        """Atomically reserve a run-quota slot at dispatch time.

        The reservation is a conditional upsert (``used + reserved < limit``),
        so concurrent claimers can never overshoot; on success the run is
        stamped with ``quota_reserved_at`` so settlement releases the right
        period. Unlimited orgs are admitted without touching the ledger.

        Returns:
            True if the run may dispatch, False when the quota is exhausted.
        """
        limit = self.effective(session, db_run.org_id, QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH)
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


QUOTAS.register(QUOTA_MAX_SOURCES, QuotaDefinition(key=QUOTA_MAX_SOURCES, count=_count_sources))
QUOTAS.register(QUOTA_MAX_ASSETS_PER_SOURCE, QuotaDefinition(key=QUOTA_MAX_ASSETS_PER_SOURCE))
QUOTAS.register(
    QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH,
    QuotaDefinition(key=QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH, metric=METRIC_SUCCESSFUL_RUNS),
)


class QuotaMixin(StoreBase):
    """Store methods for quota limits and usage reads."""

    def get_quota_overrides(self, org_id: UUID) -> dict[str, int]:
        """The organisation's set overrides as ``{key: limit}`` (null rows excluded)."""
        with self._session() as session:
            rows = session.exec(select(Quota).where(Quota.org_id == org_id)).all()
            return {row.key: row.limit for row in rows if row.limit is not None}

    def list_quota_overrides(self) -> dict[UUID, dict[str, int]]:
        """Every organisation's set overrides, keyed by org id."""
        with self._session() as session:
            rows = session.exec(select(Quota)).all()
            overrides: dict[UUID, dict[str, int]] = {}
            for row in rows:
                if row.limit is not None:
                    overrides.setdefault(row.org_id, {})[row.key] = row.limit
            return overrides

    def set_quota(self, org_id: UUID, limits: dict[str, int | None]) -> dict[str, int]:
        """Set an organisation's quota overrides; only the given keys change.

        ``None`` clears a key so it falls back to the global default (the
        row is kept as a null-limit lock anchor).

        Returns:
            The organisation's overrides after the update.

        Raises:
            ValueError: On an unknown quota key or a negative value.
        """
        if unknown := {key for key in limits if key not in QUOTAS}:
            raise ValueError(f"Unknown quota limit(s): {sorted(unknown)}")
        if negative := {key for key, value in limits.items() if value is not None and value < 0}:
            raise ValueError(f"Quota limit(s) must be >= 0: {sorted(negative)}")
        table = Quota.__table__  # ty: ignore[unresolved-attribute]
        with self._session() as session:
            for key, value in limits.items():
                statement = (
                    _insert_fn(session)(table)
                    .values(org_id=org_id, key=key, limit=value)
                    .on_conflict_do_update(index_elements=["org_id", "key"], set_={"limit": value})
                )
                session.execute(statement)  # ty: ignore[deprecated]
            session.commit()
        return self.get_quota_overrides(org_id)

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
