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

import abc
import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, ClassVar
from uuid import UUID

from interloper.errors import QuotaExceededError
from interloper.registry import Registry
from sqlalchemy import Engine, func
from sqlalchemy import select as sa_select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlmodel import Session, col, select

from interloper_db.models import Component, Quota, Run, Usage
from interloper_db.session import commit, session_scope

METRIC_SUCCESSFUL_RUNS = "successful_runs"

QUOTA_MAX_SOURCES = "max_sources"
QUOTA_MAX_ASSETS_PER_SOURCE = "max_assets_per_source"
QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH = "max_successful_runs_per_month"
QUOTA_MAX_BACKFILL_PARTITIONS = "max_backfill_partitions"


# -- Definition ----------------------------------------------------------------


@dataclass(frozen=True)
class QuotaDefinition(abc.ABC):
    """One per-organisation quota: its key, label, and check semantics.

    Subclasses own how usage is measured and compared; the
    :class:`QuotaService` only resolves the effective limit and delegates.
    ``subject`` is caller-supplied context interpolated into the rejection
    message — the part only the call site knows (an entity label, the
    operation being attempted).

    Subclasses that measure existing state set ``requires_lock`` so the limit
    is resolved under the ``(org, key)`` row lock. Consumption checks leave it
    false: their authoritative gate is the atomic ledger reservation, not the
    check.
    """

    key: str
    label: str
    message: Callable[[int, int, str | None], str]

    requires_lock: ClassVar[bool] = False

    def __post_init__(self) -> None:
        """Validate the identity every gate and rejection message depends on.

        Raises:
            ValueError: If the key or the label is empty.
        """
        if not self.key or not self.label:
            raise ValueError("A quota definition needs a key and a label")

    @abc.abstractmethod
    def check(
        self,
        session: Session,
        org_id: UUID,
        limit: int,
        *,
        used: int | None = None,
        subject: str | None = None,
    ) -> None:
        """Raise :class:`QuotaExceededError` when the limit rejects the operation.

        Args:
            session: Open session the work is done through.
            org_id: Organisation the quota is enforced for.
            limit: The effective limit, already resolved by the caller.
            used: Usage stated by the call site, or None to let the definition
                measure it (not every definition can).
            subject: Context interpolated into the rejection message, or None
                when the message needs none.
        """

    def _reject(self, used: int, limit: int, subject: str | None) -> None:
        """Refuse the operation with the definition's own message.

        Args:
            used: The usage figure that breached the limit.
            limit: The effective limit that rejected the operation.
            subject: Context interpolated into the message, or None when the
                message needs none.

        Raises:
            QuotaExceededError: Always; the method exists to raise it.
        """
        raise QuotaExceededError(self.message(used, limit, subject), quota=self.key, limit=limit, used=used)


@dataclass(frozen=True)
class CapacityQuota(QuotaDefinition):
    """Limits how many of something can exist right now (never metered).

    Two check flavors: without ``used`` the current amount is measured via
    ``count`` and admitting one more must stay within the limit; with
    ``used`` the caller states the *desired final* amount (declarative —
    no counting race regardless of what exists today).
    """

    requires_lock: ClassVar[bool] = True

    count: Callable[[Session, UUID], int] | None = None

    def check(
        self,
        session: Session,
        org_id: UUID,
        limit: int,
        *,
        used: int | None = None,
        subject: str | None = None,
    ) -> None:
        """Admit the operation only if capacity stays within the limit.

        Args:
            session: Open session the work is done through.
            org_id: Organisation the quota is enforced for.
            limit: The effective limit, already resolved under the row lock.
            used: The desired final amount, or None to measure the current
                amount and admit one more.
            subject: Context interpolated into the rejection message, or None
                when the message needs none.

        Raises:
            ValueError: If ``used`` is omitted and the definition carries no
                ``count`` callback.
        """
        if used is None:
            if self.count is None:
                raise ValueError(f"Quota '{self.key}' has no usage counter; pass used= to check it declaratively")
            current = self.count(session, org_id)
            if current >= limit:
                self._reject(current, limit, subject)
        elif used > limit:
            self._reject(used, limit, subject)


@dataclass(frozen=True)
class BoundQuota(QuotaDefinition):
    """Bounds a single operation's magnitude (stateless — no count, no ledger).

    The gate always supplies ``used`` (the operation's size); nothing is
    measured or reserved, so no lock is needed: the check is a pure
    comparison against the effective limit.
    """

    def check(
        self,
        session: Session,
        org_id: UUID,
        limit: int,
        *,
        used: int | None = None,
        subject: str | None = None,
    ) -> None:
        """Admit the operation only if its magnitude stays within the limit.

        Args:
            session: Open session the work is done through.
            org_id: Organisation the quota is enforced for, unused for the
                same reason.
            limit: The effective limit, already resolved by the caller.
            used: The operation's size; required, since nothing is measured.
            subject: Context interpolated into the rejection message, or None
                when the message needs none.

        Raises:
            ValueError: If ``used`` is omitted.
        """
        if used is None:
            raise ValueError(f"Quota '{self.key}' bounds a single operation; pass used= with its size")
        if used > limit:
            self._reject(used, limit, subject)


@dataclass(frozen=True)
class ConsumptionQuota(QuotaDefinition):
    """Limits what accumulates per period, charged under ``metric`` in the ledger."""

    metric: str = ""

    def __post_init__(self) -> None:
        """Validate the identity plus the ledger metric this quota charges under.

        Raises:
            ValueError: If the key, the label, or the metric is empty.
        """
        super().__post_init__()
        if not self.metric:
            raise ValueError(f"Consumption quota '{self.key}' needs the ledger metric it charges under")

    def committed(self, session: Session, org_id: UUID) -> int:
        """The org's committed usage this period: ledger ``used + reserved``.

        Args:
            session: Open session the work is done through.
            org_id: Organisation whose ledger row is read.

        Returns:
            The committed count, or 0 when the period has no ledger row yet.
        """
        period_start = month_start(db_now(session))
        row = session.get(Usage, (org_id, self.metric, period_start))
        return (row.used + row.reserved) if row else 0

    def check(
        self,
        session: Session,
        org_id: UUID,
        limit: int,
        *,
        used: int | None = None,
        subject: str | None = None,
    ) -> None:
        """Admit the operation only if committed usage is below the limit.

        Args:
            session: Open session the work is done through.
            org_id: Organisation the quota is enforced for.
            limit: The effective limit, already resolved by the caller.
            used: Committed usage stated by the call site, or None to read it
                from the ledger.
            subject: Context interpolated into the rejection message, or None
                when the message needs none.
        """
        committed = used if used is not None else self.committed(session, org_id)
        if committed >= limit:
            self._reject(committed, limit, subject)

    def reserve(self, session: Session, org_id: UUID, limit: int) -> datetime | None:
        """Atomically reserve one unit against the period's ledger row.

        A conditional upsert (``used + reserved < limit``), so concurrent
        reservers can never overshoot.

        Args:
            session: Open session the work is done through.
            org_id: Organisation the unit is reserved for.
            limit: The effective limit the upsert is conditioned on; a limit of
                zero or less admits nothing.

        Returns:
            The reservation timestamp (DB clock), or None when exhausted.
        """
        if limit <= 0:
            return None
        now = db_now(session)
        period_start = month_start(now)
        table = Usage.__table__  # ty: ignore[unresolved-attribute]
        statement = (
            _insert_fn(session)(table)
            .values(org_id=org_id, metric=self.metric, period_start=period_start, used=0, reserved=1)
            .on_conflict_do_update(
                index_elements=["org_id", "metric", "period_start"],
                set_={"reserved": table.c.reserved + 1},
                where=(table.c.used + table.c.reserved < limit),
            )
        )
        result = session.execute(statement)  # ty: ignore[deprecated]
        return now if result.rowcount == 1 else None  # ty: ignore[unresolved-attribute]


# -- Registry ------------------------------------------------------------------

# Code-registered (no entry-point group): enforcement is welded into the store,
# so quotas are not a plugin surface. Limits are stored one row per key, so
# adding a quota is a registration plus its enforcement site, never a schema
# change. Instance defaults live as same-named optional fields on
# ``QuotaSettings``.
QUOTAS: Registry[QuotaDefinition] = Registry()


# -- Metering ------------------------------------------------------------------


def month_start(moment: datetime) -> dt.date:
    """The first day of the UTC calendar month a moment falls in.

    Naive values are treated as UTC (SQLite round-trips columns naive).

    Args:
        moment: The instant to attribute, aware or naive.

    Returns:
        The first day of that UTC month, as a date.
    """
    if moment.tzinfo is not None:
        moment = moment.astimezone(timezone.utc)
    return dt.date(moment.year, moment.month, 1)


def next_month_start(period_start: dt.date) -> dt.date:
    """The first day of the month after ``period_start``.

    Args:
        period_start: The first day of a month, as produced by
            :func:`month_start`.

    Returns:
        The first day of the following month, rolling the year over December.
    """
    if period_start.month == 12:
        return dt.date(period_start.year + 1, 1, 1)
    return dt.date(period_start.year, period_start.month + 1, 1)


def db_now(session: Session) -> datetime:
    """The database server's current time, as an aware UTC datetime.

    The DB clock is the single billing clock: completions are written by
    whatever process executes the run (for docker/k8s a child pod), whose
    wall clock is not trusted for month attribution.

    Args:
    session: Open session the work is done through.

    Returns:
        The server time, always tz-aware (UTC is assumed when the dialect
        returns a naive value).
    """
    value = session.scalar(sa_select(func.current_timestamp()))
    if isinstance(value, str):  # SQLite returns text
        value = datetime.fromisoformat(value)
    assert value is not None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value


def _insert_fn(session: Session) -> Any:
    """The dialect's upsert-capable insert constructor.

    Args:
    session: Open session the work is done through.

    Returns:
        The postgresql insert constructor, or the sqlite one for every other
        dialect.
    """
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

    Args:
        session: Open session the work is done through.
        org_id: Organisation the counters belong to.
        metric: Ledger metric being charged; must be one a registered
            consumption quota declares.
        period_start: First day of the UTC month the counters belong to.
        used: Delta applied to ``used``, defaulting to no change.
        reserved: Delta applied to ``reserved``, defaulting to no change;
            negative releases a reservation.

    Raises:
        ValueError: If no registered quota charges under ``metric``.
    """
    if all(getattr(definition, "metric", None) != metric for definition in QUOTAS.values()):
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

    Args:
        session: Open session the work is done through.
        db_run: The completing run, read for its org and its
            ``quota_reserved_at`` stamp (unset when nothing was reserved).
        success: Whether the run succeeded; only successes are charged.
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


# -- Enforcement ---------------------------------------------------------------


def _count_sources(session: Session, org_id: UUID) -> int:
    """Current number of sources — the usage side of ``max_sources``.

    Args:
        session: Open session the work is done through.
        org_id: Organisation whose sources are counted.

    Returns:
        The number of source components the organisation owns.
    """
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

    def __init__(self, engine: Engine, defaults: Callable[[], Any]) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
            defaults: Zero-arg provider of the instance-wide quota defaults,
                read per call so late reconfiguration is visible.
        """
        self._engine = engine
        self._defaults = defaults

    # -- Limits ----------------------------------------------------------------

    def effective(self, org_id: UUID, key: str, *, lock: bool = False) -> int | None:
        """Resolve a limit: org override wins over the global default; None = unlimited.

        With ``lock`` the ``(org, key)`` row is upserted (null-limit lock
        anchor) and held ``FOR UPDATE`` until the caller's transaction ends,
        serializing checks per organisation *and* key so independent quotas
        never block each other. An unregistered key fails loudly with
        ``KeyError`` before any database work.

        Args:
            org_id: Organisation whose override is resolved.
            key: Registered quota key to resolve.
            lock: Whether to take the ``(org, key)`` row lock, defaulting to a
                plain read.

        Returns:
            The effective limit, or None when the quota is unlimited.
        """
        with session_scope(self._engine) as session:
            QUOTAS[key]  # loud failure on unregistered keys
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

    def get_quota_overrides(self, org_id: UUID) -> dict[str, int]:
        """The organisation's set overrides as ``{key: limit}`` (null rows excluded).

        Args:
            org_id: Organisation whose overrides are read.

        Returns:
            The set overrides; empty when the organisation runs on the
            instance defaults alone.
        """
        with session_scope(self._engine) as session:
            rows = session.exec(select(Quota).where(Quota.org_id == org_id)).all()
            return {row.key: row.limit for row in rows if row.limit is not None}

    def list_quota_overrides(self) -> dict[UUID, dict[str, int]]:
        """Every organisation's set overrides, keyed by org id.

        Returns:
            One ``{key: limit}`` mapping per organisation that has at least
            one set override; organisations with none are absent.
        """
        with session_scope(self._engine) as session:
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

        Args:
            org_id: Organisation whose overrides are written.
            limits: The keys to change, mapped to their new limit or to None
                to clear the override.

        Returns:
            The organisation's overrides after the update.

        Raises:
            ValueError: On an unknown quota key or a negative value.
        """
        with session_scope(self._engine) as session:
            if unknown := {key for key in limits if key not in QUOTAS}:
                raise ValueError(f"Unknown quota limit(s): {sorted(unknown)}")
            if negative := {key for key, value in limits.items() if value is not None and value < 0}:
                raise ValueError(f"Quota limit(s) must be >= 0: {sorted(negative)}")
            table = Quota.__table__  # ty: ignore[unresolved-attribute]
            for key, value in limits.items():
                statement = (
                    _insert_fn(session)(table)
                    .values(org_id=org_id, key=key, limit=value)
                    .on_conflict_do_update(index_elements=["org_id", "key"], set_={"limit": value})
                )
                session.execute(statement)  # ty: ignore[deprecated]
            commit(session)
            return self.get_quota_overrides(org_id)

        # -- Enforcement -----------------------------------------------------------

    def check(
        self,
        org_id: UUID,
        key: str,
        *,
        used: int | None = None,
        subject: str | None = None,
    ) -> None:
        """The one enforcement gate: resolve the limit, delegate to the definition.

        Part of the caller's transaction. No-op while the quota is
        unlimited; capacity definitions re-resolve under the ``(org, key)``
        row lock before comparing. ``used`` and ``subject`` are forwarded to
        :meth:`QuotaDefinition.check`.

        Args:
            org_id: Organisation the quota is enforced for.
            key: Registered quota key to enforce.
            used: Usage stated by the call site, or None to let the definition
                measure it.
            subject: Context interpolated into the rejection message, or None
                when the message needs none.
        """
        definition = QUOTAS[key]
        limit = self.effective(org_id, key)
        if limit is None:
            return
        if definition.requires_lock:
            limit = self.effective(org_id, key, lock=True)
            if limit is None:
                return
        with session_scope(self._engine) as session:
            definition.check(session, org_id, limit, used=used, subject=subject)

    def run_status(self, org_id: UUID) -> tuple[int, int | None]:
        """The org's committed run usage this period: ``(used + reserved, limit)``.

        Limit None means unlimited (and the ledger is not read at all).

        Args:
            org_id: Organisation whose run usage is reported.

        Returns:
            The committed count paired with the effective limit; ``(0, None)``
            while the quota is unlimited.

        Raises:
            TypeError: If the run quota key is registered as something other
                than a consumption quota.
        """
        with session_scope(self._engine) as session:
            definition = QUOTAS[QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH]
            if not isinstance(definition, ConsumptionQuota):
                raise TypeError(f"'{QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH}' is not registered as a consumption quota")
            limit = self.effective(org_id, QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH)
            if limit is None:
                return 0, None
            return definition.committed(session, org_id), limit

    def try_reserve_run(self, db_run: Run) -> bool:
        """Atomically reserve a run-quota slot at dispatch time.

        The authoritative run gate: on success the run is stamped with
        ``quota_reserved_at`` so settlement releases the right period.
        Unlimited orgs are admitted without touching the ledger.

        Args:
            db_run: The run being dispatched; stamped in place on success.

        Returns:
            True if the run may dispatch, False when the quota is exhausted.

        Raises:
            TypeError: If the run quota key is registered as something other
                than a consumption quota.
        """
        with session_scope(self._engine) as session:
            definition = QUOTAS[QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH]
            if not isinstance(definition, ConsumptionQuota):
                raise TypeError(f"'{QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH}' is not registered as a consumption quota")
            limit = self.effective(db_run.org_id, QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH)
            if limit is None:
                return True
            reserved_at = definition.reserve(session, db_run.org_id, limit)
            if reserved_at is None:
                return False
            db_run.quota_reserved_at = reserved_at
            session.add(db_run)
            commit(session)
            return True

        # -- Usage -----------------------------------------------------------------

    def list_usage(
        self,
        *,
        period_start: dt.date | None = None,
        org_id: UUID | None = None,
    ) -> list[Usage]:
        """Usage ledger rows, optionally filtered by period and organisation.

        Args:
            period_start: First day of the UTC month to restrict to, or None
                for every period.
            org_id: Organisation to restrict to, or None for every
                organisation.

        Returns:
            The matching ledger rows, one per ``(org, metric, period)``.
        """
        with session_scope(self._engine) as session:
            statement = select(Usage)
            if period_start is not None:
                statement = statement.where(Usage.period_start == period_start)
            if org_id is not None:
                statement = statement.where(Usage.org_id == org_id)
            return list(session.exec(statement).all())

    def current_period_start(self) -> dt.date:
        """The current UTC calendar month, per the database clock.

        Returns:
            The first day of the month usage is currently charged into.
        """
        with session_scope(self._engine) as session:
            return month_start(db_now(session))

    def count_sources_by_org(self) -> dict[UUID, int]:
        """Current number of sources per organisation.

        Returns:
            The source count keyed by org id; organisations with no sources
            are absent.
        """
        with session_scope(self._engine) as session:
            rows = session.exec(
                select(Component.org_id, func.count())
                .where(Component.kind == "source")
                .group_by(Component.org_id)  # ty: ignore[invalid-argument-type]
            ).all()
            return dict(rows)

    def max_assets_per_source_by_org(self) -> dict[UUID, int]:
        """The largest child-asset count of any single source, per organisation.

        Returns:
            The peak per-source asset count keyed by org id; organisations
            with no parented assets are absent.
        """
        with session_scope(self._engine) as session:
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

        Args:
            period_start: First day of the UTC month to recompute, whose
                following month bounds the window.

        Returns:
            The successful-run count keyed by org id; organisations with none
            are absent.
        """
        with session_scope(self._engine) as session:
            lower = datetime.combine(period_start, dt.time.min, tzinfo=timezone.utc)
            upper = datetime.combine(next_month_start(period_start), dt.time.min, tzinfo=timezone.utc)
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

        Returns:
            One ``{org_id, period_start, ledger, recomputed}`` entry per
            drifting organisation, ordered by org id; empty when the ledger
            agrees with the runs table.
        """
        # Scoped, though the handle is unused: the three reads below join it, so
        # the ledger and the runs table are compared at one point in time.
        with session_scope(self._engine):
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


# -- Registration --------------------------------------------------------------


QUOTAS.register(
    QUOTA_MAX_SOURCES,
    CapacityQuota(
        key=QUOTA_MAX_SOURCES,
        label="Max sources",
        count=_count_sources,
        message=lambda used, limit, _subject: f"Organisation is at its source limit ({used}/{limit})",
    ),
)
QUOTAS.register(
    QUOTA_MAX_ASSETS_PER_SOURCE,
    CapacityQuota(
        key=QUOTA_MAX_ASSETS_PER_SOURCE,
        label="Max assets per source",
        message=lambda used, limit, subject: (
            f"Source '{subject}' would have {used} assets, exceeding the limit of {limit}"
        ),
    ),
)
QUOTAS.register(
    QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH,
    ConsumptionQuota(
        key=QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH,
        label="Max successful runs / month",
        metric=METRIC_SUCCESSFUL_RUNS,
        message=lambda used, limit, subject: (
            f"Cannot queue {subject or 'run'}: the monthly successful-run quota is exhausted ({used}/{limit})"
        ),
    ),
)
QUOTAS.register(
    QUOTA_MAX_BACKFILL_PARTITIONS,
    BoundQuota(
        key=QUOTA_MAX_BACKFILL_PARTITIONS,
        label="Max backfill partitions",
        message=lambda used, limit, _subject: f"Backfill spans {used} partitions, exceeding the limit of {limit}",
    ),
)
