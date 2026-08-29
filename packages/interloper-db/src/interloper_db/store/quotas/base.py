"""Limit resolution and the enforcement gates every quota passes through."""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from interloper.utils import add_months
from sqlalchemy import Engine, func
from sqlalchemy import select as sa_select
from sqlmodel import col, select

from interloper_db.models import Component, Quota, Run, Usage
from interloper_db.session import commit, dialect_insert, session_scope
from interloper_db.store.quotas.definitions import (
    QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH,
    QUOTAS,
    ConsumptionQuota,
)
from interloper_db.store.quotas.metering import METRIC_SUCCESSFUL_RUNS, UsageLedger


class QuotaStore:
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
                    dialect_insert(session)(table)
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
                    dialect_insert(session)(table)
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
            return UsageLedger(session).current_period()

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
            upper = datetime.combine(add_months(period_start, 1), dt.time.min, tzinfo=timezone.utc)
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
