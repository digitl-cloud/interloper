"""Limit resolution and the enforcement gates every quota passes through."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from interloper.errors import ConfigError
from sqlalchemy import Engine
from sqlmodel import select

from interloper_db.models import Quota, Run
from interloper_db.session import commit, dialect_insert, session_scope
from interloper_db.store.quotas.definitions import (
    QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH,
    QUOTAS,
    CapacityQuota,
    ConsumptionQuota,
)


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

    def effective_limit(self, org_id: UUID, key: str, *, lock: bool = False) -> int | None:
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

    def overrides(self, org_id: UUID) -> dict[str, int]:
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

    def all_overrides(self) -> dict[UUID, dict[str, int]]:
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

    def set_overrides(self, org_id: UUID, limits: dict[str, int | None]) -> dict[str, int]:
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
            ConfigError: On an unknown quota key or a negative value.
        """
        with session_scope(self._engine) as session:
            if unknown := {key for key in limits if key not in QUOTAS}:
                raise ConfigError(f"Unknown quota limit(s): {sorted(unknown)}")
            if negative := {key for key, value in limits.items() if value is not None and value < 0}:
                raise ConfigError(f"Quota limit(s) must be >= 0: {sorted(negative)}")
            table = Quota.__table__  # ty: ignore[unresolved-attribute]
            for key, value in limits.items():
                statement = (
                    dialect_insert(session)(table)
                    .values(org_id=org_id, key=key, limit=value)
                    .on_conflict_do_update(index_elements=["org_id", "key"], set_={"limit": value})
                )
                session.execute(statement)  # ty: ignore[deprecated]
            commit(session)
            return self.overrides(org_id)

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
        limit = self.effective_limit(org_id, key)
        if limit is None:
            return
        if definition.requires_lock:
            limit = self.effective_limit(org_id, key, lock=True)
            if limit is None:
                return
        with session_scope(self._engine) as session:
            definition.check(session, org_id, limit, used=used, subject=subject)

    def admit_component(self, org_id: UUID, kind: str) -> None:
        """Admit one more component of ``kind`` past every capacity quota counting it.

        Part of the caller's transaction. A kind no quota counts is admitted
        without touching the database.

        Args:
            org_id: Organisation the component is created in.
            kind: Kind of the component being created.
        """
        for definition in QUOTAS.values():
            if isinstance(definition, CapacityQuota) and definition.kind == kind:
                self.check(org_id, definition.key)

    def admit_run(self, org_id: UUID, *, billable: bool, subject: str | None = None) -> None:
        """Fail fast when the organisation cannot queue another billable run.

        Part of the caller's transaction, and advisory: the authoritative gate
        is the dispatch-time reservation in :meth:`try_reserve_run`. A
        non-billable run is admitted unchecked, since it is never reserved or
        charged either.

        Args:
            org_id: Organisation the run is queued for.
            billable: Whether the run counts against the run quota, as its
                target's workload declares.
            subject: What is being queued, for the rejection message, or None
                for a plain run.
        """
        if billable:
            self.check(org_id, QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH, subject=subject)

    def try_reserve_run(self, db_run: Run) -> bool:
        """Atomically reserve a run-quota slot at dispatch time.

        The authoritative run gate: on success the run is stamped with
        ``quota_reserved_at`` so settlement releases the right period.
        Unlimited orgs are admitted without touching the ledger, and so are
        non-billable runs: platform plumbing is never billed at either end.

        Args:
            db_run: The run being dispatched; stamped in place on success.

        Returns:
            True if the run may dispatch, False when the quota is exhausted.

        Raises:
            TypeError: If the run quota key is registered as something other
                than a consumption quota.
        """
        if not db_run.billable:
            return True
        with session_scope(self._engine) as session:
            definition = QUOTAS[QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH]
            if not isinstance(definition, ConsumptionQuota):
                raise TypeError(f"'{QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH}' is not registered as a consumption quota")
            limit = self.effective_limit(db_run.org_id, QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH)
            if limit is None:
                return True
            reserved_at = definition.reserve(session, db_run.org_id, limit)
            if reserved_at is None:
                return False
            db_run.quota_reserved_at = reserved_at
            session.add(db_run)
            commit(session)
            return True
