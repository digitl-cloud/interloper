"""Shared store state and session policy."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import Engine
from sqlmodel import Session

if TYPE_CHECKING:
    from interloper.catalog.base import Catalog

    from interloper_db.hydration import Hydrator
    from interloper_db.store.quotas import QuotaService


class StoreBase:
    """State contract and session policy shared by every Store mixin.

    Sessions are per-method and never outlive the call. They are created
    with ``expire_on_commit=False`` so committed objects stay fully loaded
    and are safe to return detached — no ``refresh``/``expunge`` ceremony.
    The one exception: a freshly *inserted* row that is returned gets one
    ``session.refresh()`` to load its server-generated columns (``id``,
    ``created_at``).
    """

    _engine: Engine
    _catalog: Catalog
    _hydrator: Hydrator
    _encrypt: Any
    _decrypt: Any
    # QuotaSettings-shaped defaults for quota limits; None = everything
    # unlimited (quota checks short-circuit without touching the DB).
    _quota_defaults: Any = None

    @property
    def quotas(self) -> QuotaService:
        """Quota limit resolution and enforcement gates.

        Exposed so co-located machinery (the scheduler's claim/cron SQL)
        enforces against the same defaults as the store. Lazily built; the
        defaults are read per call, so late configuration is visible.
        """
        service: QuotaService | None = getattr(self, "_quota_service", None)
        if service is None:
            from interloper_db.store.quotas import QuotaService as _QuotaService

            service = _QuotaService(lambda: self._quota_defaults)
            self._quota_service = service
        return service

    @property
    def engine(self) -> Engine:
        """The engine this store operates on.

        Exposed so co-located machinery (the scheduler's claim/sweep SQL)
        runs against the same engine as the store, not an ambient global.
        """
        return self._engine

    def _session(self) -> Session:
        """Open a session on the store's engine.

        Returns:
            A new session following the class's session policy.
        """
        return Session(self._engine, expire_on_commit=False)
