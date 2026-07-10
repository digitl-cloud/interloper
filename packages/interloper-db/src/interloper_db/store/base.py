"""Shared store state and session policy."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import Engine
from sqlmodel import Session

if TYPE_CHECKING:
    from interloper.catalog.base import Catalog

    from interloper_db.hydration import Hydrator


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

    def _session(self) -> Session:
        """Open a session on the store's engine.

        Returns:
            A new session following the class's session policy.
        """
        return Session(self._engine, expire_on_commit=False)
