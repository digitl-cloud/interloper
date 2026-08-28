"""Drift surface of the Store: catalog-resolution status for stored keys."""

from __future__ import annotations

from interloper_db.drift import ComponentStatus, asset_status, source_status
from interloper_db.store.base import StoreBase


class DriftMixin(StoreBase):
    """Store methods that surface catalog drift for persisted components.

    Thin delegation to the pure resolver functions, passing the Store's
    enabled ``_catalog`` so callers (API routes, hydration) never reach into
    catalog internals.
    """

    def source_status(self, key: str) -> ComponentStatus:
        """Resolution state of a stored source key against the catalog.

        Args:
            key: Stored source key to resolve.

        Returns:
            The key's status: ok, disabled, or missing.
        """
        return source_status(self._catalog, key)

    def asset_status(self, key: str, *, source_key: str | None = None) -> ComponentStatus:
        """Resolution state of a stored asset key against the catalog.

        Args:
            key: Stored asset key to resolve.
            source_key: Key of the owning source, whose status the asset's
                cascades from. None (the default) resolves a standalone asset,
                which is itself a catalog component.

        Returns:
            The key's status: ok, disabled, or missing.
        """
        return asset_status(self._catalog, key, source_key=source_key)
