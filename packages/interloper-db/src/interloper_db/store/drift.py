"""Drift surface of the Store: catalog-resolution status for stored keys."""

from __future__ import annotations

from interloper.catalog.base import Catalog

from interloper_db.drift import ComponentStatus, asset_status, source_status


class DriftStore:
    """Store methods that surface catalog drift for persisted components.

    Thin delegation to the pure resolver functions, passing the Store's
    enabled ``_catalog`` so callers (API routes, hydration) never reach into
    catalog internals.
    """

    def __init__(self, catalog: Catalog) -> None:
        """Bind the facet to what it works through.

        Args:
            catalog: Catalog its component keys resolve against.
        """
        self._catalog = catalog

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
