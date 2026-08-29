"""Drift: whether a stored component key still resolves against the catalog.

A key that no longer resolves means the class was renamed, removed, or is
shipped by a package this deployment does not install. Drift is reported as
a value rather than raised, so the UI can show a stale component instead of
failing to load the page.
"""

from __future__ import annotations

from enum import Enum

from interloper.catalog.base import Catalog

from interloper_db.catalog import _discovered_catalog, resolve_source_cls


class ComponentStatus(str, Enum):
    """Resolution state of a persisted component against the catalog."""

    OK = "ok"
    """Key resolves in the enabled catalog — the component is live."""

    DISABLED = "disabled"
    """Key exists in code but is not exposed by this deployment's catalog."""

    MISSING = "missing"
    """Key no longer exists in code at all — this is drift."""


def source_status(
    catalog: Catalog,
    key: str,
    *,
    discovered: Catalog | None = None,
) -> ComponentStatus:
    """Resolve a source key to its :class:`ComponentStatus`.

    Args:
        catalog: The enabled catalog (what this deployment exposes).
        key: The stored source key.
        discovered: The discovered universe; defaults to the cached
            :func:`_discovered_catalog`. Injectable for testing.

    Returns:
        ``OK`` when the key is enabled here, ``DISABLED`` when it exists in
        code but is not exposed, ``MISSING`` when it is gone from the code.
    """
    if key in catalog.components:
        return ComponentStatus.OK
    discovered = discovered if discovered is not None else _discovered_catalog()
    if key in discovered.components:
        return ComponentStatus.DISABLED
    return ComponentStatus.MISSING


def asset_status(
    catalog: Catalog,
    key: str,
    *,
    source_key: str | None = None,
    discovered: Catalog | None = None,
) -> ComponentStatus:
    """Resolve an asset key to its :class:`ComponentStatus`.

    A standalone asset (``source_key is None``) is itself a catalog component
    and resolves like a source. A source-owned asset resolves *through* its
    parent: a missing/disabled parent cascades to the asset, and under a live
    parent the asset is ``ok`` only if its key is still one of the source's
    ``asset_types`` — otherwise the asset key has drifted out of the source.

    Args:
        catalog: The enabled catalog.
        key: The stored asset key.
        source_key: The owning source's key, or ``None`` for a standalone asset.
        discovered: The discovered universe; defaults to the cached one.

    Returns:
        The asset's status, cascaded from the owning source when there is one.
    """
    if source_key is None:
        return source_status(catalog, key, discovered=discovered)

    parent = source_status(catalog, source_key, discovered=discovered)
    if parent is not ComponentStatus.OK:
        return parent

    source_cls = resolve_source_cls(catalog, source_key)
    if source_cls is None:  # defensive: enabled said ok but the import failed
        return ComponentStatus.MISSING
    valid_keys = {asset_type.key for asset_type in source_cls.asset_types}
    return ComponentStatus.OK if key in valid_keys else ComponentStatus.MISSING


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
