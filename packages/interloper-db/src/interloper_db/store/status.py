"""Whether this deployment can still use a stored component.

A key that no longer resolves means the class was renamed, removed, or is
shipped by a package this deployment does not install. A payload the active
encryption key cannot decrypt means the row survives but its config does not.
Either way the component is persisted and unusable until someone acts, so it
is reported as a value rather than raised, and the UI can show a stale or
unreadable component instead of failing to load the page.

The functions here resolve bare keys against the catalog. Readability is a
row-level question, so it belongs with the row-level status in the component
facet, which calls through to here for the catalog half.
"""

from __future__ import annotations

from enum import Enum

import interloper as il
from interloper.catalog.base import Catalog
from interloper.errors import CatalogKeyError


class ComponentStatus(str, Enum):
    """Usability state of a persisted component in this deployment."""

    OK = "ok"
    """Key resolves in the enabled catalog — the component is live."""

    DISABLED = "disabled"
    """Key exists in code but is not exposed by this deployment's catalog."""

    MISSING = "missing"
    """Key no longer exists in code at all — this is drift."""

    UNREADABLE = "unreadable"
    """Key resolves, but the stored payload does not decrypt under the active
    ``INTERLOPER_ENCRYPTION_KEY`` (rotated, mismatched, or absent). The row is
    intact; its config has to be re-entered or re-keyed."""


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
        discovered: The discovered universe; defaults to the full
            installed component set. Injectable for testing.

    Returns:
        ``OK`` when the key is enabled here, ``DISABLED`` when it exists in
        code but is not exposed, ``MISSING`` when it is gone from the code.
    """
    if key in catalog.components:
        return ComponentStatus.OK
    discovered = discovered if discovered is not None else Catalog.discover()
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
        discovered: The discovered universe; defaults to the full
            installed component set.

    Returns:
        The asset's status, cascaded from the owning source when there is one.
    """
    if source_key is None:
        return source_status(catalog, key, discovered=discovered)

    parent = source_status(catalog, source_key, discovered=discovered)
    if parent is not ComponentStatus.OK:
        return parent

    try:
        source_cls = il.Source.resolve_key(source_key, catalog)
    except (CatalogKeyError, ImportError, AttributeError, TypeError):
        return ComponentStatus.MISSING  # defensive: enabled said ok but the class did not resolve
    valid_keys = {asset_type.key for asset_type in source_cls.asset_types}
    return ComponentStatus.OK if key in valid_keys else ComponentStatus.MISSING
