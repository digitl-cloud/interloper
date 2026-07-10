"""Catalog drift: the single resolution primitive for stored component keys.

Catalog keys (``Component.key``, derived from class names) are the only
join between *code* (the catalog of Python classes) and *data* (the
``components`` rows that reference those keys as bare strings, with no
foreign key back to the catalog). When a class is renamed or removed, the
stored key *drifts* — it no longer resolves against the catalog.

Rather than discover drift with a separate scanner, drift is computed as the
outcome of the resolution step every load already performs, surfaced as a
:class:`ComponentStatus` value instead of an exception. Hydration and
detection therefore consume the *same* resolver and can never disagree.

Resolution is against two catalogs the system already has:

- the **enabled** catalog (``AppSettings.catalog`` — what this deployment
  exposes), passed in by the caller (the :class:`~interloper_db.store.Store`
  holds it as ``_catalog``); and
- the **discovered** universe (:meth:`Catalog.discover` — every component any
  installed package declares), used to tell a key that is *gone from the code*
  apart from one merely *not enabled here*.

This yields a tri-state:

================  ================================================  ==========
``ComponentStatus``  meaning                                        drift?
================  ================================================  ==========
``ok``            key is in the enabled catalog — live, runnable     no
``disabled``      key is in code (discovered) but not enabled here   no
``missing``       key is gone from the code entirely                 **yes**
================  ================================================  ==========

Only ``missing`` is drift (removable). ``disabled`` is intentional — the
component may return when the deployment re-enables it — so it must never be
flagged for cleanup.
"""

from __future__ import annotations

from enum import Enum
from functools import cache

import interloper as il
from interloper.catalog.base import Catalog
from interloper.utils.imports import import_from_path


class ComponentStatus(str, Enum):
    """Resolution state of a persisted component against the catalog."""

    OK = "ok"
    """Key resolves in the enabled catalog — the component is live."""

    DISABLED = "disabled"
    """Key exists in code but is not exposed by this deployment's catalog."""

    MISSING = "missing"
    """Key no longer exists in code at all — this is drift."""


@cache
def _discovered_catalog() -> Catalog:
    """The full installed component universe, memoised for the process.

    Discovery imports every component declared via entry points, so the
    result is stable for the life of the process (entry points don't change
    at runtime) — matching the ``@cache`` on the underlying path scan.
    """
    return Catalog.discover()


def resolve_source_cls(catalog: Catalog, key: str) -> type[il.Source] | None:
    """Return the source class for *key*, or ``None`` if it does not resolve.

    Value-based counterpart to the raise-on-miss lookup used by writes. The
    catalog only holds keys whose classes imported cleanly at build time, but
    the import is still guarded so a stale definition degrades to ``None``
    rather than raising.
    """
    definition = catalog.get(key)
    if definition is None:
        return None
    try:
        obj = import_from_path(definition.path)
    except (ImportError, AttributeError):
        return None
    return obj if isinstance(obj, type) and issubclass(obj, il.Source) else None


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
    """
    if source_key is None:
        return source_status(catalog, key, discovered=discovered)

    parent = source_status(catalog, source_key, discovered=discovered)
    if parent is not ComponentStatus.OK:
        # A gone/not-exposed source drags its assets to the same state.
        return parent

    source_cls = resolve_source_cls(catalog, source_key)
    if source_cls is None:  # defensive: enabled said ok but the import failed
        return ComponentStatus.MISSING
    valid_keys = {asset_type.key for asset_type in source_cls.asset_types}
    return ComponentStatus.OK if key in valid_keys else ComponentStatus.MISSING
