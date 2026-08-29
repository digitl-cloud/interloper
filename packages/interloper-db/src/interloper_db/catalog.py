"""Catalog resolution: turning a stored component key back into its class.

``Component.key`` is the only join between *code* (the catalog of Python
classes) and *data* (the ``components`` rows, which reference those keys as
bare strings with no foreign key back to the catalog). Resolving a key is
therefore the primitive that naming, hydration, child synthesis, validation
and drift detection all build on — which is why it lives here rather than
inside any one of them.

Resolution is value-based: a key that no longer resolves returns ``None``
rather than raising, so a stale row degrades instead of breaking the caller.
"""

from __future__ import annotations

import logging

import interloper as il
from interloper.catalog.base import Catalog
from interloper.utils import import_from_path

logger = logging.getLogger(__name__)


def _discovered_catalog() -> Catalog:
    """The full installed component universe, memoised for the process.

    Discovery imports every component declared via entry points, so the
    result is stable for the life of the process (entry points don't change
    at runtime) — matching the ``@cache`` on the underlying path scan.

    Returns:
        The discovered catalog, shared across all callers in the process.
    """
    return Catalog.discover()


def resolve_component_cls(catalog: Catalog, key: str) -> type[il.Component] | None:
    """Return the component class for *key*, or ``None`` if it does not resolve.

    Value-based counterpart to the raise-on-miss lookup used by writes. The
    catalog only holds keys whose classes imported cleanly at build time, but
    the import is still guarded so a stale definition degrades to ``None``
    rather than raising.

    Args:
        catalog: The catalog to resolve against.
        key: The stored component key.

    Returns:
        The component class, or ``None`` if the key is absent from the catalog
        or its import path no longer yields a ``Component`` subclass.
    """
    definition = catalog.get(key)
    if definition is None:
        return None
    try:
        imported = import_from_path(definition.path)
    except (ImportError, AttributeError):
        return None
    return imported if isinstance(imported, type) and issubclass(imported, il.Component) else None


def resolve_source_cls(catalog: Catalog, key: str) -> type[il.Source] | None:
    """Return the source class for *key*, or ``None`` if it does not resolve.

    Args:
        catalog: The catalog to resolve against.
        key: The stored source key.

    Returns:
        The source class, or ``None`` if the key does not resolve to a
        ``Source`` subclass.
    """
    imported = resolve_component_cls(catalog, key)
    return imported if imported is not None and issubclass(imported, il.Source) else None
