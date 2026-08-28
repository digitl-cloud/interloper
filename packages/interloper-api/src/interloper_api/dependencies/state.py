"""Process-wide application state, set once at startup and read via ``Depends``.

``create_app`` installs the store, catalog and configuration objects here;
every request-scoped dependency reads them back through the getters, so no
route reaches for an ambient global of its own.
"""

from __future__ import annotations

from typing import Any

from interloper.catalog.base import Catalog
from interloper_db import Store

_store: Store | None = None
_catalog: Catalog | None = None
_auth_config: Any | None = None
_smtp_config: Any | None = None
_features: dict[str, bool] = {}
_admin_config: Any | None = None
_quota_defaults: Any | None = None


def set_store(store: Store) -> None:
    """Set the global store instance.

    Args:
        store: The Store to use for all API operations.
    """
    global _store
    _store = store


def set_catalog(catalog: Catalog) -> None:
    """Set the global catalog instance.

    Args:
        catalog: The Catalog instance.
    """
    global _catalog
    _catalog = catalog


def set_auth_config(auth_config: Any) -> None:
    """Set the global auth config.

    Args:
        auth_config: The AuthConfig instance.
    """
    global _auth_config
    _auth_config = auth_config


def get_store() -> Store:
    """Return the global store instance.

    Returns:
        The Store.

    Raises:
        RuntimeError: If the store has not been set.
    """
    if _store is None:
        raise RuntimeError("Store not initialized. Call set_store() first.")
    return _store


def get_catalog() -> Catalog:
    """Return the global ``Catalog`` instance.

    Routes that need the serialized form call ``.dump()`` themselves.

    Returns:
        The Catalog instance.

    Raises:
        RuntimeError: If the catalog has not been set.
    """
    if _catalog is None:
        raise RuntimeError("Catalog not initialized. Call set_catalog() first.")
    return _catalog


def get_auth_config() -> Any:
    """Return the global auth config.

    Returns:
        The AuthConfig instance.

    Raises:
        RuntimeError: If the auth config has not been set.
    """
    if _auth_config is None:
        raise RuntimeError("Auth config not initialized. Call set_auth_config() first.")
    return _auth_config


def set_smtp_config(smtp_config: Any) -> None:
    """Set the global SMTP config.

    Args:
        smtp_config: The SmtpConfig instance.
    """
    global _smtp_config
    _smtp_config = smtp_config


def get_smtp_config() -> Any:
    """Return the global SMTP config.

    Returns:
        The SmtpConfig instance, or None if not configured.
    """
    return _smtp_config


def set_features(features: dict[str, bool]) -> None:
    """Set the optional-feature availability flags (resolved at app creation).

    Args:
        features: Feature name → availability.
    """
    global _features
    _features = features


def get_features() -> dict[str, bool]:
    """Return the optional-feature availability flags.

    Returns:
        Feature name → availability; empty if never set.
    """
    return _features


def set_admin_config(config: Any) -> None:
    """Set the redacted instance-config snapshot (built at app creation).

    Args:
        config: The AdminConfigResponse snapshot.
    """
    global _admin_config
    _admin_config = config


def get_admin_config() -> Any:
    """Return the redacted instance-config snapshot.

    Returns:
        The AdminConfigResponse snapshot, or None if not configured.
    """
    return _admin_config


def set_quota_defaults(defaults: Any) -> None:
    """Set the global default quota limits (from settings, at app creation).

    Args:
        defaults: The QuotaSettings instance.
    """
    global _quota_defaults
    _quota_defaults = defaults


def get_quota_defaults() -> Any:
    """Return the global default quota limits.

    Returns:
        The QuotaSettings instance, or None if not configured.
    """
    return _quota_defaults
