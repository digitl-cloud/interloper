"""Shared FastAPI dependencies for store, catalog, auth, and RBAC."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import Cookie, Depends, HTTPException
from interloper.catalog.base import Catalog
from interloper_db import Organisation, Profile, Store
from interloper_db.models import Session as SessionModel

_store: Store | None = None
_catalog: Catalog | None = None
_auth_config: Any | None = None
_smtp_config: Any | None = None

# Role hierarchy: admin > editor > viewer
_ROLE_RANK = {"viewer": 0, "editor": 1, "admin": 2}


def set_store(store: Store) -> None:
    """Set the global store instance.

    Args:
        store: The Store to use for all API operations.
    """
    global _store  # noqa: PLW0603
    _store = store


def set_catalog(catalog: Catalog) -> None:
    """Set the global catalog instance.

    Args:
        catalog: The Catalog instance.
    """
    global _catalog  # noqa: PLW0603
    _catalog = catalog


def set_auth_config(auth_config: Any) -> None:
    """Set the global auth config.

    Args:
        auth_config: The AuthConfig instance.
    """
    global _auth_config  # noqa: PLW0603
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


def get_catalog() -> dict[str, Any]:
    """Return the global catalog as a serialized dict.

    Returns:
        Mapping from component key to serialized definition.

    Raises:
        RuntimeError: If the catalog has not been set.
    """
    if _catalog is None:
        raise RuntimeError("Catalog not initialized. Call set_catalog() first.")
    return _catalog.dump()


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
    global _smtp_config  # noqa: PLW0603
    _smtp_config = smtp_config


def get_smtp_config() -> Any:
    """Return the global SMTP config.

    Returns:
        The SmtpConfig instance, or None if not configured.
    """
    return _smtp_config


# -- Auth dependencies -------------------------------------------------------


def get_current_user(
    store: Store = Depends(get_store),
    session_token: str | None = Cookie(default=None),
) -> Profile:
    """Resolve the current user from the session cookie.

    Args:
        store: The Store instance.
        session_token: Session cookie value.

    Returns:
        The authenticated Profile.

    Raises:
        HTTPException: 401 if not authenticated or session invalid/expired.
    """
    if not session_token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    result = store.resolve_session(session_token)
    if not result:
        raise HTTPException(status_code=401, detail="Invalid or expired session")

    profile, _ = result
    return profile


def get_session_context(
    store: Store = Depends(get_store),
    session_token: str | None = Cookie(default=None),
) -> tuple[Profile, SessionModel]:
    """Resolve user and session from the cookie.

    Args:
        store: The Store instance.
        session_token: Session cookie value.

    Returns:
        ``(Profile, Session)`` tuple.

    Raises:
        HTTPException: 401 if not authenticated.
    """
    if not session_token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    result = store.resolve_session(session_token)
    if not result:
        raise HTTPException(status_code=401, detail="Invalid or expired session")

    return result


def get_current_org(
    store: Store = Depends(get_store),
    session_token: str | None = Cookie(default=None),
) -> Organisation:
    """Resolve the current organisation from the session.

    Args:
        store: The Store instance.
        session_token: Session cookie value.

    Returns:
        The active Organisation.

    Raises:
        HTTPException: 400 if no organisation selected, 401 if not authenticated.
    """
    if not session_token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    result = store.resolve_session(session_token)
    if not result:
        raise HTTPException(status_code=401, detail="Invalid or expired session")

    _, session_row = result
    if not session_row.organisation_id:
        raise HTTPException(status_code=400, detail="No organisation selected")

    org = store.get_organisation(session_row.organisation_id)
    if not org:
        raise HTTPException(status_code=404, detail="Organisation not found")

    return org


def get_org_id(
    org: Organisation = Depends(get_current_org),
) -> UUID:
    """Shorthand: return just the org UUID for route handlers.

    Args:
        org: The resolved Organisation.

    Returns:
        The organisation UUID.
    """
    return org.id  # type: ignore[return-value]


# -- RBAC dependencies -------------------------------------------------------


def _check_role(
    minimum: str,
    user: Profile,
    org_id: UUID,
    store: Store,
) -> Profile:
    """Verify the user has at least the required role in the org.

    Args:
        minimum: Minimum role required (``viewer``, ``editor``, ``admin``).
        user: The authenticated user.
        org_id: The active organisation UUID.
        store: The Store instance.

    Returns:
        The authenticated Profile (pass-through for dependency chaining).

    Raises:
        HTTPException: 403 if insufficient permissions.
    """
    role = store.get_user_role(user.id, org_id)  # type: ignore[arg-type]
    if role is None:
        raise HTTPException(status_code=403, detail="Not a member of this organisation")
    if _ROLE_RANK.get(role, -1) < _ROLE_RANK[minimum]:
        raise HTTPException(status_code=403, detail=f"Requires {minimum} role or higher")
    return user


def require_viewer(
    user: Profile = Depends(get_current_user),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> Profile:
    """Require at least ``viewer`` role. Any org member passes.

    Returns:
        The authenticated Profile.

    Raises:
        HTTPException: 401/403 on auth or role failure.
    """
    return _check_role("viewer", user, org_id, store)


def require_editor(
    user: Profile = Depends(get_current_user),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> Profile:
    """Require at least ``editor`` role.

    Returns:
        The authenticated Profile.

    Raises:
        HTTPException: 401/403 on auth or role failure.
    """
    return _check_role("editor", user, org_id, store)


def require_admin(
    user: Profile = Depends(get_current_user),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> Profile:
    """Require ``admin`` role.

    Returns:
        The authenticated Profile.

    Raises:
        HTTPException: 401/403 on auth or role failure.
    """
    return _check_role("admin", user, org_id, store)
