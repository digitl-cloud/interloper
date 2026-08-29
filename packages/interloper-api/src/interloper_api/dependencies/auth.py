"""Request-scoped identity: who is calling, and which organisation they are in."""

from __future__ import annotations

from uuid import UUID

from fastapi import Cookie, Depends, HTTPException
from interloper_db import Organisation, Profile, Store
from interloper_db.models import AuthSession

from interloper_api.dependencies.state import get_store


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

    result = store.auth.resolve_session(session_token)
    if not result:
        raise HTTPException(status_code=401, detail="Invalid or expired session")

    profile, _ = result
    return profile


def get_session_context(
    store: Store = Depends(get_store),
    session_token: str | None = Cookie(default=None),
) -> tuple[Profile, AuthSession]:
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

    result = store.auth.resolve_session(session_token)
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

    result = store.auth.resolve_session(session_token)
    if not result:
        raise HTTPException(status_code=401, detail="Invalid or expired session")

    _, session_row = result
    if not session_row.organisation_id:
        raise HTTPException(status_code=400, detail="No organisation selected")

    org = store.auth.get_organisation(session_row.organisation_id)
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
    return org.id
