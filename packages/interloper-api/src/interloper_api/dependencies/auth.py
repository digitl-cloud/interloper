"""Request-scoped identity: who is calling, and which organisation they are in."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import Cookie, Depends, HTTPException
from interloper_db import Organisation, Profile, Store
from interloper_db.models import AuthSession

from interloper_api.dependencies.state import get_store


def get_session_context(
    store: Store = Depends(get_store),
    session_token: str | None = Cookie(default=None),
) -> tuple[Profile, AuthSession]:
    """Resolve the caller and their login session from the session cookie.

    Args:
        store: The Store instance.
        session_token: Session cookie value.

    Returns:
        ``(Profile, AuthSession)``.

    Raises:
        HTTPException: 401 if not authenticated or the session is invalid or expired.
    """
    if not session_token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    result = store.sessions.resolve(session_token)
    if not result:
        raise HTTPException(status_code=401, detail="Invalid or expired session")
    return result


def get_current_user(
    context: tuple[Profile, AuthSession] = Depends(get_session_context),
) -> Profile:
    """Resolve the current user from the session.

    Args:
        context: The caller and their session.

    Returns:
        The authenticated Profile.
    """
    profile, _ = context
    return profile


def get_current_org(
    context: tuple[Profile, AuthSession] = Depends(get_session_context),
    store: Store = Depends(get_store),
) -> Organisation:
    """Resolve the organisation the session is working in.

    Args:
        context: The caller and their session.
        store: The Store instance.

    Returns:
        The active Organisation.

    Raises:
        HTTPException: 400 if no organisation is selected.
    """
    _, session_row = context
    if not session_row.organisation_id:
        raise HTTPException(status_code=400, detail="No organisation selected")
    return store.organisations.get(session_row.organisation_id)


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


# -- Dependency aliases --------------------------------------------------------

CurrentUserDep = Annotated[Profile, Depends(get_current_user)]
SessionContextDep = Annotated[tuple[Profile, AuthSession], Depends(get_session_context)]
OrgIdDep = Annotated[UUID, Depends(get_org_id)]
