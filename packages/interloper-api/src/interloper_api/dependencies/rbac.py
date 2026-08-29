"""Role gates and org-scoped authorization for route handlers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from fastapi import Depends, HTTPException
from interloper.errors import NotFoundError
from interloper_db import Profile, Store

from interloper_api.dependencies.auth import get_current_user, get_org_id
from interloper_api.dependencies.state import get_store

_ROLE_RANK = {"viewer": 0, "editor": 1, "admin": 2}


def authorize_org_member(
    user: Profile,
    org_id: UUID,
    store: Store,
    *,
    minimum: str = "viewer",
    detail: str = "Not found",
) -> None:
    """Authorize access to a resource owned by ``org_id`` by membership.

    Unlike the ``require_*`` dependencies, which bind to the session's *active*
    organisation, this checks the user's role in the resource's organisation —
    so ID-addressed endpoints work for members of the owning org regardless of
    which org is currently selected. Non-members get a 404 carrying the same
    ``detail`` as a missing resource, so IDs don't act as an existence oracle;
    members with an insufficient role get a 403.

    Args:
        user: The authenticated user.
        org_id: The organisation that owns the resource.
        store: The Store instance.
        minimum: Minimum role required (``viewer``, ``editor``, ``admin``).
        detail: 404 detail, matching the route's missing-resource message.

    Raises:
        HTTPException: 404 if not a member, 403 if the role is insufficient.
    """
    role = store.auth.get_user_role(user.id, org_id)
    if role is None:
        raise HTTPException(status_code=404, detail=detail)
    if _ROLE_RANK.get(role, -1) < _ROLE_RANK[minimum]:
        raise HTTPException(status_code=403, detail=f"Requires {minimum} role or higher")


def load_authorized(
    fetch: Callable[[UUID], Any],
    entity_id: UUID,
    user: Profile,
    store: Store,
    *,
    label: str,
    minimum: str = "viewer",
) -> Any:
    """Fetch an org-owned entity and authorize the user by membership in its org.

    The one ID-addressed authorization pattern shared by every entity route:
    a missing entity and a non-member get the same 404 (IDs don't act as an
    existence oracle); a member with an insufficient role gets a 403.

    Args:
        fetch: Store getter taking the entity id (e.g. ``store.components.get``).
        entity_id: The entity UUID.
        user: The authenticated user.
        store: The Store instance.
        label: Entity label for the 404 detail (e.g. ``"Asset"``).
        minimum: Minimum role required in the owning organisation.

    Returns:
        Whatever *fetch* returned.

    Raises:
        HTTPException: 404 if missing or the user is not a member of the
            owning org, 403 if the role is insufficient.
    """
    detail = f"{label} {entity_id} not found"
    try:
        entity = fetch(entity_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=detail)
    authorize_org_member(user, entity.org_id, store, minimum=minimum, detail=detail)
    return entity


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
    role = store.auth.get_user_role(user.id, org_id)
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

    Args:
        user: The authenticated user, resolved from the session cookie.
        org_id: The active organisation UUID, resolved from the session.
        store: The Store instance.

    Returns:
        The authenticated Profile.
    """
    return _check_role("viewer", user, org_id, store)


def require_editor(
    user: Profile = Depends(get_current_user),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> Profile:
    """Require at least ``editor`` role.

    Args:
        user: The authenticated user, resolved from the session cookie.
        org_id: The active organisation UUID, resolved from the session.
        store: The Store instance.

    Returns:
        The authenticated Profile.
    """
    return _check_role("editor", user, org_id, store)


def require_admin(
    user: Profile = Depends(get_current_user),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> Profile:
    """Require ``admin`` role.

    Args:
        user: The authenticated user, resolved from the session cookie.
        org_id: The active organisation UUID, resolved from the session.
        store: The Store instance.

    Returns:
        The authenticated Profile.
    """
    return _check_role("admin", user, org_id, store)


def require_super_admin(
    user: Profile = Depends(get_current_user),
) -> Profile:
    """Require platform-wide super-admin privileges.

    Unlike the org-scoped role dependencies, this is not bound to the session's
    active organisation — it gates the cross-org admin surface.

    Args:
        user: The authenticated user, resolved from the session cookie.

    Returns:
        The authenticated Profile.

    Raises:
        HTTPException: 401 if not authenticated, 403 if not a super-admin.
    """
    if not user.is_super_admin:
        raise HTTPException(status_code=403, detail="Requires super-admin privileges")
    return user
