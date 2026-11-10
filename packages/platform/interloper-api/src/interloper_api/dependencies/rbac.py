"""Role gates over the organisation a request acts in, and the super-admin gate."""

from __future__ import annotations

from collections.abc import Callable
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends, HTTPException
from interloper_db import NotFoundError, Profile

from interloper_api.dependencies.auth import OrgDep, OrgScope, SessionUserDep


def load_authorized(
    fetch: Callable[[UUID], Any],
    entity_id: UUID,
    scope: OrgScope,
    *,
    label: str,
    minimum: str = "viewer",
) -> Any:
    """Fetch an entity of the organisation the request acts in, gated on the caller's role.

    The one ID-addressed authorization pattern shared by every entity route:
    a missing entity and one of another organisation get the same 404, so
    ids don't act as an existence oracle; a role below *minimum* gets a 403.

    Args:
        fetch: Store getter taking the entity id (e.g. ``store.components.get``).
        entity_id: The entity UUID.
        scope: The organisation the path names and the caller's role there.
        label: Entity label for the 404 detail (e.g. ``"Asset"``).
        minimum: Minimum role required.

    Returns:
        Whatever *fetch* returned.

    Raises:
        HTTPException: 404 when the entity is missing or belongs to another
            organisation, 403 when the role is insufficient.
    """
    detail = f"{label} {entity_id} not found"
    try:
        entity = fetch(entity_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=detail)
    if entity.org_id != scope.organisation.id:
        raise HTTPException(status_code=404, detail=detail)
    scope.require(minimum)
    return entity


def require_viewer(scope: OrgDep) -> OrgScope:
    """Require at least the ``viewer`` role in the path's organisation.

    Args:
        scope: The organisation the path names and the caller's role there.

    Returns:
        The scope.
    """
    return scope.require("viewer")


def require_editor(scope: OrgDep) -> OrgScope:
    """Require at least the ``editor`` role in the path's organisation.

    Args:
        scope: The organisation the path names and the caller's role there.

    Returns:
        The scope.
    """
    return scope.require("editor")


def require_admin(scope: OrgDep) -> OrgScope:
    """Require the ``admin`` role in the path's organisation.

    Args:
        scope: The organisation the path names and the caller's role there.

    Returns:
        The scope.
    """
    return scope.require("admin")


def require_super_admin(user: SessionUserDep) -> Profile:
    """Require platform-wide super-admin privileges, from the login session alone.

    No token acts across organisations: a super-admin's personal or access
    token reaches the organisations it names as their admin, never this surface.

    Args:
        user: The caller, authenticated by the session cookie.

    Returns:
        The authenticated Profile.

    Raises:
        HTTPException: 403 if not a super-admin.
    """
    if not user.is_super_admin:
        raise HTTPException(status_code=403, detail="Requires super-admin privileges")
    return user


ViewerDep = Annotated[OrgScope, Depends(require_viewer)]
EditorDep = Annotated[OrgScope, Depends(require_editor)]
AdminDep = Annotated[OrgScope, Depends(require_admin)]
SuperAdminDep = Annotated[Profile, Depends(require_super_admin)]
