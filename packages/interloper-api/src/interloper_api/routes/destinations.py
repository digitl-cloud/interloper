"""Destinations API: CRUD for standalone destination instances."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from interloper.errors import NotFoundError
from interloper_db import Component, Profile, Store
from pydantic import BaseModel

from interloper_api.components import resource_map
from interloper_api.dependencies import (
    authorize_org_member,
    get_current_user,
    get_org_id,
    get_store,
    require_editor,
    require_viewer,
)

router = APIRouter()


class DestinationCreateRequest(BaseModel):
    """Request body for creating/updating a destination."""

    key: str
    name: str | None = None
    config: dict[str, Any] | None = None
    resources: dict[str, str] | None = None


class DestinationResponse(BaseModel):
    """Response body for a destination."""

    id: UUID
    key: str
    name: str | None = None
    config: dict[str, Any] | None = None
    resources: dict[str, str] = {}
    created_at: str | None = None


def _to_response(dest: Component) -> DestinationResponse:
    return DestinationResponse(
        id=dest.id,
        key=dest.key,
        name=dest.name,
        config=dest.config,
        resources=resource_map(dest),
        created_at=str(dest.created_at) if dest.created_at else None,
    )


@router.get("/")
def list_destinations(
    user: object = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[DestinationResponse]:
    """List all destinations for the current organisation."""
    destinations = store.list_destinations(org_id)
    return [_to_response(d) for d in destinations]


@router.post("/")
def create_destination(
    body: DestinationCreateRequest,
    user: object = Depends(require_editor),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> DestinationResponse:
    """Create a new destination."""
    dest = store.create_destination(
        org_id,
        key=body.key,
        name=body.name,
        config=body.config,
        resources=body.resources,
    )
    return _to_response(dest)


def _authorize_destination(destination_id: UUID, user: Profile, store: Store, *, minimum: str = "viewer") -> None:
    """Authorize the user by membership in the destination's org.

    Raises:
        HTTPException: 404 if missing or the user is not a member of the
            owning org, 403 if the role is insufficient.
    """
    try:
        dest = store.get_destination(destination_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"Destination {destination_id} not found")
    authorize_org_member(user, dest.org_id, store, minimum=minimum, detail=f"Destination {destination_id} not found")


@router.put("/{destination_id}")
def update_destination(
    destination_id: UUID,
    body: DestinationCreateRequest,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> DestinationResponse:
    """Update a destination."""
    _authorize_destination(destination_id, user, store, minimum="editor")
    try:
        dest = store.update_destination(
            destination_id,
            name=body.name,
            config=body.config,
            resources=body.resources,
        )
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return _to_response(dest)


@router.delete("/{destination_id}")
def delete_destination(
    destination_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Delete a destination."""
    _authorize_destination(destination_id, user, store, minimum="editor")
    store.delete_destination(destination_id)
    return {"status": "deleted"}
