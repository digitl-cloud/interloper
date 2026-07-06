"""Destinations API: CRUD for standalone destination instances."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from interloper.errors import NotFoundError
from interloper_db import Profile, Store
from pydantic import BaseModel

from interloper_api.components import DestinationResponse, destination_response
from interloper_api.dependencies import (
    get_current_user,
    get_org_id,
    get_store,
    load_authorized,
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


@router.get("/")
def list_destinations(
    user: object = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[DestinationResponse]:
    """List all destinations for the current organisation."""
    destinations = store.list_destinations(org_id)
    return [destination_response(d) for d in destinations]


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
    return destination_response(dest)


@router.put("/{destination_id}")
def update_destination(
    destination_id: UUID,
    body: DestinationCreateRequest,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> DestinationResponse:
    """Update a destination."""
    load_authorized(store.get_destination, destination_id, user, store, label="Destination", minimum="editor")
    try:
        dest = store.update_destination(
            destination_id,
            name=body.name,
            config=body.config,
            resources=body.resources,
        )
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return destination_response(dest)


@router.delete("/{destination_id}")
def delete_destination(
    destination_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Delete a destination."""
    load_authorized(store.get_destination, destination_id, user, store, label="Destination", minimum="editor")
    store.delete_destination(destination_id)
    return {"status": "deleted"}
