"""Resources API: CRUD for typed, optionally encrypted resources."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from interloper.catalog.base import Catalog
from interloper.errors import NotFoundError
from interloper_db import Profile, Store
from pydantic import BaseModel

from interloper_api.components import timestamp
from interloper_api.dependencies import (
    get_catalog,
    get_current_user,
    get_org_id,
    get_store,
    load_authorized,
    require_editor,
    require_viewer,
)

router = APIRouter()

# Kinds that are not resource kinds — top-level component categories.
_NON_RESOURCE_KINDS = {"source", "asset", "destination"}


class ResourceCreateRequest(BaseModel):
    """Request body for creating or updating a resource."""

    kind: str
    key: str
    name: str
    data: dict[str, Any]
    # None (default) encrypts when an encryption key is configured; pass an
    # explicit bool to force encryption on/off.
    encrypted: bool | None = None


class ResourceResponse(BaseModel):
    """Response body for a resource."""

    id: UUID
    org_id: UUID
    kind: str
    key: str
    name: str
    encrypted: bool
    created_at: str | None = None
    updated_at: str | None = None


class ResourceDetailResponse(ResourceResponse):
    """Response body for a single resource, including its data payload."""

    data: dict[str, Any] = {}


@router.get("/kinds")
def list_resource_kinds(
    catalog: Catalog = Depends(get_catalog),
) -> list[str]:
    """Return distinct resource kinds from the catalog.

    Resource kinds are all component kinds except the top-level categories
    (source, asset, destination). Currently this yields kinds like
    ``connection`` and ``config``.
    """
    kinds = {defn.kind for defn in catalog.components.values() if defn.kind and defn.kind not in _NON_RESOURCE_KINDS}
    return sorted(kinds)


@router.get("/")
def list_resources(
    kind: str | None = None,
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[ResourceResponse]:
    """List resources for the current organisation, optionally filtered by kind."""
    resources = store.list_resources(org_id, kind=kind)
    return [
        ResourceResponse(
            id=r.id,
            org_id=r.org_id,
            kind=r.kind,
            key=r.key,
            name=r.name or "",
            encrypted=r.encrypted,
            created_at=timestamp(r.created_at),
            updated_at=timestamp(r.updated_at),
        )
        for r in resources
    ]


@router.get("/{resource_id}")
def get_resource(
    resource_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> ResourceDetailResponse:
    """Get a single resource by ID, including its data payload.

    Authorized by membership in the resource's org.
    """
    r = load_authorized(store.load_resource, resource_id, user, store, label="Resource")

    return ResourceDetailResponse(
        id=r.id,
        org_id=r.org_id,
        kind=r.kind,
        key=r.key,
        name=r.name or "",
        encrypted=r.encrypted,
        data=store.decode_resource_data(r),
        created_at=timestamp(r.created_at),
        updated_at=timestamp(r.updated_at),
    )


@router.post("/")
def create_resource(
    body: ResourceCreateRequest,
    user: Profile = Depends(require_editor),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> ResourceResponse:
    """Create a new resource."""
    resource = store.create_resource(
        org_id,
        kind=body.kind,
        key=body.key,
        name=body.name,
        data=body.data,
        encrypted=body.encrypted,
    )
    return ResourceResponse(
        id=resource.id,
        org_id=resource.org_id,
        kind=resource.kind,
        key=resource.key,
        name=resource.name or "",
        encrypted=resource.encrypted,
        created_at=timestamp(resource.created_at),
        updated_at=timestamp(resource.updated_at),
    )


@router.put("/{resource_id}")
def update_resource(
    resource_id: UUID,
    body: ResourceCreateRequest,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> ResourceResponse:
    """Update an existing resource."""
    load_authorized(store.load_resource, resource_id, user, store, label="Resource", minimum="editor")
    try:
        resource = store.update_resource(
            resource_id,
            kind=body.kind,
            key=body.key,
            name=body.name,
            data=body.data,
            encrypted=body.encrypted,
        )
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"Resource {resource_id} not found")
    return ResourceResponse(
        id=resource.id,
        org_id=resource.org_id,
        kind=resource.kind,
        key=resource.key,
        name=resource.name or "",
        encrypted=resource.encrypted,
        created_at=timestamp(resource.created_at),
        updated_at=timestamp(resource.updated_at),
    )


@router.delete("/{resource_id}")
def delete_resource(
    resource_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Delete a resource."""
    load_authorized(store.load_resource, resource_id, user, store, label="Resource", minimum="editor")
    store.delete_resource(resource_id)
    return {"status": "deleted"}
