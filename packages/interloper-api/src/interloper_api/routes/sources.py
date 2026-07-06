"""Sources API: CRUD for source instances."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from interloper.errors import NotFoundError
from interloper_db import Component, ComponentStatus, Profile, Store
from pydantic import BaseModel

from interloper_api.components import destination_rows, materializable, resource_map
from interloper_api.dependencies import (
    authorize_org_member,
    get_current_user,
    get_org_id,
    get_store,
    require_editor,
    require_viewer,
)

router = APIRouter()


class SourceCreateRequest(BaseModel):
    """Request body for creating or updating a source."""

    key: str
    name: str
    config: dict[str, Any] | None = None
    resources: dict[str, str] | None = None
    asset_keys: list[str] | None = None
    destination_ids: list[str] | None = None
    cross_deps: dict[str, dict[str, str]] | None = None


class AssetResponse(BaseModel):
    """Nested asset in source response."""

    id: UUID
    key: str
    materializable: bool
    status: ComponentStatus


class DestinationResponse(BaseModel):
    """Nested destination in source response."""

    id: UUID
    key: str
    name: str | None = None
    config: dict[str, Any] | None = None
    resources: dict[str, str] = {}
    created_at: str | None = None


class SourceResponse(BaseModel):
    """Response body for a source."""

    id: UUID
    org_id: UUID
    key: str
    name: str
    config: dict[str, Any] | None = None
    status: ComponentStatus
    resources: dict[str, str] = {}
    destinations: list[DestinationResponse] = []
    assets: list[AssetResponse] = []
    created_at: str | None = None


def _build_source_response(source: Component, store: Store) -> SourceResponse:
    """Convert a source component row to a SourceResponse.

    Each source and asset carries its catalog-resolution ``status`` so the
    UI can flag drift. Status is derived from the same resolver hydration
    uses — detection is just building the response, not a separate pass.
    """
    return SourceResponse(
        id=source.id,
        org_id=source.org_id,
        key=source.key,
        name=source.name or "",
        config=source.config,
        status=store.source_status(source.key),
        resources=resource_map(source),
        destinations=[
            DestinationResponse(
                id=destination.id,
                key=destination.key,
                name=destination.name,
                config=destination.config,
                resources=resource_map(destination),
                created_at=str(destination.created_at) if destination.created_at else None,
            )
            for destination in destination_rows(source)
        ],
        assets=[
            AssetResponse(
                id=child.id,
                key=child.key,
                materializable=materializable(child),
                status=store.asset_status(child.key, source_key=source.key),
            )
            for child in source.children
        ],
        created_at=str(source.created_at) if source.created_at else None,
    )


@router.get("/")
def list_sources(
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[SourceResponse]:
    """List all sources for the current organisation."""
    return [_build_source_response(source, store) for source in store.list_sources(org_id)]


@router.post("/")
def create_source(
    body: SourceCreateRequest,
    user: Profile = Depends(require_editor),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> SourceResponse:
    """Create a new source."""
    source = store.create_source(
        org_id,
        key=body.key,
        name=body.name,
        config=body.config,
        resources=body.resources,
        asset_keys=body.asset_keys,
        destination_ids=body.destination_ids,
        cross_deps=body.cross_deps,
    )
    return _build_source_response(source, store)


def _authorize_source(source_id: UUID, user: Profile, store: Store, *, minimum: str = "viewer") -> None:
    """Authorize the user by membership in the source's org.

    Raises:
        HTTPException: 404 if missing or the user is not a member of the
            owning org, 403 if the role is insufficient.
    """
    try:
        source = store.get_source(source_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"Source {source_id} not found")
    authorize_org_member(user, source.org_id, store, minimum=minimum, detail=f"Source {source_id} not found")


@router.put("/{source_id}")
def update_source(
    source_id: UUID,
    body: SourceCreateRequest,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> SourceResponse:
    """Update a source."""
    _authorize_source(source_id, user, store, minimum="editor")
    try:
        source = store.update_source(
            source_id,
            name=body.name,
            config=body.config,
            resources=body.resources,
            asset_keys=body.asset_keys,
            destination_ids=body.destination_ids,
            cross_deps=body.cross_deps,
        )
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"Source {source_id} not found")
    return _build_source_response(source, store)


@router.delete("/{source_id}")
def delete_source(
    source_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Delete a source. Assets cascade via FK."""
    _authorize_source(source_id, user, store, minimum="editor")
    store.delete_source(source_id)
    return {"status": "deleted"}
