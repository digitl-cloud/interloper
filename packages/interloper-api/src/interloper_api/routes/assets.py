"""Assets API: CRUD endpoints for assets and dependencies."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from interloper.errors import DataNotFoundError, NotFoundError
from interloper_db import Component, ComponentStatus, Profile, Store
from pydantic import BaseModel

from interloper_api.components import (
    DestinationResponse,
    destination_response,
    destination_rows,
    materializable,
    resource_map,
    timestamp,
    user_config,
)
from interloper_api.dependencies import (
    get_current_user,
    get_org_id,
    get_store,
    load_authorized,
    require_editor,
    require_viewer,
)

router = APIRouter()


# -- Request/Response models --------------------------------------------------


class AssetCreateRequest(BaseModel):
    """Request body for creating a standalone asset."""

    key: str
    config: dict[str, Any] | None = None
    resources: dict[str, str] | None = None
    destination_ids: list[str] | None = None


class AssetUpdateRequest(BaseModel):
    """Request body for updating an asset."""

    materializable: bool | None = None
    config: dict[str, Any] | None = None
    resources: dict[str, str] | None = None
    destination_ids: list[str] | None = None


class AssetResponse(BaseModel):
    """Response body for an asset."""

    id: UUID
    source_id: UUID | None
    org_id: UUID
    key: str
    materializable: bool
    status: ComponentStatus
    config: dict[str, Any] | None = None
    resources: dict[str, str] = {}
    destinations: list[DestinationResponse] = []
    created_at: str | None = None


class DependencyResponse(BaseModel):
    """Response body for an asset dependency relation."""

    asset_id: UUID
    upstream_asset_id: UUID


class AddDependencyRequest(BaseModel):
    """Request body for adding an asset dependency."""

    upstream_asset_id: UUID
    param_name: str


class PartitionRowCountItem(BaseModel):
    """A single partition row count entry."""

    partition: str
    row_count: int


class PartitionRowCountsResponse(BaseModel):
    """Response body for partition row counts."""

    asset_key: str
    partition_column: str
    counts: list[PartitionRowCountItem]


# -- Helpers ------------------------------------------------------------------


def _asset_to_response(asset: Component, store: Store) -> AssetResponse:
    """Convert an asset component row to an AssetResponse.

    Carries the asset's catalog-resolution ``status`` (derived from the same
    resolver hydration uses) so the UI can flag drifted assets. Source-owned
    assets resolve through their parent, so ``asset.parent`` must be loaded.
    """
    source_key = asset.parent.key if asset.parent else None
    return AssetResponse(
        id=asset.id,
        source_id=asset.parent_id,
        org_id=asset.org_id,
        key=asset.key,
        materializable=materializable(asset),
        status=store.asset_status(asset.key, source_key=source_key),
        config=user_config(asset),
        resources=resource_map(asset),
        destinations=[destination_response(d) for d in destination_rows(asset)],
        created_at=timestamp(asset.created_at),
    )


# -- CRUD endpoints -----------------------------------------------------------


@router.get("/")
def list_assets(
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[AssetResponse]:
    """List all assets for the current organisation."""
    assets = store.list_assets(org_id)
    return [_asset_to_response(a, store) for a in assets]


@router.post("/", status_code=201)
def create_asset(
    body: AssetCreateRequest,
    user: Profile = Depends(require_editor),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> AssetResponse:
    """Create a standalone asset."""
    asset = store.create_asset(
        org_id,
        key=body.key,
        config=body.config,
        resources=body.resources,
        destination_ids=body.destination_ids,
    )
    return _asset_to_response(asset, store)


@router.get("/dependencies")
def list_dependencies(
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[DependencyResponse]:
    """List all asset dependency relations for the current organisation."""
    deps = store.list_dependencies(org_id)
    return [
        DependencyResponse(
            asset_id=rel.src_id,
            upstream_asset_id=rel.dst_id,
        )
        for rel in deps
    ]


@router.get("/{asset_id}")
def get_asset(
    asset_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> AssetResponse:
    """Get a single asset by ID. Authorized by membership in the asset's org."""
    asset = load_authorized(store.get_asset, asset_id, user, store, label="Asset")
    return _asset_to_response(asset, store)


@router.put("/{asset_id}")
def update_asset(
    asset_id: UUID,
    body: AssetUpdateRequest,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> AssetResponse:
    """Update an asset's configuration, resources, or destinations."""
    load_authorized(store.get_asset, asset_id, user, store, label="Asset", minimum="editor")
    try:
        asset = store.update_asset(
            asset_id,
            materializable=body.materializable,
            config=body.config,
            resources=body.resources,
            destination_ids=body.destination_ids,
        )
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"Asset {asset_id} not found")
    return _asset_to_response(asset, store)


@router.delete("/{asset_id}")
def delete_asset(
    asset_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Delete a standalone asset."""
    load_authorized(store.get_asset, asset_id, user, store, label="Asset", minimum="editor")
    try:
        store.delete_asset(asset_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"Asset {asset_id} not found")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"status": "deleted"}


# -- Dependency endpoints -----------------------------------------------------


@router.post("/{asset_id}/dependencies", status_code=201)
def add_dependency(
    asset_id: UUID,
    body: AddDependencyRequest,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> DependencyResponse:
    """Add a dependency between two assets. Both must belong to the same org."""
    asset = load_authorized(store.get_asset, asset_id, user, store, label="Asset", minimum="editor")
    upstream = load_authorized(store.get_asset, body.upstream_asset_id, user, store, label="Asset", minimum="editor")
    if upstream.org_id != asset.org_id:
        raise HTTPException(status_code=404, detail=f"Asset {body.upstream_asset_id} not found")
    try:
        relation = store.add_dependency(asset_id, body.upstream_asset_id, body.param_name)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return DependencyResponse(
        asset_id=relation.src_id,
        upstream_asset_id=relation.dst_id,
    )


@router.delete("/{asset_id}/dependencies/{upstream_asset_id}", status_code=204)
def remove_dependency(
    asset_id: UUID,
    upstream_asset_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> None:
    """Remove an asset dependency."""
    load_authorized(store.get_asset, asset_id, user, store, label="Asset", minimum="editor")
    try:
        store.remove_dependency(asset_id, upstream_asset_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


# -- Partition endpoint -------------------------------------------------------


@router.get("/{asset_id}/partition-row-counts")
def get_partition_row_counts(
    asset_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> PartitionRowCountsResponse:
    """Get row counts grouped by partition for an asset."""
    load_authorized(store.get_asset, asset_id, user, store, label="Asset")
    try:
        il_asset = store.load_asset(asset_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if not il_asset.partitioning:
        raise HTTPException(status_code=400, detail="Asset is not partitioned")

    try:
        counts = il_asset.partition_row_counts()
    except NotImplementedError:
        raise HTTPException(status_code=400, detail="Destination does not support partition row counts")
    except DataNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return PartitionRowCountsResponse(
        asset_key=il_asset.key,
        partition_column=il_asset.partitioning.column,
        counts=[
            PartitionRowCountItem(partition=str(k), row_count=v)
            for k, v in sorted(counts.items())
        ],
    )
