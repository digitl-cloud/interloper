"""Assets API: CRUD endpoints for assets and dependencies."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from interloper.errors import DataNotFoundError, NotFoundError
from interloper_db import Profile, Store
from interloper_db.models import Asset, AssetResource, Destination, DestinationResource
from pydantic import BaseModel
from sqlmodel import Session, select

from interloper_api.dependencies import get_org_id, get_store, require_editor, require_viewer

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


class DestinationResponse(BaseModel):
    """Nested destination in asset response."""

    id: UUID
    key: str
    name: str | None = None
    config: dict[str, Any] | None = None
    resources: dict[str, str] = {}
    created_at: str | None = None


class AssetResponse(BaseModel):
    """Response body for an asset."""

    id: UUID
    source_id: UUID | None
    org_id: UUID
    key: str
    materializable: bool
    config: dict[str, Any] | None = None
    resources: dict[str, str] = {}
    destinations: list[DestinationResponse] = []
    created_at: str | None = None


class DependencyResponse(BaseModel):
    """Response body for an asset dependency edge."""

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


def _resource_map(junction_cls: type, fk_column: str, fk_value: UUID) -> dict[str, str]:
    """Build a {slot_key: resource_id} map from junction rows."""
    from interloper_db.engine import get_engine

    col = getattr(junction_cls, fk_column)
    with Session(get_engine()) as s:
        rows = s.exec(select(junction_cls).where(col == fk_value)).all()
    return {r.key: str(r.resource_id) for r in rows}


def _dest_to_response(dest: Destination) -> DestinationResponse:
    return DestinationResponse(
        id=dest.id,  # type: ignore[arg-type]
        key=dest.key,
        name=dest.name,
        config=dest.config,
        resources=_resource_map(DestinationResource, "destination_id", dest.id),  # type: ignore[arg-type]
        created_at=str(dest.created_at) if dest.created_at else None,
    )


def _asset_to_response(asset: Asset) -> AssetResponse:
    """Convert a DB Asset to an AssetResponse."""
    return AssetResponse(
        id=asset.id,  # type: ignore[arg-type]
        source_id=asset.source_id,
        org_id=asset.org_id,
        key=asset.key,
        materializable=asset.materializable,
        config=asset.config,
        resources=_resource_map(AssetResource, "asset_id", asset.id),  # type: ignore[arg-type]
        destinations=[_dest_to_response(d) for d in asset.destinations],
        created_at=str(asset.created_at) if asset.created_at else None,
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
    return [_asset_to_response(a) for a in assets]


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
    return _asset_to_response(asset)


@router.get("/dependencies")
def list_dependencies(
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[DependencyResponse]:
    """List all asset dependency edges for the current organisation."""
    deps = store.list_dependencies(org_id)
    return [
        DependencyResponse(
            asset_id=d.asset_id,
            upstream_asset_id=d.upstream_asset_id,
        )
        for d in deps
    ]


@router.get("/{asset_id}")
def get_asset(
    asset_id: UUID,
    user: Profile = Depends(require_viewer),
    store: Store = Depends(get_store),
) -> AssetResponse:
    """Get a single asset by ID."""
    try:
        asset = store.get_asset(asset_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"Asset {asset_id} not found")
    return _asset_to_response(asset)


@router.put("/{asset_id}")
def update_asset(
    asset_id: UUID,
    body: AssetUpdateRequest,
    user: Profile = Depends(require_editor),
    store: Store = Depends(get_store),
) -> AssetResponse:
    """Update an asset's configuration, resources, or destinations."""
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
    return _asset_to_response(asset)


@router.delete("/{asset_id}")
def delete_asset(
    asset_id: UUID,
    user: Profile = Depends(require_editor),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Delete a standalone asset."""
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
    user: Profile = Depends(require_editor),
    store: Store = Depends(get_store),
) -> DependencyResponse:
    """Add a dependency between two assets."""
    try:
        dep = store.add_dependency(asset_id, body.upstream_asset_id, body.param_name)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return DependencyResponse(
        asset_id=dep.asset_id,
        upstream_asset_id=dep.upstream_asset_id,
    )


@router.delete("/{asset_id}/dependencies/{upstream_asset_id}", status_code=204)
def remove_dependency(
    asset_id: UUID,
    upstream_asset_id: UUID,
    user: Profile = Depends(require_editor),
    store: Store = Depends(get_store),
) -> None:
    """Remove an asset dependency."""
    try:
        store.remove_dependency(asset_id, upstream_asset_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


# -- Partition endpoint -------------------------------------------------------


@router.get("/{asset_id}/partition-row-counts")
def get_partition_row_counts(
    asset_id: UUID,
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> PartitionRowCountsResponse:
    """Get row counts grouped by partition for an asset."""
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
