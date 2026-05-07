"""Sources API: CRUD for source instances."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from interloper.errors import NotFoundError
from interloper_db import Profile, Store
from interloper_db.models import Destination, DestinationResource, Source, SourceResource
from pydantic import BaseModel
from sqlalchemy.orm import selectinload
from sqlmodel import Session, select

from interloper_api.dependencies import get_org_id, get_store, require_editor, require_viewer

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
    resources: dict[str, str] = {}
    destinations: list[DestinationResponse] = []
    assets: list[AssetResponse] = []
    created_at: str | None = None


def _resource_map(session: Session, junction_cls: type, fk_column: str, fk_value: UUID) -> dict[str, str]:
    """Build a {slot_key: resource_id} map from junction rows."""
    col = getattr(junction_cls, fk_column)
    rows = session.exec(select(junction_cls).where(col == fk_value)).all()
    return {r.key: str(r.resource_id) for r in rows}


def _build_source_response(session: Session, source: Source) -> SourceResponse:
    """Convert a DB Source to a SourceResponse within a session."""
    return SourceResponse(
        id=source.id,  # type: ignore[arg-type]
        org_id=source.org_id,
        key=source.key,
        name=source.name,
        config=source.config,
        resources=_resource_map(session, SourceResource, "source_id", source.id),  # type: ignore[arg-type]
        destinations=[
            DestinationResponse(
                id=d.id,  # type: ignore[arg-type]
                key=d.key,
                name=d.name,
                config=d.config,
                resources=_resource_map(session, DestinationResource, "destination_id", d.id),  # type: ignore[arg-type]
                created_at=str(d.created_at) if d.created_at else None,
            )
            for d in source.destinations
        ],
        assets=[
            AssetResponse(id=a.id, key=a.key, materializable=a.materializable)  # type: ignore[arg-type]
            for a in source.assets
        ],
        created_at=str(source.created_at) if source.created_at else None,
    )


def _load_source_for_response(source_id: UUID) -> SourceResponse:
    """Load a source with relations and build the response."""
    from interloper_db.engine import get_engine

    with Session(get_engine()) as session:
        source = session.get(
            Source,
            source_id,
            options=[
                selectinload(Source.assets),  # type: ignore[arg-type]
                selectinload(Source.resources),  # type: ignore[arg-type]
                selectinload(Source.destinations).selectinload(Destination.resources),  # type: ignore[arg-type]
            ],
        )
        if not source:
            raise NotFoundError(f"Source {source_id} not found")
        return _build_source_response(session, source)


@router.get("/")
def list_sources(
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[SourceResponse]:
    """List all sources for the current organisation."""
    from interloper_db.engine import get_engine

    sources = store.list_sources(org_id)
    with Session(get_engine()) as session:
        return [_build_source_response(session, s) for s in sources]


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
    return _load_source_for_response(source.id)  # type: ignore[arg-type]


@router.put("/{source_id}")
def update_source(
    source_id: UUID,
    body: SourceCreateRequest,
    user: Profile = Depends(require_editor),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> SourceResponse:
    """Update a source."""
    try:
        store.update_source(
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
    return _load_source_for_response(source_id)


@router.delete("/{source_id}")
def delete_source(
    source_id: UUID,
    user: Profile = Depends(require_editor),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Delete a source. Assets cascade via FK."""
    store.delete_source(source_id)
    return {"status": "deleted"}
