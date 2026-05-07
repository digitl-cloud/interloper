"""Catalog API: serves component definitions for frontend consumption."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from interloper_api.dependencies import get_catalog

router = APIRouter()


@router.get("/")
def list_catalog(catalog: dict[str, Any] = Depends(get_catalog)) -> dict[str, Any]:
    """Return the full catalog."""
    return catalog


@router.get("/{key}")
def get_definition(key: str, catalog: dict[str, Any] = Depends(get_catalog)) -> dict[str, Any]:
    """Return a single component definition by key.

    Args:
        key: The component key.
        catalog: Injected catalog.

    Raises:
        HTTPException: If the key is not found.
    """
    from fastapi import HTTPException

    if key not in catalog:
        raise HTTPException(status_code=404, detail=f"Component '{key}' not found in catalog")
    return catalog[key]


@router.get("/kind/{kind}")
def list_by_kind(kind: str, catalog: dict[str, Any] = Depends(get_catalog)) -> dict[str, Any]:
    """Return all catalog entries matching a kind (source, asset, resource, destination).

    Args:
        kind: The component kind to filter by.
        catalog: Injected catalog.
    """
    return {k: v for k, v in catalog.items() if v.get("kind") == kind}
