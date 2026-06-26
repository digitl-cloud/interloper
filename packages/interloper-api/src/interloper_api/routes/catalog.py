"""Catalog API: serves component definitions for frontend consumption."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from interloper.catalog.base import Catalog

from interloper_api.dependencies import get_catalog

router = APIRouter()


@router.get("/")
def list_catalog(catalog: Catalog = Depends(get_catalog)) -> dict[str, Any]:
    """Return the full catalog."""
    return catalog.dump()


@router.get("/{key}")
def get_definition(key: str, catalog: Catalog = Depends(get_catalog)) -> dict[str, Any]:
    """Return a single component definition by key.

    Args:
        key: The component key.
        catalog: Injected catalog.

    Raises:
        HTTPException: If the key is not found.
    """
    defn = catalog.get(key)
    if defn is None:
        raise HTTPException(status_code=404, detail=f"Component '{key}' not found in catalog")
    return defn.model_dump(mode="json")


@router.get("/kind/{kind}")
def list_by_kind(kind: str, catalog: Catalog = Depends(get_catalog)) -> dict[str, Any]:
    """Return all catalog entries matching a kind (source, asset, resource, destination).

    Args:
        kind: The component kind to filter by.
        catalog: Injected catalog.
    """
    return {k: v.model_dump(mode="json") for k, v in catalog.components.items() if v.kind == kind}
