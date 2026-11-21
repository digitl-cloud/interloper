"""How an organisation's collection is kept: edited in the app, or served from files by ``interloper dev``."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from interloper_api.dependencies import StoreDep, require_viewer

org_router = APIRouter(prefix="/collection", tags=["collection"], dependencies=[Depends(require_viewer)])


class CollectionResponse(BaseModel):
    """How the collection is kept.

    ``served_from`` names the files ``interloper dev`` serves it from, which
    makes it read-only in the app; ``None`` when the app edits it.
    """

    served_from: str | None = None


@org_router.get("")
def read_collection(store: StoreDep) -> CollectionResponse:
    """Say how the organisation's collection is kept.

    Args:
        store: The store, which knows whether files serve the collection.

    Returns:
        The files it is served from, if any.
    """
    return CollectionResponse(served_from=store.served_from)
