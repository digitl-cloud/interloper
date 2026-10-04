"""Relations API: the edges between an organisation's components, org-wide.

One component's edges are written through its own routes
(``/components/{id}/relations``); this router reads them across the whole
organisation, the way the graph and the dependency views need them.
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query
from interloper_db import Page, RelationQuery
from interloper_db.models import ComponentRelation
from pydantic import BaseModel

from interloper_api.dependencies import OrgIdDep, StoreDep, ViewerDep

router = APIRouter(prefix="/relations", tags=["relations"])


class RelationResponse(BaseModel):
    """One named, directed edge between two components.

    ``src_kind`` rides along so the graph and upstream views can filter
    asset-to-asset rows without a second lookup.
    """

    src_id: UUID
    name: str
    dst_id: UUID
    src_kind: str
    dst_kind: str

    @classmethod
    def from_relation(cls, relation: ComponentRelation) -> RelationResponse:
        """Describe a relation row.

        Args:
            relation: The relation row.

        Returns:
            The response model.
        """
        return cls(
            src_id=relation.src_id,
            name=relation.name,
            dst_id=relation.dst_id,
            src_kind=relation.src_kind,
            dst_kind=relation.dst_kind,
        )


@router.get("")
def list_relations(
    user: ViewerDep,
    org_id: OrgIdDep,
    store: StoreDep,
    query: Annotated[RelationQuery, Query()],
) -> Page[RelationResponse]:
    """List the organisation's relations, optionally filtered by name and endpoint kinds.

    Args:
        user: The authenticated user, required to hold at least the viewer role.
        org_id: The active organisation UUID.
        store: The Store instance.
        query: The name, source and destination kinds, and the window to read.

    Returns:
        The page of relations.
    """
    return store.relations.list(org_id, query).map(RelationResponse.from_relation)
