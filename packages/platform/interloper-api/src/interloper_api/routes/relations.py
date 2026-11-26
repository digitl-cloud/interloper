"""Relations API: the edges between an organisation's components, org-wide.

One component's edges are written through its own routes
(``/components/{id}/relations``); this router reads them across the whole
organisation, the way the graph and the dependency views need them.
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query
from interloper_db import Edge, Page, RelationQuery
from interloper_db.models import ComponentRelation
from pydantic import BaseModel

from interloper_api.dependencies import StoreDep, ViewerDep

org_router = APIRouter(prefix="/relations", tags=["relations"])


class RelationResponse(BaseModel):
    """One relation row: a component, under one of its relations, points at another.

    ``from_kind`` rides along so the graph and upstream views can filter
    asset-to-asset rows without a second lookup.
    """

    from_id: UUID
    relation: str
    to_id: UUID
    from_kind: str
    to_kind: str

    @classmethod
    def from_relation(cls, relation: Edge | ComponentRelation) -> RelationResponse:
        """Describe a relation: an edge a listing read, or a row a write stored.

        Args:
            relation: The edge or the stored row.

        Returns:
            The response model.
        """
        return cls(
            from_id=relation.from_id,
            relation=relation.relation,
            to_id=relation.to_id,
            from_kind=relation.from_kind,
            to_kind=relation.to_kind,
        )


@org_router.get("")
def list_relations(
    scope: ViewerDep,
    store: StoreDep,
    query: Annotated[RelationQuery, Query()],
) -> Page[RelationResponse]:
    """List the organisation's relation rows, optionally filtered by relation and endpoint kinds.

    Args:
        scope: The organisation the path names, the caller at least a viewer there.
        store: The Store instance.
        query: The relation, the from and to kinds, and the window to read.

    Returns:
        The page of relations.
    """
    return store.relations.list(scope.organisation.id, query).map(RelationResponse.from_relation)
