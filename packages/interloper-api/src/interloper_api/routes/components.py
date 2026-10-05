"""Components API: one surface for every stored component, of every kind.

A generic CRUD for persisted instances of every component kind, plus the
edges a component holds. Operations on a component *class* (resolving a
FetchField's options, checking a connection) are catalog routes, addressed
by catalog key (:mod:`interloper_api.routes.catalog`).

The response shape is kind-agnostic — identity, drift ``status``, ``config``
(decoded for secret kinds on detail responses; the schema's ``x-public``
subset elsewhere), machine-owned ``state``, typed ``relations``, and the
components a row owns under ``children`` (a source's assets). The owner is
the unit: an owned component rides inside its owner and never lists on its
own, the way the catalog reaches an owned definition through its owner.
What a kind's config looks like and which relation types it may declare
come from the catalog (``/catalog``), not from this router.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Response
from interloper.component import KINDS
from interloper.errors import ComponentDriftError, DataNotFoundError
from interloper_db import Component, ComponentQuery, ComponentStatus, DeleteImpact, Page, Store
from pydantic import BaseModel

from interloper_api.dependencies import (
    CurrentUserDep,
    EditorDep,
    OrgIdDep,
    StoreDep,
    ViewerDep,
    load_authorized,
)
from interloper_api.routes.relations import RelationResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/components", tags=["components"])


# -- Request/Response models ---------------------------------------------------


class RelationEntry(BaseModel):
    """One relation binding in a create/update request."""

    dst_id: UUID


class RelationCreateRequest(RelationEntry):
    """Request body for adding one relation."""

    name: str


class RelationRef(BaseModel):
    """One relation binding in a component response, with enough of its target to name it.

    ``dst_key`` and ``dst_name`` ride along so a surface can label what a
    component is bound to without holding that component's own row.
    """

    dst_id: UUID
    dst_kind: str
    dst_key: str
    dst_name: str | None = None


class UsedByRef(BaseModel):
    """A component bound to something about to be deleted, as the 409 ``used_by`` payload names it."""

    id: str
    kind: str
    key: str
    name: str | None = None


class DeleteImpactResponse(BaseModel):
    """The preview behind a delete confirmation: who blocks it, who merely detaches."""

    blocking: list[UsedByRef]
    detaching: list[UsedByRef]

    @classmethod
    def from_impact(cls, impact: DeleteImpact) -> DeleteImpactResponse:
        """Convert the store's preview to its response model.

        Args:
            impact: The store's blocking and detaching referrers.

        Returns:
            The response model.
        """
        return cls(
            blocking=[UsedByRef.model_validate(ref) for ref in impact.blocking],
            detaching=[UsedByRef.model_validate(ref) for ref in impact.detaching],
        )


class ComponentCreateRequest(BaseModel):
    """Request body for creating a component of any kind.

    ``encrypted`` applies to secret kinds only: None encrypts whenever an
    encryption key is configured, an explicit bool forces it on or off.
    ``children`` applies to source kinds only and names the child asset keys
    to enable (None enables all of them). Every relation name listed in
    ``relations`` is replaced wholesale, so an empty list clears that name.
    """

    kind: str
    key: str
    name: str | None = None
    config: dict[str, Any] | None = None
    encrypted: bool | None = None
    children: list[str] | None = None
    relations: dict[str, list[RelationEntry]] | None = None


class ComponentUpdateRequest(BaseModel):
    """Request body for updating a component. Omitted facets are untouched."""

    name: str | None = None
    config: dict[str, Any] | None = None
    encrypted: bool | None = None
    children: list[str] | None = None
    relations: dict[str, list[RelationEntry]] | None = None


class ComponentResponse(BaseModel):
    """Response body for a component of any kind.

    A secret kind whose payload does not decrypt carries ``status``
    ``unreadable`` and no ``config`` at all, rather than a subset that was
    never read.
    """

    id: UUID
    org_id: UUID
    kind: str
    key: str
    name: str | None = None
    discriminator: str | None = None
    status: ComponentStatus
    config: dict[str, Any] | None = None
    state: dict[str, Any] | None = None
    encrypted: bool = False
    parent_id: UUID | None = None
    relations: dict[str, list[RelationRef]] = {}
    children: list[ComponentResponse] = []
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_row(cls, 
        row: Component,
        store: Store,
        *,
        include_config: bool,
        with_children: bool = True,
    ) -> ComponentResponse:
        """Convert a component row to its response model.

        The row is read once: ``status`` is the usability state hydration gates
        on (catalog resolution, then whether the payload decodes) and every
        view of the payload comes off that same reading. Secret kinds expose
        their decoded payload as ``config`` only when *include_config* is set
        (detail responses); otherwise ``config`` carries just the schema's
        ``x-public`` subset (operational fields such as a connection's
        ``auto_renew``). An ``unreadable`` row carries no ``config`` either
        way: the reason rides its ``status``, so the collection still lists and
        the UI can say what is wrong instead of the request failing over one
        row.

        Args:
            row: The component row to convert.
            store: The Store instance.
            include_config: Whether a secret kind's decoded config is exposed.
            with_children: Whether the components the row owns are nested in
                the response.

        Returns:
            The response model.
        """
        reading = store.components.read(row)

        config = reading.config
        if KINDS[row.kind].sensitive and not include_config:
            config = None if reading.status is ComponentStatus.UNREADABLE else reading.public_config

        return cls(
            id=row.id,
            org_id=row.org_id,
            kind=row.kind,
            key=row.key,
            name=row.name,
            discriminator=reading.discriminator,
            status=reading.status,
            config=config,
            state=row.state,
            encrypted=row.encrypted,
            parent_id=row.parent_id,
            relations=_relations_of(row),
            children=[
                ComponentResponse.from_row(child, store, include_config=include_config, with_children=False)
                for child in row.children
            ]
            if with_children
            else [],
            created_at=row.created_at,
            updated_at=row.updated_at,
        )


class PartitionRowCountItem(BaseModel):
    """A single partition row count entry."""

    partition: str
    row_count: int


class PartitionRowCountsResponse(BaseModel):
    """Response body for partition row counts."""

    asset_key: str
    partition_column: str
    counts: list[PartitionRowCountItem]


# -- Helpers -------------------------------------------------------------------


def _relations_of(row: Component) -> dict[str, list[RelationRef]]:
    """Group a component's outgoing relations by name.

    Args:
        row: The component row, with its ``out_relations`` eager-loaded.

    Returns:
        A ``{name: [bindings]}`` map of the row's outgoing relations.
    """
    grouped: dict[str, list[RelationRef]] = {}
    for relation in row.out_relations:
        grouped.setdefault(relation.name, []).append(
            RelationRef(
                dst_id=relation.dst_id,
                dst_kind=relation.dst_kind,
                dst_key=relation.dst.key if relation.dst else "",
                dst_name=relation.dst.name if relation.dst else None,
            )
        )
    return grouped


def _bindings(relations: dict[str, list[RelationEntry]] | None) -> dict[str, list[UUID]] | None:
    """Flatten a request's relation entries into the ids the store takes.

    Args:
        relations: The request's ``{name: [entries]}`` map, or None to leave
            every relation name untouched.

    Returns:
        A ``{name: [dst_id, ...]}`` map, or None when *relations* is None.
    """
    if relations is None:
        return None
    return {name: [entry.dst_id for entry in entries] for name, entries in relations.items()}


# -- Component endpoints -------------------------------------------------------


@router.get("")
def list_components(
    user: ViewerDep,
    org_id: OrgIdDep,
    store: StoreDep,
    query: Annotated[ComponentQuery, Query()],
) -> Page[ComponentResponse]:
    """List the organisation's root components, optionally filtered by kind(s) and text.

    An owned component (a source's asset) rides under its owner's
    ``children`` rather than listing on its own, so ``kind=asset`` yields the
    standalone assets alone.

    Args:
        user: The authenticated user.
        org_id: The active organisation UUID.
        store: The Store instance.
        query: The kinds, text and window to read.

    Returns:
        The page of components, secret configs withheld.
    """
    return store.components.list(org_id, query).map(
        lambda row: ComponentResponse.from_row(row, store, include_config=False)
    )


@router.get("/delete-impact")
def get_delete_impact(
    user: CurrentUserDep,
    store: StoreDep,
    component_id: Annotated[list[UUID], Query(alias="id")],
) -> DeleteImpactResponse:
    """Preview what deleting the given components does to the components bound to them.

    The rule the delete guard enforces, evaluated without deleting, so a
    confirmation can say up front what refuses the deletion and what merely
    loses a binding.

    Args:
        user: The authenticated user.
        store: The Store instance.
        component_id: The components about to be deleted, repeated per id.

    Returns:
        The blocking and detaching referrers.
    """
    for one in component_id:
        load_authorized(store.components.get, one, user, store, label="Component")
    return DeleteImpactResponse.from_impact(store.components.delete_impact(component_id))


@router.post("", status_code=201)
def create_component(
    body: ComponentCreateRequest,
    user: EditorDep,
    org_id: OrgIdDep,
    store: StoreDep,
) -> ComponentResponse:
    """Create a component of any kind.

    Args:
        body: The component spec: kind, key, name, config, and the optional
            encryption, children and relation facets.
        user: The authenticated user.
        org_id: The active organisation UUID.
        store: The Store instance.

    Returns:
        The created component, with its config decoded.
    """
    row = store.components.create(
        org_id,
        kind=body.kind,
        key=body.key,
        name=body.name,
        config=body.config,
        encrypted=body.encrypted,
        children=body.children,
        relations=_bindings(body.relations),
    )
    return ComponentResponse.from_row(row, store, include_config=True)


@router.get("/{component_id}")
def get_component(
    component_id: UUID,
    user: CurrentUserDep,
    store: StoreDep,
) -> ComponentResponse:
    """Get a single component by ID, including its decoded config payload.

    Args:
        component_id: The component UUID.
        user: The authenticated user.
        store: The Store instance.

    Returns:
        The component, with its config decoded.
    """
    row = load_authorized(store.components.get, component_id, user, store, label="Component")
    return ComponentResponse.from_row(row, store, include_config=True)


@router.put("/{component_id}")
def update_component(
    component_id: UUID,
    body: ComponentUpdateRequest,
    user: CurrentUserDep,
    store: StoreDep,
) -> ComponentResponse:
    """Update a component's spec. Omitted facets are untouched.

    Args:
        component_id: The component UUID.
        body: The facets to update; the omitted ones are left as they are.
        user: The authenticated user.
        store: The Store instance.

    Returns:
        The updated component, with its config decoded.
    """
    load_authorized(store.components.get, component_id, user, store, label="Component", minimum="editor")
    row = store.components.update(
        component_id,
        name=body.name,
        config=body.config,
        encrypted=body.encrypted,
        children=body.children,
        relations=_bindings(body.relations),
    )
    return ComponentResponse.from_row(row, store, include_config=True)


@router.delete("/{component_id}", status_code=204)
def delete_component(
    component_id: UUID,
    user: CurrentUserDep,
    store: StoreDep,
) -> Response:
    """Delete a component. Refused (409) while other components are bound to it.

    Args:
        component_id: The component UUID.
        user: The authenticated user.
        store: The Store instance.

    Returns:
        An empty 204 response.
    """
    load_authorized(store.components.get, component_id, user, store, label="Component", minimum="editor")
    store.components.delete(component_id)
    return Response(status_code=204)


# -- Relation endpoints --------------------------------------------------------


@router.post("/{component_id}/relations", status_code=201)
def add_relation(
    component_id: UUID,
    body: RelationCreateRequest,
    user: CurrentUserDep,
    store: StoreDep,
) -> RelationResponse:
    """Add one relation from a component (e.g. a dependency edge).

    Args:
        component_id: The source component's UUID.
        body: The relation to add: its name and target ``dst_id``.
        user: The authenticated user.
        store: The Store instance.

    Returns:
        The created relation row.

    Raises:
        HTTPException: 404 when the target belongs to another organisation.
    """
    source = load_authorized(store.components.get, component_id, user, store, label="Component", minimum="editor")
    destination_row = load_authorized(
        store.components.get, body.dst_id, user, store, label="Component", minimum="editor"
    )
    if destination_row.org_id != source.org_id:
        raise HTTPException(status_code=404, detail=f"Component {body.dst_id} not found")
    relation = store.relations.add(component_id, name=body.name, dst_id=body.dst_id)
    return RelationResponse.from_relation(relation)


@router.delete("/{component_id}/relations/{name}/{dst_id}", status_code=204)
def remove_relation(
    component_id: UUID,
    name: str,
    dst_id: UUID,
    user: CurrentUserDep,
    store: StoreDep,
) -> None:
    """Remove a component's relation of one name toward one destination.

    Refused (400) for required dependency names - repoint them instead.

    Args:
        component_id: The source component's UUID.
        name: The relation name to remove.
        dst_id: The target component's UUID.
        user: The authenticated user.
        store: The Store instance.
    """
    load_authorized(store.components.get, component_id, user, store, label="Component", minimum="editor")
    store.relations.delete(component_id, name=name, dst_id=dst_id)


# -- Partition endpoint --------------------------------------------------------


@router.get("/{component_id}/partition-row-counts")
def get_partition_row_counts(
    component_id: UUID,
    user: CurrentUserDep,
    store: StoreDep,
) -> PartitionRowCountsResponse:
    """Get row counts grouped by partition for an asset.

    Args:
        component_id: The asset component's UUID.
        user: The authenticated user.
        store: The Store instance.

    Returns:
        The asset's per-partition row counts, ordered by partition.

    Raises:
        HTTPException: 404 if the asset is missing, has drifted from the
            catalog or holds no data yet, 400 if it is not partitioned or its
            destination cannot count partitions, 500 for anything else.
    """
    load_authorized(store.components.get, component_id, user, store, label="Component")
    try:
        il_asset = store.components.load(component_id)
    except ComponentDriftError as e:
        raise HTTPException(status_code=404, detail=str(e))

    partitioning = getattr(il_asset, "partitioning", None)
    if not partitioning:
        raise HTTPException(status_code=400, detail="Component is not a partitioned asset")

    try:
        counts = il_asset.partition_row_counts()  # ty: ignore[unresolved-attribute]
    except NotImplementedError:
        raise HTTPException(status_code=400, detail="Destination does not support partition row counts")
    except DataNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:  # noqa: BLE001 — any destination failure is a 500, never a traceback
        raise HTTPException(status_code=500, detail=str(e))

    return PartitionRowCountsResponse(
        asset_key=type(il_asset).key,
        partition_column=partitioning.column,
        counts=[PartitionRowCountItem(partition=str(k), row_count=v) for k, v in sorted(counts.items())],
    )
