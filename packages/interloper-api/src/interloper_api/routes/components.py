"""Components API: one surface for every component operation.

A generic CRUD for persisted instances of every component kind, plus the
type-level operations that execute a component class against a candidate,
unsaved config — resolving a FetchField's options (``/resolve``) and
checking a connection (``/check``).

The response shape is kind-agnostic — identity, drift ``status``, ``config``
(decoded for secret kinds on detail responses), machine-owned ``state``,
typed ``relations``, and one level of ``children`` (a source's assets).
What a kind's config looks like and which relation types it may declare
come from the catalog (``/catalog``), not from this router.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Annotated, Any, Literal
from uuid import UUID

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from interloper.catalog.base import Catalog
from interloper.component import KINDS
from interloper.connection.base import Connection
from interloper.errors import (
    CatalogKeyError,
    ComponentDriftError,
    ConfigError,
    ConnectionCheckError,
    DataNotFoundError,
    InUseError,
    NotFoundError,
)
from interloper.resource.fields import is_fetch_field_provider
from interloper.utils.concurrency import invoke
from interloper.utils.imports import import_from_path
from interloper_db import Component, ComponentStatus, Profile, Store
from pydantic import BaseModel, Field, ValidationError

from interloper_api.dependencies import (
    get_catalog,
    get_current_user,
    get_org_id,
    get_store,
    load_authorized,
    require_editor,
    require_viewer,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# -- Request/Response models --------------------------------------------------


class RelationEntry(BaseModel):
    """One relation binding in a create/update request."""

    dst_id: UUID
    slot: str = ""


class RelationCreateRequest(RelationEntry):
    """Request body for adding one relation."""

    type: str


class RelationRef(BaseModel):
    """One relation binding in a component response."""

    dst_id: UUID
    slot: str = ""
    dst_kind: str


class RelationResponse(BaseModel):
    """An org-wide relation row (graph edges, dependency lists)."""

    src_id: UUID
    dst_id: UUID
    type: str
    slot: str
    dst_kind: str


class ComponentCreateRequest(BaseModel):
    """Request body for creating a component of any kind."""

    kind: str
    key: str
    name: str | None = None
    config: dict[str, Any] | None = None
    # Secret kinds only: None (default) encrypts when a key is configured;
    # pass an explicit bool to force encryption on/off.
    encrypted: bool | None = None
    # Source kinds only: which child asset keys to enable (None = all).
    children: list[str] | None = None
    # {type: [bindings]} — each listed type is replaced ([] clears it).
    relations: dict[str, list[RelationEntry]] | None = None


class ComponentUpdateRequest(BaseModel):
    """Request body for updating a component. Omitted facets are untouched."""

    name: str | None = None
    config: dict[str, Any] | None = None
    encrypted: bool | None = None
    children: list[str] | None = None
    relations: dict[str, list[RelationEntry]] | None = None


class ComponentResponse(BaseModel):
    """Response body for a component of any kind."""

    id: UUID
    org_id: UUID
    kind: str
    key: str
    name: str | None = None
    status: ComponentStatus
    config: dict[str, Any] | None = None
    state: dict[str, Any] | None = None
    encrypted: bool = False
    parent_id: UUID | None = None
    relations: dict[str, list[RelationRef]] = {}
    children: list[ComponentResponse] = []
    created_at: str | None = None
    updated_at: str | None = None


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


def _relations_of(row: Component) -> dict[str, list[RelationRef]]:
    grouped: dict[str, list[RelationRef]] = {}
    for rel in row.out_relations:
        grouped.setdefault(rel.type, []).append(RelationRef(dst_id=rel.dst_id, slot=rel.slot, dst_kind=rel.dst_kind))
    return grouped


def _to_response(
    row: Component,
    store: Store,
    *,
    include_config: bool,
    parent_key: str | None = None,
    with_children: bool = True,
) -> ComponentResponse:
    """Convert a component row to its response model.

    ``status`` is the catalog-resolution state (drift detection), derived from
    the same resolver hydration uses. Secret kinds expose their decoded
    payload as ``config`` only when *include_config* is set (detail responses).
    """
    if row.kind == "asset":
        source_key = parent_key if parent_key is not None else (row.parent.key if row.parent else None)
        status = store.asset_status(row.key, source_key=source_key)
    else:
        status = store.source_status(row.key)

    config: dict[str, Any] | None = row.config
    if KINDS[row.kind].sensitive:
        config = store.decode_config(row) if include_config else None

    return ComponentResponse(
        id=row.id,
        org_id=row.org_id,
        kind=row.kind,
        key=row.key,
        name=row.name,
        status=status,
        config=config,
        state=row.state,
        encrypted=row.encrypted,
        parent_id=row.parent_id,
        relations=_relations_of(row),
        children=[
            _to_response(child, store, include_config=include_config, parent_key=row.key, with_children=False)
            for child in row.children
        ]
        if with_children
        else [],
        created_at=str(row.created_at) if row.created_at else None,
        updated_at=str(row.updated_at) if row.updated_at else None,
    )


def _bindings(relations: dict[str, list[RelationEntry]] | None) -> dict[str, list[tuple[UUID, str]]] | None:
    if relations is None:
        return None
    return {type_: [(entry.dst_id, entry.slot) for entry in entries] for type_, entries in relations.items()}


# -- Endpoints ----------------------------------------------------------------


@router.get("/")
def list_components(
    kind: Annotated[list[str] | None, Query()] = None,
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[ComponentResponse]:
    """List the organisation's components, optionally filtered by kind(s)."""
    rows = store.list_components(org_id, kinds=kind)
    return [_to_response(row, store, include_config=False) for row in rows]


@router.get("/relations")
def list_relations(
    type: str | None = None,
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[RelationResponse]:
    """List the organisation's component relations, optionally by type."""
    return [
        RelationResponse(src_id=rel.src_id, dst_id=rel.dst_id, type=rel.type, slot=rel.slot, dst_kind=rel.dst_kind)
        for rel in store.list_relations(org_id, type=type)
    ]


@router.post("/", status_code=201)
def create_component(
    body: ComponentCreateRequest,
    user: Profile = Depends(require_editor),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> ComponentResponse:
    """Create a component of any kind."""
    try:
        row = store.create_component(
            org_id,
            kind=body.kind,
            key=body.key,
            name=body.name,
            config=body.config,
            encrypted=body.encrypted,
            children=body.children,
            relations=_bindings(body.relations),
        )
    except (ConfigError, CatalogKeyError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return _to_response(row, store, include_config=True)


@router.get("/{component_id}")
def get_component(
    component_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> ComponentResponse:
    """Get a single component by ID, including its decoded config payload."""
    row = load_authorized(store.get_component, component_id, user, store, label="Component")
    return _to_response(row, store, include_config=True)


@router.put("/{component_id}")
def update_component(
    component_id: UUID,
    body: ComponentUpdateRequest,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> ComponentResponse:
    """Update a component's spec. Omitted facets are untouched."""
    load_authorized(store.get_component, component_id, user, store, label="Component", minimum="editor")
    try:
        row = store.update_component(
            component_id,
            name=body.name,
            config=body.config,
            encrypted=body.encrypted,
            children=body.children,
            relations=_bindings(body.relations),
        )
    except (ConfigError, CatalogKeyError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return _to_response(row, store, include_config=True)


@router.delete("/{component_id}")
def delete_component(
    component_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Delete a component. Refused (409) while other components are bound to it."""
    load_authorized(store.get_component, component_id, user, store, label="Component", minimum="editor")
    try:
        store.delete_component(component_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except InUseError as e:
        raise HTTPException(status_code=409, detail={"message": str(e), "used_by": e.referrers})
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"status": "deleted"}


# -- Relation endpoints ---------------------------------------------------------


@router.post("/{component_id}/relations", status_code=201)
def add_relation(
    component_id: UUID,
    body: RelationCreateRequest,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> RelationResponse:
    """Add one relation from a component (e.g. a dependency edge)."""
    src = load_authorized(store.get_component, component_id, user, store, label="Component", minimum="editor")
    dst = load_authorized(store.get_component, body.dst_id, user, store, label="Component", minimum="editor")
    if dst.org_id != src.org_id:
        raise HTTPException(status_code=404, detail=f"Component {body.dst_id} not found")
    try:
        rel = store.add_relation(component_id, type=body.type, dst_id=body.dst_id, slot=body.slot)
    except ConfigError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return RelationResponse(src_id=rel.src_id, dst_id=rel.dst_id, type=rel.type, slot=rel.slot, dst_kind=rel.dst_kind)


@router.delete("/{component_id}/relations/{type}/{dst_id}", status_code=204)
def remove_relation(
    component_id: UUID,
    type: str,
    dst_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> None:
    """Remove a component's relations of one type toward one destination."""
    load_authorized(store.get_component, component_id, user, store, label="Component", minimum="editor")
    store.remove_relation(component_id, type=type, dst_id=dst_id)


# -- Partition endpoint ---------------------------------------------------------


@router.get("/{component_id}/partition-row-counts")
def get_partition_row_counts(
    component_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> PartitionRowCountsResponse:
    """Get row counts grouped by partition for an asset."""
    load_authorized(store.get_component, component_id, user, store, label="Component")
    try:
        il_asset = store.load(component_id)
    except (NotFoundError, ComponentDriftError) as e:
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return PartitionRowCountsResponse(
        asset_key=type(il_asset).key,
        partition_column=partitioning.column,
        counts=[PartitionRowCountItem(partition=str(k), row_count=v) for k, v in sorted(counts.items())],
    )


# -- Field resolution ----------------------------------------------------------


def handle_error(error: Exception, context: str) -> None:
    """Map external API errors to appropriate HTTP responses."""
    logger.error("Error %s: %s", context, error)

    if isinstance(error, httpx.HTTPStatusError):
        status = error.response.status_code
        if status in (401, 403):
            raise HTTPException(status_code=status, detail=f"Authorization failed while {context}.")
        if status == 404:
            raise HTTPException(status_code=404, detail=f"Resource not found while {context}.")

    if isinstance(error, HTTPException):
        raise error

    raise HTTPException(status_code=500, detail=f"Failed {context}.")


class ResolveRequest(BaseModel):
    """A request to resolve one provider-backed FetchField's options."""

    component_key: str
    field: str
    # Credentials per resource slot, e.g. {"connection": {"access_token": ...}}.
    deps: dict[str, dict[str, Any]] = {}


@router.post("/resolve")
async def resolve_fetch_field(
    body: ResolveRequest,
    catalog: Catalog = Depends(get_catalog),
    _user: Profile = Depends(require_viewer),
) -> list[dict[str, Any]]:
    """Resolve the options for a ``FetchField(provider=...)`` field.

    One endpoint resolves any field declared with
    ``FetchField(provider="<slot>.<method>")`` — there are no hand-written
    per-provider routes. The component definition comes from the catalog
    (authoritative — the provider reference comes from the server's schema,
    never the client), the resource in ``<slot>`` is instantiated from the
    credentials the form already holds, and the ``@fetch_field_provider``
    method ``<method>`` is called on it. That marker is the allowlist: only
    methods opted in that way may be invoked, so the browser cannot call
    arbitrary attributes.
    """
    defn = catalog.get(body.component_key)
    if defn is None:
        raise HTTPException(status_code=404, detail=f"Unknown component '{body.component_key}'")

    # Read the provider from the server's own schema, so the client cannot
    # redirect the call to an arbitrary method.
    prop = getattr(defn, "config_schema", {}).get("properties", {}).get(body.field, {})
    provider = prop.get("x-fetch", {}).get("provider")
    if not provider:
        raise HTTPException(
            status_code=400,
            detail=f"Field '{body.field}' on '{body.component_key}' is not a provider-backed FetchField",
        )
    slot, _, method = str(provider).partition(".")

    component_cls = import_from_path(defn.path)
    resource_cls = getattr(component_cls, "resource_types", {}).get(slot)
    if resource_cls is None:
        raise HTTPException(status_code=400, detail=f"Resource slot '{slot}' not found on '{body.component_key}'")

    # Only pass through fields the resource actually declares — the form may
    # carry extra markers (e.g. an internal id) that the model would reject.
    raw = body.deps.get(slot, {})
    creds = {k: v for k, v in raw.items() if k in resource_cls.model_fields}
    resource = resource_cls(**creds)

    fn = getattr(resource, method, None)
    if not is_fetch_field_provider(fn):
        # Should never happen — validated at catalog build — but guard anyway.
        raise HTTPException(status_code=403, detail=f"'{provider}' is not a fetch provider")
    assert fn is not None  # narrowed by the is_fetch_field_provider guard above

    try:
        result = await invoke(fn)
    except Exception as exc:
        handle_error(exc, f"resolving {body.component_key}.{body.field}")
        return []
    return list(result or [])


# -- Connection check ----------------------------------------------------------


#: Upper bound on a live check — the wizard must never hang on a dead host.
CHECK_TIMEOUT = 15.0


class FieldError(BaseModel):
    """One static-validation error, addressed to a config field."""

    field: str
    message: str


class CheckRequest(BaseModel):
    """A request to check one connection's candidate config."""

    component_key: str
    config: dict[str, Any] = {}


class CheckResponse(BaseModel):
    """The outcome of a connection check.

    ``live`` distinguishes a full check from a static-only one (the class
    implements no ``check()`` hook). ``category`` classifies failures so the
    UI can hint at a fix: bad ``config`` values, rejected ``auth``,
    unreachable ``network``, or an uncategorised ``error``.
    """

    ok: bool
    live: bool
    message: str | None = None
    category: Literal["config", "auth", "network", "error"] | None = None
    errors: list[FieldError] = Field(default_factory=list)


def _check_failure(exc: Exception, key: str) -> CheckResponse:
    """Map a live-check exception to its response.

    Full details are logged server-side only — provider errors may carry
    URLs with tokens.

    Returns:
        The categorised failure response.
    """
    logger.error("Connection check failed for '%s': %s", key, exc)

    if isinstance(exc, ConnectionCheckError):
        return CheckResponse(ok=False, live=True, category="error", message=str(exc))
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status in (401, 403):
            return CheckResponse(ok=False, live=True, category="auth", message="The provider rejected the credentials.")
        return CheckResponse(
            ok=False, live=True, category="error", message=f"The provider responded with HTTP {status}."
        )
    if isinstance(exc, (TimeoutError, httpx.TimeoutException)):
        return CheckResponse(ok=False, live=True, category="network", message="The provider did not respond in time.")
    if isinstance(exc, httpx.TransportError):
        return CheckResponse(ok=False, live=True, category="network", message="The provider could not be reached.")
    return CheckResponse(ok=False, live=True, category="error", message="The connection check failed unexpectedly.")


@router.post("/check")
async def check_connection(
    body: CheckRequest,
    catalog: Catalog = Depends(get_catalog),
    _user: Profile = Depends(require_viewer),
) -> CheckResponse:
    """Check a connection's candidate config, statically and (when supported) live.

    The static tier instantiates the connection class from the config the
    form holds — pydantic validation surfaces per-field errors. The live
    tier calls the class's ``check()`` hook (when implemented), a
    lightweight authenticated call against the provider. A failed check is
    this endpoint's *expected* output, so failures are reported as
    ``ok: false`` in a 200 response, never as HTTP errors; only an unknown
    component key is a 404.
    """
    defn = catalog.get(body.component_key)
    if defn is None or defn.kind != "connection":
        raise HTTPException(status_code=404, detail=f"Unknown connection '{body.component_key}'")

    connection_cls = import_from_path(defn.path)
    assert issubclass(connection_cls, Connection)  # guaranteed by the kind check above

    # Only pass through fields the connection actually declares — the form may
    # carry extra markers (e.g. an internal id) that the model would reject.
    config = {k: v for k, v in body.config.items() if k in connection_cls.model_fields}
    try:
        conn = connection_cls(**config)
    except ValidationError as exc:
        errors = [FieldError(field=".".join(str(loc) for loc in e["loc"]), message=e["msg"]) for e in exc.errors()]
        return CheckResponse(
            ok=False, live=False, category="config", message="The configuration is invalid.", errors=errors
        )

    if not connection_cls.checkable():
        return CheckResponse(ok=True, live=False)

    try:
        ok = bool(await asyncio.wait_for(invoke(conn.check), timeout=CHECK_TIMEOUT))
    except Exception as exc:
        return _check_failure(exc, body.component_key)
    if not ok:
        return CheckResponse(ok=False, live=True, category="error", message="The connection check failed.")
    return CheckResponse(ok=True, live=True)
