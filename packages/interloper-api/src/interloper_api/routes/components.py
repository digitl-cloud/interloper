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

router = APIRouter(prefix="/components", tags=["components"])


# -- Request/Response models ---------------------------------------------------


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
    """Request body for creating a component of any kind.

    ``encrypted`` applies to secret kinds only: None encrypts whenever an
    encryption key is configured, an explicit bool forces it on or off.
    ``children`` applies to source kinds only and names the child asset keys
    to enable (None enables all of them). Every relation type listed in
    ``relations`` is replaced wholesale, so an empty list clears that type.
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

    ``auto_renew`` surfaces a connection's renewal toggle even in list
    responses, where a secret kind's ``config`` stays undisclosed — the
    toggle is operational metadata, not a credential.
    """

    id: UUID
    org_id: UUID
    kind: str
    key: str
    name: str | None = None
    status: ComponentStatus
    config: dict[str, Any] | None = None
    state: dict[str, Any] | None = None
    encrypted: bool = False
    auto_renew: bool | None = None
    parent_id: UUID | None = None
    relations: dict[str, list[RelationRef]] = {}
    children: list[ComponentResponse] = []
    created_at: str | None = None
    updated_at: str | None = None

    @classmethod
    def from_row(cls, 
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

        Args:
            row: The component row to convert.
            store: The Store instance.
            include_config: Whether a secret kind's decoded config is exposed.
            parent_key: The parent source's key when the caller already knows it,
                sparing a lazy load of ``row.parent`` for asset rows.
            with_children: Whether the row's children are nested in the response.

        Returns:
            The response model.
        """
        status = store.components.status(row, parent_key=parent_key)

        config: dict[str, Any] | None = row.config
        if KINDS[row.kind].sensitive:
            config = store.components.decode_config(row) if include_config else None

        auto_renew: bool | None = None
        if row.kind == "connection":
            auto_renew = bool((config or store.components.decode_config(row)).get("auto_renew", True))

        return cls(
            id=row.id,
            org_id=row.org_id,
            kind=row.kind,
            key=row.key,
            name=row.name,
            status=status,
            config=config,
            state=row.state,
            encrypted=row.encrypted,
            auto_renew=auto_renew,
            parent_id=row.parent_id,
            relations=_relations_of(row),
            children=[
                ComponentResponse.from_row(
                    child, store, include_config=include_config, parent_key=row.key, with_children=False
                )
                for child in row.children
            ]
            if with_children
            else [],
            created_at=str(row.created_at) if row.created_at else None,
            updated_at=str(row.updated_at) if row.updated_at else None,
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
    """Group a component's outgoing relations by relation type.

    Args:
        row: The component row, with its ``out_relations`` eager-loaded.

    Returns:
        A ``{type: [bindings]}`` map of the row's outgoing relations.
    """
    grouped: dict[str, list[RelationRef]] = {}
    for relation in row.out_relations:
        grouped.setdefault(relation.type, []).append(
            RelationRef(dst_id=relation.dst_id, slot=relation.slot, dst_kind=relation.dst_kind)
        )
    return grouped


def _bindings(relations: dict[str, list[RelationEntry]] | None) -> dict[str, list[tuple[UUID, str]]] | None:
    """Flatten a request's relation entries into the tuples the store takes.

    Args:
        relations: The request's ``{type: [entries]}`` map, or None to leave
            every relation type untouched.

    Returns:
        A ``{type: [(dst_id, slot)]}`` map, or None when *relations* is None.
    """
    if relations is None:
        return None
    return {type_: [(entry.dst_id, entry.slot) for entry in entries] for type_, entries in relations.items()}


# -- Component endpoints -------------------------------------------------------


@router.get("/")
def list_components(
    kind: Annotated[list[str] | None, Query()] = None,
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[ComponentResponse]:
    """List the organisation's components, optionally filtered by kind(s).

    Args:
        kind: The component kinds to keep; None lists every kind.
        user: The authenticated user.
        org_id: The active organisation UUID.
        store: The Store instance.

    Returns:
        The organisation's components, secret configs withheld.
    """
    rows = store.components.list_all(org_id, kinds=kind)
    return [ComponentResponse.from_row(row, store, include_config=False) for row in rows]


@router.get("/relations")
def list_relations(
    type: str | None = None,
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[RelationResponse]:
    """List the organisation's component relations, optionally by type.

    Args:
        type: The relation type to keep; None lists every type.
        user: The authenticated user.
        org_id: The active organisation UUID.
        store: The Store instance.

    Returns:
        The organisation's relation rows.
    """
    return [
        RelationResponse(
            src_id=relation.src_id,
            dst_id=relation.dst_id,
            type=relation.type,
            slot=relation.slot,
            dst_kind=relation.dst_kind,
        )
        for relation in store.relations.list_all(org_id, type=type)
    ]


@router.post("/", status_code=201)
def create_component(
    body: ComponentCreateRequest,
    user: Profile = Depends(require_editor),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
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

    Raises:
        HTTPException: 400 for an invalid config or an unknown catalog key,
            404 when a relation points at a component that does not exist.
    """
    try:
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
    except (ConfigError, CatalogKeyError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return ComponentResponse.from_row(row, store, include_config=True)


@router.get("/{component_id}")
def get_component(
    component_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
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
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> ComponentResponse:
    """Update a component's spec. Omitted facets are untouched.

    Args:
        component_id: The component UUID.
        body: The facets to update; the omitted ones are left as they are.
        user: The authenticated user.
        store: The Store instance.

    Returns:
        The updated component, with its config decoded.

    Raises:
        HTTPException: 400 for an invalid config or an unknown catalog key,
            404 when a relation points at a component that does not exist,
            409 when the update would break a binding another component
            depends on.
    """
    load_authorized(store.components.get, component_id, user, store, label="Component", minimum="editor")
    try:
        row = store.components.update(
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
    except InUseError as e:
        raise HTTPException(status_code=409, detail={"message": str(e), "used_by": e.referrers})
    return ComponentResponse.from_row(row, store, include_config=True)


@router.delete("/{component_id}")
def delete_component(
    component_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Delete a component. Refused (409) while other components are bound to it.

    Args:
        component_id: The component UUID.
        user: The authenticated user.
        store: The Store instance.

    Returns:
        A ``{"status": "deleted"}`` acknowledgement.

    Raises:
        HTTPException: 404 if the component is already gone, 409 while other
            components are bound to it, 400 if the store refuses the delete.
    """
    load_authorized(store.components.get, component_id, user, store, label="Component", minimum="editor")
    try:
        store.components.delete(component_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except InUseError as e:
        raise HTTPException(status_code=409, detail={"message": str(e), "used_by": e.referrers})
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"status": "deleted"}


# -- Relation endpoints --------------------------------------------------------


@router.post("/{component_id}/relations", status_code=201)
def add_relation(
    component_id: UUID,
    body: RelationCreateRequest,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> RelationResponse:
    """Add one relation from a component (e.g. a dependency edge).

    Args:
        component_id: The source component's UUID.
        body: The relation to add: its type, target ``dst_id`` and slot.
        user: The authenticated user.
        store: The Store instance.

    Returns:
        The created relation row.

    Raises:
        HTTPException: 404 when the target is missing or belongs to another
            organisation, 400 when the relation is not allowed on this kind.
    """
    source = load_authorized(store.components.get, component_id, user, store, label="Component", minimum="editor")
    destination_row = load_authorized(
        store.components.get, body.dst_id, user, store, label="Component", minimum="editor"
    )
    if destination_row.org_id != source.org_id:
        raise HTTPException(status_code=404, detail=f"Component {body.dst_id} not found")
    try:
        relation = store.relations.add(component_id, type=body.type, dst_id=body.dst_id, slot=body.slot)
    except ConfigError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return RelationResponse(
        src_id=relation.src_id,
        dst_id=relation.dst_id,
        type=relation.type,
        slot=relation.slot,
        dst_kind=relation.dst_kind,
    )


@router.delete("/{component_id}/relations/{type}/{dst_id}", status_code=204)
def remove_relation(
    component_id: UUID,
    type: str,
    dst_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> None:
    """Remove a component's relations of one type toward one destination.

    Refused (400) for required dependency slots — repoint them instead.

    Args:
        component_id: The source component's UUID.
        type: The relation type to remove.
        dst_id: The target component's UUID.
        user: The authenticated user.
        store: The Store instance.

    Raises:
        HTTPException: 400 for a required dependency slot.
    """
    load_authorized(store.components.get, component_id, user, store, label="Component", minimum="editor")
    try:
        store.relations.remove(component_id, type=type, dst_id=dst_id)
    except ConfigError as e:
        raise HTTPException(status_code=400, detail=str(e))


# -- Partition endpoint --------------------------------------------------------


@router.get("/{component_id}/partition-row-counts")
def get_partition_row_counts(
    component_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
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
    except Exception as e:  # noqa: BLE001 — any destination failure is a 500, never a traceback
        raise HTTPException(status_code=500, detail=str(e))

    return PartitionRowCountsResponse(
        asset_key=type(il_asset).key,
        partition_column=partitioning.column,
        counts=[PartitionRowCountItem(partition=str(k), row_count=v) for k, v in sorted(counts.items())],
    )


# -- Field resolution ----------------------------------------------------------


def handle_error(error: Exception, context: str) -> None:
    """Map external API errors to appropriate HTTP responses.

    Args:
        error: The exception raised while calling the external API.
        context: What was being attempted, phrased as a gerund clause for the
            detail message (e.g. ``"resolving facebook.ads_stats"``).

    Raises:
        HTTPException: Always — *error* re-raised as-is when it already is
            one, otherwise the status mapped from it, or 500 by default.
    """
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
    """A request to resolve one provider-backed FetchField's options.

    ``deps`` carries the credentials the form already holds, keyed by resource
    slot (e.g. ``{"connection": {"access_token": ...}}``).
    """

    component_key: str
    field: str
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

    Args:
        body: The component key, the field name, and the per-slot credentials
            the form currently holds.
        catalog: The Catalog instance.
        _user: The authenticated user (viewer gate).

    Returns:
        The field's options, as the provider returned them.

    Raises:
        HTTPException: 404 for an unknown component key, 400 when the field is
            not a provider-backed FetchField or names an unknown resource
            slot, 403 when the target method is not a fetch provider.
    """
    defn = catalog.get(body.component_key)
    if defn is None:
        raise HTTPException(status_code=404, detail=f"Unknown component '{body.component_key}'")

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
    except Exception as exception:  # noqa: BLE001 — every provider failure is mapped to a response
        handle_error(exception, f"resolving {body.component_key}.{body.field}")
        return []
    return list(result or [])


# -- Connection check ----------------------------------------------------------


# Upper bound on a live check — the wizard must never hang on a dead host.
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

    @classmethod
    def from_failure(cls, exception: Exception, key: str) -> CheckResponse:
        """Map a live-check exception to its response.

        Full details are logged server-side only — provider errors may carry
        URLs with tokens.

        Args:
            exception: The exception the live check raised.
            key: The connection's catalog key, for the log line.

        Returns:
            The categorised failure response.
        """
        logger.error("Connection check failed for '%s': %s", key, exception)

        if isinstance(exception, ConnectionCheckError):
            return cls(ok=False, live=True, category="error", message=str(exception))
        if isinstance(exception, httpx.HTTPStatusError):
            status = exception.response.status_code
            if status in (401, 403):
                return cls(ok=False, live=True, category="auth", message="The provider rejected the credentials.")
            return cls(
                ok=False, live=True, category="error", message=f"The provider responded with HTTP {status}."
            )
        if isinstance(exception, (TimeoutError, httpx.TimeoutException)):
            return cls(ok=False, live=True, category="network", message="The provider did not respond in time.")
        if isinstance(exception, httpx.TransportError):
            return cls(ok=False, live=True, category="network", message="The provider could not be reached.")
        return cls(ok=False, live=True, category="error", message="The connection check failed unexpectedly.")


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

    Args:
        body: The connection key and the candidate config to check.
        catalog: The Catalog instance.
        _user: The authenticated user (viewer gate).

    Returns:
        The check outcome; a failed check is still a 200 with ``ok: false``.

    Raises:
        HTTPException: 404 for an unknown connection key.
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
        connection = connection_cls(**config)
    except ValidationError as exception:
        errors = [
            FieldError(field=".".join(str(loc) for loc in e["loc"]), message=e["msg"])
            for e in exception.errors()
        ]
        return CheckResponse(
            ok=False, live=False, category="config", message="The configuration is invalid.", errors=errors
        )

    if not connection_cls.checkable():
        return CheckResponse(ok=True, live=False)

    try:
        ok = bool(await asyncio.wait_for(invoke(connection.check), timeout=CHECK_TIMEOUT))
    except Exception as exception:  # noqa: BLE001 — a failed check is a result, never a raise
        return CheckResponse.from_failure(exception, body.component_key)
    if not ok:
        return CheckResponse(ok=False, live=True, category="error", message="The connection check failed.")
    return CheckResponse(ok=True, live=True)
