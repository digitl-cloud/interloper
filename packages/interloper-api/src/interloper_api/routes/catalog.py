"""Catalog API: the component definitions this deployment ships, and the operations on a type.

Besides serving the definitions, the routes here execute a component *class*
against a candidate, unsaved config: resolving a FetchField's options and
checking a connection. They act on a type, not on a stored component, so they
are addressed by catalog key.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Literal, NoReturn

import httpx2
import interloper as il
from fastapi import APIRouter, Depends, HTTPException
from interloper.connection.base import Connection
from interloper.errors import ConnectionCheckError, format_exception
from interloper.resource.fields import is_fetch_field_provider
from interloper.utils.concurrency import invoke
from interloper.utils.imports import import_from_path
from pydantic import BaseModel, Field, ValidationError

from interloper_api.dependencies import CatalogDep, EditorDep, require_viewer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/catalog", tags=["catalog"], dependencies=[Depends(require_viewer)])


@router.get("")
def list_catalog(catalog: CatalogDep) -> dict[str, Any]:
    """Return the full catalog.

    Args:
        catalog: Injected catalog.

    Returns:
        Every catalog entry, serialised.
    """
    return catalog.dump()


@router.get("/resource-kinds")
def list_resource_kinds(catalog: CatalogDep) -> list[str]:
    """Return distinct resource kinds from the catalog.

    A resource kind is any registered kind anchored under ``Resource``
    (currently ``connection`` and ``config``) - the kinds usable as
    relation bindings on other components.

    Args:
        catalog: Injected catalog.

    Returns:
        The resource kinds present in the catalog, sorted.
    """
    return sorted(
        {
            defn.kind
            for defn in catalog.components.values()
            if (anchor := il.KINDS.get(defn.kind)) is not None and issubclass(anchor, il.Resource)
        }
    )


# -- Field resolution ----------------------------------------------------------


def handle_error(error: Exception, context: str) -> NoReturn:
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

    if isinstance(error, httpx2.HTTPStatusError):
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

    ``deps`` carries the credentials the form already holds, keyed by relation
    name (e.g. ``{"connection": {"access_token": ...}}``).
    """

    field: str
    deps: dict[str, dict[str, Any]] = {}


@router.post("/{key}/resolve")
async def resolve_fetch_field(
    key: str,
    body: ResolveRequest,
    catalog: CatalogDep,
    _user: EditorDep,
) -> list[dict[str, Any]]:
    """Resolve the options for a ``FetchField(provider=...)`` field.

    One endpoint resolves any field declared with
    ``FetchField(provider="<name>.<method>")`` - there are no hand-written
    per-provider routes. The component definition comes from the catalog
    (authoritative - the provider reference comes from the server's schema,
    never the client), the resource named by the relation is instantiated
    from the credentials the form already holds, and the
    ``@fetch_field_provider`` method ``<method>`` is called on it. That
    marker is the allowlist: only methods opted in that way may be invoked,
    so the browser cannot call arbitrary attributes.

    Args:
        key: The catalog key of the component whose field is resolved.
        body: The field name and the per-relation credentials the form
            currently holds.
        catalog: The Catalog instance.
        _user: The authenticated user (editor gate).

    Returns:
        The field's options, as the provider returned them.

    Raises:
        HTTPException: 404 for an unknown component key, 400 when the field is
            not a provider-backed FetchField, names an unknown or undeclared
            relation, or the credentials the form holds cannot build that
            relation's resource, 403 when the target method is not a fetch
            provider.
    """
    defn = catalog.get(key)
    if defn is None:
        raise HTTPException(status_code=404, detail=f"Unknown component '{key}'")

    prop = getattr(defn, "config_schema", {}).get("properties", {}).get(body.field, {})
    provider = prop.get("x-fetch", {}).get("provider")
    if not provider:
        raise HTTPException(
            status_code=400,
            detail=f"Field '{body.field}' on '{key}' is not a provider-backed FetchField",
        )
    name, _, method = str(provider).partition(".")

    component_cls = import_from_path(defn.path)
    relation = component_cls.relations.get(name)
    resource_cls = relation.target if relation else None
    if resource_cls is None:
        raise HTTPException(
            status_code=400,
            detail=f"Relation '{name}' not found on '{key}' or not declared from a component class",
        )

    # Only pass through fields the resource actually declares - the form may
    # carry extra markers (e.g. an internal id) that the model would reject.
    raw = body.deps.get(name, {})
    creds = {k: v for k, v in raw.items() if k in resource_cls.model_fields}
    try:
        resource = resource_cls(**creds)
    except ValidationError as error:
        # format_exception, never str(error): pydantic echoes the input values, which are credentials.
        raise HTTPException(
            status_code=400,
            detail=f"Cannot resolve '{body.field}' from the '{name}' credentials given: {format_exception(error)}",
        )

    fn = getattr(resource, method, None)
    if not is_fetch_field_provider(fn):
        # Should never happen — validated at catalog build — but guard anyway.
        raise HTTPException(status_code=403, detail=f"'{provider}' is not a fetch provider")
    assert fn is not None  # narrowed by the is_fetch_field_provider guard above

    try:
        result = await invoke(fn)
    except Exception as exception:  # noqa: BLE001 — every provider failure is mapped to a response
        handle_error(exception, f"resolving {key}.{body.field}")
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
        if isinstance(exception, httpx2.HTTPStatusError):
            status = exception.response.status_code
            if status in (401, 403):
                return cls(ok=False, live=True, category="auth", message="The provider rejected the credentials.")
            return cls(ok=False, live=True, category="error", message=f"The provider responded with HTTP {status}.")
        if isinstance(exception, (TimeoutError, httpx2.TimeoutException)):
            return cls(ok=False, live=True, category="network", message="The provider did not respond in time.")
        if isinstance(exception, httpx2.TransportError):
            return cls(ok=False, live=True, category="network", message="The provider could not be reached.")
        return cls(ok=False, live=True, category="error", message="The connection check failed unexpectedly.")


@router.post("/{key}/check")
async def check_connection(
    key: str,
    body: CheckRequest,
    catalog: CatalogDep,
    _user: EditorDep,
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
        key: The catalog key of the connection to check.
        body: The candidate config to check.
        catalog: The Catalog instance.
        _user: The authenticated user (editor gate).

    Returns:
        The check outcome; a failed check is still a 200 with ``ok: false``.

    Raises:
        HTTPException: 404 for an unknown connection key.
    """
    defn = catalog.get(key)
    if defn is None or defn.kind != "connection":
        raise HTTPException(status_code=404, detail=f"Unknown connection '{key}'")

    connection_cls = import_from_path(defn.path)
    assert issubclass(connection_cls, Connection)  # guaranteed by the kind check above

    # Only pass through fields the connection actually declares — the form may
    # carry extra markers (e.g. an internal id) that the model would reject.
    config = {k: v for k, v in body.config.items() if k in connection_cls.model_fields}
    try:
        connection = connection_cls(**config)
    except ValidationError as exception:
        errors = [
            FieldError(field=".".join(str(loc) for loc in e["loc"]), message=e["msg"]) for e in exception.errors()
        ]
        return CheckResponse(
            ok=False, live=False, category="config", message="The configuration is invalid.", errors=errors
        )

    if not connection_cls.checkable():
        return CheckResponse(ok=True, live=False)

    try:
        ok = bool(await asyncio.wait_for(invoke(connection.check), timeout=CHECK_TIMEOUT))
    except Exception as exception:  # noqa: BLE001 — a failed check is a result, never a raise
        return CheckResponse.from_failure(exception, key)
    if not ok:
        return CheckResponse(ok=False, live=True, category="error", message="The connection check failed.")
    return CheckResponse(ok=True, live=True)
