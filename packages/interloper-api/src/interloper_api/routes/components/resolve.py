"""Generic FetchField resolver.

One endpoint resolves the options of any field declared with
``FetchField(provider="<slot>.<method>")`` — there are no hand-written
per-provider routes. It works by:

1. Looking up the component definition in the catalog (authoritative — the
   provider reference comes from the server's schema, never the client).
2. Importing the component class and reading the resource class in ``<slot>``.
3. Instantiating that resource from the credentials the form already holds.
4. Calling the ``@fetch_field_provider`` method ``<method>`` on it.

The ``@fetch_field_provider`` marker is the allowlist: only methods opted in that
way may be invoked, so the browser cannot call arbitrary attributes.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException
from interloper.catalog.base import Catalog
from interloper.resource.fields import is_fetch_field_provider
from interloper.utils.concurrency import invoke
from interloper.utils.imports import import_from_path
from interloper_db import Profile
from pydantic import BaseModel

from interloper_api.dependencies import get_catalog, require_viewer

logger = logging.getLogger(__name__)

sub_router = APIRouter()


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


@sub_router.post("/resolve")
async def resolve_fetch_field(
    body: ResolveRequest,
    catalog: Catalog = Depends(get_catalog),
    _user: Profile = Depends(require_viewer),
) -> list[dict[str, Any]]:
    """Resolve the options for a ``FetchField(provider=...)`` field."""
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
