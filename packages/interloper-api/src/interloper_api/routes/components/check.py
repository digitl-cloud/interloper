"""Generic connection checker.

One endpoint validates any connection's candidate config before it is saved:

1. Looking up the connection definition in the catalog and importing its class.
2. Instantiating it from the config the form holds — pydantic validation is
   the static tier, surfacing per-field errors.
3. Calling the class's ``check()`` hook (when implemented) — the live tier,
   a lightweight authenticated call against the provider.

A failed check is this endpoint's *expected* output, so failures are reported
as ``ok: false`` in a 200 response, never as HTTP errors; only an unknown
component key is a 404.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Literal

import httpx
from fastapi import APIRouter, Depends, HTTPException
from interloper.catalog.base import Catalog
from interloper.connection.base import Connection
from interloper.errors import ConnectionCheckError
from interloper.utils.concurrency import invoke
from interloper.utils.imports import import_from_path
from interloper_db import Profile
from pydantic import BaseModel, Field, ValidationError

from interloper_api.dependencies import get_catalog, require_viewer

logger = logging.getLogger(__name__)

sub_router = APIRouter()

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


def _failure(exc: Exception, key: str) -> CheckResponse:
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


@sub_router.post("/check")
async def check_connection(
    body: CheckRequest,
    catalog: Catalog = Depends(get_catalog),
    _user: Profile = Depends(require_viewer),
) -> CheckResponse:
    """Check a connection's candidate config, statically and (when supported) live."""
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
        return _failure(exc, body.component_key)
    if not ok:
        return CheckResponse(ok=False, live=True, category="error", message="The connection check failed.")
    return CheckResponse(ok=True, live=True)
