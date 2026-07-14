"""Collection tools — the component instances persisted for the organisation.

Generic over component kinds, mirroring the framework's component
architecture: one lister for any kind (sensitive kinds project
identity-only, driven by ``KINDS``), plus the connection operations that
are irreducibly kind-specific. Credentials never transit the model:
``request_connection_setup`` only signals the app to present a secure
setup form, and the browser submits credentials to the API directly.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from uuid import UUID

import httpx
from google.adk.tools.tool_context import ToolContext
from interloper.component import KINDS
from interloper.connection.base import Connection
from interloper.errors import ComponentDriftError, ConnectionCheckError, HydrationError
from interloper.oauth import is_provider_configured
from interloper.utils.concurrency import invoke
from pydantic import ValidationError

from interloper_agent.context import get_catalog, get_org_id, get_store, serialize

logger = logging.getLogger(__name__)

#: Upper bound on a live connection check — the agent must never hang on a dead host.
_CHECK_TIMEOUT = 15.0


def list_components(kind: str | None = None, tool_context: ToolContext | None = None) -> dict[str, Any]:
    """List the components in the organisation's collection.

    This answers "what do we have?" — what *could* be added (the catalog of
    definitions) is the Catalog specialist's domain. Sensitive kinds
    (connections, configs, resources) always return identity and metadata
    only — never credential or config values.

    Args:
        kind: Component kind to list — e.g. 'source', 'connection',
            'destination'. Omit for per-kind counts only; call again with a
            kind for the entries.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        catalog = get_catalog()

        if kind is None:
            counts: dict[str, int] = {}
            for c in store.list_components(org_id):
                counts[c.kind] = counts.get(c.kind, 0) + 1
            return {
                "status": "success",
                "component_counts": counts,
                "message": "Call again with a kind for the entries.",
            }
        if kind not in KINDS:
            return {"status": "error", "error": f"Unknown kind '{kind}'", "valid_kinds": sorted(KINDS.keys())}

        results = []
        for c in store.list_components(org_id, kinds=[kind]):
            entry: dict[str, Any] = {
                "id": serialize(c.id),
                "key": c.key,
                "name": c.name,
                "type_name": (catalog.get(c.key) or {}).get("name", c.key),
                "created_at": serialize(c.created_at),
            }
            # Fail closed: a sensitive kind's config is (or wraps) credentials.
            if not KINDS[kind].sensitive:
                entry["config"] = serialize(c.config)
            if kind == "source":
                entry["asset_count"] = len(c.children)
            results.append(entry)

        return {"status": "success", "kind": kind, "count": len(results), "components": results}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# -- Connection operations (kind-specific by nature) ------------------------------


def request_connection_setup(
    connection_key: str,
    name: str | None = None,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """Present a secure connection setup form to the user in the app.

    Call this to let the user create a connection: the app renders the form
    for the given definition (OAuth sign-in when available, manual
    credential entry otherwise) and the credentials go directly to the API.
    Never ask the user to share credentials in the chat instead.

    The response notes whether the user can sign in with the provider
    (``oauth_available``) or must enter credentials manually; an unknown key
    fails with the list of valid connection keys.

    Args:
        connection_key: Catalog key of the connection definition — usually
            ``<source_key>_connection`` (e.g. 'facebook_ads_connection').
        name: Optional display name to prefill in the form.
    """
    try:
        catalog = get_catalog()
        defn = catalog.get(connection_key)
        if defn is None or defn.get("kind") != "connection":
            valid = sorted(k for k, d in catalog.items() if d.get("kind") == "connection")
            return {
                "status": "error",
                "error": f"Connection definition '{connection_key}' not found in catalog",
                "valid_keys": valid,
            }
        oauth = (defn.get("config_schema") or {}).get("x-oauth")
        return {
            "status": "success",
            "message": (
                "Setup form presented to the user. Ask them to complete it "
                "(and to say so when done), then verify with list_components."
            ),
            "connection_key": connection_key,
            "name": name,
            "oauth": oauth is not None,
            "oauth_available": is_provider_configured(oauth["provider"]) if oauth else False,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def _categorise(exc: Exception) -> tuple[str, str]:
    """Map a ``check()`` failure to an LLM-safe ``(category, message)`` pair.

    Written against the categorisation contract documented on
    ``Connection.check()``. Raw provider errors may carry URLs with tokens,
    and this tool's output enters the model context — only curated messages
    leave here; details are logged server-side.

    Returns:
        The ``(category, message)`` pair.
    """
    if isinstance(exc, ConnectionCheckError):
        return "error", str(exc)
    if isinstance(exc, httpx.HTTPStatusError):
        if exc.response.status_code in (401, 403):
            return "auth", "The provider rejected the credentials."
        return "error", f"The provider responded with HTTP {exc.response.status_code}."
    if isinstance(exc, (TimeoutError, httpx.TimeoutException)):
        return "network", "The provider did not respond in time."
    if isinstance(exc, httpx.TransportError):
        return "network", "The provider could not be reached."
    return "error", "The connection check failed unexpectedly."


async def check_connection(connection_id: str, tool_context: ToolContext) -> dict[str, Any]:
    """Run a health check on an existing connection.

    Hydrates the stored connection (which validates its config against the
    current catalog and environment) and, when the type supports it, makes a
    lightweight authenticated call to the provider to prove the credentials
    work. Use this to verify a connection after the user sets it up, or when
    data collection fails with authentication-looking errors.

    Args:
        connection_id: UUID of the connection, from list_components.

    Returns ``ok`` plus, on failure, a ``category`` ('config', 'auth',
    'network', 'error') and message. ``live`` is false when the type
    implements no check and only hydration was verified.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()

        component = store.get_component(UUID(connection_id), kind="connection")
        if component.org_id != org_id:
            return {"status": "error", "error": f"Connection '{connection_id}' not found"}
        info = {"id": connection_id, "name": component.name, "key": component.key}

        try:
            conn = store.load(component.id)
        except ComponentDriftError as e:
            return {"status": "success", "connection": info, "ok": False, "live": False,
                    "category": "config", "message": str(e)}
        except HydrationError as e:
            logger.error("Connection '%s' (%s) failed to hydrate: %s", component.name, component.key, e)
            # Never forward the wrapped message: pydantic errors embed input
            # values, which for connections may be secrets — name fields only.
            if isinstance(e.__cause__, ValidationError):
                fields = ", ".join(
                    ".".join(str(loc) for loc in err["loc"]) or "(root)" for err in e.__cause__.errors()
                )
                message = f"The stored config is no longer valid for this connection type (invalid fields: {fields})."
            else:
                message = "The stored connection could not be reconstructed."
            return {"status": "success", "connection": info, "ok": False, "live": False,
                    "category": "config", "message": message}

        if not isinstance(conn, Connection) or not conn.checkable():
            return {"status": "success", "connection": info, "ok": True, "live": False,
                    "message": "This connection type implements no live check; the stored config hydrates."}

        try:
            ok = bool(await asyncio.wait_for(invoke(conn.check), timeout=_CHECK_TIMEOUT))
        except Exception as e:  # noqa: BLE001 — any hook failure is a categorised result, never a raise
            logger.error("Connection check failed for '%s' (%s): %s", component.name, component.key, e)
            category, message = _categorise(e)
            return {"status": "success", "connection": info, "ok": False, "live": True,
                    "category": category, "message": message}
        if not ok:
            return {"status": "success", "connection": info, "ok": False, "live": True,
                    "category": "error", "message": "The connection check failed."}
        return {"status": "success", "connection": info, "ok": True, "live": True}
    except Exception as e:
        return {"status": "error", "error": str(e)}
