"""Connection tools — discovery and setup orchestration.

Credentials never transit the model: ``request_connection_setup`` only
signals the app to present a secure setup form (OAuth sign-in or manual
entry), and the browser submits credentials to the API directly. Listing
tools return identity and metadata only, never credential values.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from google.adk.tools.tool_context import ToolContext
from interloper.connection.base import Connection
from interloper.connection.check import check_connection_config
from interloper.oauth import is_provider_configured
from interloper.utils.imports import import_from_path

from interloper_agent.context import get_catalog, get_org_id, get_store, serialize


def list_connection_types(tool_context: ToolContext) -> dict[str, Any]:
    """List the connection types available in the catalog.

    Use this to pick the right type before requesting setup. Each entry
    notes its required config fields and whether OAuth sign-in is supported
    by the type (``oauth``) and usable in this deployment
    (``oauth_available``) — when OAuth is not available, the user will have
    to enter credentials manually in the setup form.
    """
    try:
        catalog = get_catalog()
        results = []
        for key, defn in catalog.items():
            if defn.get("kind") != "connection":
                continue
            schema = defn.get("config_schema") or {}
            oauth = schema.get("x-oauth")
            results.append({
                "key": key,
                "name": defn.get("name", key),
                "description": defn.get("description"),
                "provider": defn.get("provider"),
                "required_fields": schema.get("required", []),
                "oauth": oauth is not None,
                "oauth_available": is_provider_configured(oauth["provider"]) if oauth else False,
            })
        return {"status": "success", "count": len(results), "connection_types": results}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def list_connections(tool_context: ToolContext) -> dict[str, Any]:
    """List the connections configured in the organisation.

    Returns identity and metadata only — never credential values.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        catalog = get_catalog()
        connections = [
            {
                "id": serialize(c.id),
                "key": c.key,
                "name": c.name,
                "type_name": (catalog.get(c.key) or {}).get("name", c.key),
                "created_at": serialize(c.created_at),
            }
            for c in store.list_components(org_id, kinds=["connection"])
        ]
        return {"status": "success", "count": len(connections), "connections": connections}
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def check_connection(connection_id: str, tool_context: ToolContext) -> dict[str, Any]:
    """Run a health check on an existing connection.

    Validates the stored config and, when the connection type supports it,
    makes a lightweight authenticated call to the provider to prove the
    credentials work. Use this to verify a connection after the user sets it
    up, or when data collection fails with authentication-looking errors.

    Args:
        connection_id: UUID of the connection, from list_connections.

    Returns ``ok`` plus, on failure, a ``category`` ('config', 'auth',
    'network', 'error') and message. ``live`` is false when the type
    implements no check and only static validation ran.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        catalog = get_catalog()

        component = store.get_component(UUID(connection_id), kind="connection")
        if component.org_id != org_id:
            return {"status": "error", "error": f"Connection '{connection_id}' not found"}
        defn = catalog.get(component.key)
        if defn is None:
            return {"status": "error", "error": f"Connection type '{component.key}' is not in the catalog"}

        connection_cls = import_from_path(defn["path"])
        if not issubclass(connection_cls, Connection):
            return {"status": "error", "error": f"'{component.key}' is not a connection type"}

        result = await check_connection_config(connection_cls, store.decode_config(component), key=component.key)
        return {
            "status": "success",
            "connection": {"id": connection_id, "name": component.name, "key": component.key},
            "ok": result.ok,
            "live": result.live,
            "category": result.category,
            "message": result.message,
            "field_errors": [{"field": e.field, "message": e.message} for e in result.errors],
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def request_connection_setup(
    connection_key: str,
    name: str | None = None,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """Present a secure connection setup form to the user in the app.

    Call this to let the user create a connection: the app renders the form
    for the given type (OAuth sign-in when available, manual credential
    entry otherwise) and the credentials go directly to the API. Never ask
    the user to share credentials in the chat instead.

    Args:
        connection_key: Catalog key of the connection type
            (e.g. 'facebook_ads_connection'), from list_connection_types.
        name: Optional display name to prefill in the form.
    """
    try:
        catalog = get_catalog()
        defn = catalog.get(connection_key)
        if defn is None or defn.get("kind") != "connection":
            return {"status": "error", "error": f"Connection type '{connection_key}' not found in catalog"}
        return {
            "status": "success",
            "message": (
                "Setup form presented to the user. Ask them to complete it "
                "(and to say so when done), then verify with list_connections."
            ),
            "connection_key": connection_key,
            "name": name,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
