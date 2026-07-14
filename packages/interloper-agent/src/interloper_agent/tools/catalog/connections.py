"""Connection tools over the catalog — the connection definitions the platform ships."""

from __future__ import annotations

from typing import Any

from google.adk.tools.tool_context import ToolContext
from interloper.oauth import is_provider_configured

from interloper_agent.context import get_catalog


def list_connections(tool_context: ToolContext) -> dict[str, Any]:
    """List the connection definitions available in the catalog.

    Each entry notes its required config fields and whether OAuth sign-in is
    supported by the definition (``oauth``) and usable in this deployment
    (``oauth_available``) — when OAuth is not available, credentials must be
    entered manually in the setup form. Setting connections up (and the
    connections the organisation actually has) is the Collection
    specialist's domain.
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
        return {"status": "success", "count": len(results), "connections": results}
    except Exception as e:
        return {"status": "error", "error": str(e)}
