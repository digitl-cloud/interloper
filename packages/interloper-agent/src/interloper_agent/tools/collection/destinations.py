"""Destination tools — the org's collection of destinations."""

from __future__ import annotations

from typing import Any

from google.adk.tools.tool_context import ToolContext

from interloper_agent.context import get_org_id, get_store, serialize


def list_destinations(tool_context: ToolContext) -> dict[str, Any]:
    """List the destinations in the organisation's collection.

    Returns each destination with its key, name, config, and resource bindings.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        destinations = store.list_components(org_id, kinds=["destination"])
        return {"status": "success", "destinations": [serialize(d) for d in destinations]}
    except Exception as e:
        return {"status": "error", "error": str(e)}
