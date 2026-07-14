"""Source tools over the org's collection — the source instances actually set up."""

from __future__ import annotations

from typing import Any

from google.adk.tools.tool_context import ToolContext

from interloper_agent.context import get_catalog, get_org_id, get_store, serialize


def list_sources(tool_context: ToolContext) -> dict[str, Any]:
    """List the sources in the organisation's collection.

    This answers "what sources do we/I have?" — it returns only the source
    instances persisted for the org, each with its key, instance name, asset
    count, and creation date, enriched with catalog metadata (display name,
    icon). For the catalog of source definitions that *could* be added, use
    ``list_catalog_sources`` instead.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        catalog = get_catalog()

        sources = store.list_components(org_id, kinds=["source"])
        results = []
        for s in sources:
            entry = serialize(s)
            entry["asset_count"] = len(s.children)
            defn = catalog.get(s.key)
            if defn is not None:
                entry["catalog"] = {
                    "name": defn.get("name", s.key),
                    "description": defn.get("description"),
                    "icon": defn.get("icon"),
                    "tags": defn.get("tags", []),
                }
            results.append(entry)

        return {"status": "success", "count": len(results), "sources": results}
    except Exception as e:
        return {"status": "error", "error": str(e)}
