"""Source tools over the catalog — the source definitions the platform ships."""

from __future__ import annotations

from typing import Any

from google.adk.tools.tool_context import ToolContext

from interloper_agent.context import get_catalog, get_org_id, get_store


def list_sources(tool_context: ToolContext) -> dict[str, Any]:
    """List the source definitions available in the catalog.

    This answers "what sources does Interloper support / could we add?" — it
    returns every source definition in the catalog, whether or not it is in
    the org's collection, with how many instances the collection holds. The
    collection itself (the sources the organisation actually has) is the
    Collection specialist's domain.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        catalog = get_catalog()

        collection_counts: dict[str, int] = {}
        for s in store.list_components(org_id, kinds=["source"]):
            collection_counts[s.key] = collection_counts.get(s.key, 0) + 1

        results = []
        for key, defn in catalog.items():
            if defn.get("kind") != "source":
                continue
            count = collection_counts.get(key, 0)
            results.append({
                "key": key,
                "name": defn.get("name", key),
                "description": defn.get("description"),
                "icon": defn.get("icon"),
                "asset_count": len(defn.get("assets", [])),
                "tags": defn.get("tags", []),
                "in_collection": count > 0,
                "collection_count": count,
            })

        return {"status": "success", "count": len(results), "sources": results}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def get_source_detail(source_key: str, tool_context: ToolContext) -> dict[str, Any]:
    """Get full catalog detail for a source definition.

    This is the catalog definition (the source *type*), not an instance from
    the org's collection.

    Args:
        source_key: The source key (e.g. 'facebook_ads').

    Returns the source definition including config schema, resource types,
    destination types, and a list of all its assets with their schemas.
    """
    try:
        catalog = get_catalog()
        defn = catalog.get(source_key)
        if defn is None or defn.get("kind") != "source":
            return {"status": "error", "error": f"Source '{source_key}' not found in catalog"}
        return {"status": "success", "source": defn}
    except Exception as e:
        return {"status": "error", "error": str(e)}
