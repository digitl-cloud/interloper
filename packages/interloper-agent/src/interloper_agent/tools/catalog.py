"""Catalog tools — the component definitions the platform ships.

Generic over component kinds, mirroring the framework's component
architecture: one lister and one getter for any kind, plus the
asset-schema operations that are irreducibly kind-specific.
"""

from __future__ import annotations

from typing import Any

from google.adk.tools.tool_context import ToolContext
from interloper.oauth import is_provider_configured

from interloper_agent.context import get_catalog, get_org_id, get_store


def list_definitions(kind: str | None = None, tool_context: ToolContext | None = None) -> dict[str, Any]:
    """List the component definitions available in the catalog.

    This answers "what does Interloper support / what could we add?" — the
    org's collection itself is the Collection specialist's domain.

    Source entries note how many instances the org's collection holds
    (``in_collection`` / ``collection_count``); connection entries note
    whether OAuth sign-in is supported (``oauth``) and usable in this
    deployment (``oauth_available``) vs manual credential entry.

    Args:
        kind: Component kind to list — e.g. 'source', 'connection',
            'destination'. Omit for per-kind counts only; call again with a
            kind for the entries.
    """
    try:
        catalog = get_catalog()
        counts: dict[str, int] = {}
        for defn in catalog.values():
            counts[defn.get("kind", "?")] = counts.get(defn.get("kind", "?"), 0) + 1

        if kind is None:
            return {
                "status": "success",
                "definition_counts": counts,
                "message": "Call again with a kind for the entries.",
            }
        if kind not in counts:
            return {
                "status": "error",
                "error": f"No '{kind}' definitions in the catalog",
                "valid_kinds": sorted(counts),
            }

        collection_counts: dict[str, int] = {}
        if kind == "source":
            org_id = get_org_id(tool_context)
            for s in get_store().list_components(org_id, kinds=["source"]):
                collection_counts[s.key] = collection_counts.get(s.key, 0) + 1

        results = []
        for key, defn in catalog.items():
            if defn.get("kind") != kind:
                continue
            entry: dict[str, Any] = {
                "key": key,
                "name": defn.get("name", key),
                "description": defn.get("description"),
                "icon": defn.get("icon"),
                "tags": defn.get("tags", []),
            }
            if kind == "source":
                count = collection_counts.get(key, 0)
                entry["asset_count"] = len(defn.get("assets", []))
                entry["in_collection"] = count > 0
                entry["collection_count"] = count
            elif kind == "connection":
                schema = defn.get("config_schema") or {}
                oauth = schema.get("x-oauth")
                entry["provider"] = defn.get("provider")
                entry["required_fields"] = schema.get("required", [])
                entry["oauth"] = oauth is not None
                entry["oauth_available"] = is_provider_configured(oauth["provider"]) if oauth else False
            results.append(entry)

        return {"status": "success", "kind": kind, "count": len(results), "definitions": results}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def get_definition(key: str, tool_context: ToolContext) -> dict[str, Any]:
    """Get a component definition's full catalog detail.

    For a source this includes the config schema, resource slots,
    destination types, and all its assets with their schemas. This is the
    catalog definition (the component *type*), not an instance from the
    org's collection.

    Args:
        key: The definition's catalog key (e.g. 'facebook_ads',
            'facebook_ads_connection').
    """
    try:
        catalog = get_catalog()
        defn = catalog.get(key)
        if defn is None:
            return {"status": "error", "error": f"Definition '{key}' not found in catalog"}
        return {"status": "success", "definition": defn}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# -- Asset schemas (kind-specific by nature) -------------------------------------


def get_asset_schema(source_key: str, asset_key: str, tool_context: ToolContext) -> dict[str, Any]:
    """Get the JSON schema for a specific asset within a source.

    Schemas come from the catalog: they are a property of the source
    definition, shared by every instance in the org's collection.

    Args:
        source_key: The source key (e.g. 'facebook_ads').
        asset_key: The asset key within the source (e.g. 'ad_insights').

    Returns the asset schema with field names, types, and descriptions.
    """
    try:
        catalog = get_catalog()
        defn = catalog.get(source_key)
        if defn is None or defn.get("kind") != "source":
            return {"status": "error", "error": f"Source '{source_key}' not found in catalog"}

        for asset_def in defn.get("assets", []):
            if asset_def.get("key") == asset_key or asset_def.get("qualified_key") == f"{source_key}.{asset_key}":
                return {
                    "status": "success",
                    "source_key": source_key,
                    "asset_key": asset_key,
                    "qualified_key": f"{source_key}.{asset_key}",
                    "schema": asset_def.get("asset_schema"),
                    "partitioning": asset_def.get("partitioning"),
                    "tags": asset_def.get("tags", []),
                    "requires": asset_def.get("requires", {}),
                    "optional_requires": asset_def.get("optional_requires", {}),
                }

        return {"status": "error", "error": f"Asset '{asset_key}' not found in source '{source_key}'"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def search_fields(query: str, tool_context: ToolContext) -> dict[str, Any]:
    """Search for fields across all asset schemas in the catalog matching a query string.

    Searches every source definition's schemas, whether or not the source is
    in the org's collection.

    Args:
        query: Substring to search for in field names and descriptions (case-insensitive).

    Returns matching fields grouped by source and asset, with field type and description.
    """
    try:
        catalog = get_catalog()
        query_lower = query.lower()
        matches: list[dict[str, Any]] = []

        for key, defn in catalog.items():
            if defn.get("kind") != "source":
                continue
            source_key = key
            for asset_def in defn.get("assets", []):
                asset_key = asset_def.get("key", "")
                schema = asset_def.get("asset_schema")
                if not schema or "properties" not in schema:
                    continue
                for field_name, field_info in schema["properties"].items():
                    field_desc = field_info.get("description", "")
                    if query_lower in field_name.lower() or query_lower in field_desc.lower():
                        matches.append({
                            "source_key": source_key,
                            "asset_key": asset_key,
                            "qualified_key": f"{source_key}.{asset_key}",
                            "field_name": field_name,
                            "field_type": _extract_type(field_info),
                            "description": field_desc,
                        })

        return {"status": "success", "query": query, "match_count": len(matches), "matches": matches}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def compare_schemas(
    source_key_a: str,
    asset_key_a: str,
    source_key_b: str,
    asset_key_b: str,
    tool_context: ToolContext,
) -> dict[str, Any]:
    """Compare the schemas of two assets side by side.

    Args:
        source_key_a: Source key for the first asset.
        asset_key_a: Asset key for the first asset.
        source_key_b: Source key for the second asset.
        asset_key_b: Asset key for the second asset.

    Returns shared fields, fields unique to each asset, and any type mismatches.
    """
    try:
        schema_a = _get_schema_properties(source_key_a, asset_key_a)
        schema_b = _get_schema_properties(source_key_b, asset_key_b)

        if isinstance(schema_a, dict) and "error" in schema_a:
            return {"status": "error", "error": schema_a["error"]}
        if isinstance(schema_b, dict) and "error" in schema_b:
            return {"status": "error", "error": schema_b["error"]}

        assert isinstance(schema_a, dict) and isinstance(schema_b, dict)
        fields_a = set(schema_a.keys())
        fields_b = set(schema_b.keys())

        shared = fields_a & fields_b
        only_a = fields_a - fields_b
        only_b = fields_b - fields_a

        shared_details = []
        for field in sorted(shared):
            type_a = _extract_type(schema_a[field])
            type_b = _extract_type(schema_b[field])
            shared_details.append({
                "field": field,
                "type_a": type_a,
                "type_b": type_b,
                "type_match": type_a == type_b,
            })

        return {
            "status": "success",
            "asset_a": f"{source_key_a}.{asset_key_a}",
            "asset_b": f"{source_key_b}.{asset_key_b}",
            "shared_count": len(shared),
            "only_a_count": len(only_a),
            "only_b_count": len(only_b),
            "shared_fields": shared_details,
            "only_in_a": sorted(only_a),
            "only_in_b": sorted(only_b),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


# -- Helpers -------------------------------------------------------------------


def _extract_type(field_info: dict[str, Any]) -> str:
    """Extract a human-readable type string from a JSON schema field definition."""
    if "anyOf" in field_info:
        types = [t.get("type", "unknown") for t in field_info["anyOf"] if t.get("type") != "null"]
        return types[0] if len(types) == 1 else " | ".join(types) if types else "any"
    return field_info.get("type", "unknown")


def _get_schema_properties(source_key: str, asset_key: str) -> dict[str, Any]:
    """Retrieve the properties dict from an asset's JSON schema."""
    catalog = get_catalog()
    defn = catalog.get(source_key)
    if defn is None or defn.get("kind") != "source":
        return {"error": f"Source '{source_key}' not found in catalog"}
    for asset_def in defn.get("assets", []):
        if asset_def.get("key") == asset_key:
            schema = asset_def.get("asset_schema")
            if not schema or "properties" not in schema:
                return {"error": f"Asset '{source_key}.{asset_key}' has no schema"}
            return schema["properties"]
    return {"error": f"Asset '{asset_key}' not found in source '{source_key}'"}
