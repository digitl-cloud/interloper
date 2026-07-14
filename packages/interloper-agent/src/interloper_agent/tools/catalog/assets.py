"""Asset tools — schema inspection, field search, and schema comparison.

Asset schemas live in the catalog: a schema is a property of the source
definition, shared by every instance in an org's collection.
"""

from __future__ import annotations

from typing import Any

from google.adk.tools.tool_context import ToolContext

from interloper_agent.context import get_catalog


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
