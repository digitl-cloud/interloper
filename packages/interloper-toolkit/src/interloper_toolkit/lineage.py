"""Lineage tools — dependency analysis, impact assessment, and DAG traversal."""

from __future__ import annotations

from collections import defaultdict
from typing import Any
from uuid import UUID

from interloper_toolkit.context import ToolkitContext


def get_upstream(ctx: ToolkitContext, asset_id: str) -> dict[str, Any]:
    """Get the direct upstream dependencies of an asset.

    Args:
        asset_id: UUID of the asset to inspect.

    Returns the list of upstream assets that this asset depends on,
    including the parameter name used for each dependency.
    """
    try:
        deps = ctx.store.list_relations(ctx.org_id, type="dependency")
        target = UUID(asset_id)

        upstream = []
        for dep in deps:
            if dep.src_id == target:
                asset = ctx.store.get_component(dep.dst_id, kind="asset")
                upstream.append({
                    "upstream_asset_id": str(dep.dst_id),
                    "param_name": dep.slot,
                    "asset_key": asset.key,
                    "source_id": str(asset.parent_id),
                })

        return {"status": "success", "asset_id": asset_id, "upstream": upstream}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def get_downstream(ctx: ToolkitContext, asset_id: str) -> dict[str, Any]:
    """Get the direct downstream dependents of an asset.

    Args:
        asset_id: UUID of the asset to inspect.

    Returns the list of assets that directly depend on this asset.
    """
    try:
        deps = ctx.store.list_relations(ctx.org_id, type="dependency")
        target = UUID(asset_id)

        downstream = []
        for dep in deps:
            if dep.dst_id == target:
                asset = ctx.store.get_component(dep.src_id, kind="asset")
                downstream.append({
                    "asset_id": str(dep.src_id),
                    "param_name": dep.slot,
                    "asset_key": asset.key,
                    "source_id": str(asset.parent_id),
                })

        return {"status": "success", "asset_id": asset_id, "downstream": downstream}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def get_full_lineage(ctx: ToolkitContext, asset_id: str, direction: str = "upstream") -> dict[str, Any]:
    """Recursively traverse the full lineage of an asset.

    Args:
        asset_id: UUID of the asset to start from.
        direction: Either 'upstream' (ancestors) or 'downstream' (dependents).

    Returns an ordered list of assets in the lineage chain with depth levels.
    """
    try:
        adj, asset_info = _build_adjacency(ctx, direction)
        target = UUID(asset_id)

        visited: set[UUID] = set()
        result: list[dict[str, Any]] = []
        queue: list[tuple[UUID, int]] = [(target, 0)]

        while queue:
            current, depth = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            if current != target:
                info = asset_info.get(current, {})
                result.append({"asset_id": str(current), "depth": depth, **info})
            for neighbor in adj.get(current, []):
                if neighbor not in visited:
                    queue.append((neighbor, depth + 1))

        return {
            "status": "success",
            "asset_id": asset_id,
            "direction": direction,
            "lineage_count": len(result),
            "lineage": result,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def impact_analysis(ctx: ToolkitContext, asset_id: str) -> dict[str, Any]:
    """Analyze the downstream impact if an asset fails or is disabled.

    Args:
        asset_id: UUID of the asset to analyze.

    Returns all downstream assets grouped by source, with total affected count.
    """
    try:
        adj, asset_info = _build_adjacency(ctx, "downstream")
        target = UUID(asset_id)

        visited: set[UUID] = set()
        affected: list[dict[str, Any]] = []
        queue: list[tuple[UUID, int]] = [(target, 0)]

        while queue:
            current, depth = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            if current != target:
                info = asset_info.get(current, {})
                affected.append({"asset_id": str(current), "depth": depth, **info})
            for neighbor in adj.get(current, []):
                if neighbor not in visited:
                    queue.append((neighbor, depth + 1))

        # Group by source
        by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in affected:
            by_source[item.get("source_key", "unknown")].append(item)

        return {
            "status": "success",
            "asset_id": asset_id,
            "total_affected": len(affected),
            "by_source": {k: v for k, v in by_source.items()},
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def cross_source_dependencies(ctx: ToolkitContext) -> dict[str, Any]:
    """List all dependency edges that cross source boundaries.

    Returns edges where the upstream and downstream assets belong to
    different sources.
    """
    try:
        deps = ctx.store.list_relations(ctx.org_id, type="dependency")
        assets = ctx.store.list_components(ctx.org_id, kinds=["asset"])

        asset_source: dict[UUID, UUID | None] = {}
        asset_info: dict[UUID, dict[str, str]] = {}
        for a in assets:
            if not a.id:
                continue
            asset_source[a.id] = a.parent_id
            asset_info[a.id] = {"asset_key": a.key, "source_id": str(a.parent_id) if a.parent_id else ""}

        cross_deps = []
        for dep in deps:
            src_down = asset_source.get(dep.src_id)
            src_up = asset_source.get(dep.dst_id)
            if src_down and src_up and src_down != src_up:
                cross_deps.append({
                    "downstream_asset_id": str(dep.src_id),
                    "downstream": asset_info.get(dep.src_id, {}),
                    "upstream_asset_id": str(dep.dst_id),
                    "upstream": asset_info.get(dep.dst_id, {}),
                    "param_name": dep.slot,
                })

        return {"status": "success", "cross_source_count": len(cross_deps), "dependencies": cross_deps}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# -- Helpers -------------------------------------------------------------------


def _build_adjacency(
    ctx: ToolkitContext,
    direction: str,
) -> tuple[dict[UUID, list[UUID]], dict[UUID, dict[str, Any]]]:
    """Build an adjacency map and asset info lookup from all dependencies.

    Args:
        ctx: The toolkit context.
        direction: 'upstream' builds child→parents; 'downstream' builds parent→children.

    Returns:
        (adjacency_map, asset_info_map)
    """
    deps = ctx.store.list_relations(ctx.org_id, type="dependency")
    assets = ctx.store.list_components(ctx.org_id, kinds=["asset"])

    asset_info: dict[UUID, dict[str, Any]] = {}
    source_keys: dict[UUID, str] = {}
    for a in assets:
        if not a.id:
            continue
        asset_info[a.id] = {"asset_key": a.key, "source_id": str(a.parent_id) if a.parent_id else ""}
        if a.parent and a.parent_id:
            source_keys[a.parent_id] = a.parent.key

    # Add source_key to asset_info
    for uid, info in asset_info.items():
        sid_str = info["source_id"]
        if sid_str:
            info["source_key"] = source_keys.get(UUID(sid_str), "unknown")
        else:
            info["source_key"] = ""

    adj: dict[UUID, list[UUID]] = defaultdict(list)
    for dep in deps:
        if direction == "upstream":
            adj[dep.src_id].append(dep.dst_id)
        else:
            adj[dep.dst_id].append(dep.src_id)

    return adj, asset_info
