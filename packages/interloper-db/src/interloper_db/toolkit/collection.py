"""Collection tools — the org's component instances, read-only.

Creation and connection operations stay with the agent; this module is
shared with surfaces that must stay read-only.
"""

from __future__ import annotations

from typing import Any

from interloper.component import KINDS

from interloper_db.toolkit.context import ToolkitContext, serialize


def list_components(ctx: ToolkitContext, kind: str | None = None) -> dict[str, Any]:
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
        if kind is None:
            counts: dict[str, int] = {}
            for c in ctx.store.list_components(ctx.org_id):
                counts[c.kind] = counts.get(c.kind, 0) + 1
            return {
                "status": "success",
                "component_counts": counts,
                "message": "Call again with a kind for the entries.",
            }
        if kind not in KINDS:
            return {"status": "error", "error": f"Unknown kind '{kind}'", "valid_kinds": sorted(KINDS.keys())}

        results = []
        for c in ctx.store.list_components(ctx.org_id, kinds=[kind]):
            entry: dict[str, Any] = {
                "id": serialize(c.id),
                "key": c.key,
                "name": c.name,
                "type_name": (ctx.catalog.get(c.key) or {}).get("name", c.key),
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
