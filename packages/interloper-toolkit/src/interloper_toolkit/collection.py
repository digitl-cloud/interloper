"""Collection tools: the org's component instances.

``list_components`` is read-only, shared with surfaces that must stay
read-only (kind-specific creation and connection operations stay with the
agent). ``bind_relation`` and ``unbind_relation`` are the exception: they
write, generically over every kind's declared relations, so a caller must
never register them alongside a read-only tool set (see
``interloper_mcp.tools``, deliberately read-only), only wherever that
surface's own write tools already live.
"""

from __future__ import annotations

from uuid import UUID

from interloper.component import KINDS
from interloper.errors import ConfigError, NotFoundError

from interloper_toolkit.context import ToolkitContext
from interloper_toolkit.models import (
    BindResult,
    ComponentCounts,
    ComponentList,
    ComponentSummary,
    ToolError,
    UnbindResult,
)


def list_components(
    ctx: ToolkitContext, kind: str | None = None
) -> ComponentCounts | ComponentList | ToolError:
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
            for c in ctx.store.components.list_all(ctx.org_id):
                counts[c.kind] = counts.get(c.kind, 0) + 1
            return ComponentCounts(
                component_counts=counts,
                message="Call again with a kind for the entries.",
            )
        if kind not in KINDS:
            return ToolError(error=f"Unknown kind '{kind}'", valid_kinds=sorted(KINDS.keys()))

        results = []
        for c in ctx.store.components.list_all(ctx.org_id, kinds=[kind]):
            entry = ComponentSummary(
                id=str(c.id),
                key=c.key,
                name=c.name,
                type_name=(ctx.catalog.get(c.key) or {}).get("name", c.key),
                created_at=c.created_at,
            )
            # Fail closed: a sensitive kind's config is (or wraps) credentials.
            if not KINDS[kind].sensitive:
                entry.config = c.config
            if kind == "source":
                entry.asset_count = len(c.children)
            results.append(entry)

        return ComponentList(kind=kind, count=len(results), components=results)
    except Exception as e:
        return ToolError(error=str(e))


# -- Relations (write) ---------------------------------------------------------


def bind_relation(ctx: ToolkitContext, component_id: str, name: str, dst_id: str) -> BindResult | ToolError:
    """Bind one component to another under a declared relation name.

    Works on any kind: a source's connection, a job's watched assets, a
    destination target, whatever the component's own class declares under
    that name. A ``many`` name accumulates; a single-valued one repoints, so
    rebinding it needs no prior unbind_relation call.

    Args:
        component_id: UUID of the component the relation originates from.
        name: Relation name, which the component's class must declare.
        dst_id: UUID of the destination component the relation points at.
            Must belong to the same organisation as the source.
    """
    try:
        row = ctx.store.relations.add(UUID(component_id), name=name, dst_id=UUID(dst_id))
    except (ConfigError, NotFoundError, ValueError) as e:
        return ToolError(error=str(e))
    return BindResult(src_id=str(row.src_id), name=row.name, dst_id=str(row.dst_id), dst_kind=row.dst_kind)


def unbind_relation(ctx: ToolkitContext, component_id: str, name: str, dst_id: str) -> UnbindResult | ToolError:
    """Detach one component from another under a declared relation name.

    Args:
        component_id: UUID of the component the relation originates from.
        name: Relation name the edge is filed under.
        dst_id: UUID of the destination the removed edge points at. Removing
            an edge that isn't there is a no-op.
    """
    try:
        ctx.store.relations.remove(UUID(component_id), name=name, dst_id=UUID(dst_id))
    except (ConfigError, NotFoundError, ValueError) as e:
        return ToolError(error=str(e))
    return UnbindResult(src_id=component_id, name=name, dst_id=dst_id)
