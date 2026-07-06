"""Shared component persistence machinery.

Every kind-specific store mixin builds on these helpers: fetch a component
row (optionally kind-checked), replace a component's relations of one type,
and the standard eager-load option set for list/detail queries.

``sync_relations`` is the only write path for component relations. It
resolves each destination row to stamp the relation with the denormalized
``org_id``/``src_kind``/``dst_kind`` triple that the composite foreign keys
verify — so a bad binding (missing or cross-org destination) fails here
with a clear error rather than as an FK violation at commit.
"""

from __future__ import annotations

from uuid import UUID

from interloper.errors import NotFoundError
from sqlalchemy.orm import selectinload
from sqlmodel import Session, select

from interloper_db.models import Component, ComponentRelation

# Eager-load set for rows returned to API consumers: the parent, children
# with their relations, and two hops (component → destination → resources).
COMPONENT_LOAD_OPTIONS = [
    selectinload(Component.parent),  # ty: ignore[invalid-argument-type]
    selectinload(Component.children)  # ty: ignore[invalid-argument-type]
    .selectinload(Component.out_relations)  # ty: ignore[invalid-argument-type]
    .selectinload(ComponentRelation.dst),  # ty: ignore[invalid-argument-type]
    selectinload(Component.out_relations)  # ty: ignore[invalid-argument-type]
    .selectinload(ComponentRelation.dst)  # ty: ignore[invalid-argument-type]
    .selectinload(Component.out_relations)  # ty: ignore[invalid-argument-type]
    .selectinload(ComponentRelation.dst),  # ty: ignore[invalid-argument-type]
]


def get_component(session: Session, component_id: UUID, *, kind: str | None = None) -> Component:
    """Fetch a component row, optionally asserting its kind.

    Returns:
        The component row.

    Raises:
        NotFoundError: If no row exists, or it has a different kind.
    """
    db_component = session.get(Component, component_id)
    if not db_component or (kind is not None and db_component.kind != kind):
        raise NotFoundError(f"{kind or 'component'} {component_id} not found".capitalize())
    return db_component


def load_component(session: Session, component_id: UUID, *, kind: str | None = None) -> Component:
    """Fetch a component row with children and relations eager-loaded.

    Returns:
        The component row, safe to hand out detached.

    Raises:
        NotFoundError: If no row exists, or it has a different kind.
    """
    statement = select(Component).where(Component.id == component_id).options(*COMPONENT_LOAD_OPTIONS)
    db_component = session.exec(statement).first()
    if not db_component or (kind is not None and db_component.kind != kind):
        raise NotFoundError(f"{kind or 'component'} {component_id} not found".capitalize())
    return db_component


def list_components(session: Session, org_id: UUID, *, kind: str) -> list[Component]:
    """List an organisation's components of one kind, eager-loaded."""
    statement = (
        select(Component)
        .where(Component.org_id == org_id, Component.kind == kind)
        .options(*COMPONENT_LOAD_OPTIONS)
        .order_by(Component.created_at)  # ty: ignore[invalid-argument-type]
    )
    return list(session.exec(statement).all())


def add_relation(session: Session, src: Component, dst: Component, type: str, slot: str = "") -> ComponentRelation:
    """Add one relation, stamping the denormalized org/kind triple from the rows.

    Returns:
        The pending relation (added to the session, not flushed).
    """
    relation = ComponentRelation(
        src_id=src.id,
        type=type,
        slot=slot,
        dst_id=dst.id,
        org_id=src.org_id,
        src_kind=src.kind,
        dst_kind=dst.kind,
    )
    session.add(relation)
    return relation


def sync_relations(
    session: Session,
    src: Component,
    type: str,
    bindings: dict[str, UUID] | list[UUID] | None,
) -> None:
    """Replace all relations of *type* from *src* with the given bindings.

    Args:
        session: Active DB session.
        src: The source component (must be persisted/flushed).
        type: The relation type to replace.
        bindings: ``{slot: dst_id}`` for slotted types, or a list of
            destination ids for slotless ones. ``None`` clears the type.

    Raises:
        NotFoundError: If a destination id resolves to no component, or to
            one in another organisation.
    """
    existing = session.exec(
        select(ComponentRelation).where(ComponentRelation.src_id == src.id, ComponentRelation.type == type)
    ).all()
    for relation in existing:
        session.delete(relation)
    session.flush()

    items = bindings.items() if isinstance(bindings, dict) else [("", dst_id) for dst_id in bindings or []]
    for slot, dst_id in items:
        dst = session.get(Component, dst_id)
        if dst is None or dst.org_id != src.org_id:
            raise NotFoundError(f"Component {dst_id} not found (binding '{type}'{f'/{slot}' if slot else ''})")
        add_relation(session, src, dst, type, slot)
