"""Relation policy: validated reads and writes for the edge table.

The class vocabulary is the contract. It resolves parent-aware — a
source-owned asset's definition (dependency slots, ``required`` flags)
lives on the parent source's definition — with the kind's anchor as the
drift fallback. Writes enforce the declared shape: relation type, dst
kind, slot names, and each slot's expected destination identity (resolved
through :func:`~interloper.asset.base.expected_dependency` for dependency
slots). Unbinding follows the vocabulary's ``on_unbind`` semantics: bound
required slots of a blocking type refuse it. Rows are stamped with the
denormalized ``org_id``/``src_kind``/``dst_kind`` triple the composite
foreign keys verify.
"""

from __future__ import annotations

from collections.abc import Iterable
from uuid import UUID

import interloper as il
from interloper.asset.base import expected_dependency
from interloper.errors import ConfigError, NotFoundError
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from interloper_db.models import Component, ComponentRelation
from interloper_db.store.base import StoreBase

# One relation binding: (destination component id, slot). Slot is "" for
# slotless relation types.
Binding = tuple[UUID, str]


class RelationMixin(StoreBase):
    """Store methods for validated relation reads and writes."""

    def list_relations(self, org_id: UUID, *, type: str | None = None) -> list[ComponentRelation]:
        """List an organisation's component relations, optionally by type."""
        with self._session() as session:
            statement = select(ComponentRelation).where(ComponentRelation.org_id == org_id)
            if type:
                statement = statement.where(ComponentRelation.type == type)
            return list(session.exec(statement).all())

    def add_relation(self, component_id: UUID, *, type: str, dst_id: UUID, slot: str = "") -> ComponentRelation:
        """Add one relation from a component.

        Slotted types upsert per slot: re-binding an already-bound slot
        repoints it to the new destination, and re-adding an identical
        relation is a no-op returning the existing row.

        Returns:
            The relation (created, repointed, or already present).

        Raises:
            NotFoundError: If either endpoint is missing or cross-org.
            ConfigError: If the kind's vocabulary doesn't declare the type or
                the slot, or the destination doesn't match the slot's shape.
        """
        with self._session() as session:
            src = session.get(Component, component_id)
            if not src:
                raise NotFoundError(f"Component {component_id} not found")
            definition = self._check_vocabulary(session, src, type)
            dst = self._resolve_dst(session, src, definition, type, slot, dst_id)
            relation = _upsert_relation(session, src, dst, type, slot, per_slot=definition.slotted)
            try:
                session.commit()
            except IntegrityError:
                raise ConfigError(
                    f"Relation '{type}'{f' slot {slot!r}' if slot else ''} on '{src.key}' "
                    f"was modified concurrently; retry"
                )
            return relation

    def remove_relation(self, component_id: UUID, *, type: str, dst_id: UUID) -> None:
        """Remove a component's relations of one type toward one destination.

        Raises:
            ConfigError: If a matching edge fills a slot the vocabulary
                refuses to unbind (a bound required dependency) — repoint
                the slot or remove the dependent asset instead.
        """
        with self._session() as session:
            statement = select(ComponentRelation).where(
                ComponentRelation.src_id == component_id,
                ComponentRelation.type == type,
                ComponentRelation.dst_id == dst_id,
            )
            relations = session.exec(statement).all()
            src = session.get(Component, component_id) if relations else None
            if src is not None:
                definition = self._relation_vocabulary(session, src).get(type)
                if blocked := _blocked_unbinds(definition, (relation.slot for relation in relations)):
                    raise ConfigError(
                        f"Required '{type}' slot(s) {blocked} of '{src.key}' cannot be unbound; "
                        f"repoint them or remove the dependent asset instead"
                    )
            for relation in relations:
                session.delete(relation)
            session.commit()

    def _sync_relations(
        self,
        session: Session,
        src: Component,
        relations: dict[str, list[Binding]] | None,
    ) -> None:
        """Replace the relation types present in *relations* (empty list clears).

        Bound slots the vocabulary refuses to unbind (required dependencies)
        must stay bound — repointing (same slot, different destination) is
        allowed.
        """
        for type_, bindings in (relations or {}).items():
            definition = self._check_vocabulary(session, src, type_)
            existing = session.exec(
                select(ComponentRelation).where(ComponentRelation.src_id == src.id, ComponentRelation.type == type_)
            ).all()
            kept = {slot for _, slot in bindings}
            dropped = (relation.slot for relation in existing if relation.slot not in kept)
            if blocked := _blocked_unbinds(definition, dropped):
                raise ConfigError(
                    f"Required '{type_}' slot(s) {blocked} of '{src.key}' cannot be unbound; "
                    f"repoint them or remove the dependent asset instead"
                )
            for relation in existing:
                session.delete(relation)
            session.flush()
            for dst_id, slot in bindings:
                dst = self._resolve_dst(session, src, definition, type_, slot, dst_id)
                _add_relation(session, src, dst, type_, slot)

    def _check_vocabulary(self, session: Session, src: Component, type_: str) -> il.RelationDefinition:
        """Reject relation types the row's class vocabulary doesn't declare.

        Returns:
            The type's relation definition (slots included for owned assets).
        """
        vocabulary = self._relation_vocabulary(session, src)
        if type_ not in vocabulary:
            raise ConfigError(
                f"Components of kind '{src.kind}' ('{src.key}') declare no '{type_}' relations "
                f"(allowed: {sorted(vocabulary) or 'none'})"
            )
        return vocabulary[type_]

    def _resolve_dst(
        self, session: Session, src: Component, definition: il.RelationDefinition, type_: str, slot: str, dst_id: UUID
    ) -> Component:
        """Resolve a relation destination, enforcing existence, same-org, and the vocabulary's shape."""
        dst = session.get(Component, dst_id)
        if dst is None or dst.org_id != src.org_id:
            raise NotFoundError(f"Component {dst_id} not found (relation '{type_}'{f'/{slot}' if slot else ''})")
        if dst.id == src.id:
            raise ConfigError(f"Relation '{type_}' on '{src.key}' may not point at the component itself")
        if dst.kind not in definition.kinds:
            raise ConfigError(
                f"Relation '{type_}' on kind '{src.kind}' may not point at a '{dst.kind}' "
                f"component (allowed: {definition.kinds})"
            )
        if not definition.slotted and slot:
            raise ConfigError(f"Relation '{type_}' on kind '{src.kind}' is not slotted (got slot '{slot}')")
        # A definition with no declared slots (a kind anchor reached through
        # the drift fallback) can't vet slot names or targets — skip, fail-open.
        if definition.slotted and definition.slots:
            slot_def = definition.slots.get(slot)
            if slot_def is None:
                raise ConfigError(
                    f"Relation '{type_}' on '{src.key}' declares no slot '{slot}' "
                    f"(declared: {sorted(definition.slots)})"
                )
            self._check_slot_target(session, src, type_, slot, slot_def, dst)
        return dst

    def _check_slot_target(
        self, session: Session, src: Component, type_: str, slot: str, slot_def: il.RelationSlot, dst: Component
    ) -> None:
        """Enforce a slot's declared destination identity, when it declares one.

        Dependency slot keys resolve through ``expected_dependency``: an
        intra-source dep (the declarer's own source) must bind a sibling of
        the same source instance; a cross-source one accepts the named
        source's asset from any instance. Other slotted types (resources)
        declare a plain component key.
        """
        if not slot_def.key:
            return
        if type_ != "dependency":
            if dst.key != slot_def.key:
                raise ConfigError(
                    f"Relation '{type_}' slot '{slot}' of '{src.key}' expects a '{slot_def.key}' "
                    f"component, got '{dst.key}'"
                )
            return
        src_parent = session.get(Component, src.parent_id) if src.parent_id else None
        own_source_key = src_parent.key if src_parent else None
        expected_source, expected_asset = expected_dependency(slot_def.key, own_source_key=own_source_key)
        if dst.key != expected_asset:
            raise ConfigError(
                f"Dependency slot '{slot}' of '{src.key}' expects asset '{slot_def.key}', got '{dst.key}'"
            )
        if expected_source == own_source_key:
            if src.parent_id is not None and dst.parent_id != src.parent_id:
                raise ConfigError(
                    f"Dependency slot '{slot}' of '{src.key}' must bind a sibling asset "
                    f"of the same source instance"
                )
        else:
            dst_parent = session.get(Component, dst.parent_id) if dst.parent_id else None
            if dst_parent is None or dst_parent.key != expected_source:
                raise ConfigError(
                    f"Dependency slot '{slot}' of '{src.key}' expects an asset of source "
                    f"'{expected_source}', got one of '{dst_parent.key if dst_parent else 'none'}'"
                )

    def _relation_vocabulary(self, session: Session, db_src: Component) -> dict[str, il.RelationDefinition]:
        """A referrer row's relation vocabulary, slots included.

        Source-owned assets are not top-level catalog entries: their concrete
        definition (which carries the dependency slots' ``required`` flags)
        comes from the parent source's definition. Everything else resolves
        through the catalog with the kind's anchor as drift fallback.
        """
        if db_src.kind == "asset" and db_src.parent_id is not None:
            parent = session.get(Component, db_src.parent_id)
            parent_definition = self._catalog.get(parent.key) if parent else None
            for asset_definition in getattr(parent_definition, "assets", []):
                if asset_definition.key == db_src.key:
                    return asset_definition.relations
        return self._catalog.vocabulary(db_src.kind, db_src.key)

    def _relation_detaches(self, session: Session, db_src: Component, relation: ComponentRelation) -> bool:
        """Whether a relation detaches (rather than blocks) when its destination is deleted.

        Consults the referrer's own vocabulary: a type declared
        ``on_delete="detach"`` detaches, as does a slot the referrer declares
        optional (``RelationSlot.required=False`` — an ``optional_requires``
        dependency). Anything unresolvable — unknown type, drifted key,
        undeclared slot — blocks, keeping the guard fail-closed.
        """
        definition = self._relation_vocabulary(session, db_src).get(relation.type)
        if definition is None:
            return False
        if definition.on_delete == "detach":
            return True
        slot = definition.slots.get(relation.slot)
        return slot is not None and not slot.required


def _blocked_unbinds(definition: il.RelationDefinition | None, slots: Iterable[str]) -> list[str]:
    """Bound slots whose explicit unbinding the vocabulary refuses.

    A slot blocks when its type declares ``on_unbind="block"`` and the slot
    is required. Unknown definitions or slots (drift) don't block.
    """
    if definition is None or definition.on_unbind != "block":
        return []
    return sorted(
        {slot for slot in slots if (slot_def := definition.slots.get(slot)) is not None and slot_def.required}
    )


def _add_relation(session: Session, src: Component, dst: Component, type_: str, slot: str = "") -> ComponentRelation:
    """Add one relation, stamping the denormalized org/kind triple from the rows.

    Returns:
        The pending relation (added to the session, not flushed).
    """
    relation = ComponentRelation(
        src_id=src.id,
        type=type_,
        slot=slot,
        dst_id=dst.id,
        org_id=src.org_id,
        src_kind=src.kind,
        dst_kind=dst.kind,
    )
    session.add(relation)
    return relation


def _upsert_relation(
    session: Session, src: Component, dst: Component, type_: str, slot: str, *, per_slot: bool
) -> ComponentRelation:
    """Idempotent relation write: an identical edge is returned as-is.

    With *per_slot* (slotted types), a slot bound to a different destination
    is repointed — the old edge is replaced by the new one.
    """
    statement = select(ComponentRelation).where(
        ComponentRelation.src_id == src.id,
        ComponentRelation.type == type_,
        ComponentRelation.slot == slot,
    )
    if not per_slot:
        statement = statement.where(ComponentRelation.dst_id == dst.id)
    existing = session.exec(statement).all()
    if match := next((relation for relation in existing if relation.dst_id == dst.id), None):
        return match
    for relation in existing:
        session.delete(relation)
    if existing:
        session.flush()
    return _add_relation(session, src, dst, type_, slot)
