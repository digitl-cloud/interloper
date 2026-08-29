"""Relation policy: validated reads and writes for the edge table.

The class vocabulary is the contract. It resolves parent-aware — a
source-owned asset's definition (dependency slots, ``required`` flags)
lives on the parent source's definition — with the kind's anchor as the
drift fallback. Writes enforce the declared shape: relation type, dst
kind, slot names, and each slot's expected destination identity (resolved
through :meth:`~interloper.asset.base.AssetIdentity.resolve` for dependency
slots). Unbinding follows the vocabulary's ``on_unbind`` semantics: bound
required slots of a blocking type refuse it. Rows are stamped with the
denormalized ``org_id``/``src_kind``/``dst_kind`` triple the composite
foreign keys verify.
"""

from __future__ import annotations

from collections.abc import Iterable
from uuid import UUID

import interloper as il
from interloper.asset.base import AssetIdentity
from interloper.catalog.base import Catalog
from interloper.errors import ConfigError, NotFoundError
from sqlalchemy import Engine
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from interloper_db.models import Component, ComponentRelation
from interloper_db.session import commit, session_scope

# One relation binding: (destination component id, slot). Slot is "" for
# slotless relation types.
Binding = tuple[UUID, str]


class RelationStore:
    """Store methods for validated relation reads and writes."""

    def __init__(self, engine: Engine, catalog: Catalog) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
            catalog: Catalog its relation vocabulary resolves against.
        """
        self._engine = engine
        self._catalog = catalog

    # -- Public API ------------------------------------------------------------

    def list_all(self, org_id: UUID, *, type: str | None = None) -> list[ComponentRelation]:
        """List an organisation's component relations, optionally by type.

        Args:
            org_id: Organisation whose relations are listed.
            type: Relation type to restrict the listing to. None (the default)
                lists every type.

        Returns:
            The organisation's relation rows, in no guaranteed order.
        """
        with session_scope(self._engine) as session:
            statement = select(ComponentRelation).where(ComponentRelation.org_id == org_id)
            if type:
                statement = statement.where(ComponentRelation.type == type)
            return list(session.exec(statement).all())

    def add(self, component_id: UUID, *, type: str, dst_id: UUID, slot: str = "") -> ComponentRelation:
        """Add one relation from a component.

        Slotted types upsert per slot: re-binding an already-bound slot
        repoints it to the new destination, and re-adding an identical
        relation is a no-op returning the existing row.

        Args:
            component_id: Source component the relation originates from.
            type: Relation type, which the source kind's vocabulary must
                declare.
            dst_id: Destination component the relation points at. Must belong
                to the same organisation as the source.
            slot: Slot the relation fills, for slotted types. Empty (the
                default) for slotless ones.

        Returns:
            The relation (created, repointed, or already present).

        Raises:
            NotFoundError: If either endpoint is missing or cross-org.
            ConfigError: If the kind's vocabulary doesn't declare the type or
                the slot, or the destination doesn't match the slot's shape.
        """
        with session_scope(self._engine) as session:
            src = session.get(Component, component_id)
            if not src:
                raise NotFoundError(f"Component {component_id} not found")
            definition = self._check_vocabulary(session, src, type)
            dst = self._resolve_dst(session, src, definition, type, slot, dst_id)
            relation = self._upsert_relation(session, src, dst, type, slot, per_slot=definition.slotted)
            try:
                commit(session)
            except IntegrityError:
                raise ConfigError(
                    f"Relation '{type}'{f' slot {slot!r}' if slot else ''} on '{src.key}' "
                    f"was modified concurrently; retry"
                )
            return relation

    def remove(self, component_id: UUID, *, type: str, dst_id: UUID) -> None:
        """Remove a component's relations of one type toward one destination.

        Args:
            component_id: Source component the relations originate from.
            type: Relation type to remove.
            dst_id: Destination the removed relations point at. Every slot
                bound to it under *type* is removed.

        Raises:
            ConfigError: If a matching edge fills a slot the vocabulary
                refuses to unbind (a bound required dependency) — repoint
                the slot or remove the dependent asset instead.
        """
        with session_scope(self._engine) as session:
            statement = select(ComponentRelation).where(
                ComponentRelation.src_id == component_id,
                ComponentRelation.type == type,
                ComponentRelation.dst_id == dst_id,
            )
            relations = session.exec(statement).all()
            src = session.get(Component, component_id) if relations else None
            if src is not None:
                definition = self._relation_vocabulary(session, src).get(type)
                if blocked := self._blocked_unbinds(definition, (relation.slot for relation in relations)):
                    raise ConfigError(
                        f"Required '{type}' slot(s) {blocked} of '{src.key}' cannot be unbound; "
                        f"repoint them or remove the dependent asset instead"
                    )
            for relation in relations:
                session.delete(relation)
            commit(session)

    # -- Internals -------------------------------------------------------------

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

        Args:
            session: Open session the replacement is written through; deletes
                and inserts are flushed, never committed here.
            src: Source component whose relations are replaced.
            relations: Bindings to install, keyed by relation type. Only the
                types present are touched; an empty binding list clears that
                type. None leaves every relation untouched.

        Raises:
            ConfigError: If dropping a binding would unbind a required slot
                the vocabulary refuses to unbind.
        """
        for relation_type, bindings in (relations or {}).items():
            definition = self._check_vocabulary(session, src, relation_type)
            existing = session.exec(
                select(ComponentRelation).where(
                    ComponentRelation.src_id == src.id, ComponentRelation.type == relation_type
                )
            ).all()
            kept = {slot for _, slot in bindings}
            dropped = (relation.slot for relation in existing if relation.slot not in kept)
            if blocked := self._blocked_unbinds(definition, dropped):
                raise ConfigError(
                    f"Required '{relation_type}' slot(s) {blocked} of '{src.key}' cannot be unbound; "
                    f"repoint them or remove the dependent asset instead"
                )
            for relation in existing:
                session.delete(relation)
            session.flush()
            for dst_id, slot in bindings:
                dst = self._resolve_dst(session, src, definition, relation_type, slot, dst_id)
                _add_relation(session, src, dst, relation_type, slot)

    def _check_vocabulary(self, session: Session, src: Component, relation_type: str) -> il.RelationDefinition:
        """Reject relation types the row's class vocabulary doesn't declare.

        Args:
            session: Open session used to resolve the row's vocabulary.
            src: Source component whose vocabulary is consulted.
            relation_type: Relation type to check against that vocabulary.

        Returns:
            The type's relation definition (slots included for owned assets).

        Raises:
            ConfigError: If the source kind's vocabulary declares no relation
                of that type.
        """
        vocabulary = self._relation_vocabulary(session, src)
        if relation_type not in vocabulary:
            raise ConfigError(
                f"Components of kind '{src.kind}' ('{src.key}') declare no '{relation_type}' relations "
                f"(allowed: {sorted(vocabulary) or 'none'})"
            )
        return vocabulary[relation_type]

    def _resolve_dst(
        self,
        session: Session,
        src: Component,
        definition: il.RelationDefinition,
        relation_type: str,
        slot: str,
        dst_id: UUID,
    ) -> Component:
        """Resolve a relation destination, enforcing existence, same-org, and the vocabulary's shape.

        Args:
            session: Open session the destination row is loaded through.
            src: Source component the relation originates from.
            definition: Relation definition the destination must satisfy
                (allowed kinds, slotted-ness, declared slots).
            relation_type: Relation type, used for error messages.
            slot: Slot the relation fills. Empty for slotless types.
            dst_id: Destination component to resolve.

        Returns:
            The destination row, validated against the definition.

        Raises:
            NotFoundError: If the destination is missing or belongs to another
                organisation.
            ConfigError: If the destination is the source itself, its kind is
                not allowed, a slot is given for a slotless type, or the slot
                is not declared by the definition.
        """
        dst = session.get(Component, dst_id)
        if dst is None or dst.org_id != src.org_id:
            raise NotFoundError(
                f"Component {dst_id} not found (relation '{relation_type}'{f'/{slot}' if slot else ''})"
            )
        if dst.id == src.id:
            raise ConfigError(f"Relation '{relation_type}' on '{src.key}' may not point at the component itself")
        if dst.kind not in definition.kinds:
            raise ConfigError(
                f"Relation '{relation_type}' on kind '{src.kind}' may not point at a '{dst.kind}' "
                f"component (allowed: {definition.kinds})"
            )
        if not definition.slotted and slot:
            raise ConfigError(f"Relation '{relation_type}' on kind '{src.kind}' is not slotted (got slot '{slot}')")
        # A definition with no declared slots (a kind anchor reached through
        # the drift fallback) can't vet slot names or targets — skip, fail-open.
        if definition.slotted and definition.slots:
            slot_def = definition.slots.get(slot)
            if slot_def is None:
                raise ConfigError(
                    f"Relation '{relation_type}' on '{src.key}' declares no slot '{slot}' "
                    f"(declared: {sorted(definition.slots)})"
                )
            self._check_slot_target(session, src, relation_type, slot, slot_def, dst)
        return dst

    def _check_slot_target(
        self, session: Session, src: Component, relation_type: str, slot: str, slot_def: il.RelationSlot, dst: Component
    ) -> None:
        """Enforce a slot's declared destination identity, when it declares one.

        Dependency slot keys resolve through ``AssetIdentity.resolve``: an
        intra-source dep (the declarer's own source) must bind a sibling of
        the same source instance; a cross-source one accepts the named
        source's asset from any instance. Other slotted types (resources)
        declare a plain component key.

        Args:
            session: Open session the parent rows are loaded through.
            src: Source component declaring the slot.
            relation_type: Relation type the slot belongs to. ``"dependency"`` selects
                the asset-identity resolution described above.
            slot: Slot name, used for error messages.
            slot_def: Slot definition. An empty ``key`` declares no expected
                identity and the check is skipped.
            dst: Destination component to validate against the slot.

        Raises:
            ConfigError: If the destination's key, or the source instance it
                belongs to, doesn't match the slot's declared identity.
        """
        if not slot_def.key:
            return
        if relation_type != "dependency":
            if dst.key != slot_def.key:
                raise ConfigError(
                    f"Relation '{relation_type}' slot '{slot}' of '{src.key}' expects a '{slot_def.key}' "
                    f"component, got '{dst.key}'"
                )
            return
        src_parent = session.get(Component, src.parent_id) if src.parent_id else None
        own_source_key = src_parent.key if src_parent else None
        expected = AssetIdentity.resolve(slot_def.key, own_source_key=own_source_key)
        if dst.key != expected.asset_key:
            raise ConfigError(
                f"Dependency slot '{slot}' of '{src.key}' expects asset '{slot_def.key}', got '{dst.key}'"
            )
        if expected.source_key == own_source_key:
            if src.parent_id is not None and dst.parent_id != src.parent_id:
                raise ConfigError(
                    f"Dependency slot '{slot}' of '{src.key}' must bind a sibling asset "
                    f"of the same source instance"
                )
        else:
            dst_parent = session.get(Component, dst.parent_id) if dst.parent_id else None
            if dst_parent is None or dst_parent.key != expected.source_key:
                raise ConfigError(
                    f"Dependency slot '{slot}' of '{src.key}' expects an asset of source "
                    f"'{expected.source_key}', got one of '{dst_parent.key if dst_parent else 'none'}'"
                )

    def _relation_vocabulary(self, session: Session, db_source: Component) -> dict[str, il.RelationDefinition]:
        """A referrer row's relation vocabulary, slots included.

        Source-owned assets are not top-level catalog entries: their concrete
        definition (which carries the dependency slots' ``required`` flags)
        comes from the parent source's definition. Everything else resolves
        through the catalog with the kind's anchor as drift fallback.

        Args:
            session: Open session the parent row is loaded through.
            db_source: Referrer row whose vocabulary is resolved.

        Returns:
            The declared relation definitions keyed by relation type. Empty
            when nothing resolves.
        """
        if db_source.kind == "asset" and db_source.parent_id is not None:
            parent = session.get(Component, db_source.parent_id)
            parent_definition = self._catalog.get(parent.key) if parent else None
            for asset_definition in getattr(parent_definition, "assets", []):
                if asset_definition.key == db_source.key:
                    return asset_definition.relations
        return self._catalog.vocabulary(db_source.kind, db_source.key)

    def _relation_detaches(self, session: Session, db_source: Component, relation: ComponentRelation) -> bool:
        """Whether a relation detaches (rather than blocks) when its destination is deleted.

        Consults the referrer's own vocabulary: a type declared
        ``on_delete="detach"`` detaches, as does a slot the referrer declares
        optional (``RelationSlot.required=False`` — an ``optional_requires``
        dependency). Anything unresolvable — unknown type, drifted key,
        undeclared slot — blocks, keeping the guard fail-closed.

        Args:
            session: Open session the referrer's vocabulary is resolved
                through.
            db_source: Referrer row holding the relation.
            relation: Relation whose destination is about to be deleted.

        Returns:
            True if the relation may be detached, False if it blocks the
            deletion.
        """
        definition = self._relation_vocabulary(session, db_source).get(relation.type)
        if definition is None:
            return False
        if definition.on_delete == "detach":
            return True
        slot = definition.slots.get(relation.slot)
        return slot is not None and not slot.required

    @staticmethod
    def _blocked_unbinds(definition: il.RelationDefinition | None, slots: Iterable[str]) -> list[str]:
        """Bound slots whose explicit unbinding the vocabulary refuses.

        A slot blocks when its type declares ``on_unbind="block"`` and the slot
        is required. Unknown definitions or slots (drift) don't block.

        Args:
            definition: Relation definition the slots belong to. None (an
                unresolvable type) blocks nothing.
            slots: Slot names about to be unbound. Duplicates are collapsed.

        Returns:
            The blocking slot names, sorted. Empty when the unbind is allowed.
        """
        if definition is None or definition.on_unbind != "block":
            return []
        return sorted(
            {slot for slot in slots if (slot_def := definition.slots.get(slot)) is not None and slot_def.required}
        )

    @staticmethod
    def _upsert_relation(
        session: Session, src: Component, dst: Component, relation_type: str, slot: str, *, per_slot: bool
    ) -> ComponentRelation:
        """Idempotent relation write: an identical edge is returned as-is.

        With *per_slot* (slotted types), a slot bound to a different destination
        is repointed — the old edge is replaced by the new one.

        Args:
            session: Open session the write goes through; flushed, never
                committed here.
            src: Source component the relation originates from.
            dst: Destination component the relation points at.
            relation_type: Relation type to write.
            slot: Slot the relation fills. Empty for slotless types.
            per_slot: Whether the type is slotted, making the slot alone the
                identity of the edge (so re-binding repoints it). When False,
                the destination is part of that identity.

        Returns:
            The matching relation: the untouched existing row, or the newly
            added one (pending, not flushed).
        """
        statement = select(ComponentRelation).where(
            ComponentRelation.src_id == src.id,
            ComponentRelation.type == relation_type,
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
        return _add_relation(session, src, dst, relation_type, slot)


def _add_relation(
    session: Session, src: Component, dst: Component, relation_type: str, slot: str = ""
) -> ComponentRelation:
    """Add one relation, stamping the denormalized org/kind triple from the rows.

    Args:
        session: Open session the row is added to; not flushed or committed.
        src: Source component the relation originates from.
        dst: Destination component the relation points at.
        relation_type: Relation type to write.
        slot: Slot the relation fills. Empty (the default) for slotless types.

    Returns:
        The pending relation (added to the session, not flushed).
    """
    relation = ComponentRelation(
        src_id=src.id,
        type=relation_type,
        slot=slot,
        dst_id=dst.id,
        org_id=src.org_id,
        src_kind=src.kind,
        dst_kind=dst.kind,
    )
    session.add(relation)
    return relation


