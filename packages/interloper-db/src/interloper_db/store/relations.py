"""Relation policy: validated reads and writes for the named edge table.

An edge is a name, and the class declaring that name is the contract. The
vocabulary resolves parent-aware, since a source-owned asset's declaration
lives on the source that owns it, with the kind's anchor as the drift
fallback. Every write asks the declared
:class:`~interloper.component.relation.Relation` whether it accepts the
candidate, so the database enforces exactly what the framework enforces in
memory: the declared kinds, and the identity the declared keys expect
(bare, qualified or wildcard). No component fills its own relation, and a
bare key stays inside the owner's own source instance, which identities
alone cannot tell apart. Single-valued names hold one edge and repoint on
rewrite; ``many`` names accumulate. A non-optional name cannot be
emptied, only repointed. Every write takes the source row's lock, so the
guards cannot be read around concurrently. Rows are stamped with the
denormalized ``org_id``/``src_kind``/``dst_kind`` triple the composite
foreign keys verify.
"""

from __future__ import annotations

from uuid import UUID

import interloper as il
from interloper.catalog.base import Catalog
from interloper.errors import ConfigError, NotFoundError
from sqlalchemy import Engine
from sqlmodel import Session, col, select

from interloper_db.models import Component, ComponentRelation
from interloper_db.session import commit, session_scope

# One relation binding: the destination component id.
Binding = UUID


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

    def list_all(
        self,
        org_id: UUID,
        *,
        name: str | None = None,
        src_kind: str | None = None,
        dst_kind: str | None = None,
    ) -> list[ComponentRelation]:
        """List an organisation's component relations, optionally filtered.

        Args:
            org_id: Organisation whose relations are listed.
            name: Relation name to restrict the listing to. None (the default)
                lists every name.
            src_kind: Kind the source component must have. None (the default)
                accepts any kind.
            dst_kind: Kind the destination component must have. None (the
                default) accepts any kind.

        Returns:
            The organisation's matching relation rows, in no guaranteed order.
        """
        with session_scope(self._engine) as session:
            statement = select(ComponentRelation).where(ComponentRelation.org_id == org_id)
            if name:
                statement = statement.where(ComponentRelation.name == name)
            if src_kind:
                statement = statement.where(ComponentRelation.src_kind == src_kind)
            if dst_kind:
                statement = statement.where(ComponentRelation.dst_kind == dst_kind)
            return list(session.exec(statement).all())

    def add(self, component_id: UUID, *, name: str, dst_id: UUID) -> ComponentRelation:
        """Bind one component to another under a declared relation name.

        A ``many`` name accumulates; a single-valued one repoints, so
        rebinding it needs no :meth:`remove` first. Re-adding an edge that is
        already there returns it untouched. An undeclared name, a missing or
        cross-org endpoint and a destination the declared relation refuses all
        propagate from the checks this delegates to (:meth:`_lock`,
        :meth:`_relation`, :meth:`_resolve`).

        Args:
            component_id: Source component the relation originates from.
            name: Relation name, which the source's class must declare.
            dst_id: Destination component the relation points at. Must belong
                to the same organisation as the source.

        Returns:
            The relation row (created, repointed, or already present).
        """
        with session_scope(self._engine) as session:
            src = self._lock(session, component_id)
            relation = self._relation(session, src, name)
            dst = self._resolve(session, src, relation, name, dst_id)
            existing = self._rows(session, src.id, name)
            if match := next((row for row in existing if row.dst_id == dst.id), None):
                return match
            if not relation.many:
                for row in existing:
                    session.delete(row)
                session.flush()
            row = self._insert(session, src, dst, name)
            commit(session)
            return row

    def remove(self, component_id: UUID, *, name: str, dst_id: UUID) -> None:
        """Detach one component from another under a declared relation name.

        Takes the source row's lock before reading the edges, so the
        non-optional guard below cannot be read around by a concurrent write.
        A missing source propagates as a ``NotFoundError`` from
        :meth:`_lock`.

        Unlike every other write path this takes no undeclared-name guard,
        deliberately: a name the class no longer declares is exactly the row
        someone needs to clear, and refusing it would leave the component
        unloadable with no way out.

        Args:
            component_id: Source component the relation originates from.
            name: Relation name the edge is filed under.
            dst_id: Destination the removed edge points at. An edge that isn't
                there is a no-op.

        Raises:
            ConfigError: If the edge is the last one of a non-optional
                relation, which may be repointed but not emptied.
        """
        with session_scope(self._engine) as session:
            src = self._lock(session, component_id)
            rows = self._rows(session, src.id, name)
            row = next((candidate for candidate in rows if candidate.dst_id == dst_id), None)
            if row is None:
                return
            relation = self._vocabulary(session, src).get(name)
            if len(rows) == 1 and relation is not None and not relation.optional:
                raise ConfigError(
                    f"'{src.key}'.{name} is non-optional and cannot be emptied; "
                    f"repoint it or remove the dependent component instead"
                )
            session.delete(row)
            commit(session)

    def bind_siblings(
        self,
        session: Session,
        source_cls: type[il.Source],
        children_by_key: dict[str, Component],
    ) -> None:
        """Top up the intra-source edges a source class binds between its own assets.

        The wiring is read from
        :meth:`~interloper.source.base.Source.sibling_bindings`, the same
        classmethod a live source binds its assets from, so a persisted source
        holds exactly the edges its in-memory counterpart does: one edge per
        relation name, never onto the declaring asset itself, and nothing for a
        declared key that reaches outside the source. Idempotent over the full
        child set, so an asset enabled after its siblings still gets the edges
        into it wired, while a name that already holds an edge is left alone
        and a binding made by hand survives.

        Args:
            session: Open session the edges are added to; neither flushed nor
                committed here.
            source_cls: Catalog class whose declaration decides the wiring.
            children_by_key: The source's enabled child rows, keyed by asset key.
        """
        bound = {
            (row[0], row[1])
            for row in session.exec(
                select(ComponentRelation.src_id, ComponentRelation.name).where(
                    col(ComponentRelation.src_id).in_([child.id for child in children_by_key.values()])
                )
            ).all()
        }
        for asset_key, names in source_cls.sibling_bindings().items():
            child = children_by_key.get(asset_key)
            if child is None:
                continue
            for name, sibling_key in names.items():
                sibling = children_by_key.get(sibling_key)
                if sibling is None or (child.id, name) in bound:
                    continue
                self._insert(session, child, sibling, name)

    # -- Internals -------------------------------------------------------------

    def _sync_relations(self, session: Session, src: Component, bindings: dict[str, list[Binding]] | None) -> None:
        """Replace the relation names present in *bindings* (empty list clears).

        Takes the source row's lock, like every other write path, so two
        concurrent replacements of one name cannot interleave their deletes
        and inserts. An undeclared name and a destination the declared
        relation refuses both propagate from the checks this delegates to
        (:meth:`_relation`, :meth:`_resolve`).

        Args:
            session: Open session the replacement is written through; deletes
                and inserts are flushed, never committed here.
            src: Source component whose relations are replaced.
            bindings: Destination ids to install, keyed by relation name. Only
                the names present are touched, each replaced wholesale.
                None leaves every relation untouched.

        Raises:
            ConfigError: If a single-valued name is given several
                destinations, or if the replacement would empty a
                non-optional relation.
        """
        if not bindings:
            return
        src = self._lock(session, src.id)
        for name, dst_ids in bindings.items():
            relation = self._relation(session, src, name)
            if not relation.many and len(dst_ids) > 1:
                raise ConfigError(f"'{src.key}'.{name} is single-valued and takes one target at a time")
            existing = self._rows(session, src.id, name)
            # Gated on existing rows: creation may leave a non-optional name
            # unbound, which only hydration refuses; an update that clears
            # one is a removal.
            if existing and not dst_ids and not relation.optional:
                raise ConfigError(
                    f"'{src.key}'.{name} is non-optional and cannot be emptied; "
                    f"repoint it or remove the dependent component instead"
                )
            kept = set(dst_ids)
            for row in existing:
                if row.dst_id not in kept:
                    session.delete(row)
            session.flush()
            held = {row.dst_id for row in existing if row.dst_id in kept}
            for dst_id in dst_ids:
                if dst_id in held:
                    continue
                held.add(dst_id)
                self._insert(session, src, self._resolve(session, src, relation, name, dst_id), name)

    def _relation_detaches(self, session: Session, referrer: Component, relation_row: ComponentRelation) -> bool:
        """Whether an edge detaches (rather than blocks) when its destination is deleted.

        Consults the referrer's own vocabulary: a name declared
        ``on_delete="detach"`` detaches, as does an optional one, since the
        referrer keeps working with nothing bound there. Anything
        unresolvable (an undeclared name, a drifted key) blocks, keeping the
        guard fail-closed.

        Args:
            session: Open session the referrer's vocabulary is resolved
                through.
            referrer: Referrer row holding the edge.
            relation_row: Edge whose destination is about to be deleted.

        Returns:
            True if the edge may be detached, False if it blocks the deletion.
        """
        relation = self._vocabulary(session, referrer).get(relation_row.name)
        if relation is None:
            return False
        return relation.on_delete == "detach" or relation.optional

    def _vocabulary(self, session: Session, row: Component) -> dict[str, il.Relation]:
        """The relation vocabulary governing a component row.

        Args:
            session: Open session the parent row is read through.
            row: Component row whose vocabulary is resolved.

        Returns:
            The declared relations keyed by name, read from the owning
            source's declaration for a source-owned asset. Empty when nothing
            resolves.
        """
        return self._catalog.vocabulary(row.kind, row.key, parent_key=row.parent_key(session))

    def _relation(self, session: Session, src: Component, name: str) -> il.Relation:
        """The relation a row's class declares under *name*.

        Args:
            session: Open session the vocabulary is resolved through.
            src: Component row whose class declares the relation.
            name: Relation name to look up.

        Returns:
            The declared relation.

        Raises:
            ConfigError: If the row's class declares no relation of that name.
        """
        vocabulary = self._vocabulary(session, src)
        if name not in vocabulary:
            raise ConfigError(
                f"'{src.key}' ({src.kind}) declares no relation '{name}' (declared: {sorted(vocabulary)})"
            )
        return vocabulary[name]

    def _identity(self, session: Session, row: Component) -> il.ComponentIdentity:
        """What a component row is, for relation matching.

        Args:
            session: Open session the parent row is read through.
            row: Component row to identify.

        Returns:
            The row's identity: its owning source's key (None when it has no
            parent) and its own key.
        """
        return il.ComponentIdentity(row.parent_key(session), row.key)

    def _resolve(self, session: Session, src: Component, relation: il.Relation, name: str, dst_id: UUID) -> Component:
        """Load a relation destination and check the declared relation accepts it.

        Two rules the identity match cannot see are checked here. A component
        never fills its own relation, whatever the declared keys match. And a
        bare declared key names a component of the owner's *own* source
        instance, while identities carry only the catalog key of the owning
        source: two instances of one source have the same identity, so the
        instance is compared by parent row.

        Args:
            session: Open session the destination row is loaded through.
            src: Source component the relation originates from.
            relation: Declared relation the destination must satisfy.
            name: Relation name, used for error messages.
            dst_id: Destination component to resolve.

        Returns:
            The destination row, accepted by the relation.

        Raises:
            NotFoundError: If the destination is missing or belongs to another
                organisation.
            ConfigError: If the destination is the source itself, if the
                relation does not accept the destination's kind or identity,
                or if a sibling relation is pointed at another source
                instance's component.
        """
        dst = session.get(Component, dst_id)
        if dst is None or dst.org_id != src.org_id:
            raise NotFoundError(f"Component {dst_id} not found (relation '{name}')")
        if dst.id == src.id:
            raise ConfigError(f"'{src.key}'.{name} cannot point at the component itself")
        if not relation.accepts(dst.kind, self._identity(session, dst), owner=self._identity(session, src)):
            raise ConfigError(
                f"'{src.key}'.{name} does not accept {dst.kind} '{dst.key}' "
                f"(declared: kind {relation.kinds()}, key {relation.keys() or 'any'})"
            )
        if relation.source_local and src.parent_id is not None and dst.parent_id != src.parent_id:
            raise ConfigError(f"'{src.key}'.{name} names a sibling; '{dst.key}' belongs to another source instance")
        return dst

    def _lock(self, session: Session, component_id: UUID) -> Component:
        """Load the source row of a write, holding it for the transaction.

        The row lock serializes concurrent writes to the same source, so two
        rebinds of one single-valued name cannot both read an empty edge set
        and both insert. Every write path goes through here, which is the
        only guarantee available to a reader: SQLite, what the tests run on,
        has no row locks and needs none, since its writes are serialized
        database-wide, so no test can observe the lock being taken.

        Args:
            session: Open session the row is loaded through.
            component_id: Source component the write originates from.

        Returns:
            The source row, locked on backends that support it.

        Raises:
            NotFoundError: If the component does not exist.
        """
        statement = select(Component).where(Component.id == component_id)
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            statement = statement.with_for_update()
        src = session.exec(statement).first()
        if src is None:
            raise NotFoundError(f"Component {component_id} not found")
        return src

    @staticmethod
    def _rows(session: Session, src_id: UUID, name: str) -> list[ComponentRelation]:
        """The edges a component currently holds under one relation name.

        Args:
            session: Open session the rows are read through.
            src_id: Source component the edges originate from.
            name: Relation name the edges are filed under.

        Returns:
            The matching edge rows, in no guaranteed order.
        """
        statement = select(ComponentRelation).where(ComponentRelation.src_id == src_id, ComponentRelation.name == name)
        return list(session.exec(statement).all())

    @staticmethod
    def _insert(session: Session, src: Component, dst: Component, name: str) -> ComponentRelation:
        """Add one edge, stamping the denormalized org/kind triple from the rows.

        Args:
            session: Open session the row is added to; not flushed or committed.
            src: Source component the relation originates from.
            dst: Destination component the relation points at.
            name: Relation name to file the edge under.

        Returns:
            The pending edge (added to the session, not flushed).
        """
        row = ComponentRelation(
            src_id=src.id,
            name=name,
            dst_id=dst.id,
            org_id=src.org_id,
            src_kind=src.kind,
            dst_kind=dst.kind,
        )
        session.add(row)
        return row
