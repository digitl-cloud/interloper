"""Relation policy: validated reads and writes for the named edge table.

An edge is a name, and the class declaring that name is the contract. The
vocabulary resolves parent-aware, since a source-owned asset's declaration
lives on the source that owns it, with the kind's anchor as the drift
fallback. Every write asks the declared
:class:`~interloper.component.relation.Relation` whether it accepts the
candidate, so the database enforces exactly what the framework enforces in
memory: the declared kinds, and the identity the declared keys expect
(bare, qualified or wildcard). Single-valued names hold one edge and
repoint on rewrite; ``many`` names accumulate. A non-optional name cannot
be emptied, only repointed. Rows are stamped with the denormalized
``org_id``/``src_kind``/``dst_kind`` triple the composite foreign keys
verify.
"""

from __future__ import annotations

from uuid import UUID

import interloper as il
from interloper.catalog.base import Catalog
from interloper.errors import ConfigError, NotFoundError
from sqlalchemy import Engine
from sqlmodel import Session, select

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
            rows = self._rows(session, component_id, name)
            row = next((candidate for candidate in rows if candidate.dst_id == dst_id), None)
            if row is None:
                return
            src = session.get(Component, component_id)
            relation = self._vocabulary(session, src).get(name) if src is not None else None
            if len(rows) == 1 and relation is not None and not relation.optional:
                raise ConfigError(
                    f"'{src.key if src else component_id}'.{name} is non-optional and cannot be emptied; "
                    f"repoint it or remove the dependent component instead"
                )
            session.delete(row)
            commit(session)

    # -- Internals -------------------------------------------------------------

    def _sync_relations(self, session: Session, src: Component, bindings: dict[str, list[Binding]] | None) -> None:
        """Replace the relation names present in *bindings* (empty list clears).

        Args:
            session: Open session the replacement is written through; deletes
                and inserts are flushed, never committed here.
            src: Source component whose relations are replaced.
            bindings: Destination ids to install, keyed by relation name. Only
                the names present are touched, each replaced wholesale.
                None leaves every relation untouched.

        Raises:
            ConfigError: If a name is not declared by the source's class, if a
                single-valued name is given several destinations, if the
                relation does not accept one of them, or if the replacement
                would empty a non-optional relation.
        """
        for name, dst_ids in (bindings or {}).items():
            relation = self._relation(session, src, name)
            if not relation.many and len(dst_ids) > 1:
                raise ConfigError(f"'{src.key}'.{name} is single-valued and takes one target at a time")
            existing = self._rows(session, src.id, name)
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
            ConfigError: If the relation does not accept the destination's kind
                or identity.
        """
        dst = session.get(Component, dst_id)
        if dst is None or dst.org_id != src.org_id:
            raise NotFoundError(f"Component {dst_id} not found (relation '{name}')")
        if not relation.accepts(dst.kind, self._identity(session, dst), owner=self._identity(session, src)):
            raise ConfigError(
                f"'{src.key}'.{name} does not accept {dst.kind} '{dst.key}' "
                f"(declared: kind {relation.kinds()}, key {relation.keys() or 'any'})"
            )
        return dst

    def _lock(self, session: Session, component_id: UUID) -> Component:
        """Load the source row of a write, holding it for the transaction.

        The row lock serializes concurrent writes to the same source, so two
        rebinds of one single-valued name cannot both read an empty edge set
        and both insert. SQLite has no row locks and needs none: its writes
        are serialized database-wide.

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
