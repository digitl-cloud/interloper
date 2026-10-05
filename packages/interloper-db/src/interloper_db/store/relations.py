"""Relation policy: validated reads and writes for the named edge table.

An edge is a name, and the class declaring that name is the contract. The
vocabulary resolves by the row's qualified key, since a source-owned asset's
declaration lives on the source that owns it, with the kind's anchor as the
drift fallback. Every write asks the declared
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

import builtins
from collections.abc import Mapping, Sequence
from uuid import UUID

import interloper as il
from interloper.catalog.base import Catalog
from interloper.errors import ConfigError, NotFoundError
from sqlalchemy import Engine
from sqlmodel import Session, col, select

from interloper_db.models import Component, ComponentRelation
from interloper_db.session import commit, session_scope
from interloper_db.store.page import Page, PageQuery

# One relation binding: the destination component id.
Binding = UUID


class RelationQuery(PageQuery):
    """Which of an organisation's relations a listing reads.

    Attributes:
        name: Keep relations filed under this name; ``None`` keeps every name.
        src_kind: Keep relations whose source is of this kind.
        dst_kind: Keep relations whose destination is of this kind.
        dst_id: Keep relations pointing at any of these components.
    """

    name: str | None = None
    src_kind: str | None = None
    dst_kind: str | None = None
    dst_id: builtins.list[UUID] | None = None


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

    def list(self, org_id: UUID, query: RelationQuery) -> Page[ComponentRelation]:
        """List an organisation's relations, optionally filtered.

        Args:
            org_id: Organisation whose relations are listed.
            query: Which relation name, source and destination kinds, and the
                window to read.

        Returns:
            The page of relation rows, in a stable order.
        """
        statement = (
            select(ComponentRelation)
            .where(ComponentRelation.org_id == org_id)
            .order_by(col(ComponentRelation.src_id), col(ComponentRelation.name), col(ComponentRelation.dst_id))
        )
        if query.name:
            statement = statement.where(ComponentRelation.name == query.name)
        if query.src_kind:
            statement = statement.where(ComponentRelation.src_kind == query.src_kind)
        if query.dst_kind:
            statement = statement.where(ComponentRelation.dst_kind == query.dst_kind)
        if query.dst_id:
            statement = statement.where(col(ComponentRelation.dst_id).in_(query.dst_id))
        with session_scope(self._engine) as session:
            return Page.read(session, statement, query)

    def add(self, component_id: UUID, *, name: str, dst_id: UUID) -> ComponentRelation:
        """Bind one component to another under a declared relation name.

        A ``many`` name accumulates; a single-valued one repoints, so
        rebinding it needs no :meth:`delete` first. Re-adding an edge that is
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
            src = self._lock(component_id)
            relation = self._relation(src, name)
            dst = self._resolve(src, relation, name, dst_id)
            existing = self._rows(src.id, name)
            if match := next((row for row in existing if row.dst_id == dst.id), None):
                return match
            if not relation.many:
                for row in existing:
                    session.delete(row)
                session.flush()
            row = self._insert(src, dst, name)
            commit(session)
            return row

    def delete(self, component_id: UUID, *, name: str, dst_id: UUID) -> None:
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
            src = self._lock(component_id)
            rows = self._rows(src.id, name)
            row = next((candidate for candidate in rows if candidate.dst_id == dst_id), None)
            if row is None:
                return
            relation = self._vocabulary(src).get(name)
            if len(rows) == 1 and relation is not None and not relation.optional:
                raise ConfigError(
                    f"'{src.key}'.{name} is non-optional and cannot be emptied; "
                    f"repoint it or remove the dependent component instead"
                )
            session.delete(row)
            commit(session)

    def bind_siblings(self, source_cls: type[il.Source], children_by_key: dict[str, Component]) -> None:
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

        Part of the caller's transaction: the edges are added, neither flushed
        nor committed here.

        Args:
            source_cls: Catalog class whose declaration decides the wiring.
            children_by_key: The source's enabled child rows, keyed by asset key.
        """
        statement = select(ComponentRelation.src_id, ComponentRelation.name).where(
            col(ComponentRelation.src_id).in_([child.id for child in children_by_key.values()])
        )
        with session_scope(self._engine) as session:
            bound = {(row[0], row[1]) for row in session.exec(statement).all()}
        for asset_key, names in source_cls.sibling_bindings().items():
            child = children_by_key.get(asset_key)
            if child is None:
                continue
            for name, sibling_key in names.items():
                sibling = children_by_key.get(sibling_key)
                if sibling is None or (child.id, name) in bound:
                    continue
                self._insert(child, sibling, name)

    def sync(self, src: Component, bindings: Mapping[str, Sequence[Binding]] | None) -> None:
        """Replace the relation names present in *bindings* (empty list clears).

        Part of the caller's transaction. Takes the source row's lock, like every other write path, so two
        concurrent replacements of one name cannot interleave their deletes
        and inserts. An undeclared name and a destination the declared
        relation refuses both propagate from the checks this delegates to
        (:meth:`_relation`, :meth:`_resolve`).

        Args:
            src: Source component whose relations are replaced.
            bindings: Destination ids to install, keyed by relation name. Only
                the names present are touched, each replaced wholesale.
                None leaves every relation untouched.
        """
        if not bindings:
            return
        src = self._lock(src.id)
        with session_scope(self._engine) as session:
            self._replace(session, src, bindings)

    def _replace(self, session: Session, src: Component, bindings: Mapping[str, Sequence[Binding]]) -> None:
        """Replace each named relation's edges, in the session :meth:`sync` holds the row's lock in.

        Args:
            session: The open session.
            src: The locked source row.
            bindings: Destination ids to install, keyed by relation name.

        Raises:
            ConfigError: If a single-valued name is given several
                destinations, or if the replacement would empty a
                non-optional relation.
        """
        for name, dst_ids in bindings.items():
            relation = self._relation(src, name)
            if not relation.many and len(dst_ids) > 1:
                raise ConfigError(f"'{src.key}'.{name} is single-valued and takes one target at a time")
            existing = self._rows(src.id, name)
            # Gated on existing rows so the message names what happened: an
            # update that clears a bound name is a removal; a name left
            # unbound is caught by `_require_bound` once every name is synced.
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
                self._insert(src, self._resolve(src, relation, name, dst_id), name)

    def check_bound(self, src: Component) -> None:
        """Refuse a row that leaves a relation its class declares non-optional unbound.

        The class declaration is what the row's code runs against: a Slack
        hook without its connection, a source without its connection, would
        only fail where it fires or runs. Checked after every save, so a row
        that predates the guard is caught on its next save too.

        Args:
            src: Component row whose relations are checked.

        Raises:
            ConfigError: If a non-optional relation has no row.
        """
        for name, relation in self._vocabulary(src).items():
            if not relation.optional and not self._rows(src.id, name):
                raise ConfigError(f"'{src.key}'.{name} is required: bind a {' or '.join(relation.kinds)}")

    def detaches(self, referrer: Component, relation_row: ComponentRelation) -> bool:
        """Whether an edge detaches (rather than blocks) when its destination is deleted.

        Consults the referrer's own vocabulary, where ``on_delete`` is the
        sole authority: only a name declared ``on_delete="detach"`` detaches.
        Whether the name is ``optional`` is a separate question, about leaving
        it unbound or emptying it, and does not license deleting a target the
        referrer holds. Anything unresolvable (an undeclared name, a drifted
        key) blocks, keeping the guard fail-closed.

        Args:
            referrer: Referrer row holding the edge.
            relation_row: Edge whose destination is about to be deleted.

        Returns:
            True if the edge may be detached, False if it blocks the deletion.
        """
        relation = self._vocabulary(referrer).get(relation_row.name)
        if relation is None:
            return False
        return relation.on_delete == "detach"

    # -- Internals -------------------------------------------------------------

    def _vocabulary(self, row: Component) -> dict[str, il.Relation]:
        """The relation vocabulary governing a component row.

        Args:
            row: Component row whose vocabulary is resolved.

        Returns:
            The declared relations keyed by name, read from the owning
            source's declaration for a source-owned asset. Empty when nothing
            resolves.
        """
        return self._catalog.vocabulary(row.kind, row.qualified_key)

    def _relation(self, src: Component, name: str) -> il.Relation:
        """The relation a row's class declares under *name*.

        Args:
            src: Component row whose class declares the relation.
            name: Relation name to look up.

        Returns:
            The declared relation.

        Raises:
            ConfigError: If the row's class declares no relation of that name.
        """
        vocabulary = self._vocabulary(src)
        if name not in vocabulary:
            raise ConfigError(
                f"'{src.key}' ({src.kind}) declares no relation '{name}' (declared: {sorted(vocabulary)})"
            )
        return vocabulary[name]

    def _resolve(self, src: Component, relation: il.Relation, name: str, dst_id: UUID) -> Component:
        """Load a relation destination and check the declared relation accepts it.

        Two rules the identity match cannot see are checked here. A component
        never fills its own relation, whatever the declared keys match. And a
        bare declared key names a component of the owner's *own* source
        instance, while identities carry only the catalog key of the owning
        source: two instances of one source have the same identity, so the
        instance is compared by parent row.

        Args:
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
        with session_scope(self._engine) as session:
            dst = session.get(Component, dst_id)
        if dst is None or dst.org_id != src.org_id:
            raise NotFoundError(f"Component {dst_id} not found (relation '{name}')")
        if dst.id == src.id:
            raise ConfigError(f"'{src.key}'.{name} cannot point at the component itself")
        if not relation.accepts(dst.kind, dst.identity, owner=src.identity):
            raise ConfigError(
                f"'{src.key}'.{name} does not accept {dst.kind} '{dst.key}' "
                f"(declared: kind {relation.kinds}, key {relation.keys or 'any'})"
            )
        if relation.local and src.parent_id is not None and dst.parent_id != src.parent_id:
            raise ConfigError(f"'{src.key}'.{name} names a sibling; '{dst.key}' belongs to another source instance")
        return dst

    def _lock(self, component_id: UUID) -> Component:
        """Load the source row of a write, holding it for the transaction.

        The row lock serializes concurrent writes to the same source, so two
        rebinds of one single-valued name cannot both read an empty edge set
        and both insert. Every write path goes through here.

        Args:
            component_id: Source component the write originates from.

        Returns:
            The source row, locked.

        Raises:
            NotFoundError: If the component does not exist.
        """
        with session_scope(self._engine) as session:
            src = session.exec(select(Component).where(Component.id == component_id).with_for_update()).first()
            if src is None:
                raise NotFoundError(f"Component {component_id} not found")
            return src

    def _rows(self, src_id: UUID, name: str) -> builtins.list[ComponentRelation]:
        """The edges a component currently holds under one relation name.

        Args:
            src_id: Source component the edges originate from.
            name: Relation name the edges are filed under.

        Returns:
            The matching edge rows, in no guaranteed order.
        """
        statement = select(ComponentRelation).where(ComponentRelation.src_id == src_id, ComponentRelation.name == name)
        with session_scope(self._engine) as session:
            return list(session.exec(statement).all())

    def _insert(self, src: Component, dst: Component, name: str) -> ComponentRelation:
        """Add one edge, stamping the denormalized org/kind triple from the rows.

        Args:
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
        with session_scope(self._engine) as session:
            session.add(row)
        return row
