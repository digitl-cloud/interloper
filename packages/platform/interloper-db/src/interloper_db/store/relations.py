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
emptied, only repointed, except an asset's upstream relation: a row is a
curation, and an upstream relation holding none follows every asset of the
organisation it matches (:meth:`RelationStore.automatic`), never written.
Every write takes the source row's lock, so the guards cannot be read around
concurrently. Rows are stamped with the
denormalized ``org_id``/``src_kind``/``dst_kind`` triple the composite
foreign keys verify.
"""

from __future__ import annotations

import builtins
from collections.abc import Mapping, Sequence
from uuid import UUID

import interloper as il
from interloper.catalog import Catalog
from interloper.errors import ConfigError
from sqlalchemy import Engine
from sqlalchemy import select as sa_select
from sqlalchemy.orm import aliased
from sqlmodel import Session, col, select

from interloper_db.errors import NotFoundError, ReadOnlyError
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

    def __init__(self, engine: Engine, catalog: Catalog, *, served_from: str | None = None) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
            catalog: Catalog its relation vocabulary resolves against.
            served_from: The files the collection is served from, which
                refuses every relation the app asks to add or remove; ``None``
                for a collection the app edits.
        """
        self._engine = engine
        self._catalog = catalog
        self._served_from = served_from

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
        if query.src_kind not in {None, "asset"} or query.dst_kind not in {None, "asset"}:
            with session_scope(self._engine) as session:
                return Page.read(session, statement, query)
        with session_scope(self._engine) as session:
            rows = builtins.list(session.exec(statement).all())
        automatic = [
            edge
            for edge in self.automatic(org_id)
            if (not query.name or edge.name == query.name) and (not query.dst_id or edge.dst_id in query.dst_id)
        ]
        merged = sorted([*rows, *automatic], key=lambda edge: (edge.src_id, edge.name, edge.dst_id))
        return Page.window(merged, query)

    def automatic(self, org_id: UUID) -> builtins.list[ComponentRelation]:
        """The edges an organisation's unbound upstream relations follow, never written.

        For every asset and every upstream relation it holds no row under, one
        unsaved row per asset filling it: a sibling the source's declaration
        names (`SourceDefinition.sibling_bindings`), else every asset of the
        organisation the relation matches (`Relation.matches`). Candidates are
        the asset rows whose catalog key resolves, enabled or not: a drifted
        asset cannot be loaded, and one stale row must not fail every reader of
        a wildcard.

        Args:
            org_id: Organisation whose edges are resolved.

        Returns:
            The unsaved edges, ordered by source, name and destination.
        """
        parent = aliased(Component)
        assets_statement = (
            sa_select(col(Component.id), col(Component.key), col(Component.parent_id), col(parent.key))
            .outerjoin(parent, col(parent.id) == col(Component.parent_id))
            .where(col(Component.org_id) == org_id, col(Component.kind) == "asset")
        )
        curated_statement = (
            sa_select(col(ComponentRelation.src_id), col(ComponentRelation.name))
            .where(col(ComponentRelation.org_id) == org_id, col(ComponentRelation.src_kind) == "asset")
            .distinct()
        )
        with session_scope(self._engine) as session:
            rows = session.connection().execute(assets_statement).all()
            curated = {(src_id, name) for src_id, name in session.connection().execute(curated_statement).all()}
        definitions: dict[UUID, il.AssetDefinition] = {}
        identities: dict[UUID, il.ComponentIdentity] = {}
        for asset_id, key, _, source_key in rows:
            definition = self._catalog.get(f"{source_key}.{key}" if source_key else key)
            if isinstance(definition, il.AssetDefinition):
                definitions[asset_id] = definition
                identities[asset_id] = il.ComponentIdentity(source_key, key)
        parents = {asset_id: parent_id for asset_id, _, parent_id, _ in rows if asset_id in identities}
        by_parent: dict[UUID | None, dict[str, UUID]] = {}
        by_key: dict[str, dict[UUID, il.ComponentIdentity]] = {}
        for asset_id, parent_id in parents.items():
            by_parent.setdefault(parent_id, {})[identities[asset_id].key] = asset_id
            by_key.setdefault(identities[asset_id].key, {})[asset_id] = identities[asset_id]
        wiring: dict[str, dict[str, dict[str, str]]] = {}
        edges: builtins.list[ComponentRelation] = []
        for asset_id, definition in definitions.items():
            identity = identities[asset_id]
            siblings = by_parent[parents[asset_id]]
            source_key = identity.source_key
            if source_key is not None and source_key not in wiring:
                source = self._catalog.get(source_key)
                wiring[source_key] = source.sibling_bindings() if isinstance(source, il.SourceDefinition) else {}
            wired = wiring.get(source_key, {}).get(identity.key, {}) if source_key else {}
            for name, relation in definition.relations.items():
                if "asset" not in relation.kinds or (asset_id, name) in curated:
                    continue
                if name in wired:
                    matched = [siblings[wired[name]]] if wired[name] in siblings else []
                else:
                    # A declared key names one asset key whatever its source part, so only those can match.
                    pool = {
                        candidate_id: candidate
                        for declared in relation.keys
                        for candidate_id, candidate in by_key.get(declared.rpartition(".")[2], {}).items()
                        if candidate_id != asset_id
                    }
                    matched = relation.matches(identity, pool, siblings=set(siblings.values()))
                edges.extend(
                    ComponentRelation(
                        src_id=asset_id, name=name, dst_id=dst_id, org_id=org_id, src_kind="asset", dst_kind="asset"
                    )
                    for dst_id in matched
                )
        return sorted(edges, key=lambda edge: (edge.src_id, edge.name, edge.dst_id))

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
            self._refuse_served()
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
            self._refuse_served()
            src = self._lock(component_id)
            rows = self._rows(src.id, name)
            row = next((candidate for candidate in rows if candidate.dst_id == dst_id), None)
            if row is None:
                return
            relation = self._vocabulary(src).get(name)
            if len(rows) == 1 and relation is not None and not relation.optional and not self._follows(src, relation):
                raise ConfigError(
                    f"'{src.key}'.{name} is non-optional and cannot be emptied; "
                    f"repoint it or remove the dependent component instead"
                )
            session.delete(row)
            commit(session)

    def clear(self, src: Component, names: Sequence[str]) -> None:
        """Remove every edge filed under these names, required or not, whatever the collection's mode.

        Part of the caller's transaction. It bypasses the read-only refusal
        and the guard on a required relation, so it serves only the writer of
        a collection served from files, which binds every relation exactly as
        the file says.

        Args:
            src: Source component whose edges are removed.
            names: The relation names to empty.
        """
        with session_scope(self._engine) as session:
            for name in names:
                for row in self._rows(src.id, name):
                    session.delete(row)
            session.flush()

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
            if not relation.optional and not self._follows(src, relation) and not self._rows(src.id, name):
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

    def _refuse_served(self) -> None:
        """Refuse a relation change the app asks of a collection its files serve.

        Raises:
            ReadOnlyError: If the collection is served from files.
        """
        if self._served_from is not None:
            raise ReadOnlyError(f"The collection is served from {self._served_from}: change it there")

    @staticmethod
    def _follows(src: Component, relation: il.Relation) -> bool:
        """Whether a relation with no row is filled by :meth:`automatic`, so emptying it is no gap.

        Args:
            src: Component row declaring the relation.
            relation: The declared relation.

        Returns:
            True for an asset's upstream relation declaring the keys it matches.
        """
        return src.kind == "asset" and "asset" in relation.kinds and bool(relation.keys)

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
