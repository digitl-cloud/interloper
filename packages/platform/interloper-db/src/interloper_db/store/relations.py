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
organisation it matches, never written. What it follows is decided per pair
of identities off the catalog (:class:`UpstreamRules`) and expanded to rows
in SQL, inside the statement that reads the rows: a listing page stays one
windowed statement and its count, a load one statement per row.
Every write takes the source row's lock, so the guards cannot be read around
concurrently. Rows are stamped with the
denormalized ``org_id``/``src_kind``/``dst_kind`` triple the composite
foreign keys verify.
"""

from __future__ import annotations

import builtins
from collections.abc import Mapping, Sequence
from functools import cached_property
from typing import Any, NamedTuple
from uuid import UUID

import interloper as il
from interloper.catalog import Catalog
from interloper.errors import ConfigError
from pydantic import Field
from sqlalchemy import Engine, Select, String, and_, column, exists, false, func, literal, text, true, union_all
from sqlalchemy import select as sa_select
from sqlmodel import Session, col, select

from interloper_db.errors import NotFoundError, ReadOnlyError
from interloper_db.models import Component, ComponentRelation, table_of
from interloper_db.session import commit, session_scope
from interloper_db.store.page import MAX_PAGE_SIZE, Page, PageQuery

# One relation binding: the destination component id.
Binding = UUID


class RelationQuery(PageQuery):
    """Which of an organisation's relations a listing reads.

    Unlike other listings, leaving ``limit`` out reads the whole matching set,
    over HTTP too: followed edges are derived on each read, so every page
    costs the whole derivation, and a reader that wants them all reads them
    in one page rather than paying it once per page.

    Attributes:
        limit: How many relations to read; ``None`` reads them all.
        name: Keep relations filed under this name; ``None`` keeps every name.
        src_kind: Keep relations whose source is of this kind.
        dst_kind: Keep relations whose destination is of this kind.
        dst_id: Keep relations pointing at any of these components.
    """

    limit: int | None = Field(default=None, ge=1, le=MAX_PAGE_SIZE)
    name: str | None = None
    src_kind: str | None = None
    dst_kind: str | None = None
    dst_id: builtins.list[UUID] | None = None


class Edge(NamedTuple):
    """One relation in effect, as a listing reads it: a stored row, or an edge an unbound upstream relation follows.

    A followed edge has no row to load, and a listing reads thousands of
    either, so it reads columns and hands each back as this tuple.
    """

    src_id: UUID
    name: str
    dst_id: UUID
    org_id: UUID
    src_kind: str
    dst_kind: str


class UpstreamRule(NamedTuple):
    """One identity-level match: an asset of ``owner`` follows, under ``name``, the assets of ``upstream``.

    ``sibling`` keeps the owner's own source instance only: the relation is
    `local`, or the owner's source wires it to a sibling
    (`SourceDefinition.sibling_bindings`).
    """

    owner: il.ComponentIdentity
    name: str
    upstream: il.ComponentIdentity
    sibling: bool


class UpstreamRules:
    """What every asset declaration's upstream relations follow when they hold no row, read off a catalog once.

    `Relation.fills` decides from two identities alone, and the catalog holds
    every identity an organisation's rows can resolve to, so what fills a
    relation is known as rules over identities before any row is read. The
    relation store expands them to an organisation's rows in SQL. A row whose
    key drifted out of the catalog has no identity here, so it neither
    follows nor is followed.
    """

    def __init__(self, catalog: Catalog) -> None:
        """Read every asset declaration's rules off the catalog.

        Args:
            catalog: The catalog whose asset declarations, and the identities
                they can match, the rules are read from.
        """
        owners: list[tuple[il.ComponentIdentity, il.AssetDefinition, dict[str, str]]] = []
        for key, definition in catalog.components.items():
            if isinstance(definition, il.SourceDefinition):
                wiring = definition.sibling_bindings()
                owners.extend(
                    (il.ComponentIdentity(key, asset.key), asset, wiring.get(asset.key, {}))
                    for asset in definition.assets
                )
            elif isinstance(definition, il.AssetDefinition):
                owners.append((il.ComponentIdentity(None, key), definition, {}))
        by_key: dict[str, list[il.ComponentIdentity]] = {}
        for identity, _, _ in owners:
            by_key.setdefault(identity.key, []).append(identity)
        self._by_owner: dict[il.ComponentIdentity, list[UpstreamRule]] = {}
        for owner, definition, wired in owners:
            rules: list[UpstreamRule] = []
            for name, relation in definition.relations.items():
                if "asset" not in relation.kinds:
                    continue
                if name in wired:
                    sibling = il.ComponentIdentity(owner.source_key, wired[name])
                    rules.append(UpstreamRule(owner, name, sibling, sibling=True))
                    continue
                # A declared key names one asset key whatever its source part, so only those identities can fill it.
                candidates = dict.fromkeys(
                    identity for declared in relation.keys for identity in by_key.get(declared.rpartition(".")[2], [])
                )
                rules.extend(
                    UpstreamRule(owner, name, identity, sibling=relation.local)
                    for identity in candidates
                    if relation.fills(owner, identity)
                )
            if rules:
                self._by_owner[owner] = rules

    def of(self, owner: il.ComponentIdentity) -> list[UpstreamRule]:
        """The rules of one asset declaration.

        Args:
            owner: The declaring asset's identity.

        Returns:
            Its rules; empty when none of its relations follows anything.
        """
        return self._by_owner.get(owner, [])

    def named(self, name: str | None) -> list[UpstreamRule]:
        """Every declaration's rules, or only those filed under one relation name.

        Args:
            name: The relation name to keep; ``None`` keeps every rule.

        Returns:
            The rules.
        """
        return [rule for rules in self._by_owner.values() for rule in rules if name is None or rule.name == name]


class FollowedEdges:
    """The edges unbound upstream relations follow in one organisation, as selections over its rows.

    One lives for one statement. Across the organisation (:meth:`of`), each
    selection reads ``(src_id, name, dst_id, org_id)`` in two steps: the
    owners, the assets of a rule's owner identity holding no row under its
    name, and from each owner every other asset of the organisation of the
    rule's upstream identity. Owners are few next to what they follow (a star
    schema's views over every account), so they are resolved first and once.
    For one asset (:meth:`of_one`) the asset and its rules are already known,
    so each rule is one indexed lookup. A sibling rule follows the assets
    sharing the owner's parent; parentless assets are siblings of one
    another, as in a DAG.
    """

    def __init__(self, org_id: UUID, *, dst_ids: Sequence[UUID] | None = None) -> None:
        """Scope the selections, building none yet.

        Args:
            org_id: Organisation whose rows the rules expand to.
            dst_ids: Keep only edges pointing at these assets; ``None`` keeps all.
        """
        self._org_id = org_id
        self._dst_ids = dst_ids

    def of(self, rules: Sequence[UpstreamRule]) -> list[Select[Any]]:
        """The selections expanding *rules*, one per way a rule reaches its rows.

        Args:
            rules: The rules to expand.

        Returns:
            The selections to union; none when no rule applies.
        """
        across = [rule for rule in rules if not rule.sibling and rule.upstream.source_key is not None]
        parentless = [rule for rule in rules if not rule.sibling and rule.upstream.source_key is None]
        owned = [rule for rule in rules if rule.sibling and rule.owner.source_key is not None]
        loose = [rule for rule in rules if rule.sibling and rule.owner.source_key is None]
        return [
            *([self._across(across)] if across else []),
            *([self._parentless(parentless)] if parentless else []),
            *([self._owned(owned)] if owned else []),
            *([self._loose(loose)] if loose else []),
        ]

    def of_one(self, src: Component, rules: Sequence[UpstreamRule]) -> list[Select[Any]]:
        """The selections a load of one asset needs, each reading ``(name, dst_id)``.

        An owned asset's sibling rules need none: its source binds them as it
        is built (`SourceDefinition.sibling_bindings`), from the instances it
        already holds.

        Args:
            src: The asset row the rules belong to.
            rules: Its rules.

        Returns:
            One selection per rule left; a rule its asset curates selects nothing.
        """
        selections: list[Select[Any]] = []
        for rule in rules:
            if rule.sibling and src.parent_id is not None:
                continue
            dst = table_of(Component).alias()
            statement = (
                sa_select(literal(rule.name).label("name"), dst.c.id.label("dst_id"))
                .select_from(dst)
                .where(
                    dst.c.key == rule.upstream.key,
                    dst.c.id != src.id,
                    ~exists().where(col(ComponentRelation.src_id) == src.id, col(ComponentRelation.name) == rule.name),
                )
            )
            if rule.sibling or rule.upstream.source_key is None:
                statement = statement.where(
                    dst.c.org_id == self._org_id, dst.c.kind == "asset", dst.c.parent_id.is_(None)
                )
            else:
                parent = table_of(Component).alias()
                statement = statement.join(parent, parent.c.id == dst.c.parent_id).where(
                    parent.c.org_id == self._org_id, parent.c.key == rule.upstream.source_key
                )
            selections.append(statement)
        return selections

    # -- Internals -------------------------------------------------------------

    def _across(self, rules: Sequence[UpstreamRule]) -> Select[Any]:
        """Edges reaching every asset a source of the upstream identity's key owns.

        A hash join of two sets resolved once each, the owners and every asset
        an upstream identity names: an asset key such as ``campaigns_stats``
        is shared by many platforms, so the candidates are narrowed by their
        source's key as well.

        Args:
            rules: Rules that are not sibling rules, whose upstream an asset of
                a source fills.

        Returns:
            The selection.
        """
        owners = self._anywhere("across", rules)
        dst = table_of(Component).alias()
        dst_parent = table_of(Component).alias()
        candidates = (
            sa_select(dst.c.id.label("id"), dst_parent.c.key.label("source"), dst.c.key.label("key"))
            .select_from(dst)
            .join(dst_parent, dst_parent.c.id == dst.c.parent_id)
            .where(
                dst.c.org_id == self._org_id,
                dst.c.kind == "asset",
                dst.c.key.in_(sorted({rule.upstream.key for rule in rules})),
                dst_parent.c.key.in_(sorted({rule.upstream.source_key or "" for rule in rules})),
            )
            .cte("across_candidates")
            .prefix_with("MATERIALIZED")
        )
        return self._edges(owners, candidates.c.id).join(
            candidates,
            and_(
                candidates.c.source == owners.c.upstream_source,
                candidates.c.key == owners.c.upstream_key,
                candidates.c.id != owners.c.src_id,
            ),
        )

    def _parentless(self, rules: Sequence[UpstreamRule]) -> Select[Any]:
        """Edges reaching every parentless asset of the upstream identity's key.

        Args:
            rules: Rules that are not sibling rules, whose upstream a parentless
                asset fills.

        Returns:
            The selection.
        """
        owners = self._anywhere("parentless", rules)
        dst = table_of(Component).alias()
        return self._edges(owners, dst.c.id).join(
            dst,
            and_(
                dst.c.org_id == owners.c.org_id,
                dst.c.kind == "asset",
                dst.c.parent_id.is_(None),
                dst.c.key == owners.c.upstream_key,
                dst.c.id != owners.c.src_id,
            ),
        )

    def _anywhere(self, name: str, rules: Sequence[UpstreamRule]) -> Any:
        """The owners of rules reaching outside their own source instance, resolved once.

        Args:
            name: The rules table's name, unique within the statement.
            rules: The rules, none of them sibling rules.

        Returns:
            The owners, as a materialized common table expression.
        """
        table = self._table(name, rules, sources=True)
        src = table_of(Component).alias()
        src_parent = table_of(Component).alias()
        return (
            self._owners(src, table, rules)
            .outerjoin(src_parent, src_parent.c.id == src.c.parent_id)
            .where(func.coalesce(src_parent.c.key, "") == table.c.owner_source)
            .cte(f"{name}_owners")
            .prefix_with("MATERIALIZED")
        )

    def _owned(self, rules: Sequence[UpstreamRule]) -> Select[Any]:
        """Edges reaching an owned asset's siblings, through the parent they share.

        Args:
            rules: Sibling rules of owned assets.

        Returns:
            The selection.
        """
        table = self._table("owned", rules, sources=True)
        src = table_of(Component).alias()
        src_parent = table_of(Component).alias()
        owners = (
            self._owners(src, table, rules)
            .join(src_parent, src_parent.c.id == src.c.parent_id)
            .where(src_parent.c.key == table.c.owner_source)
            .cte("owned_owners")
            .prefix_with("MATERIALIZED")
        )
        dst = table_of(Component).alias()
        return self._edges(owners, dst.c.id).join(
            dst,
            and_(
                dst.c.parent_id == owners.c.parent_id,
                dst.c.key == owners.c.upstream_key,
                dst.c.id != owners.c.src_id,
            ),
        )

    def _loose(self, rules: Sequence[UpstreamRule]) -> Select[Any]:
        """Edges reaching a parentless asset's siblings, the organisation's other parentless assets.

        Args:
            rules: Sibling rules of parentless assets.

        Returns:
            The selection.
        """
        table = self._table("loose", rules, sources=False)
        src = table_of(Component).alias()
        owners = (
            self._owners(src, table, rules)
            .where(src.c.parent_id.is_(None))
            .cte("loose_owners")
            .prefix_with("MATERIALIZED")
        )
        dst = table_of(Component).alias()
        return self._edges(owners, dst.c.id).join(
            dst,
            and_(
                dst.c.org_id == owners.c.org_id,
                dst.c.kind == "asset",
                dst.c.parent_id.is_(None),
                dst.c.key == owners.c.upstream_key,
                dst.c.id != owners.c.src_id,
            ),
        )

    def _owners(self, src: Any, table: Any, rules: Sequence[UpstreamRule]) -> Select[Any]:
        """The organisation's assets a rule's owner key names, each holding no row under the rule's name.

        The caller narrows them to the rule's owner source.

        Args:
            src: The alias the owners are read through.
            table: The rules table.
            rules: The rules the table holds, whose owner keys narrow the read.

        Returns:
            The selection of ``(src_id, org_id, parent_id, name, upstream_key)``
            and, when the table carries them, ``upstream_source``.
        """
        statement = (
            sa_select(
                src.c.id.label("src_id"),
                src.c.org_id.label("org_id"),
                src.c.parent_id.label("parent_id"),
                *(carried for carried in table.c if carried.key in ("name", "upstream_source", "upstream_key")),
            )
            .select_from(src)
            .join(table, table.c.owner_key == src.c.key)
            .where(
                src.c.org_id == self._org_id,
                src.c.kind == "asset",
                src.c.key.in_(sorted({rule.owner.key for rule in rules})),
                ~exists().where(col(ComponentRelation.src_id) == src.c.id, col(ComponentRelation.name) == table.c.name),
            )
        )
        return statement

    def _edges(self, owners: Any, followed: Any) -> Select[Any]:
        """The columns every selection reads, from its owners to the followed asset its caller joins.

        Args:
            owners: The owners each edge starts from.
            followed: The followed asset's id column, from what the caller joins.

        Returns:
            The selection, before its caller's joins.
        """
        statement = sa_select(owners.c.src_id, owners.c.name, followed.label("dst_id"), owners.c.org_id).select_from(
            owners
        )
        if self._dst_ids:
            statement = statement.where(followed.in_(self._dst_ids))
        return statement

    @staticmethod
    def _table(name: str, rules: Sequence[UpstreamRule], *, sources: bool) -> Any:
        """The rules as a table the selection joins, written out as literals.

        Literal rows rather than bound ``VALUES``: the rules are the catalog's
        and fixed per process, and a literal table keeps the statement in
        SQLAlchemy's compiled cache, which a ``VALUES`` construct opts out of.

        Args:
            name: The table's name, unique within the statement.
            rules: The rules it holds.
            sources: Whether it carries the owner's and the upstream's source
                keys, ``""`` standing for none; a sibling rule of a parentless
                asset needs neither.

        Returns:
            The table, as a common table expression.
        """
        if sources:
            labels = ("owner_source", "owner_key", "name", "upstream_source", "upstream_key")
            rows = [
                (
                    rule.owner.source_key or "",
                    rule.owner.key,
                    rule.name,
                    rule.upstream.source_key or "",
                    rule.upstream.key,
                )
                for rule in rules
            ]
        else:
            labels = ("owner_key", "name", "upstream_key")
            rows = [(rule.owner.key, rule.name, rule.upstream.key) for rule in rules]
        quoted = ", ".join("(" + ", ".join("'" + value.replace("'", "''") + "'" for value in row) + ")" for row in rows)
        selected = ", ".join(f"rule.column{index} AS {label}" for index, label in enumerate(labels, start=1))
        body = text(f"SELECT {selected} FROM (VALUES {quoted}) AS rule")
        return body.columns(*(column(label, String) for label in labels)).cte(name)


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

    def list(self, org_id: UUID, query: RelationQuery) -> Page[Edge]:
        """List an organisation's relations, optionally filtered.

        Stored rows and the edges unbound upstream relations follow are one
        ordered selection, so a page is a windowed statement and its count
        whatever the organisation's size.

        Args:
            org_id: Organisation whose relations are listed.
            query: Which relation name, source and destination kinds, and the
                window to read.

        Returns:
            The page of edges, in a stable order.
        """
        filters: builtins.list[Any] = [ComponentRelation.org_id == org_id]
        if query.name:
            filters.append(ComponentRelation.name == query.name)
        if query.src_kind:
            filters.append(ComponentRelation.src_kind == query.src_kind)
        if query.dst_kind:
            filters.append(ComponentRelation.dst_kind == query.dst_kind)
        if query.dst_id:
            filters.append(col(ComponentRelation.dst_id).in_(query.dst_id))
        stored = sa_select(
            col(ComponentRelation.src_id),
            col(ComponentRelation.name),
            col(ComponentRelation.dst_id),
            col(ComponentRelation.org_id),
            col(ComponentRelation.src_kind),
            col(ComponentRelation.dst_kind),
        ).where(*filters)
        follows = query.src_kind in {None, "asset"} and query.dst_kind in {None, "asset"}
        rules = self._upstreams.named(query.name) if follows else []
        followed = [
            branch.add_columns(literal("asset").label("src_kind"), literal("asset").label("dst_kind"))
            for branch in FollowedEdges(org_id, dst_ids=query.dst_id).of(rules)
        ]
        edges = union_all(stored, *followed).subquery("edges") if followed else stored.subquery("edges")
        statement = select(*edges.c).order_by(edges.c.src_id, edges.c.name, edges.c.dst_id)
        with session_scope(self._engine) as session:
            return Page.read(session, statement, query).map(Edge._make)

    def targets(self, src: Component, *, follow: bool) -> tuple[dict[str, builtins.list[Component]], set[str]]:
        """The rows a component's relations point at, by name, in one statement.

        The component's own rows and, when *follow* holds, the assets each of
        its upstream relations holding none follows, but for an owned asset's
        siblings, which its source binds (:meth:`FollowedEdges.of_one`).

        Args:
            src: The component row; an owned asset's parent must be loaded or
                loadable, since its identity picks the rules.
            follow: Whether unbound upstream relations follow what they match;
                hydration passes it for an enabled asset only.

        Returns:
            The target rows by relation name, ordered by ``(name, dst_id)``,
            and the names among them filled by following rather than by rows.
        """
        lookups: builtins.list[Select[Any]] = []
        if follow:
            source_key, _, key = src.qualified_key.rpartition(".")
            rules = self._upstreams.of(il.ComponentIdentity(source_key or None, key))
            lookups = FollowedEdges(src.org_id).of_one(src, rules)
        if lookups:
            stored = sa_select(
                col(ComponentRelation.name).label("name"),
                col(ComponentRelation.dst_id).label("dst_id"),
                false().label("followed"),
            ).where(col(ComponentRelation.src_id) == src.id)
            branches: builtins.list[Select[Any]] = [stored]
            branches.extend(lookup.add_columns(true().label("followed")) for lookup in lookups)
            edges = union_all(*branches).subquery("edges")
            statement = (
                select(edges.c.name, edges.c.followed, Component)
                .join(Component, col(Component.id) == edges.c.dst_id)
                .order_by(edges.c.name, edges.c.dst_id)
            )
        else:
            statement = (
                select(ComponentRelation.name, false(), Component)
                .join(Component, col(Component.id) == col(ComponentRelation.dst_id))
                .where(ComponentRelation.src_id == src.id)
                .order_by(col(ComponentRelation.name), col(ComponentRelation.dst_id))
            )
        grouped: dict[str, builtins.list[Component]] = {}
        followed: set[str] = set()
        with session_scope(self._engine) as session:
            for name, is_followed, target in session.exec(statement).all():
                grouped.setdefault(name, []).append(target)
                if is_followed:
                    followed.add(name)
        return grouped, followed

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

    @cached_property
    def _upstreams(self) -> UpstreamRules:
        """The catalog's upstream rules, read off it the first time a read follows a relation.

        Returns:
            The rules of this store's catalog.
        """
        return UpstreamRules(self._catalog)

    @staticmethod
    def _follows(src: Component, relation: il.Relation) -> bool:
        """Whether a relation with no row follows what it matches, so emptying it is no gap.

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
