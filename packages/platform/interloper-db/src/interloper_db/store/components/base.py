"""Component rows: their CRUD and what one row reads as in this deployment.

CRUD and relations are kind-agnostic; the semantics a kind genuinely owns
are applied where the row's ``kind`` demands them:

- **secret kinds** (connection/config/resource): the ``config`` payload is
  encrypted into the ``data`` column (fail-closed without a key) and decoded
  on read (callers only ever see ``config``).
- **source**: child asset rows are kept in sync with the catalog class's
  ``asset_types`` after every write, including the sibling relations the
  class declares between its own assets.
- **asset**: an owned asset resolves by its qualified key, so its status
  cascades through its source's.

Turning a row into a live framework component is
:mod:`~interloper_db.store.components.hydration`'s job.

Relation reads and writes are not here at all: this store composes a
:class:`~interloper_db.store.relations.RelationStore` and delegates to it,
so the acceptance rules a relation name carries live in one place (see
:mod:`interloper_db.store.relations`).
"""

from __future__ import annotations

import builtins
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

import interloper as il
from interloper.catalog import Catalog
from interloper.errors import CatalogKeyError, ConfigError
from interloper.partitioning import TimeGranularity
from sqlalchemy import Engine, or_
from sqlalchemy import select as sa_select
from sqlalchemy.orm import aliased, selectinload
from sqlmodel import Session, col, select

from interloper_db.errors import InUseError, NotFoundError, ReadOnlyError
from interloper_db.models import Component, ComponentRelation
from interloper_db.session import commit, save, session_scope
from interloper_db.store.components.declaration import Declaration
from interloper_db.store.components.hydration import Hydrator
from interloper_db.store.page import Page, PageQuery
from interloper_db.store.quotas import QUOTA_MAX_ASSETS_PER_SOURCE, QuotaStore
from interloper_db.store.relations import RelationStore

SENSITIVE_KINDS = tuple(kind for kind, anchor in il.KINDS.items() if anchor.sensitive)
"""The kinds whose payload is stored encrypted, so reading a row of one has to decode it."""

# Eager-load set for rows returned to API consumers: the parent, the row's
# own relations with the rows they point at, and the children with theirs, so
# the whole unit reads off a detached row.
COMPONENT_LOAD_OPTIONS = [
    selectinload(Component.parent),  # ty: ignore[invalid-argument-type]
    selectinload(Component.out_relations).selectinload(ComponentRelation.dst),  # ty: ignore[invalid-argument-type]
    selectinload(Component.children)  # ty: ignore[invalid-argument-type]
    .selectinload(Component.out_relations)  # ty: ignore[invalid-argument-type]
    .selectinload(ComponentRelation.dst),  # ty: ignore[invalid-argument-type]
    selectinload(Component.children).selectinload(Component.parent),  # ty: ignore[invalid-argument-type]
]


class ComponentStatus(StrEnum):
    """Usability state of a persisted component in this deployment."""

    OK = "ok"
    """Key resolves in the enabled catalog: the component is live."""

    DISABLED = "disabled"
    """Key exists in code but is not exposed by this deployment's catalog."""

    MISSING = "missing"
    """Key no longer exists in code at all: this is drift."""

    UNREADABLE = "unreadable"
    """Key resolves, but the stored payload does not decrypt under the active
    ``INTERLOPER_ENCRYPTION_KEY`` (rotated, mismatched, or absent). The row is
    intact; its config has to be re-entered or re-keyed."""


class ComponentQuery(PageQuery):
    """Which of an organisation's components a listing reads.

    Attributes:
        kind: Keep components of these kinds; ``None`` keeps every kind.
        q: Keep components whose name or key contains this, case-insensitively.
        roots_only: Keep only components no other component owns, each
            carrying its owned components under ``children`` — the
            collection's unit, the way the catalog reaches an owned definition
            through its owner. ``False`` lists every row, owned ones included.
    """

    kind: list[str] | None = None
    q: str | None = None
    roots_only: bool = True


@dataclass(frozen=True)
class ComponentReading:
    """What one read of a component row yields for a response.

    ``config`` is the decoded payload, ``None`` when the row cannot be read;
    ``public_config`` is its schema-marked ``x-public`` subset; ``discriminator``
    is the class's discriminator value read off it. The whole reading costs one
    decode, so a response derives every field from it instead of decoding per
    field.
    """

    status: ComponentStatus
    config: dict[str, Any] | None
    public_config: dict[str, Any]
    discriminator: str | None


#: The state key holding the digest of what a file last declared for a row.
DECLARED_DIGEST = "declared_digest"


@dataclass(frozen=True)
class ComponentOutline:
    """A component reduced to what names it and tells its state, with no payload and no relation.

    Attributes:
        id: The component.
        kind: Its kind.
        parent_id: The source that owns it, for an owned asset.
        enabled: Whether its config leaves it enabled, as ``Component.enabled`` reads it.
        status: Its status, the one :meth:`ComponentStore.read` gives its row.
        key: Its definition key.
        source_key: The key of the source that owns it, for an owned asset.
        name: Its name, ``None`` when it has none.
        start: The first day its partitions are owed, as ``Component.start`` reads it.
    """

    id: UUID
    kind: str
    parent_id: UUID | None
    enabled: bool
    status: ComponentStatus
    key: str
    source_key: str | None
    name: str | None
    start: date | None

    @property
    def qualified_key(self) -> str:
        """The catalog key, qualified by the owning source's key, as ``Component.qualified_key`` reads it.

        Returns:
            ``source.asset`` for an owned asset, the bare key otherwise.
        """
        return str(il.ComponentIdentity(self.source_key, self.key))


@dataclass(frozen=True)
class DeleteImpact:
    """What deleting a set of components does to the components bound to them.

    Each entry is a ``{id, kind, key, name}`` mapping, the shape
    :class:`~interloper_db.errors.InUseError` reports. A referrer that blocks
    through any relation is listed only as blocking.
    """

    blocking: list[dict[str, str | None]]
    detaching: list[dict[str, str | None]]

    @classmethod
    def from_referrers(cls, blocking: Iterable[Component], detaching: Iterable[Component]) -> DeleteImpact:
        """Describe each referrer the way :class:`InUseError` reports one, in display order.

        Args:
            blocking: The referrers whose relations block the deletion.
            detaching: The referrers whose relations detach instead.

        Returns:
            The impact.
        """

        def refs(components: Iterable[Component]) -> list[dict[str, str | None]]:
            return [
                {"id": str(c.id), "kind": c.kind, "key": c.key, "name": c.name}
                for c in sorted(components, key=lambda c: ((c.name or c.key).lower(), str(c.id)))
            ]

        return cls(blocking=refs(blocking), detaching=refs(detaching))


@dataclass
class Applied:
    """What applying the files' declarations did, for ``interloper dev`` to report.

    Attributes:
        written: How many components the files declare, each written.
        created: Ids of the rows created.
        deleted: Ids of the rows the files no longer declare, deleted.
    """

    written: int = 0
    created: list[UUID] = field(default_factory=list)
    deleted: list[UUID] = field(default_factory=list)


class ComponentStore:
    """Store methods for component rows: CRUD, reading, and live components through the hydrator."""

    def __init__(
        self,
        engine: Engine,
        catalog: Catalog,
        hydrator: Hydrator,
        encrypt: Callable[[bytes], bytes] | None,
        decrypt: Callable[[bytes], bytes] | None,
        quotas: QuotaStore,
        relations: RelationStore,
        *,
        served_from: str | None = None,
    ) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
            catalog: Catalog its component keys resolve against.
            hydrator: Hydrator that turns rows into live framework components.
            encrypt: Callable encrypting a sensitive payload, or None for plaintext.
            decrypt: Callable decrypting a sensitive payload, or None when no key is configured.
            quotas: Quota gates it enforces through.
            relations: Relation facet it wires component edges through.
            served_from: The files the collection is served from, which
                refuses every create, update and delete the app asks for,
                ``apply`` being their one writer; ``None`` for a collection
                the app edits.
        """
        self._engine = engine
        self._catalog = catalog
        self._hydrator = hydrator
        self._encrypt = encrypt
        self._decrypt = decrypt
        self._quotas = quotas
        self._relations = relations
        self._served_from = served_from

    # -- CRUD ------------------------------------------------------------------

    def create(
        self,
        org_id: UUID,
        *,
        kind: str,
        key: str,
        name: str | None = None,
        config: dict[str, Any] | None = None,
        encrypted: bool | None = None,
        children: Sequence[str] | None = None,
        relations: Mapping[str, Sequence[UUID]] | None = None,
    ) -> Component:
        """Create a component of any kind.

        Args:
            org_id: Organisation UUID.
            kind: Component kind (source, asset, destination, connection, …).
            key: Catalog key identifying the component class.
            name: User-facing label.
            config: Instance configuration. For secret kinds this is the
                payload that gets encrypted into the data column.
            encrypted: Secret kinds only — ``True``/``None`` (default) encrypt,
                ``False`` opts into plaintext storage.
            children: Source kinds only — which child asset keys to enable
                (``None`` enables all the catalog class declares).
            relations: ``{name: [dst_id, …]}``, replaced per name.

        Returns:
            The created component row, eager-loaded.

        Raises:
            ConfigError: If ``children`` is passed for a kind that has none.
        """
        self._refuse_served()
        with session_scope(self._engine) as session:
            self._quotas.admit_component(org_id, kind)
            db_component = Component(org_id=org_id, kind=kind, key=key, name=name)
            db_component.write_config(config, encrypt=self._encrypt, encrypted=encrypted)
            if name is None:
                db_component.name = self._derived_name(db_component, config)
            session.add(db_component)
            session.flush()
            if kind == "source":
                self._check_source_collision(db_component)
                self._sync_children(db_component, children)
            elif children is not None:
                raise ConfigError(f"Components of kind '{kind}' have no children")
            self._relations.sync(db_component, relations)
            self._relations.check_bound(db_component)
            commit(session)
            return self.get(db_component.id, org_id=org_id)

    def get(self, component_id: UUID, *, org_id: UUID | None = None, kind: str | None = None) -> Component:
        """Load a component row by ID, its relations and children eager-loaded.

        Args:
            component_id: The component UUID.
            org_id: Organisation the row must belong to; a mismatch raises
                ``NotFoundError`` like an absent row, so a caller cannot learn
                that an id exists in another tenant. ``None`` accepts any
                organisation, for a caller that authorizes by the row's own
                ``org_id`` afterwards (the API) or serves every organisation
                (the scheduler).
            kind: Kind the row must have (``None`` accepts any kind); a
                mismatch raises ``NotFoundError`` the same way.

        Returns:
            The component row, safe to hand out detached.

        Raises:
            NotFoundError: If no row exists, or it has a different kind or
                organisation.
        """
        statement = select(Component).where(Component.id == component_id).options(*COMPONENT_LOAD_OPTIONS)
        with session_scope(self._engine) as session:
            db_component = session.exec(statement).first()
        if (
            db_component is None
            or (org_id is not None and db_component.org_id != org_id)
            or (kind is not None and db_component.kind != kind)
        ):
            raise NotFoundError(f"{kind or 'component'} {component_id} not found".capitalize())
        return db_component

    def list(self, org_id: UUID, query: ComponentQuery) -> Page[Component]:
        """List an organisation's components, oldest first.

        Args:
            org_id: Organisation UUID.
            query: Which kinds, what text, whether owned components list on
                their own, and the window to read.

        Returns:
            The page of components, eager-loaded and safe to hand out detached.
        """
        statement = (
            select(Component)
            .where(Component.org_id == org_id)
            .options(*COMPONENT_LOAD_OPTIONS)
            .order_by(col(Component.created_at), col(Component.id))
        )
        if query.kind:
            statement = statement.where(col(Component.kind).in_(query.kind))
        if query.q:
            statement = statement.where(
                col(Component.name).icontains(query.q, autoescape=True)
                | col(Component.key).icontains(query.q, autoescape=True)
            )
        if query.roots_only:
            statement = statement.where(col(Component.parent_id).is_(None))
        with session_scope(self._engine) as session:
            return Page.read(session, statement, query)

    def update(
        self,
        component_id: UUID,
        *,
        name: str | None = None,
        config: dict[str, Any] | None = None,
        encrypted: bool | None = None,
        children: Sequence[str] | None = None,
        relations: Mapping[str, Sequence[UUID]] | None = None,
    ) -> Component:
        """Update a component's spec. ``None`` leaves a facet untouched.

        Passing ``children`` makes a source's child asset set exactly that
        list; omitting it leaves the current set as is. The machine-owned
        ``state`` column is left alone, with one exception: a job whose config
        changes has its cached ``next_run_at`` cleared, so the scheduler
        re-derives the schedule from the new spec on its next tick instead of
        firing once more at the moment the old spec produced.

        Args:
            component_id: The component UUID.
            name: New user-facing label.
            config: New instance configuration, replacing the stored one
                wholesale. For secret kinds it is encrypted into the data
                column.
            encrypted: Secret kinds only — ``True``/``None`` (default) encrypt,
                ``False`` opts into plaintext storage.
            children: Source kinds only — the exact set of child asset keys to
                keep enabled.
            relations: ``{name: [dst_id, …]}``, replaced per name.

        Returns:
            The updated component row, eager-loaded.

        Raises:
            ConfigError: If ``children`` is passed for a kind that has none.
        """
        self._refuse_served()
        with session_scope(self._engine) as session:
            db_component = self._lock(component_id)
            if name is not None:
                db_component.name = name
            if config is not None:
                # A name equal to the old config's derived default (or blank) is
                # system-owned and follows the config; anything else, a rename in
                # this same call included, is user-owned and untouched.
                if name is None:
                    old_default = self._derived_name(db_component, self._current_config(db_component))
                    if db_component.name is None or db_component.name == old_default:
                        db_component.name = self._derived_name(db_component, config) or db_component.name
                spec_changed = config != (db_component.config or {})
                db_component.write_config(config, encrypt=self._encrypt, encrypted=encrypted)
                if db_component.kind == "job" and spec_changed:
                    db_component.stamp_state(next_run_at=None)
            if db_component.kind == "source":
                self._check_source_collision(db_component)
                if children is not None:
                    self._sync_children(db_component, children)
            elif children is not None:
                raise ConfigError(f"Components of kind '{db_component.kind}' have no children")
            self._relations.sync(db_component, relations)
            self._relations.check_bound(db_component)
            commit(session)
            return self.get(component_id, org_id=db_component.org_id)

    def delete(self, component_id: UUID) -> None:
        """Delete a component. Children and out-bound relations cascade via FK.

        In-bound relations follow the ``on_delete`` the referrer's class
        declares for the name they are filed under, and nothing else: a
        consuming relation (a bound ``connection``, a bound ``destination``,
        a required upstream) blocks the deletion; one declared
        ``on_delete="detach"`` (a job's ``targets``, a hook's ``watches``, an
        optional upstream) detaches, its row cascading away while the
        referrer keeps working with reduced scope.

        Args:
            component_id: The component UUID.

        Raises:
            InUseError: If other components hold blocking relations into this
                one or its children, which must be unbound or deleted first.
            ConfigError: If the component is source-owned (delete or update
                the parent source instead).
        """
        self._refuse_served()
        with session_scope(self._engine) as session:
            db_component = self._lock(component_id)
            if db_component.parent_id is not None:
                raise ConfigError("Cannot delete a source-owned asset directly. Delete or update the source instead.")
            if referrers := self._blocking_referrers(db_component):
                names = ", ".join(str(r["name"] or r["key"]) for r in referrers)
                raise InUseError(
                    f"Cannot delete {db_component.kind} '{db_component.name or db_component.key}': in use by {names}",
                    referrers=referrers,
                )
            session.delete(db_component)
            commit(session)

    def _blocking_referrers(self, db_component: Component) -> builtins.list[dict[str, str | None]]:
        """Components outside a component's subtree whose relations into it block deletion.

        Deleting a relation destination cascades the edge row, which would
        leave a *consuming* referrer silently broken at its next run, so
        those relations refuse the deletion. An edge whose name the referrer
        declares ``on_delete="detach"`` is skipped: cascading it is the
        intended outcome. Edges internal to the subtree (a source's
        own sibling relations) don't count, and a referrer that is a
        source-owned asset is reported as its parent source, the unit the
        user can act on.

        Args:
            db_component: The row about to be deleted.

        Returns:
            One ``{id, kind, key, name}`` dict per blocking referrer, sorted by
            display name; empty when the deletion is unobstructed.
        """
        # Child ids via a bare SELECT, not the ORM relationship: loading the
        # children into the session that is about to delete their parent
        # invites the unit of work to manage them.
        with session_scope(self._engine) as session:
            child_ids = session.exec(select(Component.id).where(Component.parent_id == db_component.id)).all()
        subtree_ids = {db_component.id} | set(child_ids)
        return self._referrers_into(subtree_ids, subtree_ids).blocking

    def delete_impact(self, component_ids: Sequence[UUID]) -> DeleteImpact:
        """Preview what deleting *component_ids* does to the components bound to them.

        The rule :meth:`delete` enforces, evaluated without deleting:
        referrers outside the subtree (the ids plus the components they own)
        whose relations into it block, and those whose relations detach.

        Args:
            component_ids: The components about to be deleted.

        Returns:
            The blocking and detaching referrers.

        Raises:
            NotFoundError: If any of the ids does not exist.
        """
        with session_scope(self._engine) as session:
            for component_id in component_ids:
                if session.get(Component, component_id) is None:
                    raise NotFoundError(f"Component {component_id} not found")
            child_ids = session.exec(select(Component.id).where(col(Component.parent_id).in_(component_ids))).all()
            subtree_ids = set(component_ids) | set(child_ids)
            return self._referrers_into(subtree_ids, subtree_ids)

    def _referrers_into(self, target_ids: set[UUID], subtree_ids: set[UUID]) -> DeleteImpact:
        """Referrers whose relations point into *target_ids* from outside *subtree_ids*, by outcome.

        An edge whose name the referrer declares ``on_delete="detach"``
        detaches; every other blocks. A referrer that is an owned component is
        reported as its owner, the unit the user can act on, and one that
        blocks through any edge is reported as blocking alone.

        Args:
            target_ids: Component IDs whose in-bound relations are inspected.
            subtree_ids: Component IDs that count as "inside" — relations
                originating there are ignored.

        Returns:
            The blocking and detaching referrers, each sorted by display name.
        """
        statement = select(ComponentRelation).where(
            col(ComponentRelation.dst_id).in_(target_ids), col(ComponentRelation.src_id).not_in(subtree_ids)
        )
        blocking: dict[UUID, Component] = {}
        detaching: dict[UUID, Component] = {}
        with session_scope(self._engine) as session:
            for relation in session.exec(statement).all():
                src = session.get(Component, relation.src_id)
                if src is None:
                    continue
                detaches = self._relations.detaches(src, relation)
                if src.parent_id is not None and src.parent_id not in subtree_ids:
                    src = session.get(Component, src.parent_id) or src
                (detaching if detaches else blocking)[src.id] = src
        for component_id in blocking:
            detaching.pop(component_id, None)
        return DeleteImpact.from_referrers(blocking.values(), detaching.values())

    # -- Served collection -----------------------------------------------------

    def apply(self, org_id: UUID, declarations: Sequence[Declaration]) -> Applied:
        """Make an organisation's components exactly what its files declare, as the one writer of a served collection.

        Every declaration is written under its id, created or rewritten in
        place, its assets with it: name, configuration and relations exactly
        as the file says, a relation name the file does not bind left empty.
        A declaration unchanged since the last apply (its digest, kept in the
        row's state) is not written again, so what the platform wrote into it
        since (renewed credentials) stays. Every component the files no longer
        declare is deleted, its runs kept and unlinked, before any asset is
        removed, so an edit that drops an asset together with what reads it
        applies at once. A job whose configuration changed has its cached next
        run cleared, as an edit in the app would. A required relation the
        file leaves unbound is accepted: the component then fails its run as
        any incomplete one does. One transaction: a failure leaves everything
        as it was.

        Args:
            org_id: Organisation UUID, the one the files fill.
            declarations: What the files declare, every row after the rows it binds.

        Returns:
            What changed.
        """
        applied = Applied(written=len(declarations))
        declared = {declaration.id for declaration in declarations}
        with session_scope(self._engine) as session:
            changed: builtins.list[tuple[Component, Declaration]] = []
            for declaration in declarations:
                row = session.get(Component, declaration.id)
                if row is None:
                    row = self._insert_declared(session, org_id, declaration)
                    applied.created.append(row.id)
                elif (row.state or {}).get(DECLARED_DIGEST) == declaration.digest:
                    continue
                else:
                    self._rewrite_declared(row, declaration)
                changed.append((row, declaration))
            session.flush()
            for row in session.exec(
                select(Component).where(Component.org_id == org_id, col(Component.parent_id).is_(None))
            ).all():
                if row.id not in declared:
                    session.delete(row)
                    applied.deleted.append(row.id)
            session.flush()
            for row, declaration in changed:
                self._clear_unbound(session, row.id, declaration)
                for child in declaration.children or []:
                    self._clear_unbound(session, child.id, child)
            for row, declaration in changed:
                if declaration.children is not None:
                    self._declare_children(row, declaration)
            for row, declaration in changed:
                for bound in (declaration, *(declaration.children or [])):
                    target = session.get(Component, bound.id)
                    if target is not None and bound.relations:
                        self._relations.sync(target, bound.relations)
                row.stamp_state(**{DECLARED_DIGEST: declaration.digest})
                if declaration.children is not None:
                    self._check_source_collision(row)
            commit(session)
        return applied

    def _insert_declared(self, session: Session, org_id: UUID, declaration: Declaration) -> Component:
        """Create a declared row without its assets or relations, which apply adds once every row exists.

        Args:
            session: The open session.
            org_id: Organisation UUID.
            declaration: What it declares.

        Returns:
            The row.
        """
        self._quotas.admit_component(org_id, declaration.kind)
        row = Component(id=declaration.id, org_id=org_id, kind=declaration.kind, key=declaration.key)
        row.write_config(declaration.config, encrypt=self._encrypt, encrypted=None)
        row.name = self._derived_name(row, declaration.config)
        session.add(row)
        return row

    def _rewrite_declared(self, row: Component, declaration: Declaration) -> None:
        """Rewrite a declared row's name and configuration as its file says now, its id and history kept.

        Args:
            row: The row.
            declaration: What it declares now.
        """
        spec_changed = declaration.config != (self._current_config(row) or {})
        row.write_config(declaration.config, encrypt=self._encrypt, encrypted=None)
        row.name = self._derived_name(row, declaration.config)
        if row.kind == "job" and spec_changed:
            row.stamp_state(next_run_at=None)

    def _clear_unbound(self, session: Session, row_id: UUID, declaration: Declaration) -> None:
        """Empty every relation name a row holds that its declaration does not bind.

        Args:
            session: The open session.
            row_id: The row, which may not exist yet (an asset still to create).
            declaration: What it declares.
        """
        row = session.get(Component, row_id)
        if row is None:
            return
        bound = set(session.exec(select(ComponentRelation.name).where(ComponentRelation.src_id == row_id)))
        if unbound := sorted(bound - set(declaration.relations)):
            self._relations.clear(row, unbound)

    def _declare_children(self, row: Component, declaration: Declaration) -> None:
        """Keep a declared source's assets to the ones it declares, each with its own configuration.

        Args:
            row: The source row.
            declaration: What it declares.
        """
        children = {child.key: child for child in declaration.children or []}
        self._sync_children(row, builtins.list(children), child_id=Declaration.child_id)
        with session_scope(self._engine) as session:
            for child in session.exec(select(Component).where(Component.parent_id == row.id)).all():
                child.write_config(children[child.key].config, encrypt=self._encrypt, encrypted=None)

    def _refuse_served(self) -> None:
        """Refuse a change the app asks of a collection its files serve.

        Raises:
            ReadOnlyError: If the collection is served from files.
        """
        if self._served_from is not None:
            raise ReadOnlyError(f"The collection is served from {self._served_from}: change it there")

    # -- Reading ---------------------------------------------------------------

    def load(self, component_id: UUID) -> il.Component:
        """Hydrate a framework component of any kind from its row.

        Args:
            component_id: The component UUID.

        Returns:
            The live component; see :meth:`Hydrator.load` for what fails closed.
        """
        return self._hydrator.load(component_id)

    def read(self, db_component: Component) -> ComponentReading:
        """Read a row once: its status and every view of its payload a response shows.

        The catalog answers first: without a resolvable key there is no schema
        to read the payload against, so drift outranks readability. The payload
        is then decoded once; a payload that does not decode makes an otherwise
        live row ``UNREADABLE``, while a drifted row keeps whatever config it
        holds, so a surface can still show what the row carries. The row's
        class says which fields are public and which one is the
        discriminator; the payload supplies their values.

        Args:
            db_component: The row to read, its parent loaded when it has one.

        Returns:
            The reading: status, decoded config, its public subset and the
            discriminator value.
        """
        status = self._key_status(db_component.qualified_key)
        config = self._current_config(db_component)
        if config is None and status is ComponentStatus.OK:
            status = ComponentStatus.UNREADABLE
        cls = self._resolve_class(db_component)
        if cls is None or config is None:
            return ComponentReading(status=status, config=config, public_config={}, discriminator=None)
        field = cls.discriminator_field()
        discriminator = config.get(field) if field else None
        return ComponentReading(
            status=status,
            config=config,
            public_config={name: config[name] for name in cls.public_fields() if name in config},
            discriminator=str(discriminator) if discriminator else None,
        )

    def outlines(self, org_id: UUID, *, kinds: Sequence[str] | None = None) -> builtins.list[ComponentOutline]:
        """Outline the components of an organisation without loading their rows.

        The catalog resolves each definition once, however many rows
        instantiate it. A row is loaded only where its status depends on its
        own content: a sensitive kind's payload has to decode.

        Args:
            org_id: Organisation UUID.
            kinds: Keep these kinds; ``None`` keeps every kind.

        Returns:
            One outline per component, in no particular order.
        """
        parent = aliased(Component)
        statement = (
            sa_select(
                col(Component.id),
                col(Component.kind),
                col(Component.parent_id),
                col(Component.config)["enabled"].as_boolean().is_not(False),
                col(parent.key),
                col(Component.key),
                col(Component.name),
                col(Component.config)["start"].as_string(),
                col(parent.config)["start"].as_string(),
            )
            .outerjoin(parent, col(parent.id) == col(Component.parent_id))
            .where(col(Component.org_id) == org_id)
        )
        sensitive_kinds = [kind for kind in SENSITIVE_KINDS if kinds is None or kind in kinds]
        if kinds is not None:
            statement = statement.where(col(Component.kind).in_(kinds))
        sensitive = select(Component).where(Component.org_id == org_id, col(Component.kind).in_(sensitive_kinds))
        with session_scope(self._engine) as session:
            rows = session.connection().execute(statement).all()
            decoded = {row.id: self.read(row).status for row in session.exec(sensitive).all()}
        resolved: dict[il.ComponentIdentity, ComponentStatus] = {}
        outlines = []
        for component_id, kind, parent_id, enabled, source_key, key, name, own_start, source_start in rows:
            identity = il.ComponentIdentity(source_key, key)
            if identity not in resolved:
                resolved[identity] = self._key_status(str(identity))
            start = own_start or source_start
            outlines.append(
                ComponentOutline(
                    component_id,
                    kind,
                    parent_id,
                    enabled,
                    decoded.get(component_id, resolved[identity]),
                    key=key,
                    source_key=source_key,
                    name=name,
                    start=date.fromisoformat(start) if start else None,
                )
            )
        return outlines

    def _key_status(self, key: str) -> ComponentStatus:
        """Whether a qualified key resolves here, exists only in the installed code, or is gone.

        An owned asset resolves by its qualified key, so a drifted source
        cascades to its assets, and an asset its source no longer declares
        reads as drifted under a live source.

        Args:
            key: The qualified key to resolve.

        Returns:
            ``OK``, ``DISABLED`` or ``MISSING``.
        """
        if self._catalog.get(key) is not None:
            return ComponentStatus.OK
        if Catalog.discover().get(key) is not None:
            return ComponentStatus.DISABLED
        return ComponentStatus.MISSING

    def _current_config(self, db_component: Component) -> dict[str, Any] | None:
        """The row's stored config payload, decoding secret kinds.

        Args:
            db_component: The row to read the payload from.

        Returns:
            The decoded configuration dict, or ``None`` when it can't be
            decoded (no cipher configured, or a corrupt payload).
        """
        try:
            return db_component.read_config(self._decrypt)
        except Exception:  # noqa: BLE001 — no cipher / corrupt payload: treat as underivable
            return None

    def merge_config(self, component_id: UUID, fields: dict[str, Any]) -> Component:
        """Merge fields into a component's stored config payload.

        The write-back primitive for operation config effects (e.g. a
        machine-renewed credential): merges into the decoded payload and
        re-applies it through the same encryption path as create/update,
        preserving the row's plaintext opt-in. Everything else about the
        row (name, relations, state) is untouched.

        Args:
            component_id: The component UUID.
            fields: Config fields to merge over the stored payload.

        Returns:
            The updated component row.
        """
        with session_scope(self._engine) as session:
            db_component = self._lock(component_id)
            payload = {**db_component.read_config(self._decrypt), **fields}
            encrypted = db_component.encrypted if il.KINDS[db_component.kind].sensitive else None
            db_component.write_config(payload, encrypt=self._encrypt, encrypted=encrypted)
            save(session, db_component)
            return self.get(component_id)

    def stamp_state(self, component_id: UUID, **fields: Any) -> Component:
        """Merge machine-owned state fields onto a component row, by id.

        The session-owning counterpart to :meth:`Component.stamp_state`, for
        callers that hold an id rather than a row (the run executor applying
        operation state effects).

        Args:
            component_id: The component UUID.
            **fields: State fields to set, merged over the existing payload.

        Returns:
            The updated component row.
        """
        with session_scope(self._engine) as session:
            db_component = self._lock(component_id)
            db_component.stamp_state(**fields)
            save(session, db_component)
            return self.get(component_id)

    def lock_due(
        self,
        kind: str,
        state_key: str,
        *,
        now: datetime,
        limit: int,
        keys: Sequence[str] | None = None,
        enabled_only: bool = False,
    ) -> builtins.list[Component]:
        """Lock the rows whose machine-owned *state_key* instant has come, most overdue first.

        A row that never carried the instant is due too, after every overdue
        one. Rows another process holds are skipped (``SKIP LOCKED``), so
        concurrent schedulers split the work. The locks last as long as the
        enclosing :meth:`Store.transaction`, which is where a caller advances
        the instant before the rows are released.

        Args:
            kind: The component kind to scan.
            state_key: The state field holding each row's next due instant.
            now: The instant rows are due by.
            limit: The most rows to lock.
            keys: Keep rows of these catalog keys; ``None`` keeps every key.
            enabled_only: Keep only rows whose config enables them.

        Returns:
            The locked rows.
        """
        due = col(Component.state)[state_key].as_string()
        statement = (
            select(Component)
            .where(Component.kind == kind)
            .where(or_(due <= now.isoformat(), due.is_(None)))
            .order_by(due.asc().nulls_last())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        if keys is not None:
            statement = statement.where(col(Component.key).in_(keys))
        if enabled_only:
            statement = statement.where(col(Component.config)["enabled"].as_boolean())
        with session_scope(self._engine) as session:
            return [*session.exec(statement).all()]

    # -- Kind semantics --------------------------------------------------------

    def _resolve_class(self, db_component: Component) -> type[il.Component] | None:
        """The component class a row's ``key`` and ``kind`` select in this catalog.

        Args:
            db_component: The row whose ``key`` and ``kind`` select the class.

        Returns:
            The class, or ``None`` when the key doesn't resolve or resolves to
            a class of another kind (a stale row).
        """
        try:
            cls = il.Component.resolve_key(db_component.key, self._catalog)
        except (CatalogKeyError, ImportError, AttributeError, TypeError):
            return None
        return cls if cls.kind == db_component.kind else None

    def _derived_name(self, db_component: Component, config: dict[str, Any] | None) -> str | None:
        """The display name the component class derives from *config*.

        Args:
            db_component: The row whose ``key`` and ``kind`` select the class.
            config: The configuration to construct that class from.

        Returns:
            ``instance_name()`` of an instance constructed from the config, or
            ``None`` when the key doesn't resolve or the config can't construct
            the class.
        """
        cls = self._resolve_class(db_component)
        if cls is None:
            return None
        try:
            instance = cls(**(config or {}))
        except Exception:  # noqa: BLE001 — incomplete/stale config: nothing to derive
            return None
        return instance.instance_name()

    def _check_source_collision(self, db_source: Component) -> None:
        """Reject a source instance whose materialization target collides with a sibling.

        Two instances of the same source class write to the same physical
        ``dataset.table`` set unless they differ in ``dataset`` or in what
        the class's ``table`` derives from their config — such an
        instance would silently overwrite the sibling's data on every run.

        Targets are computed by instantiating the class from each config, so
        the check is exact under any ``table`` override. An instance
        whose config can't construct the class yet is skipped — it can't run
        either, and the check re-fires on every update.

        Args:
            db_source: The source row being created or updated.

        Raises:
            ConfigError: If a same-key sibling in the org targets the same tables.
        """
        try:
            source_cls = il.Source.resolve_key(db_source.key, self._catalog)
        except (CatalogKeyError, ImportError, AttributeError, TypeError):
            return  # drifted key: nothing to derive a target from

        def targets(config: dict[str, Any] | None) -> set[tuple[str | None, str]] | None:
            try:
                source = source_cls(**(config or {}))
            except Exception:  # noqa: BLE001 — incomplete/stale config: nothing to compare
                return None
            return {(asset.dataset, asset.table) for asset in source.assets}

        mine = targets(db_source.config)
        if not mine:
            return
        statement = select(Component).where(
            Component.org_id == db_source.org_id,
            Component.kind == "source",
            Component.key == db_source.key,
            Component.id != db_source.id,
        )
        with session_scope(self._engine) as session:
            siblings = session.exec(statement).all()
        for sibling in siblings:
            overlap = mine & (targets(sibling.config) or set())
            if overlap:
                dataset, table = min(overlap)
                raise ConfigError(
                    f"Source '{db_source.key}' already has an instance materializing to '{dataset}.{table}'. "
                    f"Configure a distinct discriminator (or dataset) so the two don't overwrite each other's data."
                )

    def _sync_children(
        self,
        db_source: Component,
        child_keys: Sequence[str] | None,
        *,
        child_id: Callable[[UUID, str], UUID] | None = None,
    ) -> None:
        """Sync a source's child asset rows to match the desired set.

        When ``child_keys`` is provided, only those assets will exist —
        missing ones are created, extra ones are removed. Removal follows the
        delete guard's semantics: blocking relations from outside the source
        (a required cross-source upstream) raise ``InUseError``; detaching
        ones and intra-source edges cascade. ``None`` is the source-creation
        default and enables every asset the catalog class declares. Existing
        rows keep their IDs (and therefore their cross-source upstreams, event
        references, and per-asset overrides).

        Args:
            db_source: The source row whose children are synced.
            child_keys: The exact asset keys to keep enabled, or ``None`` to
                enable every asset the catalog class declares.
            child_id: Derives a new asset's id from its source's and its key,
                for a file that refers to it before it exists; ``None`` for a
                fresh id.

        Raises:
            CatalogKeyError: If the source key does not resolve in the catalog.
            ConfigError: If ``child_keys`` names assets the source doesn't declare.
        """
        try:
            source_cls = il.Source.resolve_key(db_source.key, self._catalog)
        except (CatalogKeyError, ImportError, AttributeError, TypeError) as error:
            raise CatalogKeyError(f"Unknown source key: {db_source.key}") from error
        all_keys = {asset_type.key for asset_type in source_cls.asset_types}
        if child_keys is not None and (unknown := set(child_keys) - all_keys):
            raise ConfigError(
                f"Source '{db_source.key}' declares no asset(s) {sorted(unknown)} (available: {sorted(all_keys)})"
            )

        with session_scope(self._engine) as session:
            self._sync_children_in(session, db_source, source_cls, all_keys, child_keys, child_id)

    def _sync_children_in(
        self,
        session: Session,
        db_source: Component,
        source_cls: type[il.Source],
        all_keys: set[str],
        child_keys: Sequence[str] | None,
        child_id: Callable[[UUID, str], UUID] | None,
    ) -> None:
        """Reshape the child rows in the session :meth:`_sync_children` opened.

        Args:
            session: The open session.
            db_source: The source row whose children are synced.
            source_cls: The source's catalog class.
            all_keys: Every asset key the class declares.
            child_keys: The exact asset keys to keep enabled, or ``None`` for all.
            child_id: Derives a new asset's id, or ``None`` for a fresh one.

        Raises:
            InUseError: If a removed asset is referenced from outside the source.
        """
        existing = {
            child.key: child
            for child in session.exec(select(Component).where(Component.parent_id == db_source.id)).all()
        }
        target = set(child_keys) if child_keys is not None else all_keys
        self._quotas.check(
            db_source.org_id,
            QUOTA_MAX_ASSETS_PER_SOURCE,
            used=len(target),
            subject=db_source.name or db_source.key,
        )
        to_create = target - set(existing)
        to_remove = set(existing) - target

        if to_remove:
            # Removing a child cascades its inbound relations — the same loss
            # the delete guard protects against, so guard here too. Relations
            # from inside the source's own subtree don't block: reshaping the
            # child set is exactly what this call is for.
            subtree_ids = {db_source.id} | {child.id for child in existing.values()}
            removed_ids = {existing[key].id for key in to_remove}
            if referrers := self._referrers_into(removed_ids, subtree_ids).blocking:
                names = ", ".join(str(r["name"] or r["key"]) for r in referrers)
                raise InUseError(
                    f"Cannot remove asset(s) {sorted(to_remove)} from source "
                    f"'{db_source.name or db_source.key}': in use by {names}",
                    referrers=referrers,
                )
            for key in to_remove:
                session.delete(existing[key])
            session.flush()

        children = {key: child for key, child in existing.items() if key in target}
        for key in to_create:
            child = Component(org_id=db_source.org_id, kind="asset", key=key, parent_id=db_source.id)
            if child_id is not None:
                child.id = child_id(db_source.id, key)
            children[key] = child
            session.add(child)
        session.flush()

        self._relations.bind_siblings(source_cls, children)

    def job_partition_granularities(self, job_ids: Sequence[UUID]) -> dict[UUID, set[TimeGranularity]]:
        """The granularities each job's window-partitioned targets declare, in three queries.

        Granularity lives on the target assets' catalog definitions, never on
        the job's config (a denormalized copy could silently drift from the
        catalog). A source target contributes its partitioned assets'
        granularities, an asset target its own; a snapshot asset ignores the
        window and contributes none, so a job of snapshots alone runs
        unwindowed. A target whose key does not
        resolve contributes nothing (drift is the run path's problem, not the
        scheduler's). The jobs' target relations, the target rows and their
        parents are each read in one query, whatever the number of jobs.

        Args:
            job_ids: UUIDs of the job components.

        Returns:
            Per job, the distinct granularities its targets resolve to; a job
            with no partitioned target maps to an empty set. The caller
            decides what more than one granularity means.
        """
        if not job_ids:
            return {}
        relations_statement = select(ComponentRelation).where(
            col(ComponentRelation.src_id).in_(job_ids), ComponentRelation.name == "targets"
        )
        with session_scope(self._engine) as session:
            relations = session.exec(relations_statement).all()
            targets = {
                row.id: row
                for row in session.exec(
                    select(Component)
                    .where(col(Component.id).in_({relation.dst_id for relation in relations}))
                    .options(selectinload(Component.parent))  # ty: ignore[invalid-argument-type]
                ).all()
            }
        granularities: dict[UUID, set[TimeGranularity]] = {job_id: set() for job_id in job_ids}
        for relation in relations:
            if (target := targets.get(relation.dst_id)) is None:
                continue
            definition = self._catalog.get(target.qualified_key)
            if isinstance(definition, il.SourceDefinition):
                partitionings = definition.partitionings()
            elif isinstance(definition, il.AssetDefinition) and definition.partitioning is not None:
                partitionings = [definition.partitioning]
            else:
                partitionings = []
            configs = [il.PartitionConfig.from_definition(partitioning) for partitioning in partitionings]
            granularities[relation.src_id].update(
                config.granularity
                for config in configs
                if config.windowed and isinstance(config, il.TimePartitionConfig)
            )
        return granularities

    def asset_partitionings(self, assets: Iterable[Component | ComponentOutline]) -> dict[UUID, il.TimePartitionConfig]:
        """The partitioning of each partitioned asset among some asset rows.

        Partitioning lives on the catalog definition, never on the row, which
        resolves by its qualified key, so the assets of one definition share
        one partitioning. A row whose key does not resolve (a drifted key, a
        disabled or missing source) is skipped, as is an unpartitioned asset.

        Args:
            assets: The asset rows, their parents loaded, or their outlines.

        Returns:
            Each time- or snapshot-partitioned asset's partitioning by row id; an asset
            partitioned another way has no coverage yet and is skipped.
        """
        by_key: dict[str, il.TimePartitionConfig | None] = {}
        partitionings: dict[UUID, il.TimePartitionConfig] = {}
        for asset in assets:
            key = asset.qualified_key
            if key not in by_key:
                definition = self._catalog.get(key)
                partitioning = definition.partitioning if isinstance(definition, il.AssetDefinition) else None
                config = il.PartitionConfig.from_definition(partitioning) if partitioning is not None else None
                by_key[key] = config if isinstance(config, il.TimePartitionConfig) else None
            if (config := by_key[key]) is not None:
                partitionings[asset.id] = config
        return partitionings

    def target_assets(self, job: Component) -> builtins.list[Component]:
        """The assets a job targets, directly or through a source it targets.

        Args:
            job: The job row, its relations loaded.

        Returns:
            The asset rows, oldest first, their parents loaded.
        """
        targets = [relation.dst_id for relation in job.out_relations if relation.name == "targets"]
        statement = (
            select(Component)
            .where(
                Component.kind == "asset",
                col(Component.id).in_(targets) | col(Component.parent_id).in_(targets),
            )
            .options(selectinload(Component.parent))  # ty: ignore[invalid-argument-type]
            .order_by(col(Component.created_at), col(Component.id))
        )
        with session_scope(self._engine) as session:
            return list(session.exec(statement).all())

    # -- Internals -------------------------------------------------------------

    def _lock(self, component_id: UUID) -> Component:
        """Load a component row for a write, holding it for the rest of the transaction.

        Args:
            component_id: The component UUID.

        Returns:
            The component row alone: a write reshapes its relations and
            children, so it reads them fresh afterwards rather than through
            collections loaded here.

        Raises:
            NotFoundError: If no row exists.
        """
        statement = select(Component).where(Component.id == component_id).with_for_update()
        with session_scope(self._engine) as session:
            db_component = session.exec(statement).first()
            if db_component is None:
                raise NotFoundError(f"Component {component_id} not found")
            return db_component
