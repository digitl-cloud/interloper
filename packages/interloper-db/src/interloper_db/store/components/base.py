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
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

import interloper as il
from interloper.catalog.base import Catalog
from interloper.errors import (
    CatalogKeyError,
    ConfigError,
    InUseError,
    NotFoundError,
)
from interloper.partitioning.time import TimeGranularity
from sqlalchemy import Engine, or_
from sqlalchemy.orm import selectinload
from sqlmodel import Session, col, select

from interloper_db.models import Component, ComponentRelation
from interloper_db.session import commit, session_scope
from interloper_db.store.components.hydration import Hydrator
from interloper_db.store.page import Page, PageQuery
from interloper_db.store.quotas import QUOTA_MAX_ASSETS_PER_SOURCE, QuotaStore
from interloper_db.store.relations import RelationStore

# Eager-load set for rows returned to API consumers: the parent, the row's
# own relations with the rows they point at, and the children with theirs, so
# the whole unit reads off a detached row.
COMPONENT_LOAD_OPTIONS = [
    selectinload(Component.parent),  # ty: ignore[invalid-argument-type]
    selectinload(Component.out_relations)  # ty: ignore[invalid-argument-type]
    .selectinload(ComponentRelation.dst),  # ty: ignore[invalid-argument-type]
    selectinload(Component.children)  # ty: ignore[invalid-argument-type]
    .selectinload(Component.out_relations)  # ty: ignore[invalid-argument-type]
    .selectinload(ComponentRelation.dst),  # ty: ignore[invalid-argument-type]
    selectinload(Component.children).selectinload(Component.parent),  # ty: ignore[invalid-argument-type]
]


class ComponentStatus(str, Enum):
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


@dataclass(frozen=True)
class DeleteImpact:
    """What deleting a set of components does to the components bound to them.

    Each entry is a ``{id, kind, key, name}`` mapping, the shape
    :class:`~interloper.errors.InUseError` reports. A referrer that blocks
    through any relation is listed only as blocking.
    """

    blocking: list[dict[str, str | None]]
    detaching: list[dict[str, str | None]]


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
        """
        self._engine = engine
        self._catalog = catalog
        self._hydrator = hydrator
        self._encrypt = encrypt
        self._decrypt = decrypt
        self._quotas = quotas
        self._relations = relations

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
        with session_scope(self._engine) as session:
            self._quotas.admit_component(org_id, kind)
            db_component = Component(org_id=org_id, kind=kind, key=key, name=name)
            db_component.write_config(config, encrypt=self._encrypt, encrypted=encrypted)
            if name is None:
                db_component.name = self._derived_name(db_component, config)
            session.add(db_component)
            session.flush()
            if kind == "source":
                self._check_source_collision(session, db_component)
                self._ensure_children(session, db_component, children)
            elif children is not None:
                raise ConfigError(f"Components of kind '{kind}' have no children")
            self._relations._sync_relations(session, db_component, relations)
            self._relations._require_bound(session, db_component)
            commit(session)
            return self._load_component(session, db_component.id)

    def get(self, component_id: UUID, *, kind: str | None = None, org_id: UUID | None = None) -> Component:
        """Load a component row by ID with relations eager-loaded.

        Args:
            component_id: The component UUID.
            kind: Kind the row must have (``None`` accepts any kind); a
                mismatch raises ``NotFoundError`` like an absent row, so a
                caller cannot learn that an id exists under another kind.
            org_id: Organisation the row must belong to (``None`` accepts
                any); a mismatch raises ``NotFoundError`` like an absent row,
                so a caller cannot learn that an id exists in another tenant.

        Returns:
            The component row, eager-loaded and safe to hand out detached.
        """
        with session_scope(self._engine) as session:
            return self._load_component(session, component_id, kind=kind, org_id=org_id)

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
            NotFoundError: If the component is not found.
            ConfigError: If ``children`` is passed for a kind that has none.
        """
        with session_scope(self._engine) as session:
            db_component = session.get(Component, component_id)
            if not db_component:
                raise NotFoundError(f"Component {component_id} not found")
            if name is not None:
                db_component.name = name
            if config is not None:
                self._refresh_derived_name(db_component, new_config=config, explicit_rename=name is not None)
                spec_changed = config != (db_component.config or {})
                db_component.write_config(config, encrypt=self._encrypt, encrypted=encrypted)
                if db_component.kind == "job" and spec_changed:
                    db_component.stamp_state(next_run_at=None)
            if db_component.kind == "source":
                self._check_source_collision(session, db_component)
                if children is not None:
                    self._ensure_children(session, db_component, children)
            elif children is not None:
                raise ConfigError(f"Components of kind '{db_component.kind}' have no children")
            self._relations._sync_relations(session, db_component, relations)
            self._relations._require_bound(session, db_component)
            commit(session)
            return self._load_component(session, component_id)

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
            NotFoundError: If the component is not found.
            InUseError: If other components hold blocking relations into this
                one or its children — those must be unbound or deleted first.
            ConfigError: If the component is source-owned (delete or update
                the parent source instead).
        """
        with session_scope(self._engine) as session:
            db_component = session.get(Component, component_id)
            if not db_component:
                raise NotFoundError(f"Component {component_id} not found")
            if db_component.parent_id is not None:
                raise ConfigError("Cannot delete a source-owned asset directly. Delete or update the source instead.")
            if referrers := self._blocking_referrers(session, db_component):
                names = ", ".join(str(r["name"] or r["key"]) for r in referrers)
                raise InUseError(
                    f"Cannot delete {db_component.kind} '{db_component.name or db_component.key}': in use by {names}",
                    referrers=referrers,
                )
            session.delete(db_component)
            commit(session)

    def _blocking_referrers(self, session: Session, db_component: Component) -> builtins.list[dict[str, str | None]]:
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
            session: Open session the deletion is being staged in.
            db_component: The row about to be deleted.

        Returns:
            One ``{id, kind, key, name}`` dict per blocking referrer, sorted by
            display name; empty when the deletion is unobstructed.
        """
        # Child ids via a bare SELECT, not the ORM relationship: loading the
        # children into the session that is about to delete their parent
        # invites the unit of work to manage them.
        child_ids = session.exec(select(Component.id).where(Component.parent_id == db_component.id)).all()
        subtree_ids = {db_component.id} | set(child_ids)
        return self._referrers_into(session, subtree_ids, subtree_ids).blocking

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
            child_ids = session.exec(
                select(Component.id).where(col(Component.parent_id).in_(component_ids))
            ).all()
            subtree_ids = set(component_ids) | set(child_ids)
            return self._referrers_into(session, subtree_ids, subtree_ids)

    def _referrers_into(self, session: Session, target_ids: set[UUID], subtree_ids: set[UUID]) -> DeleteImpact:
        """Referrers whose relations point into *target_ids* from outside *subtree_ids*, by outcome.

        An edge whose name the referrer declares ``on_delete="detach"``
        detaches; every other blocks. A referrer that is an owned component is
        reported as its owner, the unit the user can act on, and one that
        blocks through any edge is reported as blocking alone.

        Args:
            session: Open session to query in.
            target_ids: Component IDs whose in-bound relations are inspected.
            subtree_ids: Component IDs that count as "inside" — relations
                originating there are ignored.

        Returns:
            The blocking and detaching referrers, each sorted by display name.
        """
        rows = session.exec(
            select(ComponentRelation).where(
                col(ComponentRelation.dst_id).in_(target_ids),
                col(ComponentRelation.src_id).not_in(subtree_ids),
            )
        ).all()
        blocking: dict[UUID, Component] = {}
        detaching: dict[UUID, Component] = {}
        for relation in rows:
            src = session.get(Component, relation.src_id)
            if src is None:
                continue
            detaches = self._relations._relation_detaches(session, src, relation)
            if src.parent_id is not None and src.parent_id not in subtree_ids:
                src = session.get(Component, src.parent_id) or src
            (detaching if detaches else blocking)[src.id] = src
        for component_id in blocking:
            detaching.pop(component_id, None)
        return DeleteImpact(
            blocking=self._referrer_refs(blocking.values()),
            detaching=self._referrer_refs(detaching.values()),
        )

    @staticmethod
    def _referrer_refs(components: Iterable[Component]) -> builtins.list[dict[str, str | None]]:
        """``{id, kind, key, name}`` mappings for *components*, sorted by display name.

        Args:
            components: The referrer rows.

        Returns:
            One mapping per component, in display order.
        """
        return [
            {"id": str(c.id), "kind": c.kind, "key": c.key, "name": c.name}
            for c in sorted(components, key=lambda c: ((c.name or c.key).lower(), str(c.id)))
        ]

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
        status = self._key_status(db_component)
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

    def _key_status(self, db_component: Component) -> ComponentStatus:
        """Whether a row's key resolves here, exists only in the installed code, or is gone.

        An owned asset resolves by its qualified key, so a drifted source
        cascades to its assets, and an asset its source no longer declares
        reads as drifted under a live source.

        Args:
            db_component: The row to resolve, its parent loaded when it has one.

        Returns:
            ``OK``, ``DISABLED`` or ``MISSING``.
        """
        key = db_component.qualified_key
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
            db_component = self._load_component(session, component_id)
            payload = {**db_component.read_config(self._decrypt), **fields}
            encrypted = db_component.encrypted if il.KINDS[db_component.kind].sensitive else None
            db_component.write_config(payload, encrypt=self._encrypt, encrypted=encrypted)
            session.add(db_component)
            commit(session)
            session.refresh(db_component)
            return db_component

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
            db_component = self._load_component(session, component_id)
            db_component.stamp_state(**fields)
            session.add(db_component)
            commit(session)
            session.refresh(db_component)
            return db_component

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
        due = Component.state[state_key].as_string()  # ty: ignore[not-subscriptable]
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
            statement = statement.where(Component.config["enabled"].as_boolean())  # ty: ignore[not-subscriptable]
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

    def _refresh_derived_name(
        self, db_component: Component, *, new_config: dict[str, Any], explicit_rename: bool
    ) -> None:
        """Let a never-customized display name follow a config change.

        A name equal to the old config's derived default (or blank) is
        system-owned and follows along; anything else — including a rename in
        this same call — is user-owned and untouched.

        Args:
            db_component: The row, still carrying its old config and name.
            new_config: The configuration about to replace the stored one.
            explicit_rename: Whether the same update also sets ``name``, which
                makes the name user-owned and stops it from following.
        """
        if explicit_rename:
            return
        old_default = self._derived_name(db_component, self._current_config(db_component))
        if db_component.name is None or db_component.name == old_default:
            db_component.name = self._derived_name(db_component, new_config) or db_component.name

    def _check_source_collision(self, session: Session, db_source: Component) -> None:
        """Reject a source instance whose materialization target collides with a sibling.

        Two instances of the same source class write to the same physical
        ``dataset.table`` set unless they differ in ``dataset`` or in what
        the class's ``asset_table`` derives from their config — such an
        instance would silently overwrite the sibling's data on every run.

        Targets are computed by instantiating the class from each config, so
        the check is exact under any ``asset_table`` override. An instance
        whose config can't construct the class yet is skipped — it can't run
        either, and the check re-fires on every update.

        Args:
            session: Open session the source is being written in.
            db_source: The source row being created or updated.

        Raises:
            ConfigError: If a same-key sibling in the org targets the same tables.
        """
        try:
            source_cls = il.Source.resolve_key(db_source.key, self._catalog)
        except (CatalogKeyError, ImportError, AttributeError, TypeError):
            return  # drifted key: nothing to derive a target from

        def targets(config: dict[str, Any] | None) -> set[tuple[str, str]] | None:
            try:
                source = source_cls(**(config or {}))
            except Exception:  # noqa: BLE001 — incomplete/stale config: nothing to compare
                return None
            return {(asset.dataset, asset.table) for asset in source.assets}

        mine = targets(db_source.config)
        if not mine:
            return
        siblings = session.exec(
            select(Component).where(
                Component.org_id == db_source.org_id,
                Component.kind == "source",
                Component.key == db_source.key,
                Component.id != db_source.id,
            )
        ).all()
        for sibling in siblings:
            overlap = mine & (targets(sibling.config) or set())
            if overlap:
                dataset, table = min(overlap)
                raise ConfigError(
                    f"Source '{db_source.key}' already has an instance materializing to '{dataset}.{table}'. "
                    f"Configure a distinct discriminator (or dataset) so the two don't overwrite each other's data."
                )

    def _ensure_children(self, session: Session, db_source: Component, child_keys: Sequence[str] | None) -> None:
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
            session: Open session the source is being written in.
            db_source: The source row whose children are synced.
            child_keys: The exact asset keys to keep enabled, or ``None`` to
                enable every asset the catalog class declares.

        Raises:
            CatalogKeyError: If the source key does not resolve in the catalog.
            ConfigError: If ``child_keys`` names assets the source doesn't declare.
            InUseError: If a removed asset is referenced from outside the source.
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
            if referrers := self._referrers_into(session, removed_ids, subtree_ids).blocking:
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
            children[key] = Component(org_id=db_source.org_id, kind="asset", key=key, parent_id=db_source.id)
            session.add(children[key])
        session.flush()

        self._relations._bind_siblings(session, source_cls, children)

    def job_partition_granularity(self, job_id: UUID) -> TimeGranularity | None:
        """Resolve the granularity a job's partitioned targets share.

        Granularity lives on the target assets' catalog definitions, never on
        the job's config (a denormalized copy could silently drift from the
        catalog). A source target contributes the granularities of its
        partitioned assets; an owned-asset target is looked up inside its
        parent source's definition.

        Args:
            job_id: UUID of the job component.

        Returns:
            The single granularity, or ``None`` when no partitioned target
            resolves (the caller decides the fallback).

        Raises:
            ConfigError: If the targets disagree on granularity — scheduling a
                window would be wrong for some of them, so fail closed.
        """
        with session_scope(self._engine) as session:
            granularities = self._job_target_granularities(session, [job_id]).get(job_id, set())
        if len(granularities) > 1:
            names = ", ".join(sorted(g.value for g in granularities))
            raise ConfigError(f"Job targets disagree on partition granularity ({names})")
        return next(iter(granularities), None)

    def job_partition_granularities(self, job_ids: Sequence[UUID]) -> dict[UUID, TimeGranularity | None]:
        """Resolve :meth:`job_partition_granularity` for several jobs in one session.

        A read-side view, for callers that describe windows rather than
        schedule them: a job whose targets disagree reads as unpartitioned
        instead of failing the whole batch.

        Args:
            job_ids: UUIDs of the job components.

        Returns:
            Each job's granularity by id, ``None`` when no partitioned target
            resolves or the targets disagree.
        """
        if not job_ids:
            return {}
        with session_scope(self._engine) as session:
            granularities = self._job_target_granularities(session, job_ids)
        return {
            job_id: next(iter(shared)) if len(shared := granularities.get(job_id, set())) == 1 else None
            for job_id in job_ids
        }

    def _job_target_granularities(self, session: Session, job_ids: Sequence[UUID]) -> dict[UUID, set[TimeGranularity]]:
        """The granularities each job's partitioned targets declare, in three queries.

        The jobs' target relations, the target rows and their parents are each
        read in one query, whatever the number of jobs. A source target
        contributes its partitioned assets' granularities, an asset target
        its own; a target whose key does not resolve contributes nothing
        (drift is the run path's problem, not the scheduler's).

        Args:
            session: Open session to resolve the targets in.
            job_ids: UUIDs of the job components.

        Returns:
            Per job with at least one target relation, the distinct
            granularities its targets resolve to.
        """
        relations = session.exec(
            select(ComponentRelation).where(
                col(ComponentRelation.src_id).in_(job_ids), ComponentRelation.name == "targets"
            )
        ).all()
        targets = {
            row.id: row
            for row in session.exec(
                select(Component)
                .where(col(Component.id).in_({relation.dst_id for relation in relations}))
                .options(selectinload(Component.parent))  # ty: ignore[invalid-argument-type]
            ).all()
        }
        granularities: dict[UUID, set[TimeGranularity]] = {}
        for relation in relations:
            shared = granularities.setdefault(relation.src_id, set())
            if (target := targets.get(relation.dst_id)) is None:
                continue
            definition = self._catalog.get(target.qualified_key)
            if isinstance(definition, il.SourceDefinition):
                partitionings = definition.partitionings()
            elif isinstance(definition, il.AssetDefinition) and definition.partitioning is not None:
                partitionings = [definition.partitioning]
            else:
                partitionings = []
            shared.update(
                TimeGranularity(partitioning["granularity"])
                for partitioning in partitionings
                if partitioning.get("granularity") is not None
            )
        return granularities

    # -- Internals -------------------------------------------------------------

    @staticmethod
    def _load_component(
        session: Session, component_id: UUID, *, kind: str | None = None, org_id: UUID | None = None
    ) -> Component:
        """Fetch a component row with children and relations eager-loaded.

        Args:
            session: Open session to query in.
            component_id: The component UUID.
            kind: Kind the row must have (``None`` accepts any kind); a
                mismatch is reported as a missing row.
            org_id: Organisation the row must belong to (``None`` accepts
                any); a mismatch is reported as a missing row.

        Returns:
            The component row, safe to hand out detached.

        Raises:
            NotFoundError: If no row exists, or it has a different kind or
                organisation.
        """
        statement = select(Component).where(Component.id == component_id).options(*COMPONENT_LOAD_OPTIONS)
        db_component = session.exec(statement).first()
        if (
            not db_component
            or (kind is not None and db_component.kind != kind)
            or (org_id is not None and db_component.org_id != org_id)
        ):
            raise NotFoundError(f"{kind or 'component'} {component_id} not found".capitalize())
        return db_component
