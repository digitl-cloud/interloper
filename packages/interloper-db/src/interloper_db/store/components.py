"""Component persistence: one generic surface for every kind.

CRUD, relations, and hydration are kind-agnostic; the semantics a kind
genuinely owns are applied where the row's ``kind`` demands them:

- **secret kinds** (connection/config/resource): the ``config`` payload is
  encrypted into the ``data`` column (fail-closed without a key) and decoded
  on read — callers only ever see ``config``.
- **source**: child asset rows are kept in sync with the catalog class's
  ``asset_types`` after every write, including intra-source dependency wiring.
- **asset**: a source-owned asset hydrates through its parent; its drift
  status cascades through the parent's.
- **job**: hydration drift-checks every target before reconstruction.

Relation reads and writes live in the :class:`RelationMixin` layer this
mixin builds on — see :mod:`interloper_db.store.relations`.
"""

from __future__ import annotations

import json
from typing import Any, cast
from uuid import UUID

import interloper as il
from interloper.asset.base import AssetIdentity
from interloper.catalog.base import Catalog
from interloper.errors import (
    CatalogKeyError,
    ComponentDriftError,
    ConfigError,
    HydrationError,
    InUseError,
    NotFoundError,
    format_exception,
)
from interloper.partitioning.time import TimeGranularity
from interloper.telemetry import attributes
from interloper.telemetry.tracer import tracer
from sqlalchemy import Engine
from sqlalchemy.orm import selectinload
from sqlmodel import Session, col, select

from interloper_db.catalog import resolve_component_cls, resolve_source_cls
from interloper_db.models import Component, ComponentRelation
from interloper_db.session import commit, session_scope
from interloper_db.store.drift import ComponentStatus, asset_status, source_status
from interloper_db.store.hydration import Hydrator
from interloper_db.store.quotas import QUOTA_MAX_ASSETS_PER_SOURCE, QUOTA_MAX_SOURCES, QuotaService
from interloper_db.store.relations import Binding, RelationStore, _add_relation

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


class ComponentStore:
    """Store methods for component CRUD and hydration, on the relation layer."""

    def __init__(
        self,
        engine: Engine,
        catalog: Catalog,
        hydrator: Hydrator,
        encrypt: Any,
        quotas: QuotaService,
        relations: RelationStore,
    ) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
            catalog: Catalog its component keys resolve against.
            hydrator: Hydrator that turns rows back into framework objects.
            encrypt: Callable that encrypts a resource payload, or None for plaintext.
            quotas: Quota gates it enforces through.
            relations: Relation facet it wires component edges through.
        """
        self._engine = engine
        self._catalog = catalog
        self._hydrator = hydrator
        self._encrypt = encrypt
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
        children: list[str] | None = None,
        relations: dict[str, list[Binding]] | None = None,
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
            relations: ``{type: [(dst_id, slot), …]}`` — synced per type.

        Returns:
            The created component row, eager-loaded.

        Raises:
            ConfigError: If ``children`` is passed for a kind that has none.
        """
        with session_scope(self._engine) as session:
            if kind == "source":
                self._quotas.check(org_id, QUOTA_MAX_SOURCES)
            db_component = Component(org_id=org_id, kind=kind, key=key, name=name)
            self._apply_config(db_component, config, encrypted)
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
            commit(session)
            return self.load_component(session, db_component.id)

    def get(self, component_id: UUID, *, kind: str | None = None) -> Component:
        """Load a component row by ID with relations eager-loaded.

        Args:
            component_id: The component UUID.
            kind: Kind the row must have (``None`` accepts any kind); a
                mismatch is reported as a missing row.

        Returns:
            The component row, eager-loaded and safe to hand out detached.
        """
        with session_scope(self._engine) as session:
            return self.load_component(session, component_id, kind=kind)

    def list_all(self, org_id: UUID, *, kinds: list[str] | None = None) -> list[Component]:
        """List an organisation's components, optionally filtered by kind.

        Args:
            org_id: Organisation UUID.
            kinds: Kinds to include (``None`` = all).

        Returns:
            Eager-loaded component rows, oldest first.
        """
        with session_scope(self._engine) as session:
            statement = (
                select(Component)
                .where(Component.org_id == org_id)
                .options(*COMPONENT_LOAD_OPTIONS)
                .order_by(Component.created_at)  # ty: ignore[invalid-argument-type]
            )
            if kinds:
                statement = statement.where(col(Component.kind).in_(kinds))
            return list(session.exec(statement).all())

    def update(
        self,
        component_id: UUID,
        *,
        name: str | None = None,
        config: dict[str, Any] | None = None,
        encrypted: bool | None = None,
        children: list[str] | None = None,
        relations: dict[str, list[Binding]] | None = None,
    ) -> Component:
        """Update a component's spec. ``None`` leaves a facet untouched.

        Passing ``children`` makes a source's child asset set exactly that
        list; omitting it leaves the current set as is. The machine-owned
        ``state`` column is never touched here.

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
            relations: ``{type: [(dst_id, slot), …]}`` — synced per type.

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
                self._apply_config(db_component, config, encrypted)
            if db_component.kind == "source":
                self._check_source_collision(session, db_component)
                if children is not None:
                    self._ensure_children(session, db_component, children)
            elif children is not None:
                raise ConfigError(f"Components of kind '{db_component.kind}' have no children")
            self._relations._sync_relations(session, db_component, relations)
            commit(session)
            return self.load_component(session, component_id)

    def delete(self, component_id: UUID) -> None:
        """Delete a component. Children and out-bound relations cascade via FK.

        In-bound relations follow their declared ``on_delete`` semantics:
        consumption relations (a bound connection, a required dependency)
        block the deletion; orchestration pointers (a job's ``target``, a
        hook's ``watch``, optional dependency slots) detach — the relation
        row cascades away and the referrer keeps working with reduced scope.

        Args:
            component_id: The component UUID.

        Raises:
            NotFoundError: If the component is not found.
            InUseError: If other components hold blocking relations into this
                one or its children — those must be unbound or deleted first.
            ValueError: If the component is source-owned (delete or update
                the parent source instead).
        """
        with session_scope(self._engine) as session:
            db_component = session.get(Component, component_id)
            if not db_component:
                raise NotFoundError(f"Component {component_id} not found")
            if db_component.parent_id is not None:
                raise ValueError("Cannot delete a source-owned asset directly. Delete or update the source instead.")
            if referrers := self._blocking_referrers(session, db_component):
                names = ", ".join(str(r["name"] or r["key"]) for r in referrers)
                raise InUseError(
                    f"Cannot delete {db_component.kind} '{db_component.name or db_component.key}': in use by {names}",
                    referrers=referrers,
                )
            session.delete(db_component)
            commit(session)

    def _blocking_referrers(self, session: Session, db_component: Component) -> list[dict[str, str | None]]:
        """Components outside a component's subtree whose relations into it block deletion.

        Deleting a relation destination cascades the binding row, which would
        leave a *consuming* referrer silently broken at its next run — those
        relations refuse the deletion. Relations whose vocabulary declares
        ``on_delete="detach"`` (and optional slots) are skipped: cascading
        them is the intended outcome. Relations internal to the subtree (a
        source's own asset dependencies) don't count, and a referrer that is
        a source-owned asset is reported as its parent source — the unit the
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
        return self._blocking_referrers_into(session, subtree_ids, subtree_ids)

    def _blocking_referrers_into(
        self, session: Session, target_ids: set[UUID], subtree_ids: set[UUID]
    ) -> list[dict[str, str | None]]:
        """Blocking referrers whose relations point into *target_ids* from outside *subtree_ids*.

        Args:
            session: Open session to query in.
            target_ids: Component IDs whose in-bound relations are inspected.
            subtree_ids: Component IDs that count as "inside" — relations
                originating there are ignored.

        Returns:
            One ``{id, kind, key, name}`` dict per blocking referrer, sorted by
            display name; empty when nothing blocks.
        """
        rows = session.exec(
            select(ComponentRelation).where(
                col(ComponentRelation.dst_id).in_(target_ids),
                col(ComponentRelation.src_id).not_in(subtree_ids),
            )
        ).all()
        referrers: dict[UUID, Component] = {}
        for relation in rows:
            src = session.get(Component, relation.src_id)
            if src is None or self._relations._relation_detaches(session, src, relation):
                continue
            if src.parent_id is not None and src.parent_id not in subtree_ids:
                src = session.get(Component, src.parent_id) or src
            referrers[src.id] = src
        return [
            {"id": str(c.id), "kind": c.kind, "key": c.key, "name": c.name}
            for c in sorted(referrers.values(), key=lambda c: ((c.name or c.key).lower(), str(c.id)))
        ]

    # -- Hydration & status ----------------------------------------------------

    def load(self, component_id: UUID) -> il.Component:
        """Hydrate a framework component of any kind from its row.

        Source-owned assets hydrate through their parent source and are
        extracted from it; jobs drift-check every target first. Fails closed
        on any catalog drift: see :meth:`_load` for what a missing row, a
        drifted key or a failed reconstruction raises.

        Args:
            component_id: The component UUID.

        Returns:
            The reconstructed framework component.
        """
        with tracer().start_as_current_span(
            "interloper.store.load", attributes={attributes.TARGET_ID: str(component_id)}
        ):
            return self._load(component_id)

    def _load(self, component_id: UUID) -> il.Component:
        """Hydrate a component row (the traced body of :meth:`load`).

        Args:
            component_id: The component UUID.

        Returns:
            The reconstructed framework component.

        Raises:
            NotFoundError: If the component is not found.
            ComponentDriftError: If a catalog key no longer resolves.
            HydrationError: If reconstruction fails.
        """
        with session_scope(self._engine) as session:
            db_component = session.get(Component, component_id)
            if not db_component:
                raise NotFoundError(f"Component {component_id} not found")
            owned_asset = db_component.kind == "asset" and db_component.parent_id is not None
            if not owned_asset:
                status = self._row_status(session, db_component)
                if status is not ComponentStatus.OK:
                    raise ComponentDriftError(
                        f"{db_component.kind.capitalize()} '{db_component.key}' ({db_component.id}) "
                        f"cannot be hydrated: its catalog key is {status.value}."
                    )
                if db_component.kind == "job":
                    self._check_job_targets(session, db_component)
                spec = self._hydrator.build_component_spec(session, db_component)

        # Reconstruction happens outside the session: it imports classes and,
        # for owned assets, recursively loads the parent source.
        if owned_asset:
            return self._load_owned_asset(db_component.parent_id, db_component.key, component_id)
        try:
            return il.Component.from_spec(spec)
        except Exception as e:
            # format_exception, never str(e): a ValidationError here carries the
            # decrypted payload of sensitive kinds in its input_value dumps, and
            # this message is persisted into run events and shown in the UI.
            raise HydrationError(
                f"Failed to hydrate {db_component.kind} '{db_component.key}' ({db_component.id}): {format_exception(e)}"
            ) from e

    def _load_owned_asset(self, parent_id: UUID, key: str, asset_id: UUID) -> il.Asset:
        """Hydrate a source-owned asset through its parent source.

        The parent source is the unit of reconstruction — loading it binds
        all its assets — and the child is picked out by key.

        Args:
            parent_id: UUID of the owning source component.
            key: Catalog key of the asset to pick out of the source.
            asset_id: UUID of the asset row, for the drift error message.

        Returns:
            The bound asset instance.

        Raises:
            ComponentDriftError: If the source no longer declares the key.
        """
        source = cast(il.Source, self.load(parent_id))
        for asset in source.assets:
            if asset.key == key:
                return asset
        raise ComponentDriftError(
            f"Asset '{key}' ({asset_id}) is no longer declared by source '{source.key}'; its catalog key has drifted."
        )

    def status(self, db_component: Component) -> ComponentStatus:
        """Catalog-resolution status of a component row (drift detection).

        Source-owned assets resolve through their parent, so ``parent`` must
        be loaded (it is on rows returned by this mixin).

        Args:
            db_component: The row to resolve against the catalog.

        Returns:
            ``OK``, ``DISABLED`` or ``MISSING`` for the row's catalog key.
        """
        if db_component.kind == "asset":
            source_key = db_component.parent.key if db_component.parent else None
            return asset_status(self._catalog, db_component.key, source_key=source_key)
        return source_status(self._catalog, db_component.key)

    def decode_config(self, db_component: Component) -> dict[str, Any]:
        """The component's config payload, decrypting secret kinds.

        Args:
            db_component: The row to read the payload from.

        Returns:
            The decoded configuration dict (empty when the row carries none).
        """
        if il.KINDS[db_component.kind].sensitive:
            return self._hydrator.decode_data(db_component)
        return dict(db_component.config or {})

    # -- Kind semantics --------------------------------------------------------

    def _apply_config(self, db_component: Component, config: dict[str, Any] | None, encrypted: bool | None) -> None:
        """Write the config payload onto the row, encrypting secret kinds.

        Args:
            db_component: The row to write onto.
            config: The payload to store (``None`` stores an empty payload).
            encrypted: Secret kinds only — ``True``/``None`` (default) encrypt,
                ``False`` opts into plaintext storage.
        """
        if il.KINDS[db_component.kind].sensitive:
            db_component.data, db_component.encrypted = self._encode_data(config or {}, encrypted)
            db_component.config = None
        else:
            db_component.config = config

    def _encode_data(self, data: dict[str, Any], encrypted: bool | None) -> tuple[bytes, bool]:
        """Serialise a secret payload and encrypt it according to ``encrypted``.

        Args:
            data: The payload to serialise.
            encrypted: ``True``/``None`` (default) encrypt, ``False`` opts into
                plaintext storage.

        Returns:
            A ``(blob, encrypted)`` tuple: the bytes to persist and whether
            they are encrypted.

        Raises:
            ConfigError: If encryption is required (the default, or an explicit
                ``True``) but no encryption key is configured. Fails closed so
                secrets are never silently written in plaintext.
        """
        should_encrypt = True if encrypted is None else encrypted
        raw = json.dumps(data).encode()
        if should_encrypt:
            if not self._encrypt:
                raise ConfigError(
                    "Refusing to store a resource without encryption at rest: "
                    "INTERLOPER_ENCRYPTION_KEY is not configured. Set it, or pass "
                    "encrypted=false to store this resource in plaintext."
                )
            raw = self._encrypt(raw)
        return raw, should_encrypt

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
        cls = resolve_component_cls(self._catalog, db_component.key)
        if cls is None or cls.kind != db_component.kind:
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

    def _current_config(self, db_component: Component) -> dict[str, Any] | None:
        """The row's stored config payload, decoding secret kinds.

        Args:
            db_component: The row to read the payload from.

        Returns:
            The decoded configuration dict, or ``None`` when it can't be
            decoded (no cipher configured, or a corrupt payload).
        """
        try:
            return self.decode_config(db_component)
        except Exception:  # noqa: BLE001 — no cipher / corrupt payload: treat as underivable
            return None

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
        source_cls = resolve_source_cls(self._catalog, db_source.key)
        if source_cls is None:
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
                dataset, table = sorted(overlap)[0]
                raise ConfigError(
                    f"Source '{db_source.key}' already has an instance materializing to '{dataset}.{table}'. "
                    f"Configure a distinct discriminator (or dataset) so the two don't overwrite each other's data."
                )

    def _ensure_children(self, session: Session, db_source: Component, child_keys: list[str] | None) -> None:
        """Sync a source's child asset rows to match the desired set.

        When ``child_keys`` is provided, only those assets will exist —
        missing ones are created, extra ones are removed. Removal follows the
        delete guard's semantics: blocking relations from outside the source
        (a required cross-source dependency) raise ``InUseError``; detaching
        ones and intra-source edges cascade. ``None`` is the source-creation
        default and enables every asset the catalog class declares. Existing
        rows keep their IDs (and therefore their cross-source deps, event
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
        source_cls = resolve_source_cls(self._catalog, db_source.key)
        if source_cls is None:
            raise CatalogKeyError(f"Unknown source key: {db_source.key}")
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
            if referrers := self._blocking_referrers_into(session, removed_ids, subtree_ids):
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

        self._wire_intra_deps(session, source_cls, children)

    @staticmethod
    def _wire_intra_deps(
        session: Session,
        source_cls: type[il.Source],
        children_by_key: dict[str, Component],
    ) -> None:
        """Top up missing intra-source dependency relations from class metadata.

        Idempotent over the full child set — assets enabled after their
        siblings still get the edges *into* them wired. Slots that already
        hold an edge are never touched, so manual bindings survive.

        Args:
            session: Open session the relations are added to.
            source_cls: The catalog class declaring the dependencies.
            children_by_key: The source's enabled child rows, keyed by asset key.
        """
        source_key = source_cls.key
        child_ids = [child.id for child in children_by_key.values()]
        bound = {
            (row[0], row[1])
            for row in session.exec(
                select(ComponentRelation.src_id, ComponentRelation.slot).where(
                    col(ComponentRelation.src_id).in_(child_ids),
                    ComponentRelation.type == "dependency",
                )
            ).all()
        }
        for asset_type in source_cls.asset_types:
            asset_key = asset_type.key
            child = children_by_key.get(asset_key)
            if child is None:
                continue
            all_requires = {**asset_type.requires, **asset_type.optional_requires}
            for param_name, declared_key in all_requires.items():
                expected = AssetIdentity.resolve(declared_key, own_source_key=source_key)
                if expected.source_key != source_key or expected.asset_key == asset_key:
                    continue
                if expected.asset_key not in children_by_key or (child.id, param_name) in bound:
                    continue
                _add_relation(session, child, children_by_key[expected.asset_key], "dependency", param_name)

    def _row_status(self, session: Session, db_component: Component) -> ComponentStatus:
        """Row drift status inside an open session (parent fetched on demand).

        Args:
            session: Open session used to fetch an owned asset's parent.
            db_component: The row to resolve against the catalog.

        Returns:
            ``OK``, ``DISABLED`` or ``MISSING`` for the row's catalog key.
        """
        if db_component.kind == "asset" and db_component.parent_id is not None:
            parent = session.get(Component, db_component.parent_id)
            return asset_status(self._catalog, db_component.key, source_key=parent.key if parent else None)
        return source_status(self._catalog, db_component.key)

    def job_partition_granularity(self, session: Session, job_id: UUID) -> TimeGranularity | None:
        """Resolve the granularity a job's partitioned targets share.

        Granularity lives on the target assets' catalog definitions, never on
        the job's config (a denormalized copy could silently drift from the
        catalog). A source target contributes the granularities of its
        partitioned assets; an owned-asset target is looked up inside its
        parent source's definition.

        Args:
            session: Open session to resolve the targets in.
            job_id: UUID of the job component.

        Returns:
            The single granularity, or ``None`` when no partitioned target
            resolves (the caller decides the fallback).

        Raises:
            ValueError: If the targets disagree on granularity — scheduling a
                window would be wrong for some of them, so fail closed.
        """
        granularities: set[TimeGranularity] = set()
        targets = session.exec(
            select(ComponentRelation).where(ComponentRelation.src_id == job_id, ComponentRelation.type == "target")
        ).all()
        for relation in targets:
            target = session.get(Component, relation.dst_id)
            if target is None:
                continue
            for partitioning in self._target_partitionings(session, target):
                if (granularity := partitioning.get("granularity")) is not None:
                    granularities.add(TimeGranularity(granularity))
        if len(granularities) > 1:
            names = ", ".join(sorted(g.value for g in granularities))
            raise ValueError(f"Job targets disagree on partition granularity ({names})")
        return next(iter(granularities), None)

    def _target_partitionings(self, session: Session, target: Component) -> list[dict[str, Any]]:
        """The partitioning dicts of one target's partitioned assets.

        Args:
            session: Open session used to fetch an owned asset's parent.
            target: The job target row (a source or an asset).

        Returns:
            One dict per partitioned asset the target resolves to; empty when
            the catalog key does not resolve (drift is the run path's problem,
            not the scheduler's).
        """
        if target.kind == "source":
            defn = self._catalog.get(target.key)
            assets = getattr(defn, "assets", None) or []
            return [a.partitioning for a in assets if a.partitioning is not None]
        if target.kind == "asset":
            if target.parent_id is not None:
                parent = session.get(Component, target.parent_id)
                defn = self._catalog.get(parent.key) if parent else None
                assets = getattr(defn, "assets", None) or []
                return [a.partitioning for a in assets if a.key == target.key and a.partitioning is not None]
            defn = self._catalog.get(target.key)
            partitioning = getattr(defn, "partitioning", None)
            return [partitioning] if partitioning is not None else []
        return []

    def _check_job_targets(self, session: Session, db_job: Component) -> None:
        """Fail closed when any job target's catalog key has drifted.

        Args:
            session: Open session to resolve the targets in.
            db_job: The job row being hydrated.

        Raises:
            ComponentDriftError: If a target's catalog key is disabled or missing.
        """
        targets = session.exec(
            select(ComponentRelation).where(ComponentRelation.src_id == db_job.id, ComponentRelation.type == "target")
        ).all()
        for relation in targets:
            target = session.get(Component, relation.dst_id)
            if target is None:
                continue  # defensive: FKs make this unreachable
            status = self._row_status(session, target)
            if status is not ComponentStatus.OK:
                raise ComponentDriftError(
                    f"Job '{db_job.name}' ({db_job.id}) cannot be hydrated: target "
                    f"{target.kind} '{target.key}' ({target.id}) is {status.value}."
                )

    # -- Internals -------------------------------------------------------------

    @staticmethod
    def load_component(session: Session, component_id: UUID, *, kind: str | None = None) -> Component:
        """Fetch a component row with children and relations eager-loaded.

        Args:
            session: Open session to query in.
            component_id: The component UUID.
            kind: Kind the row must have (``None`` accepts any kind); a
                mismatch is reported as a missing row.

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


# -- Component state -----------------------------------------------------------
