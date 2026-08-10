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
from interloper.errors import (
    CatalogKeyError,
    ComponentDriftError,
    ConfigError,
    HydrationError,
    InUseError,
    NotFoundError,
)
from sqlalchemy.orm import selectinload
from sqlmodel import Session, col, select

from interloper_db.drift import ComponentStatus, asset_status, resolve_component_cls, resolve_source_cls, source_status
from interloper_db.models import Component, ComponentRelation
from interloper_db.store.quotas import check_asset_quota, check_source_quota
from interloper_db.store.relations import Binding, RelationMixin, _add_relation

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

class ComponentMixin(RelationMixin):
    """Store methods for component CRUD and hydration, on the relation layer."""

    # -- CRUD ------------------------------------------------------------------

    def create_component(
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
        """
        with self._session() as session:
            if kind == "source":
                check_source_quota(session, org_id, self._quota_defaults)
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
            self._sync_relations(session, db_component, relations)
            session.commit()
            return load_component(session, db_component.id)

    def get_component(self, component_id: UUID, *, kind: str | None = None) -> Component:
        """Load a component row by ID with relations eager-loaded.

        Args:
            component_id: The component UUID.
            kind: Optional kind assertion.

        Returns:
            The component row.

        Raises:
            NotFoundError: If no row exists (or it has a different kind).
        """
        with self._session() as session:
            return load_component(session, component_id, kind=kind)

    def list_components(self, org_id: UUID, *, kinds: list[str] | None = None) -> list[Component]:
        """List an organisation's components, optionally filtered by kind.

        Args:
            org_id: Organisation UUID.
            kinds: Kinds to include (``None`` = all).

        Returns:
            Eager-loaded component rows, oldest first.
        """
        with self._session() as session:
            statement = (
                select(Component)
                .where(Component.org_id == org_id)
                .options(*COMPONENT_LOAD_OPTIONS)
                .order_by(Component.created_at)  # ty: ignore[invalid-argument-type]
            )
            if kinds:
                statement = statement.where(col(Component.kind).in_(kinds))
            return list(session.exec(statement).all())

    def update_component(
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

        Returns:
            The updated component row, eager-loaded.

        Raises:
            NotFoundError: If the component is not found.
        """
        with self._session() as session:
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
            self._sync_relations(session, db_component, relations)
            session.commit()
            return load_component(session, component_id)

    def delete_component(self, component_id: UUID) -> None:
        """Delete a component. Children and out-bound relations cascade via FK.

        In-bound relations follow their declared ``on_delete`` semantics:
        consumption relations (a bound connection, a required dependency)
        block the deletion; orchestration pointers (a job's ``target``, a
        hook's ``watch``, optional dependency slots) detach — the relation
        row cascades away and the referrer keeps working with reduced scope.

        Raises:
            NotFoundError: If the component is not found.
            InUseError: If other components hold blocking relations into this
                one or its children — those must be unbound or deleted first.
            ValueError: If the component is source-owned (delete or update
                the parent source instead).
        """
        with self._session() as session:
            db_component = session.get(Component, component_id)
            if not db_component:
                raise NotFoundError(f"Component {component_id} not found")
            if db_component.parent_id is not None:
                raise ValueError("Cannot delete a source-owned asset directly. Delete or update the source instead.")
            if referrers := self._blocking_referrers(session, db_component):
                names = ", ".join(str(r["name"] or r["key"]) for r in referrers)
                raise InUseError(
                    f"Cannot delete {db_component.kind} '{db_component.name or db_component.key}': "
                    f"in use by {names}",
                    referrers=referrers,
                )
            session.delete(db_component)
            session.commit()

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
        """Blocking referrers whose relations point into *target_ids* from outside *subtree_ids*."""
        rows = session.exec(
            select(ComponentRelation).where(
                col(ComponentRelation.dst_id).in_(target_ids),
                col(ComponentRelation.src_id).not_in(subtree_ids),
            )
        ).all()
        referrers: dict[UUID, Component] = {}
        for relation in rows:
            src = session.get(Component, relation.src_id)
            if src is None or self._relation_detaches(session, src, relation):
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
        on any catalog drift.

        Returns:
            The reconstructed framework component.

        Raises:
            NotFoundError: If the component is not found.
            ComponentDriftError: If a catalog key no longer resolves.
            HydrationError: If reconstruction fails.
        """
        with self._session() as session:
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
            raise HydrationError(
                f"Failed to hydrate {db_component.kind} '{db_component.key}' ({db_component.id}): {e}"
            ) from e

    def _load_owned_asset(self, parent_id: UUID, key: str, asset_id: UUID) -> il.Asset:
        """Hydrate a source-owned asset through its parent source.

        The parent source is the unit of reconstruction — loading it binds
        all its assets — and the child is picked out by key.

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
            f"Asset '{key}' ({asset_id}) is no longer declared "
            f"by source '{source.key}'; its catalog key has drifted."
        )

    def component_status(self, db_component: Component) -> ComponentStatus:
        """Catalog-resolution status of a component row (drift detection).

        Source-owned assets resolve through their parent, so ``parent`` must
        be loaded (it is on rows returned by this mixin).
        """
        if db_component.kind == "asset":
            source_key = db_component.parent.key if db_component.parent else None
            return asset_status(self._catalog, db_component.key, source_key=source_key)
        return source_status(self._catalog, db_component.key)

    def decode_config(self, db_component: Component) -> dict[str, Any]:
        """The component's config payload, decrypting secret kinds.

        Returns:
            The decoded configuration dict.
        """
        if il.KINDS[db_component.kind].sensitive:
            return self._hydrator.decode_data(db_component)
        return dict(db_component.config or {})

    # -- Kind semantics --------------------------------------------------------

    def _apply_config(self, db_component: Component, config: dict[str, Any] | None, encrypted: bool | None) -> None:
        """Write the config payload onto the row, encrypting secret kinds."""
        if il.KINDS[db_component.kind].sensitive:
            db_component.data, db_component.encrypted = self._encode_data(config or {}, encrypted)
            db_component.config = None
        else:
            db_component.config = config

    def _encode_data(self, data: dict[str, Any], encrypted: bool | None) -> tuple[bytes, bool]:
        """Serialise a secret payload and encrypt it according to ``encrypted``.

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
        """
        if explicit_rename:
            return
        old_default = self._derived_name(db_component, self._current_config(db_component))
        if db_component.name is None or db_component.name == old_default:
            db_component.name = self._derived_name(db_component, new_config) or db_component.name

    def _current_config(self, db_component: Component) -> dict[str, Any] | None:
        """The row's stored config payload, decoding secret kinds (``None`` when undecodable)."""
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
        # An explicit list is the exact child set; None (creation) is all
        # catalog assets.
        target = set(child_keys) if child_keys is not None else all_keys
        check_asset_quota(session, db_source, len(target), self._quota_defaults)
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
                # Cross-source deps are never auto-wired; nor self-edges.
                if expected.source_key != source_key or expected.asset_key == asset_key:
                    continue
                if expected.asset_key not in children_by_key or (child.id, param_name) in bound:
                    continue
                _add_relation(session, child, children_by_key[expected.asset_key], "dependency", param_name)

    def _row_status(self, session: Session, db_component: Component) -> ComponentStatus:
        """Row drift status inside an open session (parent fetched on demand)."""
        if db_component.kind == "asset" and db_component.parent_id is not None:
            parent = session.get(Component, db_component.parent_id)
            return asset_status(self._catalog, db_component.key, source_key=parent.key if parent else None)
        return source_status(self._catalog, db_component.key)

    def _check_job_targets(self, session: Session, db_job: Component) -> None:
        """Fail closed when any job target's catalog key has drifted."""
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


# -- Session-level helpers (shared with tests and internal callers) ------------


def load_component(session: Session, component_id: UUID, *, kind: str | None = None) -> Component:
    """Fetch a component row with children and relations eager-loaded.

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


def stamp_component_state(db_component: Component, **fields: Any) -> None:
    """Merge machine-owned state fields onto a component row (spec untouched).

    Datetimes are written in the canonical timezone-aware ISO form (so
    lexicographic comparison in SQL stays chronological); the merged payload
    is validated against the kind's ``state_model`` — shape only, stored
    strings are never rewritten. The caller owns the session and commit.
    """
    import datetime as dt

    state = dict(db_component.state or {})
    for key, value in fields.items():
        state[key] = value.isoformat() if isinstance(value, dt.datetime) else value
    model = il.KINDS[db_component.kind].state_model
    if model is not None:
        model.model_validate(state)
    db_component.state = state
