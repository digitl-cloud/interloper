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

Relation writes are validated against the kind's class-declared relation
vocabulary (``Component.relation_types``) and stamped with the denormalized
``org_id``/``src_kind``/``dst_kind`` triple the composite foreign keys verify.
"""

from __future__ import annotations

import json
from typing import Any, cast
from uuid import UUID

import interloper as il
from interloper.errors import (
    CatalogKeyError,
    ComponentDriftError,
    ConfigError,
    HydrationError,
    NotFoundError,
)
from sqlalchemy.orm import selectinload
from sqlmodel import Session, col, select

from interloper_db.drift import ComponentStatus, asset_status, resolve_source_cls, source_status
from interloper_db.engine import get_engine
from interloper_db.hydration import Hydrator
from interloper_db.models import Component, ComponentRelation

# One relation binding: (destination component id, slot). Slot is "" for
# slotless relation types.
Binding = tuple[UUID, str]

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

class ComponentMixin:
    """Store methods for component CRUD, relations, and hydration."""

    _encrypt: Any
    _decrypt: Any
    _hydrator: Hydrator
    _catalog: il.Catalog

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

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
        with Session(get_engine()) as session:
            db_component = Component(org_id=org_id, kind=kind, key=key, name=name)
            self._apply_config(db_component, config, encrypted)
            session.add(db_component)
            session.flush()
            if kind == "source":
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
        with Session(get_engine()) as session:
            return load_component(session, component_id, kind=kind)

    def list_components(self, org_id: UUID, *, kinds: list[str] | None = None) -> list[Component]:
        """List an organisation's components, optionally filtered by kind.

        Args:
            org_id: Organisation UUID.
            kinds: Kinds to include (``None`` = all).

        Returns:
            Eager-loaded component rows, oldest first.
        """
        with Session(get_engine()) as session:
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

        Source kinds always top up newly available catalog assets; passing
        ``children`` makes the child set exactly that list. The machine-owned
        ``state`` column is never touched here.

        Returns:
            The updated component row, eager-loaded.

        Raises:
            NotFoundError: If the component is not found.
        """
        with Session(get_engine()) as session:
            db_component = session.get(Component, component_id)
            if not db_component:
                raise NotFoundError(f"Component {component_id} not found")
            if name is not None:
                db_component.name = name
            if config is not None:
                self._apply_config(db_component, config, encrypted)
            if db_component.kind == "source":
                self._ensure_children(session, db_component, children)
            elif children is not None:
                raise ConfigError(f"Components of kind '{db_component.kind}' have no children")
            self._sync_relations(session, db_component, relations)
            session.commit()
            return load_component(session, component_id)

    def delete_component(self, component_id: UUID) -> None:
        """Delete a component. Children and relations cascade via FK.

        Raises:
            NotFoundError: If the component is not found.
            ValueError: If the component is source-owned (delete or update
                the parent source instead).
        """
        with Session(get_engine()) as session:
            db_component = session.get(Component, component_id)
            if not db_component:
                raise NotFoundError(f"Component {component_id} not found")
            if db_component.parent_id is not None:
                raise ValueError("Cannot delete a source-owned asset directly. Delete or update the source instead.")
            session.delete(db_component)
            session.commit()

    # ------------------------------------------------------------------
    # Relations
    # ------------------------------------------------------------------

    def list_relations(self, org_id: UUID, *, type: str | None = None) -> list[ComponentRelation]:
        """List an organisation's component relations, optionally by type."""
        with Session(get_engine()) as session:
            statement = select(ComponentRelation).where(ComponentRelation.org_id == org_id)
            if type:
                statement = statement.where(ComponentRelation.type == type)
            return list(session.exec(statement).all())

    def add_relation(self, component_id: UUID, *, type: str, dst_id: UUID, slot: str = "") -> ComponentRelation:
        """Add one relation from a component.

        Returns:
            The created relation.

        Raises:
            NotFoundError: If either endpoint is missing or cross-org.
            ConfigError: If the kind's vocabulary doesn't declare the type.
        """
        with Session(get_engine()) as session:
            src = session.get(Component, component_id)
            if not src:
                raise NotFoundError(f"Component {component_id} not found")
            self._check_vocabulary(src.kind, type)
            relation = _add_relation(session, src, self._resolve_dst(session, src, type, slot, dst_id), type, slot)
            session.commit()
            session.refresh(relation)
            return relation

    def remove_relation(self, component_id: UUID, *, type: str, dst_id: UUID) -> None:
        """Remove a component's relations of one type toward one destination."""
        with Session(get_engine()) as session:
            statement = select(ComponentRelation).where(
                ComponentRelation.src_id == component_id,
                ComponentRelation.type == type,
                ComponentRelation.dst_id == dst_id,
            )
            for relation in session.exec(statement).all():
                session.delete(relation)
            session.commit()

    # ------------------------------------------------------------------
    # Hydration & status
    # ------------------------------------------------------------------

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
        with Session(get_engine()) as session:
            db_component = session.get(Component, component_id)
            if not db_component:
                raise NotFoundError(f"Component {component_id} not found")

            if db_component.kind == "asset" and db_component.parent_id is not None:
                parent_id, child_key, child_id = db_component.parent_id, db_component.key, db_component.id

        if db_component.kind == "asset" and db_component.parent_id is not None:
            source = cast(il.Source, self.load(parent_id))
            for asset in source.assets:
                if type(asset).key == child_key:
                    return asset
            raise ComponentDriftError(
                f"Asset '{child_key}' ({child_id}) is no longer declared "
                f"by source '{type(source).key}'; its catalog key has drifted."
            )

        with Session(get_engine()) as session:
            db_component = session.get(Component, component_id)
            assert db_component is not None  # checked above
            status = self._row_status(session, db_component)
            if status is not ComponentStatus.OK:
                raise ComponentDriftError(
                    f"{db_component.kind.capitalize()} '{db_component.key}' ({db_component.id}) "
                    f"cannot be hydrated: its catalog key is {status.value}."
                )
            if db_component.kind == "job":
                self._check_job_targets(session, db_component)
            spec = self._hydrator.build_component_spec(session, db_component)

        try:
            return spec.reconstruct()
        except Exception as e:
            raise HydrationError(
                f"Failed to hydrate {db_component.kind} '{db_component.key}' ({db_component.id}): {e}"
            ) from e

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
        if il.KINDS.sensitive(db_component.kind):
            return self._hydrator.decode_data(db_component)
        return dict(db_component.config or {})

    # ------------------------------------------------------------------
    # Kind semantics
    # ------------------------------------------------------------------

    def _apply_config(self, db_component: Component, config: dict[str, Any] | None, encrypted: bool | None) -> None:
        """Write the config payload onto the row, encrypting secret kinds."""
        if il.KINDS.sensitive(db_component.kind):
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

    def _ensure_children(self, session: Session, db_source: Component, child_keys: list[str] | None) -> None:
        """Sync a source's child asset rows to match the desired set.

        When ``child_keys`` is provided, only those assets will exist —
        missing ones are created, extra ones are removed (their relations,
        including dependencies pointing at them, cascade). When ``None``,
        existing rows are kept and newly available catalog assets are added.
        Existing rows keep their IDs (and therefore their cross-source deps,
        event references, and per-asset overrides).
        """
        source_cls = resolve_source_cls(self._catalog, db_source.key)
        if source_cls is None:
            raise CatalogKeyError(f"Unknown source key: {db_source.key}")
        all_keys = {asset_type.key for asset_type in source_cls.asset_types}
        desired = set(child_keys) & all_keys if child_keys is not None else None

        existing = session.exec(select(Component).where(Component.parent_id == db_source.id)).all()
        existing_by_key = {child.key: child for child in existing}

        if desired is not None:
            for key in set(existing_by_key) - desired:
                session.delete(existing_by_key[key])
            session.flush()

        keys_to_ensure = desired if desired is not None else all_keys

        added_keys: set[str] = set()
        children_by_key: dict[str, Component] = {
            key: child for key, child in existing_by_key.items() if key in keys_to_ensure
        }
        for asset_type in source_cls.asset_types:
            if asset_type.key in keys_to_ensure and asset_type.key not in existing_by_key:
                child = Component(org_id=db_source.org_id, kind="asset", key=asset_type.key, parent_id=db_source.id)
                session.add(child)
                children_by_key[asset_type.key] = child
                added_keys.add(asset_type.key)
        session.flush()

        if added_keys:
            self._wire_intra_deps(session, source_cls, children_by_key, only_for=added_keys)

    @staticmethod
    def _wire_intra_deps(
        session: Session,
        source_cls: type[il.Source],
        children_by_key: dict[str, Component],
        only_for: set[str],
    ) -> None:
        """Persist intra-source dependency relations from class metadata."""
        source_key = source_cls.key
        for asset_type in source_cls.asset_types:
            asset_key = asset_type.key
            if asset_key not in only_for or asset_key not in children_by_key:
                continue
            all_requires = {**asset_type.requires, **asset_type.optional_requires}
            for param_name, required_qualified_key in all_requires.items():
                # Only wire intra-source deps (qualified key belongs to this source)
                if "." in required_qualified_key:
                    qualified_source, qualified_asset = required_qualified_key.split(".", 1)
                    if qualified_source != source_key:
                        continue
                else:
                    qualified_asset = required_qualified_key
                if qualified_asset in children_by_key:
                    _add_relation(
                        session, children_by_key[asset_key], children_by_key[qualified_asset], "dependency", param_name
                    )

    def _sync_relations(
        self,
        session: Session,
        src: Component,
        relations: dict[str, list[Binding]] | None,
    ) -> None:
        """Replace the relation types present in *relations* (empty list clears)."""
        for type_, bindings in (relations or {}).items():
            self._check_vocabulary(src.kind, type_)
            existing = session.exec(
                select(ComponentRelation).where(ComponentRelation.src_id == src.id, ComponentRelation.type == type_)
            ).all()
            for relation in existing:
                session.delete(relation)
            session.flush()
            for dst_id, slot in bindings:
                _add_relation(session, src, self._resolve_dst(session, src, type_, slot, dst_id), type_, slot)

    @staticmethod
    def _check_vocabulary(kind: str, type_: str) -> None:
        """Reject relation types the kind's class vocabulary doesn't declare."""
        vocabulary = il.KINDS.relation_types(kind)
        if type_ not in vocabulary:
            raise ConfigError(
                f"Components of kind '{kind}' declare no '{type_}' relations "
                f"(allowed: {sorted(vocabulary) or 'none'})"
            )

    @staticmethod
    def _resolve_dst(session: Session, src: Component, type_: str, slot: str, dst_id: UUID) -> Component:
        """Resolve a relation destination, enforcing existence and same-org."""
        dst = session.get(Component, dst_id)
        if dst is None or dst.org_id != src.org_id:
            raise NotFoundError(f"Component {dst_id} not found (relation '{type_}'{f'/{slot}' if slot else ''})")
        return dst

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


# ----------------------------------------------------------------------
# Session-level helpers (shared with tests and internal callers)
# ----------------------------------------------------------------------


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


def _add_relation(session: Session, src: Component, dst: Component, type_: str, slot: str = "") -> ComponentRelation:
    """Add one relation, stamping the denormalized org/kind triple from the rows.

    Returns:
        The pending relation (added to the session, not flushed).
    """
    relation = ComponentRelation(
        src_id=src.id,
        type=type_,
        slot=slot,
        dst_id=dst.id,
        org_id=src.org_id,
        src_kind=src.kind,
        dst_kind=dst.kind,
    )
    session.add(relation)
    return relation
