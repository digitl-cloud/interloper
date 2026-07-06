"""Source persistence: CRUD and hydration.

A source is a component row; its assets are child component rows
(``parent_id``), kept in sync with the catalog class's ``asset_types``.
Deleting a source cascades to its children and every relation touching them.
"""

from __future__ import annotations

from typing import Any, cast
from uuid import UUID

import interloper as il
from interloper.errors import CatalogKeyError, ComponentDriftError, HydrationError, NotFoundError
from sqlmodel import Session, select

from interloper_db.drift import ComponentStatus, resolve_source_cls, source_status
from interloper_db.engine import get_engine
from interloper_db.hydration import Hydrator
from interloper_db.models import Component, ComponentRelation
from interloper_db.store.components import add_relation, list_components, load_component, sync_relations


class SourceMixin:
    """Store methods for source lifecycle and hydration."""

    _hydrator: Hydrator
    _catalog: il.Catalog

    def create_source(
        self,
        org_id: UUID,
        *,
        key: str,
        name: str,
        config: dict[str, Any] | None = None,
        resources: dict[str, str] | None = None,
        asset_keys: list[str] | None = None,
        destination_ids: list[str] | None = None,
        cross_deps: dict[str, dict[str, str]] | None = None,
    ) -> Component:
        """Create a source and sync its asset rows.

        Args:
            asset_keys: Which asset types to create rows for. None = all.
        """
        with Session(get_engine()) as session:
            db_source = Component(org_id=org_id, kind="source", key=key, name=name, config=config)
            session.add(db_source)
            session.flush()
            sync_relations(session, db_source, "resource", _as_uuid_map(resources))
            sync_relations(session, db_source, "destination", _as_uuids(destination_ids))
            self._ensure_children(session, db_source, asset_keys=asset_keys)
            if cross_deps:
                self._persist_cross_deps(session, db_source, cross_deps)
            session.commit()
            return load_component(session, db_source.id, kind="source")

    def update_source(
        self,
        source_id: UUID,
        *,
        name: str | None = None,
        config: dict[str, Any] | None = None,
        resources: dict[str, str] | None = None,
        asset_keys: list[str] | None = None,
        destination_ids: list[str] | None = None,
        cross_deps: dict[str, dict[str, str]] | None = None,
    ) -> Component:
        """Update a source and sync its asset rows.

        Args:
            asset_keys: Which asset types should exist. None = keep as-is.
        """
        with Session(get_engine()) as session:
            db_source = session.get(Component, source_id)
            if not db_source or db_source.kind != "source":
                raise NotFoundError(f"Source {source_id} not found")
            if name is not None:
                db_source.name = name
            if config is not None:
                db_source.config = config
            if resources is not None:
                sync_relations(session, db_source, "resource", _as_uuid_map(resources))
            if destination_ids is not None:
                sync_relations(session, db_source, "destination", _as_uuids(destination_ids))
            self._ensure_children(session, db_source, asset_keys=asset_keys)
            if cross_deps is not None:
                self._persist_cross_deps(session, db_source, cross_deps)
            session.commit()
            return load_component(session, source_id, kind="source")

    def get_source(self, source_id: UUID) -> Component:
        """Load a source row by ID, with children and relations eager-loaded.

        Args:
            source_id: The source UUID.

        Returns:
            The component row.

        Raises:
            NotFoundError: If the source is not found.
        """
        with Session(get_engine()) as session:
            return load_component(session, source_id, kind="source")

    def list_sources(self, org_id: UUID) -> list[Component]:
        """List all sources with children and relations loaded."""
        with Session(get_engine()) as session:
            return list_components(session, org_id, kind="source")

    def load_source_for_asset(self, asset_id: UUID) -> il.Source:
        """Load and hydrate the source that owns the given asset."""
        with Session(get_engine()) as session:
            db_asset = session.get(Component, asset_id)
            if not db_asset or db_asset.kind != "asset":
                raise NotFoundError(f"Asset {asset_id} not found")
            if not db_asset.parent_id:
                raise NotFoundError(f"Asset {asset_id} is standalone (no source)")
            return self.load_source(db_asset.parent_id)

    def delete_source(self, source_id: UUID) -> None:
        """Delete a source. Children and relations cascade via FK."""
        with Session(get_engine()) as session:
            db_source = session.get(Component, source_id)
            if db_source and db_source.kind == "source":
                session.delete(db_source)
                session.commit()

    # ------------------------------------------------------------------
    # Child asset sync
    # ------------------------------------------------------------------

    def _ensure_children(
        self,
        session: Session,
        db_source: Component,
        asset_keys: list[str] | None = None,
    ) -> None:
        """Sync child asset rows to match the desired set.

        When ``asset_keys`` is provided, only those assets will exist —
        missing ones are created, extra ones are removed. When ``None``,
        existing rows are kept as-is and any newly available catalog
        assets are added.

        Existing rows keep their IDs (and therefore their cross-source
        deps, event references, and per-asset overrides).
        """
        source_cls = self._resolve_source_cls(db_source.key)
        all_keys = {asset_type.key for asset_type in source_cls.asset_types}
        desired = set(asset_keys) & all_keys if asset_keys is not None else None

        existing = session.exec(select(Component).where(Component.parent_id == db_source.id)).all()
        existing_by_key = {child.key: child for child in existing}

        # Remove assets no longer desired (only when asset_keys is explicit).
        # Their relations — including dependencies pointing at them — cascade.
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
                child = Component(
                    org_id=db_source.org_id,
                    kind="asset",
                    key=asset_type.key,
                    parent_id=db_source.id,
                )
                session.add(child)
                children_by_key[asset_type.key] = child
                added_keys.add(asset_type.key)
        session.flush()

        if added_keys:
            self._create_intra_deps(session, source_cls, children_by_key, only_for=added_keys)

    def _create_intra_deps(
        self,
        session: Session,
        source_cls: type[il.Source],
        children_by_key: dict[str, Component],
        only_for: set[str],
    ) -> None:
        """Persist intra-source deps from class-level metadata.

        Uses ``requires`` and ``optional_requires`` on asset types
        (populated by ``_infer_all_requires`` at import time) to
        determine which sibling assets are dependencies.  No source
        instantiation needed.
        """
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
                    add_relation(
                        session,
                        children_by_key[asset_key],
                        children_by_key[qualified_asset],
                        "dependency",
                        param_name,
                    )

    def _persist_cross_deps(
        self,
        session: Session,
        db_source: Component,
        cross_deps: dict[str, dict[str, str]],
    ) -> None:
        """Persist cross-source deps from user selections.

        ``cross_deps`` maps ``{asset_key: {param_name: upstream_asset_id}}``.
        Existing deps for the given params are replaced; other params keep
        their relations (intra-source wiring stays intact).
        """
        children = session.exec(select(Component).where(Component.parent_id == db_source.id)).all()
        children_by_key = {child.key: child for child in children}

        for asset_key, dependency_map in cross_deps.items():
            child = children_by_key.get(asset_key)
            if not child or not child.id:
                continue

            for param_name, upstream_id in dependency_map.items():
                existing = session.exec(
                    select(ComponentRelation).where(
                        ComponentRelation.src_id == child.id,
                        ComponentRelation.type == "dependency",
                        ComponentRelation.slot == param_name,
                    )
                ).all()
                for relation in existing:
                    session.delete(relation)
                session.flush()
                if upstream_id:
                    upstream = session.get(Component, UUID(upstream_id))
                    if upstream:
                        add_relation(session, child, upstream, "dependency", param_name)

    # ------------------------------------------------------------------
    # Hydration
    # ------------------------------------------------------------------

    def load_source(self, source_id: UUID) -> il.Source:
        """Hydrate a framework Source from a DB row.

        Delegates to :class:`~interloper_db.hydration.Hydrator` to build a
        ``ComponentSpec`` tree, then reconstructs the live instance via
        ``spec.reconstruct()``.
        """
        with Session(get_engine()) as session:
            db_source = session.get(Component, source_id)
            if not db_source or db_source.kind != "source":
                raise NotFoundError(f"Source {source_id} not found")

            status = source_status(self._catalog, db_source.key)
            if status is not ComponentStatus.OK:
                raise ComponentDriftError(
                    f"Source '{db_source.key}' ({db_source.id}) cannot be hydrated: "
                    f"its catalog key is {status.value}."
                )

            spec = self._hydrator.build_component_spec(session, db_source)

        try:
            return cast(il.Source, spec.reconstruct())
        except Exception as e:
            raise HydrationError(f"Failed to hydrate source '{db_source.key}' ({db_source.id}): {e}") from e

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_source_cls(self, key: str) -> type[il.Source]:
        """Import the source class from the catalog (raise-on-miss).

        Shares the value-based resolver used for drift detection so the write
        path and the read/status path can never disagree on what resolves.
        """
        source_cls = resolve_source_cls(self._catalog, key)
        if source_cls is None:
            raise CatalogKeyError(f"Unknown source key: {key}")
        return source_cls


def _as_uuid_map(bindings: dict[str, str] | None) -> dict[str, UUID]:
    """Convert a ``{slot: uuid_string}`` binding map to UUIDs."""
    return {slot: UUID(value) for slot, value in (bindings or {}).items()}


def _as_uuids(ids: list[str] | None) -> list[UUID]:
    """Convert a list of uuid strings to UUIDs."""
    return [UUID(value) for value in ids or []]
