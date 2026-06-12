"""Source persistence: CRUD and hydration."""

from __future__ import annotations

from typing import Any, cast
from uuid import UUID

import interloper as il
from interloper.errors import CatalogKeyError, HydrationError, SourceNotFoundError
from sqlalchemy.orm import selectinload
from sqlmodel import Session, col, select

from interloper_db.engine import get_engine
from interloper_db.hydration import Hydrator
from interloper_db.models import (
    Asset,
    AssetDependency,
    Destination,
    Source,
    SourceDestination,
    SourceResource,
)


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
    ) -> Source:
        """Create a source and sync its asset rows.

        Args:
            asset_keys: Which asset types to create rows for. None = all.
        """
        with Session(get_engine()) as session:
            db_source = Source(
                org_id=org_id,
                key=key,
                name=name,
                config=config,
            )
            session.add(db_source)
            session.flush()
            self._sync_resource_bindings(session, db_source, resources)
            self._sync_destination_bindings(session, db_source, destination_ids)
            self._ensure_assets(session, db_source, asset_keys=asset_keys)
            if cross_deps:
                self._persist_cross_deps(session, db_source, cross_deps)
            session.commit()
            session.refresh(db_source)
            return db_source

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
    ) -> Source:
        """Update a source and sync its asset rows.

        Args:
            asset_keys: Which asset types should exist. None = keep as-is.
        """
        with Session(get_engine()) as session:
            db_source = session.get(Source, source_id)
            if not db_source:
                raise SourceNotFoundError(f"Source {source_id} not found")
            if name is not None:
                db_source.name = name
            if config is not None:
                db_source.config = config
            if resources is not None:
                self._sync_resource_bindings(session, db_source, resources)
            if destination_ids is not None:
                self._sync_destination_bindings(session, db_source, destination_ids)
            self._ensure_assets(session, db_source, asset_keys=asset_keys)
            if cross_deps is not None:
                self._persist_cross_deps(session, db_source, cross_deps)
            session.commit()
            session.refresh(db_source)
            return db_source

    def get_source(self, source_id: UUID) -> Source:
        """Load a source row by ID.

        Args:
            source_id: The source UUID.

        Returns:
            The Source row.

        Raises:
            SourceNotFoundError: If the source is not found.
        """
        with Session(get_engine()) as session:
            db_source = session.get(Source, source_id)
            if not db_source:
                raise SourceNotFoundError(f"Source {source_id} not found")
            return db_source

    def list_sources(self, org_id: UUID) -> list[Source]:
        """List all sources with assets and destinations loaded."""
        with Session(get_engine()) as session:
            statement = (
                select(Source)
                .where(Source.org_id == org_id)
                .options(
                    selectinload(Source.assets),  # ty: ignore[invalid-argument-type]
                    selectinload(Source.resources),  # ty: ignore[invalid-argument-type]
                    selectinload(Source.destinations).selectinload(Destination.resources),  # ty: ignore[invalid-argument-type]
                )
            )
            return list(session.exec(statement).all())

    def load_source_for_asset(self, asset_id: UUID) -> il.Source:
        """Load and hydrate the source that owns the given asset."""
        with Session(get_engine()) as session:
            db_asset = session.get(Asset, asset_id)
            if not db_asset:
                raise SourceNotFoundError(f"Asset {asset_id} not found")
            if not db_asset.source_id:
                raise SourceNotFoundError(f"Asset {asset_id} is standalone (no source)")
            return self.load_source(db_asset.source_id)

    def delete_source(self, source_id: UUID) -> None:
        """Delete a source. Assets cascade via FK."""
        with Session(get_engine()) as session:
            db_source = session.get(Source, source_id)
            if db_source:
                session.delete(db_source)
                session.commit()

    # ------------------------------------------------------------------
    # Asset sync
    # ------------------------------------------------------------------

    def _ensure_assets(
        self,
        session: Session,
        db_source: Source,
        asset_keys: list[str] | None = None,
    ) -> None:
        """Sync asset rows to match the desired set.

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

        existing_assets = session.exec(select(Asset).where(Asset.source_id == db_source.id)).all()
        existing_by_key = {db_asset.key: db_asset for db_asset in existing_assets}

        # Remove assets no longer desired (only when asset_keys is explicit)
        if desired is not None:
            for key in set(existing_by_key) - desired:
                db_asset = existing_by_key[key]
                if db_asset.id:
                    dependencies = session.exec(
                        select(AssetDependency).where(
                            (col(AssetDependency.asset_id) == db_asset.id)
                            | (col(AssetDependency.upstream_asset_id) == db_asset.id)
                        )
                    ).all()
                    for dependency in dependencies:
                        session.delete(dependency)
                session.delete(db_asset)

        # Determine which keys to ensure exist
        keys_to_ensure = desired if desired is not None else all_keys

        # Add missing
        added_keys: set[str] = set()
        asset_map: dict[str, Asset] = {}
        for key in keys_to_ensure & set(existing_by_key):
            asset_map[key] = existing_by_key[key]
        for asset_type in source_cls.asset_types:
            if asset_type.key in keys_to_ensure and asset_type.key not in existing_by_key:
                db_asset = Asset(
                    source_id=db_source.id,
                    org_id=db_source.org_id,
                    key=asset_type.key,
                    materializable=True,
                )
                session.add(db_asset)
                session.flush()
                asset_map[asset_type.key] = db_asset
                added_keys.add(asset_type.key)

        # Wire intra-source deps for new assets
        if added_keys:
            self._create_intra_deps(session, source_cls, asset_map, only_for=added_keys)

    def _create_intra_deps(
        self,
        session: Session,
        source_cls: type[il.Source],
        asset_map: dict[str, Asset],
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
            if asset_key not in only_for or asset_key not in asset_map:
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

                if qualified_asset in asset_map:
                    session.add(
                        AssetDependency(
                            asset_id=asset_map[asset_key].id,
                            upstream_asset_id=asset_map[qualified_asset].id,
                            param_name=param_name,
                        )
                    )

    def _persist_cross_deps(
        self,
        session: Session,
        db_source: Source,
        cross_deps: dict[str, dict[str, str]],
    ) -> None:
        """Persist cross-source deps from user selections.

        ``cross_deps`` maps ``{asset_key: {param_name: upstream_asset_id}}``.
        Existing cross-source deps for the given params are replaced.
        """
        key_to_asset: dict[str, Asset] = {}
        for db_asset in db_source.assets:
            key_to_asset[db_asset.key] = db_asset

        for asset_key, dependency_map in cross_deps.items():
            db_asset = key_to_asset.get(asset_key)
            if not db_asset or not db_asset.id:
                continue

            for param_name in dependency_map:
                existing_dependencies = session.exec(
                    select(AssetDependency).where(
                        AssetDependency.asset_id == db_asset.id,
                        AssetDependency.param_name == param_name,
                    )
                ).all()
                for dependency in existing_dependencies:
                    session.delete(dependency)

            for param_name, upstream_id in dependency_map.items():
                if upstream_id:
                    session.add(
                        AssetDependency(
                            asset_id=db_asset.id,
                            upstream_asset_id=UUID(upstream_id),
                            param_name=param_name,
                        )
                    )

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
            db_source = session.get(
                Source,
                source_id,
                options=[
                    selectinload(Source.assets).selectinload(Asset.resources),  # ty: ignore[invalid-argument-type]
                    selectinload(Source.assets).selectinload(Asset.destinations).selectinload(Destination.resources),  # ty: ignore[invalid-argument-type]
                    selectinload(Source.resources),  # ty: ignore[invalid-argument-type]
                    selectinload(Source.destinations).selectinload(Destination.resources),  # ty: ignore[invalid-argument-type]
                ],
            )
            if not db_source:
                raise SourceNotFoundError(f"Source {source_id} not found")

            spec = self._hydrator.build_source_spec(session, db_source)

        try:
            return cast(il.Source, spec.reconstruct())
        except Exception as e:
            raise HydrationError(f"Failed to hydrate source '{db_source.key}' ({db_source.id}): {e}") from e

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _sync_resource_bindings(
        session: Session,
        db_source: Source,
        resources: dict[str, str] | None,
    ) -> None:
        """Replace all resource bindings for a source."""
        existing_bindings = session.exec(select(SourceResource).where(SourceResource.source_id == db_source.id)).all()
        for binding in existing_bindings:
            session.delete(binding)
        for key, resource_id in (resources or {}).items():
            session.add(
                SourceResource(source_id=db_source.id, resource_id=UUID(resource_id), key=key)
            )

    @staticmethod
    def _sync_destination_bindings(
        session: Session,
        db_source: Source,
        destination_ids: list[str] | None,
    ) -> None:
        """Replace all destination bindings for a source."""
        existing_bindings = session.exec(
            select(SourceDestination).where(SourceDestination.source_id == db_source.id)
        ).all()
        for binding in existing_bindings:
            session.delete(binding)
        for destination_id in destination_ids or []:
            session.add(
                SourceDestination(source_id=db_source.id, destination_id=UUID(destination_id))
            )

    def _resolve_source_cls(self, key: str) -> type[il.Source]:
        """Import the source class from the catalog."""
        from interloper.utils.imports import import_from_path

        catalog = self._catalog
        definition = catalog.get(key)
        if not definition:
            raise CatalogKeyError(f"Unknown source key: {key}")
        return import_from_path(definition.path)
