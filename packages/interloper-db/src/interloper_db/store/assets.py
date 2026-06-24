"""Asset persistence: CRUD, hydration, and dependencies."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

import interloper as il
from interloper.errors import ComponentDriftError, HydrationError, NotFoundError
from sqlalchemy.orm import selectinload
from sqlmodel import Session, select

from interloper_db.drift import ComponentStatus, asset_status
from interloper_db.engine import get_engine
from interloper_db.hydration import Hydrator
from interloper_db.models import (
    Asset,
    AssetDependency,
    AssetDestination,
    AssetResource,
    Destination,
)


def _load_asset_with_relations(session: Session, asset_id: UUID) -> Asset:
    """Load an asset with all relationships eager-loaded."""
    statement = (
        select(Asset)
        .where(Asset.id == asset_id)
        .options(
            selectinload(Asset.source),  # ty: ignore[invalid-argument-type]
            selectinload(Asset.resources),  # ty: ignore[invalid-argument-type]
            selectinload(Asset.destinations).selectinload(Destination.resources),  # ty: ignore[invalid-argument-type]
        )
    )
    db_asset = session.exec(statement).first()
    if not db_asset:
        raise NotFoundError(f"Asset {asset_id} not found")
    return db_asset


def _sync_resource_bindings(session: Session, db_asset: Asset, resources: dict[str, str] | None) -> None:
    """Replace all resource bindings for an asset."""
    existing_bindings = session.exec(select(AssetResource).where(AssetResource.asset_id == db_asset.id)).all()
    for binding in existing_bindings:
        session.delete(binding)
    for key, resource_id in (resources or {}).items():
        session.add(
            AssetResource(asset_id=db_asset.id, resource_id=UUID(resource_id), key=key)
        )


def _sync_destination_bindings(session: Session, db_asset: Asset, destination_ids: list[str] | None) -> None:
    """Replace all destination bindings for an asset."""
    existing_bindings = session.exec(select(AssetDestination).where(AssetDestination.asset_id == db_asset.id)).all()
    for binding in existing_bindings:
        session.delete(binding)
    for destination_id in destination_ids or []:
        session.add(
            AssetDestination(asset_id=db_asset.id, destination_id=UUID(destination_id))
        )


class AssetMixin:
    """Store methods for asset management."""

    _hydrator: Hydrator
    _catalog: il.Catalog

    if TYPE_CHECKING:
        # Provided by SourceMixin on the composed Store.
        def load_source(self, source_id: UUID) -> il.Source: ...


    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def create_asset(
        self,
        org_id: UUID,
        *,
        key: str,
        config: dict[str, Any] | None = None,
        resources: dict[str, str] | None = None,
        destination_ids: list[str] | None = None,
    ) -> Asset:
        """Create a standalone asset (source_id=None).

        Args:
            org_id: Organisation UUID.
            key: Catalog key identifying the asset class.
            config: Optional per-asset configuration.
            resources: Optional resource bindings ``{slot_name: resource_uuid}``.
            destination_ids: Optional destination UUIDs.

        Returns:
            The created Asset row with relationships loaded.
        """
        with Session(get_engine()) as session:
            db_asset = Asset(
                org_id=org_id,
                key=key,
                config=config,
                materializable=True,
            )
            session.add(db_asset)
            session.flush()
            created_id = db_asset.id
            _sync_resource_bindings(session, db_asset, resources)
            _sync_destination_bindings(session, db_asset, destination_ids)
            session.commit()
        with Session(get_engine()) as session:
            return _load_asset_with_relations(session, created_id)

    def get_asset(self, asset_id: UUID) -> Asset:
        """Load an asset by ID with relationships.

        Args:
            asset_id: The asset UUID.

        Returns:
            The Asset row with resources and destinations loaded.

        Raises:
            NotFoundError: If the asset is not found.
        """
        with Session(get_engine()) as session:
            return _load_asset_with_relations(session, asset_id)

    def update_asset(
        self,
        asset_id: UUID,
        *,
        materializable: bool | None = None,
        config: dict[str, Any] | None = None,
        resources: dict[str, str] | None = None,
        destination_ids: list[str] | None = None,
    ) -> Asset:
        """Update an asset's configuration, resources, or destinations.

        Supports per-asset overrides for both standalone and source-owned assets.

        Args:
            asset_id: The asset UUID.
            materializable: Toggle materialization on/off.
            config: Per-asset config overrides (replaces existing).
            resources: Per-asset resource bindings (replaces existing).
            destination_ids: Per-asset destination bindings (replaces existing).

        Returns:
            The updated Asset row.

        Raises:
            NotFoundError: If the asset is not found.
        """
        with Session(get_engine()) as session:
            db_asset = session.get(Asset, asset_id)
            if not db_asset:
                raise NotFoundError(f"Asset {asset_id} not found")
            if materializable is not None:
                db_asset.materializable = materializable
            if config is not None:
                db_asset.config = config
            if resources is not None:
                _sync_resource_bindings(session, db_asset, resources)
            if destination_ids is not None:
                _sync_destination_bindings(session, db_asset, destination_ids)
            session.commit()
        with Session(get_engine()) as session:
            return _load_asset_with_relations(session, asset_id)

    def delete_asset(self, asset_id: UUID) -> None:
        """Delete a standalone asset. Refuses to delete source-owned assets.

        Args:
            asset_id: The asset UUID.

        Raises:
            NotFoundError: If the asset is not found.
            ValueError: If the asset belongs to a source (use source deletion instead).
        """
        with Session(get_engine()) as session:
            db_asset = session.get(Asset, asset_id)
            if not db_asset:
                raise NotFoundError(f"Asset {asset_id} not found")
            if db_asset.source_id is not None:
                raise ValueError("Cannot delete a source-owned asset directly. Delete or update the source instead.")
            session.delete(db_asset)
            session.commit()

    def list_assets(self, org_id: UUID) -> list[Asset]:
        """List all assets for an organisation with relationships loaded.

        Args:
            org_id: Organisation UUID.

        Returns:
            List of Asset rows.
        """
        with Session(get_engine()) as session:
            statement = (
                select(Asset)
                .where(Asset.org_id == org_id)
                .options(
                    selectinload(Asset.source),  # ty: ignore[invalid-argument-type]
                    selectinload(Asset.resources),  # ty: ignore[invalid-argument-type]
                    selectinload(Asset.destinations).selectinload(Destination.resources),  # ty: ignore[invalid-argument-type]
                )
            )
            return list(session.exec(statement).all())

    # ------------------------------------------------------------------
    # Hydration
    # ------------------------------------------------------------------

    def load_asset(self, asset_id: UUID) -> il.Asset:
        """Hydrate a single framework Asset from a DB row.

        Source-owned assets are hydrated by loading the parent source and
        extracting the matching asset instance.  Standalone assets are
        built directly via :class:`~interloper_db.hydration.Hydrator`.

        Args:
            asset_id: The asset UUID.

        Returns:
            The hydrated framework Asset instance.

        Raises:
            NotFoundError: If the asset is not found.
            HydrationError: If hydration fails.
        """
        with Session(get_engine()) as session:
            db_asset = _load_asset_with_relations(session, asset_id)

            if db_asset.source_id is not None:
                # Source-owned: hydrate via parent source and extract. A drifted
                # parent raises ComponentDriftError from load_source; if the
                # parent is live but no longer declares this key, the asset key
                # itself has drifted out of the source.
                source = self.load_source(db_asset.source_id)
                for asset in source.assets:
                    if type(asset).key == db_asset.key:
                        return asset
                raise ComponentDriftError(
                    f"Asset '{db_asset.key}' ({db_asset.id}) is no longer declared "
                    f"by source '{type(source).key}'; its catalog key has drifted."
                )

            # Standalone: drift-check then build spec and reconstruct.
            status = asset_status(self._catalog, db_asset.key)
            if status is not ComponentStatus.OK:
                raise ComponentDriftError(
                    f"Asset '{db_asset.key}' ({db_asset.id}) cannot be hydrated: "
                    f"its catalog key is {status.value}."
                )
            spec = self._hydrator.build_asset_spec(session, db_asset)

        try:
            return cast(il.Asset, spec.reconstruct())
        except Exception as e:
            raise HydrationError(
                f"Failed to hydrate standalone asset '{db_asset.key}' ({db_asset.id}): {e}"
            ) from e

    # ------------------------------------------------------------------
    # Dependencies
    # ------------------------------------------------------------------

    def add_dependency(self, asset_id: UUID, upstream_asset_id: UUID, param_name: str) -> AssetDependency:
        """Add an asset dependency.

        Args:
            asset_id: Downstream asset UUID.
            upstream_asset_id: Upstream asset UUID.
            param_name: The downstream function parameter this dep wires to.

        Returns:
            The created AssetDependency row.

        Raises:
            NotFoundError: If either asset does not exist.
        """
        with Session(get_engine()) as session:
            if not session.get(Asset, asset_id):
                raise NotFoundError(f"Asset {asset_id} not found")
            if not session.get(Asset, upstream_asset_id):
                raise NotFoundError(f"Upstream asset {upstream_asset_id} not found")
            dependency = AssetDependency(asset_id=asset_id, upstream_asset_id=upstream_asset_id, param_name=param_name)
            session.add(dependency)
            session.commit()
            session.refresh(dependency)
            return dependency

    def remove_dependency(self, asset_id: UUID, upstream_asset_id: UUID) -> None:
        """Remove an asset dependency.

        Args:
            asset_id: Downstream asset UUID.
            upstream_asset_id: Upstream asset UUID.
        """
        with Session(get_engine()) as session:
            statement = select(AssetDependency).where(
                AssetDependency.asset_id == asset_id,
                AssetDependency.upstream_asset_id == upstream_asset_id,
            )
            dependency = session.exec(statement).first()
            if dependency:
                session.delete(dependency)
                session.commit()

    def list_dependencies(self, org_id: UUID) -> list[AssetDependency]:
        """List all asset dependencies for an organisation.

        Args:
            org_id: Organisation UUID.

        Returns:
            List of AssetDependency rows.
        """
        with Session(get_engine()) as session:
            statement = (
                select(AssetDependency)
                .join(Asset, AssetDependency.asset_id == Asset.id)  # ty: ignore[invalid-argument-type]
                .where(Asset.org_id == org_id)
            )
            return list(session.exec(statement).all())
