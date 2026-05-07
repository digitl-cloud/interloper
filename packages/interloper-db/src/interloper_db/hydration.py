"""Hydration: translates DB rows into ``ComponentSpec`` trees.

This module is a pure transformation layer.  It reads rows from the
database and builds the ``ComponentSpec`` tree that ``Component.from_spec``
expects.  No framework classes are instantiated here — reconstruction
happens at the call site via ``spec.reconstruct()``::

    hydrator = Hydrator(catalog, decrypt=decrypt_fn)
    with Session(engine) as session:
        db_source = session.get(Source, source_id, options=[...])
        spec = hydrator.build_source_spec(session, db_source)
    source = spec.reconstruct()

The Store wraps this pattern in thin ``load_*`` convenience methods, but
any caller can use the hydrator directly to assemble a spec (for example,
to serialize it to JSON and send it across a process boundary).
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from typing import Any
from uuid import UUID

from interloper.catalog.base import Catalog
from interloper.component.base import ComponentSpec
from interloper.errors import CatalogKeyError
from sqlmodel import Session, select

from interloper_db.models import (
    Asset,
    AssetDependency,
    AssetResource,
    Destination,
    DestinationResource,
    Resource,
    Source,
    SourceResource,
)


class Hydrator:
    """Builds ``ComponentSpec`` trees from DB rows.

    The hydrator holds a catalog (for import-path lookups) and an optional
    decrypt callable (for resource data).  All methods are pure
    transformations — they read rows and return specs without ever
    instantiating framework classes.  Reconstruction is the caller's job.
    """

    def __init__(
        self,
        catalog: Catalog,
        decrypt: Callable[[bytes], bytes] | None = None,
    ) -> None:
        """Initialize the hydrator.

        Args:
            catalog: Catalog used to resolve ``key → import path``.
            decrypt: Optional ``(bytes) -> bytes`` callable for decrypting
                resource data blobs marked ``encrypted=True``.
        """
        self._catalog = catalog
        self._decrypt = decrypt

    # ------------------------------------------------------------------
    # Resource
    # ------------------------------------------------------------------

    def build_resource_spec(self, db_resource: Resource) -> ComponentSpec:
        """Build a spec from a Resource row.

        Resources are leaves — they carry their full state in ``data``
        and have no nested specs.  No session is needed.

        Args:
            db_resource: The Resource row.

        Returns:
            A ``ComponentSpec`` with the row's ``id`` and decoded data.
        """
        path = self._resolve_path(db_resource.key, kind="resource")
        init = self.decode_resource_data(db_resource)
        return ComponentSpec(
            path=path,
            id=str(db_resource.id) if db_resource.id else "",
            init=init or None,
        )

    def decode_resource_data(self, db_resource: Resource) -> dict[str, Any]:
        """Decrypt (when needed) and JSON-decode a resource's data blob.

        Args:
            db_resource: The Resource row whose ``data`` bytes should be
                decoded.

        Returns:
            The decoded configuration dict, or ``{}`` if the row carries
            no data.
        """
        if db_resource.data is None:
            return {}
        raw = db_resource.data
        if db_resource.encrypted and self._decrypt:
            raw = self._decrypt(raw)
        return json.loads(raw)

    # ------------------------------------------------------------------
    # Destination
    # ------------------------------------------------------------------

    def build_destination_spec(
        self,
        session: Session,
        db_destination: Destination,
    ) -> ComponentSpec:
        """Build a spec from a Destination row.

        Captures the destination's ``config`` dict and resolves its
        resource bindings as nested resource specs under ``init.resources``.

        Args:
            session: Active DB session.
            db_destination: The Destination row.

        Returns:
            A ``ComponentSpec`` with the row's ``id`` and resolved bindings.
        """
        path = self._resolve_path(db_destination.key, kind="destination")

        init: dict[str, Any] = dict(db_destination.config) if db_destination.config else {}

        bindings = session.exec(
            select(DestinationResource).where(DestinationResource.destination_id == db_destination.id)
        ).all()
        resources = self._resource_specs_from_bindings(session, bindings)
        if resources:
            init["resources"] = resources

        return ComponentSpec(
            path=path,
            id=str(db_destination.id) if db_destination.id else "",
            init=init or None,
        )

    # ------------------------------------------------------------------
    # Asset
    # ------------------------------------------------------------------

    def build_asset_spec(
        self,
        session: Session,
        db_asset: Asset,
    ) -> ComponentSpec:
        """Build a spec from a standalone Asset row.

        Source-owned assets should not be built through this method —
        they're embedded in the parent source's spec via
        :meth:`build_source_spec`, which handles the
        ``"source_path:asset_key"`` path convention.

        Args:
            session: Active DB session.
            db_asset: A standalone Asset row (``source_id`` is ``None``).

        Returns:
            A ``ComponentSpec`` with the row's ``id`` and all per-asset
            state (materializable, config, resources, destinations, deps).
        """
        path = self._resolve_path(db_asset.key, kind="asset")
        init = self._build_asset_init(session, db_asset)
        return ComponentSpec(
            path=path,
            id=str(db_asset.id) if db_asset.id else "",
            init=init or None,
        )

    def _build_asset_init(
        self,
        session: Session,
        db_asset: Asset,
    ) -> dict[str, Any]:
        """Build the ``init`` dict for an asset spec.

        Captures materializable, per-field config overrides, resource
        bindings, destination bindings, and cross-asset dependencies.

        Returns:
            A dict suitable for use as a ``ComponentSpec.init``.
        """
        init: dict[str, Any] = {"materializable": db_asset.materializable}

        if db_asset.config:
            init.update(db_asset.config)

        if db_asset.id:
            resource_bindings = session.exec(
                select(AssetResource).where(AssetResource.asset_id == db_asset.id)
            ).all()
            resources = self._resource_specs_from_bindings(session, resource_bindings)
            if resources:
                init["resources"] = resources

            destination_specs = [
                self.build_destination_spec(session, db_dest).model_dump(mode="json")
                for db_dest in db_asset.destinations
            ]
            if destination_specs:
                init["destination"] = (
                    destination_specs if len(destination_specs) > 1 else destination_specs[0]
                )

            deps = self._deps_for_asset(session, db_asset.id)
            if deps:
                init["deps"] = deps

        return init

    def _deps_for_asset(
        self,
        session: Session,
        asset_id: UUID,
    ) -> dict[str, str]:
        """Return ``{param_name: upstream_uuid}`` from ``AssetDependency`` rows."""
        dependency_rows = session.exec(
            select(AssetDependency).where(AssetDependency.asset_id == asset_id)
        ).all()
        return {d.param_name: str(d.upstream_asset_id) for d in dependency_rows}

    # ------------------------------------------------------------------
    # Source
    # ------------------------------------------------------------------

    def build_source_spec(
        self,
        session: Session,
        db_source: Source,
    ) -> ComponentSpec:
        """Build a spec from a Source row including per-asset overrides.

        The returned spec is self-contained: it carries the source's own
        config, resource bindings, destination bindings, plus an
        ``assets`` override map keyed by asset key.  Each entry is the
        sparse init payload for one asset (materializable, config,
        resources, destinations, deps).  Reconstruction via
        ``ComponentSpec.reconstruct()`` produces a live ``Source`` whose
        ``_apply_asset_overrides`` validator materialises each asset from
        ``asset_types`` with the overrides applied.

        Args:
            session: Active DB session.
            db_source: The Source row (relationships should be eagerly
                loaded via ``selectinload``).

        Returns:
            A ``ComponentSpec`` capturing the source and its assets.
        """
        path = self._resolve_path(db_source.key, kind="source")

        init: dict[str, Any] = {}

        if db_source.config:
            init.update(db_source.config)

        resource_bindings = session.exec(
            select(SourceResource).where(SourceResource.source_id == db_source.id)
        ).all()
        resources = self._resource_specs_from_bindings(session, resource_bindings)
        if resources:
            init["resources"] = resources

        destination_specs = [
            self.build_destination_spec(session, db_dest).model_dump(mode="json")
            for db_dest in db_source.destinations
        ]
        if destination_specs:
            init["destination"] = (
                destination_specs if len(destination_specs) > 1 else destination_specs[0]
            )

        asset_overrides: dict[str, dict[str, Any]] = {
            db_asset.key: {"id": str(db_asset.id), **self._build_asset_init(session, db_asset)}
            for db_asset in db_source.assets
        }
        if asset_overrides:
            init["assets"] = asset_overrides

        return ComponentSpec(
            path=path,
            id=str(db_source.id) if db_source.id else "",
            init=init or None,
        )

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _resolve_path(self, key: str, *, kind: str) -> str:
        """Look up a component's import path via the catalog.

        Args:
            key: The catalog key.
            kind: Component kind, used only in the error message.

        Returns:
            The resolved import path.

        Raises:
            CatalogKeyError: If the catalog has no entry for *key*.
        """
        definition = self._catalog.get(key)
        if not definition:
            raise CatalogKeyError(f"Unknown {kind} key: {key}")
        return definition.path

    def _resource_specs_from_bindings(
        self,
        session: Session,
        bindings: Iterable[Any],
    ) -> dict[str, dict[str, Any]]:
        """Build a ``{slot: resource_spec_dict}`` map from binding rows.

        Each binding row must expose ``resource_id`` and ``key`` attributes.

        Returns:
            A dict mapping slot name → resource spec (as a JSON-safe dict).
        """
        result: dict[str, dict[str, Any]] = {}
        for binding in bindings:
            db_resource = session.get(Resource, binding.resource_id)
            if db_resource:
                spec = self.build_resource_spec(db_resource)
                result[binding.key] = spec.model_dump(mode="json")
        return result
