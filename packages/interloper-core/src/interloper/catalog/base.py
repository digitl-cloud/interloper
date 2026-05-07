"""Catalog: flat registry of all component definitions keyed by component key.

The catalog collects definitions from sources, their assets, resources,
and destinations into a single ``dict[str, ComponentDefinition]``.

Usage::

    import interloper as il

    # From settings (falls back to auto-discovery)
    catalog = il.Catalog.from_settings()

    # Auto-discover from interloper_assets directly
    catalog = il.Catalog.from_interloper_assets()
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, Field

from interloper.asset.base import Asset
from interloper.component import ComponentDefinition
from interloper.destination.base import Destination
from interloper.resource.base import Resource
from interloper.settings import AppSettings
from interloper.source.base import Source

logger = logging.getLogger(__name__)

COMPONENT_TYPES = (Source, Destination, Asset, Resource)


class Catalog(BaseModel):
    """Catalog of all component definitions."""

    components: dict[str, ComponentDefinition] = Field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> ComponentDefinition | None:
        """Look up a component definition by key.

        Args:
            key: The component key.
            default: Value to return if key is not found.

        Returns:
            The component definition, or *default* if not found.
        """
        return self.components.get(key, default)

    def to_paths(self) -> list[str]:
        """Extract the import paths of all components in the catalog.

        Useful for passing the catalog across process boundaries (e.g. to
        a Docker container via env var) without serializing the full
        definitions.

        Returns:
            Sorted list of fully qualified import paths.
        """
        return sorted(defn.path for defn in self.components.values())

    def dump(self) -> dict[str, dict[str, Any]]:
        """Serialize all definitions to JSON-compatible dicts.

        Returns:
            Mapping from component key to serialized definition.
        """
        return {k: v.model_dump(mode="json") for k, v in self.components.items()}

    @classmethod
    def from_paths(cls, paths: list[str]) -> Catalog:
        """Reconstruct a catalog from a list of import paths.

        Args:
            paths: Fully qualified import paths to component classes.

        Returns:
            Catalog of all component definitions.
        """
        from interloper.utils.imports import import_from_path

        components: dict[str, ComponentDefinition] = {}
        for path in paths:
            try:
                obj = import_from_path(path)
                if not isinstance(obj, type):
                    continue
                if not any(issubclass(obj, t) for t in COMPONENT_TYPES):
                    continue

                components[obj.key] = obj.definition()

                # Auto-discover resources and destinations reachable from
                # sources and assets so the catalog is complete without
                # requiring every nested type to be listed explicitly.
                if issubclass(obj, Source):
                    for resources in obj.resource_types.values():
                        components.setdefault(resources.key, resources.definition())
                    for assets in obj.asset_types:
                        for resources in assets.resource_types.values():
                            components.setdefault(resources.key, resources.definition())
                        for destinations in assets.destination_types:
                            components.setdefault(destinations.key, destinations.definition())

                elif issubclass(obj, Asset):
                    for resources in obj.resource_types.values():
                        components.setdefault(resources.key, resources.definition())
                    for destinations in obj.destination_types:
                        components.setdefault(destinations.key, destinations.definition())

                elif issubclass(obj, Destination):
                    for resources in obj.resource_types.values():
                        components.setdefault(resources.key, resources.definition())

            except (ImportError, AttributeError) as exc:
                logger.warning("Failed to import component '%s': %s", path, exc)

        return Catalog(components=components)

    @classmethod
    def from_settings(cls) -> Catalog:
        """Load catalog from the ``AppSettings.catalog`` import paths.

        Falls back to :meth:`from_interloper_assets` when no paths are
        configured.

        Returns:
            Catalog of all component definitions.
        """
        settings = AppSettings.get()

        return cls.from_paths(settings.catalog)

    @classmethod
    def from_assets(cls, sources_or_assets: list[type[Source | Asset]]) -> Catalog:
        """Load catalog from a list of asset classes.

        Returns:
            Catalog of all component definitions.
        """
        return cls(
            components={source_or_asset.key: source_or_asset.definition() for source_or_asset in sources_or_assets}
        )

    @classmethod
    def from_interloper_assets(cls) -> Catalog:
        """Load catalog from the ``interloper-assets`` package.

        Returns:
            Catalog of all component definitions.
        """
        from interloper.utils.imports import get_object_path

        paths: list[str] = []

        try:
            import interloper_assets

            for attr in dir(interloper_assets):
                obj = getattr(interloper_assets, attr)
                if isinstance(obj, type) and any(issubclass(obj, t) for t in COMPONENT_TYPES):
                    paths.append(get_object_path(obj))
        except ImportError:
            pass

        return cls.from_paths(paths)
