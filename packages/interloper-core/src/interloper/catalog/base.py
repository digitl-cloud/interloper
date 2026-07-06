"""Catalog: flat registry of all component definitions keyed by component key.

The catalog collects definitions from sources, their assets, resources,
and destinations into a single ``dict[str, ComponentDefinition]``.

Discovery vs enablement: installed packages declare the components they
provide through the ``interloper.components`` entry-point group (the
*available universe*); the ``AppSettings.catalog`` import paths select what a
deployment actually exposes. When no paths are configured, the catalog is
the full discovered universe.

Framework built-ins (``Job``) are part of the framework itself, not the
connector universe, so they are always present regardless of enablement.

Usage::

    import interloper as il

    # From settings; falls back to entry-point discovery when unset
    catalog = il.Catalog.from_settings()

    # The full discovered universe, regardless of settings
    catalog = il.Catalog.discover()
"""

from __future__ import annotations

import logging
from functools import cache
from importlib.metadata import entry_points
from types import ModuleType
from typing import Any

from pydantic import BaseModel, Field

from interloper.asset.base import Asset
from interloper.component import Component, ComponentDefinition
from interloper.destination.base import Destination
from interloper.job.base import Job
from interloper.resource.base import Resource
from interloper.settings import AppSettings
from interloper.source.base import Source

logger = logging.getLogger(__name__)

COMPONENT_TYPES = (Source, Destination, Asset, Resource, Job)

BUILTIN_COMPONENTS: tuple[type[Component], ...] = (Job,)

_ENTRY_POINT_GROUP = "interloper.components"


def _with_builtins(components: dict[str, ComponentDefinition]) -> dict[str, ComponentDefinition]:
    """Ensure framework built-ins are present, regardless of enablement.

    Returns:
        The same mapping, with any missing built-in definitions added.
    """
    for builtin in BUILTIN_COMPONENTS:
        components.setdefault(builtin.key, builtin.definition())
    return components


@cache
def _discovered_paths() -> tuple[str, ...]:
    """Collect component import paths from installed entry points.

    Each entry under ``interloper.components`` names a module whose public
    attributes are scanned for component classes (or a component class
    directly).

    Returns:
        Import paths of every discovered component class.
    """
    from interloper.utils.imports import get_object_path

    paths: list[str] = []
    for entry_point in entry_points(group=_ENTRY_POINT_GROUP):
        loaded = entry_point.load()
        if isinstance(loaded, ModuleType):
            for attr in dir(loaded):
                obj = getattr(loaded, attr)
                if isinstance(obj, type) and any(issubclass(obj, t) for t in COMPONENT_TYPES):
                    paths.append(get_object_path(obj))
        elif isinstance(loaded, type) and any(issubclass(loaded, t) for t in COMPONENT_TYPES):
            paths.append(get_object_path(loaded))
    return tuple(paths)


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

        return Catalog(components=_with_builtins(components))

    @classmethod
    def from_settings(cls) -> Catalog:
        """Load catalog from the ``AppSettings.catalog`` import paths.

        The configured paths are the deployment's *enablement* list; when no
        paths are configured, falls back to :meth:`discover` — everything the
        installed packages declare.

        Returns:
            Catalog of all component definitions.
        """
        settings = AppSettings.get()
        if settings.catalog:
            return cls.from_paths(settings.catalog)
        return cls.discover()

    @classmethod
    def from_assets(cls, sources_or_assets: list[type[Source | Asset]]) -> Catalog:
        """Load catalog from a list of asset classes.

        Returns:
            Catalog of all component definitions.
        """
        return cls(
            components=_with_builtins(
                {source_or_asset.key: source_or_asset.definition() for source_or_asset in sources_or_assets}
            )
        )

    @classmethod
    def discover(cls) -> Catalog:
        """Load the catalog from installed ``interloper.components`` entry points.

        The available universe: every component declared by every installed
        package, discovered from package metadata — no hardcoded package
        names, no import-order dependence.

        Returns:
            Catalog of all discovered component definitions.
        """
        return cls.from_paths(list(_discovered_paths()))
