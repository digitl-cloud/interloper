"""Catalog: flat registry of all component definitions keyed by component key.

Registration is two entry-point groups, and nothing else:

- ``interloper.kinds`` — what kinds exist: one entry per kind, naming its
  anchor class (consumed by :data:`interloper.KINDS`).
- ``interloper.components`` — what component classes exist: every concrete
  component an installed package provides, framework classes included
  (core declares ``cron_job``/``trigger_hook``/``webhook_hook`` here, the
  same way ``interloper-assets`` declares its connectors).

Installation defines the *declared universe*; ``AppSettings.catalog``
narrows it to what a deployment *enables*. An enabled catalog holds the
configured components, everything they depend on (their resources and, for
sources, their assets' resources and destinations, transitively) and the
framework's own components (the ``interloper`` package's jobs and hooks,
present in every catalog). No configured paths means the whole universe.
Kind anchors are framework, not content: they live in the registry and never
appear in the catalog.

Usage::

    import interloper as il

    catalog = il.Catalog.from_settings()   # the enabled subset, or the universe
    catalog = il.Catalog.discover()        # the declared universe
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from functools import cache
from importlib.metadata import entry_points
from types import ModuleType
from typing import Any

from pydantic import BaseModel, Field

from interloper.asset.base import Asset
from interloper.component import KINDS, Component, ComponentDefinition, RelationDefinition
from interloper.errors import ConfigError
from interloper.settings import AppSettings
from interloper.source.base import Source, SourceDefinition

logger = logging.getLogger(__name__)

_ENTRY_POINT = "interloper.components"


class Catalog(BaseModel):
    """Catalog of all component definitions."""

    components: dict[str, ComponentDefinition] = Field(default_factory=dict)

    # -- Lookup & export -------------------------------------------------------

    def get(self, key: str, default: Any = None, *, parent_key: str | None = None) -> ComponentDefinition | None:
        """Look up a component definition by key.

        The catalog is flat, but a source's assets are not in it: they are
        declared inside their source and reachable only through
        :attr:`SourceDefinition.assets`. Pass *parent_key* to resolve such an
        asset the way its owner declares it — the concrete
        :class:`AssetDefinition`, carrying the composite import path, the
        partitioning and the dependency slots the flat key cannot name. A
        parent that does not resolve, or does not declare the key, falls back
        to the flat lookup.

        Args:
            key: The component key.
            default: Value to return if key is not found.
            parent_key: Key of the owning source, for a source-owned asset.
                Defaults to ``None``, a flat lookup.

        Returns:
            The component definition, or *default* if not found.
        """
        if parent_key is not None:
            parent = self.components.get(parent_key)
            if isinstance(parent, SourceDefinition):
                for asset in parent.assets:
                    if asset.key == key:
                        return asset
        return self.components.get(key, default)

    def vocabulary(self, kind: str, key: str, *, parent_key: str | None = None) -> dict[str, RelationDefinition]:
        """The relation vocabulary governing a persisted component row.

        The class definition is authoritative — a concrete class may extend
        its anchor's vocabulary (``TriggerHook`` adds ``target``). The
        kind's anchor is the fallback when the key does not resolve to a
        matching definition (drifted keys), keeping validation fail-closed on
        the kind's shared minimum.

        Args:
            kind: The row's component kind.
            key: The row's catalog key.
            parent_key: Key of the owning source, for a source-owned asset,
                whose declaration carries the dependency slots. Defaults to
                ``None``, a flat lookup.

        Returns:
            Relation type → definition.
        """
        definition = self.get(key, parent_key=parent_key)
        if definition is not None and definition.kind == kind:
            return definition.relations
        return KINDS[kind].relation_types

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

    # -- Constructors ----------------------------------------------------------

    @classmethod
    def from_paths(cls, paths: list[str]) -> Catalog:
        """Build a catalog enabling the components at *paths*.

        Args:
            paths: Fully qualified import paths to the enabled component
                classes — a deployment's ``catalog`` setting, or a
                ``to_paths()`` list crossing a process boundary. Paths that
                fail to import are skipped with a warning.

        Returns:
            Catalog of the listed components, their dependencies and the
            framework's own components.
        """
        from interloper.utils.imports import import_from_path

        classes: list[type[Component]] = []
        for path in paths:
            try:
                loaded = import_from_path(path)
            except (ImportError, AttributeError) as exception:
                logger.warning("Failed to import component '%s': %s", path, exception)
                continue
            if isinstance(loaded, type) and issubclass(loaded, Component):
                classes.append(loaded)

        return cls.from_assets(classes)

    @classmethod
    def from_settings(cls) -> Catalog:
        """Load the catalog a deployment enables.

        ``AppSettings.catalog`` lists the enabled import paths; when it is
        empty, the catalog is the whole declared universe.

        Returns:
            Catalog of all enabled component definitions.
        """
        settings = AppSettings.get()
        if settings.catalog:
            return cls.from_paths(settings.catalog)
        return cls.discover()

    @classmethod
    def from_assets(cls, sources_or_assets: Iterable[type[Component]]) -> Catalog:
        """Build a catalog enabling the given component classes.

        Args:
            sources_or_assets: The enabled component classes — typically
                sources and assets, but any component kind is accepted.

        Returns:
            Catalog of the given components, their dependencies and the
            framework's own components.
        """
        definitions = cls._definitions_from(cls._with_dependencies(sources_or_assets))
        for key, definition in cls._framework_definitions().items():
            definitions.setdefault(key, definition)
        return cls(components=definitions)

    @classmethod
    def discover(cls) -> Catalog:
        """Load the declared universe from ``interloper.components`` entry points.

        Cheap to call repeatedly: entry points cannot change at runtime, so
        the scan and the class imports behind it are cached for the process
        and only the mapping is rebuilt.

        Returns:
            Catalog of every component declared by every installed package.
        """
        return cls(components=dict(cls._declared_definitions()))

    # -- Discovery internals ---------------------------------------------------
    @classmethod
    def _declared_classes(cls) -> tuple[type[Component], ...]:
        """Component classes declared under ``interloper.components``.

        Each entry names a component class directly, or a module whose public
        attributes are scanned for component classes.

        Returns:
            Every declared component class.
        """
        classes: list[type[Component]] = []
        for entry_point in entry_points(group=_ENTRY_POINT):
            loaded = entry_point.load()
            if isinstance(loaded, ModuleType):
                for attribute_name in dir(loaded):
                    member = getattr(loaded, attribute_name)
                    if isinstance(member, type) and issubclass(member, Component):
                        classes.append(member)
            elif isinstance(loaded, type) and issubclass(loaded, Component):
                classes.append(loaded)
        return tuple(classes)

    @classmethod
    def _with_dependencies(cls, components: Iterable[type[Component]]) -> list[type[Component]]:
        """Close *components* over what they depend on.

        A component's dependencies are its resource classes and, for an asset,
        its destination classes. A source's assets are not catalog entries
        (they are reached through their source), but their dependencies are
        the source's too. The walk is transitive (a connection's own resources
        come along), so the catalog never carries a relation slot whose key it
        cannot resolve.

        Args:
            components: The explicitly enabled component classes.

        Returns:
            The enabled classes followed by their dependencies, each once, in
            discovery order.
        """
        closed: list[type[Component]] = []
        pending = list(components)
        while pending:
            component = pending.pop(0)
            if component in closed:
                continue
            closed.append(component)
            pending.extend(cls._dependencies_of(component))
            if issubclass(component, Source):
                for asset_type in component.asset_types:
                    pending.extend(cls._dependencies_of(asset_type))
        return closed

    @classmethod
    def _dependencies_of(cls, component: type[Component]) -> list[type[Component]]:
        """The component classes *component* directly depends on.

        Args:
            component: The class whose declared resource (and, for an asset,
                destination) classes are wanted.

        Returns:
            The direct dependencies, resources first.
        """
        dependencies: list[type[Component]] = list(component.resource_types.values())
        if issubclass(component, Asset):
            dependencies.extend(component.destination_types)
        return dependencies

    @classmethod
    def _definitions_from(cls, components: Iterable[type[Component]]) -> dict[str, ComponentDefinition]:
        """Build definitions from component classes.

        Every class self-describes through ``definition()``; nothing is walked,
        inferred, or registered — the catalog contains exactly what was
        declared, and kinds must already be registered when it loads.

        Args:
            components: The component classes to define; non-component entries
                are skipped, and the first definition per key wins.

        Returns:
            Mapping from component key to definition.

        Raises:
            ConfigError: If a declared component's kind has no registered
                anchor — declare it under the ``interloper.kinds`` group.
        """
        definitions: dict[str, ComponentDefinition] = {}
        for component in components:
            if not (isinstance(component, type) and issubclass(component, Component)):
                continue
            if component.kind not in KINDS:
                raise ConfigError(
                    f"Component '{component.key}' declares kind '{component.kind}', which is not registered — "
                    "declare its anchor under the 'interloper.kinds' entry-point group"
                )
            definitions.setdefault(component.key, component.definition())
        return definitions

    @classmethod
    @cache
    def _declared_definitions(cls) -> dict[str, ComponentDefinition]:
        """Definitions of the declared universe (cached).

        Returns:
            Mapping from component key to definition.
        """
        return cls._definitions_from(cls._declared_classes())

    @classmethod
    @cache
    def _framework_definitions(cls) -> dict[str, ComponentDefinition]:
        """Definitions of the framework's own declared components (cached).

        The framework is the ``interloper`` package: its declarations (jobs,
        hooks) are infrastructure every deployment runs on, not content a
        deployment curates, so they are present in every catalog.

        Returns:
            Mapping from component key to definition.
        """
        return cls._definitions_from(
            component for component in cls._declared_classes() if component.__module__.partition(".")[0] == "interloper"
        )
