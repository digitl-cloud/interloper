"""Catalog: flat registry of all component definitions keyed by component key.

Registration is two entry-point groups, and nothing else:

- ``interloper.kinds`` — what kinds exist: one entry per kind, naming its
  anchor class (consumed by :data:`interloper.KINDS`).
- ``interloper.components`` — what component classes exist: every concrete
  component an installed package provides, framework classes included
  (core declares ``cron_job``/``trigger_hook``/``webhook_hook`` here, the
  same way ``interloper-assets`` declares its connectors).

Every catalog contains the full declared universe — installation is the
opt-in. The ``AppSettings.catalog`` import paths *add* components that are
not shipped as entry points (e.g. local deployment-specific classes); they
never narrow. Kind anchors are framework, not content: they live in the
registry and never appear in the catalog.

Usage::

    import interloper as il

    catalog = il.Catalog.from_settings()   # universe + configured extras
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
from interloper.component import KINDS, Component, ComponentDefinition
from interloper.errors import ConfigError
from interloper.settings import AppSettings
from interloper.source.base import Source

logger = logging.getLogger(__name__)

_ENTRY_POINT = "interloper.components"


# ------------------------------------------------------------------
# Discovery
# ------------------------------------------------------------------


def _declared_classes() -> tuple[type[Component], ...]:
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
            for attr in dir(loaded):
                obj = getattr(loaded, attr)
                if isinstance(obj, type) and issubclass(obj, Component):
                    classes.append(obj)
        elif isinstance(loaded, type) and issubclass(loaded, Component):
            classes.append(loaded)
    return tuple(classes)


def _definitions_from(components: Iterable[type[Component]]) -> dict[str, ComponentDefinition]:
    """Build definitions from component classes.

    Every class self-describes through ``definition()``; nothing is walked,
    inferred, or registered — the catalog contains exactly what was
    declared, and kinds must already be registered when it loads.

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


@cache
def _declared_definitions() -> dict[str, ComponentDefinition]:
    """Definitions of the declared universe (cached).

    Returns:
        Mapping from component key to definition.
    """
    return _definitions_from(_declared_classes())


def _with_declared(definitions: dict[str, ComponentDefinition]) -> dict[str, ComponentDefinition]:
    """Ensure the declared universe is present.

    Every catalog contains every component the installed packages declare —
    installation is the opt-in. Explicitly provided definitions win over
    declared ones with the same key.

    Returns:
        The same mapping, with any missing declared definitions added.
    """
    for key, definition in _declared_definitions().items():
        definitions.setdefault(key, definition)
    return definitions


class Catalog(BaseModel):
    """Catalog of all component definitions."""

    components: dict[str, ComponentDefinition] = Field(default_factory=dict)

    # ------------------------------------------------------------------
    # Lookup & export
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_paths(cls, paths: list[str]) -> Catalog:
        """Build a catalog from import paths, plus the declared universe.

        Args:
            paths: Fully qualified import paths to additional component
                classes (e.g. local components not shipped as entry points,
                or a ``to_paths()`` list crossing a process boundary).

        Returns:
            Catalog of all component definitions.
        """
        from interloper.utils.imports import import_from_path

        classes: list[type[Component]] = []
        for path in paths:
            try:
                loaded = import_from_path(path)
            except (ImportError, AttributeError) as exc:
                logger.warning("Failed to import component '%s': %s", path, exc)
                continue
            if isinstance(loaded, type) and issubclass(loaded, Component):
                classes.append(loaded)

        return Catalog(components=_with_declared(_definitions_from(classes)))

    @classmethod
    def from_settings(cls) -> Catalog:
        """Load the catalog, adding any ``AppSettings.catalog`` import paths.

        The declared universe is always present; the configured paths add
        components that are not shipped as entry points (e.g. local
        deployment-specific classes).

        Returns:
            Catalog of all component definitions.
        """
        settings = AppSettings.get()
        return cls.from_paths(settings.catalog or [])

    @classmethod
    def from_assets(cls, sources_or_assets: list[type[Source | Asset]]) -> Catalog:
        """Load catalog from a list of asset classes.

        Returns:
            Catalog of all component definitions.
        """
        return cls(components=_with_declared(_definitions_from(sources_or_assets)))

    @classmethod
    def discover(cls) -> Catalog:
        """Load the declared universe from ``interloper.components`` entry points.

        Returns:
            Catalog of every component declared by every installed package.
        """
        return cls.from_paths([])
