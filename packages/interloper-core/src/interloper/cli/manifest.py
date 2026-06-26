"""Declarative run manifest: a YAML spec compiled into an executable run plan.

A run manifest is a human-authored YAML file that declares *what* to
materialize — sources/assets with their configuration, destinations, an
optional runner override, and a partition — without any Python code.  It is
the declarative counterpart of building a :class:`~interloper.dag.base.DAG`
by hand, and feeds the exact same execution machinery (``interloper run``).

The manifest is a *workload* description and deliberately does not carry
environment concerns: the catalog and the default runner come from
``interloper.yaml`` / ``INTERLOPER_*`` env vars (:class:`AppSettings`), and
the manifest only references or overrides them.

Example::

    name: facebook-backfill

    runner:                       # optional, overrides AppSettings.runner
      type: async
      config:
        max_concurrency: 4

    resources:                    # reusable Resources (connections, …), by alias
      gcp:
        type: google_cloud_connection
        config:
          service_account_key: ${GCP_KEY}

    destinations:                 # reusable Destinations, by alias
      bq:
        type: bigquery_destination
        auto: true                # used by any item that declares no destinations
        config:
          connection: {ref: gcp}  # refs may nest

    assets:
      - source: facebook_ads      # catalog key or dotted import path
        config:                   # init kwargs for the source
          dataset: raw_facebook
        select: [campaigns, ads]  # no destinations → writes to the auto 'bq'
      - asset: demo_asset         # standalone assets work too
        destinations: [{ref: bq}] # explicit (one or more; writes fan out)

    partition:
      date: 2026-06-01            # or start/end for a window

Auto-use
--------

A ``resources``/``destinations`` entry marked ``auto: true`` is applied to
every source/standalone asset that does not provide its own: an auto
destination becomes the item's destination when it declares none, and an auto
resource fills any empty resource slot it satisfies by type.  Explicit
``destinations`` and ``config`` resources always take precedence.

Component references
--------------------

Wherever a component instance is expected (a ``destinations`` entry, or any
value nested inside a ``config`` block), two forms are recognized:

* ``{type: <ref>, config: {...}}`` — an inline definition.  The ``type`` is
  resolved against the catalog when it is a bare key (no dots), or imported
  directly when it is a dotted/composite path.
* ``{ref: <alias>}`` — a reference to a component declared in the top-level
  ``resources`` or ``destinations`` block.  Each alias is instantiated once
  and the *same instance* is reused everywhere it is referenced, so a single
  client or auth token is shared across sources.  (Sharing is in-process:
  runners that serialize a per-asset sub-DAG to a child process rebuild their
  own instances there.)

Environment interpolation
-------------------------

``${VAR}`` placeholders in any string value are replaced from the process
environment at load time, so credentials never need to live in the file.
Unresolved variables are a hard error.
"""

from __future__ import annotations

import datetime as dt
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from interloper.errors import ManifestError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from interloper.asset.base import Asset
    from interloper.component import Component
    from interloper.dag.base import DAG
    from interloper.destination import Destination
    from interloper.partitioning import Partition, PartitionWindow
    from interloper.source.base import Source

_ENV_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

# A mapping with this exact key shape (and a string ``type``) is treated as
# a component reference anywhere inside a ``config`` block.
_COMPONENT_REF_KEYS = frozenset({"type", "config", "id"})


def _interpolate_env(value: Any, missing: set[str]) -> Any:
    """Recursively substitute ``${VAR}`` placeholders in string values.

    Unknown variables are collected into *missing* (and left in place) so
    the caller can report them all at once.

    Returns:
        The value with all resolvable placeholders substituted.
    """
    if isinstance(value, str):

        def sub(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in os.environ:
                missing.add(name)
                return match.group(0)
            return os.environ[name]

        return _ENV_VAR_RE.sub(sub, value)
    if isinstance(value, dict):
        return {k: _interpolate_env(v, missing) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate_env(v, missing) for v in value]
    return value


def _is_component_ref(value: Any) -> bool:
    """Check whether a value is a component-reference mapping.

    Returns:
        True for mappings shaped like ``{type: str, config?: dict, id?: str}``.
    """
    return (
        isinstance(value, dict)
        and isinstance(value.get("type"), str)
        and set(value) <= _COMPONENT_REF_KEYS
    )


def _is_ref(value: Any) -> bool:
    """Check whether a value references a ``resources``/``destinations`` component.

    Returns:
        True for mappings shaped like ``{ref: <alias>}``.
    """
    return isinstance(value, dict) and set(value) == {"ref"} and isinstance(value.get("ref"), str)


class _Resolver:
    """Resolve component type references to classes and build instances.

    Bare keys (no ``.`` or ``:``) are looked up in the catalog built from
    ``AppSettings.catalog`` (loaded lazily, at most once); dotted or
    composite paths are imported directly.

    The named ``resources`` and ``destinations`` blocks share a single alias
    namespace, resolved on demand via :meth:`resolve_ref`; each alias is built
    at most once and the instance is cached, so every ``{ref: <alias>}``
    returns the same object.  An entry must satisfy the kind of the block it is
    declared under (``resources`` → :class:`Resource`, ``destinations`` →
    :class:`Destination`).
    """

    def __init__(
        self,
        resources: Mapping[str, ComponentManifest] | None = None,
        destinations: Mapping[str, ComponentManifest] | None = None,
    ) -> None:
        self._catalog: Any = None
        self._instances: dict[str, Component] = {}
        self._resolving: set[str] = set()
        # Merge the two named blocks into one alias namespace, recording the
        # block each alias came from so its built instance can be kind-checked.
        self._registry: dict[str, ComponentManifest] = {}
        self._kinds: dict[str, str] = {}
        for block, entries in (("resources", resources or {}), ("destinations", destinations or {})):
            for alias, manifest in entries.items():
                if alias in self._registry:
                    raise ManifestError(
                        f"Alias '{alias}' is declared in both 'resources' and 'destinations'"
                    )
                self._registry[alias] = manifest
                self._kinds[alias] = block

    def resolve_ref(self, alias: str) -> Component:
        """Resolve an alias to its (cached) component instance.

        Returns:
            The referenced component instance, built once and memoized.

        Raises:
            ManifestError: If the alias is undefined, part of a reference cycle,
                or does not match the kind of its declaring block.
        """
        if alias in self._instances:
            return self._instances[alias]
        if alias not in self._registry:
            raise ManifestError(f"Unknown reference '{alias}'. Declared: {sorted(self._registry) or 'none'}")
        if alias in self._resolving:
            raise ManifestError(f"Circular reference involving '{alias}'")
        self._resolving.add(alias)
        try:
            instance = self.build_component(self._registry[alias])
        finally:
            self._resolving.discard(alias)
        self._check_kind(alias, instance)
        self._instances[alias] = instance
        return instance

    def _check_kind(self, alias: str, instance: Component) -> None:
        """Ensure a resolved alias matches the kind of its declaring block.

        Raises:
            ManifestError: If a ``resources`` alias is not a ``Resource`` or a
                ``destinations`` alias is not a ``Destination``.
        """
        from interloper.destination import Destination
        from interloper.resource import Resource

        if self._kinds[alias] == "resources" and not isinstance(instance, Resource):
            raise ManifestError(f"'{alias}' is declared under 'resources' but is not a Resource")
        if self._kinds[alias] == "destinations" and not isinstance(instance, Destination):
            raise ManifestError(f"'{alias}' is declared under 'destinations' but is not a Destination")

    def build_destination(self, entry: RefManifest | ComponentManifest) -> Destination:
        """Resolve a ``destinations`` entry (ref or inline) to a Destination.

        Returns:
            The destination instance.

        Raises:
            ManifestError: If the entry does not resolve to a ``Destination``.
        """
        from interloper.destination import Destination

        if isinstance(entry, RefManifest):
            instance: Component = self.resolve_ref(entry.ref)
            label = f"reference '{entry.ref}'"
        else:
            instance = self.build_component(entry)
            label = f"'{entry.type}'"
        if not isinstance(instance, Destination):
            raise ManifestError(f"{label} is not a Destination")
        return instance

    def resolve(self, ref: str) -> type:
        """Resolve a type reference to a class.

        Returns:
            The resolved class.

        Raises:
            ManifestError: If the reference cannot be resolved to a class.
        """
        from interloper.utils.imports import import_from_path

        if "." in ref or ":" in ref:
            path = ref
        else:
            if self._catalog is None:
                from interloper.catalog import Catalog

                self._catalog = Catalog.from_settings()
            defn = self._catalog.get(ref)
            if defn is None:
                raise ManifestError(
                    f"Unknown catalog key '{ref}'. Use a key from the configured catalog "
                    f"or a fully qualified import path."
                )
            path = defn.path

        try:
            obj = import_from_path(path)
        except (ImportError, AttributeError) as exc:
            raise ManifestError(f"Failed to import '{path}': {exc}") from exc
        if not isinstance(obj, type):
            raise ManifestError(f"'{path}' did not resolve to a class")
        return obj

    def build_component(self, ref: ComponentManifest) -> Component:
        """Instantiate a component from its manifest reference.

        Returns:
            The component instance.

        Raises:
            ManifestError: If the class is not a Component or instantiation fails.
        """
        from interloper.component import Component

        cls = self.resolve(ref.type)
        if not issubclass(cls, Component):
            raise ManifestError(f"'{ref.type}' is not an interloper component")
        kwargs = self.build_init(ref.config)
        if ref.id:
            kwargs["id"] = ref.id
        try:
            return cls(**kwargs)
        except Exception as exc:
            raise ManifestError(f"Failed to instantiate '{ref.type}': {exc}") from exc

    def build_init(self, config: dict[str, Any]) -> dict[str, Any]:
        """Build init kwargs from a config block, instantiating nested component refs.

        Returns:
            The kwargs with every nested component reference replaced by an instance.
        """
        return {k: self._build_value(v) for k, v in config.items()}

    def _build_value(self, value: Any) -> Any:
        if _is_ref(value):
            return self.resolve_ref(value["ref"])
        if _is_component_ref(value):
            return self.build_component(ComponentManifest.model_validate(value))
        if isinstance(value, dict):
            return {k: self._build_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._build_value(v) for v in value]
        return value


class _ManifestModel(BaseModel):
    """Base for manifest models: unknown keys are typos, reject them."""

    model_config = ConfigDict(extra="forbid")


class ComponentManifest(_ManifestModel):
    """Reference to a component to instantiate: catalog key or import path plus init kwargs."""

    type: str
    config: dict[str, Any] = Field(default_factory=dict)
    id: str = ""


class RegistryEntryManifest(ComponentManifest):
    """A ``resources``/``destinations`` registry entry.

    ``auto`` opts the component into automatic use: an auto destination becomes
    the destination of any item that declares none, and an auto resource fills
    any empty resource slot it satisfies by type. Explicit references always win.
    """

    auto: bool = False


class RefManifest(_ManifestModel):
    """Reference to a component declared in the manifest's ``resources``/``destinations`` blocks."""

    ref: str


class RunnerManifest(_ManifestModel):
    """Runner override: same shape as the ``runner`` block of ``interloper.yaml``."""

    type: str
    config: dict[str, Any] = Field(default_factory=dict)


class PartitionManifest(_ManifestModel):
    """Partition selector: a single ``date`` or a ``start``/``end`` window."""

    date: dt.date | None = None
    start: dt.date | None = None
    end: dt.date | None = None

    @model_validator(mode="after")
    def _check_exclusive(self) -> PartitionManifest:
        has_window = self.start is not None or self.end is not None
        if self.date is not None and has_window:
            raise ValueError("'date' cannot be combined with 'start'/'end'")
        if has_window and (self.start is None or self.end is None):
            raise ValueError("both 'start' and 'end' must be provided for a window")
        if self.date is None and not has_window:
            raise ValueError("either 'date' or 'start'/'end' must be provided")
        return self

    def resolve(self) -> Partition | PartitionWindow:
        """Convert to a framework partition object.

        Returns:
            A ``TimePartition`` for ``date``, a ``TimePartitionWindow`` for a window.
        """
        from interloper.partitioning import TimePartition, TimePartitionWindow

        if self.date is not None:
            return TimePartition(self.date)
        assert self.start is not None and self.end is not None
        return TimePartitionWindow(start=self.start, end=self.end)


class AssetItemManifest(_ManifestModel):
    """One DAG item: a source (with optional asset selection) or a standalone asset."""

    source: str | None = None
    asset: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    destinations: list[RefManifest | ComponentManifest] | None = None
    select: list[str] | None = None

    @model_validator(mode="after")
    def _check_shape(self) -> AssetItemManifest:
        if (self.source is None) == (self.asset is None):
            raise ValueError("exactly one of 'source' or 'asset' must be set")
        if self.select is not None and self.source is None:
            raise ValueError("'select' is only valid on source items")
        return self


@dataclass
class RunPlan:
    """The compiled, executable form of a run manifest."""

    dag: DAG
    partition: Partition | PartitionWindow | None
    runner: RunnerManifest | None
    name: str = ""


class RunManifest(_ManifestModel):
    """Declarative description of a materialization run.

    Load with :meth:`from_yaml_file` / :meth:`from_yaml` and turn into an
    executable :class:`RunPlan` with :meth:`compile`.
    """

    name: str = ""
    runner: RunnerManifest | None = None
    resources: dict[str, RegistryEntryManifest] = Field(default_factory=dict)
    destinations: dict[str, RegistryEntryManifest] = Field(default_factory=dict)
    assets: list[AssetItemManifest] = Field(min_length=1)
    partition: PartitionManifest | None = None

    @classmethod
    def from_yaml_file(cls, path: str | Path) -> RunManifest:
        """Load a manifest from a YAML file.

        Returns:
            The validated manifest.

        Raises:
            ManifestError: If the file is missing, unparsable, references
                undefined environment variables, or fails validation.
        """
        path = Path(path)
        try:
            text = path.read_text()
        except OSError as exc:
            raise ManifestError(f"Cannot read manifest file '{path}': {exc}") from exc
        return cls.from_yaml(text, origin=str(path))

    @classmethod
    def from_yaml(cls, text: str, *, origin: str = "<string>") -> RunManifest:
        """Load a manifest from YAML text, interpolating ``${VAR}`` placeholders.

        Returns:
            The validated manifest.

        Raises:
            ManifestError: On parse, interpolation, or validation failure.
        """
        import yaml
        from pydantic import ValidationError

        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise ManifestError(f"Invalid YAML in manifest '{origin}': {exc}") from exc
        if not isinstance(data, dict):
            raise ManifestError(f"Manifest '{origin}' must be a YAML mapping")

        missing: set[str] = set()
        data = _interpolate_env(data, missing)
        if missing:
            raise ManifestError(
                f"Manifest '{origin}' references undefined environment variable(s): "
                f"{', '.join(sorted(missing))}"
            )

        try:
            return cls.model_validate(data)
        except ValidationError as exc:
            raise ManifestError(f"Invalid manifest '{origin}': {exc}") from exc

    def compile(self) -> RunPlan:
        """Compile the manifest into an executable run plan.

        Resolves every component reference, instantiates sources/assets with
        their config and destinations, applies asset selection, and builds
        the (validated) DAG.

        Returns:
            The compiled run plan.

        Raises:
            ManifestError: If any reference fails to resolve or instantiate,
                or a ``select`` entry names an unknown asset.
        """
        from interloper.asset.base import Asset
        from interloper.dag.base import DAG
        from interloper.source.base import Source

        resolver = _Resolver(resources=self.resources, destinations=self.destinations)

        # Components opted into automatic use. resolve_ref kind-checks them, so
        # auto_resources are Resources and auto_destinations are Destinations.
        auto_resources = [resolver.resolve_ref(alias) for alias, entry in self.resources.items() if entry.auto]
        auto_destinations = [resolver.resolve_ref(alias) for alias, entry in self.destinations.items() if entry.auto]

        items: list[Source | Asset] = []
        for item in self.assets:
            ref = item.source or item.asset
            assert ref is not None
            cls = resolver.resolve(ref)
            expected = Source if item.source else Asset
            if not issubclass(cls, expected):
                raise ManifestError(f"'{ref}' is not a {expected.__name__} subclass")

            kwargs = resolver.build_init(item.config)
            if item.destinations is not None:
                if "destination" in kwargs:
                    raise ManifestError(
                        f"'{ref}' sets a destination in both 'config' and the 'destinations' field"
                    )
                kwargs["destination"] = [resolver.build_destination(d) for d in item.destinations]
            elif "destination" not in kwargs and auto_destinations:
                kwargs["destination"] = list(auto_destinations)

            if auto_resources:
                _apply_auto_resources(cls, kwargs, auto_resources)

            try:
                instance = cls(**kwargs)
            except Exception as exc:
                raise ManifestError(f"Failed to instantiate '{ref}': {exc}") from exc

            if isinstance(instance, Source) and item.select is not None:
                instance.assets = _select_assets(instance, item.select)
            items.append(instance)

        try:
            dag = DAG(*items)
        except Exception as exc:
            raise ManifestError(f"Failed to build DAG: {exc}") from exc

        return RunPlan(
            dag=dag,
            partition=self.partition.resolve() if self.partition is not None else None,
            runner=self.runner,
            name=self.name,
        )


def _apply_auto_resources(cls: type, kwargs: dict[str, Any], auto_resources: list[Component]) -> None:
    """Fill *cls*'s empty resource slots in *kwargs* from auto-use resources.

    A slot is left untouched when already provided (via ``resources`` or a
    slot-named kwarg from ``config``); otherwise it is filled by the first
    auto resource that satisfies the slot's declared type. Mutates *kwargs*.
    """
    resource_types: dict[str, type] = getattr(cls, "resource_types", {})
    provided = set(kwargs.get("resources") or {}) | (set(kwargs) & set(resource_types))

    additions: dict[str, Component] = {}
    for name, res_type in resource_types.items():
        if name in provided:
            continue
        match = next((res for res in auto_resources if isinstance(res, res_type)), None)
        if match is not None:
            additions[name] = match

    if additions:
        kwargs["resources"] = {**(kwargs.get("resources") or {}), **additions}


def _select_assets(source: Source, select: list[str]) -> list[Asset]:
    """Apply an asset selection to a source's instantiated assets.

    Unselected assets stay in the list but are marked non-materializable,
    so intra-source dependency wiring keeps validating while only the
    selected assets execute (same mechanism as :meth:`DAG.mini_dag`).

    Returns:
        The new asset list with the selection applied.

    Raises:
        ManifestError: If a selected key matches no asset of the source.
    """
    known = {type(a).key for a in source.assets}
    unknown = [k for k in select if k not in known]
    if unknown:
        raise ManifestError(
            f"Source '{type(source).key}' has no asset(s) {unknown}; available: {sorted(known)}"
        )
    selected = set(select)
    return [a if type(a).key in selected else a(materializable=False) for a in source.assets]
