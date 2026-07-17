"""Source: a component that groups assets with shared resources and destinations."""

from __future__ import annotations

import inspect
from typing import Any, ClassVar

from pydantic import Field, field_validator, model_validator
from typing_extensions import Self

from interloper.asset import Asset
from interloper.asset.base import AssetDefinition
from interloper.component import Component, ComponentDefinition, RelationDefinition
from interloper.destination import Destination
from interloper.normalizer import MaterializationStrategy, Normalizer
from interloper.resource import Resource
from interloper.resource.fields import InputField, SelectField, validate_fetch_field_providers
from interloper.serializable import IgnoredDescriptor, Spec, dump_spec_value
from interloper.utils.imports import get_object_path
from interloper.utils.text import to_label, validate_key


class AssetRef(IgnoredDescriptor):
    """Class attribute that exposes a source-owned asset.

    At **class access** (``FacebookAds.campaigns``) returns the asset
    *class* — this is what makes ``import_from_path`` work on composite
    paths like ``"module:FacebookAds.campaigns"`` without ever having to
    instantiate the source.

    At **instance access** (``facebook_ads.campaigns``) returns the live
    asset instance owned by that source.

    Replaces the old ``Source.__getattr__`` mechanism, which only worked
    at instance level and forced reconstruction code to instantiate the
    source just to reach its assets.
    """

    def __init__(self, asset_cls: type[Asset]) -> None:
        """Bind this descriptor to an asset class."""
        self.asset_cls = asset_cls
        self.attr_name: str = ""

    def __set_name__(self, owner: type, name: str) -> None:
        """Capture the attribute name this descriptor was installed under."""
        self.attr_name = name

    def __get__(self, instance: Any, owner: type | None = None) -> Any:
        """Return the asset class at class access, the instance at instance access.

        Raises:
            AttributeError: If accessed on an instance whose ``assets`` list
                contains no asset matching this descriptor's key.
        """
        if instance is None:
            return self.asset_cls
        for asset in instance.assets:
            if type(asset).key == self.asset_cls.key:
                return asset
        raise AttributeError(
            f"Source '{type(instance).__name__}' has no asset with key '{self.asset_cls.key}'"
        )


class SourceDefinition(ComponentDefinition):
    """Definition of a source including its nested asset definitions.

    Cross-entity references use keys:
    - ``resources`` maps slot name → resource catalog key

    Same-entity data is inlined:
    - ``assets`` are owned by this source, so their definitions are nested
    """

    assets: list[AssetDefinition] = Field(default_factory=list)


class Source(Component):
    """A grouping component that holds assets with shared resources and destinations.

    Define a source by subclassing and setting class attributes::

        class MySource(Source):
            resource_types = {"config": ProdConfig}
            destinations = [PostgresDest(connection="...")]
            asset_types = [Users, Orders]

    Access assets by key via attribute access::

        source = MySource()
        source.users  # returns the Asset with key "users"
    """

    # Definition
    destination_types: ClassVar[list[type[Destination]]] = []
    asset_types: ClassVar[list[type[Asset]]] = []
    tags: ClassVar[list[str]] = []
    runnable: ClassVar[bool] = True
    relation_types: ClassVar[dict[str, RelationDefinition]] = {
        "resource": RelationDefinition(kinds=["connection", "config", "resource"], field="resources", slotted=True),
        "destination": RelationDefinition(kinds=["destination"], field="destinations"),
    }
    internal_fields: ClassVar[frozenset[str]] = frozenset({"assets", "destinations", "normalizer", "select"})

    # State
    destinations: list[Destination] = Field(default_factory=list)
    normalizer: Normalizer | None = Field(default=None)
    materialization_strategy: MaterializationStrategy | None = Field(
        default=None,
        title="Materialization Strategy",
        description=(
            "Default strategy for this source's assets: 'auto' validates "
            "against the schema, 'strict' fails on any mismatch, 'reconcile' "
            "coerces values to the schema. Assets declaring their own "
            "strategy keep it; leave empty to use each asset's default."
        ),
    )
    assets: list[Asset] = Field(default_factory=list)
    select: list[str] | None = Field(
        default=None, description="Asset keys to materialize; others stay as read-only dependencies"
    )

    @field_validator("destinations", mode="before")
    @classmethod
    def _coerce_destinations(cls, value: Any) -> Any:
        """Accept a single destination or ``None`` where a list is expected.

        Returns:
            The value as a list.
        """
        if value is None:
            return []
        return value if isinstance(value, (list, tuple)) else [value]

    # Exposed fields
    dataset: str = InputField(default="", description="Defaults to the source key when left empty")
    default_destination_key: str = SelectField(
        title="Default Destination",
        default="",
        options_from="destinations",
        description="When an asset has multiple destinations, downstream assets use this to know where to read from",
    )

    # -- Construction & resolution ---------------------------------------------

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Auto-discover assets and infer requires at source definition time.

        Asset classes defined in the source body (via ``@asset`` on
        methods) appear as class attributes.  We collect them into
        ``asset_types`` and replace each class attribute with an
        :class:`AssetRef` descriptor that exposes the class at class
        level and the live instance at instance level.
        """
        super().__init_subclass__(**kwargs)
        cls._collect_asset_types()
        cls._infer_all_requires()

    @model_validator(mode="before")
    @classmethod
    def _apply_asset_overrides(cls, data: Any) -> Any:
        """Convert an ``assets`` override map into a list of asset instances.

        Supports two shapes for the ``assets`` init kwarg:

        - ``list[Asset]`` — a pre-built list; passed through untouched.
        - ``dict[str, dict]`` — an override map keyed by asset key, where
          each value is the ``init`` payload for that asset.  The source
          is the unit of reconstruction: for every entry in
          ``asset_types`` we build ``asset_cls(**overrides)``, defaulting
          unmapped keys to a bare ``asset_cls()``.

        This is what ``Source.to_spec()`` emits and what
        ``Spec.reconstruct()`` hands back in after the walker
        has resolved any nested component specs inside the overrides.

        Returns:
            The (possibly rewritten) input data.
        """
        if not isinstance(data, dict):
            return data
        assets = data.get("assets")
        if not isinstance(assets, dict):
            return data

        instances: list[Asset] = []
        for asset_cls in cls.asset_types:
            if asset_cls.key in assets:
                instances.append(asset_cls(**assets[asset_cls.key]))
        data["assets"] = instances
        return data

    @classmethod
    def relation_definitions(cls) -> dict[str, RelationDefinition]:
        """Enrich the vocabulary with the source's allowed destination keys.

        Returns:
            Relation type → enriched definition.
        """
        relations = super().relation_definitions()
        if "destination" in relations:
            relations["destination"] = relations["destination"].model_copy(
                update={"keys": [dest_cls.key for dest_cls in cls.destination_types]}
            )
        return relations

    def model_post_init(self, context: Any) -> None:
        """Instantiate default asset types (if none were supplied) and resolve trickle-down fields."""
        super().model_post_init(context)
        if not self.assets:
            self.assets = [cls() for cls in self.asset_types]
        self._resolve()
        if self.select is not None:
            self._apply_select()

    def _apply_select(self) -> None:
        """Mark assets outside ``select`` as non-materializable.

        Unselected assets stay in the list so intra-source dependency wiring
        keeps validating (and their outputs stay readable), but only the
        selected assets execute — the same mechanism as
        :meth:`~interloper.dag.base.DAG.mini_dag`.

        Raises:
            SourceError: If a selected key matches no asset of this source.
        """
        from interloper.errors import SourceError

        known = {type(a).key for a in self.assets}
        unknown = [k for k in self.select or [] if k not in known]
        if unknown:
            raise SourceError(f"Source '{type(self).key}' has no asset(s) {unknown}; available: {sorted(known)}")
        selected = set(self.select or [])
        self.assets = [a if type(a).key in selected else a(materializable=False) for a in self.assets]
        for asset in self.assets:
            asset._source = self

    # -- Serialization ---------------------------------------------------------

    def to_spec(self) -> Spec:
        """Serialize to a spec with ``assets`` as a key → init override map.

        Source is the unit of reconstruction: each asset's own state is
        serialised as a plain dict under the asset's key, with no
        individual ``path``.  This mirrors :meth:`_apply_asset_overrides`
        on the reconstruction side, and keeps the source spec compact
        (no duplicated class paths, no nested ``Spec`` wrapping
        for each asset).

        Returns:
            A ``Spec`` capturing this source and its assets.
        """
        init: dict[str, Any] = {}
        for name in type(self).model_fields:
            if name == "id":
                continue
            value = getattr(self, name)
            if value is None:
                continue
            if name == "assets":
                overrides: dict[str, Any] = {}
                for asset in value:
                    asset_spec = asset.to_spec()
                    asset_init = dict(asset_spec.init or {})
                    if asset_spec.id:
                        # Preserve instance id so dependencies wire up after round-trip
                        asset_init["id"] = asset_spec.id
                    overrides[type(asset).key] = asset_init
                if overrides:
                    init["assets"] = overrides
                continue
            init[name] = dump_spec_value(value)

        return Spec(path=self.path(), id=self.id, init=init or None)

    # -- Assets ----------------------------------------------------------------

    @classmethod
    def _collect_asset_types(cls) -> None:
        """Collect Asset subclasses from the class namespace into ``asset_types``.

        When ``@asset`` decorates a method in the source body, it
        transforms the method into an Asset class and sets it as a class
        attribute.  This method collects those into ``asset_types`` and
        replaces each entry with an :class:`AssetRef` descriptor, which
        exposes the class at class-level access and the live asset
        instance at instance-level access.
        """
        from interloper.asset.base import Asset

        # Only process assets defined directly on this class, not inherited.
        own: list[tuple[str, type[Asset]]] = []
        for attr_name, value in list(cls.__dict__.items()):
            if isinstance(value, type) and issubclass(value, Asset) and value is not Asset:
                own.append((attr_name, value))

        if own:
            # Merge with any asset_types already set (e.g. by the decorator).
            existing = list(cls.__dict__.get("asset_types", []))
            existing_keys = {a.key for a in existing}
            for _, asset_cls in own:
                if asset_cls.key not in existing_keys:
                    existing.append(asset_cls)
            cls.asset_types = existing

            # Replace the raw class attribute with a descriptor so that
            # class access returns the class and instance access returns
            # the live asset instance.
            for attr_name, asset_cls in own:
                ref = AssetRef(asset_cls)
                ref.__set_name__(cls, attr_name)
                setattr(cls, attr_name, ref)

    @classmethod
    def register_asset_type(cls, asset_cls: type[Asset]) -> None:
        """Register an Asset subclass as a child of this source post-hoc.

        Appends to ``asset_types`` (if not already present) and installs
        an :class:`AssetRef` descriptor under the asset class's
        ``__name__``, so that ``import_from_path`` can reach the asset
        via the composite ``"module:Source.AssetName"`` form.

        Normally assets are collected automatically from the class body
        by :meth:`_collect_asset_types`.  This classmethod exists for
        imperative registration (e.g. in tests or dynamic source
        composition) where the asset isn't a class-body attribute.

        Args:
            asset_cls: The Asset subclass to register.
        """
        if not any(a is asset_cls for a in cls.asset_types):
            cls.asset_types = [*cls.asset_types, asset_cls]
        asset_cls._source_type = cls
        ref = AssetRef(asset_cls)
        ref.__set_name__(cls, asset_cls.__name__)
        setattr(cls, asset_cls.__name__, ref)

    @classmethod
    def _infer_all_requires(cls) -> None:
        """Populate ``requires`` and ``optional_requires`` on asset classes.

        Matches parameter names against sibling asset keys. Parameters
        with a ``None`` default are inferred as optional.
        """
        sibling_keys: set[str] = {a.key for a in cls.asset_types}
        for asset_cls in cls.asset_types:
            if not hasattr(asset_cls, "data"):
                continue
            sig = inspect.signature(asset_cls.data)
            inferred: dict[str, str] = {}
            inferred_optional: dict[str, str] = {}
            for param_name, param in sig.parameters.items():
                if param_name in ("self", "context", "source", "kwargs"):
                    continue
                if param_name in asset_cls.resource_types:
                    continue
                if param_name in asset_cls.requires:
                    continue
                if param_name in asset_cls.optional_requires:
                    continue
                if param_name in sibling_keys and param_name != asset_cls.key:
                    qualified = f"{cls.key}.{param_name}"
                    if param.default is None:
                        inferred_optional[param_name] = qualified
                    else:
                        inferred[param_name] = qualified
            if inferred:
                asset_cls.requires = {**asset_cls.requires, **inferred}
            if inferred_optional:
                asset_cls.optional_requires = {**asset_cls.optional_requires, **inferred_optional}

    @classmethod
    def asset_def(cls, key: str) -> AssetDefinition:
        """Look up an asset definition by key.

        Returns an :class:`AssetDefinition` with ``source_key`` set,
        so callers can use ``.qualified_key`` for cross-source references::

            FacebookAds.asset_def("campaigns").qualified_key
            # → "facebook_ads.campaigns"

        Args:
            key: The asset key (snake_cased class name).

        Returns:
            The asset definition with source context.

        Raises:
            KeyError: If no asset matches the key.
        """
        for asset_cls in cls.asset_types:
            if asset_cls.key == key:
                defn = asset_cls.definition()
                defn.source_key = cls.key
                return defn
        raise KeyError(f"Source '{cls.key}' has no asset with key '{key}'")

    def asset_table(self, asset: Asset) -> str:
        """Physical table name for one of this source's assets.

        Defaults to suffixing the asset key with the instance's
        :attr:`~interloper.component.base.Component.discriminator` (the config
        field marked ``discriminator=True``), so instances of a multi-account
        source materialize side by side in one dataset instead of overwriting
        each other's data. Without a discriminator the asset key is used as-is.

        Override for full control over the composition; keep the
        ``{asset.key}__{suffix}`` shape so tables stay wildcard-queryable per
        asset. The return value is coerced to a valid identifier by
        :attr:`Asset.table`.

        Returns:
            The physical table name for the asset.
        """
        discriminator = self.discriminator
        return f"{asset.key}__{discriminator}" if discriminator else asset.key

    def _resolve(self) -> None:
        """Apply source-level defaults to assets that don't define their own."""
        if not self.dataset:
            self.dataset = self.key
        validate_key(self.dataset)

        siblings: dict[str, Asset] = {type(a).key: a for a in self.assets}

        for asset in self.assets:
            asset._source = self
            if not asset.dataset:
                asset.dataset = self.dataset
            validate_key(asset.table)
            if not asset.default_destination_key and self.default_destination_key:
                asset.default_destination_key = self.default_destination_key
            if not asset.destinations and self.destinations:
                asset.destinations = list(self.destinations)
            if asset.normalizer is None and self.normalizer is not None:
                asset.normalizer = self.normalizer
            if (
                self.materialization_strategy is not None
                and asset.materialization_strategy == MaterializationStrategy.AUTO
            ):
                asset.materialization_strategy = self.materialization_strategy

            # Trickle source resources down to assets by name, then by type
            self.trickle_resources(asset)
            self._resolve_deps(asset, siblings)

        # Trickle source resources down to destinations
        for dest in self.destinations:
            self.trickle_resources(dest)

    def __getattr__(self, name: str) -> Asset:
        """Instance-level asset lookup fallback.

        At runtime, source-owned asset access is normally served by the
        :class:`AssetRef` descriptor installed on the class by
        :meth:`_collect_asset_types`, so Python never reaches this
        method.  It exists for two reasons:

        1. **Static analysis** — it tells type checkers that
           ``source.<asset_key>`` yields an :class:`~interloper.Asset`,
           since the dynamically-installed descriptors aren't visible to
           them.
        2. **Safety net** — sources built imperatively (e.g. in tests)
           that populate ``asset_types`` without going through
           :meth:`_collect_asset_types` still get ergonomic attribute
           access.

        Returns:
            The asset matching the given key.

        Raises:
            AttributeError: If no asset matches the given key.
        """
        # Delegate private attrs and Pydantic internals to BaseModel
        if name.startswith("_"):
            return super().__getattr__(name)  # ty: ignore[unresolved-attribute]
        for asset in self.assets:
            if type(asset).key == name:
                return asset
        raise AttributeError(f"Source has no asset with key '{name}'")

    def _resolve_deps(self, asset: Asset, siblings: dict[str, Asset]) -> None:
        """Wire intra-source dependencies for a single asset.

        Looks at ``requires`` and ``optional_requires`` entries whose
        qualified key belongs to this source.  If a sibling asset
        matches, wires it into ``asset.dependencies``.

        Pre-existing ``dependencies`` entries (e.g. hydrated from persisted
        relations) are never overwritten.
        """
        asset_cls = type(asset)
        for mapping in (asset_cls.requires, asset_cls.optional_requires):
            for param_name, required_qk in mapping.items():
                if param_name in asset.dependencies:
                    continue
                # Only wire intra-source: qualified key must belong to this source
                if "." in required_qk:
                    source_key, asset_key = required_qk.split(".", 1)
                    if source_key != type(self).key:
                        continue
                else:
                    asset_key = required_qk
                sibling = siblings.get(asset_key)
                if sibling is not None and sibling is not asset:
                    asset.dependencies[param_name] = sibling.id

    # -- Definition ------------------------------------------------------------

    @classmethod
    def definition(cls) -> SourceDefinition:
        """Produce a structured definition of this source including its assets.

        Returns:
            A SourceDefinition with metadata and nested asset definitions.
        """
        # Use the *resolved* resource map: it includes slots declared via typed
        # annotations (``connection: XConnection``), not just those set in
        # ``__dict__`` by the ``@source(resources=...)`` decorator. This is what
        # ``Catalog.from_paths`` and the FetchField resolver read, so the
        # SourceDefinition's ``resources`` (and the connection step / fetch
        # fields the frontend builds from it) stay consistent for both styles.
        res_types: dict[str, type[Resource]] = cls.resource_types
        validate_fetch_field_providers(cls, res_types)

        return SourceDefinition(
            kind=cls.kind,
            key=cls.key,
            path=get_object_path(cls),
            name=cls.name or to_label(cls.__name__),
            icon=cls.icon,
            description=cls.__doc__ or "",
            tags=list(cls.tags),
            config_schema=cls.config_schema(),
            relations=cls.relation_definitions(),
            assets=[asset_cls.definition().model_copy(update={"source_key": cls.key}) for asset_cls in cls.asset_types],
        )

    # -- Reconfiguration -------------------------------------------------------

    def __call__(
        self,
        *,
        resources: dict[str, Resource] | None = None,
        destinations: Destination | list[Destination] | None = None,
        dataset: str | None = None,
        default_destination_key: str | None = None,
        materializable: bool | None = None,
        normalizer: Normalizer | None = None,
        materialization_strategy: MaterializationStrategy | None = None,
    ) -> Self:
        """Return a reconfigured copy of this source."""
        copy = self.model_copy(deep=True)
        for a in copy.assets:
            a._source = copy
        if resources is not None:
            copy.resources = {**copy.resources, **resources}
        if destinations is not None:
            copy.destinations = destinations if isinstance(destinations, list) else [destinations]
        if dataset is not None:
            # Assets resolved their dataset at construction: re-point those that
            # inherited the source's, preserving per-asset overrides.
            for a in copy.assets:
                if a.dataset == copy.dataset:
                    a.dataset = dataset
            copy.dataset = dataset
        if default_destination_key is not None:
            copy.default_destination_key = default_destination_key
        if materializable is not None:
            copy.assets = [a(materializable=materializable) for a in copy.assets]
        if normalizer is not None:
            copy.normalizer = normalizer
        if materialization_strategy is not None:
            copy.materialization_strategy = materialization_strategy
        return copy

