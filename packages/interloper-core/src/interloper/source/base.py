"""Source: a component that groups assets with shared relations and destinations."""

from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import Field, model_validator
from typing_extensions import Self

from interloper.asset import Asset
from interloper.asset.base import AssetDefinition
from interloper.component import Component, ComponentDefinition, ComponentIdentity, Relation
from interloper.normalizer import MaterializationStrategy, Normalizer
from interloper.operation import Operation, Workload
from interloper.resource.fields import InputField, SelectField, validate_fetch_field_providers
from interloper.serializable import IgnoredDescriptor, Spec
from interloper.utils.imports import get_object_path
from interloper.utils.text import to_label, validate_key

if TYPE_CHECKING:
    from interloper.destination import Destination


class AssetRef(IgnoredDescriptor):
    """Class attribute that exposes a source-owned asset.

    At **class access** (``FacebookAds.campaigns``) returns the asset
    *class*: this is what makes ``import_from_path`` work on composite
    paths like ``"module:FacebookAds.campaigns"`` without ever having to
    instantiate the source.

    At **instance access** (``facebook_ads.campaigns``) returns the live
    asset instance owned by that source.

    Replaces the old ``Source.__getattr__`` mechanism, which only worked
    at instance level and forced reconstruction code to instantiate the
    source just to reach its assets.
    """

    def __init__(self, asset_cls: type[Asset]) -> None:
        """Bind this descriptor to an asset class.

        Args:
            asset_cls: The source-owned Asset subclass this descriptor exposes.
        """
        self.asset_cls = asset_cls
        self.attr_name: str = ""

    def __set_name__(self, owner: type, name: str) -> None:
        """Capture the attribute name this descriptor was installed under.

        Args:
            owner: The class the descriptor is being installed on.
            name: The attribute name it is bound to in that class body.
        """
        self.attr_name = name

    def __get__(self, instance: Any, owner: type | None = None) -> Any:
        """Return the asset class at class access, the instance at instance access.

        Args:
            instance: The source instance the attribute was read from, or
                ``None`` for class-level access.
            owner: The class owning the descriptor. Unused; part of the
                descriptor protocol. Defaults to ``None``.

        Returns:
            The asset class when *instance* is ``None``, else the live asset
            instance owned by that source.

        Raises:
            AttributeError: If accessed on an instance whose ``assets`` list
                contains no asset matching this descriptor's key.
        """
        if instance is None:
            return self.asset_cls
        for asset in instance.assets:
            if asset.key == self.asset_cls.key:
                return asset
        raise AttributeError(
            f"Source '{type(instance).__name__}' has no asset with key '{self.asset_cls.key}'"
        )


class SourceDefinition(ComponentDefinition):
    """Definition of a source including its nested asset definitions.

    Cross-entity references use keys: ``relations`` names the kinds and keys
    that may fill each declared link. Same-entity data is inlined: ``assets``
    are owned by this source, so their definitions are nested.
    """

    assets: list[AssetDefinition] = Field(default_factory=list)


class Source(Component, Workload):
    """A grouping component that holds assets with shared relations and destinations.

    Define a source by subclassing: an annotation naming a component class
    declares a relation the source's assets inherit, and ``asset_types``
    (or asset classes written in the body) names what it materializes::

        class MySource(Source):
            connection: MyConnection
            asset_types = [Users, Orders]

    Access assets by key via attribute access::

        source = MySource()
        source.users  # returns the Asset with key "users"
    """

    # Definition
    asset_types: ClassVar[list[type[Asset]]] = []
    tags: ClassVar[list[str]] = []
    internal_fields: ClassVar[frozenset[str]] = frozenset({"assets", "normalizer", "select"})

    destinations: list[Destination] = Relation("destination", many=True, optional=True)

    # State
    normalizer: Normalizer | None = Field(default=None)
    materialization_strategy: MaterializationStrategy | None = SelectField(
        default=MaterializationStrategy.AUTO,
        label="Materialization Strategy",
        description="Default strategy for this source's assets.",
        info=(
            "'Auto' coerces data to the schema (or infers a schema when "
            "none is declared), 'Strict' fails on any mismatch, 'Reconcile' "
            "requires a schema and coerces values to it. Assets declaring "
            "their own strategy keep it."
        ),
    )
    assets: list[Asset] = Field(default_factory=list)
    select: list[str] | None = Field(
        default=None, description="Asset keys to materialize; others stay as read-only dependencies"
    )

    # Exposed fields
    dataset: str = InputField(default="", description="Defaults to the source key when left empty")
    default_destination_key: str = SelectField(
        label="Default Destination",
        default="",
        options_from="destinations",
        description="When an asset has multiple destinations, downstream assets use this to know where to read from",
    )

    # -- Construction & resolution ---------------------------------------------

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Auto-discover the source's assets at definition time.

        Asset classes defined in the source body (via ``@asset`` on
        methods) appear as class attributes.  We collect them into
        ``asset_types`` and replace each class attribute with an
        :class:`AssetRef` descriptor that exposes the class at class
        level and the live instance at instance level.

        Args:
            **kwargs: Class-creation keyword arguments, forwarded untouched to
                ``super().__init_subclass__``.
        """
        super().__init_subclass__(**kwargs)
        cls._collect_asset_types()

    @model_validator(mode="before")
    @classmethod
    def _apply_asset_overrides(cls, data: Any) -> Any:
        """Convert an ``assets`` override map into a list of asset instances.

        Supports two shapes for the ``assets`` init kwarg:

        - ``list[Asset]``: a pre-built list; passed through untouched.
        - ``dict[str, dict]``: an override map keyed by asset key, where
          each value is the ``init`` payload for that asset.  The source
          is the unit of reconstruction: for every entry in
          ``asset_types`` we build ``asset_cls(**overrides)``, defaulting
          unmapped keys to a bare ``asset_cls()``.

        This is what ``Source.to_spec()`` emits and what
        ``Spec.reconstruct()`` hands back in after the walker
        has resolved any nested component specs inside the overrides.

        An override may name a relation target by reference rather than
        carry it: those are held back the way
        :meth:`~interloper.serializable.base.Spec.reconstruct` holds back a
        component's own, and bound on the asset once the document is whole.

        Args:
            data: The raw model input. Anything that is not a dict, or whose
                ``assets`` entry is not an override map, is returned untouched.

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
            if asset_cls.key not in assets:
                continue
            overrides, pending = asset_cls._split_references(assets[asset_cls.key])
            instance = asset_cls(**overrides)
            instance._pending_references = pending
            instances.append(instance)
        data["assets"] = instances
        return data

    def model_post_init(self, context: Any) -> None:
        """Build the source's assets, resolve their defaults and wire them to each other.

        The assets are incomplete when built here: this source's own relation
        kwargs are bound, and trickled into them, only after
        ``model_post_init`` returns. That is why nothing is validated at
        construction; :meth:`validate_relations` cascades into each asset
        when the DAG or reconstruction asks, with everything this source can
        fill already bound.

        Args:
            context: Pydantic's post-init context, forwarded untouched to
                ``super().model_post_init``.
        """
        super().model_post_init(context)
        if not self.assets:
            self.assets = [cls() for cls in self.asset_types]
        self._resolve()
        if self.select is not None:
            self._apply_select()
        self._bind_siblings()

    def validate_relations(self, nodes: Mapping[str, Component] | None = None) -> None:
        """Check this source's own relations, then cascade into every materializing asset.

        An asset this source does not materialize is only ever read, so what
        fills its own relations is nothing any run needs: it is left
        unchecked, the same position :meth:`~interloper.dag.base.DAG._check_relations`
        takes on a node it only reads. That is what keeps a source
        reconstructible from a partial set of assets, where a read-only asset
        names a sibling the document does not carry.

        Args:
            nodes: Every node materializing in the same run, keyed by id,
                forwarded to each asset's own check; see
                :meth:`~interloper.component.base.Component.validate_relations`.
                ``None`` skips the DAG-membership check.
        """
        super().validate_relations(nodes)
        for asset in self.assets:
            if asset.materializable:
                asset.validate_relations(nodes)

    def _apply_select(self) -> None:
        """Mark assets outside ``select`` as non-materializable.

        Unselected assets stay in the list so intra-source dependency wiring
        can still resolve them by key and their outputs stay readable, but
        only the selected assets execute, and :meth:`validate_relations`
        checks only those: the same position as
        :meth:`~interloper.dag.base.DAG.mini_dag` on a node it only reads.

        Raises:
            SourceError: If a selected key matches no asset of this source.
        """
        from interloper.errors import SourceError

        known = {a.key for a in self.assets}
        unknown = [k for k in self.select or [] if k not in known]
        if unknown:
            raise SourceError(f"Source '{self.key}' has no asset(s) {unknown}; available: {sorted(known)}")
        selected = set(self.select or [])
        self.assets = [a if a.key in selected else a(materializable=False) for a in self.assets]
        for asset in self.assets:
            asset.parent = self

    # -- Serialization ---------------------------------------------------------

    def _to_spec(
        self,
        *,
        seen: set[str],
        without: Collection[str] = (),
        drop: Mapping[str, Collection[str]] | None = None,
    ) -> Spec:
        """Serialize to a spec whose ``assets`` is a key to init override map.

        Args:
            seen: Ids the traversal has already written out in full; see
                :meth:`~interloper.component.base.Component._to_spec`.
            without: Init keys to leave out.
            drop: Target ids to leave out of one of this source's own
                relations, keyed by relation name, rather than the whole
                relation; see :meth:`~interloper.component.base.Component._to_spec`.

        Returns:
            A ``Spec`` capturing this source and its assets.
        """
        return self._source_spec(seen=seen, without=without, assets=self.assets, drop=drop)

    def _source_spec(
        self,
        *,
        seen: set[str],
        without: Collection[str] = (),
        assets: list[Asset],
        drop: Mapping[str, Collection[str]] | None = None,
        asset_drop: Mapping[str, Mapping[str, Collection[str]]] | None = None,
    ) -> Spec:
        """Serialize this source over a given set of asset instances.

        Source is the unit of reconstruction: an asset never travels under a
        relation, only under the source that owns it, as a plain init payload
        carrying its id and no ``path`` of its own. This mirrors
        :meth:`_apply_asset_overrides` on the reconstruction side and keeps
        the document compact.

        A relation an asset holds only because this source trickled it down
        is left out of its payload: the same trickle refills it when the
        source rebinds its own target on reconstruction, so writing it out
        would say twice what the source already says once.

        *assets* and *asset_drop* are what let a graph serialise a source
        from its own asset copies rather than the source's originals: a
        mini-DAG flags the parents it only reads as non-materializable, a
        run may hold just some of a source's assets, and a binding pointing
        outside the document has nowhere to be written (see
        :meth:`~interloper.dag.base.DAG.to_spec`).

        Args:
            seen: Ids the traversal has already written out in full.
            without: Init keys to leave out of this source's own payload.
            assets: The asset instances to write out under ``assets``.
            drop: Target ids to leave out of one of this source's own
                relations, keyed by relation name.
            asset_drop: Target ids to leave out of one of an asset's
                relations, keyed first by that asset's key and then by
                relation name; the per-asset counterpart of *drop*.

        Returns:
            A ``Spec`` capturing this source and the given assets.
        """
        spec = super()._to_spec(seen=seen, without=(*without, "assets"), drop=drop)
        if not assets:
            return spec
        trickled = {name: self._trickled_asset_keys(name) for name in self._bound}
        overrides: dict[str, Any] = {}
        for asset in assets:
            skipped = {name for name, keys in trickled.items() if asset.key in keys}
            asset_init = dict(
                asset._to_spec(seen=seen, without=skipped, drop=(asset_drop or {}).get(asset.key)).init or {}
            )
            # The id is what every binding naming this asset resolves through.
            asset_init["id"] = asset.id
            overrides[asset.key] = asset_init
        init = dict(spec.init or {})
        init["assets"] = overrides
        return spec.model_copy(update={"init": init})

    def _children(self) -> list[Component]:
        """The assets this source owns, which travel inside its own spec.

        Returns:
            This source's asset instances.
        """
        return list(self.assets)

    # -- Assets ----------------------------------------------------------------

    def operations(self) -> list[Operation]:
        """The source's own assets: the operations a run on it executes.

        Returns:
            The assets this source holds.
        """
        return list(self.assets)

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
    def sibling_bindings(cls) -> dict[str, dict[str, str]]:
        """Which of this source's assets each asset relation resolves to, by key.

        Only intra-source wiring is decided here: a declared key that resolves
        to another source, or to a wildcard, needs the whole DAG to be
        resolved and is left to it.

        Returns:
            Asset key to a map of relation name to the sibling asset key that
            fills it; assets with no sibling wiring are absent.
        """
        siblings = {asset_cls.key for asset_cls in cls.asset_types}
        bindings: dict[str, dict[str, str]] = {}
        for asset_cls in cls.asset_types:
            for name, relation in asset_cls.relations.items():
                if "asset" not in relation.kinds():
                    continue
                declared_keys = relation.keys()
                for declared in declared_keys:
                    expected = ComponentIdentity.resolve(declared, own_source_key=cls.key)
                    if expected.source_key == cls.key and expected.key in siblings and expected.key != asset_cls.key:
                        bindings.setdefault(asset_cls.key, {})[name] = expected.key
        return bindings

    def _bind_siblings(self) -> None:
        """Bind each asset's sibling relations to this source's own asset instances.

        A relation the asset already holds is left alone, so a binding made by
        hand or hydrated from persistence always wins.
        """
        by_key = {asset.key: asset for asset in self.assets}
        for asset_key, names in type(self).sibling_bindings().items():
            asset = by_key.get(asset_key)
            if asset is None:
                continue
            for name, sibling_key in names.items():
                if not asset.bound(name) and sibling_key in by_key:
                    asset.bind(name, by_key[sibling_key])

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

        Args:
            asset: The asset to name a table for; only its ``key`` is read.

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

        for asset in self.assets:
            asset.parent = self
            if not asset.dataset:
                asset.dataset = self.dataset
            validate_key(asset.table)
            if not asset.default_destination_key and self.default_destination_key:
                asset.default_destination_key = self.default_destination_key
            if asset.normalizer is None and self.normalizer is not None:
                asset.normalizer = self.normalizer
            if (
                self.materialization_strategy is not None
                and asset.materialization_strategy == MaterializationStrategy.AUTO
            ):
                asset.materialization_strategy = self.materialization_strategy

    def _trickle_down(self) -> None:
        """Fill the unbound relations of this source's assets and destinations from its own.

        A binding is the moment this runs: relation keyword arguments reach a
        component after ``model_post_init`` has already built its assets, so
        without a pass on :meth:`_rebound` a source's connection would never
        reach them.
        """
        for asset in self.assets:
            self.trickle(asset)
        for destination in self.destinations:
            self.trickle(destination)

    def _trickled_asset_keys(self, name: str) -> set[str]:
        """Which of this source's assets hold exactly what this source itself has bound.

        A child receives a relation's binding only through
        :meth:`~interloper.component.base.Component.trickle`, which passes
        this source's own targets through unchanged; the target ids are what
        tell that binding apart from one an asset bound on its own. Ids and
        not object identity, because a deep copy rebuilds a source's own
        bindings and its assets' separately: the copy's assets then hold
        distinct objects carrying the same ids, and an identity comparison
        would read every trickled binding on a copy as the asset's own.

        Args:
            name: The relation name to check.

        Returns:
            Keys of the assets whose current binding for *name* is exactly
            this source's own list of targets, in order. Empty when this
            source itself holds nothing for *name*.
        """
        own = [target.id for target in self._bound.get(name, [])]
        if not own:
            return set()
        return {asset.key for asset in self.assets if [target.id for target in asset._bound.get(name, [])] == own}

    def _rebound(self, name: str) -> None:
        """Trickle this source's bindings down whenever one of them changes.

        Args:
            name: The relation name whose binding changed; every relation the
                source holds trickles, so the name itself is not read.
        """
        self._trickle_down()

    def __getattr__(self, name: str) -> Asset:
        """Instance-level asset lookup fallback.

        At runtime, source-owned asset access is normally served by the
        :class:`AssetRef` descriptor installed on the class by
        :meth:`_collect_asset_types`, so Python never reaches this
        method.  It exists for two reasons:

        1. **Static analysis**: it tells type checkers that
           ``source.<asset_key>`` yields an :class:`~interloper.Asset`,
           since the dynamically-installed descriptors aren't visible to
           them.
        2. **Safety net**: sources built imperatively (e.g. in tests)
           that populate ``asset_types`` without going through
           :meth:`_collect_asset_types` still get ergonomic attribute
           access.

        Args:
            name: The attribute being looked up, read as an asset key.
                Underscore-prefixed names are delegated to ``BaseModel``.

        Returns:
            The asset matching the given key.

        Raises:
            AttributeError: If no asset matches the given key.
        """
        # Delegate private attributes and Pydantic internals to BaseModel
        if name.startswith("_"):
            return super().__getattr__(name)  # ty: ignore[unresolved-attribute]
        for asset in self.assets:
            if asset.key == name:
                return asset
        raise AttributeError(f"Source has no asset with key '{name}'")

    # -- Definition ------------------------------------------------------------

    @classmethod
    def definition(cls) -> SourceDefinition:
        """Produce a structured definition of this source including its assets.

        Returns:
            A SourceDefinition with metadata and nested asset definitions.
        """
        validate_fetch_field_providers(cls, cls.relations)

        return SourceDefinition(
            kind=cls.kind,
            key=cls.key,
            path=get_object_path(cls),
            name=cls.name or to_label(cls.__name__),
            icon=cls.icon,
            description=cls.__doc__ or "",
            tags=list(cls.tags),
            config_schema=cls.config_schema(),
            relations=dict(cls.relations),
            assets=[asset_cls.definition().model_copy(update={"source_key": cls.key}) for asset_cls in cls.asset_types],
        )

    # -- Reconfiguration -------------------------------------------------------

    def __call__(
        self,
        *,
        dataset: str | None = None,
        default_destination_key: str | None = None,
        materializable: bool | None = None,
        normalizer: Normalizer | None = None,
        materialization_strategy: MaterializationStrategy | None = None,
        **relations: Any,
    ) -> Self:
        """Return a reconfigured copy of this source.

        Every fixed parameter defaults to ``None``, meaning "leave as is". A
        name in **relations follows a different rule: passing it at all
        changes it, since ``None`` there clears the binding rather than
        leaving it alone; only leaving the name out entirely leaves it as is.

        Args:
            dataset: Replacement dataset. Assets that inherited the source's
                dataset are re-pointed; per-asset overrides are preserved.
            default_destination_key: Replacement key of the destination
                downstream assets read from.
            materializable: Applied to every asset of the copy.
            normalizer: Replacement normalizer for the source.
            materialization_strategy: Replacement default strategy for the
                source.
            **relations: Replacement targets for the copy's declared
                relations, keyed by relation name: a single component, a list
                of them, or ``None`` to clear the binding. Rebinding a name
                also clears it from any asset of the copy whose current
                binding is exactly what this source had trickled into it, so
                the new target reaches that asset once the copy re-trickles;
                an asset that bound the relation itself keeps its own
                binding.

        Returns:
            A deep copy of this source carrying the overrides.

        Raises:
            TypeError: If a keyword argument names no declared relation.
        """
        unknown = [name for name in relations if name not in type(self).relations]
        if unknown:
            raise TypeError(f"{type(self).__name__} declares no relation(s): {', '.join(sorted(unknown))}")
        stale = {name: self._trickled_asset_keys(name) for name in relations}
        copy = self.model_copy(deep=True)
        for asset in copy.assets:
            asset.parent = copy
            for name, keys in stale.items():
                if asset.key in keys:
                    asset._bound.pop(name, None)
        for name, value in relations.items():
            setattr(copy, name, value)
        if dataset is not None:
            # Assets resolved their dataset at construction: re-point those that
            # inherited the source's, preserving per-asset overrides.
            for asset in copy.assets:
                if asset.dataset == copy.dataset:
                    asset.dataset = dataset
            copy.dataset = dataset
        if default_destination_key is not None:
            copy.default_destination_key = default_destination_key
        if materializable is not None:
            copy.assets = [asset(materializable=materializable) for asset in copy.assets]
        if normalizer is not None:
            copy.normalizer = normalizer
        if materialization_strategy is not None:
            copy.materialization_strategy = materialization_strategy
        return copy
