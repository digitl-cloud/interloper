"""Source: a component that groups assets with shared relations and destinations."""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import ConfigDict, Field, model_validator

from interloper.asset import Asset, Reference
from interloper.asset.base import AssetDefinition, AssetMeta
from interloper.component import Component, ComponentDefinition, ComponentIdentity, Maturity, Relation
from interloper.errors import ConfigError
from interloper.normalizer import MaterializationStrategy, Normalizer
from interloper.operation import Operation, Workload
from interloper.resource.fields import InputField, SelectField
from interloper.retry import RetryPolicy
from interloper.serializable import ClassBody, IgnoredDescriptor
from interloper.utils.text import to_identifier, validate_key

if TYPE_CHECKING:
    from types import FunctionType

    from interloper.destination import Destination


class SourceDefinition(ComponentDefinition):
    """Definition of a source including its nested asset definitions.

    Cross-entity references use keys: ``relations`` names the kinds and keys
    that may fill each declared link. Same-entity data is inlined: ``assets``
    are owned by this source, so their definitions are nested.
    """

    assets: list[AssetDefinition] = Field(default_factory=list)

    def partitionings(self) -> list[dict[str, Any]]:
        """The partitioning of each partitioned asset the source declares.

        Returns:
            One partitioning per partitioned asset, in declaration order.
        """
        return [asset.partitioning for asset in self.assets if asset.partitioning is not None]


class Source(Component, Workload):
    """A grouping component that holds assets with shared relations and destinations.

    Define a source by subclassing: an annotation naming a component class
    declares a relation the source's assets inherit, and ``asset_types``
    (or asset classes written in the body) names what it materializes:

    ```py
    class MySource(Source):
        connection: MyConnection
        asset_types = [Users, Orders]
    ```

    An asset is reached by its key: on the class it is the asset class, on an
    instance the live asset (see `AssetMeta`):

    ```py
    MySource.users      # the asset class
    MySource().users    # this source's Asset with key "users"
    ```

    Assets written in the class body are the typed form; one given only in
    ``asset_types`` is reached the same way at runtime.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, ignored_types=(IgnoredDescriptor, Relation, AssetMeta))

    # Definition
    asset_types: ClassVar[list[type[Asset]]] = []
    tags: ClassVar[list[str]] = []
    internal_fields: ClassVar[frozenset[str]] = frozenset({"assets", "normalizer", "enabled"})

    destinations: list[Destination] = Relation("destination", many=True, optional=True)

    # State
    normalizer: Normalizer | None = Field(default=None)
    assets: list[Asset] = Field(default_factory=list)

    # Exposed fields
    dataset: str | None = InputField(
        default=None,
        description="Defaults to the source key when left empty",
        section="Destination",
    )
    default_destination_key: str = SelectField(
        label="Default Destination",
        default="",
        options_from="destinations",
        description="When an asset has multiple destinations, downstream assets use this to know where to read from",
        section="Destination",
    )
    materialization_strategy: MaterializationStrategy | None = SelectField(
        default=MaterializationStrategy.RECONCILE,
        label="Materialization Strategy",
        description="Default strategy for this source's assets.",
        info=(
            "'Reconcile' coerces data to the schema (or infers a schema when "
            "none is declared); 'Strict' requires a schema and fails on any "
            "mismatch. Assets declaring their own strategy keep it."
        ),
        section="Materialization",
    )
    retry: RetryPolicy | None = Field(
        default=None,
        title="Retry",
        description="Default attempt budget for this source's assets",
        json_schema_extra={"x-section": "Operation"},
    )
    enabled: bool = Field(default=True, description="Whether this source's assets execute")

    # -- Construction & resolution ---------------------------------------------

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Collect the source's assets at definition time (see `_collect_asset_types`).

        Args:
            **kwargs: Class-creation keyword arguments, forwarded untouched to
                ``super().__init_subclass__``.
        """
        super().__init_subclass__(**kwargs)
        cls._collect_asset_types()

    @classmethod
    def body_from_function(cls, fn: FunctionType) -> ClassBody:
        """Read a decorated function into a source body: what it returns are the assets.

        The function is called once, for the asset classes it returns (one or a
        list); its annotated parameters become configuration fields, their
        defaults the field defaults.

        Args:
            fn: The decorated function.

        Returns:
            The body carrying ``asset_types`` and the fields.
        """
        returned = fn()
        asset_types = returned if isinstance(returned, list) else [returned] if isinstance(returned, type) else []
        members: dict[str, Any] = {"asset_types": asset_types}
        annotations: dict[str, Any] = {}
        hints = getattr(fn, "__annotations__", {})
        for name, parameter in inspect.signature(fn).parameters.items():
            if name in ("self", "context", "kwargs") or name not in hints:
                continue
            annotations[name] = hints[name]
            if parameter.default is not inspect.Parameter.empty:
                members[name] = parameter.default
        return ClassBody.from_function(fn, members=members, annotations=annotations)

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
        ``Spec.reconstruct()`` hands back in after the walker has resolved
        any nested component specs inside the overrides and held back any
        ``{"ref": id}`` they carried (see
        `SerializationContext`).

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
            instances.append(asset_cls(**assets[asset_cls.key]))
        data["assets"] = instances
        return data

    def model_post_init(self, context: Any) -> None:
        """Build the source's assets, own them, check where they land, and wire them to each other.

        The assets are incomplete when built here: this source's own relation
        kwargs, which they read through it, are bound only after
        ``model_post_init`` returns. That is why nothing is validated at
        construction; `validate_relations` cascades into each asset
        when the DAG or reconstruction asks, with everything this source can
        fill already bound.

        Args:
            context: Pydantic's post-init context, forwarded untouched to
                ``super().model_post_init``.

        Raises:
            ConfigError: If an asset already belongs to another source.
        """
        super().model_post_init(context)
        if not self.assets:
            self.assets = [cls() for cls in self.asset_types]
        for asset in self.assets:
            if asset.parent is not None and asset.parent is not self:
                raise ConfigError(f"Asset '{asset.key}' already belongs to source '{asset.parent.qualified_key}'")
            asset.parent = self
        self._check_locations()
        self._bind_siblings()

    def _owned_by_field(self) -> dict[str, list[Component]]:
        """A source owns its assets.

        Returns:
            ``{"assets": ...}``, the assets in order.
        """
        return {"assets": list(self.assets)}

    def validate_relations(self, nodes: Mapping[str, Component] | None = None) -> None:
        """Check this source's own relations, then cascade into every materializing asset.

        An asset this source does not materialize is only ever read, so what
        fills its own relations is nothing any run needs: it is left
        unchecked, the same position `_check_relations`
        takes on a node it only reads. That is what keeps a source
        reconstructible from a partial set of assets, where a read-only asset
        names a sibling the document does not carry.

        Args:
            nodes: Every node materializing in the same run, keyed by id,
                forwarded to each asset's own check; see
                `validate_relations`.
                ``None`` skips the DAG-membership check.
        """
        super().validate_relations(nodes)
        for asset in self.assets:
            if asset.enabled:
                asset.validate_relations(nodes)

    if TYPE_CHECKING:

        def __getattr__(self, name: str) -> Any:
            """Type what a source exposes by name that a checker cannot see.

            Static only, never defined at runtime: a decorated source is typed
            as `Source`, so its own fields and the assets given in
            ``asset_types`` are invisible to a checker. Assets written in the
            class body type precisely through `AssetMeta` instead.

            Args:
                name: The attribute read.

            Returns:
                Whatever the source exposes under *name*.
            """
            ...

    # -- Assets ----------------------------------------------------------------

    def inherited(self, child: Component, name: str, own: Any) -> Any:
        """What one of this source's assets reads for an inherited setting.

        The dataset is part of an asset's location, which the source names
        (as `table` names the rest): the asset's own dataset, else this
        source's, else this source's key, with any ``{field}`` filled from
        this source's fields by `format`. Every other setting but ``enabled``
        is handed down as `Component.inherited` does.

        An asset runs only while it and this source are both enabled, which is
        how ``source(enabled=False)`` puts a read-only source in a run.

        Args:
            child: The asset reading the setting.
            name: The setting's field name.
            own: The asset's stored value, ``None`` while unset.

        Returns:
            The effective value of the setting for *child*.
        """
        if name == "dataset":
            return self.format(own or self.dataset or self.key)
        if name == "enabled":
            return bool(own) and self.enabled
        return super().inherited(child, name, own)

    def operations(self) -> list[Operation]:
        """The source's own assets: the operations a run on it executes.

        Returns:
            The assets this source holds.
        """
        return list(self.assets)

    def table(self, asset: Asset) -> str:
        """The physical table name of one of this source's assets.

        A reference's table is its ``table_name`` as written, with this
        source's fields filled in by `format`. Any other asset's is the asset
        key suffixed with the instance's `discriminator` (the config field
        marked ``discriminator=True``) and coerced to a valid identifier, so
        instances of a multi-account source materialize side by side in one
        dataset instead of overwriting each other's data. Without a
        discriminator the asset key is used as-is.

        Override for another composition; keep the ``{asset.key}__{suffix}``
        shape so tables stay wildcard-queryable per asset, leave references
        to ``super()``, and return a valid identifier: the result is
        validated when the source resolves its assets, not coerced.

        Args:
            asset: The asset to name a table for.

        Returns:
            The physical table name for the asset.
        """
        if isinstance(asset, Reference) and asset.table_name is not None:
            return self.format(asset.table_name)
        discriminator = self.discriminator
        return to_identifier(f"{asset.key}__{discriminator}" if discriminator else asset.key)

    # -- Definition ------------------------------------------------------------

    @classmethod
    def definition(cls) -> SourceDefinition:
        """Produce a structured definition of this source including its assets.

        An asset shows the less mature of its own maturity and the source's:
        an alpha source makes every one of its assets alpha. Both are read
        from the validated definitions, never from the declared class values.

        Returns:
            A SourceDefinition with metadata and nested asset definitions.
        """
        own = super().definition()
        assets = []
        for asset_cls in cls.asset_types:
            asset = asset_cls.definition()
            maturity = Maturity.least(asset.maturity, own.maturity)
            assets.append(asset.model_copy(update={"source_key": cls.key, "maturity": maturity}))
        return SourceDefinition(**dict(own), assets=assets)

    # -- Internals -------------------------------------------------------------

    @classmethod
    def _collect_asset_types(cls) -> None:
        """Gather the source's asset classes, own each, and make each reachable under its key.

        The asset classes are the ones ``asset_types`` lists (which the
        function form of `source` fills) followed by the ones written in the
        class body, each once. A class collecting none inherits its parent's.
        Each collected class is owned by this source and installed as a class
        attribute under its key; an asset class is its own descriptor (see
        `AssetMeta`), so ``Shop.orders`` is the class and ``Shop().orders`` the
        live asset.

        Raises:
            TypeError: If an asset class already belongs to another source, or
                if its key names another attribute of this source.
        """
        listed: list[type[Asset]] = list(cls.__dict__.get("asset_types", []))
        declared = [value for value in cls.__dict__.values() if isinstance(value, type) and issubclass(value, Asset)]
        collected = listed + [asset_cls for asset_cls in declared if asset_cls not in listed]
        if not collected:
            return
        for asset_cls in collected:
            owner = asset_cls._source_type
            if owner is not None and owner is not cls:
                raise TypeError(f"Asset '{asset_cls.__name__}' already belongs to source '{owner.__name__}'")
            current = inspect.getattr_static(cls, asset_cls.key, asset_cls)
            if current is not asset_cls:
                raise TypeError(
                    f"Source '{cls.__name__}' cannot expose asset '{asset_cls.key}': the name is taken by {current!r}"
                )
            asset_cls._source_type = cls
            setattr(cls, asset_cls.key, asset_cls)
        cls.asset_types = collected

    @classmethod
    def sibling_bindings(cls) -> dict[str, dict[str, str]]:
        """Which of this source's assets each asset relation resolves to, by key.

        Only intra-source wiring is decided here: a declared key that resolves
        to another source, or to a wildcard, needs the whole DAG to be
        resolved and is left to it. Public for the relation store, which
        persists the same wiring; connector code never needs it.

        Returns:
            Asset key to a map of relation name to the sibling asset key that
            fills it; assets with no sibling wiring are absent.
        """
        siblings = {asset_cls.key for asset_cls in cls.asset_types}
        bindings: dict[str, dict[str, str]] = {}
        for asset_cls in cls.asset_types:
            for name, relation in asset_cls.relations.items():
                if "asset" not in relation.kinds:
                    continue
                declared_keys = relation.keys
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

    def format(self, template: str) -> str:
        """Fill a location template with this source's fields.

        An asset's ``dataset`` and a reference's ``table`` may name a field of
        the source in braces (``"analytics_{property_id}"``), which is how a
        declaration written once in the class body varies per instance.

        Args:
            template: A table or dataset name, possibly with ``{field}`` placeholders.

        Returns:
            The name with every placeholder filled.

        Raises:
            ConfigError: If the template names something that is not a field of this source.
        """
        try:
            return template.format(**{name: getattr(self, name) for name in type(self).model_fields})
        except KeyError as error:
            raise ConfigError(
                f"Source '{self.key}': '{template}' names {error}, which is not one of its fields"
            ) from error

    def _check_locations(self) -> None:
        """Resolve every asset's location once, so a bad one fails at construction.

        This source's dataset and every materializing asset's table must be
        valid identifiers, and every asset's dataset template must name fields
        of this source. A reference names an existing table verbatim, wildcards
        included, so its table is not checked.
        """
        validate_key(self.dataset or self.key)
        for asset in self.assets:
            _ = asset.dataset  # fills the template, refusing a placeholder that names no field
            if not isinstance(asset, Reference):
                validate_key(asset.table)
