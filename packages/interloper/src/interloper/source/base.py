"""This module contains the base classes for sources."""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from copy import copy
from functools import partial
from inspect import signature
from typing import Any

from typing_extensions import Self

from interloper import errors
from interloper.asset.base import Asset
from interloper.execution.strategy import MaterializationStrategy
from interloper.io.base import IO
from interloper.normalizer import Normalizer
from interloper.params.base import AssetParam, ContextualAssetParam


class Source(ABC):
    """An abstract class for sources."""

    name: str
    dataset: str | None
    io: dict[str, IO] | IO
    default_io_key: str | None
    auto_asset_deps: bool
    normalizer: Normalizer | None
    materializable: bool
    materialization_strategy: MaterializationStrategy
    default_assets_args: dict[str, Any]
    _initialized: bool

    def __init__(
        self,
        name: str,
        *,
        dataset: str | None = None,
        io: dict[str, IO] | IO | None = None,
        default_io_key: str | None = None,
        auto_asset_deps: bool = True,
        normalizer: Normalizer | None = None,
        materializable: bool = True,
        materialization_strategy: MaterializationStrategy = MaterializationStrategy.FLEXIBLE,
        default_assets_args: dict[str, Any] | None = None,
    ):
        """Initialize the source.

        Args:
            name: The name of the source.
            dataset: The dataset of the source.
            io: The IO of the source.
            default_io_key: The default IO key of the source.
            auto_asset_deps: Whether to automatically set the upstream dependencies for assets.
            normalizer: The normalizer of the source.
            materializable: Whether the assets in the source are materializable.
            materialization_strategy: The materialization strategy of the source.
            default_assets_args: The default arguments for the assets in the source.
        """
        super().__init__()
        self._assets: dict[str, Asset] = {}
        self.name = name
        self.dataset = dataset or name
        self.io = io or {}
        self.default_io_key = default_io_key
        self.auto_asset_deps = auto_asset_deps
        self.normalizer = normalizer
        self.materializable = materializable
        self.materialization_strategy = materialization_strategy
        self.default_assets_args = default_assets_args or {}
        self._initialized = False

    ############
    # Magic
    ############
    def __call__(
        self,
        *,
        name: str | None = None,
        dataset: str | None = None,
        io: dict[str, IO] | IO | None = None,
        default_io_key: str | None = None,
        materializable: bool | None = None,
        default_assets_args: dict[str, Any] | None = None,
        materialization_strategy: MaterializationStrategy | None = None,
        **kwargs: Any,
    ) -> "Source":
        """Create a copy of the source with new parameters.

        Args:
            name: The name of the source.
            dataset: The dataset of the source.
            io: The IO of the source.
            default_io_key: The default IO key of the source.
            materializable: Whether the assets in the source are materializable.
            default_assets_args: The default arguments for the assets in the source.
            materialization_strategy: The materialization strategy of the source.
            **kwargs: The parameters to bind to the asset_definitions function.

        Returns:
            A new source with the updated parameters.
        """
        c = copy(self)
        c.name = name or self.name
        c.dataset = dataset or self.dataset
        c.io = io or self.io
        c.default_io_key = default_io_key or self.default_io_key
        c.default_assets_args = default_assets_args or self.default_assets_args
        c.materializable = materializable or self.materializable
        c.materialization_strategy = materialization_strategy or self.materialization_strategy
        c.bind(**kwargs)
        c._build_assets()
        return c

    def __copy__(self) -> Self:
        """Create a copy of the source.

        Returns:
            A copy of the source.
        """
        cls = self.__class__
        _copy = cls.__new__(cls)
        _copy.__dict__.update(self.__dict__)
        return _copy

    def __getattr__(self, name: str) -> Asset:
        """Get an asset from the source.

        This method lazily initializes the source if it hasn't been initialized yet.

        Args:
            name: The name of the asset.

        Returns:
            The asset.
        """
        self._ensure_initialized()
        return super().__getattribute__(name)

    def __setattr__(self, name: str, value: Any) -> None:
        """Set an attribute on the source.

        Args:
            name: The name of the attribute.
            value: The value of the attribute.

        Raises:
            SourceDefinitionError: If trying to overwrite an existing asset.
        """
        # Since _assets is accessed here in the __setattr__ method, we need to handle it separately
        # to avoid recursion with __getattr__ when _assets is not yet initialized.
        if name == "_assets":
            super().__setattr__(name, value)
            return

        # Prevent overwriting the assets
        try:
            if name in self._assets:
                raise errors.SourceDefinitionError(f"Asset {name} is read-only")
        except AttributeError:
            # Race condition: _assets is not yet initialized
            pass

        super().__setattr__(name, value)

    def __getitem__(self, name: str) -> Asset:
        """Get an asset from the source by name.

        Args:
            name: The name of the asset.

        Returns:
            The asset.

        Raises:
            SourceValueError: If the asset is not found.
        """
        self._ensure_initialized()
        if name not in self._assets:
            raise errors.SourceValueError(f"Asset {name} not found in source {self.name}")
        return self._assets[name]

    def __contains__(self, name: str) -> bool:
        """Check if an asset is in the source.

        Args:
            name: The name of the asset.

        Returns:
            True if the asset is in the source, False otherwise.
        """
        self._ensure_initialized()
        return name in self._assets

    #############
    # Properties
    #############
    @property
    def assets(self) -> list[Asset]:
        """Get all assets, ensuring initialization."""
        self._ensure_initialized()
        return list(self._assets.values())

    ############
    # Public
    ############
    @abstractmethod
    def asset_definitions(self) -> Sequence[Asset]:
        """The definitions of the assets in the source."""
        ...

    def bind(self, **params: Any) -> None:
        """Bind parameters to the asset_definitions function.

        Args:
            **params: The parameters to bind.

        Raises:
            SourceValueError: If a parameter is not a valid parameter for the source.
        """
        sig = signature(self.asset_definitions)
        current_params = [p.name for p in sig.parameters.values()]
        final_params = {}

        for param_name, param_value in params.items():
            if param_name not in current_params:
                raise errors.SourceValueError(f"Parameter {param_name} is not a valid parameter for source {self.name}")

            final_params[param_name] = param_value

        self.asset_definitions = partial(self.asset_definitions, **final_params)

    ############
    # Private
    ############
    def _ensure_initialized(self) -> None:
        if object.__getattribute__(self, "_initialized") is False:  # avoid recursion with __getattr__
            self._build_assets()
            self._initialized = True

    def _build_assets(self) -> None:
        self._assets: dict[str, Asset] = {}

        params = self._resolve_parameters()
        for asset in self.asset_definitions(**params):
            if not isinstance(asset, Asset):
                raise errors.SourceValueError(f"Expected an instance of Asset, but got {type(asset)}")
            if asset.name in self._assets:
                raise errors.SourceValueError(f"Duplicate asset name '{asset.name}'")

            asset._source = self  # Set the source
            asset.bind(**self.default_assets_args, ignore_unknown_params=True)  # Bind the default args to the asset
            setattr(self, asset.name, asset)  # Set the asset as an attribute
            self._assets[asset.name] = asset  # Add the asset to the assets dictionary

        # Automatically set the upstream deps map for each asset if the source has the corresponding asset with the same
        # name. Needs to be done after all assets are built.
        if self.auto_asset_deps:
            for asset in self._assets.values():
                for upstream_asset in asset.upstream_assets:
                    # This is true if upstream_asset.ref matches the name of an asset in the source and not the full
                    # asset ID. Depends on:
                    #  - Source collects assets by name, not by ID -> always true
                    #  - UpstreamAsset param is initialized with the name of an asset from the same source
                    if upstream_asset.key in self._assets:
                        asset.deps[upstream_asset.key] = self._assets[upstream_asset.key]

    def _resolve_parameters(self) -> dict:
        sig = signature(self.asset_definitions)
        final_params = {}

        for param in sig.parameters.values():
            # No user defined paramters and no default value
            if param.default is param.empty:
                raise errors.SourceParamError(f"Cannot resolve parameter {param.name} for source {self.name}")

            # ContextualAssetParam not supported for sources (requires an execution context)
            if isinstance(param.default, ContextualAssetParam):
                raise errors.SourceParamError(f"ContextualAssetParam {param.name} not supported in source parameters")

            # Default value is an asset_param -> resolve it
            if isinstance(param.default, AssetParam):
                try:
                    final_params[param.name] = param.default.resolve()
                except Exception as e:
                    raise errors.SourceParamError(
                        f"Failed to resolve parameter {param.name} for source {self.name}: {e}"
                    )

                continue

            # Default value is not a asset_param
            final_params[param.name] = param.default

        return final_params
