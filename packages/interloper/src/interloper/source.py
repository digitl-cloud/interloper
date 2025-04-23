from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from copy import copy
from functools import partial
from inspect import signature
from typing import Any, overload

from typing_extensions import Self

from interloper import errors
from interloper.asset import Asset
from interloper.execution.strategy import MaterializationStrategy
from interloper.io.base import IO
from interloper.normalizer import Normalizer
from interloper.param import AssetParam, ContextualAssetParam


class Source(ABC):
    name: str
    dataset: str | None
    io: dict[str, IO]
    default_io_key: str | None
    auto_asset_deps: bool
    normalizer: Normalizer | None
    materialization_strategy: MaterializationStrategy
    default_assets_args: dict[str, Any]
    _initialized: bool

    def __init__(
        self,
        name: str,
        *,
        dataset: str | None = None,
        io: dict[str, IO] | None = None,
        default_io_key: str | None = None,
        auto_asset_deps: bool = True,
        normalizer: Normalizer | None = None,
        materialization_strategy: MaterializationStrategy = MaterializationStrategy.FLEXIBLE,
        default_assets_args: dict[str, Any] | None = None,
    ):
        super().__init__()
        self._assets: dict[str, Asset] = {}
        self.name = name
        self.dataset = dataset or name
        self.io = dict(io) if io else {}
        self.default_io_key = default_io_key
        self.auto_asset_deps = auto_asset_deps
        self.normalizer = normalizer
        self.materialization_strategy = materialization_strategy
        self.default_assets_args = default_assets_args or {}
        self._initialized = False

    ############
    # Magic
    ############
    def __call__(
        self,
        *,
        dataset: str | None = None,
        io: dict[str, IO] | None = None,
        default_io_key: str | None = None,
        default_assets_args: dict[str, Any] | None = None,
        materialization_strategy: MaterializationStrategy | None = None,
        **kwargs: Any,
    ) -> "Source":
        c = copy(self)
        c.dataset = dataset or self.dataset
        c.io = io or self.io
        c.default_io_key = default_io_key or self.default_io_key
        c.default_assets_args = default_assets_args or self.default_assets_args
        c.materialization_strategy = materialization_strategy or self.materialization_strategy
        c.bind(**kwargs)
        c._build_assets()
        return c

    def __copy__(self) -> Self:
        cls = self.__class__
        _copy = cls.__new__(cls)
        _copy.__dict__.update(self.__dict__)
        return _copy

    def __getattr__(self, name: str) -> Asset:
        """
        Custom __getattr__. Lazily initializes the source if it hasn't been initialized yet.

        This method is intended to force the type checker to recognize attributes as instances of the Asset class.
        However, it should not affect the type of the Source class attributes.
        """

        self._ensure_initialized()
        return super().__getattribute__(name)

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Custom `__setattr__` method to handle setting attributes on the object.

        This method ensures that certain attributes are handled in a specific way:
        - Prevents overwriting existing assets by raising an AttributeError if an attempt is made to set an attribute
          that already exists in `_assets`.
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

    #############
    # Properties
    #############
    @property
    def assets(self) -> list[Asset]:
        """
        Get all assets, ensuring initialization.
        """

        self._ensure_initialized()
        return list(self._assets.values())

    ############
    # Public
    ############
    @abstractmethod
    def asset_definitions(self) -> Sequence[Asset]: ...

    def bind(self, **params: Any) -> None:
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

        # Automatically set the deps for the asset if the source has the corresponding asset with the same name
        # Needs to be done after all assets are built
        for asset in self._assets.values():
            if self.auto_asset_deps:
                for upstream_asset in asset.upstream_assets:
                    if upstream_asset.name in self._assets:
                        asset.deps[upstream_asset.name] = upstream_asset.name

    def _resolve_parameters(self) -> dict:
        sig = signature(self.asset_definitions)
        final_params = {}

        for param in sig.parameters.values():
            # # No user defined paramters and no default value
            if param.default is param.empty:
                raise errors.SourceParamError(f"Cannot resolve parameter {param.name} for source {self.name}")

            # ContextualAssetParam not supported for sources (requires an execution context)
            if isinstance(param.default, ContextualAssetParam):
                raise errors.SourceParamError(f"ContextualAssetParam {param.name} not supported in source parameters")

            # Default value is a asset_param -> resolve it
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


class SourceDecorator:
    # Decorator used without parameters
    @overload
    def __new__(cls, func: Callable) -> Source: ...

    # Decorator used with parameters
    @overload
    def __new__(
        cls,
        *,
        name: str | None = None,
        dataset: str | None = None,
        auto_asset_deps: bool = True,
        normalizer: Normalizer | None = None,
        materialization_strategy: MaterializationStrategy = MaterializationStrategy.FLEXIBLE,
    ) -> Self: ...

    def __new__(cls, func: Callable | None = None, *args: Any, **kwargs: Any):
        instance = super().__new__(cls)

        # Decorator used without parameters
        if func:
            assert callable(func)
            instance.__init__(func, **kwargs)
            return instance(func)

        # Decorator used with parameters
        else:
            return instance

    def __init__(
        self,
        func: Callable | None = None,
        name: str | None = None,
        dataset: str | None = None,
        auto_asset_deps: bool = True,
        normalizer: Normalizer | None = None,
        materialization_strategy: MaterializationStrategy = MaterializationStrategy.FLEXIBLE,
    ):
        self.name = name
        self.dataset = dataset
        self.auto_asset_deps = auto_asset_deps
        self.normalizer = normalizer
        self.materialization_strategy = materialization_strategy

    def __call__(self, func: Callable) -> Source:
        """
        Dynamically creates an instance of a concrete Source class that implements the assets method
        using the decorated function.
        """

        class ConcreteSource(Source):
            # Define the dynamically provided asset_definitions method
            def asset_definitions(self, *args: Any, **kwargs: Any) -> Any:
                return func(*args, **kwargs)

            def __repr__(self) -> str:
                return f"<Source {self.name} at {hex(id(self))}>"

        # Override `asset_definitions` signature to dynamically match the signature of the provided `func`
        original_sig, wrapper_sig = signature(func), signature(ConcreteSource.asset_definitions)
        parameters = [wrapper_sig.parameters.get("self"), *original_sig.parameters.values()]
        ConcreteSource.asset_definitions.__signature__ = wrapper_sig.replace(
            parameters=parameters,
            return_annotation=original_sig.return_annotation,
        )

        return ConcreteSource(
            name=self.name or func.__name__,
            dataset=self.dataset,
            auto_asset_deps=self.auto_asset_deps,
            normalizer=self.normalizer,
            materialization_strategy=self.materialization_strategy,
        )


source = SourceDecorator
