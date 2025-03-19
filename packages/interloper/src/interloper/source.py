from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from functools import partial
from inspect import signature
from typing import Any, overload

from typing_extensions import Self

from interloper.asset import Asset
from interloper.io.base import IO
from interloper.normalizer import Normalizer
from interloper.param import AssetParam, ContextualAssetParam


@dataclass
class Source(ABC):
    # Should be kept at the top to avoid race conditions in __setattr__
    _assets: dict[str, Asset] = field(default_factory=dict, init=False)

    name: str
    io: dict[str, IO] = field(default_factory=dict)
    default_io_key: str | None = None
    auto_asset_deps: bool = True
    normalizer: Normalizer | None = None

    def __post_init__(self) -> None:
        self._build_assets()

    def __call__(
        self,
        *,
        io: dict[str, IO] | None = None,
        default_io_key: str | None = None,
        **kwargs: Any,
    ) -> "Source":
        copy = replace(self)
        copy.io = io or self.io
        copy.default_io_key = default_io_key or self.default_io_key
        copy.bind(**kwargs)
        copy._build_assets()
        return copy

    def __getattr__(self, name: str) -> Asset:
        """
        Custom __getattr__ method to retrieve an attribute by name.

        This method is intended to force the type checker to recognize attributes as instances of the Asset class.
        However, it should not affect the type of the attributes explicitly defined in the Source class.
        """
        return super().__getattribute__(name)

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Custom `__setattr__` method to handle setting attributes on the object.

        This method ensures that certain attributes are handled in a specific way:
        - Prevents overwriting existing assets by raising an AttributeError if an attempt is made to set an attribute
          that already exists in `_assets`.
        - Propagates specific attributes (defined in `asset_attributes`) to all assets in `_assets` if they are not
          already set.
        """

        # Since _assets is accessed here in the __setattr__ method, we need to handle it separately
        # to avoid recursion with __getattr__ when _assets is not yet initialized.
        if name == "_assets":
            super().__setattr__(name, value)
            return

        # Prevent overwriting the assets
        if name in self._assets:
            raise AttributeError(f"Asset {name} is read-only")

        # Attributes that should be propagated to the assets
        asset_attributes = ["io", "default_io_key", "normalizer"]
        if name in asset_attributes:
            for asset in self._assets.values():
                if not getattr(asset, name):
                    setattr(asset, name, value)

        super().__setattr__(name, value)

    @abstractmethod
    def asset_definitions(self) -> Sequence[Asset]: ...

    @property
    def assets(self) -> list[Asset]:
        return list(self._assets.values())

    def bind(self, **params: Any) -> None:
        sig = signature(self.asset_definitions)
        current_params = [p.name for p in sig.parameters.values()]
        final_params = {}

        for param_name, param_value in params.items():
            if param_name not in current_params:
                raise ValueError(f"Parameter {param_name} is not a valid parameter for source {self.name}")

            final_params[param_name] = param_value

        self.asset_definitions = partial(self.asset_definitions, **final_params)

    def _build_assets(self) -> None:
        self._assets: dict[str, Asset] = {}

        params = self._resolve_parameters()
        for asset in self.asset_definitions(**params):
            if not isinstance(asset, Asset):
                raise ValueError(f"Expected an instance of Asset, but got {type(asset)}")
            if asset.name in self._assets:
                raise ValueError(f"Duplicate asset name '{asset.name}'")

            # Propagate some source attributes to the asset as defaults
            if not asset.has_io:
                asset.io = self.io
            asset.default_io_key = asset.default_io_key or self.default_io_key
            asset.normalizer = asset.normalizer or self.normalizer

            setattr(self, asset.name, asset)
            self._assets[asset.name] = asset

        # Automatically set the deps for the asset if the source has the correspondign asset with the same name
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
            # No user defined paramters and no default value
            if param.default is param.empty:
                raise ValueError(f"Source {self.name} requires a default value for parameter {param.name}")

            # ContextualAssetParam not supported for sources (requires an execution context)
            if isinstance(param.default, ContextualAssetParam):
                raise ValueError(f"ContextualAssetParam {param.name} not supported in source parameters")

            # Default value is a asset_param -> resolve it
            if isinstance(param.default, AssetParam):
                final_params[param.name] = param.default.resolve()
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
        auto_asset_deps: bool = True,
        normalizer: Normalizer | None = None,
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
        auto_asset_deps: bool = True,
        normalizer: Normalizer | None = None,
    ):
        self.name = name
        self.auto_asset_deps = auto_asset_deps
        self.normalizer = normalizer

    def __call__(self, func: Callable) -> Source:
        """
        Dynamically creates an instance of a concrete Source class that implements the assets method
        using the decorated function.
        """

        class ConcreteSource(Source):
            # Define the dynamically provided asset_definitions method
            def asset_definitions(self, *args: Any, **kwargs: Any) -> Any:
                return func(*args, **kwargs)

        # Override `asset_definitions` signature to dynamically match the signature of the provided `func`
        original_sig, wrapper_sig = signature(func), signature(ConcreteSource.asset_definitions)
        parameters = [wrapper_sig.parameters.get("self"), *original_sig.parameters.values()]
        ConcreteSource.asset_definitions.__signature__ = wrapper_sig.replace(
            parameters=parameters,
            return_annotation=original_sig.return_annotation,
        )

        return ConcreteSource(
            name=self.name or func.__name__,
            auto_asset_deps=self.auto_asset_deps,
            normalizer=self.normalizer,
        )


source = SourceDecorator
