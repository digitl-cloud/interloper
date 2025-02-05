from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from functools import partial
from inspect import signature
from typing import Any, overload

from typing_extensions import Self

from dead.asset import Asset
from dead.io import IO
from dead.sentinel import RunnableSentinel, Sentinel


class Source(ABC):
    name: str
    _io: dict[str, IO]
    default_io_key: str | None
    auto_asset_deps: bool

    def __init__(
        self,
        name: str,
        io: dict[str, IO] | None = None,
        default_io_key: str | None = None,
        auto_asset_deps: bool = True,
    ) -> None:
        self.name = name
        self._io = io or {}
        self.default_io_key = default_io_key
        self.auto_asset_deps = auto_asset_deps

        self._build_assets()

    def __call__(
        self,
        *,
        io: dict[str, IO] | None = None,
        default_io_key: str | None = None,
        **kwargs: Any,
    ) -> "Source":
        source = self._copy()
        source.io = io or self.io
        source.default_io_key = default_io_key or self.default_io_key

        source._bind_params(**kwargs)
        source._build_assets()
        return source

    def __getattr__(self, name: str) -> Asset:
        return super().__getattribute__(name)

    @abstractmethod
    def asset_definitions(self) -> Sequence[Asset]: ...

    @property
    def assets(self) -> list[Asset]:
        return list(self._assets.values())

    @property
    def io(self) -> dict[str, IO]:
        return self._io

    @io.setter
    def io(self, value: dict[str, IO]) -> None:
        self._io = value

        for asset in self._assets.values():
            if not asset.has_io and value is not None:
                asset.io = value

    @classmethod
    def from_asset_defs_fn(
        cls,
        name: str,
        asset_defs_fn: Callable,
        auto_asset_deps: bool = True,
    ):
        """
        Dynamically creates an instance of a concrete Source class that implements the assets method.
        """

        class ConcreteSource(cls):
            # Define the dynamically provided asset_definitions method
            def asset_definitions(self, *args: Any, **kwargs: Any) -> Any:
                return asset_defs_fn(*args, **kwargs)

        # Override `asset_definitions` signature to dynamically match the signature of the provided `asset_defs_fn`
        original_sig, wrapper_sig = signature(asset_defs_fn), signature(ConcreteSource.asset_definitions)
        parameters = [wrapper_sig.parameters.get("self"), *original_sig.parameters.values()]
        ConcreteSource.asset_definitions.__signature__ = wrapper_sig.replace(parameters=parameters)

        return ConcreteSource(
            name=name,
            auto_asset_deps=auto_asset_deps,
        )

    def _build_assets(self) -> None:
        self._assets: dict[str, Asset] = {}

        params = self._evaluate_params()
        for asset in self.asset_definitions(**params):
            if not isinstance(asset, Asset):
                raise ValueError(f"Expected an instance of Asset, but got {type(asset)}")
            if asset.name in self._assets:
                raise ValueError(f"Duplicate asset name '{asset.name}'")

            if not asset.has_io:
                asset.io = self.io
                
            asset.default_io_key = asset.default_io_key or self.default_io_key

            # Automatically set the deps for the asset if the source has another asset with the same name
            if self.auto_asset_deps:
                for upstream_asset in asset.upstream_assets:
                    if upstream_asset.name in self._assets:
                        asset.deps[upstream_asset.name] = upstream_asset.name

            self._assets[asset.name] = asset
            setattr(self, asset.name, asset)

    def _copy(self) -> "Source":
        return self.__class__(
            name=self.name,
            io=self.io,
        )

    def _bind_params(self, **new_params: Any) -> None:
        sig = signature(self.asset_definitions)
        current_params = [p.name for p in sig.parameters.values()]
        final_params = {}

        for param_name, param_value in new_params.items():
            if param_name not in current_params:
                raise ValueError(f"Parameter {param_name} is not a valid parameter for source {self.name}")

            final_params[param_name] = param_value

        self.asset_definitions = partial(self.asset_definitions, **final_params)

    def _evaluate_params(self) -> dict:
        sig = signature(self.asset_definitions)
        final_params = {}

        for param in sig.parameters.values():
            # # Runtime user defined parameters take precedence over sentinels
            # if param.name in new_params:
            #     final_params[param.name] = new_params[param.name]
            #     continue

            # No user defined paramters and no default value
            if param.default is param.empty:
                raise ValueError(f"Cannot resolve parameter {param.name} for source {self.name}")

            # RunnableSentinel not supported for sources (requires an execution context)
            if isinstance(param.default, RunnableSentinel):
                raise ValueError(f"RunnableSentinel {param.name} not supported in source parameters")

            # Default value is a sentinel -> resolve it
            if isinstance(param.default, Sentinel):
                final_params[param.name] = param.default.resolve()
                continue

            # Default value is not a sentinel
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
    ):
        self.name = name
        self.auto_asset_deps = auto_asset_deps

    def __call__(self, func: Callable) -> Source:
        return Source.from_asset_defs_fn(
            name=self.name or func.__name__,
            asset_defs_fn=func,
            auto_asset_deps=self.auto_asset_deps,
        )


source = SourceDecorator
