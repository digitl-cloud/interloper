from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from inspect import signature
from typing import TYPE_CHECKING, Any, TypeVar, overload

from typing_extensions import Self

from dead.io import IO, IOContext
from dead.sentinel import RunnableSentinel, Sentinel, UpstreamAsset

if TYPE_CHECKING:
    from dead.pipeline import ExecutionContext


T = TypeVar("T")


@dataclass
class Asset(ABC):
    name: str
    deps: dict[str, str] = field(default_factory=dict)
    io: dict[str, IO] = field(default_factory=dict)
    default_io_key: str | None = None
    materializable: bool = True

    def __call__(
        self,
        *,
        io: dict[str, IO] | None = None,
        default_io_key: str | None = None,
        **kwargs: Any,
    ) -> "Asset":
        asset = self._copy()
        asset.io = io or self.io
        asset.default_io_key = default_io_key or self.default_io_key

        asset._bind_params(**kwargs)
        return asset

    def __hash__(self):
        return hash(self.name)

    @abstractmethod
    def data(self) -> Any: ...

    def run(
        self,
        context: "ExecutionContext | None" = None,
        **new_params: Any,
    ) -> Any:
        params = self._evaluate_params(context, **new_params)
        yield from self.data(**params)

    # TODO: private?
    def materialize(
        self,
        context: "ExecutionContext | None" = None,
    ) -> None:
        if not self.materializable:
            raise RuntimeError(f"Asset {self.name} is not materializable")
        if not self.has_io:
            raise RuntimeError(f"Asset {self.name} does not have any IO configured")

        data = self.run(context)

        for x in self.io.values():
            x.write(IOContext(self.name), data)

    @property
    def has_io(self) -> bool:
        return self.io is not None and len(self.io) > 0

    @property
    def upstream_assets(self) -> list[UpstreamAsset]:
        sig = signature(self.data)
        return [param.default for param in sig.parameters.values() if isinstance(param.default, UpstreamAsset)]

    @classmethod
    def from_data_fn(cls, name: str, data_fn: Callable):
        """
        Dynamically creates an instance of a concrete Asset class that implements the data method.
        """

        class ConcreteAsset(cls):
            # Define the dynamically provided data method
            def data(self, *args: Any, **kwargs: Any) -> Any:
                return data_fn(*args, **kwargs)

        # Override `data` signature to dynamically match the signature of the provided `data_fn`
        original_sig, wrapper_sig = signature(data_fn), signature(ConcreteAsset.data)
        parameters = [wrapper_sig.parameters.get("self"), *original_sig.parameters.values()]
        ConcreteAsset.data.__signature__ = wrapper_sig.replace(parameters=parameters)

        return ConcreteAsset(name=name)

    def _copy(self) -> "Asset":
        return self.__class__(
            name=self.name,
            io=self.io,
            materializable=self.materializable,
        )

    def _bind_params(self, **new_params: Any) -> None:
        sig = signature(self.data)
        current_params = [p.name for p in sig.parameters.values()]
        final_params = {}

        for param_name, param_value in new_params.items():
            if param_name not in current_params:
                raise ValueError(f"Parameter {param_name} is not a valid parameter for asset {self.name}")

            final_params[param_name] = param_value

        self.data = partial(self.data, **final_params)

    def _evaluate_params(
        self,
        context: "ExecutionContext | None" = None,
        **new_params: Any,
    ) -> dict:
        sig = signature(self.data)
        final_params = {}

        for param in sig.parameters.values():
            # Runtime user defined parameters take precedence over sentinels
            if param.name in new_params:
                final_params[param.name] = new_params[param.name]
                continue

            # No user defined paramters and no default value
            if param.default is param.empty:
                raise ValueError(f"Cannot resolve parameter {param.name} for asset {self.name}")

            if isinstance(param.default, RunnableSentinel):
                if context is None:
                    raise ValueError(f"RunnableSentinel {param.name} requires an execution context")
                final_params[param.name] = param.default.resolve(context)
                continue

            # Default value is a sentinel
            if isinstance(param.default, Sentinel):
                final_params[param.name] = param.default.resolve()
                continue

            # Default value is not a sentinel
            final_params[param.name] = param.default

        return final_params


class AssetDecorator:
    # Decorator used without parameters
    @overload
    def __new__(cls, func: Callable) -> Asset: ...

    # Decorator used with parameters
    @overload
    def __new__(
        cls,
        *,
        name: str | None = None,
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
    ):
        self.name = name

    def __call__(self, func: Callable) -> Asset:
        return Asset.from_data_fn(
            name=self.name or func.__name__,
            data_fn=func,
        )


asset = AssetDecorator
