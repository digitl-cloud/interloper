import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from inspect import Parameter, signature
from typing import TYPE_CHECKING, Any, TypeVar, overload

from typing_extensions import Self

from dead.core.io import IO, IOContext
from dead.core.param import AssetParam, ContextualAssetParam, TimeAssetParam, UpstreamAsset
from dead.core.schema import TTableSchema
from dead.core.utils import safe_isinstance

if TYPE_CHECKING:
    from dead.core.pipeline import ExecutionContext


logger = logging.getLogger(__name__)
T = TypeVar("T")


@dataclass
class Asset(ABC):
    name: str
    deps: dict[str, str] = field(default_factory=dict)
    io: dict[str, IO] = field(default_factory=dict)
    materializable: bool = True
    default_io_key: str | None = None
    schema: TTableSchema | None = None
    partition_column: str | None = None

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
        asset.bind(**kwargs)
        return asset

    def __hash__(self):
        return hash(self.name)

    @abstractmethod
    def data(self) -> Any: ...

    def run(
        self,
        context: "ExecutionContext | None" = None,
        **params: Any,
    ) -> Any:
        params, return_type = self._resolve_parameters(context, **params)
        data = self.data(**params)

        # TODO: review safe_isinstance
        if return_type != Parameter.empty and return_type is not Any and not safe_isinstance(data, return_type):
            raise TypeError(f"Asset {self.name} returned data of type {type(data)}, expected {return_type}")

        logger.debug(f"Asset {self.name} executed (Type check passed ✔)")

        return data

    def materialize(
        self,
        context: "ExecutionContext | None" = None,
    ) -> None:
        logger.info(f"Materializing asset {self.name}...")

        if not self.materializable:
            logger.warning(f"Asset {self.name} is not materializable. Skipping.")
            return

        if not self.has_io:
            raise RuntimeError(f"Asset {self.name} does not have any IO configured")

        if context:
            if context.partition and not self.partition_column:
                raise ValueError(f"Asset {self.name} does not support partitioning (missing partition_column config)")

        data = self.run(context)

        io_context = IOContext(
            asset=self,
            partition=context.partition if context else None,
        )
        for x in self.io.values():
            x.write(io_context, data)

        logger.info(f"Asset {self.name} materialization complete")

    def bind(self, **params: Any) -> None:
        sig = signature(self.data)
        current_params = [p.name for p in sig.parameters.values()]
        final_params = {}

        for param_name, param_value in params.items():
            if param_name not in current_params:
                raise ValueError(f"Parameter {param_name} is not a valid parameter for asset {self.name}")

            final_params[param_name] = param_value

        self.data = partial(self.data, **final_params)

    @property
    def has_io(self) -> bool:
        return self.io is not None and len(self.io) > 0

    @property
    def upstream_assets(self) -> list[UpstreamAsset]:
        sig = signature(self.data)
        return [param.default for param in sig.parameters.values() if isinstance(param.default, UpstreamAsset)]

    def _copy(self) -> "Asset":
        return self.__class__(
            name=self.name,
            io=self.io,
            materializable=self.materializable,
            schema=self.schema,
        )

    def _resolve_parameters(
        self,
        context: "ExecutionContext | None" = None,
        **overriding_params: Any,
    ) -> tuple[dict[str, Any], Any]:
        sig = signature(self.data)
        final_params = {}
        has_time_asset_param = False

        if sig.return_annotation is None:
            raise ValueError(f"None is not a valid return type for asset {self.name}")

        for param in sig.parameters.values():
            # Runtime user defined parameters take precedence over asset_params
            if param.name in overriding_params:
                final_params[param.name] = overriding_params[param.name]
                continue

            # No user defined paramters and no default value
            if param.default is param.empty:
                raise ValueError(f"Cannot resolve parameter {param.name} for asset {self.name}")

            if isinstance(param.default, TimeAssetParam):
                if has_time_asset_param:
                    raise ValueError("Asset can only have one TimeAssetParam")
                has_time_asset_param = True

            if isinstance(param.default, ContextualAssetParam):
                if context is None:
                    raise ValueError(f"ContextualAssetParam {param.name} requires an execution context")
                final_params[param.name] = param.default.resolve(context)
                continue

            # Default value is a asset_param
            if isinstance(param.default, AssetParam):
                final_params[param.name] = param.default.resolve()
                continue

            # Default value is not a asset_param
            final_params[param.name] = param.default

        return final_params, sig.return_annotation


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
        schema: TTableSchema | None = None,
        partition_column: str | None = None,
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
        schema: TTableSchema | None = None,
        partition_column: str | None = None,
    ):
        self.name = name
        self.schema = schema
        self.partition_column = partition_column

    def __call__(self, func: Callable) -> Asset:
        """
        Dynamically creates an instance of a concrete Asset class that implements the data method
        using the decorated function.
        """

        class ConcreteAsset(Asset):
            # Define the dynamically provided data method
            def data(self, *args: Any, **kwargs: Any) -> Any:
                return func(*args, **kwargs)

        # Override `data` signature to dynamically match the signature of the provided `func`
        original_sig, wrapper_sig = signature(func), signature(ConcreteAsset.data)
        parameters = [wrapper_sig.parameters.get("self"), *original_sig.parameters.values()]
        ConcreteAsset.data.__signature__ = wrapper_sig.replace(
            parameters=parameters,
            return_annotation=original_sig.return_annotation,
        )

        return ConcreteAsset(
            name=self.name or func.__name__,
            schema=self.schema,
            partition_column=self.partition_column,
        )


asset = AssetDecorator
