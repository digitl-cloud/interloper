import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from copy import copy
from functools import partial
from inspect import Parameter, signature
from typing import TYPE_CHECKING, Any, TypeVar, overload

from typing_extensions import Self

from interloper import errors
from interloper.io.base import IO, IOContext
from interloper.normalizer import Normalizer
from interloper.param import AssetParam, ContextualAssetParam, UpstreamAsset
from interloper.partitioning.strategies import PartitionStrategy
from interloper.schema import TableSchema

if TYPE_CHECKING:
    from interloper.pipeline import ExecutionContext
    from interloper.source import Source


logger = logging.getLogger(__name__)
T = TypeVar("T")


class Asset(ABC):
    name: str
    deps: dict[str, str]
    materializable: bool
    schema: type[TableSchema] | None
    partition_strategy: PartitionStrategy | None
    _source: "Source | None"
    _dataset: str | None
    _io: dict[str, IO[Any]]
    _default_io_key: str | None
    _normalizer: Normalizer | None

    def __init__(
        self,
        name: str,
        *,
        source: "Source | None" = None,
        dataset: str | None = None,
        deps: dict[str, str] | None = None,
        io: dict[str, IO[Any]] | None = None,
        materializable: bool = True,
        default_io_key: str | None = None,
        schema: type[TableSchema] | None = None,
        normalizer: Normalizer | None = None,
        partition_strategy: PartitionStrategy | None = None,
    ):
        self._source: Source | None = source

        self.name = name
        self.deps = deps or {}
        self.materializable = materializable
        self.schema = schema
        self.partition_strategy = partition_strategy

        # Attributes shared with Source
        # Those attributes need getters to fallback on the source's attributes if not defined
        self._dataset = dataset
        self._io = io or {}
        self._default_io_key = default_io_key
        self._normalizer = normalizer

    #############
    # Magic
    #############
    def __call__(
        self,
        *,
        io: dict[str, IO] | None = None,  # TODO: support single IO
        default_io_key: str | None = None,
        **kwargs: Any,
    ) -> "Asset":
        c = copy(self)  # TODO: implement __copy__
        c._io = io or self._io
        c._default_io_key = default_io_key or self._default_io_key
        c.bind(**kwargs)
        return c

    def __hash__(self):
        return hash(self.name)

    #############
    # Properties
    #############
    @property
    def dataset(self) -> str | None:
        return self._dataset or (self._source and self._source.dataset)

    @dataset.setter
    def dataset(self, value: str | None) -> None:
        self._dataset = value

    @property
    def io(self) -> dict[str, IO]:
        return self._io or (self._source and self._source.io) or {}

    @io.setter
    def io(self, value: dict[str, IO]) -> None:
        self._io = value

    @property
    def default_io_key(self) -> str | None:
        return self._default_io_key or (self._source and self._source.default_io_key)

    @default_io_key.setter
    def default_io_key(self, value: str | None) -> None:
        self._default_io_key = value

    @property
    def normalizer(self) -> Normalizer | None:
        return self._normalizer or (self._source and self._source.normalizer)

    @normalizer.setter
    def normalizer(self, value: Normalizer) -> None:
        if not isinstance(value, Normalizer):
            raise errors.AssetValueError(f"Normalizer must be an instance of Normalizer, got {type(value).__name__}")
        self._normalizer = value

    @property
    def has_io(self) -> bool:
        return self.io is not None and len(self.io) > 0

    @property
    def upstream_assets(self) -> list[UpstreamAsset]:
        sig = signature(self.data)
        return [param.default for param in sig.parameters.values() if isinstance(param.default, UpstreamAsset)]

    @property
    def allows_partition_window(self) -> bool:
        # TODO: should check if the asset has a DateWindow asset param?
        return self.partition_strategy is not None and self.partition_strategy.allow_window

    #############
    # Public
    #############
    @abstractmethod
    def data(self) -> Any: ...

    def run(
        self,
        context: "ExecutionContext | None" = None,
        **params: Any,
    ) -> Any:
        # Parameter resolution
        params, return_type = self._resolve_parameters(context, **params)

        # Execution
        data = self.data(**params)

        # Type checking
        if return_type != Parameter.empty and return_type is not Any and not isinstance(data, return_type):
            raise errors.AssetValueError(
                f"Asset {self.name} returned data of type {type(data).__name__}, expected {return_type.__name__}"
            )
        logger.info(f"Asset {self.name} executed (Type check passed ✔)")

        # Normalization
        if self.normalizer:
            try:
                data = self.normalizer.normalize(data)
                logger.info(f"Asset {self.name} normalized")
            except Exception as e:
                raise errors.AssetMaterializationError(f"Failed to normalize data for asset {self.name}: {e}")

            # Schema inference
            try:
                inferred_schema = self.normalizer.infer_schema(data)
                # inferred_schema.print_implementation()
            except Exception as e:
                raise errors.AssetSchemaError(f"Failed to infer schema for asset {self.name}: {e}")

            # TODO: schema resolution strategy should play a role here
            if not self.schema:
                self.schema = inferred_schema
            else:
                equal, diff = self.schema.compare(inferred_schema)
                if equal:
                    logger.info(f"Asset {self.name} schema inferred from data (Schema check passed ✔)")
                else:
                    logger.warning(f"Schema mismatch for asset {self.name} between provided and inferred schemas")
                    logger.debug(f"Schema diff: \n{json.dumps(diff, indent=2, default=str)}")

        return data

    def materialize(
        self,
        context: "ExecutionContext | None" = None,
    ) -> None:
        logger.info(f"Materializing asset {self.name} {f'partition(s) {context.partition}' if context else ''}")

        if not self.materializable:
            logger.warning(f"Asset {self.name} is not materializable. Skipping.")
            return

        if not self.has_io:
            raise errors.AssetMaterializationError(f"Asset {self.name} does not have any IO configured")

        if context:
            if context.partition and not self.partition_strategy:
                raise errors.AssetMaterializationError(
                    f"Asset {self.name} does not support partitioning (missing partition_strategy config)"
                )

        data = self.run(context)
        io_context = IOContext(
            asset=self,
            partition=context.partition if context else None,
        )
        for io in self.io.values():
            io.write(io_context, data)

        logger.info(f"Asset {self.name} materialization complete")

    def bind(self, ignore_unknown_params: bool = False, **params: Any) -> None:
        sig = signature(self.data)
        current_params = [p.name for p in sig.parameters.values()]
        final_params = {}

        for param_name, param_value in params.items():
            if param_name not in current_params:
                if not ignore_unknown_params:
                    raise errors.AssetValueError(
                        f"Parameter {param_name} is not a valid parameter for asset {self.name}"
                    )
                continue

            final_params[param_name] = param_value

        self.data = partial(self.data, **final_params)

    #############
    # Private
    #############
    def _resolve_parameters(
        self,
        context: "ExecutionContext | None" = None,
        **overriding_params: Any,
    ) -> tuple[dict[str, Any], Any]:
        sig = signature(self.data)
        final_params = {}

        if sig.return_annotation is None:
            raise errors.AssetDefinitionError(f"None is not a valid return type for asset {self.name}")

        for param in sig.parameters.values():
            # Runtime user defined parameters take precedence over asset_params
            if param.name in overriding_params:
                final_params[param.name] = overriding_params[param.name]
                continue

            # No user defined paramters and no default value
            if param.default is param.empty:
                raise errors.AssetParamResolutionError(f"Cannot resolve parameter {param.name} for asset {self.name}")

            if isinstance(param.default, ContextualAssetParam):
                if context is None:
                    raise errors.AssetParamResolutionError(
                        f"Cannot resolve parameter {param.name} for asset {self.name}"
                        # f"ContextualAssetParam {param.name} requires an execution context"
                    )
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
        dataset: str | None = None,
        schema: type[TableSchema] | None = None,
        normalizer: Normalizer | None = None,
        partition_strategy: PartitionStrategy | None = None,
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
        schema: type[TableSchema] | None = None,
        normalizer: Normalizer | None = None,
        partition_strategy: PartitionStrategy | None = None,
    ):
        self.name = name
        self.dataset = dataset
        self.schema = schema
        self.normalizer = normalizer
        self.partition_strategy = partition_strategy

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
            dataset=self.dataset,
            schema=self.schema,
            normalizer=self.normalizer,
            partition_strategy=self.partition_strategy,
        )


asset = AssetDecorator
