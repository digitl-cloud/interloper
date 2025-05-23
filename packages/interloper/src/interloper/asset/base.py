import json
import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, wait
from copy import copy
from functools import partial
from inspect import Parameter, signature
from typing import TYPE_CHECKING, Any, TypeVar

from opentelemetry import trace
from opentelemetry.util.types import Attributes as SpanAttributes
from typing_extensions import Self

from interloper import errors
from interloper.execution.context import AssetExecutionContext, ExecutionContext
from interloper.execution.observable import EventType, Observable
from interloper.execution.strategy import MaterializationStrategy
from interloper.io.base import IO, IOContext
from interloper.normalizer import Normalizer
from interloper.param import AssetParam, ContextualAssetParam, UpstreamAsset
from interloper.partitioning.config import PartitionConfig
from interloper.schema import AssetSchema

if TYPE_CHECKING:
    from interloper.source.base import Source


logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)
T = TypeVar("T")


class Asset(ABC, Observable):
    name: str
    deps: dict[str, str]
    schema: type[AssetSchema] | None
    partitioning: PartitionConfig | None
    _source: "Source | None"
    _dataset: str | None
    _io: dict[str, IO]
    _default_io_key: str | None
    _normalizer: Normalizer | None
    _materializable: bool | None
    _materialization_strategy: MaterializationStrategy | None

    def __init__(
        self,
        name: str,
        *,
        source: "Source | None" = None,
        dataset: str | None = None,
        deps: dict[str, str] | None = None,
        io: dict[str, IO] | None = None,
        default_io_key: str | None = None,
        schema: type[AssetSchema] | None = None,
        normalizer: Normalizer | None = None,
        partitioning: PartitionConfig | None = None,
        materializable: bool | None = None,
        materialization_strategy: MaterializationStrategy | None = None,
    ):
        super().__init__()

        self._source: Source | None = source
        self.name = name
        self.deps = deps or {}
        self.schema = schema
        self.partitioning = partitioning

        # Attributes shared with Source
        # Those attributes need getters to fallback on the source's attributes if not defined
        self._dataset = dataset
        self._io = io or {}
        self._default_io_key = default_io_key
        self._normalizer = normalizer
        self._materializable = materializable
        self._materialization_strategy = materialization_strategy

    #############
    # Magic
    #############
    def __copy__(self) -> Self:
        cls = self.__class__
        _copy = cls.__new__(cls)
        _copy.__dict__.update(self.__dict__)
        return _copy

    def __call__(
        self,
        *,
        io: dict[str, IO] | None = None,  # TODO: support single IO
        default_io_key: str | None = None,
        deps: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> "Asset":
        c = copy(self)
        c._io = io or self._io
        c._default_io_key = default_io_key or self._default_io_key
        c.deps = deps or self.deps
        c.bind(**kwargs)
        return c

    def __hash__(self):
        return hash(self.id)

    #############
    # Properties
    #############
    @property
    def id(self) -> str:
        return f"{self.dataset}.{self.name}" if self.dataset else self.name

    @property
    def source(self) -> "Source | None":
        return self._source

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
    def materializable(self) -> bool:
        if self._materializable is not None:
            return self._materializable
        if self._source:
            return self._source.materializable
        return True

    @materializable.setter
    def materializable(self, value: bool) -> None:
        self._materializable = value

    @property
    def materialization_strategy(self) -> MaterializationStrategy:
        return (
            self._materialization_strategy
            or (self._source and self._source.materialization_strategy)
            or MaterializationStrategy.FLEXIBLE
        )

    @materialization_strategy.setter
    def materialization_strategy(self, value: MaterializationStrategy) -> None:
        self._materialization_strategy = value

    @property
    def data_type(self) -> type | None:
        sig = signature(self.data)
        if sig.return_annotation is Parameter.empty:
            return None
        return sig.return_annotation

    @property
    def has_io(self) -> bool:
        return self.io is not None and len(self.io) > 0

    @property
    def upstream_assets(self) -> list[UpstreamAsset]:
        sig = signature(self.data)
        return [param.default for param in sig.parameters.values() if isinstance(param.default, UpstreamAsset)]

    @property
    def is_partitioned(self) -> bool:
        return self.partitioning is not None

    @property
    def allows_partition_window(self) -> bool:
        # TODO: should check if the asset has a DateWindow asset param?
        return self.partitioning is not None and self.partitioning.allow_window

    #############
    # Public
    #############
    @abstractmethod
    def data(self) -> Any: ...

    def run(
        self,
        context: ExecutionContext | None = None,
        **params: Any,
    ) -> Any:
        """
        Execute + Normalize
        """

        with tracer.start_as_current_span("interloper.asset.run", attributes=self._get_span_attributes(context)):
            data = self._execute(context, **params)
            data = self._normalize(data, context)
            return data

    def materialize(
        self,
        context: ExecutionContext | None = None,
        **params: Any,
    ) -> None:
        """
        Execute + Normalize + Write
        """

        with tracer.start_as_current_span(
            "interloper.asset.materialize", attributes=self._get_span_attributes(context)
        ):
            logger.info(
                f"Materializing asset {self.name} "
                f"{f'partition(s) {context.partition}' if context and context.partition and self.is_partitioned else ''}"  # noqa: E501
            )

            if not self.materializable:
                logger.warning(f"Asset {self.name} is not materializable. Skipping.")
                return

            if not self.has_io:
                raise errors.AssetMaterializationError(f"Asset {self.name} does not have any IO configured")

            data = self._execute(context, **params)
            data = self._normalize(data, context)
            self._write(data, context)

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
    @Observable.event(EventType.ASSET_EXECUTION)
    def _execute(
        self,
        context: ExecutionContext | None = None,
        **params: Any,
    ) -> Any:
        with tracer.start_as_current_span("interloper.asset.execute", attributes=self._get_span_attributes(context)):
            # Parameter resolution
            params, return_type = self._resolve_parameters(context, **params)

            # Execution
            data = self.data(**params)

            if return_type != Parameter.empty and return_type != Any and not isinstance(data, return_type):
                raise errors.AssetValueError(
                    f"Asset {self.name} returned data of type {type(data).__name__}, expected {return_type.__name__}"
                )
            logger.info(f"Asset {self.name} executed (Type check passed ✔)")

            return data

    @Observable.event(EventType.ASSET_NORMALIZATION)
    def _normalize(
        self,
        data: Any,
        context: ExecutionContext | None = None,
    ) -> Any:
        with tracer.start_as_current_span("interloper.asset.normalize", attributes=self._get_span_attributes(context)):
            if self.normalizer:
                try:
                    data = self.normalizer.normalize(data)
                    logger.info(f"Asset {self.name} normalized")
                except Exception as e:
                    raise errors.AssetNormalizationError(f"Failed to normalize data for asset {self.name}: {e}")

                # Schema inference
                try:
                    inferred_schema = self.normalizer.infer_schema(data)
                except Exception as e:
                    raise errors.AssetSchemaError(f"Failed to infer schema for asset {self.name}: {e}")

                if not self.schema:
                    self.schema = inferred_schema
                else:
                    equal, diff = self.schema.compare(inferred_schema)
                    if equal:
                        logger.info(f"Asset {self.name} schema inferred from data (Schema check passed ✔)")
                    else:
                        if self.materialization_strategy == MaterializationStrategy.STRICT:
                            raise errors.AssetNormalizationError(
                                f"<STRICT> The data does not match the provided schema for asset {self.name}"
                            )
                        elif self.materialization_strategy == MaterializationStrategy.FLEXIBLE:
                            logger.warning(
                                f"<FLEXIBLE> Schema mismatch for asset {self.name} between provided "
                                "and inferred schemas"
                            )
                            logger.debug(f"Schema diff: \n{json.dumps(diff, indent=2, default=str)}")
            else:
                logger.warning(f"Asset {self.name} does not have a normalizer. Skipping normalization.")

            return data

    @Observable.event(EventType.ASSET_WRITING)
    def _write(
        self,
        data: Any,
        context: ExecutionContext | None = None,
    ) -> None:
        with tracer.start_as_current_span("interloper.asset.write", attributes=self._get_span_attributes(context)):
            # if context:
            #     if context.partition and not self.partitioning:
            #         raise errors.AssetMaterializationError(
            #             f"Asset {self.name} does not support partitioning (missing partitioning config)"
            #         )

            # Current approach: an asset can be materialized if the context has a partition but the asset does not
            # support partitioning. In that case, we drop the partition from the IO context.
            # This allows non-partitioned assets to be materialized alongside partitioned assets.
            # TODO: approach to be reviewed
            partition = None
            if context and context.partition and self.is_partitioned:
                partition = context.partition

            io_context = IOContext(asset=self, partition=partition)

            with ThreadPoolExecutor() as executor:
                futures = []
                for io in self.io.values():
                    futures.append(executor.submit(io.write, io_context, data))

                wait(futures)

                for future in futures:
                    future.result()

    def _resolve_parameters(
        self,
        context: ExecutionContext | None = None,
        **overriding_params: Any,
    ) -> tuple[dict[str, Any], Any]:
        sig = signature(self.data)
        final_params = {}

        if sig.return_annotation is None:
            raise errors.AssetDefinitionError(f"None is not a valid return type for asset {self.name}")

        for param in sig.parameters.values():
            # Overriding param: take precedences over default AssetParams
            if param.name in overriding_params:
                overriding_param = overriding_params[param.name]

                # If the overriding param is an AssetParam, we want to resolve it. Since the logic to resolve
                # AssetParam & ContextualAssetParam is handled below based on the default value,we choose to replace
                # the default value with the overriding param and proceed with the resolution.
                if isinstance(overriding_param, AssetParam):
                    param = param.replace(default=overriding_param)

                # If the overriding param is not an AssetParam, we directly use the value
                else:
                    final_params[param.name] = overriding_param
                    continue

            # No user defined paramters and no default value
            if param.default is param.empty:
                raise errors.AssetParamResolutionError(f"Cannot resolve parameter {param.name} for asset {self.name}")

            # Default value is a ContextualAssetParam
            if isinstance(param.default, ContextualAssetParam):
                if context is None:
                    raise errors.AssetParamResolutionError(
                        f"Cannot resolve parameter {param.name} for asset {self.name}"
                        # f"ContextualAssetParam {param.name} requires an execution context"
                    )

                try:
                    context = AssetExecutionContext(
                        executed_asset=self,
                        assets=context.assets,
                        partition=context.partition,
                    )
                    final_params[param.name] = param.default.resolve(context)
                except Exception as e:
                    raise errors.AssetParamResolutionError(
                        f"Failed to resolve parameter {param.name} for asset {self.name}: {e}"
                    )
                continue

            # Default value is a asset_param
            if isinstance(param.default, AssetParam):
                try:
                    final_params[param.name] = param.default.resolve()
                except Exception as e:
                    raise errors.AssetParamResolutionError(
                        f"Failed to resolve parameter {param.name} for asset {self.name}: {e}"
                    )
                continue

            # Default value is not a asset_param
            final_params[param.name] = param.default

        return_type = sig.return_annotation
        # return_type is not allow to be a Generic
        if hasattr(return_type, "__origin__"):
            raise errors.AssetDefinitionError(f"Generic return type {return_type} is not allowed for asset {self.name}")

        return final_params, return_type

    def _get_span_attributes(self, context: ExecutionContext | None = None) -> SpanAttributes:
        attributes: SpanAttributes = {
            "asset_id": self.id,
            "asset_name": self.name,
        }
        if self.source:
            attributes["source_name"] = self.source.name
        if self.dataset:
            attributes["dataset"] = self.dataset
        if context and context.partition:
            attributes["partition"] = str(context.partition)
        return attributes

