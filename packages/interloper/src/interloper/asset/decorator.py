from collections.abc import Callable
from inspect import Parameter, Signature, signature
from typing import Any, overload

from typing_extensions import Self

from interloper.asset.base import Asset
from interloper.execution.strategy import MaterializationStrategy
from interloper.normalizer import Normalizer
from interloper.partitioning.config import PartitionConfig
from interloper.schema import AssetSchema


class ConcreteAsset(Asset):
    def data(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def __repr__(self) -> str:
        source_str = f" from Source {self._source.name}" if self._source else ""
        return f"<Asset {self.name}{source_str} at {hex(id(self))}>"


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
        schema: type[AssetSchema] | None = None,
        normalizer: Normalizer | None = None,
        partitioning: PartitionConfig | None = None,
        materializable: bool | None = None,
        materialization_strategy: MaterializationStrategy | None = None,
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
        schema: type[AssetSchema] | None = None,
        normalizer: Normalizer | None = None,
        partitioning: PartitionConfig | None = None,
        materializable: bool | None = None,
        materialization_strategy: MaterializationStrategy | None = None,
    ):
        self.name = name
        self.dataset = dataset
        self.schema = schema
        self.normalizer = normalizer
        self.partitioning = partitioning
        self.materialization_strategy = materialization_strategy
        self.materializable = materializable

    def __call__(self, func: Callable) -> Asset:
        """
        Dynamically creates an instance of a concrete Asset class that implements the data method
        using the decorated function.
        """

        # class ConcreteAsset(Asset):
        #     # Define the dynamically provided data method
        #     def data(self, *args: Any, **kwargs: Any) -> Any:
        #         return func(*args, **kwargs)

        #     def __repr__(self) -> str:
        #         source_str = f" from Source {self._source.name}" if self._source else ""
        #         return f"<Asset {self.name}{source_str} at {hex(id(self))}>"

        # # Override `data` signature to dynamically match the signature of the provided `func`
        # original_sig, wrapper_sig = signature(func), signature(ConcreteAsset.data)
        # parameters = [wrapper_sig.parameters.get("self"), *original_sig.parameters.values()]
        # ConcreteAsset.data.__signature__ = wrapper_sig.replace(
        #     parameters=parameters,
        #     return_annotation=original_sig.return_annotation,
        # )

        # return ConcreteAsset(
        #     name=self.name or func.__name__,
        #     dataset=self.dataset,
        #     schema=self.schema,
        #     normalizer=self.normalizer,
        #     partitioning=self.partitioning,
        #     materializable=self.materializable,
        #     materialization_strategy=self.materialization_strategy,
        # )

        asset = ConcreteAsset(
            name=self.name or func.__name__,
            dataset=self.dataset,
            schema=self.schema,
            normalizer=self.normalizer,
            partitioning=self.partitioning,
            materializable=self.materializable,
            materialization_strategy=self.materialization_strategy,
        )

        def wrapper(self: Any, **kwargs: Any) -> Any:
            return func(**kwargs)

        sig = signature(func)
        params = [Parameter("self", Parameter.POSITIONAL_OR_KEYWORD)]
        params.extend(list(sig.parameters.values()))
        wrapper.__signature__ = Signature(parameters=params, return_annotation=sig.return_annotation)

        asset.data = wrapper.__get__(asset, ConcreteAsset)

        return asset


asset = AssetDecorator
