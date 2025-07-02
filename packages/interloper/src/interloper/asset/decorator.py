"""This module contains the asset decorator."""
from collections.abc import Callable
from inspect import Parameter, Signature, signature
from typing import Any, overload

from typing_extensions import Self

from interloper.asset.base import Asset
from interloper.execution.strategy import MaterializationStrategy
from interloper.io.base import IO
from interloper.normalizer import Normalizer
from interloper.partitioning.config import PartitionConfig
from interloper.schema import AssetSchema


class ConcreteAsset(Asset):
    """A concrete asset class used by the asset decorator."""

    def data(self, *args: Any, **kwargs: Any) -> Any:
        """This method is implemented dynamically by the asset decorator."""
        raise NotImplementedError

    def __repr__(self) -> str:
        """Return a string representation of the asset."""
        source_str = f" from Source {self._source.name}" if self._source else ""
        return f"<Asset {self.name}{source_str} at {hex(id(self))}>"


class AssetDecorator:
    """A decorator to create assets from functions."""

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
        io: IO | dict[str, IO] | None = None,
        schema: type[AssetSchema] | None = None,
        normalizer: Normalizer | None = None,
        partitioning: PartitionConfig | None = None,
        materializable: bool | None = None,
        materialization_strategy: MaterializationStrategy | None = None,
    ) -> Self: ...

    def __new__(cls, func: Callable | None = None, *args: Any, **kwargs: Any):
        """Create a new instance of the decorator."""
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
        io: IO | dict[str, IO] | None = None,
        schema: type[AssetSchema] | None = None,
        normalizer: Normalizer | None = None,
        partitioning: PartitionConfig | None = None,
        materializable: bool | None = None,
        materialization_strategy: MaterializationStrategy | None = None,
    ):
        """Initialize the decorator.

        Args:
            func: The function to decorate.
            name: The name of the asset.
            dataset: The dataset of the asset.
            io: The IO of the asset.
            schema: The schema of the asset.
            normalizer: The normalizer of the asset.
            partitioning: The partitioning of the asset.
            materializable: Whether the asset is materializable.
            materialization_strategy: The materialization strategy of the asset.
        """
        self.name = name
        self.dataset = dataset
        self.io = io
        self.schema = schema
        self.normalizer = normalizer
        self.partitioning = partitioning
        self.materialization_strategy = materialization_strategy
        self.materializable = materializable

    def __call__(self, func: Callable) -> Asset:
        """Dynamically create an instance of a concrete Asset.

        This method implements the data method using the decorated function.

        Args:
            func: The function to decorate.

        Returns:
            An instance of a concrete Asset.
        """
        asset = ConcreteAsset(
            name=self.name or func.__name__,
            dataset=self.dataset,
            io=self.io,
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
