"""This module contains the source decorator."""
from collections.abc import Callable
from inspect import Parameter, Signature, signature
from typing import Any, overload

from typing_extensions import Self

from interloper.execution.strategy import MaterializationStrategy
from interloper.io.base import IO
from interloper.normalizer import Normalizer
from interloper.source.base import Source


class ConcreteSource(Source):
    """A concrete source class used by the source decorator."""

    def asset_definitions(self, *args: Any, **kwargs: Any) -> Any:
        """This method is implemented dynamically by the source decorator."""
        raise NotImplementedError

    def __repr__(self) -> str:
        """Return a string representation of the source."""
        return f"<Source {self.name} at {hex(id(self))}>"


class SourceDecorator:
    """A decorator to create sources from functions."""

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
        io: IO | dict[str, IO] | None = None,
        materializable: bool = True,
        auto_asset_deps: bool = True,
        normalizer: Normalizer | None = None,
        materialization_strategy: MaterializationStrategy = MaterializationStrategy.FLEXIBLE,
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
        auto_asset_deps: bool = True,
        normalizer: Normalizer | None = None,
        materializable: bool = True,
        materialization_strategy: MaterializationStrategy = MaterializationStrategy.FLEXIBLE,
    ):
        """Initialize the decorator.

        Args:
            func: The function to decorate.
            name: The name of the source.
            dataset: The dataset of the source.
            io: The IO of the source.
            auto_asset_deps: Whether to automatically set the upstream dependencies for assets.
            normalizer: The normalizer of the source.
            materializable: Whether the assets in the source are materializable.
            materialization_strategy: The materialization strategy of the source.
        """
        self.name = name
        self.dataset = dataset
        self.io = io
        self.auto_asset_deps = auto_asset_deps
        self.normalizer = normalizer
        self.materializable = materializable
        self.materialization_strategy = materialization_strategy

    def __call__(self, func: Callable) -> Source:
        """Dynamically create an instance of a concrete Source.

        This method implements the assets method using the decorated function.

        Args:
            func: The function to decorate.

        Returns:
            An instance of a concrete Source.
        """
        source = ConcreteSource(
            name=self.name or func.__name__,
            dataset=self.dataset,
            io=self.io,
            auto_asset_deps=self.auto_asset_deps,
            normalizer=self.normalizer,
            materializable=self.materializable,
            materialization_strategy=self.materialization_strategy,
        )

        def wrapper(self: Any, **kwargs: Any) -> Any:
            return func(**kwargs)

        sig = signature(func)
        params = [Parameter("self", Parameter.POSITIONAL_OR_KEYWORD)]
        params.extend(list(sig.parameters.values()))
        wrapper.__signature__ = Signature(parameters=params, return_annotation=sig.return_annotation)

        source.asset_definitions = wrapper.__get__(source, ConcreteSource)

        return source


source = SourceDecorator
