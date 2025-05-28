from collections.abc import Callable
from inspect import Parameter, Signature, signature
from typing import Any, overload

from typing_extensions import Self

from interloper.execution.strategy import MaterializationStrategy
from interloper.normalizer import Normalizer
from interloper.source.base import Source


class ConcreteSource(Source):
    def asset_definitions(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"<Source {self.name} at {hex(id(self))}>"


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
        materializable: bool = True,
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
        materializable: bool = True,
        materialization_strategy: MaterializationStrategy = MaterializationStrategy.FLEXIBLE,
    ):
        self.name = name
        self.dataset = dataset
        self.auto_asset_deps = auto_asset_deps
        self.normalizer = normalizer
        self.materializable = materializable
        self.materialization_strategy = materialization_strategy

    def __call__(self, func: Callable) -> Source:
        """
        Dynamically creates an instance of a concrete Source class that implements the assets method
        using the decorated function.
        """

        # class ConcreteSource(Source):
        #     # Define the dynamically provided asset_definitions method
        #     def asset_definitions(self, *args: Any, **kwargs: Any) -> Any:
        #         return func(*args, **kwargs)

        #     def __repr__(self) -> str:
        #         return f"<Source {self.name} at {hex(id(self))}>"

        # # Override `asset_definitions` signature to dynamically match the signature of the provided `func`
        # original_sig, wrapper_sig = signature(func), signature(ConcreteSource.asset_definitions)
        # parameters = [wrapper_sig.parameters.get("self"), *original_sig.parameters.values()]
        # ConcreteSource.asset_definitions.__signature__ = wrapper_sig.replace(
        #     parameters=parameters,
        #     return_annotation=original_sig.return_annotation,
        # )

        # return ConcreteSource(
        #     name=self.name or func.__name__,
        #     dataset=self.dataset,
        #     auto_asset_deps=self.auto_asset_deps,
        #     normalizer=self.normalizer,
        #     materializable=self.materializable,
        #     materialization_strategy=self.materialization_strategy,
        # )

        source = ConcreteSource(
            name=self.name or func.__name__,
            dataset=self.dataset,
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
