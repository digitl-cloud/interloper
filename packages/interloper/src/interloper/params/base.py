"""This module contains the base asset parameter classes."""

import logging
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from interloper.execution.context import AssetExecutionContext

logger = logging.getLogger(__name__)
T = TypeVar("T")


class AssetParam(ABC, Generic[T]):
    """An abstract class for asset parameters."""

    # Forces an AssetParam's instance to be of type T
    def __new__(cls) -> T:
        """Create a new instance of the asset parameter."""
        return super().__new__(cls)  # type: ignore

    @abstractmethod
    def resolve(self) -> T:
        """Resolve the value of the parameter."""
        ...


class ContextualAssetParam(AssetParam[T], Generic[T]):
    """An abstract class for contextual asset parameters."""

    @abstractmethod
    def resolve(self, context: AssetExecutionContext) -> T:
        """Resolve the value of the parameter.

        Args:
            context: The execution context.
        """
        ...
