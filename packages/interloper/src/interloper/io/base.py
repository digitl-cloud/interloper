"""This module contains the base classes for IO."""
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from interloper.partitioning.partition import Partition
from interloper.partitioning.window import PartitionWindow
from interloper.reconciler import Reconciler
from interloper.utils.typing import match_type, safe_isinstance

if TYPE_CHECKING:
    from interloper.asset.base import Asset

logger = logging.getLogger(__name__)
T = TypeVar("T")


@dataclass(frozen=True)
class IOContext:
    """The context for an IO operation.

    Attributes:
        asset: The asset being read or written.
        partition: The partition being read or written.
    """

    asset: "Asset"
    partition: Partition | PartitionWindow | None = None


class IO(ABC):
    """An abstract class for IO operations."""

    @abstractmethod
    def write(self, context: IOContext, data: Any) -> None:
        """Write data.

        Args:
            context: The IO context.
            data: The data to write.
        """
        pass

    @abstractmethod
    def read(self, context: IOContext) -> Any:
        """Read data.

        Args:
            context: The IO context.

        Returns:
            The data that was read.
        """
        pass


@dataclass
class IOHandler(ABC, Generic[T]):
    """An abstract class for IO handlers."""

    type: type[T]
    reconciler: Reconciler[T] | None = None  # TODO: should be optional?

    @abstractmethod
    def write(self, context: IOContext, data: T) -> None:
        """Write data.

        Args:
            context: The IO context.
            data: The data to write.
        """
        ...

    @abstractmethod
    def read(self, context: IOContext) -> T:
        """Read data.

        Args:
            context: The IO context.

        Returns:
            The data that was read.
        """
        ...

    def verify_type(self, data: Any) -> None:
        """Verify the type of the data.

        Args:
            data: The data to verify.

        Raises:
            ValueError: If the data type is not supported.
        """
        if not safe_isinstance(data, self.type):
            raise ValueError(
                f"Data type {type(data).__name__} is not supported by {self.__class__.__name__}. "
                f"Expected type {self.type.__name__}."
            )


class TypedIO(IO):
    """An IO class that uses handlers for different data types."""

    _handlers: dict[type, IOHandler]

    def __init__(self, handlers: list[IOHandler]) -> None:
        """Initialize the TypedIO.

        Args:
            handlers: The handlers to use.
        """
        self._handlers = {handler.type: handler for handler in handlers}

    @property
    def supported_types(self) -> set[type]:
        """The types supported by the IO."""
        if self._handlers is None:
            return set()

        return set(self._handlers.keys())

    def get_handler(self, data_type: type | None) -> IOHandler:
        """Get the handler for a given data type.

        Args:
            data_type: The data type to get the handler for.

        Returns:
            The handler for the given data type.

        Raises:
            RuntimeError: If the data type is not supported.
        """
        if data_type is None:
            raise RuntimeError(
                f"IO {self.__class__.__name__} requires the asset to have a data type in order to select the correct "
                "handler. Add the return type annotation to the asset function that corresponds to one of the "
                f"supported types: {self.supported_types}"
            )

        for handler_type in self._handlers.keys():
            if match_type(data_type, handler_type):
                return self._handlers[handler_type]

        raise RuntimeError(
            f"IO {self.__class__.__name__} does not support data type {data_type.__name__}. "
            f"Supported types: {self.supported_types}"
        )
