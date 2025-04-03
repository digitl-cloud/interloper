import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from interloper.partitioning.partitions import Partition
from interloper.partitioning.ranges import PartitionRange
from interloper.reconciler import Reconciler

if TYPE_CHECKING:
    from interloper.asset import Asset

logger = logging.getLogger(__name__)
T = TypeVar("T")


# TODO: use ExecutionContext instead?
@dataclass(frozen=True)
class IOContext:
    asset: "Asset"
    partition: Partition | PartitionRange | None = None


class IO(ABC, Generic[T]):
    @abstractmethod
    def write(self, context: IOContext, data: T) -> None:
        pass

    @abstractmethod
    def read(self, context: IOContext) -> T:
        pass


@dataclass
class IOHandler(ABC, Generic[T]):
    type: type[T]
    reconciler: Reconciler[T]  # TODO: should be optional?

    @abstractmethod
    def write(self, context: IOContext, data: T) -> None: ...

    @abstractmethod
    def read(self, context: IOContext) -> T: ...


@dataclass
class TypedIO(Generic[T], IO[T]):
    handler: IOHandler[T]

    def write(self, context: IOContext, data: T) -> None:
        self._check_asset_type(data)
        self.handler.write(context, data)

    def read(self, context: IOContext) -> T:
        data = self.handler.read(context)
        self._check_asset_type(data)
        return data

    def _check_asset_type(self, data: Any) -> None:
        if not isinstance(data, self.handler.type):
            raise ValueError(
                f"Data type {type(data).__name__} is not supported by {self.__class__.__name__}. "
                f"Expected type {self.handler.type.__name__}"
            )
