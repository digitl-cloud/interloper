import logging
import pickle
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from interloper.core.partitioning import Partition, PartitionRange
from interloper.core.reconciler import Reconciler
from interloper.core.sanitizer import Sanitizer
from interloper.core.schema import TTableSchema

if TYPE_CHECKING:
    from interloper.core.asset import Asset

logger = logging.getLogger(__name__)
T = TypeVar("T")


# TODO: use ExecutionContext instead?
@dataclass(frozen=True)
class IOContext:
    asset: "Asset"
    partition: Partition | PartitionRange | None = None


@dataclass
class IOHandler(ABC, Generic[T]):
    type: type[T]
    sanitizer: Sanitizer[T]
    reconciler: Reconciler[T]

    @abstractmethod
    def write(self, context: IOContext, data: T) -> None: ...

    @abstractmethod
    def read(self, context: IOContext) -> T: ...


class IO(ABC, Generic[T]):
    @abstractmethod
    def write(self, context: IOContext, data: T) -> None:
        pass

    @abstractmethod
    def read(self, context: IOContext) -> T:
        pass


class TypedIO(Generic[T], IO[T]):
    def __init__(self, handler: IOHandler[T]):
        self.handler = handler

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
                f"Data type {type(data)} is not supported by {self.__class__.__name__}. "
                f"Expected type {self.handler.type.__name__}"
            )


class DatabaseClient(ABC):
    @abstractmethod
    def table_exists(self, table_name: str) -> bool: ...

    @abstractmethod
    def fetch_table_schema(self, table_name: str) -> dict[str, str]: ...

    @abstractmethod
    def create_table(self, table_name: str, schema: TTableSchema) -> None: ...

    @abstractmethod
    def get_select_partition_statement(
        self, table_name: str, column: str, partition: Partition | PartitionRange
    ) -> None: ...

    @abstractmethod
    def delete_partition(self, table_name: str, column: str, partition: Partition | PartitionRange) -> None: ...


class DatabaseIO(Generic[T], TypedIO[T]):
    def __init__(self, client: DatabaseClient, handler: IOHandler[T]):
        self.client = client
        self.handler = handler

    def write(self, context: IOContext, data: T) -> None:
        self._check_asset_type(data)

        assert context.asset.schema
        if not self.client.table_exists(context.asset.name):
            self.client.create_table(context.asset.name, context.asset.schema)

        if context.partition:
            assert context.asset.partition_strategy
            self.client.delete_partition(context.asset.name, context.asset.partition_strategy.column, context.partition)

        data = self.handler.sanitizer.sanitize(data)
        data = self.handler.reconciler.reconcile(data, self.client.fetch_table_schema(context.asset.name))

        self.handler.write(context, data)

    def read(self, context: IOContext) -> T:
        data = self.handler.read(context)
        self._check_asset_type(data)
        return data


class FileIO(IO):
    def __init__(self, base_dir: str) -> None:
        self.folder = base_dir

    def write(self, context: IOContext, data: Any) -> None:
        with open(Path(self.folder) / context.asset.name, "wb") as f:
            f.write(pickle.dumps(data))
        logger.info(f"Asset {context.asset.name} written to {self.folder}/{context.asset.name}")

    def read(self, context: IOContext) -> Any:
        with open(Path(self.folder) / context.asset.name, "rb") as f:
            data = pickle.loads(f.read())
        logger.info(f"Asset {context.asset.name} read from {self.folder}/{context.asset.name}")
        return data
