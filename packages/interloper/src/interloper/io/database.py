from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from interloper.io.base import IOContext, IOHandler, TypedIO
from interloper.partitioning.partitions import Partition
from interloper.partitioning.ranges import PartitionRange
from interloper.schema import TableSchema

T = TypeVar("T")


class DatabaseClient(ABC):
    @abstractmethod
    def table_exists(self, table_name: str) -> bool: ...

    @abstractmethod
    def fetch_table_schema(self, table_name: str) -> dict[str, str]: ...

    @abstractmethod
    def create_table(self, table_name: str, schema: type[TableSchema]) -> None: ...

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

        if not context.asset.schema:
            raise RuntimeError(
                f"Schema is required for asset {context.asset.name} when using Database IO {self.__class__.__name__}. "
                "Either provide schema with the asset definition and/or use a normalizer to infer it from the data."
            )

        if not self.client.table_exists(context.asset.name):
            self.client.create_table(context.asset.name, context.asset.schema)

        if context.partition:
            assert context.asset.partition_strategy
            self.client.delete_partition(context.asset.name, context.asset.partition_strategy.column, context.partition)

        table_schema = self.client.fetch_table_schema(context.asset.name)
        print(table_schema)
        data = self.handler.reconciler.reconcile(data, table_schema)

        self.handler.write(context, data)

    def read(self, context: IOContext) -> T:
        data = self.handler.read(context)
        self._check_asset_type(data)
        return data
