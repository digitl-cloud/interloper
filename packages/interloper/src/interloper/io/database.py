import logging
from abc import ABC, abstractmethod
from typing import Any

from interloper.execution.strategy import MaterializationStrategy
from interloper.io.base import IOContext, IOHandler, TypedIO
from interloper.partitioning.config import PartitionConfig
from interloper.partitioning.partition import Partition
from interloper.partitioning.range import PartitionRange
from interloper.schema import AssetSchema

logger = logging.getLogger(__name__)


class DatabaseClient(ABC):
    @abstractmethod
    def table_exists(self, table_name: str, dataset: str | None = None) -> bool: ...

    @abstractmethod
    def table_schema(self, table_name: str, dataset: str | None = None) -> dict[str, str]: ...

    @abstractmethod
    def create_table(
        self,
        table_name: str,
        schema: type[AssetSchema],
        dataset: str | None = None,
        partitioning: PartitionConfig | None = None,
    ) -> None: ...

    @abstractmethod
    def get_select_partition_statement(
        self,
        table_name: str,
        column: str,
        partition: Partition | PartitionRange,
        dataset: str | None = None,
    ) -> str: ...

    @abstractmethod
    def delete_partition(
        self,
        table_name: str,
        column: str,
        partition: Partition | PartitionRange,
        dataset: str | None = None,
    ) -> None: ...


class DatabaseIO(TypedIO):
    def __init__(self, client: DatabaseClient, handlers: list[IOHandler]) -> None:
        super().__init__(handlers)
        self.client = client

    def write(self, context: IOContext, data: Any) -> None:
        handler = self.get_handler(type(data))
        handler.verify_type(data)

        if not context.asset.schema:
            raise RuntimeError(
                f"Schema is required for asset {context.asset.name} when using {self.__class__.__name__}. "
                "Either provide a schema with the asset definition (`@asset(schema=...)`) "
                "and/or use a normalizer to infer the schema automatically from the data (`@asset(normalizer=...)`)."
            )

        if not self.client.table_exists(context.asset.name, context.asset.dataset):
            self.client.create_table(
                context.asset.name, context.asset.schema, context.asset.dataset, context.asset.partitioning
            )

        if context.partition:
            assert context.asset.partitioning
            self.client.delete_partition(
                context.asset.name, context.asset.partitioning.column, context.partition, context.asset.dataset
            )

        if context.asset.materialization_strategy == MaterializationStrategy.FLEXIBLE:
            if not handler.reconciler:
                logger.warning(
                    f"No reconciler found for IO {self.__class__.__name__} with handler {handler.__class__.__name__} "
                    f"when materializing asset {context.asset.name}. Skipping schema reconciliation for flexible "
                    f"materialization strategy."
                )
            else:
                table_schema = self.client.table_schema(context.asset.name, context.asset.dataset)
                data = handler.reconciler.reconcile(data, table_schema)

        handler.write(context, data)

    def read(self, context: IOContext) -> Any:
        handler = self.get_handler(type(context.asset.schema))
        data = handler.read(context)
        handler.verify_type(data)
        return data
