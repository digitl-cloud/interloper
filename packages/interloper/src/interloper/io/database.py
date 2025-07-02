"""This module contains the database IO classes."""
import logging
from abc import ABC, abstractmethod
from typing import Any

from opentelemetry import trace

from interloper.execution.strategy import MaterializationStrategy
from interloper.io.base import IOContext, IOHandler, TypedIO
from interloper.partitioning.config import PartitionConfig
from interloper.partitioning.partition import Partition
from interloper.partitioning.window import PartitionWindow
from interloper.schema import AssetSchema

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class DatabaseClient(ABC):
    """An abstract class for database clients."""

    @abstractmethod
    def table_exists(self, table_name: str, dataset: str | None = None) -> bool:
        """Check if a table exists.

        Args:
            table_name: The name of the table.
            dataset: The dataset of the table.

        Returns:
            True if the table exists, False otherwise.
        """
        ...

    @abstractmethod
    def table_schema(self, table_name: str, dataset: str | None = None) -> dict[str, str]:
        """Get the schema of a table.

        Args:
            table_name: The name of the table.
            dataset: The dataset of the table.

        Returns:
            The schema of the table.
        """
        ...

    @abstractmethod
    def create_table(
        self,
        table_name: str,
        schema: type[AssetSchema],
        dataset: str | None = None,
        partitioning: PartitionConfig | None = None,
    ) -> None:
        """Create a table.

        Args:
            table_name: The name of the table.
            schema: The schema of the table.
            dataset: The dataset of the table.
            partitioning: The partitioning of the table.
        """
        ...

    @abstractmethod
    def get_select_partition_statement(
        self,
        table_name: str,
        column: str,
        partition: Partition | PartitionWindow,
        dataset: str | None = None,
    ) -> str:
        """Get the select statement for a partition.

        Args:
            table_name: The name of the table.
            column: The partitioning column.
            partition: The partition to select.
            dataset: The dataset of the table.

        Returns:
            The select statement for the partition.
        """
        ...

    @abstractmethod
    def delete_partition(
        self,
        table_name: str,
        column: str,
        partition: Partition | PartitionWindow,
        dataset: str | None = None,
    ) -> None:
        """Delete a partition.

        Args:
            table_name: The name of the table.
            column: The partitioning column.
            partition: The partition to delete.
            dataset: The dataset of the table.
        """
        ...


class DatabaseIO(TypedIO):
    """An IO class for databases."""

    def __init__(self, client: DatabaseClient, handlers: list[IOHandler]) -> None:
        """Initialize the DatabaseIO.

        Args:
            client: The database client to use.
            handlers: The handlers to use.
        """
        super().__init__(handlers)
        self.client = client

    @tracer.start_as_current_span("interloper.io.DatabaseIO.write")
    def write(self, context: IOContext, data: Any) -> None:
        """Write data to the database.

        Args:
            context: The IO context.
            data: The data to write.

        Raises:
            RuntimeError: If the asset has no schema.
        """
        handler = self.get_handler(context.asset.data_type)
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
                    f"<FLEXIBLE> No reconciler found for IO {self.__class__.__name__} with handler "
                    f"{handler.__class__.__name__} when materializing asset {context.asset.name}. "
                    "Skipping schema reconciliation"
                )
            else:
                table_schema = self.client.table_schema(context.asset.name, context.asset.dataset)
                data = handler.reconciler.reconcile(data, table_schema)
        elif context.asset.materialization_strategy == MaterializationStrategy.STRICT:
            logger.warning(f"<STRICT> Skipping schema reconciliation for asset {context.asset.name}")

        handler.write(context, data)

    @tracer.start_as_current_span("interloper.io.DatabaseIO.read")
    def read(self, context: IOContext) -> Any:
        """Read data from the database.

        Args:
            context: The IO context.

        Returns:
            The data that was read.
        """
        handler = self.get_handler(context.asset.data_type)
        data = handler.read(context)
        handler.verify_type(data)
        return data
