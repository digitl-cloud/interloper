import logging
from typing import TypeVar

import duckdb
import interloper as itlp
import pandas as pd
from interloper_pandas import DataFrameReconciler

logger = logging.getLogger(__name__)
T = TypeVar("T")


class DuckDBClient(itlp.DatabaseClient):
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.connection = duckdb.connect(db_path)

    def table_exists(self, table_name: str, dataset: str | None = None) -> bool:
        query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}';"
        result = self.connection.execute(query).fetchone()
        return True if result and result[0] > 0 else False

    def table_schema(self, table_name: str, dataset: str | None = None) -> dict[str, str]:
        query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';"
        return dict(self.connection.execute(query).fetchall())

    def create_table(
        self,
        table_name: str,
        schema: type[itlp.TableSchema],
        dataset: str | None = None,
        partition_strategy: itlp.PartitionStrategy | None = None,
    ) -> None:
        query = f"CREATE TABLE {table_name} ({schema.to_sql()});"
        self.connection.execute(query)
        logger.info(f"Table {table_name} created in DuckDB")

    def get_select_partition_statement(
        self,
        table_name: str,
        column: str,
        partition: itlp.Partition | itlp.PartitionRange,
        dataset: str | None = None,
    ) -> str:
        if isinstance(partition, itlp.PartitionRange):
            # TODO: to be removed: support any PartitionRange
            assert isinstance(partition, itlp.TimePartitionRange)
            return f"SELECT * FROM {table_name} WHERE {column} BETWEEN '{partition.start}' AND '{partition.end}';"
        else:
            return f"SELECT * FROM {table_name} WHERE {column} = '{partition.value}';"

    def delete_partition(
        self,
        table_name: str,
        column: str,
        partition: itlp.Partition | itlp.PartitionRange,
        dataset: str | None = None,
    ) -> None:
        if isinstance(partition, itlp.PartitionRange):
            # TODO: to be removed: support any PartitionRange
            assert isinstance(partition, itlp.TimePartitionRange)
            query = f"DELETE FROM {table_name} WHERE {column} BETWEEN '{partition.start}' AND '{partition.end}';"
        else:
            query = f"DELETE FROM {table_name} WHERE {column} = '{partition.value}';"
        self.connection.execute(query)
        logger.info(f"Partition {partition} deleted from table {table_name} in DuckDB")


class DuckDBDataframeHandler(itlp.IOHandler[pd.DataFrame]):
    def __init__(self, client: DuckDBClient) -> None:
        super().__init__(
            type=pd.DataFrame,
            reconciler=DataFrameReconciler(),
        )
        self.client = client

    def write(self, context: itlp.IOContext, data: pd.DataFrame) -> None:
        if data.empty:
            logger.warning(f"Dataframe from asset {context.asset.name} is empty, not writing to DuckDB")
            return

        self.client.connection.execute(f"INSERT INTO {context.asset.name} BY NAME SELECT * FROM data")
        size = data.memory_usage(index=False).sum()
        logger.info(f"Asset {context.asset.name} written to DuckDB at ({size} bytes)")

    def read(self, context: itlp.IOContext) -> pd.DataFrame:
        if context.partition:
            assert context.asset.partition_strategy
            query = self.client.get_select_partition_statement(
                context.asset.name, context.asset.partition_strategy.column, context.partition
            )
        else:
            query = f"SELECT * FROM {context.asset.name};"

        data = self.client.connection.execute(query).fetchdf()
        size = data.memory_usage(index=False).sum()
        logger.info(f"Asset {context.asset.name} read from DuckDB ({size} bytes)")
        return data


class DuckDBDataframeIO(itlp.DatabaseIO[pd.DataFrame]):
    def __init__(self, db_path: str) -> None:
        client = DuckDBClient(db_path)
        handler = DuckDBDataframeHandler(client)
        super().__init__(handler, client)
