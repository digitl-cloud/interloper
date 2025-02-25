import logging
from typing import TypeVar

import pandas as pd

import duckdb
from interloper.core.io import DatabaseIO, IOContext, IOHandler
from interloper.core.partitioning import Partition, TimePartition
from interloper.core.schema import TTableSchema
from interloper.pandas.reconciler import DataFrameReconciler
from interloper.pandas.sanitizer import DataFrameSanitizer

logger = logging.getLogger(__name__)
T = TypeVar("T")


class DuckDBDataframeHandler(IOHandler):
    def __init__(self, client: duckdb.DuckDBPyConnection) -> None:
        self.client = client
        super().__init__(
            type=pd.DataFrame,
            sanitizer=DataFrameSanitizer(),
            reconciler=DataFrameReconciler(),
        )

    def write(self, context: IOContext, data: pd.DataFrame) -> None:
        if data.empty:
            logger.warning(f"Dataframe from asset {context.asset.name} is empty, not writing to DuckDB")
            return

        self.client.execute(f"INSERT INTO {context.asset.name} SELECT * FROM data")
        size = data.memory_usage(index=False).sum()
        logger.info(f"Asset {context.asset.name} written to DuckDB at ({size} bytes)")

    def read(self, context: IOContext) -> pd.DataFrame:
        data = self.client.execute(f"SELECT * FROM {context.asset.name}").fetchdf()
        size = data.memory_usage(index=False).sum()
        logger.info(f"Asset {context.asset.name} read from DuckDB ({size} bytes)")
        return data


class DuckDBDataframeIO(DatabaseIO):
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.client = duckdb.connect(db_path)
        super().__init__(handler=DuckDBDataframeHandler(self.client))

    def table_exists(self, table_name: str) -> bool:
        query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}';"
        result = self.client.execute(query).fetchone()
        return True if result and result[0] > 0 else False

    def fetch_table_schema(self, table_name: str) -> dict[str, str]:
        # query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';"
        # return dict(self.client.execute(query).fetchall())
        query = f"PRAGMA table_info({table_name});"
        result = self.client.execute(query).fetchall()
        return {row[1]: row[2] for row in result}

    def create_table(self, table_name: str, schema: TTableSchema) -> None:
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema.to_sql()});"
        self.client.execute(query)
        logger.info(f"Table {table_name} created in DuckDB at {self.db_path}")

    def delete_partition(self, table_name: str, column: str, partition: Partition) -> None:
        if not isinstance(partition, TimePartition):
            raise ValueError(f"Unsupported partition type: {type(partition)}")

        query = f"DELETE FROM {table_name} WHERE {column} = '{partition.date}';"
        self.client.execute(query)
        logger.info(f"Partition {partition} deleted from table {table_name} in DuckDB")
