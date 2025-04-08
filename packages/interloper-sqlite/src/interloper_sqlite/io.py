import datetime as dt
import logging
import sqlite3
from typing import TypeVar

import interloper as itlp
import pandas as pd
from interloper_pandas import DataFrameReconciler

logger = logging.getLogger(__name__)
T = TypeVar("T")

PYTHON_TO_SQL_TYPE = {
    int: "INTEGER",
    float: "FLOAT",
    str: "VARCHAR",
    bool: "BOOLEAN",
    dt.date: "DATE",
    dt.datetime: "DATETIME",
}


class SQLiteClient(itlp.DatabaseClient):
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path)

    def table_exists(self, table_name: str, dataset: str | None = None) -> bool:
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
        return bool(self.connection.execute(query).fetchone())

    def table_schema(self, table_name: str, dataset: str | None = None) -> dict[str, str]:
        query = f"PRAGMA table_info({table_name});"
        result = self.connection.execute(query).fetchall()
        return {row[1]: row[2] for row in result}

    def create_table(
        self,
        table_name: str,
        schema: type[itlp.AssetSchema],
        dataset: str | None = None,
        partitioning: itlp.PartitionConfig | None = None,
    ) -> None:
        query = f"CREATE TABLE {table_name} ({schema.to_sql(PYTHON_TO_SQL_TYPE)});"
        self.connection.execute(query)
        logger.info(f"Table {table_name} created in SQLite at {self.db_path}")

    def get_select_partition_statement(
        self, table_name: str, column: str, partition: itlp.Partition | itlp.PartitionRange, dataset: str | None = None
    ) -> str:
        if isinstance(partition, itlp.PartitionRange):
            # TODO: to be removed: support any PartitionRange
            assert isinstance(partition, itlp.TimePartitionRange)
            return f"SELECT * FROM {table_name} WHERE {column} BETWEEN '{partition.start}' AND '{partition.end}';"
        else:
            return f"SELECT * FROM {table_name} WHERE {column} = '{partition.value}';"

    def delete_partition(
        self, table_name: str, column: str, partition: itlp.Partition, dataset: str | None = None
    ) -> None:
        query = f"DELETE FROM {table_name} WHERE {column} = '{partition.value}';"
        self.connection.execute(query)
        logger.info(f"Partition {partition} deleted from table {table_name} in SQLite")


class SQLiteDataframeHandler(itlp.IOHandler[pd.DataFrame]):
    def __init__(self, client: SQLiteClient) -> None:
        super().__init__(
            type=pd.DataFrame,
            reconciler=DataFrameReconciler(),
        )
        self.client = client

    def write(self, context: itlp.IOContext, data: pd.DataFrame) -> None:
        if data.empty:
            logger.warning(f"Dataframe from asset {context.asset.name} is empty, not writing to SQLite")
            return

        data = data.astype(str)
        with self.client.connection as conn:
            conn.executemany(
                f"INSERT INTO {context.asset.name} ({', '.join(data.columns)}) "
                f"VALUES ({', '.join(['?'] * len(data.columns))})",
                data.itertuples(index=False, name=None),
            )

        size = data.memory_usage(index=False).sum()
        logger.info(f"Asset {context.asset.name} written to SQLite ({size} bytes)")

    def read(self, context: itlp.IOContext) -> pd.DataFrame:
        if context.partition:
            assert context.asset.partitioning
            query = self.client.get_select_partition_statement(
                context.asset.name, context.asset.partitioning.column, context.partition
            )
        else:
            query = f"SELECT * FROM {context.asset.name};"

        data = pd.read_sql_query(query, self.client.connection)
        size = data.memory_usage(index=False).sum()
        logger.info(f"Asset {context.asset.name} read from SQLite ({size} bytes)")
        return data


class SQLiteIO(itlp.DatabaseIO):
    def __init__(self, db_path: str) -> None:
        client = SQLiteClient(db_path)
        super().__init__(client, handlers=[SQLiteDataframeHandler(client)])
