import logging
import sqlite3
from typing import TypeVar

import pandas as pd

from interloper.core.io import DatabaseIO, IOContext, IOHandler
from interloper.core.partitioning import TimePartition
from interloper.core.schema import TTableSchema
from interloper.pandas.reconciler import DataFrameReconciler
from interloper.pandas.sanitizer import DataFrameSanitizer

logger = logging.getLogger(__name__)
T = TypeVar("T")


class SQLiteDataframeHandler(IOHandler):
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection
        super().__init__(
            type=pd.DataFrame,
            sanitizer=DataFrameSanitizer(),
            reconciler=DataFrameReconciler(),
        )

    def write(self, context: IOContext, data: pd.DataFrame) -> None:
        if data.empty:
            logger.warning(f"Dataframe from asset {context.asset.name} is empty, not writing to SQLite")
            return

        with self.connection as conn:
            conn.executemany(
                f"INSERT INTO {context.asset.name} ({', '.join(data.columns)}) "
                f"VALUES ({', '.join(['?'] * len(data.columns))})",
                data.itertuples(index=False, name=None),
            )

        size = data.memory_usage(index=False).sum()
        logger.info(f"Asset {context.asset.name} written to SQLite ({size} bytes)")

    def read(self, context: IOContext) -> pd.DataFrame:
        query = f"SELECT * FROM {context.asset.name};"
        data = pd.read_sql_query(query, self.connection)
        size = data.memory_usage(index=False).sum()
        logger.info(f"Asset {context.asset.name} read from SQLite ({size} bytes)")
        return data


class SQLiteDataframeIO(DatabaseIO):
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path)
        super().__init__(handler=SQLiteDataframeHandler(self.connection))

    @property
    def type(self) -> type[pd.DataFrame]:
        return pd.DataFrame

    def table_exists(self, table_name: str) -> bool:
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
        return bool(self.connection.execute(query).fetchone())

    def fetch_table_schema(self, table_name: str) -> dict[str, str]:
        query = f"PRAGMA table_info({table_name});"
        result = self.connection.execute(query).fetchall()
        return {row[1]: row[2] for row in result}

    def create_table(self, table_name: str, schema: TTableSchema) -> None:
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema.to_sql()});"
        self.connection.execute(query)
        logger.info(f"Table {table_name} created in SQLite at {self.db_path}")

    def delete_partition(self, table_name: str, column: str, partition: str) -> None:
        if not isinstance(partition, TimePartition):
            raise ValueError(f"Unsupported partition type: {type(partition)}")

        query = f"DELETE FROM {table_name} WHERE {column} = '{partition.date}';"
        self.connection.execute(query)
        logger.info(f"Partition {partition} deleted from table {table_name} in SQLite")
