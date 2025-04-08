import logging
import sys
from typing import Any

import interloper as itlp
import pandas as pd
from interloper_pandas import DataFrameReconciler
from sqlalchemy import MetaData, Table, create_engine, inspect, text

logger = logging.getLogger(__name__)


class SQLAlchemyClient(itlp.DatabaseClient):
    def __init__(self, url: str) -> None:
        self.engine = create_engine(url)
        self.inspector = inspect(self.engine)
        self.supports_schemas = self.engine.dialect.name not in ("sqlite",)

    def _get_table(self, table_name: str, dataset: str | None = None) -> Table:
        if dataset and self.supports_schemas:
            return Table(table_name, MetaData(schema=dataset), autoload_with=self.engine)
        return Table(table_name, MetaData(), autoload_with=self.engine)

    def table_exists(
        self,
        table_name: str,
        dataset: str | None = None,
    ) -> bool:
        if dataset and self.supports_schemas:
            return table_name in self.inspector.get_table_names(schema=dataset)
        return table_name in self.inspector.get_table_names()

    def table_schema(
        self,
        table_name: str,
        dataset: str | None = None,
    ) -> dict[str, str]:
        table = self._get_table(table_name, dataset)
        return {column.name: str(column.type.as_generic()) for column in table.columns}

    def create_table(
        self,
        table_name: str,
        schema: type[itlp.AssetSchema],
        dataset: str | None = None,
        partitioning: itlp.PartitionConfig | None = None,
    ) -> None:
        with self.engine.connect() as connection:
            # Create dataset if it doesn't exist and database supports schemas
            if dataset and self.supports_schemas:
                connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {dataset};"))
                connection.commit()

            # Create table with dataset if specified and supported
            table_dataset = f"{dataset}." if dataset and self.supports_schemas else ""
            query = f"CREATE TABLE {table_dataset}{table_name} ({schema.to_sql()});"
            connection.execute(text(query))
            connection.commit()
            logger.info(f"Table {table_dataset}{table_name} created ({self.engine.url})")

    def get_select_partition_statement(
        self,
        table_name: str,
        column: str,
        partition: itlp.Partition | itlp.PartitionRange,
        dataset: str | None = None,
    ) -> str:
        table_dataset = f"{dataset}." if dataset and self.supports_schemas else ""
        if isinstance(partition, itlp.PartitionRange):
            # TODO: to be removed: support any PartitionRange
            assert isinstance(partition, itlp.TimePartitionRange)
            return (
                f"SELECT * FROM {table_dataset}{table_name} "
                f"WHERE {column} BETWEEN '{partition.start}' AND '{partition.end}';"
            )
        else:
            return f"SELECT * FROM {table_dataset}{table_name} WHERE {column} = '{partition.value}';"

    def delete_partition(
        self,
        table_name: str,
        column: str,
        partition: itlp.Partition | itlp.PartitionRange,
        dataset: str | None = None,
    ) -> None:
        with self.engine.connect() as connection:
            table_dataset = f"{dataset}." if dataset and self.supports_schemas else ""
            if isinstance(partition, itlp.PartitionRange):
                # TODO: to be removed: support any PartitionRange
                assert isinstance(partition, itlp.TimePartitionRange)
                query = text(f"DELETE FROM {table_dataset}{table_name} WHERE {column} BETWEEN :start AND :end")
                connection.execute(query, {"start": partition.start, "end": partition.end})
            else:
                query = text(f"DELETE FROM {table_dataset}{table_name} WHERE {column} = :value")
                connection.execute(query, {"value": partition.value})
            connection.commit()
            logger.info(f"Partition {partition} deleted from table {table_dataset}{table_name}")


class SQLAlchemyDataframeHandler(itlp.IOHandler[pd.DataFrame]):
    def __init__(self, client: SQLAlchemyClient) -> None:
        super().__init__(
            type=pd.DataFrame,
            reconciler=DataFrameReconciler(),
        )
        self.client = client

    def write(self, context: itlp.IOContext, data: pd.DataFrame) -> None:
        if data.empty:
            logger.warning(f"Dataframe from asset {context.asset.name} is empty, nothing to write to Postgres")
            return

        # Write data to table
        data.to_sql(
            name=context.asset.name,
            con=self.client.engine,
            schema=context.asset.dataset if self.client.supports_schemas else None,
            if_exists="append",
            index=False,
        )
        size = data.memory_usage(index=False).sum()
        logger.info(f"Asset {context.asset.name} written to Postgres ({size} bytes)")

    def read(self, context: itlp.IOContext) -> pd.DataFrame:
        if context.partition:
            assert context.asset.partitioning
            query = self.client.get_select_partition_statement(
                context.asset.name, context.asset.partitioning.column, context.partition, context.asset.dataset
            )
        else:
            table_dataset = f"{context.asset.dataset}." if context.asset.dataset else ""
            query = f"SELECT * FROM {table_dataset}{context.asset.name};"

        data = pd.read_sql_query(query, self.client.engine)
        size = data.memory_usage(index=False).sum()
        logger.info(f"Asset {context.asset.name} read from Postgres ({size} bytes)")
        return data


class SQLAlchemyJSONHandler(itlp.IOHandler[list[dict[str, Any]]]):
    def __init__(self, client: SQLAlchemyClient) -> None:
        super().__init__(
            type=list[dict[str, Any]],
            reconciler=itlp.JSONReconciler(),
        )
        self.client = client

    def write(self, context: itlp.IOContext, data: list[dict[str, Any]]) -> None:
        if not data:
            logger.warning(f"Data from asset {context.asset.name} is empty, not writing to Postgres")
            return

        table = self.client._get_table(context.asset.name, context.asset.dataset)
        with self.client.engine.connect() as connection:
            connection.execute(table.insert(), data)
            connection.commit()

        size = sys.getsizeof(data)
        logger.info(f"Asset {context.asset.name} written to Postgres ({size} bytes)")

    def read(self, context: itlp.IOContext) -> list[dict[str, Any]]:
        table = self.client._get_table(context.asset.name, context.asset.dataset)
        with self.client.engine.connect() as connection:
            result = connection.execute(table.select())
            size = sys.getsizeof(result)
            logger.info(f"Asset {context.asset.name} read from Postgres ({size} bytes)")
            return [dict(row) for row in result]


class SQLAlchemyIO(itlp.DatabaseIO):
    def __init__(self, client: SQLAlchemyClient, handlers: list[itlp.IOHandler]) -> None:
        super().__init__(client, handlers)


class PostgresIO(SQLAlchemyIO):
    def __init__(self, database: str, user: str, password: str, host: str, port: int = 5432) -> None:
        client = SQLAlchemyClient(f"postgresql://{user}:{password}@{host}:{port}/{database}")
        super().__init__(
            client,
            handlers=[
                SQLAlchemyDataframeHandler(client),
                SQLAlchemyJSONHandler(client),
            ],
        )


class MySQLIO(SQLAlchemyIO):
    def __init__(self, database: str, user: str, password: str, host: str, port: int = 3306) -> None:
        client = SQLAlchemyClient(f"mysql://{user}:{password}@{host}:{port}/{database}")
        super().__init__(
            client,
            handlers=[
                SQLAlchemyDataframeHandler(client),
                SQLAlchemyJSONHandler(client),
            ],
        )


class SQLiteIO(SQLAlchemyIO):
    def __init__(self, db_path: str) -> None:
        client = SQLAlchemyClient(f"sqlite:///{db_path}")
        super().__init__(
            client,
            handlers=[
                SQLAlchemyDataframeHandler(client),
                SQLAlchemyJSONHandler(client),
            ],
        )
