import logging

import interloper as itlp
import pandas as pd
from interloper_pandas import DataFrameReconciler
from sqlalchemy import MetaData, create_engine, inspect, text

logger = logging.getLogger(__name__)


class SQLAlchemyClient(itlp.DatabaseClient):
    def __init__(self, url: str) -> None:
        self.engine = create_engine(url)
        self.metadata = MetaData()
        self.metadata.reflect(self.engine)
        self.inspector = inspect(self.engine)

    def table_exists(self, table_name: str) -> bool:
        return table_name in self.inspector.get_table_names()

    def fetch_table_schema(self, table_name: str) -> dict[str, str]:
        table = self.metadata.tables[table_name]
        return {column.name: str(column.type.as_generic()) for column in table.columns}

    def create_table(self, table_name: str, schema: type[itlp.TableSchema]) -> None:
        with self.engine.connect() as connection:
            query = f"CREATE TABLE {table_name} ({schema.to_sql()});"
            connection.execute(text(query))
            connection.commit()
            logger.info(f"Table {table_name} created ({self.engine.url})")

        self.metadata.reflect(self.engine)

    def get_select_partition_statement(
        self, table_name: str, column: str, partition: itlp.Partition | itlp.PartitionRange
    ) -> str:
        if isinstance(partition, itlp.PartitionRange):
            # TODO: to be removed: support any PartitionRange
            assert isinstance(partition, itlp.TimePartitionRange)
            return f"SELECT * FROM {table_name} WHERE {column} BETWEEN '{partition.start}' AND '{partition.end}';"
        else:
            return f"SELECT * FROM {table_name} WHERE {column} = '{partition.value}';"

    def delete_partition(self, table_name: str, column: str, partition: itlp.Partition | itlp.PartitionRange) -> None:
        with self.engine.connect() as connection:
            if isinstance(partition, itlp.PartitionRange):
                # TODO: to be removed: support any PartitionRange
                assert isinstance(partition, itlp.TimePartitionRange)
                query = text(f"DELETE FROM {table_name} WHERE {column} BETWEEN :start AND :end")
                connection.execute(query, {"start": partition.start, "end": partition.end})
            else:
                query = text(f"DELETE FROM {table_name} WHERE {column} = :value")
                connection.execute(query, {"value": partition.value})
            connection.commit()
            logger.info(f"Partition {partition} deleted from table {table_name}")


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

        data.to_sql(name=context.asset.name, con=self.client.engine, if_exists="append", index=False)
        size = data.memory_usage(index=False).sum()
        logger.info(f"Asset {context.asset.name} written to Postgres ({size} bytes)")

    def read(self, context: itlp.IOContext) -> pd.DataFrame:
        if context.partition:
            assert context.asset.partition_strategy
            query = self.client.get_select_partition_statement(
                context.asset.name, context.asset.partition_strategy.column, context.partition
            )
        else:
            query = f"SELECT * FROM {context.asset.name};"

        data = pd.read_sql_query(query, self.client.engine)
        size = data.memory_usage(index=False).sum()
        logger.info(f"Asset {context.asset.name} read from Postgres ({size} bytes)")
        return data


class SQLAlchemyIO(itlp.DatabaseIO):
    def __init__(self, client: SQLAlchemyClient, handler: itlp.IOHandler) -> None:
        super().__init__(client, handler)

    @classmethod
    def from_url(cls, url: str) -> "SQLAlchemyIO":
        client = SQLAlchemyClient(url)
        handler = SQLAlchemyDataframeHandler(client)
        return cls(client, handler)


class PostgresDataframeIO(SQLAlchemyIO):
    def __init__(self, database: str, user: str, password: str, host: str, port: int = 5432) -> None:
        client = SQLAlchemyClient(f"postgresql://{user}:{password}@{host}:{port}/{database}")
        handler = SQLAlchemyDataframeHandler(client)
        super().__init__(client, handler)


class MySQLDataframeIO(SQLAlchemyIO):
    def __init__(self, database: str, user: str, password: str, host: str, port: int = 3306) -> None:
        client = SQLAlchemyClient(f"mysql://{user}:{password}@{host}:{port}/{database}")
        handler = SQLAlchemyDataframeHandler(client)
        super().__init__(client, handler)


class SQLiteDataframeIO(SQLAlchemyIO):
    def __init__(self, db_path: str) -> None:
        client = SQLAlchemyClient(f"sqlite:///{db_path}")
        handler = SQLAlchemyDataframeHandler(client)
        super().__init__(client, handler)
