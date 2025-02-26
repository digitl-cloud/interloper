import logging

import pandas as pd
from sqlalchemy import Engine, MetaData, create_engine, inspect, text

from interloper.core.io import DatabaseIO, IOContext, IOHandler
from interloper.core.partitioning import Partition
from interloper.core.schema import TTableSchema
from interloper.pandas.reconciler import DataFrameReconciler
from interloper.pandas.sanitizer import DataFrameSanitizer

logger = logging.getLogger(__name__)


class SQLAlchemyDataframeHandler(IOHandler):
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        super().__init__(
            type=pd.DataFrame,
            sanitizer=DataFrameSanitizer(),
            reconciler=DataFrameReconciler(),
        )

    def write(self, context: IOContext, data: pd.DataFrame) -> None:
        if data.empty:
            logger.warning(f"Dataframe from asset {context.asset.name} is empty, nothing to write to Postgres")
            return

        data.to_sql(name=context.asset.name, con=self.engine, if_exists="append", index=False)
        size = data.memory_usage(index=False).sum()
        logger.info(f"Asset {context.asset.name} written to Postgres ({size} bytes)")

    def read(self, context: IOContext) -> pd.DataFrame:
        query = f"SELECT * FROM {context.asset.name};"
        data = pd.read_sql_query(query, self.engine)
        size = data.memory_usage(index=False).sum()
        logger.info(f"Asset {context.asset.name} read from Postgres ({size} bytes)")
        return data


class SQLAlchemyIO(DatabaseIO):
    def __init__(self, engine: Engine, handler: IOHandler) -> None:
        super().__init__(handler)
        self.engine = engine
        self.metadata = MetaData()
        self.metadata.reflect(self.engine)
        self.inspector = inspect(self.engine)

    def table_exists(self, table_name: str) -> bool:
        return table_name in self.inspector.get_table_names()

    def fetch_table_schema(self, table_name: str) -> dict[str, str]:
        table = self.metadata.tables[table_name]
        return {column.name: str(column.type.as_generic()) for column in table.columns}

    def create_table(self, table_name: str, schema: TTableSchema) -> None:
        with self.engine.connect() as connection:
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema.to_sql()});"
            connection.execute(text(query))
            connection.commit()
            logger.info(f"Table {table_name} created ({self.engine.url})")

        self.metadata.reflect(self.engine)

    def delete_partition(self, table_name: str, column: str, partition: Partition) -> None:
        with self.engine.connect() as connection:
            # TODO: use parameterized queries
            query = f"DELETE FROM {table_name} WHERE {column} = '{partition.value}';"
            connection.execute(text(query))
            connection.commit()
            logger.info(f"Partition {partition} deleted from table {table_name}")


class PostgresDataframeIO(SQLAlchemyIO):
    def __init__(self, database: str, user: str, password: str, host: str, port: int = 5432) -> None:
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
        handler = SQLAlchemyDataframeHandler(engine)
        super().__init__(engine, handler)


class MySQLDataframeIO(SQLAlchemyIO):
    def __init__(self, database: str, user: str, password: str, host: str, port: int = 3306) -> None:
        engine = create_engine(f"mysql://{user}:{password}@{host}:{port}/{database}")
        handler = SQLAlchemyDataframeHandler(engine)
        super().__init__(engine, handler)


class SQLiteDataframeIO(SQLAlchemyIO):
    def __init__(self, db_path: str) -> None:
        engine = create_engine(f"sqlite:///{db_path}")
        handler = SQLAlchemyDataframeHandler(engine)
        super().__init__(engine, handler)
