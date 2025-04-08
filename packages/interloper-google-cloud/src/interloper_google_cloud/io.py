import datetime as dt
import logging
import sys
from typing import Any

import interloper as itlp
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from interloper_pandas import DataFrameReconciler

logger = logging.getLogger(__name__)


DEFAULT_DATASET = "public"
PYTHON_TO_SQL_TYPE = {
    int: "INTEGER",
    float: "FLOAT",
    str: "STRING",
    bool: "BOOLEAN",
    dt.date: "DATE",
    dt.datetime: "DATETIME",
}


class BigQueryClient(itlp.DatabaseClient):
    def __init__(self, project: str, location: str, default_dataset: str = DEFAULT_DATASET):
        self.default_dataset = default_dataset
        self.client = bigquery.Client(project=project, location=location)

    def table_exists(self, table_name: str, dataset: str | None = None) -> bool:
        try:
            self.client.get_table(f"{dataset or self.default_dataset}.{table_name}")
            return True
        except NotFound:
            return False

    def table_schema(self, table_name: str, dataset: str | None = None) -> dict[str, str]:
        table = self.client.get_table(f"{dataset or self.default_dataset}.{table_name}")
        return {str(field.name): str(field.field_type) for field in table.schema}

    def create_table(
        self,
        table_name: str,
        schema: type[itlp.AssetSchema],
        dataset: str | None = None,
        partitioning: itlp.PartitionConfig | None = None,
    ) -> None:
        self.client.create_dataset(dataset or self.default_dataset, exists_ok=True)

        table = bigquery.Table(f"{self.client.project}.{dataset or self.default_dataset}.{table_name}")
        table.schema = [
            bigquery.SchemaField(name=name, field_type=type)
            for name, type in schema.to_tuple(format="sql", types=PYTHON_TO_SQL_TYPE)
        ]
        if partitioning:
            assert isinstance(partitioning, itlp.TimePartitionConfig)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partitioning.column,
            )

        self.client.create_table(table)

    def get_select_partition_statement(
        self,
        table_name: str,
        column: str,
        partition: itlp.Partition | itlp.PartitionRange,
        dataset: str | None = None,
    ) -> str:
        table_id = f"{dataset or self.default_dataset}.{table_name}"
        if isinstance(partition, itlp.PartitionRange):
            assert isinstance(partition, itlp.TimePartitionRange)
            return f"SELECT * FROM {table_id} WHERE {column} BETWEEN '{partition.start}' AND '{partition.end}'"
        else:
            return f"SELECT * FROM {table_id} WHERE {column} = '{partition.value}'"

    def delete_partition(
        self,
        table_name: str,
        column: str,
        partition: itlp.Partition | itlp.PartitionRange,
        dataset: str | None = None,
    ) -> None:
        table_id = f"{dataset or self.default_dataset}.{table_name}"
        if isinstance(partition, itlp.PartitionRange):
            # TODO: to be removed: support any PartitionRange
            assert isinstance(partition, itlp.TimePartitionRange)
            self.client.query(
                f"DELETE FROM {table_id} WHERE {column} BETWEEN '{partition.start}' AND '{partition.end}'"
            ).result()
        else:
            self.client.query(f"DELETE FROM {table_id} WHERE {column} = '{partition.value}'").result()
        logger.info(f"Partition {partition} deleted from table {table_id}")


class BigQueryJSONHandler(itlp.IOHandler[list[dict[str, Any]]]):
    def __init__(self, client: BigQueryClient):
        super().__init__(type=list[dict[str, Any]])
        self.client = client

    def write(self, context: itlp.IOContext, data: list[dict[str, Any]]) -> None:
        if not data:
            logger.warning(f"Data from asset {context.asset.name} is empty, not writing to BigQuery")
            return

        table_id = f"{context.asset.dataset or self.client.default_dataset}.{context.asset.name}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        self.client.client.load_table_from_json(
            json_rows=data,
            destination=table_id,
            timeout=None,
            job_config=job_config,
        ).result()

        size = sys.getsizeof(data)
        logger.info(f"Asset {context.asset.name} written to BigQuery ({size} bytes)")

    def read(self, context: itlp.IOContext) -> list[dict[str, Any]]:
        if context.partition:
            assert context.asset.partitioning
            query = self.client.get_select_partition_statement(
                context.asset.name, context.asset.partitioning.column, context.partition, context.asset.dataset
            )
        else:
            table_id = f"{context.asset.dataset or self.client.default_dataset}.{context.asset.name}"
            query = f"SELECT * FROM {table_id}"

        return [dict(row) for row in self.client.client.query(query)]


class BigQueryDataframeHandler(itlp.IOHandler[pd.DataFrame]):
    def __init__(self, client: BigQueryClient):
        super().__init__(type=pd.DataFrame, reconciler=DataFrameReconciler())
        self.client = client

    def write(self, context: itlp.IOContext, data: pd.DataFrame) -> None:
        if data.empty:
            logger.warning(f"Dataframe from asset {context.asset.name} is empty, not writing to DuckDB")
            return

        table_id = f"{context.asset.dataset or self.client.default_dataset}.{context.asset.name}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        self.client.client.load_table_from_dataframe(
            dataframe=data,
            destination=table_id,
            timeout=None,
            job_config=job_config,
        ).result()

        size = data.memory_usage(index=False).sum()
        logger.info(f"Asset {context.asset.name} written to BigQuery ({size} bytes)")

    def read(self, context: itlp.IOContext) -> pd.DataFrame:
        if context.partition:
            assert context.asset.partitioning
            query = self.client.get_select_partition_statement(
                context.asset.name, context.asset.partitioning.column, context.partition, context.asset.dataset
            )
        else:
            table_id = f"{context.asset.dataset or self.client.default_dataset}.{context.asset.name}"
            query = f"SELECT * FROM {table_id}"

        df = self.client.client.query(query).to_dataframe()
        logger.info(f"Asset {context.asset.name} read from BigQuery ({df.memory_usage(index=False).sum()} bytes)")
        return df


class BigQueryIO(itlp.DatabaseIO):
    def __init__(self, project: str, location: str, default_dataset: str = DEFAULT_DATASET):
        client = BigQueryClient(project, location, default_dataset)
        super().__init__(
            client,
            handlers=[
                BigQueryJSONHandler(client),
                BigQueryDataframeHandler(client),
            ],
        )
