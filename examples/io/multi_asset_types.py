import logging

import interloper as itlp
import pandas as pd
from interloper.normalizer import JSONNormalizer
from interloper_google_cloud.io import BigQueryIO
from interloper_pandas.normalizer import DataframeNormalizer
from interloper_sql.io import PostgresIO, SQLiteIO

itlp.basic_logging(logging.INFO)


@itlp.source(
    materialization_strategy=itlp.MaterializationStrategy.STRICT,
)
def my_source() -> tuple[itlp.Asset, ...]:
    @itlp.asset(
        normalizer=JSONNormalizer(),
    )
    def as_json() -> list:
        return [
            {"a": 1, "b": 2},
            {"b": 3, "c": "4"},
        ]

    @itlp.asset(
        normalizer=DataframeNormalizer(),
    )
    def as_dataframe() -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"a": 1, "b": 2},
                {"b": 3, "c": "4"},
            ]
        )

    return (as_json, as_dataframe)


test = my_source(
    io={
        "file": itlp.FileIO("./data"),
        "sqlite": SQLiteIO("data/sqlite.db"),
        "postgres": PostgresIO(database="interloper", user="g", password="", host="localhost", port=5432),
        "bigquery": BigQueryIO(project="dc-int-connectors-prd", location="eu"),
    },
    default_io_key="bigquery",
)


itlp.Pipeline(test.as_dataframe).materialize()
