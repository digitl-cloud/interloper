import datetime as dt
import logging

import interloper as itlp
from interloper_assets import awin
from interloper_google_cloud import BigQueryIO

# from interloper_duckdb import DuckDBIO
# from interloper_sql import PostgresIO, SQLiteIO

itlp.basic_logging(logging.INFO)


awin = awin(
    io={
        # "file": itlp.FileIO("./data"),
        # "duckdb": DuckDBIO("data/duck.db"),
        # "sqlite": SQLiteIO("data/sqlite.db"),
        # "postgres": PostgresIO(database="interloper", user="g", password="", host="localhost", port=5432),
        "bigquery": BigQueryIO(project="dc-int-connectors-prd", location="eu"),
    },
    default_io_key="bigquery",
    default_assets_args={"advertiser_id": "10990"},
    # materialization_strategy=itlp.MaterializationStrategy.STRICT,
)

data = awin.advertiser_by_publisher.run(date=dt.date(2025, 1, 1))
