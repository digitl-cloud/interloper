import datetime as dt
import logging

from interloper.assets.adservice.asset import adservice
from interloper.core.io import FileIO
from interloper.core.partitioning import TimePartition, TimePartitionRange
from interloper.core.pipeline import Pipeline
from interloper.core.utils import basic_logging

from interloper.duckdb.io import DuckDBDataframeIO

# from interloper.sqlite.io import SQLiteDataframeIO
from interloper.sql.io import PostgresDataframeIO, SQLiteDataframeIO

basic_logging(logging.INFO)


adservice.io = {
    # "file": FileIO("data"),
    "duckdb": DuckDBDataframeIO("data/duck.db"),
    # "sqlite": SQLiteDataframeIO("data/sqlite.db"),
    # "postgres": PostgresDataframeIO("interloper", "g", "", "localhost"),
}
adservice.default_io_key = "duckdb"

pipeline = Pipeline(adservice)

# pipeline.materialize(partition=TimePartition(dt.date(2024, 1, 3)))

pipeline.backfill(
    partitions=TimePartitionRange(
        start=dt.date(2024, 1, 1),
        end=dt.date(2024, 1, 3),
    )
)
