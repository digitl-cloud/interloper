import datetime as dt
import logging

from dead.assets.adservice.asset import adservice
from dead.core.io import FileIO
from dead.core.partitioning import TimePartition
from dead.core.pipeline import Pipeline
from dead.core.utils import basic_logging

from dead.duckdb.io import DuckDBDataframeIO

# from dead.sqlite.io import SQLiteDataframeIO
from dead.sql.io import PostgresDataframeIO, SQLiteDataframeIO

basic_logging(logging.INFO)


adservice.io = {
    "file": FileIO("data"),
    "duckdb": DuckDBDataframeIO("data/duck.db"),
    "sqlite": SQLiteDataframeIO("data/sqlite.db"),
    "postgres": PostgresDataframeIO("dead", "g", "", "localhost"),
}
adservice.default_io_key = "sqlite"

pipeline = Pipeline(adservice.campaigns)
pipeline.materialize(partition=TimePartition(dt.date(2024, 1, 1)))
# pipeline.backfill(dt.date(2024, 1, 1), dt.date(2024, 1, 3))
