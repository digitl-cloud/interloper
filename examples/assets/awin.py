import datetime as dt
import logging

import interloper as itlp
from interloper_assets import awin
from interloper_duckdb import DuckDBDataframeIO
from interloper_sql import SQLiteDataframeIO

itlp.basic_logging(logging.DEBUG)


awin.io = {
    "file": itlp.FileIO("./data"),
    "duckdb": DuckDBDataframeIO("data/duck.db"),
    "sqlite": SQLiteDataframeIO("data/sqlite.db"),
}
awin.dataset = "xxx"
awin.default_io_key = "duckdb"
awin.advertiser_by_publisher.bind(advertiser_id="10990")

# data = awin.advertiser_by_publisher.run(date=dt.date(2025, 1, 1))

pipeline = itlp.Pipeline(awin.advertiser_by_publisher)
pipeline.materialize(partition=itlp.TimePartition(dt.date(2025, 1, 3)))
# pipeline.backfill(
#     partitions=itlp.TimePartitionRange(
#         start=dt.date(2025, 1, 1),
#         end=dt.date(2025, 1, 2),
#     )
# )
