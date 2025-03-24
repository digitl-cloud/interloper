import datetime as dt
import logging

import interloper as itlp
from interloper_assets import awin
from interloper_duckdb import DuckDBDataframeIO
from interloper_sql import SQLiteDataframeIO

itlp.basic_logging(logging.DEBUG)


awin_base = awin(
    io={
        "file": itlp.FileIO("./data"),
        "duckdb": DuckDBDataframeIO("data/duck.db"),
        "sqlite": SQLiteDataframeIO("data/sqlite.db"),
    },
    default_io_key="duckdb",
)

awin_10990 = awin_base(default_args={"advertiser_id": "10990"})
# data = awin_10990.advertiser_by_publisher.run(date=dt.date(2025, 1, 1))

pipeline = itlp.Pipeline(awin_10990)
pipeline.materialize(partition=itlp.TimePartition(dt.date(2025, 1, 3)))
# pipeline.backfill(
#     partitions=itlp.TimePartitionRange(
#         start=dt.date(2025, 1, 1),
#         end=dt.date(2025, 1, 2),
#     )
# )
