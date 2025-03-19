import datetime as dt
import logging

import interloper as itlp
from interloper_assets import adup
from interloper_duckdb import DuckDBDataframeIO

itlp.basic_logging(logging.DEBUG)


adup.io = {"duckdb": DuckDBDataframeIO("data/duck.db")}
adup.default_io_key = "duckdb"

data = adup.ads.run(
    advertiser_id="10990",
    date_window=(dt.date(2025, 1, 1), dt.date(2025, 1, 2)),
)

# pipeline = itlp.Pipeline(adup)
# pipeline.materialize(partition=itlp.TimePartition(dt.date(2025, 1, 3)))
# pipeline.backfill(
#     partitions=itlp.TimePartitionRange(
#         start=dt.date(2025, 1, 1),
#         end=dt.date(2025, 1, 2),
#     )
# )
