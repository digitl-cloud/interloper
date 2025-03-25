import datetime as dt
import logging

import interloper as itlp
from interloper_assets import adservice
from interloper_duckdb import DuckDBDataframeIO

itlp.basic_logging(logging.DEBUG)


adservice = adservice(
    io={"duckdb": DuckDBDataframeIO("data/duck.db")},
    default_io_key="duckdb",
)

data = adservice.campaigns.run(date=dt.date(2024, 1, 1))

# pipeline = itlp.Pipeline(adservice)
# pipeline.materialize(partition=itlp.TimePartition(dt.date(2024, 1, 3)))
# pipeline.backfill(
#     partitions=itlp.TimePartitionRange(
#         start=dt.date(2024, 1, 1),
#         end=dt.date(2024, 1, 2),
#     )
# )
