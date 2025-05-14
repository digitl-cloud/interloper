import datetime as dt
import logging

import interloper as itlp
from interloper_assets import adup
from interloper_google_cloud import BigQueryIO
from interloper_sql import SQLiteIO

itlp.basic_logging(logging.DEBUG)


adup = adup(
    io={
        "sqlite": SQLiteIO(db_path="data/sqlite.db"),
        "bigquery": BigQueryIO(project="dc-int-connectors-prd", location="eu"),
    },
    default_io_key="bigquery",
)

# data = adup.ads.run(date_window=(dt.date(2025, 1, 1), dt.date(2025, 1, 2)))

pipeline = itlp.Pipeline(adup)
pipeline.materialize(partition=itlp.TimePartition(dt.date(2025, 1, 3)))
# pipeline.backfill(
#     partitions=itlp.TimePartitionWindow(
#         start=dt.date(2025, 1, 1),
#         end=dt.date(2025, 1, 2),
#     )
# )
