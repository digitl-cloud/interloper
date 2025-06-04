import datetime as dt
import logging

import interloper as itlp
from interloper_assets import adservice
from interloper_duckdb import DuckDBIO

itlp.basic_logging(logging.DEBUG)


adservice = adservice(
    io={"duckdb": DuckDBIO("data/duck.db")},
    default_io_key="duckdb",
)

data = adservice.campaigns.run(date=dt.date(2024, 1, 1))

# dag = itlp.DAG(adservice)
# dag.materialize(partition=itlp.TimePartition(dt.date(2024, 1, 3)))
# dag.backfill(
#     partitions=itlp.TimePartitionWindow(
#         start=dt.date(2024, 1, 1),
#         end=dt.date(2024, 1, 2),
#     )
# )
