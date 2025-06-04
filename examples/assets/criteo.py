import datetime as dt
import logging

import interloper as itlp
from interloper_assets import criteo
from interloper_sql import SQLiteIO

itlp.basic_logging(logging.INFO)

criteo = criteo(
    io={"sqlite": SQLiteIO(db_path="data/sqlite.db")},
    default_assets_args={"advertiser_id": "1176"},
)

itlp.DAG(criteo.ads).materialize(partition=itlp.TimePartition(dt.date(2025, 1, 1)))
