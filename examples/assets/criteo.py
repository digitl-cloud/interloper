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

pipeline = itlp.Pipeline(criteo.ads, async_events=True)
pipeline.materialize(partition=itlp.TimePartition(dt.date(2025, 1, 1)))


# @itlp.source
# def my_source() -> tuple[itlp.Asset, ...]:
#     @itlp.asset(partitioning=itlp.TimePartitionConfig("date"))
#     def my_asset_A() -> str:
#         # sleep(3.6)
#         return "A"

#     return (my_asset_A,)


# my_source.io = {"file": itlp.FileIO(base_dir="data")}


# def on_event(pipeline: itlp.Pipeline, event: itlp.Event) -> None:
#     print(f"{event.step} {event.status}")


# pipeline = itlp.Pipeline(my_source, on_event=on_event, async_events=True)
# pipeline.materialize(partition=itlp.TimePartition(dt.date(2025, 1, 1)))
