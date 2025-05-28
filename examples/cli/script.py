import datetime as dt
from time import sleep

import interloper as itlp
import pandas as pd
from interloper_pandas.normalizer import DataframeNormalizer
from interloper_sql.io import SQLiteIO


def work() -> None:
    from random import random, uniform

    sleep(uniform(0.0, 1.0))
    if random() < 0.2:
        raise Exception("Ooops")


@itlp.source(
    normalizer=DataframeNormalizer(),
    io=SQLiteIO(db_path="data/sqlite.db"),
)
def source() -> tuple[itlp.Asset, ...]:
    @itlp.asset()
    def root() -> pd.DataFrame:
        work()
        return pd.DataFrame({"val": [1, 2, 3]})

    @itlp.asset()
    def left_1(
        root: pd.DataFrame = itlp.UpstreamAsset("root"),
    ) -> pd.DataFrame:
        work()
        return pd.DataFrame(
            {
                "val": [123],
            }
        )

    @itlp.asset(partitioning=itlp.TimePartitionConfig("date", allow_window=True))
    def left_2(
        date: tuple[dt.date, dt.date] = itlp.DateWindow(),
        left_1: pd.DataFrame = itlp.UpstreamAsset("left_1"),
    ) -> pd.DataFrame:
        work()
        return pd.DataFrame(
            {
                "val": [123],
                "date": pd.Series([date[0]], dtype="datetime64[ns]"),
            }
        )

    @itlp.asset(partitioning=itlp.TimePartitionConfig("date", allow_window=True))
    def right_1(
        date: tuple[dt.date, dt.date] = itlp.DateWindow(),
        root: pd.DataFrame = itlp.UpstreamAsset("root"),
    ) -> pd.DataFrame:
        work()
        return pd.DataFrame(
            {
                "val": [123],
                "date": pd.Series([date[0]], dtype="datetime64[ns]"),
            }
        )

    @itlp.asset(partitioning=itlp.TimePartitionConfig("date", allow_window=False))
    def right_2(
        date: tuple[dt.date, dt.date] = itlp.DateWindow(),
        right_1: pd.DataFrame = itlp.UpstreamAsset("right_1"),
    ) -> pd.DataFrame:
        work()

        return pd.DataFrame(
            {
                "val": [123],
                "date": pd.Series([date[0]], dtype="datetime64[ns]"),
            }
        )

    return (root, left_1, left_2, right_1, right_2)


dag = itlp.DAG(source)
