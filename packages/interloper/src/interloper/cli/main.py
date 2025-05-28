import argparse
import datetime as dt
import logging
from time import sleep

import pandas as pd
from interloper_pandas.normalizer import DataframeNormalizer
from interloper_sql.io import SQLiteIO
from rich.live import Live

import interloper as itlp
from interloper.cli.visualizer import MaterializationVisualizer
from interloper.dag.base import DAG
from interloper.events.bus import get_event_bus
from interloper.events.event import Event, EventType
from interloper.execution.execution import MultiThreadExecution
from interloper.execution.state import ExecutionStatus
from interloper.partitioning.window import TimePartitionWindow

event_bus = get_event_bus()


########################
########################


def work() -> None:
    from random import random, uniform

    sleep(uniform(0.0, 1.0))
    if random() < 0.2:
        raise Exception("Ooops")


@itlp.source(normalizer=DataframeNormalizer())
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
        # import random

        # if random.random() < 0.5:
        #     raise Exception("Failed")

        return pd.DataFrame(
            {
                "val": [123],
                "date": pd.Series([date[0]], dtype="datetime64[ns]"),
            }
        )

    return (root, left_1, left_2, right_1, right_2)


source.io = SQLiteIO(db_path="data/sqlite.db")
source2 = source(name="source2")

########################
########################


def backfill() -> None:
    dag = DAG([source])
    partitions = TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 7))
    execution = MultiThreadExecution(dag, partitions)
    visualizer = MaterializationVisualizer()
    errors = []

    with Live(refresh_per_second=10) as live:

        def on_event(event: Event) -> None:
            if event.type == EventType.ASSET_MATERIALIZATION:
                if event.status == ExecutionStatus.FAILED:
                    errors.append(event.error)

                live.update(visualizer.render_all(execution.state_by_source, errors))

        event_bus.subscribe(on_event, is_async=True)
        execution()
        live.update(visualizer.render_all(execution.state_by_source, errors))


def main() -> None:
    logging.disable()

    parser = argparse.ArgumentParser(description="Interloper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Backfill
    subparsers.add_parser("backfill", help="Backfill a DAG")

    args = parser.parse_args()
    if args.command == "backfill":
        backfill()


if __name__ == "__main__":
    main()
