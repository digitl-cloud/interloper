import datetime as dt
import threading
from abc import ABC, abstractmethod
from collections.abc import Iterable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pprint import pp

import pandas as pd
from interloper_pandas.normalizer import DataframeNormalizer
from interloper_sql.io import SQLiteIO

import interloper as itlp
from interloper.asset.base import Asset
from interloper.dag.base import DAG
from interloper.execution.context import ExecutionContext
from interloper.execution.state import ExecutionState, ExecutionStatus
from interloper.execution.strategy import ExecutionStategy
from interloper.partitioning.partition import Partition
from interloper.partitioning.window import PartitionWindow, TimePartitionWindow


class Run:
    def __init__(
        self,
        dag: DAG,
        context: ExecutionContext,
        max_concurrency: int | None = None,
        raises: bool = True,
    ):
        self._dag = dag
        self._context = context
        self._state = ExecutionState()
        self._asset_states: dict[Asset, ExecutionState] = {asset: ExecutionState() for asset in dag.assets.values()}

        self._futures: dict[Asset, Future] = {}
        self._pool = ThreadPoolExecutor(max_workers=max_concurrency)
        self._lock = threading.Lock()
        self._raises = raises

    def start(self) -> None:
        self._state.status = ExecutionStatus.RUNNING

        while True:
            self._schedule_assets()

            scheduled = [future for asset, future in self._futures.items() if not self._is_asset_done(asset)]
            if not scheduled:
                break

            wait(scheduled, return_when=FIRST_COMPLETED)

        self._pool.shutdown(wait=True)

        failed_assets = [
            asset for asset in self._asset_states if self._asset_states[asset].status == ExecutionStatus.FAILED
        ]

        if failed_assets:
            self._state.status = ExecutionStatus.FAILED
            self._state.error = Exception(f"Failed to complete assets: {failed_assets}")
            if self._raises:
                raise self._state.error
        else:
            self._state.status = ExecutionStatus.COMPLETED

    @property
    def dag(self) -> DAG:
        return self._dag

    @property
    def context(self) -> ExecutionContext:
        return self._context

    @property
    def state(self) -> ExecutionState:
        return self._state

    @property
    def asset_states(self) -> dict[Asset, ExecutionState]:
        return self._asset_states

    def _is_asset_ready(self, asset: Asset) -> bool:
        if self._asset_states[asset].status in {
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
            ExecutionStatus.BLOCKED,
            ExecutionStatus.RUNNING,
        }:
            return False

        return all(
            self._asset_states[upstream_asset].status == ExecutionStatus.COMPLETED
            for upstream_asset in self._dag.predecessors(asset)
        )

    def _is_asset_done(self, asset: Asset) -> bool:
        return self._asset_states[asset].status in {
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
            ExecutionStatus.BLOCKED,
        }

    def _schedule_assets(self) -> None:
        with self._lock:
            for asset in self._dag.assets.values():
                if self._is_asset_ready(asset):
                    self._submit_asset(asset)

    def _submit_asset(self, asset: Asset) -> None:
        def task() -> None:
            try:
                with self._lock:
                    self._asset_states[asset].status = ExecutionStatus.RUNNING
                asset.materialize(self._context)
                with self._lock:
                    self._asset_states[asset].status = ExecutionStatus.COMPLETED
            except Exception as e:
                print(f"Failed asset {asset.id}: {e}")
                with self._lock:
                    self._asset_states[asset].status = ExecutionStatus.FAILED
                    self._asset_states[asset].error = e

                # Block all downstream assets
                for desc in self._dag.successors(asset):
                    with self._lock:
                        self._asset_states[desc].status = ExecutionStatus.BLOCKED

            self._schedule_assets()

        self._futures[asset] = self._pool.submit(task)


class Executor(ABC):
    def __init__(self, fail_fast: bool = False):
        self._runs = set()
        self._fail_fast = fail_fast

    def execute(
        self,
        dag: DAG,
        partitions: Iterable[Partition] | PartitionWindow | None = None,
    ) -> None:
        if partitions is None:
            if not dag.execution_strategy == ExecutionStategy.NOT_PARTITIONED:
                raise RuntimeError(
                    "The DAG contains partitioned assets, but no partitions were provided to the executor"
                )

            run = Run(dag, ExecutionContext(assets=dag.assets))

        else:
            non_partitioned_dag, partitioned_dag = dag.split()

            # First, run the non-partitioned part of the DAG
            # This part of the DAG has to be finished before the partitioned part can run
            run = Run(non_partitioned_dag, ExecutionContext(assets=dag.assets))
            self.submit_run(run)
            self.wait_for_runs()

            # Single run execution with a partition window if supported
            if (
                isinstance(partitions, PartitionWindow)
                and partitioned_dag.execution_strategy == ExecutionStategy.PARTITIONED_SINGLE_RUN
            ):
                run = Run(partitioned_dag, ExecutionContext(assets=dag.assets, partition=partitions))
                self.submit_run(run)

            # Otherwise, run each partition in a separate run
            else:
                for partition in partitions:
                    run = Run(partitioned_dag, ExecutionContext(assets=dag.assets, partition=partition))
                    self.submit_run(run)

        self.wait_for_runs()
        self.shutdown()

    @property
    def runs(self) -> list[Run]:
        return list(self._runs)

    @property
    def runs_by_partition(self) -> dict[Partition | PartitionWindow | None, Run]:
        return {run.context.partition: run for run in self._runs}

    @property
    def assets_by_partition(self) -> dict[Partition | PartitionWindow | None, dict[Asset, ExecutionState]]:
        return {run.context.partition: run.asset_states for run in self.runs}

    @abstractmethod
    def submit_run(self, run: Run) -> None:
        pass

    @abstractmethod
    def wait_for_runs(self) -> None:
        pass

    @abstractmethod
    def shutdown(self) -> None:
        pass


class SimpleExecutor(Executor):
    def submit_run(self, run: Run) -> None:
        try:
            self._runs.add(run)
            run.start()
        except Exception as e:
            if self._fail_fast:
                raise e

    def wait_for_runs(self) -> None:
        pass

    def shutdown(self) -> None:
        pass


class MultiThreadExecutor(Executor):
    def __init__(
        self,
        max_concurrency: int = 3,
        fail_fast: bool = False,
    ):
        super().__init__(fail_fast=fail_fast)
        self._max_concurrency = max_concurrency
        self._pool = ThreadPoolExecutor(max_workers=max_concurrency)
        self._lock = threading.Lock()
        self._futures: dict[Run, Future] = {}

    def submit_run(self, run: Run) -> None:
        def task() -> None:
            try:
                with self._lock:
                    self._runs.add(run)
                run.start()
            except Exception as e:
                print(f"Failed run {run}: {e}")
                if self._fail_fast:
                    self._cancel_runs()
                    raise e

        with self._lock:
            self._futures[run] = self._pool.submit(task)

    def wait_for_runs(self) -> None:
        wait(list(self._futures.values()))

    def shutdown(self) -> None:
        self._pool.shutdown(wait=True)

    def _cancel_runs(self) -> None:
        for future in self._futures.values():
            future.cancel()


if __name__ == "__main__":

    @itlp.source(normalizer=DataframeNormalizer())
    def source() -> tuple[itlp.Asset, ...]:
        @itlp.asset()
        def root() -> pd.DataFrame:
            print("[NOT PARTITIONED] root")
            # sleep(1)
            return pd.DataFrame({"val": [1, 2, 3]})

        @itlp.asset()
        def left_1(
            root: pd.DataFrame = itlp.UpstreamAsset("root"),
        ) -> pd.DataFrame:
            print("[NOT PARTITIONED] left_1")
            # sleep(1)
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
            print(f"[{date[0].isoformat()}] left_2")
            # sleep(1)
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
            print(f"[{date[0].isoformat()}] right_1")
            # sleep(1)
            # raise Exception("Failed")
            return pd.DataFrame(
                {
                    "val": [123],
                    "date": pd.Series([date[0]], dtype="datetime64[ns]"),
                }
            )

        @itlp.asset(partitioning=itlp.TimePartitionConfig("date", allow_window=True))
        def right_2(
            date: tuple[dt.date, dt.date] = itlp.DateWindow(),
            right_1: pd.DataFrame = itlp.UpstreamAsset("right_1"),
        ) -> pd.DataFrame:
            print(f"[{date[0].isoformat()}] right_2")
            # sleep(1)
            import random

            if random.random() < 0.5:
                raise Exception("Failed")

            return pd.DataFrame(
                {
                    "val": [123],
                    "date": pd.Series([date[0]], dtype="datetime64[ns]"),
                }
            )

        return (root, left_1, left_2, right_1, right_2)

    source.io = {
        "sqlite": SQLiteIO(db_path="data/sqlite.db"),
        # "file": FileIO(base_dir="data/file.csv"),
    }

    dag = DAG(source)

    # run = DAGRun(dag, ExecutionContext(assets=dag.assets, partition=TimePartition(dt.date(2025, 1, 1))))
    # run.start()

    # executor = SimpleExecutor()
    executor = MultiThreadExecutor(max_concurrency=1)

    executor.execute(
        dag,
        partitions=TimePartitionWindow(
            dt.date(2025, 1, 1),
            dt.date(2025, 1, 3),
        ).iter_partitions(),
    )

    print("----")
    # pp(executor.runs_by_partition)
    pp(executor.assets_by_partition)
