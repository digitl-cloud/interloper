import datetime as dt
import threading
from abc import ABC, abstractmethod
from collections.abc import Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pprint import pp

import interloper as itlp
from interloper.asset.base import Asset
from interloper.dag.base import DAG
from interloper.events.bus import get_event_bus
from interloper.events.event import Event, EventType
from interloper.execution.context import ExecutionContext
from interloper.execution.state import ExecutionState, ExecutionStatus
from interloper.execution.strategy import ExecutionStategy
from interloper.io.base import IO
from interloper.partitioning.partition import Partition
from interloper.partitioning.window import PartitionWindow, TimePartitionWindow
from interloper.source.base import Source

event_bus = get_event_bus()


class Run:
    def __init__(
        self,
        dag: DAG,
        context: ExecutionContext,
        max_concurrency: int | None = None,
        raises: bool = True,
        blocked_assets: list[Asset] | None = None,
    ):
        self._dag = dag
        self._context = context
        self._state = ExecutionState()
        self._asset_states: dict[Asset, ExecutionState] = {asset: ExecutionState() for asset in dag.assets.values()}

        self._futures: dict[Asset, Future] = {}
        self._pool = ThreadPoolExecutor(max_workers=max_concurrency)
        self._lock = threading.Lock()
        self._raises = raises

        if blocked_assets:
            self._block_assets(blocked_assets)

    @event_bus.event(EventType.RUN)
    def __call__(self) -> None:
        self._state.status = ExecutionStatus.RUNNING

        while True:
            self._schedule_assets()

            scheduled = [future for asset, future in self._futures.items() if not self._is_asset_done(asset)]
            if not scheduled:
                break

            wait(scheduled, return_when=FIRST_COMPLETED)

        self._pool.shutdown(wait=True)

        if failed_assets := self.assets_with_status(ExecutionStatus.FAILED):
            self._state.status = ExecutionStatus.FAILED
            self._state.error = Exception(f"Failed to complete assets: {failed_assets}")
            if self._raises:
                raise self._state.error
        else:
            self._state.status = ExecutionStatus.SUCCESSFUL

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

    def assets_with_status(self, status: ExecutionStatus) -> list[Asset]:
        return [asset for asset in self._asset_states if self._asset_states[asset].status == status]

    def _is_asset_ready(self, asset: Asset) -> bool:
        if self._asset_states[asset].status in {
            ExecutionStatus.SUCCESSFUL,
            ExecutionStatus.FAILED,
            ExecutionStatus.BLOCKED,
            ExecutionStatus.RUNNING,
        }:
            return False

        return all(
            self._asset_states[upstream_asset].status == ExecutionStatus.SUCCESSFUL
            for upstream_asset in self._dag.predecessors(asset)
        )

    def _is_asset_done(self, asset: Asset) -> bool:
        return self._asset_states[asset].status in {
            ExecutionStatus.SUCCESSFUL,
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
                    self._asset_states[asset].status = ExecutionStatus.SUCCESSFUL
            except Exception as e:
                # print(f"Failed asset {asset.id}: {e}")
                with self._lock:
                    self._asset_states[asset].status = ExecutionStatus.FAILED
                    self._asset_states[asset].error = e

                # Block all downstream assets
                self._block_assets(self.dag.successors(asset))

            self._schedule_assets()

        self._futures[asset] = self._pool.submit(task)

    def _block_assets(
        self,
        assets: list[Asset],
    ) -> None:
        for asset in assets:
            if asset in self._asset_states:
                with self._lock:
                    self._asset_states[asset].status = ExecutionStatus.BLOCKED

                # Recursively block downstream assets
                self._block_assets(self.dag.successors(asset))


TPartition = Partition | Sequence[Partition] | PartitionWindow | None
TExecutionState = dict[Asset, dict[TPartition, ExecutionState]]
TExecutionStateBySource = dict[Source | None, TExecutionState]
TRunsByPartition = dict[TPartition, ExecutionState]


class Execution(ABC):
    _partitions: Sequence[Partition] | PartitionWindow | None

    def __init__(
        self,
        dag: DAG,
        partitions: TPartition = None,
        fail_fast: bool = False,
    ):
        self._dag = dag
        self._runs = set()
        self._fail_fast = fail_fast

        if isinstance(partitions, Partition):
            self._partitions = [partitions]
        else:
            self._partitions = partitions

        if partitions is None and not self.dag.execution_strategy == ExecutionStategy.NOT_PARTITIONED:
            raise RuntimeError("The DAG contains partitioned assets, but no partitions were provided to the executor")

        event_bus.subscribe(self.on_event, is_async=True)
        self._init_state()

    def __del__(self) -> None:
        event_bus.unsubscribe(self.on_event)

    @event_bus.event(EventType.EXECUTION)
    def __call__(self) -> None:
        if self._partitions is None:
            run = Run(self.dag, ExecutionContext(assets=self.dag.assets))
            self.submit_run(run)
        else:
            non_partitioned_dag, partitioned_dag = self.dag.split()
            failed_assets: list[Asset] = []

            # STAGE 1: run the non-partitioned part of the DAG
            # This part of the DAG has to be completed before the partitioned part can run
            if not non_partitioned_dag.is_empty:
                run = Run(non_partitioned_dag, ExecutionContext(assets=self.dag.assets))
                self.submit_run(run)
                self.wait_for_runs()

                failed_assets.extend(run.assets_with_status(ExecutionStatus.FAILED))
                failed_assets.extend(run.assets_with_status(ExecutionStatus.BLOCKED))

            # Assets to be blocked between the non-partitioned and partitioned parts of the DAG
            blocked_assets = [blocked for failed in failed_assets for blocked in self.dag.successors(failed)]

            # STAGE 2: run the partitioned part of the DAG
            if not partitioned_dag.is_empty:
                # Single run execution with a partition window if supported
                if (
                    isinstance(self._partitions, PartitionWindow)
                    and partitioned_dag.execution_strategy == ExecutionStategy.PARTITIONED_SINGLE_RUN
                ):
                    run = Run(
                        dag=partitioned_dag,
                        context=ExecutionContext(assets=self.dag.assets, partition=self._partitions),
                        blocked_assets=blocked_assets,
                    )
                    self.submit_run(run)

                # Otherwise, run each partition in a separate run
                else:
                    for partition in self._partitions:
                        run = Run(
                            dag=partitioned_dag,
                            context=ExecutionContext(assets=self.dag.assets, partition=partition),
                            blocked_assets=blocked_assets,
                        )
                        self.submit_run(run)

        self.wait_for_runs()
        self.shutdown()

    def _init_state(self) -> None:
        self._state: TExecutionState = {}
        non_partitioned_dag, partitioned_dag = self.dag.split()

        for asset in non_partitioned_dag.assets.values():
            self._state[asset] = {}
            self._state[asset][None] = ExecutionState()

        for asset in partitioned_dag.assets.values():
            self._state[asset] = {}
            if partitioned_dag.supports_partitioning_window and isinstance(self._partitions, PartitionWindow):
                self._state[asset][self._partitions] = ExecutionState()
            else:
                assert self._partitions is not None
                for partition in list(self._partitions):
                    self._state[asset][partition] = ExecutionState()

    # TODO: This should be optimized to only update the state of the assets that have changed
    #       Metadata containing the run info could be added to the event
    def _update_state(self) -> None:
        for run in self.runs:
            for asset, state in run.asset_states.items():
                self._state[asset][run.context.partition] = state

    def on_event(self, event: Event) -> None:
        if event.type == EventType.RUN:
            self._update_state()

    @property
    def dag(self) -> DAG:
        return self._dag

    @property
    def runs(self) -> list[Run]:
        return list(self._runs)

    @property
    def state(self) -> TExecutionState:
        return self._state

    @property
    def state_by_source(self) -> TExecutionStateBySource:
        final: TExecutionStateBySource = {}
        for asset, states in self.state.items():
            if asset.source not in final:
                final[asset.source] = {}
            final[asset.source][asset] = states
        return final

    @abstractmethod
    def submit_run(self, run: Run) -> None:
        pass

    @abstractmethod
    def wait_for_runs(self) -> None:
        pass

    @abstractmethod
    def shutdown(self) -> None:
        pass


class SimpleExecution(Execution):
    def submit_run(self, run: Run) -> None:
        try:
            self._runs.add(run)
            run()
        except Exception as e:
            if self._fail_fast:
                raise e

    def wait_for_runs(self) -> None:
        pass

    def shutdown(self) -> None:
        pass


class MultiThreadExecution(Execution):
    def __init__(
        self,
        dag: DAG,
        partitions: TPartition = None,
        max_concurrency: int = 3,
        fail_fast: bool = False,
    ):
        super().__init__(dag, partitions, fail_fast)
        self._max_concurrency = max_concurrency
        self._pool = ThreadPoolExecutor(max_workers=max_concurrency)
        self._lock = threading.Lock()
        self._futures: dict[Run, Future] = {}

    def submit_run(self, run: Run) -> None:
        def task() -> None:
            try:
                with self._lock:
                    self._runs.add(run)
                run()
            except Exception as e:
                # print(f"Failed run {run}: {e}")
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
    import pandas as pd
    from interloper_pandas.normalizer import DataframeNormalizer
    from interloper_sql.io import SQLiteIO

    def work() -> None:
        from random import random, uniform
        from time import sleep

        sleep(uniform(0.0, 1.0))
        if random() < 0.2:
            raise Exception("Ooops")

    @itlp.source(normalizer=DataframeNormalizer())
    def source() -> tuple[itlp.Asset, ...]:
        @itlp.asset()
        def root() -> pd.DataFrame:
            raise Exception("Ooops")
            # work()
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

    io: dict[str, IO] = {
        # "file": itlp.FileIO(base_dir="data"),
        "sqlite": SQLiteIO(db_path="data/sqlite.db"),
    }
    source.io = io
    source2 = source(name="source2")

    dag = DAG([source, source2])
    partitions = TimePartitionWindow(dt.date(2025, 1, 1), dt.date(2025, 1, 3))

    # execution = SimpleExecution(dag, list(partitions))
    execution = MultiThreadExecution(dag, partitions)
    execution()

    print("----")
    pp(execution.state)
