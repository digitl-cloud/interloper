"""Tests for ``interloper.runner.sync_runner``."""

from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, ClassVar

import pytest
from pydantic import PrivateAttr

import interloper as il
from interloper.errors import PartitionError, RunnerError
from interloper.operation.base import Operation, OperationContext, OperationResult
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runner.results import ExecutionStatus
from interloper.runner.sync_runner import SyncRunner


class ThreadRunner(SyncRunner):
    """Minimal :class:`SyncRunner` whose executor is a thread pool.

    Stands in for the out-of-process runners (docker, kubernetes,
    multi-process) so the shared blocking DAG walk can be exercised without
    spawning anything. Its futures raise on failure, i.e. exactly the base
    ``_handle_completed`` / ``_handle_flushed`` contract.
    """

    max_workers: int = 4
    reraise: bool = False

    _pool: ThreadPoolExecutor | None = PrivateAttr(default=None)
    _submitted: list[str] = PrivateAttr(default_factory=list)

    @property
    def _capacity(self) -> int:
        """Concurrency ceiling used to throttle submissions.

        Returns:
            ``max_workers``.
        """
        return self.max_workers

    def _on_start(self) -> None:
        """Create the thread pool."""
        self._pool = ThreadPoolExecutor(max_workers=self.max_workers)

    def _on_end(self) -> None:
        """Shut the thread pool down."""
        if self._pool is not None:
            self._pool.shutdown(wait=True)
            self._pool = None

    def _submit_operation(
        self,
        operation: Operation,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> Future[Any]:
        """Run the operation on the pool.

        Args:
            operation: The operation to execute.
            partition_or_window: Partition or window to scope the run.

        Returns:
            A future resolving to the operation's :class:`OperationResult`.

        Raises:
            RunnerError: If the pool was never created.
        """
        if self._pool is None:
            raise RunnerError("Thread pool not initialized")
        self._submitted.append(operation.id)
        self.state.mark_running(operation)
        return self._pool.submit(
            lambda: asyncio.run(
                operation.execute(
                    OperationContext(
                        partition_or_window=operation.effective_partition(partition_or_window),
                        dag=self.state.dag,
                        metadata=self.state.metadata,
                    )
                )
            )
        )


@il.asset()
def source_row() -> list[dict[str, Any]]:
    """Returns one row."""
    return [{"x": 1}]


class BrokenChainSource(il.Source):
    """Source whose only downstream asset depends on an asset that always fails."""

    class Broken(il.Asset):
        """Always fails.

        Raises:
            ValueError: Always.
        """

        def data(self) -> Any:
            raise ValueError("nope")

    class Consumer(il.Asset):
        """Passes the failing upstream's rows through."""

        requires: ClassVar[dict[str, str]] = {"broken": "broken"}

        def data(self, broken: Any) -> Any:
            return broken


def _memory() -> il.MemoryDestination:
    return il.MemoryDestination()


class TestBlockingWalk:
    """The shared ``concurrent.futures`` DAG walk."""

    def test_runs_the_whole_dag(self) -> None:
        il.MemoryDestination.clear()
        dag = il.DAG(
            source_row(id="a", destinations=[_memory()]),
            source_row(id="b", destinations=[_memory()]),
        )

        result = il.run(ThreadRunner().run(dag))

        assert result.status is ExecutionStatus.COMPLETED
        assert sorted(result.completed_ids) == ["a", "b"]

    def test_dependencies_are_submitted_in_topological_order(self) -> None:
        il.MemoryDestination.clear()

        @il.asset()
        def upstream() -> list[dict[str, Any]]:
            return [{"x": 1}]

        @il.asset()
        def downstream(upstream: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return upstream

        dag = il.DAG(
            upstream(id="upstream", destinations=[_memory()]),
            downstream(id="downstream", destinations=[_memory()]),
        )
        runner = ThreadRunner()

        il.run(runner.run(dag))

        assert runner._submitted == ["upstream", "downstream"]

    def test_capacity_bounds_the_number_of_inflight_operations(self) -> None:
        il.MemoryDestination.clear()
        peak = 0
        inflight = 0
        lock = threading.Lock()
        release = threading.Event()

        @il.asset()
        def blocking() -> list[dict[str, Any]]:
            nonlocal peak, inflight
            with lock:
                inflight += 1
                peak = max(peak, inflight)
            release.wait(timeout=5)
            with lock:
                inflight -= 1
            return [{"x": 1}]

        dag = il.DAG(
            blocking(id="one", destinations=[_memory()]),
            blocking(id="two", destinations=[_memory()]),
            blocking(id="three", destinations=[_memory()]),
        )
        threading.Timer(0.3, release.set).start()

        il.run(ThreadRunner(max_workers=1).run(dag))

        assert peak == 1


class TestFailureHandling:
    """Operation failures become node state, not exceptions from the walk."""

    def test_a_failure_is_absorbed_into_the_node(self) -> None:
        il.MemoryDestination.clear()

        @il.asset()
        def boom() -> list[dict[str, Any]]:
            raise ValueError("nope")

        dag = il.DAG(boom(id="boom", destinations=[_memory()]))

        result = il.run(ThreadRunner().run(dag))

        assert result.status is ExecutionStatus.FAILED
        info = result.executions["boom"]
        assert "nope" in (info.error or "")
        assert info.traceback is not None

    def test_downstream_of_a_failure_is_canceled(self) -> None:
        il.MemoryDestination.clear()
        dag = il.DAG(BrokenChainSource(destinations=[_memory()]))

        result = il.run(ThreadRunner(fail_fast=False).run(dag))

        assert [result.executions[key].component_key for key in result.failed_ids] == ["broken"]
        assert [result.executions[key].component_key for key in result.canceled_ids] == ["consumer"]

    def test_reraise_surfaces_the_original_exception(self) -> None:
        il.MemoryDestination.clear()

        @il.asset()
        def raiser() -> list[dict[str, Any]]:
            raise KeyError("missing-field")

        dag = il.DAG(raiser(id="raiser", destinations=[_memory()]))

        with pytest.raises(KeyError, match="missing-field"):
            il.run(ThreadRunner(reraise=True).run(dag))

    def test_fail_fast_leaves_no_execution_non_terminal(self) -> None:
        il.MemoryDestination.clear()
        failed = threading.Event()

        @il.asset()
        def boom() -> list[dict[str, Any]]:
            failed.set()
            raise ValueError("nope")

        @il.asset()
        def slow() -> list[dict[str, Any]]:
            # Outlive the failure, so the flush must record our outcome.
            failed.wait(timeout=5)
            time.sleep(0.2)
            return [{"x": 1}]

        @il.asset()
        def queued() -> list[dict[str, Any]]:
            return [{"x": 2}]

        dag = il.DAG(
            boom(id="boom", destinations=[_memory()]),
            slow(id="slow", destinations=[_memory()]),
            queued(id="queued", destinations=[_memory()]),
        )

        result = il.run(ThreadRunner(max_workers=2, fail_fast=True).run(dag))

        assert result.status is ExecutionStatus.FAILED
        assert result.executions["boom"].status is ExecutionStatus.FAILED
        assert result.executions["slow"].status is ExecutionStatus.COMPLETED
        assert result.executions["queued"].status is ExecutionStatus.CANCELED


class TestMachineryErrors:
    """Failures of the walk itself, as opposed to an operation's."""

    @pytest.fixture
    def stalled_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Make the DAG report nothing ready while the run stays incomplete.

        Args:
            monkeypatch: Fixture used to neutralize ``ready_operations``.
        """
        monkeypatch.setattr(
            "interloper.runner.state.RunState.ready_operations",
            property(lambda self: []),
        )

    def test_a_stalled_dag_is_recorded_as_a_failed_run(self, stalled_state: None) -> None:
        il.MemoryDestination.clear()
        dag = il.DAG(source_row(id="a", destinations=[_memory()]))

        result = il.run(ThreadRunner(reraise=False).run(dag))

        assert result.status is ExecutionStatus.FAILED
        assert result.executions["a"].status is ExecutionStatus.CANCELED

    def test_a_stalled_dag_reraises_when_asked(self, stalled_state: None) -> None:
        il.MemoryDestination.clear()
        dag = il.DAG(source_row(id="a", destinations=[_memory()]))

        with pytest.raises(RunnerError, match="circular dependency or invalid DAG state"):
            il.run(ThreadRunner(reraise=True).run(dag))

    def test_a_teardown_failure_does_not_mask_the_result(self) -> None:
        il.MemoryDestination.clear()

        class BrokenTeardown(ThreadRunner):
            """Runner whose pool shutdown raises.

            Raises:
                RuntimeError: From ``_on_end``, always.
            """

            def _on_end(self) -> None:
                """Shut the pool down, then fail.

                Raises:
                    RuntimeError: Always.
                """
                super()._on_end()
                raise RuntimeError("pool shutdown exploded")

        dag = il.DAG(source_row(id="a", destinations=[_memory()]))

        result = il.run(BrokenTeardown().run(dag))

        assert result.status is ExecutionStatus.COMPLETED


class TestFlush:
    """In-flight futures are drained rather than abandoned."""

    @pytest.fixture
    def finished_run(self) -> tuple[ThreadRunner, Operation]:
        """A completed single-asset run, plus its one operation.

        Returns:
            The runner (state still populated) and the operation.
        """
        il.MemoryDestination.clear()
        runner = ThreadRunner()
        il.run(runner.run(il.DAG(source_row(id="a", destinations=[_memory()]))))
        return runner, runner.state.dag.operation_map["a"]

    def test_an_empty_flush_is_a_no_op(self) -> None:
        ThreadRunner()._flush({})

    def test_a_terminal_execution_is_not_recorded_twice(
        self, finished_run: tuple[ThreadRunner, Operation]
    ) -> None:
        runner, operation = finished_run
        end_time = runner.state.executions["a"].end_time

        future: Future[Any] = Future()
        future.set_result(OperationResult())
        runner._flush({future: operation})

        assert runner.state.executions["a"].end_time == end_time

    def test_the_default_flush_records_a_returned_result(
        self, finished_run: tuple[ThreadRunner, Operation]
    ) -> None:
        runner, operation = finished_run
        runner.state.executions["a"].status = ExecutionStatus.RUNNING

        future: Future[Any] = Future()
        future.set_result(OperationResult(config={"cursor": "abc"}))
        runner._flush({future: operation})

        info = runner.state.executions["a"]
        assert info.status is ExecutionStatus.COMPLETED
        assert info.effects is not None
        assert info.effects.config == {"cursor": "abc"}

    def test_the_default_flush_records_a_raised_failure(
        self, finished_run: tuple[ThreadRunner, Operation]
    ) -> None:
        runner, operation = finished_run
        runner.state.executions["a"].status = ExecutionStatus.RUNNING

        future: Future[Any] = Future()
        future.set_exception(ValueError("late failure"))
        runner._flush({future: operation})

        info = runner.state.executions["a"]
        assert info.status is ExecutionStatus.FAILED
        assert "late failure" in (info.error or "")

    def test_a_non_result_return_value_leaves_no_effects(
        self, finished_run: tuple[ThreadRunner, Operation]
    ) -> None:
        runner, operation = finished_run
        runner.state.executions["a"].status = ExecutionStatus.RUNNING

        future: Future[Any] = Future()
        future.set_result("not an OperationResult")
        runner._flush({future: operation})

        assert runner.state.executions["a"].effects is None


class TestSubmissionContract:
    """What the abstract interface guarantees to the walk."""

    def test_capacity_reports_the_pool_size(self) -> None:
        assert ThreadRunner(max_workers=7)._capacity == 7

    def test_submitting_without_a_pool_is_a_runner_error(self) -> None:
        il.MemoryDestination.clear()
        runner = ThreadRunner()
        dag = il.DAG(source_row(id="a", destinations=[_memory()]))
        runner._init_run(dag, None, None)

        with pytest.raises(RunnerError, match="Thread pool not initialized"):
            runner._submit_operation(dag.operation_map["a"], None)

    def test_the_shared_preflight_still_applies(self) -> None:
        il.MemoryDestination.clear()

        @il.asset(partitioning=il.TimePartitionConfig(column="date"))
        def daily(context: il.ExecutionContext) -> list[dict[str, Any]]:
            return [{"date": context.partition_date}]

        dag = il.DAG(daily(id="daily", destinations=[_memory()]))

        with pytest.raises(PartitionError, match="requires a partition"):
            il.run(ThreadRunner().run(dag))

    def test_an_operation_still_listed_as_ready_is_not_submitted_twice(self) -> None:
        il.MemoryDestination.clear()
        release = threading.Event()

        class LateMarkingRunner(ThreadRunner):
            """Runner that leaves the operation ``ready`` while it is in flight.

            Subclasses are free to mark running elsewhere; the walk must not
            resubmit what it already has a future for.
            """

            def _submit_operation(
                self,
                operation: Operation,
                partition_or_window: Partition | PartitionWindow | None,
            ) -> Future[Any]:
                """Submit without transitioning the operation out of ``ready``.

                Args:
                    operation: The operation to execute.
                    partition_or_window: Partition or window to scope the run.

                Returns:
                    The submitted future.
                """
                self._submitted.append(operation.id)
                assert self._pool is not None
                return self._pool.submit(
                    lambda: asyncio.run(
                        operation.execute(
                            OperationContext(
                                partition_or_window=operation.effective_partition(partition_or_window),
                                dag=self.state.dag,
                                metadata=self.state.metadata,
                            )
                        )
                    )
                )

        @il.asset()
        def quick() -> list[dict[str, Any]]:
            return [{"x": 1}]

        @il.asset()
        def blocking() -> list[dict[str, Any]]:
            release.wait(timeout=5)
            return [{"x": 1}]

        dag = il.DAG(
            quick(id="quick", destinations=[_memory()]),
            blocking(id="blocking", destinations=[_memory()]),
        )
        # ``quick`` lands first, so the walk loops again while ``blocking``
        # is still in flight and still listed as ready.
        threading.Timer(0.3, release.set).start()
        runner = LateMarkingRunner(max_workers=4)

        result = il.run(runner.run(dag))

        assert sorted(runner._submitted) == ["blocking", "quick"]
        assert result.status is ExecutionStatus.COMPLETED
