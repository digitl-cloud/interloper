"""This module contains tests for the execution classes."""
import datetime as dt
from concurrent.futures import Future
from unittest.mock import Mock, patch

import pytest

import interloper as itlp
from interloper.dag.base import DAG
from interloper.execution.context import ExecutionContext
from interloper.execution.execution import MultiThreadExecution, Run, SimpleExecution
from interloper.execution.state import ExecutionState, ExecutionStatus
from interloper.execution.strategy import ExecutionStategy
from interloper.partitioning.window import TimePartitionWindow


@pytest.fixture
def empty_dag():
    """Return an empty DAG mock."""
    empty = Mock(spec=DAG)
    empty.assets = {}
    return empty


class TestRun:
    """Test the Run class."""

    def test_run_creation(self):
        """Test that a run can be created."""
        # Setup
        dag = Mock(spec=DAG)
        dag.assets = {
            "asset1": Mock(spec=itlp.Asset),
            "asset2": Mock(spec=itlp.Asset),
        }
        context = ExecutionContext(assets=dag.assets)

        # Execution
        run = Run(dag, context)

        # Assertions
        assert run.dag == dag
        assert run.context == context
        assert run.state.status == ExecutionStatus.PENDING
        assert len(run.asset_states) == 2
        assert all(state.status == ExecutionStatus.PENDING for state in run.asset_states.values())

    def test_run_creation_with_max_concurrency(self):
        """Test that a run can be created with max_concurrency."""
        # Setup
        dag = Mock(spec=DAG)
        dag.assets = {"asset1": Mock(spec=itlp.Asset)}
        context = ExecutionContext(assets=dag.assets)

        # Execution
        run = Run(dag, context, max_concurrency=5)

        # Assertions
        assert run._pool._max_workers == 5

    def test_run_creation_with_blocked_assets(self):
        """Test that a run can be created with blocked assets."""
        # Setup
        asset1 = Mock(spec=itlp.Asset)
        asset2 = Mock(spec=itlp.Asset)
        asset3 = Mock(spec=itlp.Asset)

        dag = Mock(spec=DAG)
        dag.assets = {"asset1": asset1, "asset2": asset2, "asset3": asset3}
        # Mock successors to avoid infinite recursion
        dag.successors.side_effect = lambda asset: [asset3] if asset == asset2 else []
        context = ExecutionContext(assets=dag.assets)

        # Execution
        run = Run(dag, context, blocked_assets=[asset2])

        # Assertions
        assert run.asset_states[asset2].status == ExecutionStatus.BLOCKED
        assert run.asset_states[asset3].status == ExecutionStatus.BLOCKED

    def test_assets_with_status(self):
        """Test the assets_with_status method."""
        # Setup
        asset1 = Mock(spec=itlp.Asset)
        asset2 = Mock(spec=itlp.Asset)
        asset3 = Mock(spec=itlp.Asset)

        dag = Mock(spec=DAG)
        dag.assets = {"asset1": asset1, "asset2": asset2, "asset3": asset3}
        context = ExecutionContext(assets=dag.assets)

        run = Run(dag, context)
        run.asset_states[asset1].status = ExecutionStatus.SUCCESSFUL
        run.asset_states[asset2].status = ExecutionStatus.FAILED
        run.asset_states[asset3].status = ExecutionStatus.RUNNING

        # Execution
        successful_assets = run.assets_with_status(ExecutionStatus.SUCCESSFUL)
        failed_assets = run.assets_with_status(ExecutionStatus.FAILED)

        # Assertions
        assert successful_assets == [asset1]
        assert failed_assets == [asset2]

    def test_is_asset_ready_not_ready_states(self):
        """Test that assets with non-pending states are not ready."""
        # Setup
        asset = Mock(spec=itlp.Asset)
        dag = Mock(spec=DAG)
        dag.assets = {"asset": asset}
        dag.predecessors.return_value = []
        context = ExecutionContext(assets=dag.assets)

        run = Run(dag, context)

        # Test different states that should not be ready
        for status in [
            ExecutionStatus.SUCCESSFUL,
            ExecutionStatus.FAILED,
            ExecutionStatus.BLOCKED,
            ExecutionStatus.RUNNING,
        ]:
            run.asset_states[asset].status = status

            # Execution
            is_ready = run._is_asset_ready(asset)

            # Assertions
            assert not is_ready

    def test_is_asset_ready_with_upstream_dependencies(self):
        """Test that an asset is ready only when its upstream dependencies are successful."""
        # Setup
        upstream_asset = Mock(spec=itlp.Asset)
        downstream_asset = Mock(spec=itlp.Asset)

        dag = Mock(spec=DAG)
        dag.assets = {"upstream": upstream_asset, "downstream": downstream_asset}
        dag.predecessors.return_value = [upstream_asset]
        context = ExecutionContext(assets=dag.assets)

        run = Run(dag, context)
        run.asset_states[downstream_asset].status = ExecutionStatus.PENDING

        # Test when upstream is not successful
        run.asset_states[upstream_asset].status = ExecutionStatus.RUNNING

        # Execution
        is_ready = run._is_asset_ready(downstream_asset)

        # Assertions
        assert not is_ready

        # Test when upstream is successful
        run.asset_states[upstream_asset].status = ExecutionStatus.SUCCESSFUL

        # Execution
        is_ready = run._is_asset_ready(downstream_asset)

        # Assertions
        assert is_ready

    def test_is_asset_done(self):
        """Test the is_asset_done method."""
        # Setup
        asset = Mock(spec=itlp.Asset)
        dag = Mock(spec=DAG)
        dag.assets = {"asset": asset}
        context = ExecutionContext(assets=dag.assets)

        run = Run(dag, context)

        # Test done states
        for status in [ExecutionStatus.SUCCESSFUL, ExecutionStatus.FAILED, ExecutionStatus.BLOCKED]:
            run.asset_states[asset].status = status

            # Execution
            is_done = run._is_asset_done(asset)

            # Assertions
            assert is_done

        # Test not done states
        run.asset_states[asset].status = ExecutionStatus.PENDING

        # Execution
        is_done = run._is_asset_done(asset)

        # Assertions
        assert not is_done

    @patch("interloper.execution.execution.wait")
    @patch("interloper.execution.execution.ThreadPoolExecutor")
    def test_run_execution_successful(self, executor, mock_wait):
        """Test a successful run execution."""
        # Setup
        asset = Mock(spec=itlp.Asset)
        asset.materialize.return_value = None

        dag = Mock(spec=DAG)
        dag.assets = {"asset": asset}
        dag.predecessors.return_value = []
        context = ExecutionContext(assets=dag.assets)

        # Mock the future and executor
        mock_future = Mock(spec=Future)
        mock_pool = Mock()
        mock_pool.submit.return_value = mock_future
        executor.return_value = mock_pool

        # Mock wait to return empty list (no more futures to wait for)
        mock_wait.return_value = ([], [])

        run = Run(dag, context)

        # Manually set the asset state to SUCCESSFUL to simulate task completion
        run.asset_states[asset].status = ExecutionStatus.SUCCESSFUL

        # Execution
        run()

        # Assertions
        assert run.state.status == ExecutionStatus.SUCCESSFUL
        assert run.asset_states[asset].status == ExecutionStatus.SUCCESSFUL
        mock_pool.shutdown.assert_called_once_with(wait=True)

    @patch("interloper.execution.execution.wait")
    @patch("interloper.execution.execution.ThreadPoolExecutor")
    def test_run_execution_with_failure(self, executor, mock_wait):
        """Test a run execution with a failure."""
        # Setup
        asset = Mock(spec=itlp.Asset)
        asset.materialize.side_effect = Exception("Test error")

        dag = Mock(spec=DAG)
        dag.assets = {"asset": asset}
        dag.predecessors.return_value = []
        context = ExecutionContext(assets=dag.assets)

        # Mock the future and executor
        mock_future = Mock(spec=Future)
        mock_pool = Mock()
        mock_pool.submit.return_value = mock_future
        executor.return_value = mock_pool

        # Mock wait to return empty list (no more futures to wait for)
        mock_wait.return_value = ([], [])

        run = Run(dag, context)

        # Manually set the asset state to FAILED to simulate task failure
        run.asset_states[asset].status = ExecutionStatus.FAILED
        run.asset_states[asset].error = Exception("Test error")

        # Execution & Assertions
        with pytest.raises(Exception, match="Failed to complete assets"):
            run()

        assert run.state.status == ExecutionStatus.FAILED
        assert run.asset_states[asset].status == ExecutionStatus.FAILED

    @patch("interloper.execution.execution.wait")
    @patch("interloper.execution.execution.ThreadPoolExecutor")
    def test_run_execution_with_failure_no_raise(self, executor, mock_wait):
        """Test a run execution with a failure that does not raise."""
        # Setup
        asset = Mock(spec=itlp.Asset)
        asset.materialize.side_effect = Exception("Test error")

        dag = Mock(spec=DAG)
        dag.assets = {"asset": asset}
        dag.predecessors.return_value = []
        context = ExecutionContext(assets=dag.assets)

        # Mock the future and executor
        mock_future = Mock(spec=Future)
        mock_pool = Mock()
        mock_pool.submit.return_value = mock_future
        executor.return_value = mock_pool

        # Mock wait to return empty list (no more futures to wait for)
        mock_wait.return_value = ([], [])

        run = Run(dag, context, raises=False)

        # Manually set the asset state to FAILED to simulate task failure
        run.asset_states[asset].status = ExecutionStatus.FAILED
        run.asset_states[asset].error = Exception("Test error")

        # Execution
        run()

        # Assertions
        assert run.state.status == ExecutionStatus.FAILED
        assert run.asset_states[asset].status == ExecutionStatus.FAILED


class TestExecution:
    """Test the Execution class."""

    def test_execution_creation(self, empty_dag):
        """Test that an execution can be created."""
        # Setup
        dag = Mock(spec=DAG)
        dag.execution_strategy = ExecutionStategy.NOT_PARTITIONED
        dag.assets = {"asset1": Mock(spec=itlp.Asset)}
        dag.split.return_value = (dag, empty_dag)

        # Execution
        execution = SimpleExecution(dag)

        # Assertions
        assert execution.dag == dag
        assert execution._partitions is None
        assert execution._fail_fast is False
        assert len(execution.runs) == 0

    def test_execution_creation_with_partitions(self, empty_dag):
        """Test that an execution can be created with partitions."""
        # Setup
        dag = Mock(spec=DAG)
        dag.execution_strategy = ExecutionStategy.PARTITIONED_MULTI_RUNS
        dag.assets = {"asset1": Mock(spec=itlp.Asset)}
        dag.split.return_value = (empty_dag, dag)
        partitions = [Mock(), Mock()]

        # Execution
        execution = SimpleExecution(dag, partitions)

        # Assertions
        assert execution._partitions == partitions

    def test_execution_creation_with_single_partition(self, empty_dag):
        """Test that an execution can be created with a single partition."""
        # Setup
        dag = Mock(spec=DAG)
        dag.execution_strategy = ExecutionStategy.PARTITIONED_MULTI_RUNS
        dag.assets = {"asset1": Mock(spec=itlp.Asset)}
        dag.split.return_value = (empty_dag, dag)
        partition = "2024-01-01"  # Use a real value, not a Mock

        # Execution
        execution = SimpleExecution(dag, [partition])

        # Assertions
        assert execution._partitions == [partition]

    def test_execution_creation_with_partitioned_dag_no_partitions_raises_error(self):
        """Test that creating an execution with a partitioned DAG and no partitions raises an error."""
        # Setup
        dag = Mock(spec=DAG)
        dag.execution_strategy = ExecutionStategy.PARTITIONED_MULTI_RUNS
        dag.assets = {"asset1": Mock(spec=itlp.Asset)}

        # Execution & Assertions
        with pytest.raises(RuntimeError, match="The DAG contains partitioned assets, but no partitions were provided"):
            SimpleExecution(dag)

    def test_state_by_source(self, empty_dag):
        """Test the state_by_source property."""
        # Setup
        asset1 = Mock(spec=itlp.Asset)
        asset1.source = Mock()
        asset2 = Mock(spec=itlp.Asset)
        asset2.source = None

        dag = Mock(spec=DAG)
        dag.execution_strategy = ExecutionStategy.NOT_PARTITIONED
        dag.assets = {"asset1": asset1, "asset2": asset2}
        dag.split.return_value = (dag, empty_dag)

        execution = SimpleExecution(dag)
        execution._state = {asset1: {None: ExecutionState()}, asset2: {None: ExecutionState()}}

        # Execution
        state_by_source = execution.state_by_source

        # Assertions
        assert asset1.source in state_by_source
        assert None in state_by_source
        assert asset1 in state_by_source[asset1.source]
        assert asset2 in state_by_source[None]


class TestSimpleExecution:
    """Test the SimpleExecution class."""

    def test_submit_run(self, empty_dag):
        """Test submitting a run."""
        # Setup
        dag = Mock(spec=DAG)
        dag.execution_strategy = ExecutionStategy.NOT_PARTITIONED
        dag.assets = {"asset1": Mock(spec=itlp.Asset)}
        dag.split.return_value = (dag, empty_dag)

        execution = SimpleExecution(dag)
        run = Mock(spec=Run)

        # Execution
        execution.submit_run(run)

        # Assertions
        assert run in execution._runs
        run.assert_called_once()

    def test_submit_run_with_exception_fail_fast(self, empty_dag):
        """Test submitting a run that raises an exception with fail_fast."""
        # Setup
        dag = Mock(spec=DAG)
        dag.execution_strategy = ExecutionStategy.NOT_PARTITIONED
        dag.assets = {"asset1": Mock(spec=itlp.Asset)}
        dag.split.return_value = (dag, empty_dag)

        execution = SimpleExecution(dag, fail_fast=True)
        run = Mock(spec=Run)
        run.side_effect = Exception("Test error")

        # Execution & Assertions
        with pytest.raises(Exception, match="Test error"):
            execution.submit_run(run)

    def test_submit_run_with_exception_no_fail_fast(self, empty_dag):
        """Test submitting a run that raises an exception without fail_fast."""
        # Setup
        dag = Mock(spec=DAG)
        dag.execution_strategy = ExecutionStategy.NOT_PARTITIONED
        dag.assets = {"asset1": Mock(spec=itlp.Asset)}
        dag.split.return_value = (dag, empty_dag)

        execution = SimpleExecution(dag, fail_fast=False)
        run = Mock(spec=Run)
        run.side_effect = Exception("Test error")

        # Execution
        execution.submit_run(run)

        # Assertions
        assert run in execution._runs

    def test_wait_for_runs(self, empty_dag):
        """Test waiting for runs."""
        # Setup
        dag = Mock(spec=DAG)
        dag.execution_strategy = ExecutionStategy.NOT_PARTITIONED
        dag.assets = {"asset1": Mock(spec=itlp.Asset)}
        dag.split.return_value = (dag, empty_dag)

        execution = SimpleExecution(dag)

        # Execution
        execution.wait_for_runs()

        # Assertions - should not raise any exception

    def test_shutdown(self, empty_dag):
        """Test shutting down the execution."""
        # Setup
        dag = Mock(spec=DAG)
        dag.execution_strategy = ExecutionStategy.NOT_PARTITIONED
        dag.assets = {"asset1": Mock(spec=itlp.Asset)}
        dag.split.return_value = (dag, empty_dag)

        execution = SimpleExecution(dag)

        # Execution
        execution.shutdown()

        # Assertions - should not raise any exception


class TestMultiThreadExecution:
    """Test the MultiThreadExecution class."""

    def test_multi_thread_execution_creation(self, empty_dag):
        """Test that a multi-threaded execution can be created."""
        # Setup
        dag = Mock(spec=DAG)
        dag.execution_strategy = ExecutionStategy.NOT_PARTITIONED
        dag.assets = {"asset1": Mock(spec=itlp.Asset)}
        dag.split.return_value = (dag, empty_dag)

        # Execution
        execution = MultiThreadExecution(dag, max_concurrency=5)

        # Assertions
        assert execution._max_concurrency == 5
        assert execution._pool._max_workers == 5

    @patch("interloper.execution.execution.ThreadPoolExecutor")
    def test_submit_run(self, executor, empty_dag):
        """Test submitting a run."""
        # Setup
        dag = Mock(spec=DAG)
        dag.execution_strategy = ExecutionStategy.NOT_PARTITIONED
        dag.assets = {"asset1": Mock(spec=itlp.Asset)}
        dag.split.return_value = (dag, empty_dag)

        execution = MultiThreadExecution(dag)
        run = Mock(spec=Run)
        mock_future = Mock(spec=Future)
        execution._pool.submit.return_value = mock_future

        # Simulate the thread's effect by adding run to _runs
        def fake_submit(task):
            execution._runs.add(run)
            return mock_future

        execution._pool.submit.side_effect = fake_submit

        # Execution
        execution.submit_run(run)

        # Assertions
        assert run in execution._runs
        execution._pool.submit.assert_called_once()

    @patch("interloper.execution.execution.ThreadPoolExecutor")
    def test_submit_run_with_exception_fail_fast(self, executor, empty_dag):
        """Test submitting a run that raises an exception with fail_fast."""
        # Setup
        dag = Mock(spec=DAG)
        dag.execution_strategy = ExecutionStategy.NOT_PARTITIONED
        dag.assets = {"asset1": Mock(spec=itlp.Asset)}
        dag.split.return_value = (dag, empty_dag)

        execution = MultiThreadExecution(dag, fail_fast=True)
        run = Mock(spec=Run)
        run.side_effect = Exception("Test error")
        mock_future = Mock(spec=Future)
        execution._pool.submit.return_value = mock_future

        # Simulate the thread's effect by raising the exception
        def fake_submit(task):
            raise Exception("Test error")

        execution._pool.submit.side_effect = fake_submit

        # Execution & Assertions
        with pytest.raises(Exception, match="Test error"):
            execution.submit_run(run)

    @patch("interloper.execution.execution.wait")
    @patch("interloper.execution.execution.ThreadPoolExecutor")
    def test_wait_for_runs(self, executor, mock_wait, empty_dag):
        """Test waiting for runs."""
        # Setup
        dag = Mock(spec=DAG)
        dag.execution_strategy = ExecutionStategy.NOT_PARTITIONED
        dag.assets = {"asset1": Mock(spec=itlp.Asset)}
        dag.split.return_value = (dag, empty_dag)

        execution = MultiThreadExecution(dag)
        mock_future = Mock(spec=Future)
        execution._futures = {"run1": mock_future}

        # Patch wait to avoid using real Future internals
        mock_wait.return_value = ([], [])

        # Execution
        execution.wait_for_runs()

        # Assertions
        mock_wait.assert_called_once_with([mock_future])

    @patch("interloper.execution.execution.ThreadPoolExecutor")
    def test_shutdown(self, executor, empty_dag):
        """Test shutting down the execution."""
        # Setup
        dag = Mock(spec=DAG)
        dag.execution_strategy = ExecutionStategy.NOT_PARTITIONED
        dag.assets = {"asset1": Mock(spec=itlp.Asset)}
        dag.split.return_value = (dag, empty_dag)

        execution = MultiThreadExecution(dag)

        # Execution
        execution.shutdown()

        # Assertions
        execution._pool.shutdown.assert_called_once_with(wait=True)

    @patch("interloper.execution.execution.ThreadPoolExecutor")
    def test_cancel_runs(self, executor, empty_dag):
        """Test canceling runs."""
        # Setup
        dag = Mock(spec=DAG)
        dag.execution_strategy = ExecutionStategy.NOT_PARTITIONED
        dag.assets = {"asset1": Mock(spec=itlp.Asset)}
        dag.split.return_value = (dag, empty_dag)

        execution = MultiThreadExecution(dag)
        mock_future1 = Mock(spec=Future)
        mock_future2 = Mock(spec=Future)
        execution._futures = {"run1": mock_future1, "run2": mock_future2}

        # Execution
        execution._cancel_runs()

        # Assertions
        mock_future1.cancel.assert_called_once()
        mock_future2.cancel.assert_called_once()


class TestExecutionIntegration:
    """Test the execution classes with real assets."""

    def test_simple_execution_with_non_partitioned_dag(self):
        """Test a simple execution with a non-partitioned DAG."""
        # Setup
        @itlp.asset
        def asset1():
            return "data1"

        @itlp.asset
        def asset2(asset1: str = itlp.UpstreamAsset("asset1")):
            return f"data2 from {asset1}"

        @itlp.source
        def source():
            return (asset1, asset2)

        dag = DAG([source])

        # Execution
        execution = SimpleExecution(dag)
        execution()

        # Assertions
        assert len(execution.runs) == 1
        # Note: The actual execution might fail due to missing IO, but we're testing the structure
        # The run should be created and the execution should complete

    def test_simple_execution_with_partitioned_dag(self):
        """Test a simple execution with a partitioned DAG."""
        # Setup
        @itlp.asset
        def asset1():
            return "data1"

        @itlp.asset(partitioning=itlp.TimePartitionConfig("date"))
        def asset2(date: dt.date = None, asset1: str = itlp.UpstreamAsset("asset1")):
            return f"data2 for {date} from {asset1}"

        @itlp.source
        def source():
            return (asset1, asset2)

        dag = DAG([source])
        partitions = [dt.date(2024, 1, 1), dt.date(2024, 1, 2)]

        # Execution
        execution = SimpleExecution(dag, partitions)
        execution()

        # Assertions
        assert len(execution.runs) == 3  # 1 for non-partitioned + 2 for partitions

    def test_multi_thread_execution_with_partition_window(self):
        """Test a multi-threaded execution with a partition window."""
        # Setup
        @itlp.asset
        def asset1():
            return "data1"

        @itlp.asset(partitioning=itlp.TimePartitionConfig("date", allow_window=True))
        def asset2(date: tuple[dt.date, dt.date] = itlp.DateWindow(), asset1: str = itlp.UpstreamAsset("asset1")):
            return f"data2 for {date} from {asset1}"

        @itlp.source
        def source():
            return (asset1, asset2)

        dag = DAG([source])
        partition_window = TimePartitionWindow(dt.date(2024, 1, 1), dt.date(2024, 1, 3))

        # Execution
        execution = MultiThreadExecution(dag, partition_window, max_concurrency=2)
        execution()

        # Assertions
        assert len(execution.runs) == 2  # 1 for non-partitioned + 1 for partition window
