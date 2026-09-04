"""Tests for ``interloper.runner.multi_process``."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from concurrent.futures import Future
from pathlib import Path
from typing import Any, ClassVar

import pytest

import interloper as il
from interloper.errors import RunnerError
from interloper.runner.multi_process import MultiProcessRunner, _worker
from interloper.runner.results import ExecutionStatus
from interloper.settings import RunnerSettings

# Module level so the process pool can pickle them, and so `_worker` can
# reconstruct them from a DAG spec in a fresh interpreter.


class WorkerSource(il.Source):
    """Source whose assets exercise the worker's success and failure paths."""

    class Ok(il.Asset):
        """Returns one static row."""

        def data(self) -> Any:
            return [{"x": 1}]

    class Boom(il.Asset):
        """Always fails.

        Raises:
            ValueError: Always.
        """

        def data(self) -> Any:
            raise ValueError("worker-side failure")


class PickledSource(il.Source):
    """Two independent assets, materialized into the in-memory destination."""

    class One(il.Asset):
        """Returns one static row."""

        def data(self) -> Any:
            return [{"x": 1}]

    class Two(il.Asset):
        """Returns one static row."""

        def data(self) -> Any:
            return [{"x": 2}]


class ChainSource(il.Source):
    """A failing asset plus the downstream that depends on it."""

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


@pytest.fixture(autouse=True)
def _no_ambient_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Run where no ``interloper.yaml`` is discoverable, so telemetry stays off.

    ``_worker`` re-initializes the OpenTelemetry SDK from settings, since in
    production it runs in a fresh interpreter. Called in-process — and in
    children that inherit this working directory — the repo's own config
    would have it replace the process-wide providers the telemetry suites
    assert against, and open an exporter connection per worker.

    Args:
        tmp_path: The config-free directory to run in.
        monkeypatch: Fixture used to change the working directory.
    """
    monkeypatch.chdir(tmp_path)


@pytest.fixture
def importable_in_children() -> Iterator[None]:
    """Put this test package on ``sys.path`` so spawned workers can import it.

    pytest's importlib import mode registers test modules without adding
    their root to ``sys.path``, and a worker reconstructing the DAG spec in a
    fresh interpreter resolves the source classes below by import path.

    Yields:
        ``None``; the teardown removes the entry again.
    """
    root = str(Path(__file__).resolve().parents[2])
    sys.path.insert(0, root)
    yield
    sys.path.remove(root)


def _spec(source: il.Source) -> tuple[dict[str, Any], il.DAG]:
    """Build a DAG and the JSON spec a worker would receive for it.

    Args:
        source: The source to wrap in a DAG.

    Returns:
        The serialized spec and the DAG it came from.
    """
    dag = il.DAG(source)
    return dag.to_spec().model_dump(mode="json"), dag


class TestWorker:
    """The child-process entry point, called directly in this process."""

    def test_a_successful_operation_returns_its_effects(self) -> None:
        il.MemoryDestination.clear()
        spec, dag = _spec(WorkerSource(destinations=[il.MemoryDestination()]))
        operation = next(o for o in dag.operations if o.key == "ok")

        operation_id, success, error, tb, effects = _worker(operation.id, spec, None, {})

        assert operation_id == operation.id
        assert success is True
        assert (error, tb) == (None, None)
        assert set(effects) == {"config", "state"}

    def test_a_failing_operation_reports_its_error_and_traceback(self) -> None:
        il.MemoryDestination.clear()
        spec, dag = _spec(WorkerSource(destinations=[il.MemoryDestination()]))
        operation = next(o for o in dag.operations if o.key == "boom")

        operation_id, success, error, tb, effects = _worker(operation.id, spec, None, {})

        assert operation_id == operation.id
        assert success is False
        assert "worker-side failure" in (error or "")
        assert tb is not None and "ValueError" in tb
        assert set(effects) == {"config", "state"}

    def test_an_unresolvable_operation_id_still_reports_cleanly(self) -> None:
        # The node is looked up before any operation exists, so the worker has
        # nothing to build a failure result from.
        spec, _dag = _spec(WorkerSource(destinations=[il.MemoryDestination()]))

        operation_id, success, error, tb, effects = _worker("not-in-this-dag", spec, None, {})

        assert (operation_id, success) == ("not-in-this-dag", False)
        assert error is not None
        assert tb is not None
        assert effects == {}

    def test_a_malformed_spec_is_reported_not_raised(self) -> None:
        operation_id, success, error, tb, effects = _worker("anything", {"nodes": "nonsense"}, None, {})

        assert (operation_id, success, effects) == ("anything", False, {})
        assert error is not None
        assert tb is not None


class TestRun:
    """End-to-end runs through a real process pool."""

    def test_materializes_every_operation(self, importable_in_children: None) -> None:
        runner = MultiProcessRunner(max_workers=2)

        result = il.run(runner.run(il.DAG(PickledSource(destinations=[il.MemoryDestination()]))))

        assert result.status is ExecutionStatus.COMPLETED
        assert len(result.completed_ids) == 2

    def test_a_child_failure_lands_on_the_node(self, importable_in_children: None) -> None:
        runner = MultiProcessRunner(max_workers=2, fail_fast=False)

        result = il.run(runner.run(il.DAG(ChainSource(destinations=[il.MemoryDestination()]))))

        assert result.status is ExecutionStatus.FAILED
        failed = result.executions[result.failed_ids[0]]
        assert failed.component_key == "broken"
        assert "nope" in (failed.error or "")
        # The message crossed a process boundary; the exception object did not.
        assert failed.exception is None
        assert [result.executions[key].component_key for key in result.canceled_ids] == ["consumer"]

    def test_reraise_wraps_a_cross_process_failure(self, importable_in_children: None) -> None:
        runner = MultiProcessRunner(max_workers=1, reraise=True)

        with pytest.raises(RunnerError, match="failed: .*nope"):
            il.run(runner.run(il.DAG(ChainSource(destinations=[il.MemoryDestination()]))))

    def test_the_pool_is_torn_down_after_the_run(self, importable_in_children: None) -> None:
        runner = MultiProcessRunner(max_workers=1)

        il.run(runner.run(il.DAG(PickledSource(destinations=[il.MemoryDestination()]))))

        assert runner._pool is None
        assert runner._dag_spec is None
        assert runner._futures == {}


class TestCapacityAndSubmission:
    """Pool sizing and the submission guard."""

    def test_capacity_is_the_pool_size(self) -> None:
        assert MultiProcessRunner(max_workers=3)._capacity == 3

    def test_defaults_are_fail_fast_without_reraise(self) -> None:
        runner = MultiProcessRunner()

        assert runner.max_workers == 4
        assert runner.fail_fast is True
        assert runner.reraise is False

    def test_submitting_before_the_pool_exists_is_a_runner_error(self) -> None:
        runner = MultiProcessRunner()
        dag = il.DAG(PickledSource(destinations=[il.MemoryDestination()]))
        runner._init_run(dag, None, None)

        with pytest.raises(RunnerError, match="Process pool not initialized"):
            runner._submit_operation(dag.operations[0], None)

    def test_registered_under_its_key(self) -> None:
        assert type(il.Runner.from_settings(RunnerSettings(type="multi_process"))) is MultiProcessRunner


class TestResultInterpretation:
    """The worker tuple is unpacked into state, by both the completion and flush paths."""

    @pytest.fixture
    def prepared(self) -> tuple[MultiProcessRunner, Any]:
        """A runner with initialized state and one operation to record against.

        Returns:
            The runner and its first operation.
        """
        runner = MultiProcessRunner()
        dag = il.DAG(PickledSource(destinations=[il.MemoryDestination()]))
        runner._init_run(dag, None, None)
        operation = dag.operations[0]
        runner.state.mark_running(operation)
        return runner, operation

    @pytest.mark.parametrize("handler", ["_handle_completed", "_handle_flushed"])
    def test_a_success_tuple_records_the_effects(
        self, handler: str, prepared: tuple[MultiProcessRunner, Any]
    ) -> None:
        runner, operation = prepared
        future: Future[Any] = Future()
        future.set_result((operation.id, True, None, None, {"config": {"cursor": "z"}, "state": {}}))

        getattr(runner, handler)(future, operation)

        info = runner.state.executions[operation.id]
        assert info.status is ExecutionStatus.COMPLETED
        assert info.effects is not None
        assert info.effects.config == {"cursor": "z"}

    @pytest.mark.parametrize("handler", ["_handle_completed", "_handle_flushed"])
    def test_a_failure_tuple_records_the_error_on_the_effects(
        self, handler: str, prepared: tuple[MultiProcessRunner, Any]
    ) -> None:
        runner, operation = prepared
        future: Future[Any] = Future()
        future.set_result((operation.id, False, "child exploded", "Traceback...", {"config": {}, "state": {}}))

        getattr(runner, handler)(future, operation)

        info = runner.state.executions[operation.id]
        assert info.status is ExecutionStatus.FAILED
        assert info.error == "child exploded"
        assert info.traceback == "Traceback..."
        assert info.effects is not None
        assert info.effects.error == "child exploded"

    @pytest.mark.parametrize("handler", ["_handle_completed", "_handle_flushed"])
    def test_a_failure_without_a_message_gets_a_placeholder(
        self, handler: str, prepared: tuple[MultiProcessRunner, Any]
    ) -> None:
        runner, operation = prepared
        future: Future[Any] = Future()
        future.set_result((operation.id, False, None, None, {"config": {}, "state": {}}))

        getattr(runner, handler)(future, operation)

        assert runner.state.executions[operation.id].error == "Unknown error"

    @pytest.mark.parametrize("handler", ["_handle_completed", "_handle_flushed"])
    def test_a_pool_level_exception_becomes_the_node_failure(
        self, handler: str, prepared: tuple[MultiProcessRunner, Any]
    ) -> None:
        # A worker killed by the OS never returns a tuple; the future raises.
        runner, operation = prepared
        future: Future[Any] = Future()
        future.set_exception(BrokenPipeError("worker died"))

        getattr(runner, handler)(future, operation)

        info = runner.state.executions[operation.id]
        assert info.status is ExecutionStatus.FAILED
        assert "worker died" in (info.error or "")

    def test_completion_drops_the_future_from_the_pending_map(
        self, prepared: tuple[MultiProcessRunner, Any]
    ) -> None:
        runner, operation = prepared
        future: Future[Any] = Future()
        future.set_result((operation.id, True, None, None, {"config": {}, "state": {}}))
        runner._futures[future] = operation

        runner._handle_completed(future, operation)

        assert runner._futures == {}


def test_worker_adopts_and_releases_the_parent_span_context() -> None:
    """A worker given a propagated parent context attaches and detaches it."""
    from interloper.telemetry.propagation import inject_metadata
    from interloper.telemetry.tracer import tracer

    il.MemoryDestination.clear()
    spec, dag = _spec(WorkerSource(destinations=[il.MemoryDestination()]))
    operation = next(o for o in dag.operations if o.key == "ok")

    metadata: dict[str, Any] = {}
    with tracer().start_as_current_span("parent"):
        inject_metadata(metadata)
    assert "traceparent" in metadata

    _operation_id, success, _error, _tb, _effects = _worker(operation.id, spec, None, metadata)

    assert success is True
