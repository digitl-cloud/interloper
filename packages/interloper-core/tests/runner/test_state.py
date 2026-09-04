"""Tests for ``interloper.runner.state``."""

from __future__ import annotations

import datetime as dt
from typing import Any, ClassVar

import pytest

import interloper as il
from interloper.runner.results import ExecutionStatus
from interloper.runner.state import RunState


class ChainSource(il.Source):
    """``root`` feeds ``middle``, which feeds ``leaf``."""

    class Root(il.Asset):
        """Returns one row."""

        def data(self) -> Any:
            return [{"x": 1}]

    class Middle(il.Asset):
        """Depends on ``root``."""

        requires: ClassVar[dict[str, str]] = {"root": "root"}

        def data(self, root: Any) -> Any:
            return root

    class Leaf(il.Asset):
        """Depends on ``middle``."""

        requires: ClassVar[dict[str, str]] = {"middle": "middle"}

        def data(self, middle: Any) -> Any:
            return middle


class ForkSource(il.Source):
    """``root`` feeds two independent leaves."""

    class Root(il.Asset):
        """Returns one row."""

        def data(self) -> Any:
            return [{"x": 1}]

    class LeftLeaf(il.Asset):
        """Depends on ``root``."""

        requires: ClassVar[dict[str, str]] = {"root": "root"}

        def data(self, root: Any) -> Any:
            return root

    class RightLeaf(il.Asset):
        """Depends on ``root``."""

        requires: ClassVar[dict[str, str]] = {"root": "root"}

        def data(self, root: Any) -> Any:
            return root


@pytest.fixture
def chain() -> tuple[RunState, dict[str, Any]]:
    """A three-deep chain's state, plus its operations keyed by asset key.

    Returns:
        The initialized state and a ``{key: operation}`` mapping.
    """
    dag = il.DAG(ChainSource(destinations=[il.MemoryDestination()]))
    state = RunState(dag)
    return state, {operation.key: operation for operation in dag.operations}


class TestIdentity:
    """The metadata the state carries for every event it emits."""

    def test_a_run_id_is_generated_when_absent(self) -> None:
        state = RunState(il.DAG(ChainSource()))

        assert state.run_id
        assert state.backfill_id is None

    def test_provided_metadata_is_kept(self) -> None:
        state = RunState(il.DAG(ChainSource()), metadata={"run_id": "r1", "backfill_id": "b1"})

        assert state.run_id == "r1"
        assert state.backfill_id == "b1"


class TestElapsedTime:
    """Wall-clock timing of the run."""

    def test_unknown_before_the_run_ends(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, _ = chain
        state.start_run(None)

        assert state.elapsed_time is None

    def test_measured_once_the_run_ends(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, _ = chain
        state.start_time = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        state.end_time = state.start_time + dt.timedelta(seconds=3)

        assert state.elapsed_time == 3.0


class TestInitialisation:
    """Where every operation starts, before anything is scheduled."""

    def test_roots_are_ready_and_dependents_queued(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, operations = chain

        assert state.executions[operations["root"].id].status is ExecutionStatus.READY
        assert state.executions[operations["middle"].id].status is ExecutionStatus.QUEUED
        assert state.executions[operations["leaf"].id].status is ExecutionStatus.QUEUED

    def test_non_materializable_operations_are_skipped_from_the_start(self) -> None:
        dag = il.DAG(ChainSource(destinations=[il.MemoryDestination()]))
        state = RunState(dag)
        skipped = [
            operation
            for operation in dag.operations
            if state.executions[operation.id].status is ExecutionStatus.SKIPPED
        ]

        assert all(not operation.materializable for operation in skipped)

    def test_a_dependent_of_only_skipped_operations_is_promoted(self, monkeypatch: Any) -> None:
        # A DAG whose upstream is not materializable (e.g. a destination-only
        # node) must not leave its dependent queued forever.
        dag = il.DAG(ChainSource(destinations=[il.MemoryDestination()]))
        root = next(operation for operation in dag.operations if operation.key == "root")
        monkeypatch.setattr(type(root), "materializable", property(lambda self: self.key != "root"))

        state = RunState(dag)

        assert state.executions[root.id].status is ExecutionStatus.SKIPPED
        middle = next(operation for operation in dag.operations if operation.key == "middle")
        assert state.executions[middle.id].status is ExecutionStatus.READY


class TestStatusBuckets:
    """The per-status operation views the runners schedule from."""

    def test_each_bucket_reflects_its_status(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, operations = chain

        assert state.ready_operations == [operations["root"]]
        assert {operation.key for operation in state.queued_operations} == {"middle", "leaf"}
        assert state.running_operations == []
        assert state.completed_operations == []
        assert state.failed_operations == []

        state.mark_running(operations["root"])
        assert state.running_operations == [operations["root"]]

        state.mark_completed(operations["root"])
        assert state.completed_operations == [operations["root"]]
        assert state.ready_operations == [operations["middle"]]

        state.mark_failed(operations["middle"], "nope")
        assert state.failed_operations == [operations["middle"]]


class TestPromotion:
    """Dependents become ready only once every predecessor is done."""

    def test_a_fork_promotes_both_leaves_at_once(self) -> None:
        dag = il.DAG(ForkSource(destinations=[il.MemoryDestination()]))
        state = RunState(dag)
        operations = {operation.key: operation for operation in dag.operations}

        state.mark_completed(operations["root"])

        assert {operation.key for operation in state.ready_operations} == {"left_leaf", "right_leaf"}

    def test_a_leaf_waits_for_every_predecessor(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, operations = chain

        state.mark_completed(operations["root"])

        assert state.executions[operations["leaf"].id].status is ExecutionStatus.QUEUED


class TestFailurePropagation:
    """A failure cancels everything transitively downstream of it."""

    def test_cancels_the_whole_downstream_chain(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, operations = chain

        state.mark_failed(operations["root"], "nope")

        assert state.executions[operations["middle"].id].status is ExecutionStatus.CANCELED
        assert state.executions[operations["leaf"].id].status is ExecutionStatus.CANCELED

    def test_an_already_terminal_dependent_is_left_alone(self) -> None:
        dag = il.DAG(ForkSource(destinations=[il.MemoryDestination()]))
        state = RunState(dag)
        operations = {operation.key: operation for operation in dag.operations}
        # The left leaf already ran (a stale-state / re-entry scenario); its
        # recorded outcome must survive a sibling-path failure.
        state.executions[operations["left_leaf"].id].mark_completed()

        state.mark_failed(operations["root"], "nope")

        assert state.executions[operations["left_leaf"].id].status is ExecutionStatus.COMPLETED
        assert state.executions[operations["right_leaf"].id].status is ExecutionStatus.CANCELED

    def test_the_failure_effects_are_recorded(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        from interloper.operation.base import OperationResult

        state, operations = chain

        state.mark_failed(
            operations["root"], "nope", tb="Traceback...", effects=OperationResult(error="nope")
        )

        info = state.executions[operations["root"].id]
        assert info.traceback == "Traceback..."
        assert info.effects is not None
        assert info.effects.error == "nope"


class TestCancelPending:
    """The end-of-walk sweep that leaves nothing non-terminal."""

    def test_cancels_queued_and_ready_operations(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, operations = chain

        canceled = state.cancel_pending()

        assert {operation.key for operation in canceled} == {"root", "middle", "leaf"}
        assert all(
            state.executions[operation.id].status is ExecutionStatus.CANCELED for operation in operations.values()
        )
        assert state.is_run_complete() is True

    def test_terminal_operations_are_untouched(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, operations = chain
        state.mark_completed(operations["root"])

        canceled = state.cancel_pending()

        assert operations["root"] not in canceled
        assert state.executions[operations["root"].id].status is ExecutionStatus.COMPLETED

    def test_a_second_sweep_cancels_nothing(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, _ = chain
        state.cancel_pending()

        assert state.cancel_pending() == []


class TestRunCompletion:
    """``is_run_complete`` and the terminal event ``end_run`` emits."""

    def test_incomplete_while_anything_is_pending(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, _ = chain

        assert state.is_run_complete() is False

    def test_end_run_returns_a_copy_of_the_executions(
        self, chain: tuple[RunState, dict[str, Any]]
    ) -> None:
        state, _ = chain
        state.start_run(None)

        executions = state.end_run(ExecutionStatus.COMPLETED)

        assert executions == state.executions
        assert executions is not state.executions
        assert state.end_time is not None

    def test_a_failed_run_reports_the_run_level_error(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, _ = chain
        state.start_run(None)
        events: list[il.Event] = []
        il.EventBus.subscribe(events.append)
        try:
            state.end_run(ExecutionStatus.FAILED, error="deadlock")
            il.EventBus.flush(timeout=5.0)
        finally:
            il.EventBus.unsubscribe(events.append)

        failure = next(event for event in events if event.type is il.EventType.RUN_FAILED)
        assert failure.metadata["error"] == "deadlock"
        assert "deadlock" in failure.metadata["message"]

    def test_a_failed_run_without_an_error_counts_the_failures(
        self, chain: tuple[RunState, dict[str, Any]]
    ) -> None:
        state, operations = chain
        state.start_run(None)
        state.mark_failed(operations["root"], "nope")
        events: list[il.Event] = []
        il.EventBus.subscribe(events.append)
        try:
            state.end_run(ExecutionStatus.FAILED)
            il.EventBus.flush(timeout=5.0)
        finally:
            il.EventBus.unsubscribe(events.append)

        failure = next(event for event in events if event.type is il.EventType.RUN_FAILED)
        assert failure.metadata["message"] == "Run failed (1 operation(s) failed)"


class TestEventSuppression:
    """``emit=False`` is how cross-process runners avoid double-reporting."""

    @pytest.mark.parametrize(
        ("transition", "arguments"),
        [("mark_running", ()), ("mark_completed", ()), ("mark_canceled", ()), ("mark_failed", ("nope",))],
    )
    def test_no_event_is_emitted(
        self, transition: str, arguments: tuple[Any, ...], chain: tuple[RunState, dict[str, Any]]
    ) -> None:
        state, operations = chain
        events: list[il.Event] = []
        il.EventBus.subscribe(events.append)
        try:
            getattr(state, transition)(operations["root"], *arguments, emit=False)
            il.EventBus.flush(timeout=5.0)
        finally:
            il.EventBus.unsubscribe(events.append)

        assert events == []


def test_promotion_skips_a_successor_that_already_ran() -> None:
    """A successor outside ``queued`` is left as it is when its predecessor completes."""
    dag = il.DAG(ForkSource(destinations=[il.MemoryDestination()]))
    state = RunState(dag)
    operations = {operation.key: operation for operation in dag.operations}
    state.executions[operations["left_leaf"].id].mark_completed()

    state.mark_completed(operations["root"])

    assert state.executions[operations["left_leaf"].id].status is ExecutionStatus.COMPLETED
    assert state.executions[operations["right_leaf"].id].status is ExecutionStatus.READY
