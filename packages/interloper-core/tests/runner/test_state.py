"""Tests for ``interloper.runner.state``."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

import interloper as il
from interloper.events import Event, EventBus, EventType
from interloper.operation import OperationResult
from interloper.runner import ExecutionStatus, RunState


class ChainSource(il.Source):
    """``root`` feeds ``middle``, which feeds ``leaf``."""

    class Root(il.Asset):
        """Returns one row."""

        def data(self) -> Any:
            return [{"x": 1}]

    class Middle(il.Asset):
        """Depends on ``root``."""

        def data(self, root: il.Upstream) -> Any:
            return root.data

    class Leaf(il.Asset):
        """Depends on ``middle``."""

        def data(self, middle: il.Upstream) -> Any:
            return middle.data


class ForkSource(il.Source):
    """``root`` feeds two independent leaves."""

    class Root(il.Asset):
        """Returns one row."""

        def data(self) -> Any:
            return [{"x": 1}]

    class LeftLeaf(il.Asset):
        """Depends on ``root``."""

        def data(self, root: il.Upstream) -> Any:
            return root.data

    class RightLeaf(il.Asset):
        """Depends on ``root``."""

        def data(self, root: il.Upstream) -> Any:
            return root.data


class _Asset(il.Asset):
    """Plain asset fixture (never materialized in these tests)."""


@contextmanager
def _capture() -> Iterator[list[Event]]:
    """Collect every event delivered on the global bus within the block.

    Yields:
        The growing list of captured events.
    """
    captured: list[Event] = []
    EventBus.flush(timeout=5.0)
    EventBus.subscribe(captured.append)
    try:
        yield captured
        EventBus.flush(timeout=5.0)
    finally:
        EventBus.unsubscribe(captured.append)


def _state(source: il.Source, *, reports_progress: bool = True) -> tuple[RunState, dict[str, Any]]:
    dag = il.DAG(source)
    state = RunState(dag, None, {"run_id": "run-1"}, reports_progress=reports_progress)
    return state, {operation.key: operation for operation in dag.operations}


def _single(run_id: str, asset_id: str) -> tuple[RunState, il.Asset]:
    asset = _Asset(id=asset_id)
    return RunState(il.DAG(asset), None, {"run_id": run_id}), asset


@pytest.fixture
def chain() -> tuple[RunState, dict[str, Any]]:
    """A three-deep chain's state, plus its operations keyed by asset key.

    Returns:
        The initialized state and a ``{key: operation}`` mapping.
    """
    return _state(ChainSource(destinations=[il.MemoryDestination()]))


class TestIdentity:
    """The metadata the state carries for every event it emits."""

    def test_the_run_id_comes_from_the_metadata(self) -> None:
        state = RunState(il.DAG(ChainSource()), None, {"run_id": "r1", "backfill_id": "b1"})

        assert state.run_id == "r1"
        assert state.metadata["backfill_id"] == "b1"


class TestInitialisation:
    """Where every operation starts, before anything is scheduled."""

    def test_roots_are_ready_and_dependents_queued(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, operations = chain

        assert state.executions[operations["root"].id].status is ExecutionStatus.READY
        assert state.executions[operations["middle"].id].status is ExecutionStatus.QUEUED
        assert state.executions[operations["leaf"].id].status is ExecutionStatus.QUEUED

    def test_non_enabled_operations_are_skipped_from_the_start(self) -> None:
        dag = il.DAG(ChainSource(destinations=[il.MemoryDestination()]))
        state = RunState(dag, None, {"run_id": "run-1"})
        skipped = [
            operation
            for operation in dag.operations
            if state.executions[operation.id].status is ExecutionStatus.SKIPPED
        ]

        assert all(not operation.enabled for operation in skipped)

    def test_a_dependent_of_only_skipped_operations_is_promoted(self, monkeypatch: Any) -> None:
        # A DAG whose upstream is disabled (e.g. a destination-only
        # node) must not leave its dependent queued forever.
        dag = il.DAG(ChainSource(destinations=[il.MemoryDestination()]))
        root = next(operation for operation in dag.operations if operation.key == "root")
        # `enabled` is a field on `Operation`, so there is no class attribute to replace:
        # the property is installed, not overridden.
        monkeypatch.setattr(
            type(root), "enabled", property(lambda self: self.key != "root"), raising=False
        )

        state = RunState(dag, None, {"run_id": "run-1"})

        assert state.executions[root.id].status is ExecutionStatus.SKIPPED
        middle = next(operation for operation in dag.operations if operation.key == "middle")
        assert state.executions[middle.id].status is ExecutionStatus.READY


class TestQueue:
    """The announcement of every operation that will execute."""

    def test_announces_ready_and_queued_operations_only(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, operations = chain

        with _capture() as events:
            state.queue()

        queued = {event.metadata["component_id"] for event in events if event.type is EventType.OPERATION_QUEUED}
        assert queued == {operation.id for operation in operations.values()}


class TestStart:
    """An operation's attempt begins."""

    def test_marks_running_and_reports_the_start(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, operations = chain

        with _capture() as events:
            state.start(operations["root"])

        assert state.executions[operations["root"].id].status is ExecutionStatus.RUNNING
        assert state.ready == []
        assert [event.type for event in events] == [EventType.OPERATION_STARTED]


class TestRetry:
    """A retried attempt is recorded without becoming the node's verdict."""

    def test_reports_and_advances_the_attempt(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, operations = chain
        root = operations["root"]

        with _capture() as events:
            state.retry(root, "boom")

        assert state.executions[root.id].attempt == 2
        retried = [event for event in events if event.type is EventType.OPERATION_RETRIED]
        assert len(retried) == 1
        assert retried[0].metadata["attempt"] == 1
        assert retried[0].metadata["error"] == "boom"

    def test_a_retried_attempt_is_not_a_verdict(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, operations = chain
        root, middle = operations["root"], operations["middle"]
        state.start(root)

        state.retry(root, "boom")

        # Neither a terminal status for the node nor a cancellation downstream:
        # only an exhausted budget is a failure, and finish is what says so.
        assert state.executions[root.id].status is ExecutionStatus.RUNNING
        assert state.executions[middle.id].status is ExecutionStatus.QUEUED

    def test_events_after_a_retry_carry_the_new_attempt(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, operations = chain
        root = operations["root"]

        with _capture() as events:
            state.retry(root, "boom")
            state.start(root)
            state.finish(root, OperationResult(error="boom again"))

        started = [event for event in events if event.type is EventType.OPERATION_STARTED]
        failed = [event for event in events if event.type is EventType.OPERATION_FAILED]
        assert started[0].metadata["attempt"] == 2
        assert failed[0].metadata["attempt"] == 2


class TestFinish:
    """What an operation came to, and what it does to its dependents."""

    def test_a_success_completes_and_promotes(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, operations = chain
        state.start(operations["root"])

        with _capture() as events:
            state.finish(operations["root"], OperationResult())

        assert state.completed == [operations["root"]]
        assert state.ready == [operations["middle"]]
        assert [event.type for event in events] == [EventType.OPERATION_COMPLETED]

    def test_a_failure_fails_and_cancels_the_whole_downstream_chain(
        self, chain: tuple[RunState, dict[str, Any]]
    ) -> None:
        state, operations = chain

        with _capture() as events:
            state.finish(operations["root"], OperationResult(error="nope", traceback="Traceback..."))

        assert state.failed == [operations["root"]]
        assert state.executions[operations["middle"].id].status is ExecutionStatus.CANCELED
        assert state.executions[operations["leaf"].id].status is ExecutionStatus.CANCELED
        failed = next(event for event in events if event.type is EventType.OPERATION_FAILED)
        assert (failed.metadata["error"], failed.metadata["traceback"]) == ("nope", "Traceback...")
        canceled = [event for event in events if event.type is EventType.OPERATION_CANCELED]
        assert [event.metadata["message"] for event in canceled] == [
            "Operation 'middle' canceled (upstream failure)",
            "Operation 'leaf' canceled (upstream failure)",
        ]

    def test_keeps_the_result(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, operations = chain
        result = OperationResult(error="nope", state={"slot": 1})

        state.finish(operations["root"], result)

        assert state.executions[operations["root"].id].result is result
        assert state.executions[operations["root"].id].error == "nope"

    def test_a_fork_promotes_both_leaves_at_once(self) -> None:
        state, operations = _state(ForkSource(destinations=[il.MemoryDestination()]))

        state.finish(operations["root"], OperationResult())

        assert {operation.key for operation in state.ready} == {"left_leaf", "right_leaf"}

    def test_a_leaf_waits_for_every_predecessor(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, operations = chain

        state.finish(operations["root"], OperationResult())

        assert state.executions[operations["leaf"].id].status is ExecutionStatus.QUEUED

    def test_an_already_terminal_dependent_is_left_alone(self) -> None:
        state, operations = _state(ForkSource(destinations=[il.MemoryDestination()]))
        # The left leaf already ran (a stale-state / re-entry scenario); its
        # recorded outcome must survive a sibling-path failure.
        state.executions[operations["left_leaf"].id].finish(OperationResult())

        state.finish(operations["root"], OperationResult(error="nope"))

        assert state.executions[operations["left_leaf"].id].status is ExecutionStatus.COMPLETED
        assert state.executions[operations["right_leaf"].id].status is ExecutionStatus.CANCELED

    def test_promotion_skips_a_successor_that_already_ran(self) -> None:
        state, operations = _state(ForkSource(destinations=[il.MemoryDestination()]))
        state.executions[operations["left_leaf"].id].finish(OperationResult())

        state.finish(operations["root"], OperationResult())

        assert state.executions[operations["left_leaf"].id].status is ExecutionStatus.COMPLETED
        assert state.executions[operations["right_leaf"].id].status is ExecutionStatus.READY


class TestCancelPending:
    """The end-of-walk sweep that leaves nothing non-terminal."""

    def test_cancels_queued_ready_and_running_operations(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, operations = chain
        state.start(operations["root"])

        with _capture() as events:
            state.cancel_pending()

        assert all(
            state.executions[operation.id].status is ExecutionStatus.CANCELED for operation in operations.values()
        )
        assert state.done
        assert len([event for event in events if event.type is EventType.OPERATION_CANCELED]) == 3

    def test_terminal_operations_are_untouched(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, operations = chain
        state.finish(operations["root"], OperationResult())

        state.cancel_pending()

        assert state.executions[operations["root"].id].status is ExecutionStatus.COMPLETED

    def test_a_second_sweep_cancels_nothing(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, _ = chain
        state.cancel_pending()

        with _capture() as events:
            state.cancel_pending()

        assert events == []


class TestDone:
    """Whether every operation reached a final status."""

    def test_not_done_while_anything_is_pending(self, chain: tuple[RunState, dict[str, Any]]) -> None:
        state, _ = chain

        assert not state.done


class TestProgressReportedElsewhere:
    """Without ``reports_progress``, starts and retries are recorded silently; verdicts still emit."""

    def test_start_and_retry_are_silent_and_the_verdict_is_not(self) -> None:
        state, operations = _state(ChainSource(destinations=[il.MemoryDestination()]), reports_progress=False)
        root = operations["root"]

        with _capture() as events:
            state.start(root)
            state.retry(root, "transient")
            state.finish(root, OperationResult())

        assert [event.type for event in events] == [EventType.OPERATION_COMPLETED]
        assert state.executions[root.id].status is ExecutionStatus.COMPLETED

    def test_the_verdict_lands_on_the_attempt_the_child_reached(self) -> None:
        state, operations = _state(ChainSource(destinations=[il.MemoryDestination()]), reports_progress=False)
        root = operations["root"]

        with _capture() as events:
            state.start(root)
            state.advance(root, 3)
            state.advance(root, 2)
            state.finish(root, OperationResult(error="exited"))

        [failed] = [event for event in events if event.type is EventType.OPERATION_FAILED]
        assert failed.id == RunState.operation_event_id("run-1", root.id, EventType.OPERATION_FAILED, attempt=3)
        assert state.executions[root.id].attempt == 3


class TestEventIds:
    """Operation events carry ids derived from ``(run_id, component_id, event_type, attempt)``.

    The *same* logical event collapses to one row when produced by more than
    one party, most importantly a container's child emitting its own
    ``operation_failed`` and the host authoring one from the exit status.
    """

    def test_the_derivation_is_deterministic(self) -> None:
        base = RunState.operation_event_id("run-1", "asset-1", EventType.OPERATION_FAILED)

        assert base == RunState.operation_event_id("run-1", "asset-1", EventType.OPERATION_FAILED)
        assert base != RunState.operation_event_id("run-2", "asset-1", EventType.OPERATION_FAILED)
        assert base != RunState.operation_event_id("run-1", "asset-2", EventType.OPERATION_FAILED)
        assert base != RunState.operation_event_id("run-1", "asset-1", EventType.OPERATION_COMPLETED)

    def test_a_first_attempt_keeps_the_id_written_before_retries_existed(self) -> None:
        implicit = RunState.operation_event_id("run-1", "asset-1", EventType.OPERATION_FAILED)
        explicit = RunState.operation_event_id("run-1", "asset-1", EventType.OPERATION_FAILED, attempt=1)

        assert implicit == explicit

    def test_each_attempt_has_its_own_id(self) -> None:
        first = RunState.operation_event_id("run-1", "asset-1", EventType.OPERATION_STARTED, attempt=1)
        second = RunState.operation_event_id("run-1", "asset-1", EventType.OPERATION_STARTED, attempt=2)

        assert first != second

    def test_a_verdict_carries_the_derived_id(self) -> None:
        state, asset = _single("run-abc", "asset-xyz")
        with _capture() as events:
            state.finish(asset, OperationResult(error="boom"))
        failed = next(event for event in events if event.type is EventType.OPERATION_FAILED)
        assert failed.id == RunState.operation_event_id("run-abc", "asset-xyz", EventType.OPERATION_FAILED)

        state, asset = _single("run-abc", "asset-2")
        with _capture() as events:
            state.finish(asset, OperationResult())
        completed = next(event for event in events if event.type is EventType.OPERATION_COMPLETED)
        assert completed.id == RunState.operation_event_id("run-abc", "asset-2", EventType.OPERATION_COMPLETED)

    def test_the_hosts_and_the_childs_terminal_share_one_id(self) -> None:
        child_state, child_asset = _single("run-shared", "asset-shared")
        host_state, host_asset = _single("run-shared", "asset-shared")

        with _capture() as events:
            child_state.finish(child_asset, OperationResult(error="child error"))
            host_state.finish(host_asset, OperationResult(error="Job foo failed"))

        assert len({event.id for event in events if event.type is EventType.OPERATION_FAILED}) == 1

    def test_the_hosts_and_the_childs_queued_share_one_id(self) -> None:
        host_state, _ = _single("run-q", "asset-q")
        child_state, _ = _single("run-q", "asset-q")

        with _capture() as events:
            host_state.queue()
            child_state.queue()

        queued_ids = {event.id for event in events if event.type is EventType.OPERATION_QUEUED}
        assert queued_ids == {RunState.operation_event_id("run-q", "asset-q", EventType.OPERATION_QUEUED)}
