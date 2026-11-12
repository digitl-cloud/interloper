"""Tests for ``interloper.runner.run``."""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, ClassVar

import pytest

import interloper as il
from interloper.errors import PartitionError, RunnerError
from interloper.events import Event, EventBus, EventType
from interloper.operation import Operation, OperationResult
from interloper.runner import ExecutionStatus, Run


class First(il.Asset):
    """An upstream asset."""

    def data(self) -> list[dict[str, Any]]:
        return [{"a": 1}]


class Second(il.Asset):
    """A downstream asset."""

    def data(self, first: il.Upstream) -> list[dict[str, Any]]:
        return first.data


class ScriptedRun(Run):
    """A run whose operations return scripted results, recording what executed."""

    results: ClassVar[dict[str, OperationResult]] = {}
    executed: ClassVar[list[str]] = []

    async def execute(self, operation: Operation) -> OperationResult:
        self.executed.append(operation.key)
        await asyncio.sleep(0)
        return self.results.get(operation.key, OperationResult())


class ScriptedRunner(il.AsyncRunner):
    """An in-process runner whose run is scripted."""

    Run = ScriptedRun


@pytest.fixture(autouse=True)
def _script() -> Any:
    ScriptedRun.results = {}
    ScriptedRun.executed = []
    yield
    ScriptedRun.results = {}
    ScriptedRun.executed = []


def _dag() -> il.DAG:
    upstream = First(id="first", destinations=[il.MemoryDestination()])
    downstream = Second(id="second", destinations=[il.MemoryDestination()])
    downstream.bind("first", upstream)
    return il.DAG(upstream, downstream)


class TestWalk:
    """The one walk every kind of run shares."""

    async def test_operations_execute_in_dependency_order(self):
        result = await ScriptedRunner().run(_dag())

        assert result.status is ExecutionStatus.COMPLETED
        assert ScriptedRun.executed == ["first", "second"]

    async def test_a_failure_cancels_its_dependents(self):
        ScriptedRun.results = {"first": OperationResult(error="boom")}

        result = await ScriptedRunner(fail_fast=False).run(_dag())

        statuses = {info.component_key: info.status for info in result.executions.values()}
        assert statuses == {"first": ExecutionStatus.FAILED, "second": ExecutionStatus.CANCELED}
        assert ScriptedRun.executed == ["first"]

    async def test_a_raising_execution_is_the_runs_error(self):
        class Faulty(ScriptedRun):
            async def execute(self, operation: Operation) -> OperationResult:
                raise RuntimeError("runner bug")

        class FaultyRunner(il.AsyncRunner):
            Run = Faulty

        events: list[Event] = []
        result = await FaultyRunner(on_event=events.append).run(_dag())

        assert result.status is ExecutionStatus.FAILED
        assert {info.status for info in result.executions.values()} == {ExecutionStatus.CANCELED}
        run_failed = next(event for event in events if event.type is EventType.RUN_FAILED)
        assert run_failed.metadata["error"] == "RuntimeError: runner bug"
        with pytest.raises(RuntimeError, match="runner bug"):
            await FaultyRunner(reraise=True).run(_dag())

    async def test_an_operation_canceled_from_within_fails_instead_of_stalling_the_walk(self):
        class Canceling(ScriptedRun):
            async def execute(self, operation: Operation) -> OperationResult:
                raise asyncio.CancelledError

        class CancelingRunner(il.AsyncRunner):
            Run = Canceling

        events: list[Event] = []
        result = await CancelingRunner(on_event=events.append).run(_dag())

        statuses = {info.component_key: info.status for info in result.executions.values()}
        assert statuses == {"first": ExecutionStatus.FAILED, "second": ExecutionStatus.CANCELED}
        assert result.executions["first"].error == "Operation 'first' was canceled"
        run_failed = next(event for event in events if event.type is EventType.RUN_FAILED)
        assert "circular" not in str(run_failed.metadata.get("error"))

    async def test_a_run_that_cannot_open_fails_with_its_lifecycle_events(self):
        class Unreachable(ScriptedRun):
            async def open(self) -> None:
                raise ConnectionError("cluster unreachable")

        class UnreachableRunner(il.AsyncRunner):
            Run = Unreachable

        events: list[Event] = []
        result = await UnreachableRunner(on_event=events.append).run(_dag())

        assert result.status is ExecutionStatus.FAILED
        assert ScriptedRun.executed == []
        assert [event.type for event in events] == [
            EventType.RUN_STARTED,
            EventType.OPERATION_QUEUED,
            EventType.OPERATION_QUEUED,
            EventType.OPERATION_CANCELED,
            EventType.OPERATION_CANCELED,
            EventType.RUN_FAILED,
        ]
        assert events[-1].metadata["error"] == "ConnectionError: cluster unreachable"

    async def test_a_failing_close_does_not_mask_the_result(self):
        class Leaky(ScriptedRun):
            async def close(self) -> None:
                raise OSError("could not release")

        class LeakyRunner(il.AsyncRunner):
            Run = Leaky

        result = await LeakyRunner().run(_dag())

        assert result.status is ExecutionStatus.COMPLETED

    async def test_one_runner_serves_two_concurrent_runs(self):
        left_events: list[Event] = []
        right_events: list[Event] = []
        runner = ScriptedRunner()

        left, right = await asyncio.gather(
            runner.model_copy(update={"on_event": left_events.append}).run(_dag(), metadata={"run_id": "left"}),
            runner.model_copy(update={"on_event": right_events.append}).run(_dag(), metadata={"run_id": "right"}),
        )
        shared_events: list[Event] = []
        shared = ScriptedRunner(on_event=shared_events.append)
        both = await asyncio.gather(
            shared.run(_dag(), metadata={"run_id": "one"}),
            shared.run(_dag(), metadata={"run_id": "two"}),
        )

        assert left.status is right.status is ExecutionStatus.COMPLETED
        assert {event.metadata["run_id"] for event in left_events} == {"left"}
        assert {event.metadata["run_id"] for event in right_events} == {"right"}
        assert [result.status for result in both] == [ExecutionStatus.COMPLETED] * 2
        started = sorted(event.metadata["run_id"] for event in shared_events if event.type is EventType.RUN_STARTED)
        assert started == ["one", "two"]

    async def test_a_window_no_operation_can_serve_fails_before_anything_emits(self):
        @il.asset(partitioning=il.TimePartitionConfig(column="date"))
        def daily() -> list[dict[str, Any]]:
            return []

        events: list[Event] = []

        with pytest.raises(PartitionError):
            await ScriptedRunner(on_event=events.append).run(
                il.DAG(daily(id="daily", destinations=[il.MemoryDestination()]))
            )

        assert events == []

    def test_disabled_partitioned_operations_are_exempt_from_the_window_check(self):
        # Upstream dependencies are hydrated read-only (enabled=False);
        # they must not force a partition onto an otherwise unpartitioned run.
        @il.asset(partitioning=il.TimePartitionConfig(column="date"))
        def upstream() -> list[dict[str, Any]]:
            return [{"date": "2026-01-01"}]

        asset = upstream(id="upstream", destinations=[il.MemoryDestination()])
        asset.enabled = False

        ScriptedRun(ScriptedRunner(), il.DAG(asset), None, {"run_id": "run-1"})


class TestLifecycle:
    """The run's own events."""

    async def test_starts_and_completes(self):
        events: list[Event] = []

        await ScriptedRunner(on_event=events.append).run(_dag(), metadata={"run_id": "run-1"})

        run_events = [event for event in events if event.type in (EventType.RUN_STARTED, EventType.RUN_COMPLETED)]
        assert [event.metadata["message"] for event in run_events] == [
            "Run started (2 operations)",
            "Run completed (2/2 succeeded)",
        ]
        queued = [event for event in events if event.type is EventType.OPERATION_QUEUED]
        assert len(queued) == 2
        assert events.index(run_events[0]) < events.index(queued[0])

    async def test_a_failed_operation_fails_the_run_and_counts(self):
        ScriptedRun.results = {"first": OperationResult(error="boom")}
        events: list[Event] = []

        result = await ScriptedRunner(on_event=events.append, fail_fast=False).run(_dag())

        assert result.status is ExecutionStatus.FAILED
        run_failed = next(event for event in events if event.type is EventType.RUN_FAILED)
        assert run_failed.metadata["message"] == "Run failed (1 operation(s) failed)"
        assert run_failed.metadata["error"] is None


class TestReraise:
    """What a run raises for a caller that asked for exceptions."""

    async def test_the_original_exception_when_in_process(self):
        @il.asset
        def broken() -> list[dict[str, Any]]:
            raise KeyError("missing")

        with pytest.raises(KeyError):
            await il.AsyncRunner(reraise=True).run(il.DAG(broken(id="broken", destinations=[il.MemoryDestination()])))

    async def test_a_runner_error_when_only_the_message_crossed(self):
        ScriptedRun.results = {"first": OperationResult(error="child failed")}

        with pytest.raises(RunnerError, match="child failed"):
            await ScriptedRunner(reraise=True, fail_fast=False).run(_dag())


class Other(il.Asset):
    """An asset independent of First."""

    def data(self) -> list[dict[str, Any]]:
        return []


async def test_a_fault_while_draining_still_cancels_and_fails_the_run(monkeypatch: pytest.MonkeyPatch):
    """A fault raised while recording drained results still ends the run, failed, with nothing left open."""

    def broken(self: Run, operation: Operation, result: OperationResult) -> None:
        raise RuntimeError("recording failed")

    monkeypatch.setattr(Run, "finish", broken)
    dag = il.DAG(
        First(id="first", destinations=[il.MemoryDestination()]),
        Other(id="other", destinations=[il.MemoryDestination()]),
    )

    result = await ScriptedRunner(max_workers=2).run(dag)

    assert result.status is ExecutionStatus.FAILED
    assert all(info.is_terminal for info in result.executions.values())


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
def _capture() -> Generator[list[Event], None, None]:
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


def _made(source: il.Source) -> tuple[Run, dict[str, Any]]:
    dag = il.DAG(source)
    run = ScriptedRun(ScriptedRunner(), dag, None, {"run_id": "run-1"})
    return run, {operation.key: operation for operation in dag.operations}


def _single(run_id: str, asset_id: str) -> tuple[Run, il.Asset]:
    asset = _Asset(id=asset_id)
    return ScriptedRun(ScriptedRunner(), il.DAG(asset), None, {"run_id": run_id}), asset


@pytest.fixture
def chain() -> tuple[Run, dict[str, Any]]:
    """A run of a three-deep chain, plus its operations keyed by asset key.

    Returns:
        The run, not yet walked, and a ``{key: operation}`` mapping.
    """
    return _made(ChainSource(destinations=[il.MemoryDestination()]))


class TestIdentity:
    """The metadata the run carries for every event it emits."""

    def test_the_run_id_comes_from_the_metadata(self) -> None:
        run = ScriptedRun(ScriptedRunner(), il.DAG(ChainSource()), None, {"run_id": "r1", "backfill_id": "b1"})

        assert run.run_id == "r1"
        assert run.metadata["backfill_id"] == "b1"

    def test_a_run_id_is_generated_when_the_metadata_carries_none(self) -> None:
        run = ScriptedRun(ScriptedRunner(), il.DAG(ChainSource()), None, {"backfill_id": "b1"})

        assert run.run_id
        assert run.metadata == {"run_id": run.run_id, "backfill_id": "b1"}


class TestInitialisation:
    """Where every operation starts, before anything is scheduled."""

    def test_roots_are_ready_and_dependents_queued(self, chain: tuple[Run, dict[str, Any]]) -> None:
        run, operations = chain

        assert run.executions[operations["root"].id].status is ExecutionStatus.READY
        assert run.executions[operations["middle"].id].status is ExecutionStatus.QUEUED
        assert run.executions[operations["leaf"].id].status is ExecutionStatus.QUEUED

    def test_non_enabled_operations_are_skipped_from_the_start(self) -> None:
        dag = il.DAG(ChainSource(destinations=[il.MemoryDestination()]))
        run = ScriptedRun(ScriptedRunner(), dag, None, {"run_id": "run-1"})
        skipped = [
            operation for operation in dag.operations if run.executions[operation.id].status is ExecutionStatus.SKIPPED
        ]

        assert all(not operation.enabled for operation in skipped)

    def test_a_dependent_of_only_skipped_operations_is_promoted(self, monkeypatch: Any) -> None:
        # A DAG whose upstream is disabled (e.g. a destination-only
        # node) must not leave its dependent queued forever.
        dag = il.DAG(ChainSource(destinations=[il.MemoryDestination()]))
        root = next(operation for operation in dag.operations if operation.key == "root")
        # `enabled` is a field on `Operation`, so there is no class attribute to replace:
        # the property is installed, not overridden.
        monkeypatch.setattr(type(root), "enabled", property(lambda self: self.key != "root"), raising=False)

        run = ScriptedRun(ScriptedRunner(), dag, None, {"run_id": "run-1"})

        assert run.executions[root.id].status is ExecutionStatus.SKIPPED
        middle = next(operation for operation in dag.operations if operation.key == "middle")
        assert run.executions[middle.id].status is ExecutionStatus.READY


class TestQueue:
    """The announcement of every operation that will execute."""

    async def test_announces_ready_and_queued_operations_only(self) -> None:
        upstream = First(id="first", destinations=[il.MemoryDestination()], enabled=False)
        downstream = Second(id="second", destinations=[il.MemoryDestination()])
        downstream.bind("first", upstream)
        dag = il.DAG(upstream, downstream)
        events: list[Event] = []

        await ScriptedRunner(on_event=events.append).run(dag)

        queued = [event.metadata["component_id"] for event in events if event.type is EventType.OPERATION_QUEUED]
        assert queued == ["second"]


class TestPartitionKey:
    """Every lifecycle event names the partition its operation consumes, which files its execution."""

    def test_a_snapshot_names_the_period_it_observes_and_a_dated_asset_the_runs(self) -> None:
        @il.asset(partitioning=il.SnapshotPartitionConfig(column="date"))
        def entities() -> list:  # pragma: no cover
            return []

        @il.asset(partitioning=il.TimePartitionConfig(column="date"))
        def report() -> list:  # pragma: no cover
            return []

        @il.asset
        def lookup() -> list:  # pragma: no cover
            return []

        dag = il.DAG(entities(id="entities"), report(id="report"), lookup(id="lookup"))
        window = il.TimePartition(dt.date(2026, 10, 26)).window
        metadata = {"run_id": "run-1", "observed_at": "2026-10-29T14:00:00+00:00"}
        run = ScriptedRun(ScriptedRunner(), dag, window, metadata)

        with _capture() as events:
            for operation in dag.operations:
                run.start(operation)

        named = {event.metadata["component_id"]: event.metadata.get("partition_key") for event in events}
        assert named == {"entities": "2026-10-29", "report": "2026-10-26", "lookup": None}


class TestStart:
    """An operation's attempt begins."""

    def test_marks_running_and_reports_the_start(self, chain: tuple[Run, dict[str, Any]]) -> None:
        run, operations = chain

        with _capture() as events:
            run.start(operations["root"])

        assert run.executions[operations["root"].id].status is ExecutionStatus.RUNNING
        assert run.ready == []
        assert [event.type for event in events] == [EventType.OPERATION_STARTED]


class TestRetry:
    """A retried attempt is recorded without becoming the node's verdict."""

    def test_reports_and_advances_the_attempt(self, chain: tuple[Run, dict[str, Any]]) -> None:
        run, operations = chain
        root = operations["root"]

        with _capture() as events:
            run.retry(root, "boom")

        assert run.executions[root.id].attempt == 2
        retried = [event for event in events if event.type is EventType.OPERATION_RETRIED]
        assert len(retried) == 1
        assert retried[0].metadata["attempt"] == 1
        assert retried[0].metadata["error"] == "boom"

    def test_a_retried_attempt_is_not_a_verdict(self, chain: tuple[Run, dict[str, Any]]) -> None:
        run, operations = chain
        root, middle = operations["root"], operations["middle"]
        run.start(root)

        run.retry(root, "boom")

        # Neither a terminal status for the node nor a cancellation downstream:
        # only an exhausted budget is a failure, and finish is what says so.
        assert run.executions[root.id].status is ExecutionStatus.RUNNING
        assert run.executions[middle.id].status is ExecutionStatus.QUEUED

    def test_events_after_a_retry_carry_the_new_attempt(self, chain: tuple[Run, dict[str, Any]]) -> None:
        run, operations = chain
        root = operations["root"]

        with _capture() as events:
            run.retry(root, "boom")
            run.start(root)
            run.finish(root, OperationResult(error="boom again"))

        started = [event for event in events if event.type is EventType.OPERATION_STARTED]
        failed = [event for event in events if event.type is EventType.OPERATION_FAILED]
        assert started[0].metadata["attempt"] == 2
        assert failed[0].metadata["attempt"] == 2


class TestFinish:
    """What an operation came to, and what it does to its dependents."""

    def test_a_success_completes_and_promotes(self, chain: tuple[Run, dict[str, Any]]) -> None:
        run, operations = chain
        run.start(operations["root"])

        with _capture() as events:
            run.finish(operations["root"], OperationResult())

        assert run.completed == [operations["root"]]
        assert run.ready == [operations["middle"]]
        assert [event.type for event in events] == [EventType.OPERATION_COMPLETED]

    def test_a_failure_fails_and_cancels_the_whole_downstream_chain(self, chain: tuple[Run, dict[str, Any]]) -> None:
        run, operations = chain

        with _capture() as events:
            run.finish(operations["root"], OperationResult(error="nope", traceback="Traceback..."))

        assert run.failed == [operations["root"]]
        assert run.executions[operations["middle"].id].status is ExecutionStatus.CANCELED
        assert run.executions[operations["leaf"].id].status is ExecutionStatus.CANCELED
        failed = next(event for event in events if event.type is EventType.OPERATION_FAILED)
        assert (failed.metadata["error"], failed.metadata["traceback"]) == ("nope", "Traceback...")
        canceled = [event for event in events if event.type is EventType.OPERATION_CANCELED]
        assert [event.metadata["message"] for event in canceled] == [
            "Operation 'middle' canceled (upstream failure)",
            "Operation 'leaf' canceled (upstream failure)",
        ]

    def test_keeps_the_result(self, chain: tuple[Run, dict[str, Any]]) -> None:
        run, operations = chain
        result = OperationResult(error="nope", state={"slot": 1})

        run.finish(operations["root"], result)

        assert run.executions[operations["root"].id].result is result
        assert run.executions[operations["root"].id].error == "nope"

    def test_a_fork_promotes_both_leaves_at_once(self) -> None:
        run, operations = _made(ForkSource(destinations=[il.MemoryDestination()]))

        run.finish(operations["root"], OperationResult())

        assert {operation.key for operation in run.ready} == {"left_leaf", "right_leaf"}

    def test_a_leaf_waits_for_every_predecessor(self, chain: tuple[Run, dict[str, Any]]) -> None:
        run, operations = chain

        run.finish(operations["root"], OperationResult())

        assert run.executions[operations["leaf"].id].status is ExecutionStatus.QUEUED

    def test_an_already_terminal_dependent_is_left_alone(self) -> None:
        run, operations = _made(ForkSource(destinations=[il.MemoryDestination()]))
        # The left leaf already ran (a stale-state / re-entry scenario); its
        # recorded outcome must survive a sibling-path failure.
        run.executions[operations["left_leaf"].id].finish(OperationResult())

        run.finish(operations["root"], OperationResult(error="nope"))

        assert run.executions[operations["left_leaf"].id].status is ExecutionStatus.COMPLETED
        assert run.executions[operations["right_leaf"].id].status is ExecutionStatus.CANCELED

    def test_promotion_skips_a_successor_that_already_ran(self) -> None:
        run, operations = _made(ForkSource(destinations=[il.MemoryDestination()]))
        run.executions[operations["left_leaf"].id].finish(OperationResult())

        run.finish(operations["root"], OperationResult())

        assert run.executions[operations["left_leaf"].id].status is ExecutionStatus.COMPLETED
        assert run.executions[operations["right_leaf"].id].status is ExecutionStatus.READY


class TestCancelPending:
    """The end-of-walk sweep that leaves nothing non-terminal."""

    def test_cancels_queued_ready_and_running_operations(self, chain: tuple[Run, dict[str, Any]]) -> None:
        run, operations = chain
        run.start(operations["root"])

        with _capture() as events:
            run.cancel_pending()

        assert all(run.executions[operation.id].status is ExecutionStatus.CANCELED for operation in operations.values())
        assert run.done
        assert len([event for event in events if event.type is EventType.OPERATION_CANCELED]) == 3

    def test_terminal_operations_are_untouched(self, chain: tuple[Run, dict[str, Any]]) -> None:
        run, operations = chain
        run.finish(operations["root"], OperationResult())

        run.cancel_pending()

        assert run.executions[operations["root"].id].status is ExecutionStatus.COMPLETED

    def test_a_second_sweep_cancels_nothing(self, chain: tuple[Run, dict[str, Any]]) -> None:
        run, _ = chain
        run.cancel_pending()

        with _capture() as events:
            run.cancel_pending()

        assert events == []


class TestDone:
    """Whether every operation reached a final status."""

    def test_not_done_while_anything_is_pending(self, chain: tuple[Run, dict[str, Any]]) -> None:
        run, _ = chain

        assert not run.done


class TestEventIds:
    """Operation events carry ids derived from ``(run_id, component_id, event_type, attempt)``.

    The *same* logical event collapses to one row when produced by more than
    one party, most importantly a stopping pod delivering a verdict late and
    the store authoring one for the operation its run left open.
    """

    def test_the_derivation_is_deterministic(self) -> None:
        base = Run.operation_event_id("run-1", "asset-1", EventType.OPERATION_FAILED)

        assert base == Run.operation_event_id("run-1", "asset-1", EventType.OPERATION_FAILED)
        assert base != Run.operation_event_id("run-2", "asset-1", EventType.OPERATION_FAILED)
        assert base != Run.operation_event_id("run-1", "asset-2", EventType.OPERATION_FAILED)
        assert base != Run.operation_event_id("run-1", "asset-1", EventType.OPERATION_COMPLETED)

    def test_a_first_attempt_keeps_the_id_written_before_retries_existed(self) -> None:
        implicit = Run.operation_event_id("run-1", "asset-1", EventType.OPERATION_FAILED)
        explicit = Run.operation_event_id("run-1", "asset-1", EventType.OPERATION_FAILED, attempt=1)

        assert implicit == explicit

    def test_each_attempt_has_its_own_id(self) -> None:
        first = Run.operation_event_id("run-1", "asset-1", EventType.OPERATION_STARTED, attempt=1)
        second = Run.operation_event_id("run-1", "asset-1", EventType.OPERATION_STARTED, attempt=2)

        assert first != second

    def test_a_verdict_carries_the_derived_id(self) -> None:
        run, asset = _single("run-abc", "asset-xyz")
        with _capture() as events:
            run.finish(asset, OperationResult(error="boom"))
        failed = next(event for event in events if event.type is EventType.OPERATION_FAILED)
        assert failed.id == Run.operation_event_id("run-abc", "asset-xyz", EventType.OPERATION_FAILED)

        run, asset = _single("run-abc", "asset-2")
        with _capture() as events:
            run.finish(asset, OperationResult())
        completed = next(event for event in events if event.type is EventType.OPERATION_COMPLETED)
        assert completed.id == Run.operation_event_id("run-abc", "asset-2", EventType.OPERATION_COMPLETED)

    def test_two_authors_of_one_verdict_share_one_id(self) -> None:
        host_run, host_asset = _single("run-shared", "asset-shared")
        late_run, late_asset = _single("run-shared", "asset-shared")

        with _capture() as events:
            host_run.finish(host_asset, OperationResult(error="Job foo failed"))
            late_run.finish(late_asset, OperationResult(error="stopping pod"))

        assert len({event.id for event in events if event.type is EventType.OPERATION_FAILED}) == 1
