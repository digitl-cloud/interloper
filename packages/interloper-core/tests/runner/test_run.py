"""Tests for ``interloper.runner.run``."""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

import pytest

import interloper as il
from interloper.errors import PartitionError, RunnerError
from interloper.events import Event, EventType
from interloper.operation import Operation, OperationResult
from interloper.runner import ExecutionStatus, Run, RunState


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
    return il.DAG(upstream, Second(id="second", destinations=[il.MemoryDestination()], first=upstream))


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

    async def test_a_raising_execute_is_the_runs_error(self):
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

    def broken(self: RunState, operation: Operation, result: OperationResult) -> None:
        raise RuntimeError("recording failed")

    monkeypatch.setattr(RunState, "finish", broken)
    dag = il.DAG(
        First(id="first", destinations=[il.MemoryDestination()]),
        Other(id="other", destinations=[il.MemoryDestination()]),
    )

    result = await ScriptedRunner(max_workers=2).run(dag)

    assert result.status is ExecutionStatus.FAILED
    assert all(info.is_terminal for info in result.executions.values())
