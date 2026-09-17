"""Tests for ``interloper.runner.async_runner``."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

import interloper as il
from interloper.errors import RunnerError
from interloper.events import Event
from interloper.runner.async_runner import AsyncRunner
from interloper.runner.results import ExecutionStatus


class TestFailFast:
    """``fail_fast`` stops scheduling; it never interrupts work already running."""

    async def test_in_flight_operations_finish_and_queued_ones_are_canceled(self):
        il.MemoryDestination.clear()
        failed = asyncio.Event()

        @il.asset()
        async def boom() -> list[dict[str, Any]]:
            failed.set()
            raise ValueError("nope")

        @il.asset()
        async def slow() -> list[dict[str, Any]]:
            # Outlive the failure so the runner must decide what to do with us.
            await asyncio.wait_for(failed.wait(), timeout=5)
            await asyncio.sleep(0.05)
            return [{"x": 1}]

        @il.asset()
        def waiting() -> list[dict[str, Any]]:
            return [{"x": 2}]

        dag = il.DAG(
            boom(id="boom", destinations=[il.MemoryDestination()]),
            slow(id="slow", destinations=[il.MemoryDestination()]),
            waiting(id="waiting", destinations=[il.MemoryDestination()]),
        )
        events: list[Event] = []

        result = await AsyncRunner(max_workers=2, fail_fast=True, on_event=events.append).run(dag)

        assert result.status is ExecutionStatus.FAILED
        assert result.executions["boom"].status is ExecutionStatus.FAILED
        assert result.executions["slow"].status is ExecutionStatus.COMPLETED
        assert result.executions["waiting"].status is ExecutionStatus.CANCELED
        completed = next(event for event in events if event.type is il.EventType.OPERATION_COMPLETED)
        canceled = next(event for event in events if event.type is il.EventType.OPERATION_CANCELED)
        assert completed.metadata["component_id"] == "slow"
        assert canceled.metadata["component_id"] == "waiting"

    async def test_disabled_runs_every_operation_that_can_still_run(self):
        il.MemoryDestination.clear()

        @il.asset()
        def boom() -> list[dict[str, Any]]:
            raise ValueError("nope")

        @il.asset()
        def fine() -> list[dict[str, Any]]:
            return [{"x": 1}]

        dag = il.DAG(
            boom(id="boom", destinations=[il.MemoryDestination()]),
            fine(id="fine-1", destinations=[il.MemoryDestination()]),
            fine(id="fine-2", destinations=[il.MemoryDestination()]),
        )

        result = await AsyncRunner(max_workers=1, fail_fast=False).run(dag)

        assert result.status is ExecutionStatus.FAILED
        assert result.executions["boom"].status is ExecutionStatus.FAILED
        assert result.executions["fine-1"].status is ExecutionStatus.COMPLETED
        assert result.executions["fine-2"].status is ExecutionStatus.COMPLETED


class TestRetry:
    """An operation retries in place, within its declared budget."""

    async def test_an_operation_that_heals_on_the_second_attempt_succeeds(self):
        il.MemoryDestination.clear()
        calls: list[int] = []

        @il.asset(retry=il.RetryPolicy(max_attempts=3, delay=0.0, jitter=0.0))
        def flaky() -> list[dict[str, Any]]:
            calls.append(1)
            if len(calls) == 1:
                raise ValueError("transient")
            return [{"x": 1}]

        dag = il.DAG(flaky(id="flaky", destinations=[il.MemoryDestination()]))
        events: list[Event] = []

        result = await AsyncRunner(on_event=events.append).run(dag)

        assert result.status is ExecutionStatus.COMPLETED
        assert result.executions["flaky"].status is ExecutionStatus.COMPLETED
        assert len(calls) == 2
        assert [e.type for e in events if e.type is il.EventType.OPERATION_RETRIED]
        assert not [e.type for e in events if e.type is il.EventType.OPERATION_FAILED]

    async def test_an_exhausted_budget_fails_once(self):
        il.MemoryDestination.clear()
        calls: list[int] = []

        @il.asset(retry=il.RetryPolicy(max_attempts=2, delay=0.0, jitter=0.0))
        def broken() -> list[dict[str, Any]]:
            calls.append(1)
            raise ValueError("permanent")

        dag = il.DAG(broken(id="broken", destinations=[il.MemoryDestination()]))
        events: list[Event] = []

        result = await AsyncRunner(on_event=events.append).run(dag)

        assert result.status is ExecutionStatus.FAILED
        assert len(calls) == 2
        assert len([e for e in events if e.type is il.EventType.OPERATION_RETRIED]) == 1
        assert len([e for e in events if e.type is il.EventType.OPERATION_FAILED]) == 1

    async def test_an_operation_without_a_budget_is_attempted_once(self):
        il.MemoryDestination.clear()
        calls: list[int] = []

        @il.asset()
        def broken() -> list[dict[str, Any]]:
            calls.append(1)
            raise ValueError("permanent")

        dag = il.DAG(broken(id="broken", destinations=[il.MemoryDestination()]))

        result = await AsyncRunner().run(dag)

        assert result.status is ExecutionStatus.FAILED
        assert len(calls) == 1

    async def test_an_error_the_operation_declines_is_not_retried(self):
        il.MemoryDestination.clear()
        calls: list[int] = []

        class Picky(il.Asset):
            """Asset that knows a ValueError will never heal."""

            retry: il.RetryPolicy | None = il.RetryPolicy(max_attempts=3, delay=0.0, jitter=0.0)

            def data(self) -> list[dict[str, Any]]:
                calls.append(1)
                raise ValueError("permanent")

            def retryable(self, error: Exception) -> bool:
                return not isinstance(error, ValueError)

        dag = il.DAG(Picky(id="picky", destinations=[il.MemoryDestination()]))

        result = await AsyncRunner().run(dag)

        assert result.status is ExecutionStatus.FAILED
        assert len(calls) == 1

    async def test_a_retrying_node_does_not_trip_fail_fast(self):
        il.MemoryDestination.clear()
        calls: list[int] = []

        @il.asset(retry=il.RetryPolicy(max_attempts=2, delay=0.0, jitter=0.0))
        def flaky() -> list[dict[str, Any]]:
            calls.append(1)
            if len(calls) == 1:
                raise ValueError("transient")
            return [{"x": 1}]

        @il.asset()
        def independent() -> list[dict[str, Any]]:
            return [{"x": 2}]

        dag = il.DAG(
            flaky(id="flaky", destinations=[il.MemoryDestination()]),
            independent(id="independent", destinations=[il.MemoryDestination()]),
        )

        result = await AsyncRunner(max_workers=1, fail_fast=True).run(dag)

        assert result.status is ExecutionStatus.COMPLETED
        assert result.executions["flaky"].status is ExecutionStatus.COMPLETED
        assert result.executions["independent"].status is ExecutionStatus.COMPLETED


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

    @staticmethod
    def _dag() -> il.DAG:
        @il.asset()
        def solo() -> list[dict[str, Any]]:
            return [{"x": 1}]

        return il.DAG(solo(id="solo", destinations=[il.MemoryDestination()]))

    async def test_a_stalled_dag_is_recorded_as_a_failed_run(self, stalled_state: None):
        il.MemoryDestination.clear()

        result = await AsyncRunner(reraise=False).run(self._dag())

        assert result.status is ExecutionStatus.FAILED
        assert result.executions["solo"].status is ExecutionStatus.CANCELED

    async def test_a_stalled_dag_reraises_when_asked(self, stalled_state: None):
        il.MemoryDestination.clear()

        with pytest.raises(RunnerError, match="circular dependency or invalid DAG state"):
            await AsyncRunner(reraise=True).run(self._dag())

    async def test_a_task_level_exception_aborts_the_walk(self, monkeypatch: pytest.MonkeyPatch):
        # Operation failures are absorbed by ``_execute_operation``; an
        # exception escaping the task means the scheduling machinery broke.
        il.MemoryDestination.clear()

        async def exploding(self, operation, partition_or_window=None):
            raise MemoryError("scheduler bug")

        monkeypatch.setattr(AsyncRunner, "_execute_operation", exploding)

        result = await AsyncRunner(reraise=False).run(self._dag())

        assert result.status is ExecutionStatus.FAILED
        assert result.executions["solo"].status is ExecutionStatus.CANCELED

    async def test_a_teardown_failure_does_not_mask_the_result(self):
        il.MemoryDestination.clear()

        class BrokenTeardown(AsyncRunner):
            """Runner whose teardown hook raises."""

            def _on_end(self) -> None:
                """Fail after the walk finished.

                Raises:
                    RuntimeError: Always.
                """
                raise RuntimeError("teardown exploded")

        result = await BrokenTeardown().run(self._dag())

        assert result.status is ExecutionStatus.COMPLETED

    async def test_reraise_surfaces_the_original_operation_exception(self):
        il.MemoryDestination.clear()

        @il.asset()
        def boom() -> list[dict[str, Any]]:
            raise KeyError("missing-field")

        dag = il.DAG(boom(id="boom", destinations=[il.MemoryDestination()]))

        with pytest.raises(KeyError, match="missing-field"):
            await AsyncRunner(reraise=True).run(dag)

    async def test_an_operation_still_listed_as_ready_is_not_scheduled_twice(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        il.MemoryDestination.clear()
        release = asyncio.Event()
        scheduled: list[str] = []

        @il.asset()
        async def quick() -> list[dict[str, Any]]:
            return [{"x": 1}]

        @il.asset()
        async def blocking() -> list[dict[str, Any]]:
            await asyncio.wait_for(release.wait(), timeout=5)
            return [{"x": 1}]

        dag = il.DAG(
            quick(id="quick", destinations=[il.MemoryDestination()]),
            blocking(id="blocking", destinations=[il.MemoryDestination()]),
        )

        # Report every non-terminal operation as ready, so ``blocking`` is
        # still listed when the walk loops after ``quick`` lands — exactly
        # what the resubmission guard exists for.
        monkeypatch.setattr(
            "interloper.runner.state.RunState.ready_operations",
            property(lambda self: [o for o in self.dag.operations if not self.executions[o.id].is_terminal]),
        )
        original = AsyncRunner._execute_operation

        async def recording(self, operation, partition_or_window=None):
            scheduled.append(operation.id)
            return await original(self, operation, partition_or_window)

        monkeypatch.setattr(AsyncRunner, "_execute_operation", recording)
        asyncio.get_running_loop().call_later(0.3, release.set)

        result = await AsyncRunner(max_workers=4).run(dag)

        assert sorted(scheduled) == ["blocking", "quick"]
        assert result.status is ExecutionStatus.COMPLETED
