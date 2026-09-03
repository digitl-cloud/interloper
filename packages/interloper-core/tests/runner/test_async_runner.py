"""Tests for ``interloper.runner.async_runner``."""

from __future__ import annotations

import asyncio
from typing import Any

import interloper as il
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
