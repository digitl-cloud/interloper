"""Tests for the workload/operation contracts (``interloper.operation.base``)."""

from __future__ import annotations

from typing import Any, ClassVar
from uuid import uuid4

import pytest

import interloper as il
from interloper.errors import DAGError
from interloper.operation import Operation, OperationContext, OperationResult
from interloper.runner.results import ExecutionStatus


class _NoopOperation(Operation):
    """Minimal concrete operation for contract-level assertions."""

    async def execute(self, context: OperationContext) -> OperationResult:
        return OperationResult()


class TestContract:
    def test_workloads_are_billable_by_default(self):
        assert il.Workload.billable is True
        assert _NoopOperation.billable is True

    def test_an_operation_is_its_own_workload(self):
        operation = _NoopOperation()
        assert operation.operations() == [operation]

    def test_result_defaults_to_effectless(self):
        result = OperationResult()
        assert result.error is None
        assert result.config == {}
        assert result.state == {}

    def test_default_failure_formats_the_error(self):
        failed = _NoopOperation().failure(ValueError("boom"))
        assert failed.error == "ValueError: boom"
        assert failed.state == {}

    def test_node_protocol_defaults(self):
        operation = _NoopOperation()
        assert operation.materializable is True
        assert operation.dependencies == {}
        assert operation.partitioning is None
        assert operation.effective_partition(None) is None
        assert type(operation).capture_traceback is True


class TestKindWiring:
    """The component kinds split into workloads, operations, and neither."""

    def test_grouping_anchors_are_workloads_not_operations(self):
        for anchor in (il.Source, il.Job):
            assert issubclass(anchor, il.Workload)
            assert not issubclass(anchor, il.Operation)

    def test_asset_is_an_operation(self):
        assert issubclass(il.Asset, il.Operation)

    def test_non_workload_anchors(self):
        for kind in ("destination", "config", "resource", "hook"):
            assert not issubclass(il.KINDS[kind], il.Workload)


class _EffectfulOperation(il.Component, il.Operation):
    """Test-only operation kind carrying effects and a curated failure."""

    capture_traceback: ClassVar[bool] = False
    fail: bool = False

    async def execute(self, context: OperationContext) -> OperationResult:
        """Succeed with effects, or raise when asked to.

        Args:
            context: The facts this execution is scoped to, unused.

        Returns:
            A result carrying one config and one state effect.

        Raises:
            RuntimeError: When constructed with ``fail=True``.
        """
        if self.fail:
            raise RuntimeError("provider exploded: secret=hunter2")
        return OperationResult(config={"token": "NEW"}, state={"refreshed": True})

    def failure(self, error: Exception) -> OperationResult:
        """Curate the failure.

        Args:
            error: The exception ``execute`` raised.

        Returns:
            A result with a curated message and a state effect.
        """
        return OperationResult(error="curated", state={"failed": True})


class TestRunnerDrivesOperations:
    """The runner executes any operation, not just assets."""

    async def test_effects_land_on_the_execution_info(self):
        operation = _EffectfulOperation(id=str(uuid4()))
        events: list[il.Event] = []

        result = await il.AsyncRunner(on_event=events.append).run(il.DAG(operation))

        assert result.status is ExecutionStatus.COMPLETED
        info = result.executions[operation.id]
        assert info.effects == OperationResult(config={"token": "NEW"}, state={"refreshed": True})
        assert {event.type for event in events} >= {il.EventType.RUN_STARTED, il.EventType.RUN_COMPLETED}

    async def test_failure_is_curated_and_traceback_suppressed(self):
        operation = _EffectfulOperation(id=str(uuid4()), fail=True)
        events: list[il.Event] = []

        result = await il.AsyncRunner(on_event=events.append, fail_fast=False).run(il.DAG(operation))

        assert result.status is ExecutionStatus.FAILED
        info = result.executions[operation.id]
        assert info.error == "curated"
        assert info.traceback is None
        assert info.effects == OperationResult(error="curated", state={"failed": True})
        assert all("hunter2" not in str(event.metadata) for event in events)

    async def test_fail_fast_abort_defers_detail_to_the_operation_event(self):
        operation = _EffectfulOperation(id=str(uuid4()), fail=True)
        events: list[il.Event] = []

        result = await il.AsyncRunner(on_event=events.append, fail_fast=True).run(il.DAG(operation))

        assert result.status is ExecutionStatus.FAILED
        run_failed = next(event for event in events if event.type is il.EventType.RUN_FAILED)
        assert run_failed.metadata.get("error") is None
        operation_failed = next(event for event in events if event.type is il.EventType.OPERATION_FAILED)
        assert operation_failed.metadata["error"] == "curated"
        assert all("hunter2" not in str(event.metadata) for event in events)

    async def test_reraise_surfaces_the_original_exception_after_finalizing(self):
        operation = _EffectfulOperation(id=str(uuid4()), fail=True)
        events: list[il.Event] = []

        with pytest.raises(RuntimeError, match="hunter2"):
            await il.AsyncRunner(on_event=events.append, reraise=True).run(il.DAG(operation))

        assert any(event.type is il.EventType.RUN_FAILED for event in events)

    async def test_mixed_graph_runs_assets_and_operations_together(self):
        @il.asset()
        def solo() -> list[dict[str, Any]]:
            return [{"x": 1}]

        il.MemoryDestination.clear()
        asset = solo(id=str(uuid4()), destinations=[il.MemoryDestination()])
        operation = _EffectfulOperation(id=str(uuid4()))

        result = await il.AsyncRunner().run(il.DAG(asset, operation))

        assert result.status is ExecutionStatus.COMPLETED
        assert result.executions[asset.id].effects == OperationResult()
        effects = result.executions[operation.id].effects
        assert effects is not None and effects.config == {"token": "NEW"}


class TestWorkloadValidation:
    def test_dag_rejects_non_workloads(self):
        with pytest.raises(DAGError, match="Expected a workload"):
            il.DAG(object())  # ty: ignore[invalid-argument-type]
