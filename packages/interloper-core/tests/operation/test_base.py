"""Tests for the workload/operation contracts (``interloper.operation.base``)."""

from __future__ import annotations

from typing import Any, ClassVar
from uuid import UUID, uuid4

import pytest

import interloper as il
from interloper.component import Component
from interloper.errors import DAGError
from interloper.operation import Operation, OperationContext, OperationResult
from interloper.runner import ExecutionStatus


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

    def test_an_operation_is_a_component_with_real_identity(self):
        operation = _NoopOperation()
        assert isinstance(operation, Component)
        assert operation.key == "noop_operation"
        assert operation.relations == {}
        assert str(UUID(operation.id)) == operation.id
        assert operation.to_spec() is not None

    def test_operation_declares_no_kind_of_its_own(self):
        assert Operation.kind == ""
        assert "destination" in il.KINDS

    def test_enabled_is_a_field(self):
        assert _NoopOperation().enabled is True
        assert _NoopOperation(enabled=False).enabled is False

    def test_no_retry_policy_by_default(self):
        assert _NoopOperation().retry is None

    def test_every_error_is_retryable_by_default(self):
        assert _NoopOperation().retryable(ValueError("boom")) is True

    def test_retryable_can_be_narrowed(self):
        class Picky(Operation):
            async def execute(self, context: OperationContext) -> OperationResult:
                return OperationResult()

            def retryable(self, error: Exception) -> bool:
                return not isinstance(error, ValueError)

        assert Picky().retryable(TypeError()) is True
        assert Picky().retryable(ValueError()) is False

    def test_node_protocol_defaults(self):
        operation = _NoopOperation()
        assert operation.enabled is True
        assert operation.partitioning is None
        assert operation.effective_window(None) is None
        assert type(operation).capture_traceback is True


class TestUpstreamRelations:
    """The graph reads a node's asset-kind relations, and only those."""

    def test_a_node_declaring_no_relation_has_no_upstream(self):
        assert _NoopOperation().upstream_relations() == {}

    def test_only_asset_kind_relations_are_upstreams(self):
        @il.asset(relations={"orders": il.Relation("asset", "shop.orders")})
        def revenue(orders: il.Upstream) -> Any:  # pragma: no cover - not exercised
            return []

        relations = revenue().upstream_relations()

        assert set(relations) == {"orders"}
        assert relations["orders"].keys == ["shop.orders"]

    def test_a_many_valued_relation_is_reported_as_declared(self):
        @il.asset(relations={"campaigns": il.Relation("asset", "*.campaigns", many=True, optional=True)})
        def matches(campaigns: list[il.Upstream]) -> Any:  # pragma: no cover - not exercised
            return []

        relation = matches().upstream_relations()["campaigns"]

        assert (relation.many, relation.optional) == (True, True)


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


class _EffectfulOperation(il.Operation):
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
        assert info.result == OperationResult(config={"token": "NEW"}, state={"refreshed": True})
        assert {event.type for event in events} >= {il.EventType.RUN_STARTED, il.EventType.RUN_COMPLETED}

    async def test_failure_is_curated_and_traceback_suppressed(self):
        operation = _EffectfulOperation(id=str(uuid4()), fail=True)
        events: list[il.Event] = []

        result = await il.AsyncRunner(on_event=events.append, fail_fast=False).run(il.DAG(operation))

        assert result.status is ExecutionStatus.FAILED
        info = result.executions[operation.id]
        assert info.error == "curated"
        assert info.traceback is None
        assert info.result is not None
        assert (info.result.error, info.result.state) == ("curated", {"failed": True})
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
        assert result.executions[asset.id].result == OperationResult()
        effects = result.executions[operation.id].result
        assert effects is not None and effects.config == {"token": "NEW"}


class TestWorkloadValidation:
    def test_dag_rejects_non_workloads(self):
        with pytest.raises(DAGError, match="Expected a workload"):
            il.DAG(object())  # ty: ignore[invalid-argument-type]


class _Flaky(Operation):
    """Fails a set number of times, then succeeds with an effect."""

    failures: int = 0
    retry: il.RetryPolicy | None = il.RetryPolicy(max_attempts=3, delay=0.0, jitter=0.0)

    async def execute(self, context: OperationContext) -> OperationResult:
        if self.failures > 0:
            self.failures -= 1
            raise ConnectionError("transient")
        return OperationResult(state={"done": True})


class _Curated(Operation):
    """Always fails, and curates its failure."""

    capture_traceback: ClassVar[bool] = False

    async def execute(self, context: OperationContext) -> OperationResult:
        raise ValueError("secret in here")

    def failure(self, error: Exception) -> OperationResult:
        return OperationResult(error="curated", state={"failed": True})


class _Attempts:
    """Records what the attempt loop reports, in order."""

    def __init__(self) -> None:
        self.reported: list[str] = []

    def retry(self, error: str) -> None:
        self.reported.append(f"retry: {error}")

    def restart(self) -> None:
        self.reported.append("restart")


async def _execute(operation: Operation, attempts: _Attempts) -> OperationResult:
    return await OperationResult.from_execution(
        operation, OperationContext(), on_retry=attempts.retry, on_restart=attempts.restart
    )


class TestFromExecution:
    async def test_success_returns_the_operations_effects(self):
        attempts = _Attempts()
        result = await _execute(_Flaky(), attempts)
        assert result.succeeded
        assert result.state == {"done": True}
        assert attempts.reported == []

    async def test_a_retryable_failure_is_reported_then_restarted(self):
        attempts = _Attempts()
        result = await _execute(_Flaky(failures=2), attempts)
        assert result.succeeded
        assert attempts.reported == ["retry: ConnectionError: transient", "restart"] * 2

    async def test_an_exhausted_budget_returns_the_failure(self):
        attempts = _Attempts()
        result = await _execute(_Flaky(failures=5), attempts)
        assert not result.succeeded
        assert result.error == "ConnectionError: transient"
        assert attempts.reported == ["retry: ConnectionError: transient", "restart"] * 2
        assert isinstance(result.exception, ConnectionError)
        assert result.traceback is not None and "ConnectionError" in result.traceback

    async def test_an_error_the_operation_declines_is_not_retried(self):
        class Declining(_Flaky):
            def retryable(self, error: Exception) -> bool:
                return False

        attempts = _Attempts()
        result = await _execute(Declining(failures=1), attempts)
        assert not result.succeeded
        assert attempts.reported == []

    async def test_the_failure_hook_curates_and_the_traceback_can_be_withheld(self):
        result = await _execute(_Curated(), _Attempts())
        assert (result.error, result.state, result.traceback) == ("curated", {"failed": True}, None)
        assert isinstance(result.exception, ValueError)

    def test_the_exception_is_not_part_of_equality(self):
        assert OperationResult(error="x", exception=ValueError("a")) == OperationResult(error="x")
