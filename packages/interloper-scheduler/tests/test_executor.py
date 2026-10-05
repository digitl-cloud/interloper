"""Unit tests for ``RunExecutor``: execution telemetry and retry skip logic.

These avoid a live database by faking the store, so they stay pure unit
tests; the DAG itself runs for real through an ``AsyncRunner``.
"""

from __future__ import annotations

import builtins
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.errors import ConflictError, NotFoundError
from interloper.runner.results import ExecutionInfo, ExecutionStatus, RunResult
from interloper.telemetry.tracer import tracer
from interloper_db import RunStatus, Store
from interloper_db.models import Run

from interloper_scheduler.executor import RunExecutor


@pytest.fixture
def hydrated_asset() -> il.Asset:
    il.MemoryDestination.clear()

    @il.asset()
    def solo() -> list[dict[str, Any]]:
        return [{"x": 1}]

    return solo(id=str(uuid4()), destinations=[il.MemoryDestination()])


def test_execute_roots_its_own_trace_linked_to_the_dispatch_span(hydrated_asset: il.Asset, span_exporter: Any) -> None:
    run = _dispatched()
    store = _RecordingStore(hydrated_asset, run=run)
    executor = _executor(store)

    with tracer().start_as_current_span("dispatch") as dispatch:
        assert executor.execute(run.id) is True
    dispatch_context = dispatch.get_span_context()

    assert store.completed == [(run.id, True)]

    spans = {s.name: s for s in span_exporter.get_finished_spans()}
    root = spans["interloper.run.execute"]
    assert root.parent is None
    assert root.context.trace_id != dispatch_context.trace_id
    assert [link.context.span_id for link in root.links] == [dispatch_context.span_id]
    assert root.attributes is not None and root.attributes["interloper.run.id"] == str(run.id)

    # The DAG walk is the run trace's own span — the dispatch trace holds
    # nothing but the launch. (Hydration traces itself in ``Store.load``;
    # this store is a fake, so no such span here.)
    run_span = spans["interloper.runner.run"]
    assert run_span.parent is not None and run_span.parent.span_id == root.context.span_id
    assert run_span.context.trace_id == root.context.trace_id
    assert spans["interloper.operation.execute"].context.trace_id == root.context.trace_id


def test_execute_roots_a_trace_without_any_dispatch_span(hydrated_asset: il.Asset, span_exporter: Any) -> None:
    # Nothing dispatched this (a bare CLI launch): no ambient span, no env
    # context — the run still roots a trace, just with no link.
    run = _dispatched()
    executor = _executor(_RecordingStore(hydrated_asset, run=run))

    assert executor.execute(run.id) is True

    root = {s.name: s for s in span_exporter.get_finished_spans()}["interloper.run.execute"]
    assert root.parent is None
    assert root.links == ()


# -- Retry skip logic ----------------------------------------------------------


class _FakeExecutionStore:
    """Returns canned executions per run_id."""

    def __init__(self, executions: dict[UUID, builtins.list[dict[str, Any]]]) -> None:
        self._executions = executions

    def list(self, org_id: UUID, query: Any, *, run_id: UUID) -> SimpleNamespace:
        return SimpleNamespace(items=[SimpleNamespace(**row) for row in self._executions.get(run_id, [])])


class _RetryStore:
    """Presents the ``executions`` and ``runs`` facets the executor's retry walk reaches for."""

    def __init__(self, executions: dict[UUID, list[dict[str, Any]]], lineage: dict[UUID, UUID | None]) -> None:
        self.executions = _FakeExecutionStore(executions)
        self.runs = SimpleNamespace(get=lambda run_id: SimpleNamespace(retry_of=lineage[run_id]))


def test_succeeded_operations_are_reported() -> None:
    parent_id = uuid4()
    id_a, id_b = uuid4(), uuid4()
    store = _RetryStore(
        {parent_id: [{"component_id": id_a, "status": "success"}, {"component_id": id_b, "status": "failed"}]},
        {parent_id: None},
    )

    executor = RunExecutor(store=store)  # ty: ignore[invalid-argument-type]

    # succeeded → skipped; failed → re-runs
    assert executor._prior_successes(uuid4(), parent_id) == {id_a}


def test_statuses_match_by_component_id_not_key() -> None:
    # A run can span many assets sharing one key (e.g. an ads_stats per
    # account). One account's success must not skip the others' retries.
    parent_id = uuid4()
    id_a, id_b, id_c = uuid4(), uuid4(), uuid4()
    store = _RetryStore(
        {
            parent_id: [
                {"component_id": id_a, "status": "success"},
                {"component_id": id_b, "status": "failed"},
                {"component_id": id_c, "status": "canceled"},
            ]
        },
        {parent_id: None},
    )

    executor = RunExecutor(store=store)  # ty: ignore[invalid-argument-type]

    assert executor._prior_successes(uuid4(), parent_id) == {id_a}


def test_success_carries_forward_across_the_lineage_chain() -> None:
    # attempt1: a succeeded, b failed.  attempt2 (failed-only) re-ran only b,
    # which failed again — so attempt2 has no event for the skipped 'a'.
    # Retrying attempt2 must still skip 'a' by walking back to attempt1.
    root_id = uuid4()
    mid_id = uuid4()
    id_a, id_b = uuid4(), uuid4()
    store = _RetryStore(
        {
            mid_id: [{"component_id": id_b, "status": "failed"}],
            root_id: [{"component_id": id_a, "status": "success"}, {"component_id": id_b, "status": "failed"}],
        },
        {mid_id: root_id, root_id: None},
    )

    executor = RunExecutor(store=store)  # ty: ignore[invalid-argument-type]

    assert executor._prior_successes(uuid4(), mid_id) == {id_a}


def test_closest_ancestor_status_wins() -> None:
    # If an asset failed in the root but succeeded in a later attempt, the
    # most-recent (closest) success should win and the asset should be skipped.
    root_id = uuid4()
    mid_id = uuid4()
    id_a = uuid4()
    store = _RetryStore(
        {
            mid_id: [{"component_id": id_a, "status": "success"}],
            root_id: [{"component_id": id_a, "status": "failed"}],
        },
        {mid_id: root_id, root_id: None},
    )

    executor = RunExecutor(store=store)  # ty: ignore[invalid-argument-type]

    assert executor._prior_successes(uuid4(), mid_id) == {id_a}


def _dispatched(component_id: UUID | None = None) -> Run:
    return Run(id=uuid4(), component_id=component_id or uuid4(), org_id=uuid4(), status=RunStatus.DISPATCHED)


class _RecordingStore:
    """Store stand-in recording completions, failures and applied effects."""

    def __init__(
        self,
        target: Any,
        *,
        run: Run | None = None,
        complete_raises: bool = False,
        already_terminal: bool = False,
    ) -> None:
        """Set up the fake.

        Args:
            target: What ``components.load`` hands back.
            run: What ``runs.start`` hands back; ``None`` reads as a missing run.
            complete_raises: Whether recording the verdict raises, standing in
                for a store that is unreachable while reporting a failure.
            already_terminal: Whether recording the verdict is refused as
                already terminal, standing in for a verdict the reaper wrote first.
        """
        self.completed: list[tuple[UUID, bool]] = []
        self.failures: list[str] = []
        self.merged: list[tuple[UUID, dict[str, Any]]] = []
        self.stamped: list[tuple[UUID, dict[str, Any]]] = []
        self._run = run
        self._complete_raises = complete_raises
        self._already_terminal = already_terminal
        self.components = SimpleNamespace(
            load=lambda _component_id: target,
            merge_config=lambda component_id, config: self.merged.append((component_id, config)),
            stamp_state=lambda component_id, **state: self.stamped.append((component_id, state)),
        )
        self.runs = SimpleNamespace(start=self._start, complete=self._complete, fail=self._fail)
        self.events = SimpleNamespace(save=lambda event, org_id, run_id: None)
        self.executions = SimpleNamespace(list=lambda org_id, query, run_id: SimpleNamespace(items=[]))

    def _start(self, run_id: UUID) -> Run:
        if self._run is None:
            raise NotFoundError(f"Run {run_id} not found")
        return self._run

    def _complete(self, run_id: UUID, success: bool) -> None:
        if self._complete_raises:
            raise RuntimeError("store unreachable")
        if self._already_terminal:
            raise ConflictError(f"Run {run_id} is already failed")
        self.completed.append((run_id, success))

    def _fail(self, run_id: UUID, error: str, *, metadata: dict[str, Any] | None = None) -> None:
        self._complete(run_id, success=False)
        self.failures.append(error)


def _executor(store: _RecordingStore) -> RunExecutor:
    """Build an executor over a recording store.

    Args:
        store: The fake standing in for the real ``Store``.

    Returns:
        The executor, its store cast to the type the constructor declares.
    """
    return RunExecutor(store=cast(Store, store), runner=il.AsyncRunner())


class TestRunLookup:
    """A run that cannot be executed is skipped rather than half-started."""

    def test_a_missing_run_is_skipped(self) -> None:
        store = _RecordingStore(None)

        assert _executor(store).execute(uuid4()) is False
        assert store.completed == []

    def test_a_run_whose_target_was_deleted_fails_with_the_reason(self) -> None:
        run = Run(id=uuid4(), component_id=None, org_id=uuid4(), status=RunStatus.DISPATCHED)
        store = _RecordingStore(None, run=run)

        assert _executor(store).execute(run.id) is False
        assert store.completed == [(run.id, False)]
        assert "deleted" in store.failures[0]


class _NotAWorkload:
    """Hydrated component of a kind that declares no workload."""

    kind = "destination"


class TestWorkloadValidation:
    """A component whose kind declares no workload fails the run."""

    def test_a_non_workload_target_fails_the_run(self) -> None:
        # Silently succeeding would report a run that materialized nothing.
        run = _dispatched()
        store = _RecordingStore(_NotAWorkload(), run=run)

        assert _executor(store).execute(run.id) is False
        assert store.completed == [(run.id, False)]

    def test_the_failure_is_reported_as_an_event(self) -> None:
        run = _dispatched()
        store = _RecordingStore(_NotAWorkload(), run=run)

        _executor(store).execute(run.id)

        (error,) = store.failures
        assert "declares no workload" in error

    def test_a_store_that_cannot_record_the_failure_still_returns_false(
        self
    ) -> None:
        # Otherwise the launcher would read the raise as a crash, not a failed run.
        run = _dispatched()
        store = _RecordingStore(_NotAWorkload(), run=run, complete_raises=True)

        assert _executor(store).execute(run.id) is False


class TestAlreadyTerminal:
    """A run another writer finished first is not this executor's to finish."""

    def test_it_logs_and_reports_failure_without_retrying_the_completion(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        run = _dispatched()

        class EmptyWorkload(il.Source):
            """Source that selects none of its assets."""

        store = _RecordingStore(EmptyWorkload(select=[]), run=run, already_terminal=True)

        with caplog.at_level("WARNING", logger="interloper_scheduler.executor"):
            assert _executor(store).execute(run.id) is False

        assert store.completed == []
        assert [record.levelname for record in caplog.records] == ["WARNING"]
        assert str(run.id) in caplog.records[0].getMessage()


class TestEmptyWorkload:
    """A workload that resolves to no operations succeeds without a DAG run."""

    def test_it_completes_successfully(self) -> None:
        run = _dispatched()

        class EmptyWorkload(il.Source):
            """Source that selects none of its assets."""

        store = _RecordingStore(EmptyWorkload(select=[]), run=run)

        assert _executor(store).execute(run.id) is True
        assert store.completed == [(run.id, True)]


class TestApplyEffects:
    """An operation's returned effects land on its component row."""

    @staticmethod
    def _result(effects: il.OperationResult | None) -> il.RunResult:
        component_id = str(uuid4())
        info = ExecutionInfo(
            component_id=component_id,
            component_key="a",
            status=ExecutionStatus.COMPLETED,
        )
        info.effects = effects
        return RunResult(executions={component_id: info})

    def test_config_effects_are_merged(self, hydrated_asset: il.Asset) -> None:
        store = _RecordingStore(hydrated_asset)
        executor = _executor(store)

        executor._apply_effects(self._result(il.OperationResult(config={"cursor": "abc"})))

        assert [config for _id, config in store.merged] == [{"cursor": "abc"}]
        assert store.stamped == []

    def test_state_effects_are_stamped(self, hydrated_asset: il.Asset) -> None:
        store = _RecordingStore(hydrated_asset)
        executor = _executor(store)

        executor._apply_effects(self._result(il.OperationResult(state={"next_run_at": None})))

        assert [state for _id, state in store.stamped] == [{"next_run_at": None}]
        assert store.merged == []

    def test_a_node_without_effects_is_untouched(self, hydrated_asset: il.Asset) -> None:
        store = _RecordingStore(hydrated_asset)
        executor = _executor(store)

        executor._apply_effects(self._result(None))

        assert store.merged == []
        assert store.stamped == []

    def test_empty_effects_write_nothing(self, hydrated_asset: il.Asset) -> None:
        store = _RecordingStore(hydrated_asset)
        executor = _executor(store)

        executor._apply_effects(self._result(il.OperationResult()))

        assert store.merged == []
        assert store.stamped == []


class _UpstreamFixture(il.Asset):
    """Plain asset fixture standing in for a hydrated upstream."""

    def data(self) -> list[dict[str, Any]]:
        """Return one row.

        Returns:
            One row.
        """
        return [{"x": 1}]


class _DownstreamFixture(il.Asset):
    """Asset fixture whose optional relation accepts any asset."""

    upstream: il.Asset | None = il.Relation("asset", optional=True)

    def data(self) -> list[dict[str, Any]]:
        """Return one row.

        Returns:
            One row.
        """
        return [{"y": 1}]


class TestUpstreamJoinsReadOnly:
    """A bound upstream the run itself does not materialize joins the DAG read-only.

    The hydrator (Task 4) binds the upstream on the hydrated component
    directly; the DAG (phase 1 Task 6) is what joins it as a read-only node.
    The executor no longer walks anything itself, so this exercises the real
    ``RunExecutor.execute`` path, capturing the assembled DAG through
    ``_run_dag`` to inspect what it built.
    """

    def test_a_bound_upstream_is_joined_and_made_non_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        il.MemoryDestination.clear()
        upstream = _UpstreamFixture(id=str(uuid4()), destinations=[il.MemoryDestination()])
        target = _DownstreamFixture(
            id=str(uuid4()), destinations=[il.MemoryDestination()], upstream=upstream
        )

        run = _dispatched()

        built: list[il.DAG] = []
        real_run_dag = RunExecutor._run_dag

        def _capturing_run_dag(self: RunExecutor, dag: il.DAG, *args: Any, **kwargs: Any) -> il.RunResult:
            built.append(dag)
            return real_run_dag(self, dag, *args, **kwargs)

        monkeypatch.setattr(RunExecutor, "_run_dag", _capturing_run_dag)

        store = _RecordingStore(target, run=run)
        executor = _executor(store)

        assert executor.execute(run.id) is True

        (dag,) = built
        assert dag.operation_map[upstream.id].enabled is False
        assert dag.predecessors[target.id] == [upstream.id]


class TestRetrySkipsPriorSuccesses:
    """A failed-only retry reads earlier successes instead of recomputing them."""

    def test_a_previously_successful_node_is_made_non_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        il.MemoryDestination.clear()

        @il.asset()
        def solo() -> list[dict[str, Any]]:
            return [{"x": 1}]

        component_id = uuid4()
        target = solo(id=str(component_id), destinations=[il.MemoryDestination()])
        retry_of = uuid4()
        run = Run(
            id=uuid4(),
            component_id=uuid4(),
            org_id=uuid4(),
            status=RunStatus.DISPATCHED,
            retry_of=retry_of,
            retry_scope="failed",
        )
        store = _RecordingStore(target, run=run)
        executor = _executor(store)
        monkeypatch.setattr(executor, "_prior_successes", lambda _org_id, _retry_of: {component_id})

        assert executor.execute(run.id) is True
        assert target.enabled is False

    def test_a_whole_run_retry_recomputes_everything(self, monkeypatch: pytest.MonkeyPatch) -> None:
        il.MemoryDestination.clear()

        @il.asset()
        def solo() -> list[dict[str, Any]]:
            return [{"x": 1}]

        target = solo(id=str(uuid4()), destinations=[il.MemoryDestination()])
        run = Run(
            id=uuid4(),
            component_id=uuid4(),
            org_id=uuid4(),
            status=RunStatus.DISPATCHED,
            retry_of=uuid4(),
            retry_scope="all",
        )
        store = _RecordingStore(target, run=run)
        executor = _executor(store)
        monkeypatch.setattr(
            executor,
            "_prior_successes",
            lambda _org_id, _retry_of: pytest.fail("scope 'all' must not consult the lineage"),
        )

        assert executor.execute(run.id) is True
        assert target.enabled is True
