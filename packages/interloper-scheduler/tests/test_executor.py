"""Unit tests for ``RunExecutor``: execution telemetry and retry skip logic.

These avoid a live database by faking the store and session, so they stay
pure unit tests; the DAG itself runs for real through an ``AsyncRunner``.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.runner.results import ExecutionInfo, ExecutionStatus, RunResult
from interloper.telemetry.tracer import tracer
from interloper_db import Store
from interloper_db.models import Run
from typing_extensions import Self

from interloper_scheduler import executor as executor_module
from interloper_scheduler.executor import RunExecutor


class _FakeSession:
    """Context-manager session serving one run row; writes are no-ops."""

    def __init__(self, run: Run | None) -> None:
        self._run = run

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def get(self, model: Any, _id: UUID) -> Any:
        return self._run if model is Run else None

    def add(self, _obj: Any) -> None:
        pass

    def commit(self) -> None:
        pass

    def exec(self, _statement: Any) -> Any:
        return SimpleNamespace(all=list)


class _FakeStore:
    """Serves one hydrated asset and records completion, facet by facet."""

    engine = None  # the fake Session ignores it

    def __init__(self, asset: il.Asset) -> None:
        self._asset = asset
        self.completed: list[tuple[UUID, bool]] = []
        self.components = SimpleNamespace(load=lambda _component_id: self._asset)
        self.runs = SimpleNamespace(
            complete=lambda run_id, success: self.completed.append((run_id, success)),
            save_event=lambda event, **_kwargs: None,
        )


@pytest.fixture
def hydrated_asset() -> il.Asset:
    il.MemoryDestination.clear()

    @il.asset()
    def solo() -> list[dict[str, Any]]:
        return [{"x": 1}]

    return solo(id=str(uuid4()), destinations=[il.MemoryDestination()])


def test_execute_roots_its_own_trace_linked_to_the_dispatch_span(
    monkeypatch: pytest.MonkeyPatch, hydrated_asset: il.Asset, span_exporter: Any
) -> None:
    run = Run(id=uuid4(), component_id=uuid4(), org_id=uuid4(), status="dispatched")
    monkeypatch.setattr(executor_module, "Session", lambda _engine: _FakeSession(run))
    store = _FakeStore(hydrated_asset)
    executor = _executor(store)  # ty: ignore[invalid-argument-type]

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


def test_execute_roots_a_trace_without_any_dispatch_span(
    monkeypatch: pytest.MonkeyPatch, hydrated_asset: il.Asset, span_exporter: Any
) -> None:
    # Nothing dispatched this (a bare CLI launch): no ambient span, no env
    # context — the run still roots a trace, just with no link.
    run = Run(id=uuid4(), component_id=uuid4(), org_id=uuid4(), status="dispatched")
    monkeypatch.setattr(executor_module, "Session", lambda _engine: _FakeSession(run))
    executor = RunExecutor(store=_FakeStore(hydrated_asset), runner=il.AsyncRunner())  # ty: ignore[invalid-argument-type]

    assert executor.execute(run.id) is True

    root = {s.name: s for s in span_exporter.get_finished_spans()}["interloper.run.execute"]
    assert root.parent is None
    assert root.links == ()


# -- Retry skip logic ----------------------------------------------------------


class _FakeEventStore:
    """Returns canned executions per run_id."""

    def __init__(self, executions: dict[UUID, list[dict[str, Any]]]) -> None:
        self._executions = executions

    def list_executions(self, run_id: UUID) -> list[SimpleNamespace]:
        return [SimpleNamespace(**row) for row in self._executions.get(run_id, [])]


class _RetryStore:
    """Presents the ``events`` facet the executor's retry walk reaches for."""

    engine = None  # the fake Session ignores it

    def __init__(self, executions: dict[UUID, list[dict[str, Any]]]) -> None:
        self.events = _FakeEventStore(executions)


class _LineageSession:
    """Context-manager session whose ``get`` resolves runs by id."""

    def __init__(self, runs: dict[UUID, Any]) -> None:
        self._runs = runs

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def get(self, _model: Any, run_id: UUID) -> Any:
        return self._runs.get(run_id)


def _patch_session(monkeypatch: pytest.MonkeyPatch, runs: dict[UUID, Any]) -> None:
    monkeypatch.setattr(executor_module, "Session", lambda _engine: _LineageSession(runs))


def test_succeeded_operations_are_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    parent_id = uuid4()
    id_a, id_b = uuid4(), uuid4()
    store = _RetryStore(
        {parent_id: [{"component_id": id_a, "status": "success"}, {"component_id": id_b, "status": "failed"}]}
    )
    _patch_session(monkeypatch, {parent_id: SimpleNamespace(retry_of=None)})

    executor = RunExecutor(store=store)  # ty: ignore[invalid-argument-type]

    # succeeded → skipped; failed → re-runs
    assert executor._prior_successes(parent_id) == {id_a}


def test_statuses_match_by_component_id_not_key(monkeypatch: pytest.MonkeyPatch) -> None:
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
        }
    )
    _patch_session(monkeypatch, {parent_id: SimpleNamespace(retry_of=None)})

    executor = RunExecutor(store=store)  # ty: ignore[invalid-argument-type]

    assert executor._prior_successes(parent_id) == {id_a}


def test_success_carries_forward_across_the_lineage_chain(monkeypatch: pytest.MonkeyPatch) -> None:
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
        }
    )
    _patch_session(
        monkeypatch,
        {mid_id: SimpleNamespace(retry_of=root_id), root_id: SimpleNamespace(retry_of=None)},
    )

    executor = RunExecutor(store=store)  # ty: ignore[invalid-argument-type]

    assert executor._prior_successes(mid_id) == {id_a}


def test_closest_ancestor_status_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    # If an asset failed in the root but succeeded in a later attempt, the
    # most-recent (closest) success should win and the asset should be skipped.
    root_id = uuid4()
    mid_id = uuid4()
    id_a = uuid4()
    store = _RetryStore(
        {
            mid_id: [{"component_id": id_a, "status": "success"}],
            root_id: [{"component_id": id_a, "status": "failed"}],
        }
    )
    _patch_session(
        monkeypatch,
        {mid_id: SimpleNamespace(retry_of=root_id), root_id: SimpleNamespace(retry_of=None)},
    )

    executor = RunExecutor(store=store)  # ty: ignore[invalid-argument-type]

    assert executor._prior_successes(mid_id) == {id_a}


class _RecordingStore:
    """Store stand-in recording completions, saved events and applied effects."""

    engine = None  # the fake Session ignores it

    def __init__(self, target: Any, *, complete_raises: bool = False) -> None:
        """Set up the fake.

        Args:
            target: What ``components.load`` hands back.
            complete_raises: Whether ``runs.complete`` raises, standing in for
                a store that is unreachable while reporting a failure.
        """
        self.completed: list[tuple[UUID, bool]] = []
        self.saved_events: list[il.Event] = []
        self.merged: list[tuple[UUID, dict[str, Any]]] = []
        self.stamped: list[tuple[UUID, dict[str, Any]]] = []
        self._complete_raises = complete_raises
        self.components = SimpleNamespace(
            load=lambda _component_id: target,
            merge_config=lambda component_id, config: self.merged.append((component_id, config)),
            stamp_state=lambda component_id, **state: self.stamped.append((component_id, state)),
        )
        self.runs = SimpleNamespace(complete=self._complete)
        self.events = SimpleNamespace(
            save=lambda event, org_id, run_id: self.saved_events.append(event),
            list_executions=lambda _run_id: [],
        )

    def _complete(self, run_id: UUID, success: bool) -> None:
        if self._complete_raises:
            raise RuntimeError("store unreachable")
        self.completed.append((run_id, success))


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

    def test_a_missing_run_is_skipped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(executor_module, "Session", lambda _engine: _FakeSession(None))
        store = _RecordingStore(None)

        assert _executor(store).execute(uuid4()) is False
        assert store.completed == []

    def test_a_run_without_a_component_is_skipped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        run = Run(id=uuid4(), component_id=None, org_id=uuid4(), status="dispatched")
        monkeypatch.setattr(executor_module, "Session", lambda _engine: _FakeSession(run))
        store = _RecordingStore(None)

        assert _executor(store).execute(run.id) is False
        assert store.completed == []


class _NotAWorkload:
    """Hydrated component of a kind that declares no workload."""

    kind = "destination"


class TestWorkloadValidation:
    """A component whose kind declares no workload fails the run."""

    def test_a_non_workload_target_fails_the_run(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Silently succeeding would report a run that materialized nothing.
        run = Run(id=uuid4(), component_id=uuid4(), org_id=uuid4(), status="dispatched")
        monkeypatch.setattr(executor_module, "Session", lambda _engine: _FakeSession(run))
        store = _RecordingStore(_NotAWorkload())

        assert _executor(store).execute(run.id) is False
        assert store.completed == [(run.id, False)]

    def test_the_failure_is_reported_as_an_event(self, monkeypatch: pytest.MonkeyPatch) -> None:
        run = Run(id=uuid4(), component_id=uuid4(), org_id=uuid4(), status="dispatched")
        monkeypatch.setattr(executor_module, "Session", lambda _engine: _FakeSession(run))
        store = _RecordingStore(_NotAWorkload())

        _executor(store).execute(run.id)

        (event,) = store.saved_events
        assert event.type is il.EventType.RUN_FAILED
        assert "declares no workload" in event.metadata["error"]

    def test_a_store_that_cannot_record_the_failure_still_returns_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Otherwise the launcher would read the raise as a crash, not a failed run.
        run = Run(id=uuid4(), component_id=uuid4(), org_id=uuid4(), status="dispatched")
        monkeypatch.setattr(executor_module, "Session", lambda _engine: _FakeSession(run))
        store = _RecordingStore(_NotAWorkload(), complete_raises=True)

        assert _executor(store).execute(run.id) is False


class TestEmptyWorkload:
    """A workload that resolves to no operations succeeds without a DAG run."""

    def test_it_completes_successfully(self, monkeypatch: pytest.MonkeyPatch) -> None:
        run = Run(id=uuid4(), component_id=uuid4(), org_id=uuid4(), status="dispatched")
        monkeypatch.setattr(executor_module, "Session", lambda _engine: _FakeSession(run))

        class EmptyWorkload(il.Source):
            """Source that selects none of its assets."""

        store = _RecordingStore(EmptyWorkload(select=[]))

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


class TestResolveUpstream:
    """Upstream dependencies join the graph as read-only context."""

    @staticmethod
    def _asset(dependencies: dict[str, str] | None = None) -> il.Asset:
        @il.asset()
        def node() -> list[dict[str, Any]]:
            return [{"x": 1}]

        instance = node(id=str(uuid4()), destinations=[il.MemoryDestination()])
        if dependencies:
            instance.dependencies = dependencies
        return instance

    def test_a_dependency_is_loaded_and_made_non_materializable(self) -> None:
        # Joined upstream nodes are read from their destinations, never recomputed.
        upstream = self._asset()
        target = self._asset({"up": upstream.id})
        store = _RecordingStore(None)
        store.components = SimpleNamespace(load=lambda _id: upstream)
        executor = _executor(store)
        operations: list[il.Operation] = [target]

        executor._resolve_upstream(operations)

        assert operations == [target, upstream]
        assert upstream.materializable is False

    def test_the_walk_is_transitive(self) -> None:
        grandparent = self._asset()
        parent = self._asset({"up": grandparent.id})
        target = self._asset({"up": parent.id})
        by_id = {parent.id: parent, grandparent.id: grandparent}
        store = _RecordingStore(None)
        store.components = SimpleNamespace(load=lambda component_id: by_id[str(component_id)])
        executor = _executor(store)
        operations: list[il.Operation] = [target]

        executor._resolve_upstream(operations)

        assert [operation.id for operation in operations] == [target.id, parent.id, grandparent.id]

    def test_a_shared_dependency_is_loaded_once(self) -> None:
        shared = self._asset()
        first = self._asset({"up": shared.id})
        second = self._asset({"up": shared.id})
        loads: list[str] = []
        store = _RecordingStore(None)

        def load(component_id: Any) -> il.Asset:
            loads.append(str(component_id))
            return shared

        store.components = SimpleNamespace(load=load)
        executor = _executor(store)
        operations: list[il.Operation] = [first, second]

        executor._resolve_upstream(operations)

        assert loads == [shared.id]

    def test_a_dependency_already_in_the_graph_is_not_reloaded(self) -> None:
        upstream = self._asset()
        target = self._asset({"up": upstream.id})
        store = _RecordingStore(None)
        store.components = SimpleNamespace(
            load=lambda _id: pytest.fail("an in-graph dependency must not be reloaded")
        )
        executor = _executor(store)
        operations: list[il.Operation] = [target, upstream]

        executor._resolve_upstream(operations)

        assert len(operations) == 2

    def test_no_dependencies_is_a_no_op(self) -> None:
        target = self._asset()
        store = _RecordingStore(None)
        store.components = SimpleNamespace(load=lambda _id: pytest.fail("nothing to load"))
        executor = _executor(store)
        operations: list[il.Operation] = [target]

        executor._resolve_upstream(operations)

        assert operations == [target]


class TestRetrySkipsPriorSuccesses:
    """A failed-only retry reads earlier successes instead of recomputing them."""

    def test_a_previously_successful_node_is_made_non_materializable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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
            status="dispatched",
            retry_of=retry_of,
            retry_scope="failed",
        )
        monkeypatch.setattr(executor_module, "Session", lambda _engine: _FakeSession(run))
        store = _RecordingStore(target)
        executor = _executor(store)
        monkeypatch.setattr(executor, "_prior_successes", lambda _retry_of: {component_id})

        assert executor.execute(run.id) is True
        assert target.materializable is False

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
            status="dispatched",
            retry_of=uuid4(),
            retry_scope="all",
        )
        monkeypatch.setattr(executor_module, "Session", lambda _engine: _FakeSession(run))
        store = _RecordingStore(target)
        executor = _executor(store)
        monkeypatch.setattr(
            executor, "_prior_successes", lambda _retry_of: pytest.fail("scope 'all' must not consult the lineage")
        )

        assert executor.execute(run.id) is True
        assert target.materializable is True
