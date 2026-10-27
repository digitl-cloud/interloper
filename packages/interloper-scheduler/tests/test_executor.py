"""Unit tests for ``RunExecutor``: execution telemetry and retry skip logic.

These avoid a live database by faking the store, so they stay pure unit
tests; the DAG itself runs for real through an ``AsyncRunner``.
"""

from __future__ import annotations

import builtins
import threading
from types import SimpleNamespace
from typing import Any, ClassVar, cast
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.settings import ReaperSettings
from interloper.telemetry.tracer import tracer
from interloper_db import ConflictError, NotFoundError, RunStatus, Store
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

    assert store.events[-1].type is il.EventType.RUN_COMPLETED

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
        self.runs = SimpleNamespace(get=lambda run_id, org_id=None: SimpleNamespace(retry_of=lineage[run_id]))


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
    """Store stand-in recording every batch ingested and every failure."""

    def __init__(
        self,
        target: Any,
        *,
        run: Run | None = None,
        running: bool = True,
        fail_raises: type[Exception] | None = None,
    ) -> None:
        """Set up the fake.

        Args:
            target: What ``components.load`` hands back; ``None`` raises as a missing component.
            run: What ``runs.get`` hands back; ``None`` reads as a missing run.
            running: What every ingest answers once the run started.
            fail_raises: An exception ``runs.fail`` raises, standing in for an
                unreachable store or a verdict another writer recorded first.
        """
        self.ingested: list[tuple[list[il.Event], Any]] = []
        self.failures: list[tuple[UUID, str]] = []
        self._target = target
        self._run = run
        self._running = running
        self._fail_raises = fail_raises
        self.components = SimpleNamespace(load=self._load)
        self.completed: list[UUID] = []
        self.runs = SimpleNamespace(get=self._get, ingest=self._ingest, fail=self._fail, complete=self.completed.append)
        self.executions = SimpleNamespace(list=lambda org_id, query, run_id: SimpleNamespace(items=[]))

    @property
    def events(self) -> list[il.Event]:
        """Every event ingested, in order."""
        return [event for batch, _ in self.ingested for event in batch]

    def _load(self, component_id: UUID) -> Any:
        if self._target is None:
            raise NotFoundError(f"Component {component_id} not found")
        return self._target

    def _get(self, run_id: UUID, *, org_id: UUID | None = None) -> Run:
        if self._run is None:
            raise NotFoundError(f"Run {run_id} not found")
        return self._run

    def _ingest(self, run_id: UUID, events: list[il.Event], effects: Any = None) -> bool:
        self.ingested.append((list(events), effects))
        return self._running

    def _fail(self, run_id: UUID, error: str, *, metadata: dict[str, Any] | None = None) -> None:
        if self._fail_raises is not None:
            raise self._fail_raises(f"Run {run_id} cannot be failed")
        self.failures.append((run_id, error))


def _executor(store: _RecordingStore, **options: Any) -> RunExecutor:
    """Build an executor over a recording store.

    Args:
        store: The fake standing in for the real ``Store``.
        **options: ``reaper`` and ``on_lost``.

    Returns:
        The executor, its store cast to the type the constructor declares.
    """
    options.setdefault("reaper", ReaperSettings.model_construct(heartbeat_interval=10, heartbeat_timeout=90))
    return RunExecutor(store=cast(Store, store), runner=il.AsyncRunner(), **options)


class _Rotating(il.Operation):
    """An operation that rotates a credential, as a renewal does."""

    kind: ClassVar[str] = "rotating_fixture"

    async def execute(self, context: il.OperationContext) -> il.OperationResult:
        """Rotate the credential.

        Args:
            context: The execution context, unused.

        Returns:
            The rotated credential as a config effect.
        """
        return il.OperationResult(config={"refresh_token": "rotated"})


class TestReporting:
    """Everything a run says reaches the store through one path: ingest."""

    def test_every_event_reaches_ingest_from_start_to_verdict(self, hydrated_asset: il.Asset) -> None:
        run = _dispatched()
        store = _RecordingStore(hydrated_asset, run=run)

        assert _executor(store).execute(run.id) is True

        assert store.events[0].type is il.EventType.RUN_STARTED
        assert store.events[-1].type is il.EventType.RUN_COMPLETED
        assert store.failures == []

    def test_a_failed_operation_fails_the_run_through_its_own_run_failed(self) -> None:
        @il.asset()
        def broken() -> list[dict[str, Any]]:
            raise RuntimeError("vendor 500")

        run = _dispatched()
        store = _RecordingStore(broken(id=str(uuid4()), destinations=[il.MemoryDestination()]), run=run)

        assert _executor(store).execute(run.id) is False

        assert store.events[-1].type is il.EventType.RUN_FAILED
        assert store.failures == []

    def test_the_effects_ride_with_the_last_batch(self) -> None:
        operation = _Rotating(id=str(uuid4()))
        run = _dispatched()
        store = _RecordingStore(operation, run=run)

        assert _executor(store).execute(run.id) is True

        (ending_batch,) = [
            (batch, effects)
            for batch, effects in store.ingested
            if any(e.type is il.EventType.RUN_COMPLETED for e in batch)
        ]
        assert ending_batch[1] == {operation.id: il.Effect(config={"refresh_token": "rotated"})}
        assert all(effects is None for batch, effects in store.ingested if batch is not ending_batch[0])

    def test_a_run_ended_elsewhere_is_handed_to_on_lost(self, hydrated_asset: il.Asset) -> None:
        run = _dispatched()
        store = _RecordingStore(hydrated_asset, run=run, running=False)
        lost = threading.Event()

        assert _executor(store, on_lost=lost.set).execute(run.id) is False
        assert lost.is_set()


class TestRunLookup:
    """A run that cannot be executed is skipped, never half-started."""

    def test_a_missing_run_is_skipped(self) -> None:
        store = _RecordingStore(None)

        assert _executor(store).execute(uuid4()) is False
        assert store.ingested == [] and store.failures == []

    def test_a_run_no_longer_dispatched_is_skipped(self, hydrated_asset: il.Asset) -> None:
        run = _dispatched()
        run.status = RunStatus.CANCELED
        store = _RecordingStore(hydrated_asset, run=run)

        assert _executor(store).execute(run.id) is False
        assert store.ingested == [] and store.failures == []


class _NotAWorkload:
    """Hydrated component of a kind that declares no workload."""

    kind = "destination"


class TestAssembly:
    """A run that cannot be assembled is failed with the reason, before anything executes."""

    def test_a_deleted_target_fails_the_run(self) -> None:
        run = Run(id=uuid4(), component_id=None, org_id=uuid4(), status=RunStatus.DISPATCHED)
        store = _RecordingStore(None, run=run)

        assert _executor(store).execute(run.id) is False
        ((failed_id, error),) = store.failures
        assert failed_id == run.id and "deleted" in error
        assert store.ingested == []

    def test_a_non_workload_target_fails_the_run(self) -> None:
        run = _dispatched()
        store = _RecordingStore(_NotAWorkload(), run=run)

        assert _executor(store).execute(run.id) is False
        ((_, error),) = store.failures
        assert "declares no workload" in error

    def test_a_store_that_cannot_record_the_failure_still_returns_false(self) -> None:
        run = _dispatched()
        store = _RecordingStore(_NotAWorkload(), run=run, fail_raises=RuntimeError)

        assert _executor(store).execute(run.id) is False

    def test_a_failure_another_writer_recorded_first_is_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        run = _dispatched()
        store = _RecordingStore(_NotAWorkload(), run=run, fail_raises=ConflictError)

        with caplog.at_level("WARNING", logger="interloper_scheduler.executor"):
            assert _executor(store).execute(run.id) is False

        assert "completed by another writer first" in caplog.text

    def test_a_window_the_operations_refuse_fails_the_run_with_the_reason(self) -> None:
        @il.asset(partitioning=il.TimePartitionConfig(column="date"))
        def daily() -> list[dict[str, Any]]:
            return []

        run = _dispatched()
        run.partition_key = "2026-10"
        store = _RecordingStore(daily(id=str(uuid4()), destinations=[il.MemoryDestination()]), run=run)

        assert _executor(store).execute(run.id) is False
        ((_, error),) = store.failures
        assert "window" in error.lower()
        assert store.events == []


class TestEmptyWorkload:
    """A workload that resolves to no operations still reports a successful run."""

    def test_it_completes_successfully(self) -> None:
        class EmptyWorkload(il.Source):
            """Source that selects none of its assets."""

        run = _dispatched()
        store = _RecordingStore(EmptyWorkload(), run=run)

        assert _executor(store).execute(run.id) is True
        assert store.completed == [run.id]


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
    ``_assemble`` to inspect what it built.
    """

    def test_a_bound_upstream_is_joined_and_made_non_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        il.MemoryDestination.clear()
        upstream = _UpstreamFixture(id=str(uuid4()), destinations=[il.MemoryDestination()])
        target = _DownstreamFixture(id=str(uuid4()), destinations=[il.MemoryDestination()], upstream=upstream)

        run = _dispatched()

        built: list[il.DAG] = []
        real_assemble = RunExecutor._assemble

        def _capturing_assemble(self: RunExecutor, db_run: Run) -> il.DAG | None:
            dag = real_assemble(self, db_run)
            assert dag is not None
            built.append(dag)
            return dag

        monkeypatch.setattr(RunExecutor, "_assemble", _capturing_assemble)

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
