"""Unit tests for ``RunExecutor`` execution telemetry.

These avoid a live database by faking the store and session, so they stay
pure unit tests; the DAG itself runs for real through an ``AsyncRunner``.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.telemetry.tracer import tracer
from interloper_db.models import Run

from interloper_scheduler import executor as executor_module
from interloper_scheduler.executor import RunExecutor


class _FakeSession:
    """Context-manager session serving one run row; writes are no-ops."""

    def __init__(self, run: Run) -> None:
        self._run = run

    def __enter__(self) -> _FakeSession:
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
        return SimpleNamespace(all=lambda: [])


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
    executor = RunExecutor(store=store, runner=il.AsyncRunner())  # ty: ignore[invalid-argument-type]

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
