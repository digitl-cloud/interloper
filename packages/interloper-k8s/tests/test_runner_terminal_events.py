"""The k8s host authors a terminal event from the Job outcome (B1).

Regression for orphaned asset executions: when a child Job dies without
streaming its own terminal event, the host must still author one (``emit=True``)
so the asset does not show ``running`` forever. Previously the host marked the
asset terminal with ``emit=False``, trusting a child event that never arrived.
"""

from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import Future
from contextlib import contextmanager

import interloper as il
from interloper.errors import RunnerError
from interloper.events import Event, EventBus, EventType
from interloper.runner.state import RunState, _asset_event_id

from interloper_k8s.runner import KubernetesRunner


class _Asset(il.Asset):
    pass


@contextmanager
def _capture() -> Iterator[list[Event]]:
    captured: list[Event] = []
    EventBus.subscribe(captured.append)
    try:
        yield captured
        EventBus.flush(timeout=5.0)
    finally:
        EventBus.unsubscribe(captured.append)


def _runner_with_asset(asset_id: str, run_id: str) -> tuple[KubernetesRunner, il.Asset]:
    asset = _Asset(id=asset_id)
    runner = KubernetesRunner(image="img", reraise=False, fail_fast=False)
    runner._state = RunState(il.DAG(asset), metadata={"run_id": run_id})
    return runner, asset


def test_host_authors_asset_failed_when_job_fails() -> None:
    """A failed Job future makes the host emit a deterministic ``asset_failed``."""
    runner, asset = _runner_with_asset("asset-1", "run-1")
    future: Future[None] = Future()
    future.set_exception(RunnerError("Job interloper-run-x failed"))

    with _capture() as events:
        runner._handle_completed(future, asset)

    failed = [e for e in events if e.type == EventType.ASSET_FAILED]
    assert len(failed) == 1
    assert failed[0].id == _asset_event_id("run-1", "asset-1", EventType.ASSET_FAILED)


def test_host_authors_asset_completed_when_job_succeeds() -> None:
    """A succeeded Job future makes the host emit a deterministic ``asset_completed``."""
    runner, asset = _runner_with_asset("asset-2", "run-1")
    future: Future[None] = Future()
    future.set_result(None)

    with _capture() as events:
        runner._handle_completed(future, asset)

    completed = [e for e in events if e.type == EventType.ASSET_COMPLETED]
    assert len(completed) == 1
    assert completed[0].id == _asset_event_id("run-1", "asset-2", EventType.ASSET_COMPLETED)


def test_host_does_not_reauthor_when_asset_already_terminal() -> None:
    """If a child terminal already marked the asset terminal, the host stays quiet."""
    runner, asset = _runner_with_asset("asset-3", "run-1")
    runner.state.mark_asset_completed(asset, emit=False)  # as if child reported it
    future: Future[None] = Future()
    future.set_result(None)

    with _capture() as events:
        runner._handle_completed(future, asset)

    assert not [e for e in events if e.type in (EventType.ASSET_COMPLETED, EventType.ASSET_FAILED)]
