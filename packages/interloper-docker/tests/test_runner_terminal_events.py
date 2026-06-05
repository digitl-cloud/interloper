"""The docker host authors a terminal event from the container outcome (B1).

Mirrors the k8s regression: a container that dies without streaming its own
terminal event must still leave the asset terminal, because the host now
authors the terminal itself (``emit=True``) instead of trusting the child.
"""

from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import Future
from contextlib import contextmanager

import interloper as il
from interloper.errors import RunnerError
from interloper.events import Event, EventBus, EventType
from interloper.runner.state import RunState, _asset_event_id

from interloper_docker.runner import DockerRunner


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


def _runner_with_asset(asset_id: str, run_id: str) -> tuple[DockerRunner, il.Asset]:
    asset = _Asset(id=asset_id)
    runner = DockerRunner(image="img", reraise=False, fail_fast=False)
    runner._state = RunState(il.DAG(asset), metadata={"run_id": run_id})
    return runner, asset


def test_host_authors_asset_failed_when_container_fails() -> None:
    """A failed container future makes the host emit a deterministic ``asset_failed``."""
    runner, asset = _runner_with_asset("asset-1", "run-1")
    future: Future[None] = Future()
    future.set_exception(RunnerError("container exited 137"))

    with _capture() as events:
        runner._handle_completed(future, asset)

    failed = [e for e in events if e.type == EventType.ASSET_FAILED]
    assert len(failed) == 1
    assert failed[0].id == _asset_event_id("run-1", "asset-1", EventType.ASSET_FAILED)


def test_host_authors_asset_completed_when_container_succeeds() -> None:
    """A succeeded container future makes the host emit a deterministic ``asset_completed``."""
    runner, asset = _runner_with_asset("asset-2", "run-1")
    future: Future[None] = Future()
    future.set_result(None)

    with _capture() as events:
        runner._handle_completed(future, asset)

    completed = [e for e in events if e.type == EventType.ASSET_COMPLETED]
    assert len(completed) == 1
    assert completed[0].id == _asset_event_id("run-1", "asset-2", EventType.ASSET_COMPLETED)
