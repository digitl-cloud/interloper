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
from interloper.runner.state import RunState

from interloper_k8s.runner import KubernetesRunner


class _Asset(il.Asset):
    pass


@contextmanager
def _capture() -> Iterator[list[Event]]:
    captured: list[Event] = []
    # Drain events still in flight from earlier tests (e.g. ones that emit
    # without flushing) so they don't leak into this capture.
    EventBus.flush(timeout=5.0)
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

    failed = [e for e in events if e.type == EventType.OPERATION_FAILED]
    assert len(failed) == 1
    assert failed[0].id == RunState._operation_event_id("run-1", "asset-1", EventType.OPERATION_FAILED)


def test_host_authors_asset_completed_when_job_succeeds() -> None:
    """A succeeded Job future makes the host emit a deterministic ``asset_completed``."""
    runner, asset = _runner_with_asset("asset-2", "run-1")
    future: Future[None] = Future()
    future.set_result(None)

    with _capture() as events:
        runner._handle_completed(future, asset)

    completed = [e for e in events if e.type == EventType.OPERATION_COMPLETED]
    assert len(completed) == 1
    assert completed[0].id == RunState._operation_event_id("run-1", "asset-2", EventType.OPERATION_COMPLETED)


def test_host_does_not_reauthor_when_asset_already_terminal() -> None:
    """If a child terminal already marked the asset terminal, the host stays quiet."""
    runner, asset = _runner_with_asset("asset-3", "run-1")
    runner.state.mark_completed(asset, emit=False)  # as if child reported it
    future: Future[None] = Future()
    future.set_result(None)

    with _capture() as events:
        runner._handle_completed(future, asset)

    assert not [e for e in events if e.type in (EventType.OPERATION_COMPLETED, EventType.OPERATION_FAILED)]


def test_child_reported_failure_keeps_its_rich_error() -> None:
    """A child-streamed terminal's rich error is never overwritten by the Job status.

    Regression: when the child streamed its own terminal (rich SchemaError
    etc.), the run error used to end up being some job's generic
    "Job ... failed" string. Fail-fast aborts through state now — the walk
    loop sees ``failed_operations`` — so the handler must stay quiet and
    leave the child's record intact.
    """
    runner, asset = _runner_with_asset("asset-4", "run-1")
    runner.fail_fast = True
    runner.state.mark_failed(asset, "Schema validation failed on row 0: 41 errors", emit=False)
    future: Future[None] = Future()
    future.set_exception(RunnerError("Job interloper-run-x failed"))

    runner._handle_completed(future, asset)  # must not raise

    info = runner.state.executions[asset.id]
    assert info.error == "Schema validation failed on row 0: 41 errors"
    assert runner.state.failed_operations == [asset]


def test_job_failure_is_recorded_in_state() -> None:
    """A failed Job with no child terminal lands in state, not in a raise."""
    runner, asset = _runner_with_asset("asset-5", "run-1")
    runner.fail_fast = True
    future: Future[None] = Future()
    future.set_exception(RunnerError("Job interloper-run-x failed"))

    runner._handle_completed(future, asset)  # must not raise

    info = runner.state.executions[asset.id]
    assert info.error is not None and "Job interloper-run-x failed" in info.error
    assert isinstance(info.exception, RunnerError)
    assert runner.state.failed_operations == [asset]


def test_no_fail_fast_keeps_quiet_on_child_reported_failure() -> None:
    """Without fail-fast, a child-reported terminal needs no host action."""
    runner, asset = _runner_with_asset("asset-6", "run-1")
    runner.state.mark_failed(asset, "child error", emit=False)
    future: Future[None] = Future()
    future.set_exception(RunnerError("Job interloper-run-x failed"))

    with _capture() as events:
        runner._handle_completed(future, asset)  # must not raise

    assert not [e for e in events if e.type == EventType.OPERATION_FAILED]
