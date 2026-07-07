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
from types import SimpleNamespace
from unittest.mock import MagicMock

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


def test_fail_fast_surfaces_child_error_when_child_reported_terminal() -> None:
    """The run-level error must carry the child's rich error, not the bare Job status.

    Regression: when the child streamed its own ``asset_failed`` (rich
    SchemaError etc.), the host returned quietly and the run error ended up
    being some other job's generic "Job ... failed" string.
    """
    runner, asset = _runner_with_asset("asset-4", "run-1")
    runner.fail_fast = True
    runner.state.mark_asset_failed(asset, "Schema validation failed on row 0: 41 errors", emit=False)
    future: Future[None] = Future()
    future.set_exception(RunnerError("Job interloper-run-x failed"))

    try:
        runner._handle_completed(future, asset)
    except RunnerError as e:
        assert "Schema validation failed on row 0" in str(e)
        assert "_Asset".lower() in str(e).lower() or "asset" in str(e).lower()
    else:
        raise AssertionError("fail_fast must abort the run on a child-reported failure")


def test_fail_fast_wraps_job_error_with_asset_key() -> None:
    """The host fallback error names the asset, not just the Job."""
    runner, asset = _runner_with_asset("asset-5", "run-1")
    runner.fail_fast = True
    future: Future[None] = Future()
    future.set_exception(RunnerError("Job interloper-run-x failed"))

    try:
        runner._handle_completed(future, asset)
    except RunnerError as e:
        assert "Job interloper-run-x failed" in str(e)
        assert "failed:" in str(e)
    else:
        raise AssertionError("fail_fast must re-raise on job failure")


def test_no_fail_fast_keeps_quiet_on_child_reported_failure() -> None:
    """Without fail-fast, a child-reported terminal needs no host action."""
    runner, asset = _runner_with_asset("asset-6", "run-1")
    runner.state.mark_asset_failed(asset, "child error", emit=False)
    future: Future[None] = Future()
    future.set_exception(RunnerError("Job interloper-run-x failed"))

    with _capture() as events:
        runner._handle_completed(future, asset)  # must not raise

    assert not [e for e in events if e.type == EventType.ASSET_FAILED]


# ----------------------------------------------------------------------
# Failure recovery: the host-authored terminal carries the pod's real cause
# (not a bare "Job ... failed") when the live event stream dropped before the
# child reported its terminal.
# ----------------------------------------------------------------------


def _pod(name: str = "pod-x", *, terminated: SimpleNamespace | None = None) -> SimpleNamespace:
    container_statuses = None
    if terminated is not None:
        container_statuses = [SimpleNamespace(state=SimpleNamespace(terminated=terminated))]
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        status=SimpleNamespace(container_statuses=container_statuses),
    )


def _mock_core_v1(*, pod: SimpleNamespace | None = None, logs: str = "") -> MagicMock:
    core = MagicMock()
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[pod] if pod is not None else [])
    core.read_namespaced_pod_log.return_value = logs
    return core


def _failed_event_line(asset_id: str, error: str, traceback: str | None = None) -> str:
    metadata: dict[str, str] = {"asset_id": asset_id, "error": error}
    if traceback is not None:
        metadata["traceback"] = traceback
    return Event(type=EventType.ASSET_FAILED, metadata=metadata).to_json()


def test_recover_returns_fallback_without_job_or_client() -> None:
    """No job name (or no k8s client) → the bare fallback, no traceback."""
    runner, _ = _runner_with_asset("asset-1", "run-1")
    assert runner._recover_asset_failure(None, "asset-1", fallback="Job x failed") == ("Job x failed", None)

    runner._core_v1 = _mock_core_v1()
    assert runner._recover_asset_failure(None, "asset-1", fallback="Job x failed") == ("Job x failed", None)


def test_recover_reads_rich_terminal_from_pod_logs() -> None:
    """The child's own error + traceback are recovered from the final logs."""
    runner, _ = _runner_with_asset("asset-1", "run-1")
    line = _failed_event_line("asset-1", "429 Too Many Requests", "Traceback...\nHTTPStatusError")
    runner._core_v1 = _mock_core_v1(pod=_pod(), logs=f"some debug log\n{line}\n")

    error, tb = runner._recover_asset_failure("job-x", "asset-1", fallback="Job job-x failed")

    assert error == "429 Too Many Requests"
    assert tb is not None and "HTTPStatusError" in tb


def test_recover_ignores_other_assets_terminal_in_logs() -> None:
    """A terminal for a different asset must not be attributed to this one."""
    runner, _ = _runner_with_asset("asset-1", "run-1")
    term = SimpleNamespace(reason="Error", exit_code=1, message="log tail")
    runner._core_v1 = _mock_core_v1(pod=_pod(terminated=term), logs=_failed_event_line("other-asset", "not mine"))

    error, tb = runner._recover_asset_failure("job-x", "asset-1", fallback="Job job-x failed")

    # No matching log terminal → falls through to the termination state.
    assert error == "Job job-x failed (reason=Error, exit_code=1)"
    assert tb == "log tail"


def test_recover_falls_back_to_termination_state_on_oomkill() -> None:
    """A SIGKILLed pod has no terminal event; reason/exit_code surface instead."""
    runner, _ = _runner_with_asset("asset-1", "run-1")
    term = SimpleNamespace(reason="OOMKilled", exit_code=137, message="killed log tail")
    runner._core_v1 = _mock_core_v1(pod=_pod(terminated=term), logs="no events here\n")

    error, tb = runner._recover_asset_failure("job-x", "asset-1", fallback="Job job-x failed")

    assert error == "Job job-x failed (reason=OOMKilled, exit_code=137)"
    assert tb == "killed log tail"


def test_handle_completed_emits_recovered_error_and_traceback() -> None:
    """End to end: the host-authored ``asset_failed`` carries the recovered cause."""
    runner, asset = _runner_with_asset("asset-1", "run-1")
    runner._core_v1 = _mock_core_v1(pod=_pod(), logs=_failed_event_line("asset-1", "429 Too Many Requests", "rich-tb"))
    future: Future[None] = Future()
    future.set_exception(RunnerError("Job interloper-run-x failed"))
    runner._job_map[future] = "interloper-run-x"

    with _capture() as events:
        runner._handle_completed(future, asset)

    failed = [e for e in events if e.type == EventType.ASSET_FAILED]
    assert len(failed) == 1
    assert failed[0].metadata.get("error") == "429 Too Many Requests"
    assert failed[0].metadata.get("traceback") == "rich-tb"
