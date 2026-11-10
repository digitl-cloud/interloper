"""Tests for ``interloper.cli.commands.launch``: a platform run loaded from the API, its events reported back."""

import argparse
import json
from pathlib import Path
from typing import Any

import httpx2
import pytest

import interloper as il
from interloper.cli.commands.launch import _cmd_launch
from interloper.events import Event, EventBatch, EventType
from interloper.operation import OperationResult
from interloper.runner.spec import RunSpec


class LaunchedSource(il.Source):
    """Single-asset source a launched run executes."""

    class One(il.Asset):
        """Returns a static row."""

        def data(self) -> Any:
            return [{"x": 1}]


class FailingLaunchedSource(il.Source):
    """Single-asset source whose asset always raises."""

    class Boom(il.Asset):
        """Always fails.

        Raises:
            RuntimeError: Always.
        """

        def data(self) -> Any:
            raise RuntimeError("asset blew up")


def _args(**overrides: Any) -> argparse.Namespace:
    return argparse.Namespace(**{"run_id": "run-1", "operation_id": None, **overrides})


class FakeApi:
    """The platform's routes for one run, answering its process as the API does."""

    def __init__(self) -> None:
        """Serve no run until one is set, and take every batch as live."""
        self.spec: RunSpec | None = None
        self.spec_status = 200
        self.live = True
        self.requests: list[httpx2.Request] = []
        self.batches: list[list[Event]] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        """Answer one request of the run's process.

        Args:
            request: The request.

        Returns:
            The run's spec for ``GET``; whether the run is live for a posted batch.
        """
        self.requests.append(request)
        if request.method == "GET":
            if self.spec is None or self.spec_status != 200:
                return httpx2.Response(self.spec_status, json={"detail": "Run run-1 is canceled, not live"})
            return httpx2.Response(200, json=self.spec.model_dump(mode="json"))
        self.batches.append(EventBatch.from_dict(json.loads(request.content)).events)
        return httpx2.Response(200, json={"live": self.live})

    def serve(self, dag: il.DAG, *, partition_key: str | None = None) -> None:
        """Serve *dag* as the run, with the metadata the platform gives it.

        Args:
            dag: The run's DAG.
            partition_key: The run's partition.
        """
        metadata = {"run_id": "run-1", "org_id": "org-1", "attempt": 2}
        self.spec = RunSpec(dag=dag.to_spec(), partition_key=partition_key, metadata=metadata)

    @property
    def events(self) -> list[Event]:
        """Every event delivered, in order."""
        return [event for batch in self.batches for event in batch]


@pytest.fixture
def api(monkeypatch: pytest.MonkeyPatch) -> FakeApi:
    """The platform's API, reached with the run's token, and an exit that never ends the test process.

    Args:
        monkeypatch: Fixture used to set the token and swap the client and the exit.

    Returns:
        The API the run's process talks to.
    """
    api = FakeApi()
    monkeypatch.setenv("INTERLOPER_RUN_TOKEN", "ilr_x")

    def client(base_url: str, auth: httpx2.Auth, timeout: float) -> httpx2.Client:
        return httpx2.Client(base_url=base_url, auth=auth, timeout=timeout, transport=httpx2.MockTransport(api))

    monkeypatch.setattr("interloper.rest.RESTClient", client)
    monkeypatch.setattr("interloper.cli.commands.launch._exit_lost", lambda: None)
    return api


@pytest.fixture
def message(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Where an operation leaves what it came to.

    Args:
        monkeypatch: Fixture used to move the termination message path.
        tmp_path: Directory the message is written to.

    Returns:
        The message's path.
    """
    path = tmp_path / "termination-log"
    monkeypatch.setattr(OperationResult, "TERMINATION_MESSAGE_PATH", str(path))
    return path


class TestLaunch:
    """A platform run loaded by its id and its events reported back, as the run's token."""

    def test_the_run_is_loaded_and_every_event_reported_with_its_metadata(self, api: FakeApi) -> None:
        api.serve(il.DAG(LaunchedSource(destinations=[il.MemoryDestination()])))

        _cmd_launch(_args())

        loaded = api.requests[0]
        assert (loaded.method, str(loaded.url)) == ("GET", "http://localhost:3000/api/runs/run-1/spec")
        assert loaded.headers["authorization"] == "Bearer ilr_x"
        assert api.events[0].type is EventType.RUN_STARTED and api.events[-1].type is EventType.RUN_COMPLETED
        assert (api.events[0].metadata["org_id"], api.events[0].metadata["attempt"]) == ("org-1", 2)

    def test_the_window_is_the_runs_partition(self, api: FakeApi) -> None:
        # An unpartitioned DAG given a key: `interloper run` refuses it, a launched run reads it as the platform does.
        api.serve(il.DAG(LaunchedSource(destinations=[il.MemoryDestination()])), partition_key="2026-10-26")

        _cmd_launch(_args())

        assert api.events[0].metadata["window"] == "2026-10-26"
        assert api.events[-1].type is EventType.RUN_COMPLETED

    def test_a_run_that_cannot_be_loaded_is_reported_as_its_failure(self, api: FakeApi) -> None:
        api.spec_status = 409

        with pytest.raises(SystemExit):
            _cmd_launch(_args())

        (failed,) = api.events
        assert failed.type is EventType.RUN_FAILED
        assert "HTTP 409" in failed.metadata["error"]

    def test_a_run_ended_elsewhere_exits_nonzero(self, api: FakeApi) -> None:
        api.serve(il.DAG(FailingLaunchedSource(destinations=[il.MemoryDestination()])))
        api.live = False

        with pytest.raises(SystemExit, match="ended elsewhere"):
            _cmd_launch(_args())

    def test_without_the_runs_token_it_says_what_is_missing(self) -> None:
        with pytest.raises(SystemExit, match="needs the run's token"):
            _cmd_launch(_args())


class TestOperation:
    """``--operation-id``: one operation of the run executed once for its host, its verdict left for it."""

    def test_it_reports_only_what_happens_inside_the_operation(self, api: FakeApi) -> None:
        dag = il.DAG(LaunchedSource(destinations=[il.MemoryDestination()]))
        (operation,) = dag.operations
        api.serve(dag)

        _cmd_launch(_args(operation_id=operation.id))

        assert api.events and all(event.metadata.get("component_id") in (None, operation.id) for event in api.events)
        assert not [event for event in api.events if event.type.value.startswith(("run_", "operation_"))]
        assert EventType.ASSET_DATA_COMPLETED in {event.type for event in api.events}

    def test_it_executes_once_and_leaves_its_verdict(self, api: FakeApi, message: Path) -> None:
        dag = il.DAG(FailingLaunchedSource(destinations=[il.MemoryDestination()]))
        (operation,) = dag.operations
        operation.retry = il.RetryPolicy(max_attempts=3, delay=0)
        api.serve(dag)

        with pytest.raises(SystemExit):
            _cmd_launch(_args(operation_id=operation.id))

        assert OperationResult.from_termination_message(message.read_text(), "exited") == OperationResult(
            error="RuntimeError: asset blew up"
        )
        assert [event.type for event in api.events].count(EventType.ASSET_DATA_STARTED) == 1

    def test_one_that_cannot_start_leaves_a_final_verdict(self, api: FakeApi, message: Path) -> None:
        api.spec_status = 409

        with pytest.raises(SystemExit):
            _cmd_launch(_args(operation_id="op-1"))

        left = OperationResult.from_termination_message(message.read_text(), "exited")
        assert "HTTP 409" in (left.error or "")
        assert left.retryable is False
        assert api.events == []
