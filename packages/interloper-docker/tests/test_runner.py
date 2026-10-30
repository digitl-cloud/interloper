"""Tests for ``interloper_docker.runner``."""

from __future__ import annotations

import datetime as dt
import io
import json
import tarfile
from typing import Any

import docker
import docker.errors
import interloper as il
import pytest
from interloper.events import Event, EventType
from interloper.partitioning import TimeGranularity, TimePartition, TimePartitionConfig, TimePartitionWindow
from interloper.runner import ExecutionStatus

from interloper_docker.runner import DockerRun, DockerRunner


class FakeContainer:
    """A container whose exit code and termination message are scripted."""

    def __init__(self, status_code: int = 0, message: str | None = None) -> None:
        """Script the container.

        Args:
            status_code: Its exit code.
            message: What it left at its termination message path; ``None`` for nothing.
        """
        self.status_code = status_code
        self.message = message
        self.id = "c0ffee000000aaaa"
        self.short_id = "c0ffee000000"
        self.removed = False
        self.launched: dict[str, Any] = {}

    def get_archive(self, path: str) -> tuple[list[bytes], dict[str, Any]]:
        if self.message is None:
            raise docker.errors.NotFound(f"{path} not found")
        data = self.message.encode()
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as archive:
            info = tarfile.TarInfo(name=path.rsplit("/", 1)[-1])
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
        return [buffer.getvalue()], {}

    def wait(self) -> dict[str, int]:
        return {"StatusCode": self.status_code}

    def remove(self) -> None:
        self.removed = True


class FakeDocker:
    """A Docker client whose containers are scripted, one per attempt, or whose launch fails."""

    def __init__(self, *containers: FakeContainer) -> None:
        """Script the client.

        Args:
            *containers: The containers successive launches return; none makes launching fail.
        """
        self.scripted = list(containers)
        self.launched: list[FakeContainer] = []
        self.containers = self

    def run(self, **kwargs: Any) -> FakeContainer:
        if not self.scripted:
            raise docker.errors.APIError("no such image")
        container = self.scripted.pop(0)
        container.launched = kwargs
        self.launched.append(container)
        return container


class Contained(il.Asset):
    """An asset executed in a container."""

    def data(self) -> list[dict[str, Any]]:
        return []


@pytest.fixture
def asset() -> il.Asset:
    """The one operation of the run.

    Returns:
        An unpartitioned asset with a fixed id.
    """
    return Contained(id="asset-1", destinations=[il.MemoryDestination()])


async def _run(
    monkeypatch: pytest.MonkeyPatch, asset: il.Asset, *containers: FakeContainer
) -> tuple[il.RunResult, list[Event]]:
    monkeypatch.setattr(docker, "from_env", lambda: FakeDocker(*containers))
    events: list[Event] = []
    runner = DockerRunner(image="img", on_event=events.append)
    result = await runner.run(il.DAG(asset), metadata={"run_id": "run-1"})
    return result, events


def _lifecycle(events: list[Event]) -> list[tuple[str, int]]:
    return [(event.type.value, event.metadata["attempt"]) for event in events if "attempt" in event.metadata]


class TestContainerRun:
    async def test_the_host_reports_the_operations_lifecycle(self, monkeypatch, asset):
        _, events = await _run(monkeypatch, asset, FakeContainer())

        assert _lifecycle(events) == [("operation_queued", 1), ("operation_started", 1), ("operation_completed", 1)]

    async def test_a_failed_exit_without_a_message_names_its_exit_code(self, monkeypatch, asset):
        result, _ = await _run(monkeypatch, asset, FakeContainer(status_code=3))

        assert result.executions["asset-1"].error == "RunnerError: Container c0ffee000000 exited with code 3"

    async def test_a_failed_attempt_carries_what_the_child_left(self, monkeypatch, asset):
        message = json.dumps({"error": "ValueError: bad credentials", "retryable": False})

        result, _ = await _run(monkeypatch, asset, FakeContainer(status_code=1, message=message))

        assert result.executions["asset-1"].error == "ValueError: bad credentials"

    async def test_a_retryable_attempt_is_retried_in_a_new_container(self, monkeypatch, asset):
        asset.retry = il.RetryPolicy(max_attempts=2, delay=0)
        failed = FakeContainer(status_code=137)
        succeeded = FakeContainer()

        result, events = await _run(monkeypatch, asset, failed, succeeded)

        assert result.status is ExecutionStatus.COMPLETED
        assert _lifecycle(events) == [
            ("operation_queued", 1),
            ("operation_started", 1),
            ("operation_retried", 1),
            ("operation_started", 2),
            ("operation_completed", 2),
        ]
        assert [failed.launched["name"], succeeded.launched["name"]] == [
            "interloper_run_run-1_asset-1_1",
            "interloper_run_run-1_asset-1_2",
        ]

    async def test_an_attempt_the_child_says_is_final_is_not_retried(self, monkeypatch, asset):
        asset.retry = il.RetryPolicy(max_attempts=3, delay=0)
        message = json.dumps({"error": "ValueError: bad credentials", "retryable": False})

        result, _ = await _run(monkeypatch, asset, FakeContainer(status_code=1, message=message))

        assert result.executions["asset-1"].attempt == 1
        assert result.status is ExecutionStatus.FAILED

    async def test_an_unreachable_daemon_fails_the_run_with_its_events(self, monkeypatch, asset):
        def unreachable() -> None:
            raise docker.errors.DockerException("daemon down")

        monkeypatch.setattr(docker, "from_env", unreachable)
        events: list[Event] = []

        result = await DockerRunner(image="img", on_event=events.append).run(il.DAG(asset), metadata={"run_id": "r"})

        assert result.status is ExecutionStatus.FAILED
        assert result.executions["asset-1"].status is ExecutionStatus.CANCELED
        assert [event.type for event in events][-1] is EventType.RUN_FAILED

    async def test_a_launch_failure_is_a_failed_operation(self, monkeypatch, asset):
        result, events = await _run(monkeypatch, asset)

        assert "no such image" in (result.executions["asset-1"].error or "")
        assert EventType.OPERATION_FAILED in [event.type for event in events]

    async def test_the_container_carries_its_name_label_and_environment(self, monkeypatch, asset):
        container = FakeContainer()

        await _run(monkeypatch, asset, container)

        assert container.launched["name"] == "interloper_run_run-1_asset-1_1"
        assert container.launched["labels"] == {"interloper.component_id": "asset-1"}

    async def test_the_child_reports_its_operation_with_the_runs_token(self, monkeypatch, asset):
        monkeypatch.setenv("INTERLOPER_RUN_TOKEN", "ilr_x")
        monkeypatch.setenv("INTERLOPER_SERVER_INTERNAL_URL", "http://api")
        container = FakeContainer()

        result, _ = await _run(monkeypatch, asset, container)

        environment = container.launched["environment"]
        assert "INTERLOPER_RUN_OPERATION" not in environment
        assert (environment["INTERLOPER_RUN_TOKEN"], environment["INTERLOPER_SERVER_INTERNAL_URL"]) == (
            "ilr_x",
            "http://api",
        )
        assert result.status is ExecutionStatus.COMPLETED and container.removed

    async def test_the_container_continues_the_runs_trace(self, monkeypatch, asset, span_exporter):
        container = FakeContainer()

        await _run(monkeypatch, asset, container)

        run_span = next(span for span in span_exporter.get_finished_spans() if span.name == "interloper.runner.run")
        traceparent = container.launched["environment"].get("TRACEPARENT", "")
        assert f"{run_span.context.trace_id:032x}" in traceparent

    async def test_a_kept_container_is_not_removed(self, monkeypatch, asset):
        container = FakeContainer()
        monkeypatch.setattr(docker, "from_env", lambda: FakeDocker(container))

        await DockerRunner(image="img", auto_remove=False).run(il.DAG(asset))

        assert not container.removed


class TestCommand:
    """The command a container runs: ``interloper launch`` by the run's id, ``interloper run`` with the mini-DAG."""

    @staticmethod
    def _command(monkeypatch: pytest.MonkeyPatch, asset: il.Asset, window: Any) -> list[str]:
        monkeypatch.setattr(docker, "from_env", lambda: FakeDocker())
        dag = il.DAG(asset)
        run = DockerRun(DockerRunner(image="img"), dag, il.PartitionWindow.of(window), {"run_id": "r1"})
        return run.command(dag.operations[0])

    def test_an_unscoped_run_adds_no_flags(self, monkeypatch, asset):
        command = self._command(monkeypatch, asset, None)

        assert command[:4] == ["interloper", "run", "--format", "inline"]
        assert json.loads(command[4])
        assert command[5:] == [f"--operation-id={asset.id}"]

    def test_on_the_platform_the_child_executes_the_operation_of_the_run_by_its_id(self, monkeypatch, asset):
        monkeypatch.setenv("INTERLOPER_RUN_TOKEN", "ilr_x")

        command = self._command(monkeypatch, asset, TimePartition(dt.date(2026, 1, 1)))

        assert command == ["interloper", "launch", "r1", f"--operation-id={asset.id}"]

    def test_one_partition_is_a_date(self, monkeypatch):
        @il.asset(partitioning=TimePartitionConfig(column="date"))
        def daily() -> list:
            return []

        command = self._command(monkeypatch, daily(id="daily"), TimePartition(dt.date(2026, 1, 1)))

        assert command[-2:] == ["--date", "2026-01-01"]

    def test_an_hourly_partition_keeps_its_hour(self, monkeypatch):
        @il.asset(partitioning=TimePartitionConfig(column="date", granularity=TimeGranularity.HOUR))
        def hourly() -> list:
            return []

        partition = TimePartition(dt.datetime(2026, 1, 1, 13), TimeGranularity.HOUR)

        assert self._command(monkeypatch, hourly(id="hourly"), partition)[-2:] == ["--date", "2026-01-01T13"]

    def test_several_partitions_are_a_range(self, monkeypatch):
        @il.asset(partitioning=TimePartitionConfig(column="date", allow_window=True))
        def windowed() -> list:
            return []

        window = TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 3))
        command = self._command(monkeypatch, windowed(id="windowed"), window)

        assert command[-4:] == ["--start-date", "2026-01-01", "--end-date", "2026-01-03"]

    def test_an_unpartitioned_operation_gets_no_window(self, monkeypatch, asset):
        command = self._command(monkeypatch, asset, TimePartition(dt.date(2026, 1, 1)))

        assert command[5:] == [f"--operation-id={asset.id}"]


def test_the_run_kind_is_named_after_its_runner():
    """The Docker runner's run kind is ``DockerRun``."""
    from interloper_docker.runner import DockerRun, DockerRunner

    assert DockerRunner.Run is DockerRun
