"""Tests for ``interloper_docker.runner``."""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Iterator
from typing import Any

import docker
import docker.errors
import interloper as il
import pytest
import requests
from interloper.events import Event, EventType
from interloper.events.stderr import EVENT_LINE_PREFIX
from interloper.partitioning import TimeGranularity, TimePartition, TimePartitionConfig, TimePartitionWindow
from interloper.runner import RunState
from interloper.runner.results import ExecutionStatus

from interloper_docker.runner import ContainerRun, DockerRunner


class FakeContainer:
    """A container whose log and exit code are scripted."""

    def __init__(self, chunks: list[bytes], status_code: int = 0, broken: bool = False) -> None:
        """Script the container.

        Args:
            chunks: What its log yields.
            status_code: Its exit code.
            broken: Whether its log stream breaks after the chunks.
        """
        self.chunks = chunks
        self.status_code = status_code
        self.broken = broken
        self.id = "c0ffee000000aaaa"
        self.short_id = "c0ffee000000"
        self.removed = False
        self.launched: dict[str, Any] = {}

    def logs(self, **kwargs: Any) -> Iterator[bytes]:
        yield from self.chunks
        if self.broken:
            raise requests.exceptions.ConnectionError("stream reset")

    def wait(self) -> dict[str, int]:
        return {"StatusCode": self.status_code}

    def remove(self) -> None:
        self.removed = True


class FakeDocker:
    """A Docker client whose one container is scripted, or whose launch fails."""

    def __init__(self, container: FakeContainer | None) -> None:
        """Script the client.

        Args:
            container: The container every launch returns; ``None`` makes launching fail.
        """
        self.container = container
        self.containers = self

    def run(self, **kwargs: Any) -> FakeContainer:
        if self.container is None:
            raise docker.errors.APIError("no such image")
        self.container.launched = kwargs
        return self.container


class Contained(il.Asset):
    """An asset executed in a container."""

    def data(self) -> list[dict[str, Any]]:
        return []


def _event_line(event_type: EventType, component_id: str = "asset-1") -> bytes:
    event = Event(type=event_type, metadata={"component_id": component_id, "run_id": "run-1"})
    return (EVENT_LINE_PREFIX + event.to_json() + "\n").encode()


@pytest.fixture
def asset() -> il.Asset:
    """The one operation of the run.

    Returns:
        An unpartitioned asset with a fixed id.
    """
    return Contained(id="asset-1", destinations=[il.MemoryDestination()])


async def _run(
    monkeypatch: pytest.MonkeyPatch, container: FakeContainer | None, asset: il.Asset
) -> tuple[il.RunResult, list[Event]]:
    monkeypatch.setattr(docker, "from_env", lambda: FakeDocker(container))
    events: list[Event] = []
    runner = DockerRunner(image="img", on_event=events.append)
    result = await runner.run(il.DAG(asset), metadata={"run_id": "run-1"})
    return result, events


class TestContainerRun:
    async def test_forwards_the_childs_events_before_recording_the_verdict(self, monkeypatch, asset):
        container = FakeContainer([_event_line(EventType.ASSET_DATA_COMPLETED)])

        result, events = await _run(monkeypatch, container, asset)

        types = [event.type for event in events]
        assert types.index(EventType.ASSET_DATA_COMPLETED) < types.index(EventType.OPERATION_COMPLETED)
        assert result.status is ExecutionStatus.COMPLETED
        assert container.removed

    async def test_the_host_reports_no_start_of_its_own(self, monkeypatch, asset):
        _, events = await _run(monkeypatch, FakeContainer([]), asset)

        assert EventType.OPERATION_STARTED not in [event.type for event in events]

    async def test_a_failed_exit_is_a_failed_operation_with_the_hosts_terminal_event(self, monkeypatch, asset):
        result, events = await _run(monkeypatch, FakeContainer([], status_code=3), asset)

        [failed] = [event for event in events if event.type is EventType.OPERATION_FAILED]
        assert failed.id == RunState.operation_event_id("run-1", "asset-1", EventType.OPERATION_FAILED)
        assert result.executions["asset-1"].error == "RunnerError: Container c0ffee000000 exited with code 3"

    async def test_a_launch_failure_is_a_failed_operation(self, monkeypatch, asset):
        result, events = await _run(monkeypatch, None, asset)

        assert "no such image" in (result.executions["asset-1"].error or "")
        assert EventType.OPERATION_FAILED in [event.type for event in events]

    async def test_a_broken_log_stream_still_returns_the_verdict(self, monkeypatch, asset):
        container = FakeContainer([_event_line(EventType.ASSET_DATA_STARTED)], broken=True)

        result, events = await _run(monkeypatch, container, asset)

        assert EventType.ASSET_DATA_STARTED in [event.type for event in events]
        assert result.status is ExecutionStatus.COMPLETED

    async def test_the_container_carries_its_name_label_and_environment(self, monkeypatch, asset):
        container = FakeContainer([])

        await _run(monkeypatch, container, asset)

        assert container.launched["name"] == "interloper_run_run-1_asset-1"
        assert container.launched["labels"] == {"interloper.component_id": "asset-1"}
        assert container.launched["environment"]["INTERLOPER_EVENTS_TO_STDERR"] == "true"

    async def test_the_container_continues_the_runs_trace(self, monkeypatch, asset, span_exporter):
        container = FakeContainer([])

        await _run(monkeypatch, container, asset)

        run_span = next(span for span in span_exporter.get_finished_spans() if span.name == "interloper.runner.run")
        traceparent = container.launched["environment"].get("TRACEPARENT", "")
        assert f"{run_span.context.trace_id:032x}" in traceparent

    async def test_a_kept_container_is_not_removed(self, monkeypatch, asset):
        container = FakeContainer([])
        monkeypatch.setattr(docker, "from_env", lambda: FakeDocker(container))

        await DockerRunner(image="img", auto_remove=False).run(il.DAG(asset))

        assert not container.removed


class TestCommand:
    """The command a container runs: its operation's mini-DAG through ``interloper run --format inline``."""

    @staticmethod
    def _command(monkeypatch: pytest.MonkeyPatch, asset: il.Asset, window: Any) -> list[str]:
        monkeypatch.setattr(docker, "from_env", lambda: FakeDocker(None))
        dag = il.DAG(asset)
        run = ContainerRun(DockerRunner(image="img"), dag, il.PartitionWindow.of(window), {"run_id": "r1"})
        return run.command(dag.operations[0])

    def test_an_unscoped_run_adds_no_flags(self, monkeypatch, asset):
        command = self._command(monkeypatch, asset, None)

        assert command[:5] == ["interloper", "run", "--format", "inline", "--run-id=r1"]
        assert json.loads(command[5])
        assert len(command) == 6

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

        assert len(command) == 6
