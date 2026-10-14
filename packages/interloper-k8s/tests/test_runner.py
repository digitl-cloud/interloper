"""Tests for ``interloper_k8s.runner``."""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Iterator
from typing import Any

import interloper as il
import pytest
import urllib3
from interloper.events import Event, EventType
from interloper.events.stderr import EVENT_LINE_PREFIX
from interloper.partitioning import TimeGranularity, TimePartition, TimePartitionConfig, TimePartitionWindow
from interloper.runner import ExecutionStatus, RunState
from kubernetes import client, config
from kubernetes.client import V1Job, V1JobStatus, V1ObjectMeta, V1Pod, V1PodList, V1PodStatus
from kubernetes.client.exceptions import ApiException

from interloper_k8s.runner import KubernetesRun, KubernetesRunner


class FakeCluster:
    """One operation's Job, its pod and the pod's log, scripted; both API clients at once."""

    def __init__(
        self,
        chunks: list[bytes] | None = None,
        *,
        succeeded: bool = True,
        pod_starts: bool = True,
        broken: bool = False,
        creatable: bool = True,
    ) -> None:
        """Script the cluster.

        Args:
            chunks: What the pod's log yields.
            succeeded: Whether the Job succeeds.
            pod_starts: Whether the Job's pod ever starts.
            broken: Whether the log stream breaks after the chunks.
            creatable: Whether creating the Job succeeds.
        """
        self.chunks = chunks or []
        self.succeeded = succeeded
        self.pod_starts = pod_starts
        self.broken = broken
        self.creatable = creatable
        self.created: list[V1Job] = []

    def create_namespaced_job(self, namespace: str, body: V1Job) -> None:
        if not self.creatable:
            raise ApiException(status=422, reason="Invalid")
        self.created.append(body)

    def read_namespaced_job_status(self, name: str, namespace: str) -> V1Job:
        return V1Job(status=V1JobStatus(succeeded=1) if self.succeeded else V1JobStatus(failed=1))

    def list_namespaced_pod(self, namespace: str, label_selector: str) -> V1PodList:
        if not self.pod_starts:
            return V1PodList(items=[])
        return V1PodList(items=[V1Pod(metadata=V1ObjectMeta(name="pod-1"), status=V1PodStatus(phase="Succeeded"))])

    def read_namespaced_pod_log(self, **kwargs: Any) -> Iterator[bytes]:
        yield from self.chunks
        if self.broken:
            raise urllib3.exceptions.ProtocolError("stream reset")


class Contained(il.Asset):
    """An asset executed in a Job."""

    def data(self) -> list[dict[str, Any]]:
        return []


@pytest.fixture
def asset() -> il.Asset:
    """The one operation of the run.

    Returns:
        An unpartitioned asset with a fixed id.
    """
    return Contained(id="asset-1", destinations=[il.MemoryDestination()])


@pytest.fixture
def connect(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Point the Kubernetes client at a scripted cluster.

    Args:
        monkeypatch: Fixture used to replace the client.

    Returns:
        A function installing a ``FakeCluster`` built from its keyword arguments.
    """

    def install(**script: Any) -> FakeCluster:
        cluster = FakeCluster(**script)
        monkeypatch.setattr(config, "load_incluster_config", lambda: None)
        monkeypatch.setattr(client, "BatchV1Api", lambda: cluster)
        monkeypatch.setattr(client, "CoreV1Api", lambda: cluster)
        return cluster

    return install


def _event_line(event_type: EventType, component_id: str = "asset-1") -> bytes:
    event = Event(type=event_type, metadata={"component_id": component_id, "run_id": "run-1"})
    return (EVENT_LINE_PREFIX + event.to_json() + "\n").encode()


async def _run(asset: il.Asset) -> tuple[il.RunResult, list[Event]]:
    events: list[Event] = []
    runner = KubernetesRunner(image="img", poll_interval=0.0, on_event=events.append)
    result = await runner.run(il.DAG(asset), metadata={"run_id": "run-1"})
    return result, events


class TestJobRun:
    async def test_forwards_the_childs_events_before_recording_the_verdict(self, connect, asset):
        connect(chunks=[_event_line(EventType.ASSET_DATA_COMPLETED)])

        result, events = await _run(asset)

        types = [event.type for event in events]
        assert types.index(EventType.ASSET_DATA_COMPLETED) < types.index(EventType.OPERATION_COMPLETED)
        assert result.status is ExecutionStatus.COMPLETED

    async def test_the_host_reports_no_start_of_its_own(self, connect, asset):
        connect()

        _, events = await _run(asset)

        assert EventType.OPERATION_STARTED not in [event.type for event in events]

    async def test_a_failed_job_is_a_failed_operation_with_the_hosts_terminal_event(self, connect, asset):
        connect(succeeded=False)

        result, events = await _run(asset)

        [failed] = [event for event in events if event.type is EventType.OPERATION_FAILED]
        assert failed.id == RunState.operation_event_id("run-1", "asset-1", EventType.OPERATION_FAILED)
        assert result.executions["asset-1"].error == "RunnerError: Job interloper-run-run-1-asset-1 failed"

    async def test_a_job_that_cannot_be_created_is_a_failed_operation(self, connect, asset):
        connect(creatable=False)

        result, events = await _run(asset)

        assert "Invalid" in (result.executions["asset-1"].error or "")
        assert EventType.OPERATION_FAILED in [event.type for event in events]

    async def test_a_broken_log_stream_still_returns_the_verdict(self, connect, asset):
        connect(chunks=[_event_line(EventType.ASSET_DATA_STARTED)], broken=True)

        result, events = await _run(asset)

        assert EventType.ASSET_DATA_STARTED in [event.type for event in events]
        assert result.status is ExecutionStatus.COMPLETED

    async def test_a_job_that_fails_before_its_pod_starts_returns_the_failure(self, connect, asset):
        connect(pod_starts=False, succeeded=False)

        result, _ = await _run(asset)

        assert result.executions["asset-1"].status is ExecutionStatus.FAILED

    async def test_the_job_carries_the_runs_labels_and_the_pods_settings(self, connect, asset):
        cluster = connect()

        await KubernetesRunner(
            image="img", poll_interval=0.0, ttl_seconds_after_finished=60, image_pull_secrets=["registry"]
        ).run(il.DAG(asset), metadata={"run_id": "run-1"})

        [job] = cluster.created
        labels = {"interloper.component_id": "asset-1", "interloper.run_id": "run-1"}
        assert job.metadata.name == "interloper-run-run-1-asset-1"
        assert job.metadata.labels == labels
        assert job.spec.template.metadata.labels == labels
        assert (job.spec.backoff_limit, job.spec.ttl_seconds_after_finished) == (0, 60)
        pod = job.spec.template.spec
        assert pod.restart_policy == "Never"
        assert [secret.name for secret in pod.image_pull_secrets] == ["registry"]
        [container] = pod.containers
        assert (container.name, container.image, container.command) == ("interloper", "img", ["interloper"])
        assert {"name": "INTERLOPER_EVENTS_TO_STDERR", "value": "true"} in [
            {"name": variable.name, "value": variable.value} for variable in container.env
        ]

    async def test_the_pod_continues_the_runs_trace(self, connect, asset, span_exporter):
        cluster = connect()

        await _run(asset)

        run_span = next(span for span in span_exporter.get_finished_spans() if span.name == "interloper.runner.run")
        [job] = cluster.created
        environment = {variable.name: variable.value for variable in job.spec.template.spec.containers[0].env}
        assert f"{run_span.context.trace_id:032x}" in environment.get("TRACEPARENT", "")


class TestCommand:
    """The command a pod runs: its operation's mini-DAG through ``interloper run --format inline``."""

    @staticmethod
    def _command(connect: Any, asset: il.Asset, window: Any) -> list[str]:
        connect()
        dag = il.DAG(asset)
        run = KubernetesRun(KubernetesRunner(image="img"), dag, il.PartitionWindow.of(window), {"run_id": "r1"})
        return run.command(dag.operations[0])

    def test_an_unscoped_run_adds_no_flags(self, connect, asset):
        command = self._command(connect, asset, None)

        assert command[:5] == ["interloper", "run", "--format", "inline", "--run-id=r1"]
        assert json.loads(command[5])
        assert len(command) == 6

    def test_one_partition_is_a_date(self, connect):
        @il.asset(partitioning=TimePartitionConfig(column="date"))
        def daily() -> list:
            return []

        command = self._command(connect, daily(id="daily"), TimePartition(dt.date(2026, 1, 1)))

        assert command[-2:] == ["--date", "2026-01-01"]

    def test_an_hourly_partition_keeps_its_hour(self, connect):
        @il.asset(partitioning=TimePartitionConfig(column="date", granularity=TimeGranularity.HOUR))
        def hourly() -> list:
            return []

        partition = TimePartition(dt.datetime(2026, 1, 1, 13), TimeGranularity.HOUR)

        assert self._command(connect, hourly(id="hourly"), partition)[-2:] == ["--date", "2026-01-01T13"]

    def test_several_partitions_are_a_range(self, connect):
        @il.asset(partitioning=TimePartitionConfig(column="date", allow_window=True))
        def windowed() -> list:
            return []

        window = TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 3))
        command = self._command(connect, windowed(id="windowed"), window)

        assert command[-4:] == ["--start-date", "2026-01-01", "--end-date", "2026-01-03"]

    def test_an_unpartitioned_operation_gets_no_window(self, connect, asset):
        command = self._command(connect, asset, TimePartition(dt.date(2026, 1, 1)))

        assert len(command) == 6


def test_the_run_kind_is_named_after_its_runner():
    """The Kubernetes runner's run kind is ``KubernetesRun``."""
    from interloper_k8s.runner import KubernetesRun, KubernetesRunner

    assert KubernetesRunner.Run is KubernetesRun
