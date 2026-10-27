"""Tests for ``interloper_k8s.runner``."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import interloper as il
import pytest
from interloper.events import Event, EventType
from interloper.operation import OperationResult
from interloper.partitioning import TimeGranularity, TimePartition, TimePartitionConfig, TimePartitionWindow
from interloper.runner import ExecutionStatus
from kubernetes import client, config
from kubernetes.client import (
    V1ContainerState,
    V1ContainerStateTerminated,
    V1ContainerStatus,
    V1Job,
    V1JobStatus,
    V1Pod,
    V1PodList,
    V1PodStatus,
)
from kubernetes.client.exceptions import ApiException

from interloper_k8s.runner import KubernetesRun, KubernetesRunner


class FakeCluster:
    """One operation's Job, scripted; the batch and core API clients."""

    def __init__(self, *, succeeded: bool = True, creatable: bool = True, message: str | None = None) -> None:
        """Script the cluster.

        Args:
            succeeded: Whether the Job succeeds.
            creatable: Whether creating the Job succeeds.
            message: The termination message its pod's container leaves; ``None`` for none.
        """
        self.succeeded = succeeded
        self.creatable = creatable
        self.message = message
        self.created: list[V1Job] = []

    def create_namespaced_job(self, namespace: str, body: V1Job) -> None:
        if not self.creatable:
            raise ApiException(status=422, reason="Invalid")
        self.created.append(body)

    def read_namespaced_job_status(self, name: str, namespace: str) -> V1Job:
        return V1Job(status=V1JobStatus(succeeded=1) if self.succeeded else V1JobStatus(failed=1))

    def list_namespaced_pod(self, namespace: str, label_selector: str) -> V1PodList:
        terminated = V1ContainerStateTerminated(exit_code=1, message=self.message)
        status = V1ContainerStatus(
            name="interloper",
            image="img",
            image_id="",
            ready=False,
            restart_count=0,
            state=V1ContainerState(terminated=terminated),
        )
        return V1PodList(items=[V1Pod(status=V1PodStatus(container_statuses=[status]))])


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


async def _run(asset: il.Asset) -> tuple[il.RunResult, list[Event]]:
    events: list[Event] = []
    runner = KubernetesRunner(image="img", poll_interval=0.0, on_event=events.append)
    result = await runner.run(il.DAG(asset), metadata={"run_id": "run-1"})
    return result, events


class TestJobRun:
    async def test_the_host_reports_the_operations_lifecycle(self, connect, asset):
        connect()

        _, events = await _run(asset)

        lifecycle = [
            (event.type.value, event.metadata.get("attempt")) for event in events if "attempt" in event.metadata
        ]
        assert lifecycle == [("operation_queued", 1), ("operation_started", 1), ("operation_completed", 1)]

    async def test_a_failed_job_without_a_message_names_the_job(self, connect, asset):
        connect(succeeded=False)

        result, _ = await _run(asset)

        assert result.executions["asset-1"].error == "RunnerError: Job interloper-run-run-1-asset-1-1 failed"

    async def test_a_failed_job_carries_what_its_pod_left(self, connect, asset):
        connect(succeeded=False, message=json.dumps({"error": "ValueError: bad credentials", "retryable": False}))

        result, _ = await _run(asset)

        assert result.executions["asset-1"].error == "ValueError: bad credentials"

    async def test_an_unreachable_cluster_fails_the_run_with_its_events(self, monkeypatch, asset):
        def unreachable() -> None:
            raise config.ConfigException("no cluster")

        monkeypatch.setattr(config, "load_incluster_config", unreachable)
        monkeypatch.setattr(config, "load_kube_config", unreachable)
        events: list[Event] = []

        result = await KubernetesRunner(image="img", on_event=events.append).run(
            il.DAG(asset), metadata={"run_id": "run-1"}
        )

        assert result.status is ExecutionStatus.FAILED
        assert result.executions["asset-1"].status is ExecutionStatus.CANCELED
        assert [event.type for event in events][-1] is EventType.RUN_FAILED

    async def test_a_job_that_cannot_be_created_is_a_failed_operation(self, connect, asset):
        connect(creatable=False)

        result, events = await _run(asset)

        assert "Invalid" in (result.executions["asset-1"].error or "")
        assert EventType.OPERATION_FAILED in [event.type for event in events]

    async def test_the_job_carries_the_runs_labels_and_the_pods_settings(self, connect, asset):
        cluster = connect()

        await KubernetesRunner(
            image="img", poll_interval=0.0, ttl_seconds_after_finished=60, image_pull_secrets=["registry"]
        ).run(il.DAG(asset), metadata={"run_id": "run-1"})

        [job] = cluster.created
        labels = {"interloper.component_id": "asset-1", "interloper.run_id": "run-1"}
        assert job.metadata.name == "interloper-run-run-1-asset-1-1"
        assert job.metadata.labels == labels
        assert job.spec.template.metadata.labels == labels
        assert (job.spec.backoff_limit, job.spec.ttl_seconds_after_finished) == (0, 60)
        pod = job.spec.template.spec
        assert pod.restart_policy == "Never"
        assert [secret.name for secret in pod.image_pull_secrets] == ["registry"]
        [container] = pod.containers
        assert (container.name, container.image, container.command) == ("interloper", "img", ["interloper"])
        assert container.termination_message_path == OperationResult.TERMINATION_MESSAGE_PATH

    async def test_the_child_reports_its_operation_with_the_runs_token(self, connect, asset, monkeypatch):
        monkeypatch.setenv("INTERLOPER_RUN_TOKEN", "ilr_x")
        monkeypatch.setenv("INTERLOPER_SERVER_INTERNAL_URL", "http://api")
        cluster = connect()

        await _run(asset)

        [job] = cluster.created
        environment = {variable.name: variable.value for variable in job.spec.template.spec.containers[0].env}
        assert environment["INTERLOPER_RUN_OPERATION"] == "asset-1"
        assert (environment["INTERLOPER_RUN_TOKEN"], environment["INTERLOPER_SERVER_INTERNAL_URL"]) == (
            "ilr_x",
            "http://api",
        )

    async def test_without_a_platform_the_child_reports_nothing(self, connect, asset, monkeypatch):
        monkeypatch.delenv("INTERLOPER_RUN_TOKEN", raising=False)
        cluster = connect()

        await _run(asset)

        [job] = cluster.created
        environment = {variable.name for variable in job.spec.template.spec.containers[0].env}
        assert "INTERLOPER_RUN_TOKEN" not in environment

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
