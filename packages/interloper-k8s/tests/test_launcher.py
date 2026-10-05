"""The launcher mounts configured secrets as env into every run pod.

Runs hydrate connections, so the pod needs the same runtime-resolved
credentials (e.g. the in-house OAuth provider trio) the API resolves from
its environment; ``env_from`` is how a deployment delivers them.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast
from uuid import uuid4

import pytest
from interloper.catalog.base import Catalog
from interloper_scheduler.launcher import LaunchStatus
from kubernetes import client, config

from interloper_k8s.launcher import KubernetesLauncher


@pytest.fixture
def launcher_factory(monkeypatch: pytest.MonkeyPatch) -> Callable[..., KubernetesLauncher]:
    monkeypatch.setattr(config, "load_incluster_config", lambda: None)

    def factory(**kwargs: Any) -> KubernetesLauncher:
        return KubernetesLauncher(
            catalog=Catalog(),
            postgres_host="db",
            postgres_port=5432,
            postgres_user="user",
            postgres_password="password",
            postgres_database="interloper",
            image="img",
            **kwargs,
        )

    return factory


class _CapturingBatchV1:
    def __init__(self) -> None:
        self.jobs: list[client.V1Job] = []

    def create_namespaced_job(self, namespace: str, body: client.V1Job) -> None:
        self.jobs.append(body)


def _launched_container(launcher: KubernetesLauncher) -> client.V1Container:
    batch = _CapturingBatchV1()
    launcher._batch_v1 = cast(client.BatchV1Api, batch)
    launcher.launch(uuid4())
    return batch.jobs[0].spec.template.spec.containers[0]


def test_env_from_mounts_secrets(launcher_factory: Callable[..., KubernetesLauncher]) -> None:
    """Configured secret names become envFrom secret refs on the run container."""
    container = _launched_container(launcher_factory(env_from=["oauth-providers", "extra"]))
    assert [source.secret_ref.name for source in container.env_from] == ["oauth-providers", "extra"]


def test_env_from_defaults_to_none(launcher_factory: Callable[..., KubernetesLauncher]) -> None:
    """Without ``env_from`` the container spec carries no envFrom at all."""
    container = _launched_container(launcher_factory())
    assert container.env_from is None


class _StatusBatchV1:
    def __init__(self, status: client.V1JobStatus | None, *, missing: bool = False) -> None:
        self._status, self._missing = status, missing

    def read_namespaced_job_status(self, name: str, namespace: str) -> client.V1Job:
        if self._missing:
            raise RuntimeError("not found")
        return client.V1Job(status=self._status)


class _PodsCoreV1:
    def __init__(self, pods: list[client.V1Pod] | None) -> None:
        self._pods = pods

    def list_namespaced_pod(self, namespace: str, label_selector: str) -> client.V1PodList:
        if self._pods is None:
            raise RuntimeError("api unreachable")
        return client.V1PodList(items=self._pods)


def _describe(launcher: KubernetesLauncher, status: client.V1JobStatus | None, **batch: Any) -> Any:
    launcher._batch_v1 = cast(client.BatchV1Api, _StatusBatchV1(status, **batch))
    return launcher.describe_run(uuid4())


@pytest.mark.parametrize(
    ("status", "missing", "expected"),
    [
        (None, True, LaunchStatus.NOT_FOUND),
        (None, False, LaunchStatus.RUNNING),
        (client.V1JobStatus(active=1), False, LaunchStatus.RUNNING),
        (client.V1JobStatus(succeeded=1), False, LaunchStatus.SUCCEEDED),
    ],
)
def test_describe_run_maps_the_job_status(
    launcher_factory: Callable[..., KubernetesLauncher],
    status: client.V1JobStatus | None,
    missing: bool,
    expected: LaunchStatus,
) -> None:
    assert _describe(launcher_factory(), status, missing=missing).status is expected


def _terminated_pod() -> client.V1Pod:
    terminated = client.V1ContainerStateTerminated(reason="OOMKilled", exit_code=137, message="out of memory")
    container = client.V1ContainerStatus(
        name="run",
        image="img",
        image_id="",
        ready=False,
        restart_count=0,
        state=client.V1ContainerState(terminated=terminated),
    )
    return client.V1Pod(status=client.V1PodStatus(container_statuses=[container]))


@pytest.mark.parametrize(
    ("pods", "error"),
    [
        (None, "failed"),
        ([], "failed (no pod found)"),
        ([_terminated_pod()], "failed reason=OOMKilled exit_code=137 message=out of memory"),
    ],
)
def test_a_failed_job_reports_why(
    launcher_factory: Callable[..., KubernetesLauncher], pods: list[client.V1Pod] | None, error: str
) -> None:
    launcher = launcher_factory()
    launcher._core_v1 = cast(client.CoreV1Api, _PodsCoreV1(pods))

    state = _describe(launcher, client.V1JobStatus(failed=1))

    assert state.status is LaunchStatus.FAILED
    assert state.error is not None and state.error.endswith(error)
