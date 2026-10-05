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
from interloper.settings import LauncherSettings, PostgresSettings, ReaperSettings, RunnerSettings
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


def test_the_heartbeat_settings_reach_the_run_container(
    launcher_factory: Callable[..., KubernetesLauncher],
) -> None:
    container = _launched_container(launcher_factory(heartbeat_interval=5, heartbeat_timeout=45))
    env = {variable.name: variable.value for variable in container.env}
    assert (env["INTERLOPER_REAPER_HEARTBEAT_INTERVAL"], env["INTERLOPER_REAPER_HEARTBEAT_TIMEOUT"]) == ("5", "45")


def test_from_settings_forwards_the_heartbeat_settings(launcher_factory: Callable[..., KubernetesLauncher]) -> None:
    launcher = KubernetesLauncher.from_settings(
        LauncherSettings(type="kubernetes", config={"image": "img"}),
        postgres=PostgresSettings(),
        runner=RunnerSettings(),
        reaper=ReaperSettings(heartbeat_interval=5, heartbeat_timeout=45),
        catalog=Catalog(),
    )

    assert (launcher._heartbeat_interval, launcher._heartbeat_timeout) == (5, 45)


class _PodsCoreV1:
    def __init__(self, pods: list[client.V1Pod] | None) -> None:
        self._pods = pods

    def list_namespaced_pod(self, namespace: str, label_selector: str) -> client.V1PodList:
        if self._pods is None:
            raise RuntimeError("api unreachable")
        return client.V1PodList(items=self._pods)


def _pod(
    *,
    terminated: client.V1ContainerStateTerminated | None = None,
    reason: str | None = None,
    message: str | None = None,
) -> client.V1Pod:
    container = client.V1ContainerStatus(
        name="run",
        image="img",
        image_id="",
        ready=False,
        restart_count=0,
        state=client.V1ContainerState(terminated=terminated),
    )
    return client.V1Pod(status=client.V1PodStatus(reason=reason, message=message, container_statuses=[container]))


@pytest.mark.parametrize(
    ("pods", "diagnosis"),
    [
        (None, None),
        ([], None),
        ([_pod()], None),
        (
            [_pod(terminated=client.V1ContainerStateTerminated(reason="OOMKilled", exit_code=137, message="oom"))],
            "reason=OOMKilled exit_code=137 message=oom",
        ),
        ([_pod(reason="Evicted", message="The node was low on memory.")], "pod Evicted The node was low on memory."),
    ],
)
def test_diagnose_reads_the_pods_termination_state(
    launcher_factory: Callable[..., KubernetesLauncher], pods: list[client.V1Pod] | None, diagnosis: str | None
) -> None:
    launcher = launcher_factory()
    launcher._core_v1 = cast(client.CoreV1Api, _PodsCoreV1(pods))

    assert launcher.diagnose(uuid4()) == diagnosis
