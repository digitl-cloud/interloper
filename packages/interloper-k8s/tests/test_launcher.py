"""Tests for ``interloper_k8s.launcher``: one Job per run, its token in a Secret it owns, no database in the pod."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast
from uuid import uuid4

import pytest
from interloper.catalog import Catalog
from interloper.settings import LauncherSettings, ReaperSettings, RunnerSettings, ServerSettings
from interloper_db import RunStatus
from interloper_db.models import Run
from kubernetes import client, config

from interloper_k8s.launcher import KubernetesLauncher


@pytest.fixture
def launcher_factory(monkeypatch: pytest.MonkeyPatch) -> Callable[..., KubernetesLauncher]:
    monkeypatch.setattr(config, "load_incluster_config", lambda: None)

    def factory(**kwargs: Any) -> KubernetesLauncher:
        return KubernetesLauncher(catalog=Catalog(), image="img", internal_url="http://api", **kwargs)

    return factory


class _CapturingBatchV1:
    def __init__(self) -> None:
        self.jobs: list[client.V1Job] = []
        self.patches: list[tuple[str, dict[str, Any]]] = []
        self.deleted: list[str] = []

    def create_namespaced_job(self, namespace: str, body: client.V1Job) -> client.V1Job:
        self.jobs.append(body)
        body.metadata.uid = "job-uid"
        return body

    def patch_namespaced_job(self, name: str, namespace: str, body: dict[str, Any]) -> None:
        self.patches.append((name, body))

    def delete_namespaced_job(self, name: str, namespace: str, propagation_policy: str) -> None:
        self.deleted.append(name)


class _CapturingCoreV1:
    def __init__(self, *, fail: bool = False) -> None:
        self.secrets: list[client.V1Secret] = []
        self._fail = fail

    def create_namespaced_secret(self, namespace: str, body: client.V1Secret) -> client.V1Secret:
        if self._fail:
            raise RuntimeError("secrets is forbidden")
        self.secrets.append(body)
        return body


def _launch(
    launcher: KubernetesLauncher, *, partition_key: str | None = None
) -> tuple[_CapturingBatchV1, _CapturingCoreV1]:
    batch, core = _CapturingBatchV1(), _CapturingCoreV1()
    launcher._batch_v1 = cast(client.BatchV1Api, batch)
    launcher._core_v1 = cast(client.CoreV1Api, core)
    run = Run(id=uuid4(), org_id=uuid4(), status=RunStatus.DISPATCHED, partition_key=partition_key)
    launcher.launch(run, "ilr_x")
    return batch, core


def _launched_container(launcher: KubernetesLauncher) -> client.V1Container:
    batch, _core = _launch(launcher)
    return batch.jobs[0].spec.template.spec.containers[0]


def test_the_run_pod_holds_no_database_credentials_and_reads_its_token_from_the_secret(
    launcher_factory: Callable[..., KubernetesLauncher],
) -> None:
    container = _launched_container(launcher_factory())
    env = {variable.name: variable for variable in container.env}

    token = env["INTERLOPER_RUN_TOKEN"]
    assert token.value is None
    assert token.value_from.secret_key_ref.key == "token"
    assert env["INTERLOPER_SERVER_INTERNAL_URL"].value == "http://api"
    assert not [name for name in env if name.startswith("INTERLOPER_POSTGRES") or name == "INTERLOPER_ENCRYPTION_KEY"]


def test_the_job_starts_once_the_secret_it_owns_exists(launcher_factory: Callable[..., KubernetesLauncher]) -> None:
    batch, core = _launch(launcher_factory(), partition_key="2026-10-26")
    (job,), (secret,) = batch.jobs, core.secrets
    container = job.spec.template.spec.containers[0]

    assert job.spec.suspend is True
    assert batch.patches == [(job.metadata.name, {"spec": {"suspend": False}})]
    # The pod loads the run's DAG, partition and metadata by its id: only the token is handed over.
    assert container.args == ["launch", job.metadata.labels["interloper.run_id"]]
    assert secret.string_data == {"token": "ilr_x"}
    (owner,) = secret.metadata.owner_references
    assert (owner.kind, owner.name, owner.uid, owner.block_owner_deletion) == (
        "Job",
        job.metadata.name,
        "job-uid",
        None,
    )
    assert job.spec.template.spec.volumes is None
    assert container.volume_mounts is None


def test_a_job_whose_secret_cannot_be_created_is_deleted_unstarted(
    launcher_factory: Callable[..., KubernetesLauncher],
) -> None:
    launcher = launcher_factory()
    batch, core = _CapturingBatchV1(), _CapturingCoreV1(fail=True)
    launcher._batch_v1 = cast(client.BatchV1Api, batch)
    launcher._core_v1 = cast(client.CoreV1Api, core)

    with pytest.raises(RuntimeError, match="forbidden"):
        launcher.launch(Run(id=uuid4(), org_id=uuid4(), status=RunStatus.DISPATCHED), "ilr_x")

    assert batch.deleted == [batch.jobs[0].metadata.name]
    assert batch.patches == []


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
        server=ServerSettings(internal_url="http://interloper-api:3000"),
        runner=RunnerSettings(),
        reaper=ReaperSettings(heartbeat_interval=5, heartbeat_timeout=45),
        catalog=Catalog(),
    )

    assert (launcher._heartbeat_interval, launcher._heartbeat_timeout) == (5, 45)
    assert launcher._internal_url == "http://interloper-api:3000"


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
