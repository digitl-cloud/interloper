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
