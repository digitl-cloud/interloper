"""Kubernetes launcher: runs each job as a Kubernetes Job."""

from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING, Any
from uuid import UUID

from interloper.catalog.base import Catalog
from interloper.errors import ConfigError

if TYPE_CHECKING:
    from interloper.settings import LauncherSettings, PostgresSettings, RunnerSettings
from interloper_scheduler.launcher import Launcher, RunState, RunStatus
from kubernetes import client, config

logger = logging.getLogger(__name__)


class KubernetesLauncher(Launcher):
    """Launches each run as a Kubernetes Job.

    The Job runs a single container that executes
    ``interloper launch <run_id>``, which hydrates the DAG from the
    database and runs it to completion.

    Postgres connection parameters and the catalog are injected as
    environment variables.  The caller (``build_launcher``) provides
    the app-level defaults; any overrides from the launcher YAML
    config take precedence.
    """

    @classmethod
    def from_settings(
        cls,
        settings: LauncherSettings,
        *,
        postgres: PostgresSettings,
        runner: RunnerSettings,
        catalog: Catalog | None,
        store: Any | None = None,
    ) -> KubernetesLauncher:
        """Construct from scheduler settings (registry construction hook).

        Postgres credentials and the catalog are forwarded into the spawned
        environment; ``settings.config`` supplies the launcher-specific
        keyword arguments.

        Returns:
            The configured launcher.

        Raises:
            ConfigError: If no catalog is provided — this launcher spawns
                isolated processes and must reproduce the catalog there.
        """
        if catalog is None:
            raise ConfigError("The 'kubernetes' launcher requires a catalog.")
        return cls(
            catalog=catalog,
            postgres_host=postgres.host,
            postgres_port=postgres.port,
            postgres_user=postgres.user,
            postgres_password=postgres.password,
            postgres_database=postgres.database,
            runner_type=runner.type,
            runner_config=runner.config,
            **settings.config,
        )

    def __init__(
        self,
        catalog: Catalog,
        postgres_host: str,
        postgres_port: int,
        postgres_user: str,
        postgres_password: str,
        postgres_database: str,
        image: str,
        namespace: str = "default",
        runner_type: str = "async",
        runner_config: dict[str, Any] | None = None,
        service_account_name: str | None = None,
        image_pull_policy: str | None = None,
        image_pull_secrets: list[str] | None = None,
        resources: dict[str, dict[str, str]] | None = None,
        node_selector: dict[str, str] | None = None,
        tolerations: list[dict[str, Any]] | None = None,
        ttl_seconds_after_finished: int = 300,
    ) -> None:
        """Initialize the Kubernetes launcher.

        Args:
            catalog: Catalog to inject into the container.
            postgres_host: Postgres host for the container.
            postgres_port: Postgres port for the container.
            postgres_user: Postgres user for the container.
            postgres_password: Postgres password for the container.
            postgres_database: Postgres database for the container.
            image: Container image to use for the Job.
            namespace: Kubernetes namespace for Jobs.
            runner_type: Runner type name forwarded to the container.
            runner_config: Runner-specific kwargs forwarded to the container.
            service_account_name: K8s service account for the pod.
            image_pull_policy: Image pull policy (e.g. ``"Always"``).
            image_pull_secrets: Names of image pull secrets.
            resources: Resource requests/limits for the container.
            node_selector: Node selector labels for pod scheduling.
            tolerations: Pod tolerations for scheduling.
            ttl_seconds_after_finished: Seconds before K8s auto-deletes
                completed Jobs.
        """
        super().__init__(runner_type=runner_type, runner_config=runner_config)
        self._catalog = catalog
        self._image = image
        self._namespace = namespace
        self._postgres_host = postgres_host
        self._postgres_port = postgres_port
        self._postgres_user = postgres_user
        self._postgres_password = postgres_password
        self._postgres_database = postgres_database
        self._service_account_name = service_account_name
        self._image_pull_policy = image_pull_policy
        self._image_pull_secrets = image_pull_secrets or []
        self._resources = resources
        self._node_selector = node_selector
        self._tolerations = tolerations or []
        self._ttl_seconds_after_finished = ttl_seconds_after_finished

        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self._batch_v1 = client.BatchV1Api()
        self._core_v1 = client.CoreV1Api()

    def launch(self, run_id: UUID) -> None:
        """Create a Kubernetes Job that executes a single run.

        Args:
            run_id: The run UUID to execute.
        """
        env = self._build_env()
        job_name = f"interloper-run-{str(run_id)[:8]}"

        container = client.V1Container(
            name="interloper",
            image=self._image,
            image_pull_policy=self._image_pull_policy,
            command=["interloper"],
            args=["launch", str(run_id)],
            env=env,
            resources=self._build_resources(),
        )

        pod_spec = client.V1PodSpec(
            containers=[container],
            restart_policy="Never",
            service_account_name=self._service_account_name,
            node_selector=self._node_selector if self._node_selector else None,
            tolerations=self._build_tolerations() if self._tolerations else None,
            image_pull_secrets=[client.V1LocalObjectReference(name=s) for s in self._image_pull_secrets]
            if self._image_pull_secrets
            else None,
        )

        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                name=job_name,
                namespace=self._namespace,
                labels={"interloper.run_id": str(run_id)},
            ),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        labels={"interloper.run_id": str(run_id)},
                    ),
                    spec=pod_spec,
                ),
                backoff_limit=0,
                ttl_seconds_after_finished=self._ttl_seconds_after_finished,
            ),
        )

        try:
            self._batch_v1.create_namespaced_job(namespace=self._namespace, body=job)
            logger.info("Created Job %s for run %s", job_name, run_id)
        except Exception:
            logger.exception("Failed to create Job for run %s", run_id)
            raise

    def describe_run(self, run_id: UUID) -> RunState:
        """Return the authoritative state of a Job/pod.

        Used by the reaper to catch failed runs as soon as the Job
        terminates, without waiting for the fallback timeout.

        Args:
            run_id: The run UUID to describe.

        Returns:
            A :class:`RunState` indicating whether the Job is still
            running, has succeeded, has failed, or is gone.
        """
        from typing import cast

        from kubernetes.client import V1Job

        job_name = f"interloper-run-{str(run_id)[:8]}"

        try:
            job = cast(
                V1Job,
                self._batch_v1.read_namespaced_job_status(name=job_name, namespace=self._namespace),
            )
        except Exception:
            return RunState(status=RunStatus.NOT_FOUND)

        status = job.status
        if status is None:
            return RunState(status=RunStatus.RUNNING)

        if status.succeeded and status.succeeded > 0:
            return RunState(status=RunStatus.SUCCEEDED)

        if status.failed and status.failed > 0:
            return RunState(status=RunStatus.FAILED, error=self._pod_failure_reason(job_name))

        # active / no terminal condition yet
        return RunState(status=RunStatus.RUNNING)

    def _pod_failure_reason(self, job_name: str) -> str:
        """Build a short failure description from the pod's termination state."""
        from typing import cast

        from kubernetes.client import V1PodList

        try:
            pods = cast(
                V1PodList,
                self._core_v1.list_namespaced_pod(
                    namespace=self._namespace,
                    label_selector=f"job-name={job_name}",
                ),
            )
        except Exception:
            return f"Job {job_name} failed"

        if not pods.items:
            return f"Job {job_name} failed (no pod found)"

        pod = pods.items[0]
        parts = [f"Job {job_name} failed"]
        if pod.status is not None:
            for cs in pod.status.container_statuses or []:
                if cs.state and cs.state.terminated:
                    term = cs.state.terminated
                    parts.append(f"reason={term.reason}")
                    if term.exit_code is not None:
                        parts.append(f"exit_code={term.exit_code}")
                    if term.message:
                        parts.append(f"message={term.message}")
        return " ".join(parts)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_env(self) -> list[client.V1EnvVar]:
        """Build environment variables for the container."""
        env_map: dict[str, str] = {
            "INTERLOPER_POSTGRES_HOST": self._postgres_host,
            "INTERLOPER_POSTGRES_PORT": str(self._postgres_port),
            "INTERLOPER_POSTGRES_USER": self._postgres_user,
            "INTERLOPER_POSTGRES_PASSWORD": self._postgres_password,
            "INTERLOPER_POSTGRES_DATABASE": self._postgres_database,
            "INTERLOPER_CATALOG": json.dumps(self._catalog.to_paths()),
            "INTERLOPER_RUNNER_TYPE": self._runner_type,
            "INTERLOPER_RUNNER_CONFIG": json.dumps(self._runner_config),
        }
        encryption_key = os.environ.get("INTERLOPER_ENCRYPTION_KEY")
        if encryption_key:
            env_map["INTERLOPER_ENCRYPTION_KEY"] = encryption_key

        return [client.V1EnvVar(name=k, value=v) for k, v in env_map.items()]

    def _build_resources(self) -> client.V1ResourceRequirements | None:
        """Build resource requirements for the container."""
        if not self._resources:
            return None
        return client.V1ResourceRequirements(
            requests=self._resources.get("requests"),
            limits=self._resources.get("limits"),
        )

    def _build_tolerations(self) -> list[client.V1Toleration]:
        """Build tolerations for pod scheduling."""
        return [
            client.V1Toleration(
                key=t.get("key"),
                operator=t.get("operator", "Equal"),
                value=t.get("value"),
                effect=t.get("effect"),
            )
            for t in self._tolerations
        ]
