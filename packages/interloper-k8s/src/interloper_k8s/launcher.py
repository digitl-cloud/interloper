"""Kubernetes launcher: runs each run as a Kubernetes Job."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any
from uuid import UUID

from interloper.catalog import Catalog
from interloper.errors import ConfigError
from interloper.telemetry.propagation import child_process_env

if TYPE_CHECKING:
    from interloper.settings import LauncherSettings, ReaperSettings, RunnerSettings, ServerSettings
    from interloper_db.models import Run
from interloper_scheduler.launcher import Launcher
from kubernetes import client, config

logger = logging.getLogger(__name__)


class KubernetesLauncher(Launcher):
    """Launches each run as a Kubernetes Job.

    The Job's container runs ``interloper launch``: it loads what the
    run executes from the API's ``internal_url`` and reports its events there,
    with the run's token. The token travels in a Secret the Job owns, so
    Kubernetes deletes it with the Job. The pod holds no database credentials.
    """

    @classmethod
    def from_settings(
        cls,
        settings: LauncherSettings,
        *,
        server: ServerSettings,
        runner: RunnerSettings,
        reaper: ReaperSettings,
        catalog: Catalog | None,
        store: Any | None = None,
    ) -> KubernetesLauncher:
        """Construct from scheduler settings (registry construction hook).

        The API's internal URL and the catalog are forwarded into the spawned
        environment; ``settings.config`` supplies the launcher-specific
        keyword arguments.

        Args:
            settings: The launcher settings block (image, namespace, resources).
            server: Server settings whose ``internal_url`` the pod reports to.
            runner: Runner settings the spawned process builds its runner from.
            reaper: Run liveness settings whose heartbeat fields the spawned
                process follows.
            catalog: Catalog forwarded as import paths; required, since a pod
                cannot see the caller's.
            store: Accepted for the registry's uniform hook and unused: a pod
                reports through the API.

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
            internal_url=server.internal_url,
            runner_type=runner.type,
            runner_config=runner.config,
            heartbeat_interval=reaper.heartbeat_interval,
            heartbeat_timeout=reaper.heartbeat_timeout,
            **settings.config,
        )

    def __init__(
        self,
        catalog: Catalog,
        image: str,
        internal_url: str = "http://localhost:3000",
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
        env_from: list[str] | None = None,
        heartbeat_interval: int = 10,
        heartbeat_timeout: int = 90,
    ) -> None:
        """Initialize the Kubernetes launcher.

        Args:
            catalog: Catalog to inject into the container.
            image: Container image to use for the Job.
            internal_url: The API's base URL as reached from the pod, which
                the run reports its events to.
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
            env_from: Names of Secrets mounted as env into the container, for
                whatever else a deployment's runs read from their environment.
            heartbeat_interval: Seconds between a run's heartbeats, forwarded
                to the container.
            heartbeat_timeout: The reaper's heartbeat timeout in seconds,
                forwarded to the container.
        """
        super().__init__(runner_type=runner_type, runner_config=runner_config)
        self._catalog = catalog
        self._image = image
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = heartbeat_timeout
        self._namespace = namespace
        self._internal_url = internal_url
        self._service_account_name = service_account_name
        self._image_pull_policy = image_pull_policy
        self._image_pull_secrets = image_pull_secrets or []
        self._resources = resources
        self._node_selector = node_selector
        self._tolerations = tolerations or []
        self._ttl_seconds_after_finished = ttl_seconds_after_finished
        self._env_from = env_from or []

        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self._batch_v1 = client.BatchV1Api()
        self._core_v1 = client.CoreV1Api()

    def launch(self, run: Run, token: str) -> None:
        """Create the run's Job suspended, the Secret it owns, then start the Job.

        The Secret carries the run's token, read into the environment: it does
        not belong in the Job's own spec, which every reader of Jobs and Pods
        sees. Owned by the Job, the Secret is deleted with it. The Job is
        created suspended because the Secret's owner reference needs the Job's
        uid, and a pod started before its Secret exists would wait for it
        forever; a Job whose Secret cannot be created is deleted before it ever
        starts a pod.

        Args:
            run: The dispatched run.
            token: The run's token, written into the Secret.
        """
        name = f"interloper-run-{str(run.id)[:8]}"
        labels = {"interloper.run_id": str(run.id)}
        created = self._batch_v1.create_namespaced_job(namespace=self._namespace, body=self._job(run, name, labels))
        try:
            owner = client.V1OwnerReference(api_version="batch/v1", kind="Job", name=name, uid=created.metadata.uid)
            secret = client.V1Secret(
                metadata=client.V1ObjectMeta(
                    name=name, namespace=self._namespace, labels=labels, owner_references=[owner]
                ),
                string_data={"token": token},
            )
            self._core_v1.create_namespaced_secret(namespace=self._namespace, body=secret)
            self._batch_v1.patch_namespaced_job(name=name, namespace=self._namespace, body={"spec": {"suspend": False}})
        except Exception:
            self._batch_v1.delete_namespaced_job(name=name, namespace=self._namespace, propagation_policy="Background")
            raise
        logger.info("Created Job %s for run %s", name, run.id)

    def _job(self, run: Run, name: str, labels: dict[str, str]) -> client.V1Job:
        """The suspended Job that executes the run, its token read from its Secret.

        Args:
            run: The dispatched run.
            name: The Job's name, which its Secret shares.
            labels: The labels the Job and its pod carry.

        Returns:
            The Job to create.
        """
        command = self.command(run)
        token = client.V1EnvVar(
            name="INTERLOPER_RUN_TOKEN",
            value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(name=name, key="token")),
        )
        container = client.V1Container(
            name="interloper",
            image=self._image,
            image_pull_policy=self._image_pull_policy,
            command=command[:1],
            args=command[1:],
            env=[*self._build_env(), token],
            env_from=[client.V1EnvFromSource(secret_ref=client.V1SecretEnvSource(name=n)) for n in self._env_from]
            if self._env_from
            else None,
            resources=self._build_resources(),
        )
        pod_spec = client.V1PodSpec(
            containers=[container],
            restart_policy="Never",
            service_account_name=self._service_account_name,
            node_selector=self._node_selector or None,
            tolerations=self._build_tolerations() if self._tolerations else None,
            image_pull_secrets=[client.V1LocalObjectReference(name=s) for s in self._image_pull_secrets]
            if self._image_pull_secrets
            else None,
        )
        return client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=name, namespace=self._namespace, labels=labels),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(metadata=client.V1ObjectMeta(labels=labels), spec=pod_spec),
                backoff_limit=0,
                ttl_seconds_after_finished=self._ttl_seconds_after_finished,
                suspend=True,
            ),
        )

    def diagnose(self, run_id: UUID) -> str | None:
        """Say why a run's pod stopped, from its termination state.

        Args:
            run_id: The run UUID.

        Returns:
            The pod's eviction reason and its container's termination reason,
            exit code and message, or ``None`` when its pod is still running,
            already deleted, or unreadable.
        """
        from typing import cast

        from kubernetes.client import V1PodList

        job_name = f"interloper-run-{str(run_id)[:8]}"
        try:
            pods = cast(
                V1PodList,
                self._core_v1.list_namespaced_pod(namespace=self._namespace, label_selector=f"job-name={job_name}"),
            )
        except Exception:  # noqa: BLE001 - a diagnosis is best-effort detail on a run already found dead
            return None
        if not pods.items or pods.items[0].status is None:
            return None

        status = pods.items[0].status
        parts = [f"pod {status.reason}"] if status.reason else []
        if status.message:
            parts.append(status.message)
        for container in status.container_statuses or []:
            terminated = container.state.terminated if container.state else None
            if terminated is None:
                continue
            parts.append(f"reason={terminated.reason}")
            if terminated.exit_code is not None:
                parts.append(f"exit_code={terminated.exit_code}")
            if terminated.message:
                parts.append(f"message={terminated.message}")
        return " ".join(parts) or None

    # -- Helpers ---------------------------------------------------------------

    def _build_env(self) -> list[client.V1EnvVar]:
        """Build the plain environment variables for the container; the token is read from the run's Secret.

        Returns:
            The environment the run pod is started with.
        """
        env_map: dict[str, str] = {
            "INTERLOPER_CATALOG": json.dumps(self._catalog.to_paths()),
            "INTERLOPER_RUNNER_TYPE": self._runner_type,
            "INTERLOPER_RUNNER_CONFIG": json.dumps(self._runner_config),
            "INTERLOPER_REAPER_HEARTBEAT_INTERVAL": str(self._heartbeat_interval),
            "INTERLOPER_REAPER_HEARTBEAT_TIMEOUT": str(self._heartbeat_timeout),
            "INTERLOPER_SERVER_INTERNAL_URL": self._internal_url,
        }
        env_map.update(child_process_env())
        return [client.V1EnvVar(name=k, value=v) for k, v in env_map.items()]

    def _build_resources(self) -> client.V1ResourceRequirements | None:
        """Build resource requirements for the container.

        Returns:
            The requirements, or None when none are configured.

        """
        if not self._resources:
            return None
        return client.V1ResourceRequirements(
            requests=self._resources.get("requests"),
            limits=self._resources.get("limits"),
        )

    def _build_tolerations(self) -> list[client.V1Toleration]:
        """Build tolerations for pod scheduling.

        Returns:
            The configured tolerations, empty when none are set.

        """
        return [
            client.V1Toleration(
                key=t.get("key"),
                operator=t.get("operator", "Equal"),
                value=t.get("value"),
                effect=t.get("effect"),
            )
            for t in self._tolerations
        ]
