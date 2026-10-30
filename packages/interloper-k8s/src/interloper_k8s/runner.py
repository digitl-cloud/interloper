"""KubernetesRunner: each operation in its own Job.

Each execution of an operation is one Job, whose pod executes that operation
for the host: on the platform, ``interloper launch <run> --operation-id`` loads
the host's run by its id; outside it, ``interloper run --operation-id`` is given
the operation's mini-DAG (the operation and its upstreams, the upstreams
read-only). Either way the operation
resolves its upstreams from their destinations without recomputing them. The
host owns the operation's lifecycle and its retries; the pod reports only what
happens inside the operation, and leaves why it failed in its termination
message.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import TYPE_CHECKING, Any

from interloper.errors import RunnerError, format_exception
from interloper.operation import Operation, OperationResult
from interloper.runner import Run, Runner
from interloper.settings import AppSettings
from interloper.telemetry.propagation import child_process_env
from kubernetes import client, config
from pydantic import Field

if TYPE_CHECKING:
    from interloper.dag import DAG
    from interloper.partitioning import PartitionWindow

logger = logging.getLogger(__name__)


class KubernetesRun(Run):
    """One run of a KubernetesRunner: each execution of an operation in its own Job.

    Following a Job blocks, so each one is followed on a thread of the run's
    polling pool.
    """

    runner: KubernetesRunner
    batch: client.BatchV1Api
    core: client.CoreV1Api

    def __init__(
        self, runner: KubernetesRunner, dag: DAG, window: PartitionWindow | None, metadata: dict[str, Any]
    ) -> None:
        """Open a polling pool sized to the runner's capacity.

        Args:
            runner: The runner this is a run of.
            dag: The DAG to walk.
            window: The window the run covers, ``None`` when unscoped.
            metadata: Run-level metadata, carrying the ``run_id``.
        """
        super().__init__(runner, dag, window, metadata)
        self.polling = ThreadPoolExecutor(max_workers=runner.max_jobs)

    async def open(self) -> None:
        """Connect to the cluster: in-cluster when the host runs in a pod, from the kubeconfig otherwise."""
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self.batch = client.BatchV1Api()
        self.core = client.CoreV1Api()

    async def execute(self, operation: Operation) -> OperationResult:
        """Execute the operation once in its own Job, followed on the polling pool.

        Args:
            operation: The operation to execute.

        Returns:
            The execution's result.
        """
        # The child continues the run's trace, which only this context carries onto the pool thread.
        follow = partial(contextvars.copy_context().run, self.follow, operation)
        return await asyncio.get_running_loop().run_in_executor(self.polling, follow)

    def follow(self, operation: Operation) -> OperationResult:
        """Create the execution's Job and wait for it to finish.

        Args:
            operation: The operation the Job executes.

        Returns:
            An empty result when the Job succeeded; otherwise the failure its
            pod left in its termination message, or one naming the Job, or why
            it could not be created or watched.
        """
        name = self.name(operation)
        try:
            self.batch.create_namespaced_job(namespace=self.runner.namespace, body=self.job(operation, name))
            if self.wait(name):
                return OperationResult()
            message = self.termination_message(name)
        except Exception as error:  # noqa: BLE001 - a Job that cannot be created or watched fails its execution
            return OperationResult(error=format_exception(error))
        return OperationResult.from_termination_message(message, format_exception(RunnerError(f"Job {name} failed")))

    def termination_message(self, name: str) -> str | None:
        """The message the Job's pod left when its container terminated.

        Args:
            name: The Job's name.

        Returns:
            The message, ``None`` when the pod wrote none or is gone.
        """
        pods = self.core.list_namespaced_pod(namespace=self.runner.namespace, label_selector=f"job-name={name}").items
        for pod in pods:
            for status in (pod.status and pod.status.container_statuses) or []:
                if status.state and status.state.terminated and status.state.terminated.message:
                    return status.state.terminated.message
        return None

    def wait(self, name: str) -> bool:
        """Wait for the Job to finish.

        Args:
            name: The Job's name.

        Returns:
            Whether it succeeded.
        """
        while (succeeded := self.outcome(name)) is None:
            time.sleep(self.runner.poll_interval)
        return succeeded

    def outcome(self, name: str) -> bool | None:
        """Whether the Job succeeded: ``True`` or ``False`` once finished, ``None`` while it runs.

        Args:
            name: The Job's name.

        Returns:
            The Job's outcome so far.
        """
        status = self.batch.read_namespaced_job_status(name=name, namespace=self.runner.namespace).status
        if status is not None and status.succeeded:
            return True
        if status is not None and status.failed:
            return False
        return None

    def job(self, operation: Operation, name: str) -> client.V1Job:
        """The Job that runs the operation's mini-DAG, once, in the runner's pod settings.

        Args:
            operation: The operation the Job executes.
            name: The Job's name.

        Returns:
            The Job to create.
        """
        labels = {"interloper.component_id": operation.id, "interloper.run_id": self.state.run_id}
        command = self.command(operation)
        container = client.V1Container(
            name="interloper",
            image=self.runner.image,
            image_pull_policy=self.runner.image_pull_policy,
            command=command[:1],
            args=command[1:],
            env=self.runner.environment(),
            resources=self.runner.requirements(),
            termination_message_path=OperationResult.TERMINATION_MESSAGE_PATH,
        )
        pull_secrets = [client.V1LocalObjectReference(name=secret) for secret in self.runner.image_pull_secrets]
        pod = client.V1PodSpec(
            containers=[container],
            restart_policy="Never",
            service_account_name=self.runner.service_account_name,
            node_selector=self.runner.node_selector or None,
            tolerations=self.runner.pod_tolerations() or None,
            image_pull_secrets=pull_secrets or None,
        )
        return client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                name=name,
                namespace=self.runner.namespace,
                labels=labels,
                annotations={"interloper.component_id": operation.id},
            ),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(metadata=client.V1ObjectMeta(labels=labels), spec=pod),
                backoff_limit=0,
                ttl_seconds_after_finished=self.runner.ttl_seconds_after_finished,
            ),
        )

    def command(self, operation: Operation) -> list[str]:
        """The command the pod runs to execute one operation of the host's run.

        A platform run's child runs ``interloper launch <run> --operation-id``
        and loads the run by its id, as the host's token; any other run's
        child runs ``interloper run --operation-id`` on the operation's
        mini-DAG and window, given as its arguments.

        Args:
            operation: The operation the pod executes.

        Returns:
            The argv.
        """
        if AppSettings.get().run.token:
            return ["interloper", "launch", self.state.run_id, f"--operation-id={operation.id}"]
        spec = self.state.dag.mini_dag(operation.id).to_spec().model_dump(mode="json")
        command = ["interloper", "run", "--format", "inline", json.dumps(spec), f"--operation-id={operation.id}"]
        window = operation.effective_window(self.state.window)
        if window is not None and len(window) == 1:
            command.extend(["--date", window.first.key])
        elif window is not None:
            command.extend(["--start-date", window.first.key, "--end-date", window.last.key])
        return command

    def name(self, operation: Operation) -> str:
        """A Job name unique to this execution of the operation in this run, within Kubernetes' 63 characters.

        Args:
            operation: The operation the Job executes.

        Returns:
            ``interloper-run-<run>-<operation>-<n>``, each id cut to 8 characters, ``n`` counting the
            operation's executions.
        """
        number = self.state.executions[operation.id].attempt
        return f"interloper-run-{self.state.run_id[:8]}-{operation.id[:8]}-{number}"[:63]

    async def close(self) -> None:
        """Release the polling pool.

        A canceled run returns at once: whatever is still being followed
        finishes on its thread in the background.
        """
        self.polling.shutdown(wait=False)


class KubernetesRunner(Runner):
    """Execute each operation in its own Kubernetes Job, ``max_jobs`` at a time.

    Each execution's pod runs the image's ``interloper`` CLI on that one
    operation; the host records the execution and retries it:

    ```py
    result = await KubernetesRunner(image="my-image", on_event=log_event).run(dag)
    # or, from a sync edge: il.run(KubernetesRunner(image=...).run(dag))
    ```
    """

    Run = KubernetesRun

    image: str
    namespace: str = "default"
    max_jobs: int = 4
    env_vars: dict[str, str] = Field(default_factory=dict)
    service_account_name: str | None = None
    image_pull_policy: str | None = None
    image_pull_secrets: list[str] = Field(default_factory=list)
    resources: dict[str, dict[str, str]] | None = None
    node_selector: dict[str, str] | None = None
    tolerations: list[dict[str, Any]] = Field(default_factory=list)
    poll_interval: float = 1.0
    ttl_seconds_after_finished: int = 300
    fail_fast: bool = False
    reraise: bool = False

    @property
    def capacity(self) -> int:
        """How many Jobs a run executes at once: ``max_jobs``."""
        return self.max_jobs

    def environment(self) -> list[client.V1EnvVar]:
        """The environment every pod gets.

        Returns:
            ``env_vars``, the trace context the child continues and, on the
            platform, the host run's token and the API's URL, with which the
            child loads the run and reports to it.
        """
        environment = [client.V1EnvVar(name=name, value=value) for name, value in self.env_vars.items()]
        environment.extend(client.V1EnvVar(name=name, value=value) for name, value in child_process_env().items())
        settings = AppSettings.get()
        if settings.run.token:
            environment.extend(
                [
                    client.V1EnvVar(name="INTERLOPER_RUN_TOKEN", value=settings.run.token),
                    client.V1EnvVar(name="INTERLOPER_SERVER_INTERNAL_URL", value=settings.server.internal_url),
                ]
            )
        return environment

    def requirements(self) -> client.V1ResourceRequirements | None:
        """The resources every pod requests and is limited to.

        Returns:
            The requirements, ``None`` when none are configured.
        """
        if not self.resources:
            return None
        return client.V1ResourceRequirements(
            requests=self.resources.get("requests"), limits=self.resources.get("limits")
        )

    def pod_tolerations(self) -> list[client.V1Toleration]:
        """The tolerations every pod is scheduled with.

        Returns:
            The configured tolerations, empty when none are set.
        """
        return [
            client.V1Toleration(
                key=toleration.get("key"),
                operator=toleration.get("operator", "Equal"),
                value=toleration.get("value"),
                effect=toleration.get("effect"),
            )
            for toleration in self.tolerations
        ]
