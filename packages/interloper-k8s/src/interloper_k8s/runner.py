"""KubernetesRunner: each operation in its own Job.

A Job's pod runs its operation's mini-DAG (the operation and its upstream
ancestors, the ancestors read-only) through ``interloper run --format
inline``, so the operation resolves its upstreams from their destinations
without recomputing them. The pod reports its operation's progress as
``@EVENT:`` lines on stderr, which the host forwards to its EventBus while the
pod runs; the host authors the operation's terminal event itself, from the
Job's status, so an operation whose pod dies without a word still ends.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import TYPE_CHECKING, Any

from interloper.errors import RunnerError, format_exception
from interloper.events import EventLogReader
from interloper.operation import Operation, OperationResult
from interloper.runner.base import Runner
from interloper.runner.run import Run
from interloper.telemetry.propagation import child_process_env
from kubernetes import client, config
from pydantic import Field

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.partitioning import PartitionWindow

logger = logging.getLogger(__name__)

_STARTED = frozenset({"Running", "Succeeded", "Failed"})


class JobRun(Run):
    """One run of a KubernetesRunner: each operation in its own Job, its events read from the pod's log.

    Following a Job blocks, so each one is followed on a thread of the run's
    polling pool. The Job's status decides the verdict; the pod's log is
    forwarded alongside on a thread of its own, given a few seconds after the
    verdict to deliver the pod's last events, and abandoned past them: a pod
    lost with its node can leave a log stream open that never ends.
    """

    runner: KubernetesRunner
    reports_progress = False

    def __init__(
        self, runner: KubernetesRunner, dag: DAG, window: PartitionWindow | None, metadata: dict[str, Any]
    ) -> None:
        """Connect to the cluster and open a polling pool sized to the runner's capacity.

        Args:
            runner: The runner this is a run of.
            dag: The DAG to walk.
            window: The window the run covers, ``None`` when unscoped.
            metadata: Run-level metadata, carrying the ``run_id``.
        """
        super().__init__(runner, dag, window, metadata)
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self.batch = client.BatchV1Api()
        self.core = client.CoreV1Api()
        self.polling = ThreadPoolExecutor(max_workers=runner.max_jobs)

    async def execute(self, operation: Operation) -> OperationResult:
        """Run the operation in its own Job, followed on the polling pool.

        Args:
            operation: The operation to execute.

        Returns:
            Its result.
        """
        # The child continues the run's trace, which only this context carries onto the pool thread.
        follow = partial(contextvars.copy_context().run, self.follow, operation)
        return await asyncio.get_running_loop().run_in_executor(self.polling, follow)

    def follow(self, operation: Operation) -> OperationResult:
        """Create the operation's Job, forward its pod's events, and wait for the Job's verdict.

        Args:
            operation: The operation the Job executes.

        Returns:
            An empty result when the Job succeeded; otherwise a failure naming
            the Job, or why it could not be created or watched.
        """
        name = self.name(operation)
        try:
            self.batch.create_namespaced_job(namespace=self.runner.namespace, body=self.job(operation, name))
        except Exception as error:  # noqa: BLE001 - a Job that cannot be created fails its operation
            return OperationResult(error=format_exception(error))
        finished = threading.Event()
        forwarding = threading.Thread(target=self.forward_log, args=(name, operation, finished), daemon=True)
        forwarding.start()
        try:
            succeeded = self.wait(name)
        except Exception as error:  # noqa: BLE001 - losing the Job fails its operation, never the run
            return OperationResult(error=format_exception(error))
        finally:
            finished.set()
            forwarding.join(timeout=5.0)
        if succeeded:
            return OperationResult()
        return OperationResult(error=format_exception(RunnerError(f"Job {name} failed")))

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

    def forward_log(self, name: str, operation: Operation, finished: threading.Event) -> None:
        """Forward the operation's events from the Job's pod log until the log ends.

        A stream that breaks is logged and ends the forwarding: the Job's
        status, not the log, decides the operation's verdict.

        Args:
            name: The Job's name.
            operation: The operation it executes.
            finished: Set once the Job's verdict is known.
        """
        pod = self.pod(name, finished)
        if pod is None:
            return
        reader = EventLogReader(operation.id, label=f"pod {pod}")
        try:
            stream = self.core.read_namespaced_pod_log(
                name=pod, namespace=self.runner.namespace, container="interloper", follow=True, _preload_content=False
            )
            for chunk in stream:
                reader.feed(chunk)
        except Exception:
            logger.debug("Log stream of pod %s ended early", pod, exc_info=True)
        reader.close()

    def pod(self, name: str, finished: threading.Event) -> str | None:
        """Wait for the Job's pod to start.

        Args:
            name: The Job's name.
            finished: Set once the Job's verdict is known, which ends the wait.

        Returns:
            The pod's name, or ``None`` when the Job finished without one starting.
        """
        while not finished.is_set():
            try:
                pods = self.core.list_namespaced_pod(namespace=self.runner.namespace, label_selector=f"job-name={name}")
            except Exception:
                logger.debug("Listing the pods of Job %s failed", name, exc_info=True)
            else:
                for pod in pods.items:
                    if pod.status is not None and pod.status.phase in _STARTED:
                        return pod.metadata.name
            finished.wait(self.runner.poll_interval)
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
            env=self.runner.environment() or None,
            resources=self.runner.requirements(),
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
        """The command the pod runs: its operation's mini-DAG through ``interloper run --format inline``.

        Args:
            operation: The operation the pod executes.

        Returns:
            The argv, with the operation's own window as ``--date`` or
            ``--start-date``/``--end-date``.
        """
        spec = self.state.dag.mini_dag(operation.id).to_spec().model_dump(mode="json")
        command = ["interloper", "run", "--format", "inline", f"--run-id={self.state.run_id}", json.dumps(spec)]
        window = operation.effective_window(self.state.window)
        if window is not None and len(window) == 1:
            command.extend(["--date", window.first.id])
        elif window is not None:
            command.extend(["--start-date", window.first.id, "--end-date", window.last.id])
        return command

    def name(self, operation: Operation) -> str:
        """A Job name unique to this operation and run, within Kubernetes' 63 characters.

        Args:
            operation: The operation the Job executes.

        Returns:
            ``interloper-run-<run>-<operation>``, each id cut to 8 characters.
        """
        return f"interloper-run-{self.state.run_id[:8]}-{operation.id[:8]}"[:63]

    async def close(self) -> None:
        """Release the polling pool.

        A canceled run returns at once: whatever is still being followed
        finishes on its thread in the background.
        """
        self.polling.shutdown(wait=False)


class KubernetesRunner(Runner):
    """Execute each operation in its own Kubernetes Job, ``max_jobs`` at a time.

    The Job's pod runs the image's ``interloper`` CLI on the operation's
    mini-DAG; the host forwards the pod's events and records the Job's status:

    ```py
    result = await KubernetesRunner(image="my-image", on_event=log_event).run(dag)
    # or, from a sync edge: il.run(KubernetesRunner(image=...).run(dag))
    ```
    """

    Run = JobRun

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
            ``env_vars``, the switch that makes the child report its events on
            stderr, and the trace context the child continues.
        """
        environment = [client.V1EnvVar(name=name, value=value) for name, value in self.env_vars.items()]
        environment.append(client.V1EnvVar(name="INTERLOPER_EVENTS_TO_STDERR", value="true"))
        environment.extend(client.V1EnvVar(name=name, value=value) for name, value in child_process_env().items())
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
