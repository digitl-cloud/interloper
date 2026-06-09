"""Kubernetes-based runner that executes each asset in its own Job.

Each submitted asset runs inside a Kubernetes Job. A mini-DAG comprising
the target asset and all its upstream ancestors is sent to the container
via inline JSON so the asset can resolve its upstream dependencies from IO
without recomputing them.
"""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, cast

from interloper.asset.base import Asset
from interloper.errors import RunnerError
from interloper.events import EventBus, EventType
from interloper.events.event import parse_event_from_log_line
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.runner.sync_runner import SyncRunner
from kubernetes import client, config
from kubernetes.client import V1Job
from pydantic import Field, PrivateAttr

# Events emitted by the container's inner run — not forwarded to the host.
_RUN_EVENTS = frozenset(
    {
        EventType.RUN_STARTED,
        EventType.RUN_COMPLETED,
        EventType.RUN_FAILED,
    }
)


class KubernetesRunner(SyncRunner):
    """Execute assets as individual Kubernetes Jobs.

    For each asset, constructs a mini-DAG comprising the asset and all its
    upstream ancestors. The mini-DAG is sent to the container via inline JSON.
    Inside the container, all non-target assets are marked as
    ``materializable=False`` to avoid recomputation while still enabling
    IO-based dependency resolution.

    Fully synchronous::

        with KubernetesRunner(image="my-image", on_event=log_event) as runner:
            result = runner.run(dag)
    """

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

    _batch_v1: client.BatchV1Api | None = PrivateAttr(default=None)
    _core_v1: client.CoreV1Api | None = PrivateAttr(default=None)
    _poll_pool: ThreadPoolExecutor | None = PrivateAttr(default=None)
    _job_map: dict[Future[Any], str] = PrivateAttr(default_factory=dict)
    _log_threads: dict[str, threading.Thread] = PrivateAttr(default_factory=dict)
    _stop_log_streaming: threading.Event = PrivateAttr(default_factory=threading.Event)

    # ------------------------------------------------------------------
    # Scheduling primitives
    # ------------------------------------------------------------------

    @property
    def _capacity(self) -> int:
        return self.max_jobs

    def _on_start(self) -> None:
        """Initialize Kubernetes client."""
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()

        self._batch_v1 = client.BatchV1Api()
        self._core_v1 = client.CoreV1Api()
        self._stop_log_streaming.clear()
        self._poll_pool = ThreadPoolExecutor(max_workers=self.max_jobs)

    def _on_end(self) -> None:
        """Signal all log streaming threads to stop."""
        self._stop_log_streaming.set()
        for thread in self._log_threads.values():
            thread.join(timeout=2.0)
        self._log_threads.clear()
        if self._poll_pool is not None:
            self._poll_pool.shutdown(wait=True, cancel_futures=False)
            self._poll_pool = None
        self._job_map.clear()

    def _submit_asset(
        self,
        asset: Asset,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> Future[Any]:
        """Launch an asset as a Kubernetes Job and return a polling Future."""
        if self._poll_pool is None or self._batch_v1 is None:
            raise RunnerError("Kubernetes client not initialized")

        mini_dag = self.state.dag.mini_dag(asset.id)
        dag_spec = mini_dag.to_spec().model_dump(mode="json")

        cmd = self._build_command(dag_spec, partition_or_window, self.state.run_id)
        job_name = self._build_job_name(asset)
        env = self._build_env()
        resources = self._build_resources()
        tolerations = self._build_tolerations()

        container = client.V1Container(
            name="interloper",
            image=self.image,
            image_pull_policy=self.image_pull_policy,
            command=cmd[:1],
            args=cmd[1:],
            env=env if env else None,
            resources=resources,
        )

        pod_spec = client.V1PodSpec(
            containers=[container],
            restart_policy="Never",
            service_account_name=self.service_account_name,
            node_selector=self.node_selector if self.node_selector else None,
            tolerations=tolerations if tolerations else None,
            image_pull_secrets=[client.V1LocalObjectReference(name=s) for s in self.image_pull_secrets]
            if self.image_pull_secrets
            else None,
        )

        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                name=job_name,
                namespace=self.namespace,
                labels={
                    "interloper.asset_id": asset.id,
                    "interloper.run_id": self.state.run_id,
                },
                annotations={
                    "interloper.asset_id": asset.id,
                },
            ),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        labels={
                            "interloper.asset_id": asset.id,
                            "interloper.run_id": self.state.run_id,
                        }
                    ),
                    spec=pod_spec,
                ),
                backoff_limit=0,
                ttl_seconds_after_finished=self.ttl_seconds_after_finished,
            ),
        )

        self.state.mark_asset_running(asset, emit=False)

        try:
            self._batch_v1.create_namespaced_job(namespace=self.namespace, body=job)
        except Exception as e:
            self.state.mark_asset_failed(asset, str(e))
            done: Future[None] = Future()
            done.set_result(None)
            return done

        self._start_log_streaming(job_name, target_asset_id=asset.id)

        future = self._poll_pool.submit(self._poll_job, job_name)
        self._job_map[future] = job_name
        return future

    def _cancel_all(self, handles: list[Any]) -> None:
        """Cancel all running jobs."""
        assert self._batch_v1 is not None

        for future in handles:
            job_name = self._job_map.get(future)
            if job_name is None:
                continue
            self._stop_job_log_streaming(job_name)
            try:
                self._batch_v1.delete_namespaced_job(
                    name=job_name,
                    namespace=self.namespace,
                    body=client.V1DeleteOptions(propagation_policy="Background"),
                )
            except Exception:  # noqa: BLE001, S110
                pass

    def _handle_completed(self, future: Future[Any], asset: Asset) -> None:
        """Process a completed job future and author the terminal event.

        The host authors the terminal event itself (``emit=True``) from the
        authoritative **Job status** (succeeded vs failed) instead of trusting
        the child to have emitted one.  Deterministic event ids keep this
        idempotent: when the child *did* stream its own (richer) terminal it was
        persisted first and the host's emit dedups away
        (``ON CONFLICT DO NOTHING``); when the child died without reporting one
        (eviction / OOM / deadline mid-retry) the host's event is the only
        terminal, so the asset no longer orphans as ``running``.

        The log thread is joined with a longer timeout first so a child terminal
        that is still in flight wins the race.
        """
        job_name = self._job_map.pop(future, None)
        if job_name is not None:
            self._stop_job_log_streaming(job_name, timeout=5.0)

        info = self.state.asset_executions.get(asset.id)
        if not (info and info.is_terminal):
            try:
                future.result()
            except Exception as e:
                self.state.mark_asset_failed(asset, str(e), emit=True)
                if self.fail_fast or self.reraise:
                    raise
            else:
                self.state.mark_asset_completed(asset, emit=True)

    def _handle_flushed_future(self, future: Future[Any], asset: Asset) -> None:
        """Clean up job after flush, authoring the terminal event.

        Same host-authored-terminal contract as :meth:`_handle_completed`
        (``emit=True``, idempotent via deterministic ids) so an asset still
        in flight when the run aborts is not left orphaned as ``running``.
        """
        job_name = self._job_map.pop(future, None)
        if job_name is not None:
            self._stop_job_log_streaming(job_name)

        info = self.state.asset_executions.get(asset.id)
        if not (info and info.is_terminal):
            try:
                future.result()
            except Exception as e:  # noqa: BLE001
                self.state.mark_asset_failed(asset, str(e), emit=True)
            else:
                self.state.mark_asset_completed(asset, emit=True)

    # ------------------------------------------------------------------
    # Job polling
    # ------------------------------------------------------------------

    def _poll_job(self, job_name: str) -> None:
        """Block until the Job completes or fails; raise on failure.

        Raises:
            RunnerError: If the job fails.
        """
        assert self._batch_v1 is not None

        while not self._stop_log_streaming.is_set():
            updated_job = cast(
                V1Job,
                self._batch_v1.read_namespaced_job_status(name=job_name, namespace=self.namespace),
            )

            assert updated_job.status is not None
            status = updated_job.status

            if status.succeeded is not None and status.succeeded > 0:
                return
            if status.failed is not None and status.failed > 0:
                raise RunnerError(f"Job {job_name} failed")

            time.sleep(self.poll_interval)

        raise RunnerError(f"Job {job_name} stopped (runner shutting down)")

    # ------------------------------------------------------------------
    # Job helpers
    # ------------------------------------------------------------------

    def _build_command(
        self,
        dag_spec: dict[str, Any],
        partition_or_window: Partition | PartitionWindow | None,
        run_id: str,
    ) -> list[str]:
        """Build the command to execute in the container."""
        cmd = [
            "interloper",
            "run",
            "--format",
            "inline",
            f"--run-id={run_id}",
            json.dumps(dag_spec),
        ]

        if isinstance(partition_or_window, TimePartition):
            cmd.extend(["--date", partition_or_window.value.strftime("%Y-%m-%d")])
        elif isinstance(partition_or_window, TimePartitionWindow):
            cmd.extend(
                [
                    "--start-date",
                    partition_or_window.start.strftime("%Y-%m-%d"),
                    "--end-date",
                    partition_or_window.end.strftime("%Y-%m-%d"),
                ]
            )

        return cmd

    def _build_env(self) -> list[client.V1EnvVar]:
        """Build the environment variables for the container."""
        env = [client.V1EnvVar(name=k, value=v) for k, v in self.env_vars.items()]
        env.append(client.V1EnvVar(name="INTERLOPER_EVENTS_TO_STDERR", value="true"))
        return env

    def _build_resources(self) -> client.V1ResourceRequirements | None:
        """Build the resource requirements for the container."""
        if not self.resources:
            return None
        return client.V1ResourceRequirements(
            requests=self.resources.get("requests"),
            limits=self.resources.get("limits"),
        )

    def _build_job_name(self, asset: Asset) -> str:
        """Build the name for the Kubernetes job (max 63 chars)."""
        # return f"interloper-run-{self.state.run_id[:8]}-{to_slug_case(type(asset).key)}"[:63]
        return f"interloper-run-{self.state.run_id[:8]}-{asset.id[:8]}"[:63]

    def _build_tolerations(self) -> list[client.V1Toleration]:
        """Build tolerations for pod scheduling."""
        return [
            client.V1Toleration(
                key=t.get("key"),
                operator=t.get("operator", "Equal"),
                value=t.get("value"),
                effect=t.get("effect"),
            )
            for t in self.tolerations
        ]

    def _start_log_streaming(self, job_name: str, *, target_asset_id: str) -> None:
        """Stream events from a K8s pod's logs to the host EventBus.

        Only events for the **target asset** are forwarded.
        Container-internal ``RUN_*`` events and events for
        non-materializable parent assets are dropped.

        Args:
            job_name: The K8s Job name to stream logs from.
            target_asset_id: Only forward events with this ``asset_id``.
        """
        if self._core_v1 is None:
            return

        core_v1 = self._core_v1

        def stream_logs() -> None:
            pod_name: str | None = None
            while not self._stop_log_streaming.is_set():
                try:
                    pods = cast(
                        client.V1PodList,
                        core_v1.list_namespaced_pod(
                            namespace=self.namespace,
                            label_selector=f"job-name={job_name}",
                        ),
                    )
                    if pods.items:
                        pod = pods.items[0]
                        if pod.status and pod.status.phase in ("Running", "Succeeded", "Failed"):
                            pod_name = pod.metadata.name
                            break
                except Exception:  # noqa: BLE001, S110
                    pass
                time.sleep(0.5)

            if pod_name is None or self._stop_log_streaming.is_set():
                return

            try:
                log_stream = cast(
                    Any,
                    core_v1.read_namespaced_pod_log(
                        name=pod_name,
                        namespace=self.namespace,
                        follow=True,
                        _preload_content=False,
                        container="interloper",
                    ),
                )
                buf = ""
                for chunk in log_stream:
                    if self._stop_log_streaming.is_set():
                        break
                    buf += chunk.decode("utf-8", errors="ignore")
                    while "\n" in buf:
                        line, buf = buf.split("\n", 1)
                        line = line.rstrip()
                        if not line:
                            continue
                        try:
                            event = parse_event_from_log_line(line)
                            if event is not None:
                                if event.type in _RUN_EVENTS:
                                    continue
                                event_asset_id = event.metadata.get("asset_id")
                                if event_asset_id and event_asset_id != target_asset_id:
                                    continue
                                EventBus.emit_event(event)
                        except Exception:  # noqa: BLE001, S110
                            pass
                buf = buf.rstrip()
                if buf:
                    try:
                        event = parse_event_from_log_line(buf)
                        if event is not None and event.type not in _RUN_EVENTS:
                            event_asset_id = event.metadata.get("asset_id")
                            if not event_asset_id or event_asset_id == target_asset_id:
                                EventBus.emit_event(event)
                    except Exception:  # noqa: BLE001, S110
                        pass
            except Exception:  # noqa: BLE001, S110
                pass

        thread = threading.Thread(target=stream_logs, daemon=True)
        thread.start()
        self._log_threads[job_name] = thread

    def _stop_job_log_streaming(self, job_name: str, timeout: float = 1.0) -> None:
        """Wait for the log streaming thread to finish.

        Args:
            job_name: The job whose log thread to join.
            timeout: Maximum seconds to wait for the thread to finish.
        """
        thread = self._log_threads.pop(job_name, None)
        if thread is not None:
            thread.join(timeout=timeout)
