"""Docker-based runner that executes each asset in its own container.

Each submitted asset runs inside a fresh container. A mini-DAG comprising
the target asset and all its upstream ancestors is sent to the container
via inline JSON so the asset can resolve its upstream dependencies from IO
without recomputing them.

**Real-time events** are streamed via **stderr** using the ``@EVENT:``
prefix (see :class:`~interloper.events.StderrEventHandler`).  Events for
the target asset are forwarded to the host EventBus; events for
non-materializable parent assets and container-internal ``RUN_*`` events
are dropped.  The host updates internal state with ``emit=False`` to
avoid duplicate events.
"""

from __future__ import annotations

import json
import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

import docker
from docker.client import DockerClient
from docker.models.containers import Container
from interloper.asset.base import Asset
from interloper.errors import RunnerError
from interloper.events import EventBus, EventType
from interloper.events.event import parse_event_from_log_line
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.runner.sync_runner import SyncRunner
from pydantic import Field, PrivateAttr

logger = logging.getLogger(__name__)

# Events emitted by the container's inner run — not forwarded to the host
# because the host manages its own run lifecycle.
_RUN_EVENTS = frozenset(
    {
        EventType.RUN_STARTED,
        EventType.RUN_COMPLETED,
        EventType.RUN_FAILED,
    }
)


class DockerRunner(SyncRunner):
    """Execute assets as individual Docker containers.

    For each asset, constructs a mini-DAG comprising the asset and all its
    upstream ancestors. The mini-DAG is sent to the container via inline JSON.
    Inside the container, all non-target assets are marked as
    ``materializable=False`` to avoid recomputation while still enabling
    IO-based dependency resolution.

    Events are emitted by the container process and streamed to the host
    via stderr.  The host forwards them to its EventBus (for persistence
    by the scheduler) and updates internal state silently (``emit=False``)
    to avoid duplicate events.

    Fully synchronous::

        with DockerRunner(image="my-image", on_event=log_event) as runner:
            result = runner.run(dag)
    """

    image: str = "interloper:latest-worker"
    max_containers: int = 4
    env_vars: dict[str, str] = Field(default_factory=dict)
    volumes: dict[str, dict[str, str]] | list[str] = Field(default_factory=dict)
    fail_fast: bool = False
    reraise: bool = False
    auto_remove: bool = True

    _docker: DockerClient = PrivateAttr()
    _poll_pool: ThreadPoolExecutor | None = PrivateAttr(default=None)
    _log_threads: dict[str, threading.Thread] = PrivateAttr(default_factory=dict)
    _stop_log_streaming: threading.Event = PrivateAttr(default_factory=threading.Event)
    _container_map: dict[Future[Any], Container] = PrivateAttr(default_factory=dict)

    def model_post_init(self, context: Any) -> None:
        """Initialize Docker client after model initialization."""
        super().model_post_init(context)
        self._docker = docker.from_env()

    # ------------------------------------------------------------------
    # Scheduling primitives
    # ------------------------------------------------------------------

    @property
    def _capacity(self) -> int:
        """Maximum number of concurrent containers."""
        return self.max_containers

    def _on_start(self) -> None:
        """Create the polling thread pool."""
        self._stop_log_streaming.clear()
        self._poll_pool = ThreadPoolExecutor(max_workers=self.max_containers)

    def _on_end(self) -> None:
        """Shut down log streaming and the polling pool."""
        self._stop_log_streaming.set()
        for thread in self._log_threads.values():
            thread.join(timeout=2.0)
        self._log_threads.clear()
        if self._poll_pool is not None:
            self._poll_pool.shutdown(wait=True, cancel_futures=False)
            self._poll_pool = None
        self._container_map.clear()

    def _submit_asset(
        self,
        asset: Asset,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> Future[Any]:
        """Launch an asset in a Docker container and return a polling Future.

        Returns:
            A Future that raises :class:`RunnerError` on container failure.
        """
        if self._poll_pool is None:
            raise RunnerError("Poll pool not initialized")

        mini_dag = self.state.dag.mini_dag(asset.id)
        dag_spec = mini_dag.to_spec().model_dump(mode="json")

        cmd = self._build_command(dag_spec, partition_or_window, self.state.run_id)
        name = self._build_name(asset)
        env = self._build_env()
        volumes = self._build_volumes()

        # emit=False: the container emits ASSET_STARTED via stderr
        self.state.mark_asset_running(asset, emit=False)

        try:
            container = self._docker.containers.run(
                image=self.image,
                name=name,
                command=cmd,
                environment=env,
                volumes=volumes if volumes else None,
                labels={"interloper.asset_id": asset.id},
                remove=False,
                detach=True,
                stdout=True,
                stderr=True,
            )
        except Exception as e:
            # Container never started — emit from the host
            self.state.mark_asset_failed(asset, str(e))
            done: Future[None] = Future()
            done.set_result(None)
            return done

        self._start_log_streaming(container, target_asset_id=asset.id)

        future = self._poll_pool.submit(self._poll_container, container)
        self._container_map[future] = container
        return future

    def _handle_completed(self, future: Future[Any], asset: Asset) -> None:
        """Process a completed container future and author the terminal event.

        The host authors the terminal event itself (``emit=True``) from the
        authoritative container exit status instead of trusting the child to
        have emitted one.  Deterministic event ids keep this idempotent: the
        child's own (richer) terminal was persisted first and the host's emit
        dedups away (``ON CONFLICT DO NOTHING``); if the child died without
        reporting one, the host's event is the only terminal, so the asset no
        longer orphans as ``running``.  Assets already terminal (e.g. marked
        failed during ``_submit_asset``) are skipped.
        """
        container = self._container_map.pop(future, None)
        if container is not None:
            self._stop_container_log_streaming(container)

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

        if container is not None and self.auto_remove:
            try:
                container.remove()
            except Exception:  # noqa: BLE001, S110
                pass

    def _handle_flushed_future(self, future: Future[Any], asset: Asset) -> None:
        """Clean up container after flush, authoring the terminal event.

        Same host-authored-terminal contract as :meth:`_handle_completed`
        (``emit=True``, idempotent via deterministic ids) so an asset still
        in flight when the run aborts is not left orphaned as ``running``.
        """
        container = self._container_map.pop(future, None)
        if container is not None:
            self._stop_container_log_streaming(container)

        info = self.state.asset_executions.get(asset.id)
        if not (info and info.is_terminal):
            try:
                future.result()
            except Exception as e:  # noqa: BLE001
                self.state.mark_asset_failed(asset, str(e), emit=True)
            else:
                self.state.mark_asset_completed(asset, emit=True)

        if container is not None and self.auto_remove:
            try:
                container.remove()
            except Exception:  # noqa: BLE001, S110
                pass

    # ------------------------------------------------------------------
    # Container polling
    # ------------------------------------------------------------------

    def _poll_container(self, container: Container) -> None:
        """Block until the container exits; raise on failure.

        Raises:
            RunnerError: If the container exits with a non-zero code.
        """
        result = container.wait()
        status_code = result.get("StatusCode", 1)
        if status_code != 0:
            cid = (container.id or "unknown")[:12]
            raise RunnerError(f"Container {cid} exited with code {status_code}")

    # ------------------------------------------------------------------
    # Command and environment builders
    # ------------------------------------------------------------------

    def _build_command(
        self,
        dag_spec: dict[str, Any],
        partition_or_window: Partition | PartitionWindow | None,
        run_id: str,
    ) -> list[str]:
        """Build the CLI command for asset execution in a container."""
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

    def _build_env(self) -> dict[str, str]:
        """Build the environment variables for the container."""
        env = dict(self.env_vars)
        env["INTERLOPER_EVENTS_TO_STDERR"] = "true"
        return env

    def _build_volumes(self) -> dict[str, dict[str, str]]:
        """Build the volume mounts for the container."""
        volumes: dict[str, dict[str, str]] = {}
        if isinstance(self.volumes, dict):
            volumes.update(self.volumes)
        elif isinstance(self.volumes, list):
            for volume in self.volumes:
                parts = volume.split(":")
                volumes[parts[0]] = {"bind": parts[1], "mode": "rw"}
        return volumes

    def _build_name(self, asset: Asset) -> str:
        """Build the name for the container."""
        return f"interloper_run_{self.state.run_id[:8]}_{asset.id[:8]}"

    # ------------------------------------------------------------------
    # Real-time event streaming (stderr)
    # ------------------------------------------------------------------

    def _start_log_streaming(self, container: Container, *, target_asset_id: str) -> None:
        """Stream events from the container's stderr to the host EventBus.

        Only events belonging to the **target asset** are forwarded.
        Events for non-materializable parent assets in the mini-DAG and
        container-internal ``RUN_*`` events are dropped.

        Args:
            container: The Docker container to stream from.
            target_asset_id: Only forward events with this ``asset_id``.
        """
        cid = (container.id or "???")[:12]

        def stream_logs() -> None:
            buf = ""
            try:
                for chunk in container.logs(stream=True, follow=True, stdout=False, stderr=True):
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
                                continue
                        except Exception:  # noqa: BLE001, S110
                            pass
                        logger.debug("[container %s] %s", cid, line)
            except Exception:  # noqa: BLE001, S110
                pass
            buf = buf.rstrip()
            if buf:
                logger.debug("[container %s] %s", cid, buf)

        thread = threading.Thread(target=stream_logs, daemon=True)
        thread.start()
        if container.id is not None:
            self._log_threads[container.id] = thread

    def _stop_container_log_streaming(self, container: Container) -> None:
        """Stop and clean up the log streaming thread for a container."""
        if container.id is None:
            return
        thread = self._log_threads.pop(container.id, None)
        if thread is not None:
            thread.join(timeout=1.0)
