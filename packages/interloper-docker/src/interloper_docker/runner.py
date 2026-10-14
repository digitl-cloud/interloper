"""DockerRunner: each operation in its own container.

A container runs its operation's mini-DAG (the operation and its upstream
ancestors, the ancestors read-only) through ``interloper run --format
inline``, so the operation resolves its upstreams from their destinations
without recomputing them. The container reports its operation's progress as
``@EVENT:`` lines on stderr, which the host forwards to its EventBus while the
container runs; the host authors the operation's terminal event itself, from
the container's exit code and on the last attempt the container reported, so
an operation whose container dies without a word, or mid-retry, still ends.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import TYPE_CHECKING, Any

import docker
from docker import DockerClient
from docker.models.containers import Container
from interloper.errors import RunnerError, format_exception
from interloper.events import EventLogReader
from interloper.operation import Operation, OperationResult
from interloper.runner import Run, Runner
from interloper.telemetry.propagation import child_process_env
from pydantic import Field

if TYPE_CHECKING:
    from interloper.dag import DAG
    from interloper.partitioning import PartitionWindow

logger = logging.getLogger(__name__)


class DockerRun(Run):
    """One run of a DockerRunner: each operation in its own container, its events read from the container's log.

    Following a container (start it, forward its log, read its exit code)
    blocks, so each one is followed on a thread of the run's polling pool,
    one thread per container.
    """

    runner: DockerRunner
    reports_progress = False
    docker: DockerClient

    def __init__(
        self, runner: DockerRunner, dag: DAG, window: PartitionWindow | None, metadata: dict[str, Any]
    ) -> None:
        """Open a polling pool sized to the runner's capacity.

        Args:
            runner: The runner this is a run of.
            dag: The DAG to walk.
            window: The window the run covers, ``None`` when unscoped.
            metadata: Run-level metadata, carrying the ``run_id``.
        """
        super().__init__(runner, dag, window, metadata)
        self.polling = ThreadPoolExecutor(max_workers=runner.max_containers)

    async def open(self) -> None:
        """Connect to the Docker daemon."""
        self.docker = docker.from_env()

    async def execute(self, operation: Operation) -> OperationResult:
        """Run the operation in its own container, followed on the polling pool.

        Args:
            operation: The operation to execute.

        Returns:
            Its result, recorded on the last attempt the container reported.
        """
        reader = EventLogReader(operation.id, label=f"container {self.name(operation)}")
        # The child continues the run's trace, which only this context carries onto the pool thread.
        follow = partial(contextvars.copy_context().run, self.follow, operation, reader)
        result = await asyncio.get_running_loop().run_in_executor(self.polling, follow)
        self.state.advance(operation, reader.attempt)
        return result

    def follow(self, operation: Operation, reader: EventLogReader) -> OperationResult:
        """Start the operation's container, forward its log's events until it exits, then read its exit code.

        Args:
            operation: The operation the container executes.
            reader: Forwards the container's events.

        Returns:
            An empty result for exit code 0; otherwise a failure naming the
            code, or why the container could not be started or waited on.
        """
        try:
            container = self.launch(operation)
        except Exception as error:  # noqa: BLE001 - a container that cannot start fails its operation
            return OperationResult(error=format_exception(error))
        try:
            self.forward_log(container, reader)
            status_code = container.wait().get("StatusCode", 1)
        except Exception as error:  # noqa: BLE001 - losing the container fails its operation, never the run
            return OperationResult(error=format_exception(error))
        finally:
            if self.runner.auto_remove:
                self.remove(container)
        if status_code == 0:
            return OperationResult()
        exited = RunnerError(f"Container {container.short_id} exited with code {status_code}")
        return OperationResult(error=format_exception(exited))

    def launch(self, operation: Operation) -> Container:
        """Start the operation's container, detached.

        Args:
            operation: The operation the container executes.

        Returns:
            The running container.
        """
        return self.docker.containers.run(
            image=self.runner.image,
            name=self.name(operation),
            command=self.command(operation),
            environment=self.runner.environment(),
            volumes=self.runner.mounts() or None,
            labels={"interloper.component_id": operation.id},
            remove=False,
            detach=True,
            stdout=True,
            stderr=True,
        )

    def forward_log(self, container: Container, reader: EventLogReader) -> None:
        """Forward the operation's events from the container's log until the log ends.

        A stream that breaks is logged and ends the forwarding: the exit code,
        not the log, decides the operation's verdict.

        Args:
            container: The running container.
            reader: Forwards the container's events.
        """
        try:
            for chunk in container.logs(stream=True, follow=True, stdout=False, stderr=True):
                reader.feed(chunk)
        except Exception:
            logger.debug("Log stream of container %s ended early", container.short_id, exc_info=True)
        reader.close()

    def remove(self, container: Container) -> None:
        """Remove a finished container; a failure to is logged, never failing its operation.

        Args:
            container: The container to remove.
        """
        try:
            container.remove()
        except Exception:
            logger.warning("Removing container %s failed", container.short_id, exc_info=True)

    def command(self, operation: Operation) -> list[str]:
        """The command the container runs: its operation's mini-DAG through ``interloper run --format inline``.

        Args:
            operation: The operation the container executes.

        Returns:
            The argv, with the operation's own window as ``--date`` or
            ``--start-date``/``--end-date``.
        """
        spec = self.state.dag.mini_dag(operation.id).to_spec().model_dump(mode="json")
        command = ["interloper", "run", "--format", "inline", f"--run-id={self.state.run_id}", json.dumps(spec)]
        window = operation.effective_window(self.state.window)
        if window is not None and len(window) == 1:
            command.extend(["--date", window.first.key])
        elif window is not None:
            command.extend(["--start-date", window.first.key, "--end-date", window.last.key])
        return command

    def name(self, operation: Operation) -> str:
        """A container name unique to this operation and run.

        Args:
            operation: The operation the container executes.

        Returns:
            ``interloper_run_<run>_<operation>``, each id cut to 8 characters.
        """
        return f"interloper_run_{self.state.run_id[:8]}_{operation.id[:8]}"

    async def close(self) -> None:
        """Release the polling pool.

        A canceled run returns at once: whatever is still being followed
        finishes on its thread in the background.
        """
        self.polling.shutdown(wait=False)


class DockerRunner(Runner):
    """Execute each operation in its own Docker container, ``max_containers`` at a time.

    The container runs the image's ``interloper`` CLI on the operation's
    mini-DAG; the host forwards the container's events and records its exit
    code:

    ```py
    result = await DockerRunner(image="my-image", on_event=log_event).run(dag)
    # or, from a sync edge: il.run(DockerRunner(image=...).run(dag))
    ```
    """

    Run = DockerRun

    image: str = "interloper:latest-worker"
    max_containers: int = 4
    env_vars: dict[str, str] = Field(default_factory=dict)
    volumes: dict[str, dict[str, str]] | list[str] = Field(default_factory=dict)
    fail_fast: bool = False
    reraise: bool = False
    auto_remove: bool = True

    @property
    def capacity(self) -> int:
        """How many containers a run executes at once: ``max_containers``."""
        return self.max_containers

    def environment(self) -> dict[str, str]:
        """The environment every container gets.

        Returns:
            ``env_vars``, the switch that makes the child report its events on
            stderr, and the trace context the child continues.
        """
        environment = dict(self.env_vars)
        environment["INTERLOPER_EVENTS_TO_STDERR"] = "true"
        environment.update(child_process_env())
        return environment

    def mounts(self) -> dict[str, dict[str, str]]:
        """The volumes every container mounts, in docker-py's shape.

        Returns:
            The mounts keyed by host path; ``"host:container"`` entries are
            mounted read-write.
        """
        if isinstance(self.volumes, dict):
            return dict(self.volumes)
        mounts: dict[str, dict[str, str]] = {}
        for volume in self.volumes:
            host, bind = volume.split(":")[:2]
            mounts[host] = {"bind": bind, "mode": "rw"}
        return mounts
