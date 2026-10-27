"""DockerRunner: each operation in its own container.

Each attempt at an operation is one container, which runs the operation's
mini-DAG (the operation and its upstream ancestors, the ancestors read-only)
through ``interloper run --format inline``, so the operation resolves its
upstreams from their destinations without recomputing them. The host owns the
operation's lifecycle and its retries; the container reports only what
happens inside the operation, and leaves why its attempt failed in its
termination message.
"""

from __future__ import annotations

import asyncio
import contextvars
import io
import json
import logging
import os
import tarfile
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import TYPE_CHECKING, Any

import docker
from docker import DockerClient
from docker.errors import NotFound
from docker.models.containers import Container
from interloper.errors import RunnerError, format_exception
from interloper.operation import Operation, OperationResult
from interloper.runner import Run, Runner
from interloper.telemetry.propagation import child_process_env
from pydantic import Field

if TYPE_CHECKING:
    from interloper.dag import DAG
    from interloper.partitioning import PartitionWindow

logger = logging.getLogger(__name__)


class DockerRun(Run):
    """One run of a DockerRunner: each attempt at an operation in its own container.

    Following a container (start it, wait for its exit code) blocks, so each
    one is followed on a thread of the run's polling pool, one thread per
    container.
    """

    runner: DockerRunner
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

    async def attempt(self, operation: Operation) -> OperationResult:
        """Make one attempt in its own container, followed on the polling pool.

        Args:
            operation: The operation to attempt.

        Returns:
            The attempt's result.
        """
        # The child continues the run's trace, which only this context carries onto the pool thread.
        follow = partial(contextvars.copy_context().run, self.follow, operation)
        return await asyncio.get_running_loop().run_in_executor(self.polling, follow)

    def follow(self, operation: Operation) -> OperationResult:
        """Start the attempt's container and wait for it to exit.

        Args:
            operation: The operation the container attempts.

        Returns:
            An empty result for exit code 0; otherwise the failure the
            container left in its termination message, or one naming its exit
            code, or why it could not be started or waited on.
        """
        try:
            container = self.launch(operation)
        except Exception as error:  # noqa: BLE001 - a container that cannot start fails its attempt
            return OperationResult(error=format_exception(error))
        try:
            status_code = container.wait().get("StatusCode", 1)
            message = self.termination_message(container) if status_code else None
        except Exception as error:  # noqa: BLE001 - losing the container fails its attempt, never the run
            return OperationResult(error=format_exception(error))
        finally:
            if self.runner.auto_remove:
                self.remove(container)
        if status_code == 0:
            return OperationResult()
        exited = format_exception(RunnerError(f"Container {container.short_id} exited with code {status_code}"))
        return OperationResult.from_termination_message(message, exited)

    def termination_message(self, container: Container) -> str | None:
        """The message an exited container left at its termination message path.

        Args:
            container: The exited container.

        Returns:
            The message, ``None`` when the container wrote none.
        """
        try:
            chunks, _ = container.get_archive(OperationResult.TERMINATION_MESSAGE_PATH)
        except NotFound:
            return None
        with tarfile.open(fileobj=io.BytesIO(b"".join(chunks))) as archive:
            member = archive.extractfile(archive.getmembers()[0])
            return member.read().decode() if member is not None else None

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
            environment={**self.runner.environment(), "INTERLOPER_RUN_OPERATION": operation.id},
            volumes=self.runner.mounts() or None,
            labels={"interloper.component_id": operation.id},
            remove=False,
            detach=True,
            stdout=True,
            stderr=True,
        )

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
        """A container name unique to this attempt at the operation in this run.

        Args:
            operation: The operation the container attempts.

        Returns:
            ``interloper_run_<run>_<operation>_<attempt>``, each id cut to 8 characters.
        """
        attempt = self.state.executions[operation.id].attempt
        return f"interloper_run_{self.state.run_id[:8]}_{operation.id[:8]}_{attempt}"

    async def close(self) -> None:
        """Release the polling pool.

        A canceled run returns at once: whatever is still being followed
        finishes on its thread in the background.
        """
        self.polling.shutdown(wait=False)


class DockerRunner(Runner):
    """Execute each operation in its own Docker container, ``max_containers`` at a time.

    Each attempt's container runs the image's ``interloper`` CLI on the
    operation's mini-DAG; the host records the attempt and retries it:

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
            ``env_vars``, the trace context the child continues and, on the
            platform, the host run's token and the API's URL its child reports
            to; each container adds its own operation.
        """
        environment = dict(self.env_vars)
        environment.update(child_process_env())
        for name in ("INTERLOPER_RUN_TOKEN", "INTERLOPER_SERVER_INTERNAL_URL"):
            if value := os.environ.get(name):
                environment[name] = value
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
